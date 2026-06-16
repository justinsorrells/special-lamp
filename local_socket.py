"""Asyncio Unix socket server for local v1 clients.

Local GUI/client processes connect over a Unix domain socket and exchange
newline-delimited JSON with the controller. This module only implements the
local client socket boundary; it does not implement Redis, GUI/webapp code,
firmware, or any direct board access.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from controller import DEFAULT_LOCAL_RESPONSE_MAX_LINE_BYTES, ControllerCore
from protocol import (
    CONTROLLER_MAX_LINE_BYTES,
    ErrorCode,
    MessageType,
    ProtocolValidationError,
    build_error_response,
    parse_line,
    serialize_message,
    validate_message,
)

LOCAL_REQUEST_MAX_LINE_BYTES = CONTROLLER_MAX_LINE_BYTES
LOCAL_RESPONSE_MAX_LINE_BYTES = DEFAULT_LOCAL_RESPONSE_MAX_LINE_BYTES

@dataclass(frozen=True)
class _OutboundMessage:
    message: dict[str, Any]
    critical: bool


@dataclass(eq=False)
class LocalClientConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    outbound_maxsize: int
    response_max_line_bytes: int = LOCAL_RESPONSE_MAX_LINE_BYTES
    on_event_dropped: Callable[[], None] | None = None
    on_critical_disconnect: Callable[[], None] | None = None
    connected: bool = True
    outbound: asyncio.Queue[_OutboundMessage | None] = field(init=False)
    writer_task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.outbound = asyncio.Queue(maxsize=self.outbound_maxsize)

    @property
    def is_connected(self) -> bool:
        return self.connected and not self.writer.is_closing()

    async def send_response(self, response: dict[str, Any]) -> bool:
        return await self._send(response, critical=True)

    async def send_event(self, event: dict[str, Any], *, critical: bool = True) -> bool:
        return await self._send(event, critical=critical)

    async def close(self, *, flush: bool = True) -> None:
        self.connected = False
        if flush:
            await self._put_sentinel()
            if (
                self.writer_task is not None
                and self.writer_task is not asyncio.current_task()
                and not self.writer_task.done()
            ):
                await self.writer_task
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass
        if not flush:
            await self._put_sentinel()
            if (
                self.writer_task is not None
                and self.writer_task is not asyncio.current_task()
                and not self.writer_task.done()
            ):
                self.writer_task.cancel()
                try:
                    await self.writer_task
                except asyncio.CancelledError:
                    pass

    async def writer_loop(self) -> None:
        while True:
            item = await self.outbound.get()
            if item is None:
                return
            try:
                self.writer.write(
                    serialize_message(item.message, max_line_bytes=self.response_max_line_bytes)
                )
                await self.writer.drain()
            except (ConnectionError, OSError, ProtocolValidationError):
                self.connected = False
                return

    async def _send(self, message: dict[str, Any], *, critical: bool) -> bool:
        if not self.is_connected:
            return False
        item = _OutboundMessage(message=message, critical=critical)
        try:
            self.outbound.put_nowait(item)
            return True
        except asyncio.QueueFull:
            if not critical:
                if self._evict_oldest_noncritical():
                    self.outbound.put_nowait(item)
                    return True
                self._record_event_dropped()
                return False
            while self.outbound.full() and self._evict_oldest_noncritical():
                pass
            if not self.outbound.full():
                self.outbound.put_nowait(item)
                return True
            self._record_critical_disconnect()
            await self.close(flush=False)
            return False

    def _evict_oldest_noncritical(self) -> bool:
        items: list[_OutboundMessage | None] = []
        dropped = False
        while True:
            try:
                items.append(self.outbound.get_nowait())
            except asyncio.QueueEmpty:
                break

        for item in items:
            if not dropped and item is not None and not item.critical:
                dropped = True
                self._record_event_dropped()
                continue
            self.outbound.put_nowait(item)
        return dropped

    def _record_event_dropped(self) -> None:
        if self.on_event_dropped is not None:
            self.on_event_dropped()

    def _record_critical_disconnect(self) -> None:
        if self.on_critical_disconnect is not None:
            self.on_critical_disconnect()

    async def _put_sentinel(self) -> None:
        try:
            self.outbound.put_nowait(None)
        except asyncio.QueueFull:
            # Make room for shutdown without blocking on a saturated client queue.
            _ = self.outbound.get_nowait()
            self.outbound.put_nowait(None)


class LocalUnixSocketServer:
    """Full-duplex Unix socket server for local controller clients."""

    _CRITICAL_EVENT_NAMES = frozenset(
        {
            "estop_triggered",
            "estop_ack",
            "safety_fault",
            "safety_state",
        }
    )

    def __init__(
        self,
        *,
        socket_path: str,
        controller: ControllerCore,
        outbound_queue_size: int = 1000,
        local_response_max_line_bytes: int = LOCAL_RESPONSE_MAX_LINE_BYTES,
        estop_reset_condition_cleared: Callable[[], bool] | None = None,
    ):
        self.socket_path = socket_path
        self.controller = controller
        self.outbound_queue_size = outbound_queue_size
        self.local_response_max_line_bytes = local_response_max_line_bytes
        self.estop_reset_condition_cleared = estop_reset_condition_cleared or (lambda: True)
        self.server: asyncio.AbstractServer | None = None
        self.clients: set[LocalClientConnection] = set()
        self.client_event_dropped = 0
        self.critical_event_disconnects = 0
        self.controller.set_local_event_sink(self.broadcast_event)

    async def start(self) -> None:
        await self._prepare_socket_path()
        self.server = await asyncio.start_unix_server(
            self._handle_client,
            path=self.socket_path,
            limit=LOCAL_REQUEST_MAX_LINE_BYTES,
        )
        os.chmod(self.socket_path, 0o600)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        await asyncio.gather(*(client.close() for client in list(self.clients)), return_exceptions=True)
        self.clients.clear()
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

    async def broadcast_event(self, event: dict[str, Any], *, critical: bool | None = None) -> None:
        validate_message(event)
        if event["type"] != MessageType.EVENT.value:
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "broadcast message must be an event")
        if critical is None:
            critical = self._is_critical_event(event)
        await asyncio.gather(
            *(client.send_event(event, critical=critical) for client in list(self.clients)),
            return_exceptions=True,
        )

    async def _prepare_socket_path(self) -> None:
        if not os.path.exists(self.socket_path):
            return
        try:
            reader, writer = await asyncio.open_unix_connection(self.socket_path)
            writer.close()
            await writer.wait_closed()
        except (ConnectionError, OSError):
            os.unlink(self.socket_path)
            return
        raise RuntimeError(f"live controller already owns {self.socket_path}")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = LocalClientConnection(
            reader=reader,
            writer=writer,
            outbound_maxsize=self.outbound_queue_size,
            response_max_line_bytes=self.local_response_max_line_bytes,
            on_event_dropped=self._increment_client_event_dropped,
            on_critical_disconnect=self._increment_critical_event_disconnects,
        )
        self.clients.add(client)
        client.writer_task = asyncio.create_task(client.writer_loop())
        try:
            while client.is_connected:
                try:
                    line = await reader.readuntil(b"\n")
                except asyncio.IncompleteReadError:
                    return
                except asyncio.LimitOverrunError:
                    self.controller.record_malformed_client_message()
                    self.controller.observe_controller_event(
                        {
                            "type": MessageType.EVENT.value,
                            "source": "controller",
                            "event": "malformed_client_message",
                            "details": {"error_code": ErrorCode.INVALID_JSON.value},
                        }
                    )
                    await client.send_response(
                        build_error_response(
                            seq=0,
                            target="client",
                            code=ErrorCode.INVALID_JSON,
                            message="client line exceeds controller receive limit",
                        )
                    )
                    return
                message, error_response = self._parse_client_line(line)
                if error_response is not None:
                    self.controller.record_malformed_client_message()
                    self.controller.observe_controller_event(
                        {
                            "type": MessageType.EVENT.value,
                            "source": "controller",
                            "event": "malformed_client_message",
                            "details": {"error_code": error_response["error"]["code"]},
                        }
                    )
                    await client.send_response(error_response)
                    continue
                if message is None:
                    continue
                if message["type"] == MessageType.ESTOP_RESET.value:
                    asyncio.create_task(self._reset_estop_and_reply(client, message))
                    continue
                if message["type"] != MessageType.COMMAND.value:
                    await client.send_response(
                        build_error_response(
                            seq=self._response_seq(message),
                            target=self._response_target(message),
                            code=ErrorCode.UNKNOWN_COMMAND,
                            message=f"unsupported local message type {message['type']}",
                        )
                    )
                    continue
                if message["target"] == "controller":
                    asyncio.create_task(self._controller_command_and_reply(client, message))
                    continue
                asyncio.create_task(self._route_and_reply(client, message))
        finally:
            self.clients.discard(client)
            self.controller.record_client_disconnect()
            await client.close()

    async def _route_and_reply(
        self,
        client: LocalClientConnection,
        command: dict[str, Any],
    ) -> None:
        response = await self.controller.route_command(command)
        delivered = await client.send_response(response)
        if not delivered:
            self.controller.record_orphaned_response()

    async def _controller_command_and_reply(
        self,
        client: LocalClientConnection,
        command: dict[str, Any],
    ) -> None:
        response = self.controller.route_controller_local_command(
            command,
            max_response_line_bytes=client.response_max_line_bytes,
        )
        delivered = await client.send_response(response)
        if not delivered:
            self.controller.record_orphaned_response()

    async def _reset_estop_and_reply(
        self,
        client: LocalClientConnection,
        reset_message: dict[str, Any],
    ) -> None:
        response = self.controller.reset_estop(
            reset_message,
            condition_cleared=self.estop_reset_condition_cleared(),
        )
        delivered = await client.send_response(response)
        if not delivered:
            self.controller.record_orphaned_response()

    def _increment_client_event_dropped(self) -> None:
        self.client_event_dropped += 1
        self.controller.record_local_event_dropped()

    def _increment_critical_event_disconnects(self) -> None:
        self.critical_event_disconnects += 1
        self.controller.record_critical_event_disconnect()

    def _is_critical_event(self, event: dict[str, Any]) -> bool:
        event_name = event.get("event")
        if not isinstance(event_name, str):
            return True
        return event_name.startswith("estop") or event_name in self._CRITICAL_EVENT_NAMES

    def _parse_client_line(
        self,
        line: bytes,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        message: dict[str, Any] | None = None
        try:
            message = parse_line(line, max_line_bytes=LOCAL_REQUEST_MAX_LINE_BYTES)
            validate_message(message)
        except ProtocolValidationError as exc:
            return None, build_error_response(
                seq=self._response_seq(message),
                target=self._response_target(message),
                code=exc.error.code,
                message=exc.error.message,
            )
        return message, None

    def _response_seq(self, message: dict[str, Any] | None) -> int:
        if message is None:
            return 0
        seq = message.get("seq", 0)
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            return 0
        return seq

    def _response_target(self, message: dict[str, Any] | None) -> str:
        if message is None:
            return "client"
        source = message.get("source", "client")
        if not isinstance(source, str):
            return "client"
        return source
