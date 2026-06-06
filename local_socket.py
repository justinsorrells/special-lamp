"""Asyncio Unix socket server for local v1 clients.

Local GUI/client processes connect over a Unix domain socket and exchange
newline-delimited JSON with the controller. This module only implements the
local client socket boundary; it does not implement Redis, GUI/webapp code,
firmware, or any direct board access.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
import os
from typing import Any

from controller import ControllerCore
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


@dataclass(eq=False)
class LocalClientConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    outbound_maxsize: int
    connected: bool = True
    outbound: asyncio.Queue[dict[str, Any] | None] = field(init=False)
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

    async def close(self) -> None:
        self.connected = False
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

    async def writer_loop(self) -> None:
        while True:
            message = await self.outbound.get()
            if message is None:
                return
            try:
                self.writer.write(serialize_message(message, max_line_bytes=CONTROLLER_MAX_LINE_BYTES))
                await self.writer.drain()
            except (ConnectionError, OSError, ProtocolValidationError):
                self.connected = False
                return

    async def _send(self, message: dict[str, Any], *, critical: bool) -> bool:
        if not self.is_connected:
            return False
        try:
            self.outbound.put_nowait(message)
            return True
        except asyncio.QueueFull:
            if not critical:
                return False
            await self.close()
            return False

    async def _put_sentinel(self) -> None:
        try:
            self.outbound.put_nowait(None)
        except asyncio.QueueFull:
            # Make room for shutdown without blocking on a saturated client queue.
            _ = self.outbound.get_nowait()
            self.outbound.put_nowait(None)


class LocalUnixSocketServer:
    """Full-duplex Unix socket server for local controller clients."""

    def __init__(
        self,
        *,
        socket_path: str,
        controller: ControllerCore,
        outbound_queue_size: int = 1000,
    ):
        self.socket_path = socket_path
        self.controller = controller
        self.outbound_queue_size = outbound_queue_size
        self.server: asyncio.AbstractServer | None = None
        self.clients: set[LocalClientConnection] = set()

    async def start(self) -> None:
        await self._prepare_socket_path()
        self.server = await asyncio.start_unix_server(
            self._handle_client,
            path=self.socket_path,
            limit=CONTROLLER_MAX_LINE_BYTES,
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

    async def broadcast_event(self, event: dict[str, Any], *, critical: bool = True) -> None:
        validate_message(event)
        if event["type"] != MessageType.EVENT.value:
            raise ProtocolValidationError(ErrorCode.INVALID_TYPE, "broadcast message must be an event")
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
                    await client.send_response(error_response)
                    continue
                if message is None:
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
                asyncio.create_task(self._route_and_reply(client, message))
        finally:
            self.clients.discard(client)
            await client.close()

    async def _route_and_reply(
        self,
        client: LocalClientConnection,
        command: dict[str, Any],
    ) -> None:
        response = await self.controller.route_command(command)
        await client.send_response(response)

    def _parse_client_line(
        self,
        line: bytes,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        message: dict[str, Any] | None = None
        try:
            message = parse_line(line, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)
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
