"""Asyncio TCP board connection layer for v1 controller-to-board transport.

Boards are TCP servers. The controller connects, receives a schema as the first
message on every connection, then routes newline-delimited JSON board messages
into the existing in-memory controller core. This module does not implement a
Unix socket server, Redis, GUI integration, firmware, or webapp behavior.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from controller import ControllerCore
from interfaces import BoardWriterHandle
from protocol import (
    BOARD_MAX_LINE_BYTES,
    CONTROLLER_MAX_LINE_BYTES,
    MessageType,
    ProtocolValidationError,
    is_estop_ack_event,
    parse_message,
    serialize_message,
)
from state import BoardConnState


@dataclass(frozen=True)
class BoardEndpoint:
    board_id: str
    host: str
    port: int


class StreamBoardWriter(BoardWriterHandle):
    """Serialized newline-JSON writer for one board stream."""

    def __init__(self, board_id: str, writer: asyncio.StreamWriter):
        self.board_id = board_id
        self._writer = writer
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    async def write_message(self, message: dict[str, Any]) -> None:
        encoded = serialize_message(message, max_line_bytes=BOARD_MAX_LINE_BYTES)
        async with self._lock:
            self._writer.write(encoded)
            await self._writer.drain()

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()


class BoardTCPConnection:
    """Persistent connection manager for one configured board."""

    def __init__(
        self,
        endpoint: BoardEndpoint,
        controller: ControllerCore,
        *,
        reconnect_delay_s: float = 0.05,
        registration_timeout_s: float = 2.0,
    ):
        self.endpoint = endpoint
        self.controller = controller
        self.reconnect_delay_s = reconnect_delay_s
        self.registration_timeout_s = registration_timeout_s
        self._stop = asyncio.Event()
        self._registered = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stream_writer: StreamBoardWriter | None = None

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop.set()
        if self._stream_writer is not None:
            await self._stream_writer.close()
        if self._task is not None:
            await self._task

    async def wait_registered(self, timeout: float = 1.0) -> None:
        await asyncio.wait_for(self._registered.wait(), timeout=timeout)

    async def run(self) -> None:
        while not self._stop.is_set():
            await self._connect_and_read_once()
            if not self._stop.is_set():
                await asyncio.sleep(self.reconnect_delay_s)

    async def _connect_and_read_once(self) -> None:
        self._registered.clear()
        self.controller.set_board_state(self.endpoint.board_id, BoardConnState.CONNECTING)
        reader: asyncio.StreamReader | None = None
        raw_writer: asyncio.StreamWriter | None = None
        try:
            reader, raw_writer = await asyncio.open_connection(
                self.endpoint.host,
                self.endpoint.port,
                limit=CONTROLLER_MAX_LINE_BYTES,
            )
            self.controller.set_board_state(self.endpoint.board_id, BoardConnState.CONNECTED)
            stream_writer = StreamBoardWriter(self.endpoint.board_id, raw_writer)
            self._stream_writer = stream_writer

            schema = await asyncio.wait_for(
                self._read_valid_message(reader),
                timeout=self.registration_timeout_s,
            )
            if schema is None or schema.get("type") != MessageType.SCHEMA.value:
                self.controller.set_board_state(self.endpoint.board_id, BoardConnState.FAULTED)
                return
            if schema.get("source") != self.endpoint.board_id:
                self.controller.set_board_state(self.endpoint.board_id, BoardConnState.FAULTED)
                return

            try:
                self.controller.register_board(
                    self.endpoint.board_id,
                    writer=stream_writer,
                    schema=schema,
                )
            except (ProtocolValidationError, ValueError):
                return
            self._registered.set()
            if self.controller.state.system.estop_active:
                await self.controller.send_estop_to_board(self.endpoint.board_id)

            await self._read_loop(reader)
        except (TimeoutError, ConnectionError, OSError):
            # Future metrics hook: distinguish registration_timeouts from connect/read failures.
            self.controller.set_board_state(self.endpoint.board_id, BoardConnState.FAULTED)
        finally:
            self._registered.clear()
            self._stream_writer = None
            if raw_writer is not None:
                raw_writer.close()
                try:
                    await raw_writer.wait_closed()
                except ConnectionError:
                    pass
            if not self._stop.is_set():
                await self.controller.board_down(self.endpoint.board_id)

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        while not self._stop.is_set():
            message = await self._read_valid_message(reader)
            if message is None:
                return
            await self._handle_message(message)

    async def _read_valid_message(self, reader: asyncio.StreamReader) -> dict[str, Any] | None:
        while not self._stop.is_set():
            try:
                line = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError:
                return None
            except asyncio.LimitOverrunError:
                self.controller.record_malformed_board_message()
                return None
            parsed = parse_message(line, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)
            if parsed.ok:
                return parsed.message
            self.controller.record_malformed_board_message()
            self.controller.observe_controller_event(
                {
                    "type": MessageType.EVENT.value,
                    "source": "controller",
                    "event": "malformed_board_message",
                    "details": {
                        "board_id": self.endpoint.board_id,
                        "error_code": parsed.error.code.value if parsed.error is not None else None,
                    },
                }
            )
        return None

    async def _handle_message(self, message: dict[str, Any]) -> None:
        message_type = message["type"]
        if message_type == MessageType.RESPONSE.value:
            await self.controller.handle_board_response(message)
        elif message_type == MessageType.TELEMETRY.value:
            self.controller.record_board_telemetry(message)
        elif message_type == MessageType.EVENT.value:
            self.controller.observe_controller_event(message)
            await self._handle_event(message)
        elif message_type == MessageType.SCHEMA.value:
            # A reconnect schema is handled by establishing a fresh connection.
            return

    async def _handle_event(self, message: dict[str, Any]) -> None:
        board = self.controller.state.boards[self.endpoint.board_id]
        if is_estop_ack_event(message):
            board.mark_estop_ack()
            self.controller.observe_board_state_snapshot(self.endpoint.board_id)
        elif message.get("event") == "estop_triggered":
            await self.controller.trigger_estop(origin_board=message.get("source"))
