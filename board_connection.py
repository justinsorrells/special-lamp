"""Asyncio TCP board connection layer for v1 controller-to-board transport.

Boards are TCP servers. The controller connects, receives a schema as the first
message on every connection, then routes newline-delimited JSON board messages
into the existing in-memory controller core. This module does not implement a
Unix socket server, Redis, GUI integration, firmware, or webapp behavior.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from controller import ControllerCore
from interfaces import BoardWriterHandle
from protocol import (
    BOARD_MAX_LINE_BYTES,
    CONTROLLER_MAX_LINE_BYTES,
    MessageType,
    ProtocolValidationError,
    build_heartbeat_message,
    is_estop_ack_event,
    parse_message,
    serialize_message,
)
from state import BoardConnState

DEFAULT_RECONNECT_BACKOFF_BASE_S = 0.5
DEFAULT_RECONNECT_BACKOFF_FACTOR = 2.0
DEFAULT_RECONNECT_BACKOFF_CAP_S = 5.0


@dataclass(frozen=True)
class BoardEndpoint:
    board_id: str
    host: str
    port: int


@dataclass
class ReconnectBackoff:
    """Exponential reconnect backoff with full jitter."""

    base_delay_s: float = DEFAULT_RECONNECT_BACKOFF_BASE_S
    factor: float = DEFAULT_RECONNECT_BACKOFF_FACTOR
    cap_delay_s: float = DEFAULT_RECONNECT_BACKOFF_CAP_S
    random_fraction: Callable[[], float] = random.random
    _current_ceiling_s: float = field(init=False)

    def __post_init__(self) -> None:
        if self.base_delay_s <= 0:
            raise ValueError("reconnect backoff base delay must be positive")
        if self.factor <= 1:
            raise ValueError("reconnect backoff factor must be greater than 1")
        if self.cap_delay_s < self.base_delay_s:
            raise ValueError("reconnect backoff cap must be greater than or equal to base delay")
        self._current_ceiling_s = self.base_delay_s

    @property
    def current_ceiling_s(self) -> float:
        return self._current_ceiling_s

    def next_delay_s(self) -> float:
        fraction = self.random_fraction()
        if fraction < 0 or fraction > 1:
            raise ValueError("reconnect jitter random_fraction must return a value from 0 to 1")

        delay_s = self._current_ceiling_s * fraction
        self._current_ceiling_s = min(self.cap_delay_s, self._current_ceiling_s * self.factor)
        return delay_s

    def reset(self) -> None:
        self._current_ceiling_s = self.base_delay_s


@dataclass(frozen=True)
class HeartbeatConfig:
    enabled: bool = False
    interval_s: float = 5.0
    ack_timeout_s: float = 1.0
    suspect_after_misses: int = 3

    def __post_init__(self) -> None:
        if self.interval_s <= 0:
            raise ValueError("heartbeat interval must be positive")
        if self.ack_timeout_s <= 0:
            raise ValueError("heartbeat ack timeout must be positive")
        if self.suspect_after_misses <= 0:
            raise ValueError("heartbeat suspect threshold must be positive")


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
        reconnect_delay_s: float | None = None,
        reconnect_backoff: ReconnectBackoff | None = None,
        registration_timeout_s: float = 2.0,
        heartbeat: HeartbeatConfig | None = None,
    ):
        self.endpoint = endpoint
        self.controller = controller
        if reconnect_backoff is not None and reconnect_delay_s is not None:
            raise ValueError("pass either reconnect_delay_s or reconnect_backoff, not both")
        self.reconnect_backoff = (
            reconnect_backoff
            if reconnect_backoff is not None
            else ReconnectBackoff(
                base_delay_s=(
                    DEFAULT_RECONNECT_BACKOFF_BASE_S if reconnect_delay_s is None else reconnect_delay_s
                )
            )
        )
        self.registration_timeout_s = registration_timeout_s
        self.heartbeat = HeartbeatConfig() if heartbeat is None else heartbeat
        self._stop = asyncio.Event()
        self._registered = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._heartbeat_ack = asyncio.Event()
        self._heartbeat_seq = 0
        self._pending_heartbeat_seq: int | None = None
        self._stream_writer: StreamBoardWriter | None = None

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop.set()
        await self._stop_heartbeat()
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
                await self._wait_before_reconnect()

    async def _wait_before_reconnect(self) -> None:
        delay_s = self.reconnect_backoff.next_delay_s()
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay_s)
        except TimeoutError:
            pass

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
            self.reconnect_backoff.reset()
            self._registered.set()
            if self.controller.state.system.estop_active:
                await self.controller.send_estop_to_board(self.endpoint.board_id)
            self._start_heartbeat(stream_writer)

            await self._read_loop(reader)
        except (TimeoutError, ConnectionError, OSError):
            # Future metrics hook: distinguish registration_timeouts from connect/read failures.
            self.controller.set_board_state(self.endpoint.board_id, BoardConnState.FAULTED)
        finally:
            await self._stop_heartbeat()
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
        elif message_type == MessageType.HEARTBEAT.value:
            self._handle_heartbeat_ack(message)

    async def _handle_event(self, message: dict[str, Any]) -> None:
        board = self.controller.state.boards[self.endpoint.board_id]
        if is_estop_ack_event(message):
            board.mark_estop_ack()
            self.controller.observe_board_state_snapshot(self.endpoint.board_id)
        elif message.get("event") == "estop_triggered":
            await self.controller.trigger_estop(origin_board=message.get("source"))

    def _start_heartbeat(self, writer: StreamBoardWriter) -> None:
        if not self.heartbeat.enabled:
            self.controller.mark_heartbeat_disabled(self.endpoint.board_id)
            return
        if self._heartbeat_task is not None:
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(writer))

    async def _stop_heartbeat(self) -> None:
        task = self._heartbeat_task
        self._heartbeat_task = None
        self._pending_heartbeat_seq = None
        self._heartbeat_ack.set()
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _heartbeat_loop(self, writer: StreamBoardWriter) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.heartbeat.interval_s)
                if self._stop.is_set():
                    return
                await self._send_one_heartbeat(writer)
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError, ProtocolValidationError):
            return

    async def _send_one_heartbeat(self, writer: StreamBoardWriter) -> None:
        self._heartbeat_seq += 1
        seq = self._heartbeat_seq
        self._pending_heartbeat_seq = seq
        self._heartbeat_ack.clear()
        sent_at = asyncio.get_running_loop().time()
        await writer.write_message(
            build_heartbeat_message(
                seq=seq,
                target=self.endpoint.board_id,
            )
        )
        self.controller.record_heartbeat_sent(self.endpoint.board_id, seq=seq, sent_at=sent_at)
        try:
            await asyncio.wait_for(self._heartbeat_ack.wait(), timeout=self.heartbeat.ack_timeout_s)
        except TimeoutError:
            if self._pending_heartbeat_seq == seq:
                self._pending_heartbeat_seq = None
                self.controller.record_heartbeat_missed(
                    self.endpoint.board_id,
                    seq=seq,
                    suspect_after_misses=self.heartbeat.suspect_after_misses,
                )

    def _handle_heartbeat_ack(self, message: dict[str, Any]) -> None:
        seq = message.get("seq")
        if (
            not isinstance(seq, int)
            or isinstance(seq, bool)
            or message.get("source") != self.endpoint.board_id
            or message.get("target") != "controller"
        ):
            self.controller.record_malformed_heartbeat_ack(self.endpoint.board_id, seq=seq)
            return

        if seq != self._pending_heartbeat_seq:
            self.controller.record_late_heartbeat_ack(self.endpoint.board_id, seq=seq)
            return

        self._pending_heartbeat_seq = None
        self.controller.record_heartbeat_ack(
            self.endpoint.board_id,
            seq=seq,
            ack_at=asyncio.get_running_loop().time(),
        )
        self._heartbeat_ack.set()
