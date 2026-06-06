"""In-memory asyncio controller core for v1 command dispatch.

This module owns command routing, per-board FIFO/in-flight state, pending
resolution, timeouts, board-down handling, and e-stop dispatch. It intentionally
does not implement Unix sockets, TCP connections, Redis, GUI, or firmware.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import asyncio
from typing import Any

from interfaces import BoardWriterHandle, send_estop
from protocol import (
    ErrorCode,
    ProtocolValidationError,
    build_board_command,
    build_client_response,
    build_error_response,
    check_protocol_version,
    extract_blocked_by_estop,
    pop_pending,
    validate_message,
)
from state import (
    DEFAULT_COMMAND_FIFO_DEPTH,
    DEFAULT_COMMAND_TIMEOUT_S,
    DEFAULT_QUEUE_RESIDENCY_CAP_S,
    BoardConnState,
    BoardSeqCounter,
    BoardState,
    ControllerState,
    PendingCommand,
)


@dataclass
class ControllerCounters:
    unmatched_seq: int = 0
    board_busy_rejections: int = 0
    estop_rejections: int = 0
    stale_command_rejections: int = 0


@dataclass
class _BoardRuntime:
    state: BoardState
    writer: BoardWriterHandle | None = None
    seq_counter: BoardSeqCounter = field(default_factory=BoardSeqCounter)
    fifo: deque[PendingCommand] = field(default_factory=deque)
    blocked_by_estop: dict[str, bool] = field(default_factory=dict)


class ControllerCore:
    """Controller dispatcher core with fake/in-memory board transports."""

    def __init__(
        self,
        *,
        expected_boards: set[str],
        fifo_depth: int = DEFAULT_COMMAND_FIFO_DEPTH,
        default_execution_timeout_s: float = DEFAULT_COMMAND_TIMEOUT_S,
        default_queue_residency_cap_s: float = DEFAULT_QUEUE_RESIDENCY_CAP_S,
    ):
        self.state = ControllerState()
        self.counters = ControllerCounters()
        self.fifo_depth = fifo_depth
        self.default_execution_timeout_s = default_execution_timeout_s
        self.default_queue_residency_cap_s = default_queue_residency_cap_s
        self._boards = {
            board_id: _BoardRuntime(state=BoardState(board_id=board_id))
            for board_id in expected_boards
        }
        self.state.boards = {board_id: runtime.state for board_id, runtime in self._boards.items()}
        self._pending: dict[int, PendingCommand] = {}
        self._timeout_tasks: dict[int, asyncio.Task[None]] = {}

    def register_board(
        self,
        board_id: str,
        *,
        writer: BoardWriterHandle,
        schema: dict[str, Any],
    ) -> None:
        if board_id not in self._boards:
            raise ValueError(f"unknown board {board_id}")
        runtime = self._boards[board_id]
        try:
            check_protocol_version(schema)
        except ProtocolValidationError:
            runtime.state.conn_state = BoardConnState.FAULTED
            self.state.refresh_connected_count()
            raise
        runtime.writer = writer
        runtime.state.schema = schema
        runtime.blocked_by_estop = extract_blocked_by_estop(schema)
        runtime.state.conn_state = BoardConnState.REGISTERED
        runtime.state.queue_depth = len(runtime.fifo)
        self.state.refresh_connected_count()

    def set_board_state(self, board_id: str, conn_state: BoardConnState) -> None:
        runtime = self._boards[board_id]
        runtime.state.conn_state = conn_state
        self.state.refresh_connected_count()

    async def route_command(
        self,
        command: dict[str, Any],
        *,
        execution_timeout_s: float | None = None,
        queue_residency_cap_s: float | None = None,
    ) -> dict[str, Any]:
        validate_message(command)
        board_id = command["target"]
        client_seq = command["seq"]
        client_target = command["source"]

        runtime = self._boards.get(board_id)
        if runtime is None:
            return build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.UNKNOWN_TARGET,
                message=f"unknown board {board_id}",
            )
        if runtime.writer is None or runtime.state.conn_state is not BoardConnState.REGISTERED:
            return build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.BOARD_UNAVAILABLE,
                message=f"board {board_id} is unavailable",
            )

        reject = self._reject_for_schema_or_estop(runtime, command)
        if reject is not None:
            return build_error_response(
                seq=client_seq,
                target=client_target,
                code=reject,
                message=f"command {command['command']} rejected with {reject.value}",
            )

        if runtime.state.in_flight_board_seq is not None and len(runtime.fifo) >= self.fifo_depth:
            self.counters.board_busy_rejections += 1
            return build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.BOARD_BUSY,
                message=f"board {board_id} command FIFO is full",
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        pending = PendingCommand(
            board_id=board_id,
            board_seq=runtime.seq_counter.next(),
            client_seq=client_seq,
            client=client_target,
            future=future,
            command=command,
            enqueued_at=loop.time(),
            queue_residency_cap_s=(
                self.default_queue_residency_cap_s
                if queue_residency_cap_s is None
                else queue_residency_cap_s
            ),
            execution_timeout_s=(
                self.default_execution_timeout_s
                if execution_timeout_s is None
                else execution_timeout_s
            ),
        )
        runtime.fifo.append(pending)
        runtime.state.queue_depth = len(runtime.fifo)
        await self._dispatch_next(board_id)
        return await future

    async def handle_board_response(self, response: dict[str, Any]) -> dict[str, Any] | None:
        validate_message(response)
        board_seq = response["seq"]
        entry = pop_pending(self._pending, board_seq)
        if entry is None:
            self.counters.unmatched_seq += 1
            return None

        self._cancel_timeout(board_seq)
        runtime = self._boards[entry.board_id]
        if runtime.state.in_flight_board_seq == board_seq:
            runtime.state.in_flight_board_seq = None

        latency_ms = None
        if entry.written_at is not None:
            latency_ms = (asyncio.get_running_loop().time() - entry.written_at) * 1000
        client_response = build_client_response(
            response,
            client_seq=entry.client_seq,
            client_target=entry.client,
            board_seq=board_seq,
            latency_ms=latency_ms,
        )
        if not entry.future.done():
            entry.future.set_result(client_response)
        await self._dispatch_next(entry.board_id)
        return client_response

    async def board_down(self, board_id: str) -> None:
        runtime = self._boards[board_id]
        runtime.state.conn_state = BoardConnState.FAULTED
        runtime.writer = None
        self.state.refresh_connected_count()

        for board_seq, entry in list(self._pending.items()):
            if entry.board_id != board_id:
                continue
            popped = pop_pending(self._pending, board_seq)
            if popped is not None:
                self._cancel_timeout(board_seq)
                self._resolve_entry_error(
                    popped,
                    ErrorCode.BOARD_UNAVAILABLE,
                    f"board {board_id} went down",
                )

        while runtime.fifo:
            entry = runtime.fifo.popleft()
            self._resolve_entry_error(
                entry,
                ErrorCode.BOARD_UNAVAILABLE,
                f"board {board_id} went down",
            )
        runtime.state.queue_depth = 0
        runtime.state.in_flight_board_seq = None

    async def trigger_estop(self, *, origin_board: str | None = None) -> None:
        if self.state.system.estop_active:
            return
        self.state.system.latch_estop()
        for runtime in self._boards.values():
            while runtime.fifo:
                entry = runtime.fifo.popleft()
                self._resolve_entry_error(
                    entry,
                    ErrorCode.ESTOP_ACTIVE,
                    "queued command rejected: system is in e-stop",
                )
            runtime.state.queue_depth = 0

        await asyncio.gather(
            *(
                self.send_estop_to_board(board_id)
                for board_id, runtime in self._boards.items()
                if runtime.writer is not None and runtime.state.conn_state is BoardConnState.REGISTERED
            )
        )

    async def send_estop_to_board(self, board_id: str) -> None:
        runtime = self._boards[board_id]
        if runtime.writer is None:
            return
        runtime.state.mark_estop_sent()
        await send_estop(board_id, runtime.writer)

    def pending_count(self, board_id: str | None = None) -> int:
        if board_id is None:
            return len(self._pending)
        return sum(1 for entry in self._pending.values() if entry.board_id == board_id)

    def fifo_depth_for(self, board_id: str) -> int:
        return len(self._boards[board_id].fifo)

    def in_flight_for(self, board_id: str) -> int | None:
        return self._boards[board_id].state.in_flight_board_seq

    async def _dispatch_next(self, board_id: str) -> None:
        runtime = self._boards[board_id]
        if runtime.writer is None or runtime.state.conn_state is not BoardConnState.REGISTERED:
            return
        if runtime.state.in_flight_board_seq is not None:
            return

        loop = asyncio.get_running_loop()
        while runtime.fifo:
            entry = runtime.fifo.popleft()
            runtime.state.queue_depth = len(runtime.fifo)
            if entry.queue_residency_expired(loop.time()):
                self.counters.stale_command_rejections += 1
                self._resolve_entry_error(
                    entry,
                    ErrorCode.COMMAND_TIMEOUT,
                    "queued command exceeded residency cap",
                )
                continue

            board_command = build_board_command(
                entry.command,
                board_seq=entry.board_seq,
                board_id=board_id,
                controller_ts=loop.time(),
            )
            runtime.state.in_flight_board_seq = entry.board_seq
            entry.written_at = loop.time()
            self._pending[entry.board_seq] = entry
            await runtime.writer.write_message(board_command)
            self._timeout_tasks[entry.board_seq] = asyncio.create_task(
                self._execution_timeout(entry.board_seq, entry.execution_timeout_s)
            )
            return

    async def _execution_timeout(self, board_seq: int, timeout_s: float) -> None:
        await asyncio.sleep(timeout_s)
        entry = pop_pending(self._pending, board_seq)
        if entry is None:
            return

        runtime = self._boards[entry.board_id]
        if runtime.state.in_flight_board_seq == board_seq:
            runtime.state.in_flight_board_seq = None
        self._timeout_tasks.pop(board_seq, None)
        self._resolve_entry_error(
            entry,
            ErrorCode.COMMAND_TIMEOUT,
            "command timed out after board write",
        )
        await self._dispatch_next(entry.board_id)

    def _reject_for_schema_or_estop(
        self,
        runtime: _BoardRuntime,
        command: dict[str, Any],
    ) -> ErrorCode | None:
        blocked_by_estop = self._blocked_by_estop_for(runtime, command["command"])
        if blocked_by_estop is None:
            return ErrorCode.UNKNOWN_COMMAND
        if self.state.system.estop_active and blocked_by_estop:
            self.counters.estop_rejections += 1
            return ErrorCode.ESTOP_ACTIVE
        return None

    def _blocked_by_estop_for(self, runtime: _BoardRuntime, command_name: str) -> bool | None:
        return runtime.blocked_by_estop.get(command_name)

    def _resolve_entry_error(
        self,
        entry: PendingCommand,
        code: ErrorCode,
        message: str,
    ) -> None:
        if entry.future.done():
            return
        entry.future.set_result(
            build_error_response(
                seq=entry.client_seq,
                target=entry.client,
                code=code,
                message=message,
            )
        )

    def _cancel_timeout(self, board_seq: int) -> None:
        task = self._timeout_tasks.pop(board_seq, None)
        if task is not None:
            task.cancel()
