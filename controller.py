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
    MessageType,
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
        observability: Any | None = None,
    ):
        self.state = ControllerState()
        self.counters = ControllerCounters()
        self.observability = observability
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
        self._observe_board_state(runtime.state)
        self._observe_system_state()

    def set_board_state(self, board_id: str, conn_state: BoardConnState) -> None:
        runtime = self._boards[board_id]
        runtime.state.conn_state = conn_state
        self.state.refresh_connected_count()
        self._observe_board_state(runtime.state)
        self._observe_system_state()

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
        self._observe_command_lifecycle_phase(command, board_id=board_id, phase="received")

        runtime = self._boards.get(board_id)
        if runtime is None:
            response = build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.UNKNOWN_TARGET,
                message=f"unknown board {board_id}",
            )
            self._observe_command_lifecycle(
                command,
                response,
                board_id=board_id,
                phase="rejected_unknown_target",
            )
            return response
        if runtime.writer is None or runtime.state.conn_state is not BoardConnState.REGISTERED:
            response = build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.BOARD_UNAVAILABLE,
                message=f"board {board_id} is unavailable",
            )
            self._observe_command_lifecycle(
                command,
                response,
                board_id=board_id,
                phase="board_unavailable",
            )
            return response

        reject = self._reject_for_schema_or_estop(runtime, command)
        if reject is not None:
            response = build_error_response(
                seq=client_seq,
                target=client_target,
                code=reject,
                message=f"command {command['command']} rejected with {reject.value}",
            )
            phase = "estop_rejected" if reject is ErrorCode.ESTOP_ACTIVE else "rejected"
            self._observe_command_lifecycle(command, response, board_id=board_id, phase=phase)
            return response

        if runtime.state.in_flight_board_seq is not None and len(runtime.fifo) >= self.fifo_depth:
            self.counters.board_busy_rejections += 1
            response = build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.BOARD_BUSY,
                message=f"board {board_id} command FIFO is full",
            )
            self._observe_command_lifecycle(
                command,
                response,
                board_id=board_id,
                phase="board_busy",
            )
            return response

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
        self._observe_command_lifecycle_phase(
            command,
            board_id=board_id,
            board_seq=pending.board_seq,
            phase="routed",
        )
        runtime.fifo.append(pending)
        runtime.state.queue_depth = len(runtime.fifo)
        self._observe_command_lifecycle_phase(
            command,
            board_id=board_id,
            board_seq=pending.board_seq,
            phase="queued",
        )
        self._observe_board_state(runtime.state)
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
            self._observe_board_state(runtime.state)

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
        self._observe_command_lifecycle(
            entry.command,
            client_response,
            board_id=entry.board_id,
            board_seq=board_seq,
            phase="resolved",
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
        self._observe_board_state(runtime.state)
        self._observe_system_state()
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "board_disconnected",
                "details": {"board_id": board_id},
            }
        )

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
                    phase="board_unavailable",
                )

        while runtime.fifo:
            entry = runtime.fifo.popleft()
            self._resolve_entry_error(
                entry,
                ErrorCode.BOARD_UNAVAILABLE,
                f"board {board_id} went down",
                phase="board_unavailable",
            )
        runtime.state.queue_depth = 0
        runtime.state.in_flight_board_seq = None
        self._observe_board_state(runtime.state)

    async def trigger_estop(self, *, origin_board: str | None = None) -> None:
        if self.state.system.estop_active:
            return
        self.state.system.latch_estop()
        self._observe_system_state()
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "estop_triggered",
                "details": {"origin_board": origin_board},
            }
        )
        for runtime in self._boards.values():
            while runtime.fifo:
                entry = runtime.fifo.popleft()
                self._resolve_entry_error(
                    entry,
                    ErrorCode.ESTOP_ACTIVE,
                    "queued command rejected: system is in e-stop",
                    phase="estop_rejected",
                )
            runtime.state.queue_depth = 0
            self._observe_board_state(runtime.state)

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
        self._observe_board_state(runtime.state)
        await send_estop(board_id, runtime.writer)

    def reset_estop(
        self,
        reset_message: dict[str, Any],
        *,
        condition_cleared: bool = True,
    ) -> dict[str, Any]:
        validate_message(reset_message)
        if reset_message["type"] != MessageType.ESTOP_RESET.value:
            return build_error_response(
                seq=self._message_seq(reset_message),
                target=self._message_source(reset_message),
                code=ErrorCode.INVALID_TYPE,
                message="expected estop_reset message",
            )
        if reset_message["target"] != "controller":
            return build_error_response(
                seq=reset_message["seq"],
                target=reset_message["source"],
                code=ErrorCode.UNKNOWN_TARGET,
                message=f"unknown controller target {reset_message['target']}",
            )
        if not condition_cleared:
            return build_error_response(
                seq=reset_message["seq"],
                target=reset_message["source"],
                code=ErrorCode.ESTOP_ACTIVE,
                message="e-stop condition is still active",
            )
        self.state.system.operator_reset_estop()
        self._observe_system_state()
        response = {
            "type": MessageType.RESPONSE.value,
            "seq": reset_message["seq"],
            "source": "controller",
            "target": reset_message["source"],
            "status": "ok",
            "result": {"estop_active": False},
            "error": None,
        }
        validate_message(response)
        return response

    def pending_count(self, board_id: str | None = None) -> int:
        if board_id is None:
            return len(self._pending)
        return sum(1 for entry in self._pending.values() if entry.board_id == board_id)

    def fifo_depth_for(self, board_id: str) -> int:
        return len(self._boards[board_id].fifo)

    def in_flight_for(self, board_id: str) -> int | None:
        return self._boards[board_id].state.in_flight_board_seq

    def _message_seq(self, message: dict[str, Any]) -> int:
        seq = message.get("seq", 0)
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            return 0
        return seq

    def _message_source(self, message: dict[str, Any]) -> str:
        source = message.get("source", "client")
        if not isinstance(source, str):
            return "client"
        return source

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
            self._observe_board_state(runtime.state)
            if entry.queue_residency_expired(loop.time()):
                self.counters.stale_command_rejections += 1
                self._resolve_entry_error(
                    entry,
                    ErrorCode.COMMAND_TIMEOUT,
                    "queued command exceeded residency cap",
                    phase="queue_timeout",
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
            self._observe_board_state(runtime.state)
            self._observe_command_lifecycle_phase(
                entry.command,
                board_id=board_id,
                board_seq=entry.board_seq,
                phase="sent_to_board",
                controller_ts=entry.written_at,
            )
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
            self._observe_board_state(runtime.state)
        self._timeout_tasks.pop(board_seq, None)
        self._resolve_entry_error(
            entry,
            ErrorCode.COMMAND_TIMEOUT,
            "command timed out after board write",
            phase="timeout",
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
        *,
        phase: str = "resolved",
    ) -> None:
        if entry.future.done():
            return
        response = build_error_response(
            seq=entry.client_seq,
            target=entry.client,
            code=code,
            message=message,
        )
        self._observe_command_lifecycle(
            entry.command,
            response,
            board_id=entry.board_id,
            board_seq=entry.board_seq,
            phase=phase,
        )
        entry.future.set_result(response)

    def _cancel_timeout(self, board_seq: int) -> None:
        task = self._timeout_tasks.pop(board_seq, None)
        if task is not None:
            task.cancel()

    def observe_board_telemetry(self, message: dict[str, Any]) -> None:
        if self.observability is None:
            return
        try:
            self.observability.enqueue_board_telemetry(message)
        except Exception:
            return

    def observe_board_state_snapshot(self, board_id: str) -> None:
        self._observe_board_state(self._boards[board_id].state)

    def observe_controller_event(self, event: dict[str, Any]) -> None:
        if self.observability is None:
            return
        try:
            self.observability.enqueue_controller_event(event)
        except Exception:
            return

    def _observe_board_state(self, state: BoardState) -> None:
        if self.observability is None:
            return
        try:
            self.observability.enqueue_board_state(state)
        except Exception:
            return

    def _observe_system_state(self) -> None:
        if self.observability is None:
            return
        try:
            self.observability.enqueue_system_state(self.state.system)
        except Exception:
            return

    def _observe_command_lifecycle(
        self,
        command: dict[str, Any],
        response: dict[str, Any],
        *,
        board_id: str,
        phase: str,
        board_seq: int | None = None,
    ) -> None:
        if self.observability is None:
            return
        error = response.get("error")
        error_code = error.get("code") if isinstance(error, dict) else None
        command_id = f"{board_id}:{board_seq}" if board_seq is not None else f"{board_id}:client:{command['seq']}"
        try:
            self.observability.enqueue_command_lifecycle(
                command_id=command_id,
                seq=command["seq"],
                board_id=board_id,
                phase=phase,
                status=response["status"],
                board_seq=board_seq,
                error_code=error_code,
                command=command.get("command"),
            )
        except Exception:
            return

    def _observe_command_lifecycle_phase(
        self,
        command: dict[str, Any],
        *,
        board_id: str,
        phase: str,
        board_seq: int | None = None,
        controller_ts: float | None = None,
    ) -> None:
        if self.observability is None:
            return
        command_id = f"{board_id}:{board_seq}" if board_seq is not None else f"{board_id}:client:{command['seq']}"
        try:
            self.observability.enqueue_command_lifecycle(
                command_id=command_id,
                seq=command["seq"],
                board_id=board_id,
                phase=phase,
                status=None,
                board_seq=board_seq,
                error_code=None,
                command=command.get("command"),
                controller_ts=controller_ts,
            )
        except Exception:
            return
