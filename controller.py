"""In-memory asyncio controller core for v1 command dispatch.

This module owns command routing, per-board FIFO/in-flight state, pending
resolution, timeouts, board-down handling, and e-stop dispatch. It intentionally
does not implement Unix sockets, TCP connections, Redis, GUI, or firmware.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, fields
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
    CommandLatencyObservation,
    ControllerState,
    PendingCommand,
)

DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_S = 2.0
DEFAULT_SHUTDOWN_CLOSE_TIMEOUT_S = 0.2


@dataclass
class ControllerCounters:
    obs_dropped: int = 0
    unmatched_seq: int = 0
    orphaned_response: int = 0
    board_busy_rejections: int = 0
    estop_rejections: int = 0
    malformed_client_messages: int = 0
    malformed_board_messages: int = 0
    client_disconnects: int = 0
    board_disconnects: int = 0
    command_timeouts: int = 0
    controller_shutdown_failures: int = 0
    redis_write_failures: int = 0
    local_event_dropped: int = 0
    critical_event_disconnects: int = 0
    late_board_responses: int = 0
    duplicate_board_responses: int = 0
    commands_completed_ok: int = 0
    commands_completed_error: int = 0
    commands_completed_timeout: int = 0
    stale_command_rejections: int = 0
    heartbeat_acks_missed: int = 0
    malformed_heartbeat_acks: int = 0
    late_heartbeat_acks: int = 0
    telemetry_liveness_timeouts: int = 0

    def increment(self, name: str, amount: int = 1) -> None:
        if name not in self._counter_names():
            raise KeyError(f"unknown controller counter {name}")
        current = getattr(self, name)
        setattr(self, name, current + amount)

    def record_terminal_response(self, response: dict[str, Any]) -> None:
        status = response["status"]
        if status == "ok":
            self.commands_completed_ok += 1
            return
        if status == "error":
            self.commands_completed_error += 1
            return
        if status == "timeout":
            self.commands_completed_timeout += 1
            error = response.get("error")
            if isinstance(error, dict) and error.get("code") == ErrorCode.COMMAND_TIMEOUT.value:
                self.command_timeouts += 1
            return

    def snapshot(self) -> dict[str, int]:
        return {field.name: getattr(self, field.name) for field in fields(self)}

    @classmethod
    def _counter_names(cls) -> frozenset[str]:
        return frozenset(field.name for field in fields(cls))


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
        monotonic_clock: Callable[[], float] | None = None,
    ):
        self.state = ControllerState()
        self.counters = ControllerCounters()
        self.observability = observability
        self.fifo_depth = fifo_depth
        self.default_execution_timeout_s = default_execution_timeout_s
        self.default_queue_residency_cap_s = default_queue_residency_cap_s
        self._monotonic_clock = monotonic_clock
        self._boards = {
            board_id: _BoardRuntime(state=BoardState(board_id=board_id))
            for board_id in expected_boards
        }
        self.state.boards = {board_id: runtime.state for board_id, runtime in self._boards.items()}
        self._pending: dict[int, PendingCommand] = {}
        self._timeout_tasks: dict[int, asyncio.Task[None]] = {}
        self._resolved_board_seqs: dict[int, str] = {}
        self._resolved_board_seq_order: deque[int] = deque(maxlen=4096)
        self._shutdown_task: asyncio.Task[None] | None = None
        self._shutting_down = False

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
        if self._shutting_down:
            response = build_error_response(
                seq=client_seq,
                target=client_target,
                code=ErrorCode.CONTROLLER_SHUTDOWN,
                message="controller is shutting down",
            )
            self._record_terminal_response(response)
            return response

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
            self._record_terminal_response(response)
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
            self._record_terminal_response(response)
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
            self._record_terminal_response(response)
            return response

        if runtime.state.in_flight_board_seq is not None and len(runtime.fifo) >= self.fifo_depth:
            self.counters.increment("board_busy_rejections")
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
            self._record_terminal_response(response)
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
            enqueued_at=self._monotonic_time(),
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
            self.counters.increment("unmatched_seq")
            resolved_status = self._resolved_board_seqs.get(board_seq)
            if resolved_status == "timeout":
                self.counters.increment("late_board_responses")
            elif resolved_status is not None:
                self.counters.increment("duplicate_board_responses")
            return None

        self._cancel_timeout(board_seq)
        runtime = self._boards[entry.board_id]
        if runtime.state.in_flight_board_seq == board_seq:
            runtime.state.in_flight_board_seq = None
            self._observe_board_state(runtime.state)

        observed_at = self._monotonic_time()
        board_proc_us = self._extract_board_proc_us(response)
        latency_ms = None
        written_at = entry.written_at
        if written_at is not None:
            latency_ms = (observed_at - written_at) * 1000
            runtime.state.last_command_latency = CommandLatencyObservation(
                board_seq=board_seq,
                latency_ms=latency_ms,
                controller_ts=written_at,
                observed_at=observed_at,
                board_proc_us=board_proc_us,
            )
            self._observe_board_state(runtime.state)
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
            latency_ms=latency_ms,
            board_proc_us=board_proc_us,
        )
        self._remember_terminal_board_seq(board_seq, client_response["status"])
        self._record_terminal_response(client_response)
        if not entry.future.done():
            entry.future.set_result(client_response)
        await self._dispatch_next(entry.board_id)
        return client_response

    async def board_down(self, board_id: str) -> None:
        self.counters.increment("board_disconnects")
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

    async def shutdown(
        self,
        *,
        drain_timeout_s: float = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_S,
        close_timeout_s: float = DEFAULT_SHUTDOWN_CLOSE_TIMEOUT_S,
    ) -> None:
        current_task = asyncio.current_task()
        if self._shutdown_task is not None:
            if self._shutdown_task is not current_task:
                await self._shutdown_task
            return

        self._shutdown_task = asyncio.create_task(
            self._run_shutdown(
                drain_timeout_s=drain_timeout_s,
                close_timeout_s=close_timeout_s,
            )
        )
        await self._shutdown_task

    async def _run_shutdown(
        self,
        *,
        drain_timeout_s: float,
        close_timeout_s: float,
    ) -> None:
        self._shutting_down = True
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "controller_shutdown_started",
                "details": {"pending_count": len(self._pending)},
            }
        )
        await self._drain_pending_commands(drain_timeout_s)
        self._fail_remaining_for_shutdown()
        await self._close_board_writers(close_timeout_s)
        await self._stop_observability(close_timeout_s)
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "controller_shutdown_complete",
                "details": {"pending_count": len(self._pending)},
            }
        )

    async def _drain_pending_commands(self, drain_timeout_s: float) -> None:
        if drain_timeout_s <= 0 or not self._pending:
            return
        pending_futures = [entry.future for entry in self._pending.values() if not entry.future.done()]
        if not pending_futures:
            return
        await asyncio.wait(pending_futures, timeout=drain_timeout_s)

    def _fail_remaining_for_shutdown(self) -> None:
        for board_seq in list(self._pending):
            popped = pop_pending(self._pending, board_seq)
            if popped is None:
                continue
            self._cancel_timeout(board_seq)
            self._resolve_entry_error(
                popped,
                ErrorCode.CONTROLLER_SHUTDOWN,
                "controller shutdown before command resolved",
                phase="controller_shutdown",
            )

        for runtime in self._boards.values():
            while runtime.fifo:
                entry = runtime.fifo.popleft()
                self._resolve_entry_error(
                    entry,
                    ErrorCode.CONTROLLER_SHUTDOWN,
                    "controller shutdown before command dispatched",
                    phase="controller_shutdown",
                )
            runtime.state.queue_depth = 0
            runtime.state.in_flight_board_seq = None
            runtime.state.conn_state = BoardConnState.DISCONNECTED
            self._observe_board_state(runtime.state)
        self.state.refresh_connected_count()
        self._observe_system_state()

    async def _close_board_writers(self, close_timeout_s: float) -> None:
        await asyncio.gather(
            *(
                self._close_board_writer(runtime.writer, close_timeout_s)
                for runtime in self._boards.values()
                if runtime.writer is not None
            ),
            return_exceptions=True,
        )
        for runtime in self._boards.values():
            runtime.writer = None

    async def _close_board_writer(
        self,
        writer: BoardWriterHandle,
        close_timeout_s: float,
    ) -> None:
        try:
            close = getattr(writer, "close", None)
            if close is None:
                return
            result = close()
            if result is None:
                return
            await asyncio.wait_for(result, timeout=close_timeout_s)
        except Exception:
            self.counters.increment("controller_shutdown_failures")

    async def _stop_observability(self, close_timeout_s: float) -> None:
        try:
            stop = getattr(self.observability, "stop", None)
            if stop is None:
                return
            result = stop()
            if result is None:
                return
            await asyncio.wait_for(result, timeout=close_timeout_s)
        except Exception:
            self.counters.increment("controller_shutdown_failures")

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

    def metrics_snapshot(self) -> dict[str, int]:
        snapshot = self.counters.snapshot()
        obs_counters = getattr(self.observability, "counters", None)
        if obs_counters is not None:
            snapshot["obs_dropped"] = getattr(obs_counters, "obs_dropped", snapshot["obs_dropped"])
            snapshot["redis_write_failures"] = getattr(
                obs_counters,
                "redis_write_failures",
                snapshot["redis_write_failures"],
            )
        return dict(snapshot)

    def record_orphaned_response(self) -> None:
        self.counters.increment("orphaned_response")

    def record_malformed_client_message(self) -> None:
        self.counters.increment("malformed_client_messages")

    def record_malformed_board_message(self) -> None:
        self.counters.increment("malformed_board_messages")

    def record_client_disconnect(self) -> None:
        self.counters.increment("client_disconnects")

    def record_local_event_dropped(self) -> None:
        self.counters.increment("local_event_dropped")

    def record_critical_event_disconnect(self) -> None:
        self.counters.increment("critical_event_disconnects")

    def record_heartbeat_sent(self, board_id: str, *, seq: int, sent_at: float) -> None:
        runtime = self._boards[board_id]
        runtime.state.mark_heartbeat_sent(sent_at)
        self._observe_board_state(runtime.state)
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "heartbeat_sent",
                "details": {"board_id": board_id, "seq": seq},
            }
        )

    def record_heartbeat_ack(self, board_id: str, *, seq: int, ack_at: float) -> None:
        runtime = self._boards[board_id]
        runtime.state.mark_heartbeat_ack(ack_at)
        self._observe_board_state(runtime.state)
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "heartbeat_ack",
                "details": {"board_id": board_id, "seq": seq},
            }
        )

    def record_heartbeat_missed(
        self,
        board_id: str,
        *,
        seq: int,
        suspect_after_misses: int,
    ) -> None:
        self.counters.increment("heartbeat_acks_missed")
        runtime = self._boards[board_id]
        runtime.state.mark_heartbeat_missed(suspect_after_misses=suspect_after_misses)
        self._observe_board_state(runtime.state)
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "heartbeat_ack_missed",
                "details": {
                    "board_id": board_id,
                    "seq": seq,
                    "missed_count": runtime.state.heartbeat_missed_count,
                    "rx_path_suspect": runtime.state.rx_path_suspect,
                },
            }
        )

    def record_malformed_heartbeat_ack(self, board_id: str, *, seq: Any | None = None) -> None:
        self.counters.increment("malformed_heartbeat_acks")
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "malformed_heartbeat_ack",
                "details": {"board_id": board_id, "seq": seq},
            }
        )

    def record_late_heartbeat_ack(self, board_id: str, *, seq: int) -> None:
        self.counters.increment("late_heartbeat_acks")
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "late_heartbeat_ack",
                "details": {"board_id": board_id, "seq": seq},
            }
        )

    def mark_heartbeat_disabled(self, board_id: str) -> None:
        runtime = self._boards[board_id]
        runtime.state.mark_heartbeat_disabled()
        self._observe_board_state(runtime.state)

    def monotonic_time(self) -> float:
        return self._monotonic_time()

    def record_board_inbound(
        self,
        board_id: str,
        *,
        received_at: float | None = None,
    ) -> None:
        runtime = self._boards.get(board_id)
        if runtime is None:
            return
        if received_at is None:
            received_at = self._monotonic_time()
        runtime.state.last_seen = received_at
        self._observe_board_state(runtime.state)

    def record_telemetry_liveness_timeout(
        self,
        board_id: str,
        *,
        elapsed_s: float,
        timeout_s: float,
    ) -> None:
        self.counters.increment("telemetry_liveness_timeouts")
        self.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "telemetry_liveness_timeout",
                "details": {
                    "board_id": board_id,
                    "elapsed_s": elapsed_s,
                    "timeout_s": timeout_s,
                },
            }
        )

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
        if self._shutting_down:
            return
        if runtime.writer is None or runtime.state.conn_state is not BoardConnState.REGISTERED:
            return
        if runtime.state.in_flight_board_seq is not None:
            return

        while runtime.fifo:
            entry = runtime.fifo.popleft()
            runtime.state.queue_depth = len(runtime.fifo)
            self._observe_board_state(runtime.state)
            if entry.queue_residency_expired(self._monotonic_time()):
                self.counters.increment("stale_command_rejections")
                self._resolve_entry_error(
                    entry,
                    ErrorCode.COMMAND_TIMEOUT,
                    "queued command exceeded residency cap",
                    phase="queue_timeout",
                )
                continue

            sent_at = self._monotonic_time()
            board_command = build_board_command(
                entry.command,
                board_seq=entry.board_seq,
                board_id=board_id,
                controller_ts=sent_at,
            )
            runtime.state.in_flight_board_seq = entry.board_seq
            entry.written_at = sent_at
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
            if entry.future.done():
                self._pending.pop(entry.board_seq, None)
                if runtime.state.in_flight_board_seq == entry.board_seq:
                    runtime.state.in_flight_board_seq = None
                    self._observe_board_state(runtime.state)
                return
            if self._shutting_down:
                return
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
            self.counters.increment("estop_rejections")
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
        self._remember_terminal_board_seq(entry.board_seq, response["status"])
        self._record_terminal_response(response)
        entry.future.set_result(response)

    def _cancel_timeout(self, board_seq: int) -> None:
        task = self._timeout_tasks.pop(board_seq, None)
        if task is not None:
            task.cancel()

    def _record_terminal_response(self, response: dict[str, Any]) -> None:
        self.counters.record_terminal_response(response)

    def _remember_terminal_board_seq(self, board_seq: int | None, status: str) -> None:
        if board_seq is None:
            return
        if len(self._resolved_board_seq_order) == self._resolved_board_seq_order.maxlen:
            oldest = self._resolved_board_seq_order[0]
            self._resolved_board_seqs.pop(oldest, None)
        self._resolved_board_seq_order.append(board_seq)
        self._resolved_board_seqs[board_seq] = status

    def record_board_telemetry(
        self,
        message: dict[str, Any],
        *,
        received_at: float | None = None,
    ) -> None:
        validate_message(message)
        board_id = message["source"]
        runtime = self._boards.get(board_id)
        if runtime is None:
            return
        if received_at is None:
            received_at = self._monotonic_time()
        runtime.state.last_seen = received_at
        runtime.state.last_telemetry = message["telemetry"]
        runtime.state.telemetry_rate.observe(received_at)
        self._observe_board_telemetry(message, runtime.state)
        self._observe_board_state(runtime.state)

    def observe_board_telemetry(self, message: dict[str, Any]) -> None:
        board_id = message.get("source")
        if isinstance(board_id, str) and board_id in self._boards:
            try:
                self.record_board_telemetry(message)
            except ProtocolValidationError:
                return
            return
        self._observe_board_telemetry(message, None)

    def _observe_board_telemetry(
        self,
        message: dict[str, Any],
        state: BoardState | None,
    ) -> None:
        if self.observability is None:
            return
        try:
            telemetry_message = dict(message)
            if state is not None:
                telemetry_message["controller_received_at"] = state.last_seen
                telemetry_message["telemetry_rate_hz"] = state.telemetry_rate.rate_hz
                telemetry_message["telemetry_jitter_ms"] = state.telemetry_rate.jitter_ms
                telemetry_message["telemetry_interval_ms"] = state.telemetry_rate.last_interval_ms
                telemetry_message["telemetry_sample_count"] = state.telemetry_rate.sample_count
            self.observability.enqueue_board_telemetry(telemetry_message)
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
        latency_ms: float | None = None,
        board_proc_us: float | None = None,
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
                latency_ms=latency_ms,
                board_proc_us=board_proc_us,
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

    def _monotonic_time(self) -> float:
        if self._monotonic_clock is not None:
            return self._monotonic_clock()
        return asyncio.get_running_loop().time()

    def _extract_board_proc_us(self, response: dict[str, Any]) -> float | None:
        value = response.get("board_proc_us")
        if value is None and isinstance(response.get("result"), dict):
            value = response["result"].get("board_proc_us")
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        if value < 0:
            return None
        return float(value)
