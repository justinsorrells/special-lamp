"""Shared v1 controller state contract dataclasses.

These types describe the state shape used by later controller pieces. They do
not implement sockets, Redis access, firmware behavior, or GUI integration.
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

DEFAULT_COMMAND_FIFO_DEPTH = 6
DEFAULT_COMMAND_TIMEOUT_S = 2.0
MAX_COMMAND_TIMEOUT_S = 10.0
DEFAULT_QUEUE_RESIDENCY_CAP_S = 10.0
DEFAULT_BOARD_LIVENESS_TIMEOUT_S = 0.25
DEFAULT_LATENCY_SAMPLE_WINDOW = 1024


class BoardConnState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    REGISTERED = "REGISTERED"
    FAULTED = "FAULTED"


@dataclass
class CommandLatencyObservation:
    board_seq: int
    latency_ms: float
    controller_ts: float
    observed_at: float
    board_proc_us: float | None = None


@dataclass
class LatencyPercentileObservation:
    sample_count: int = 0
    samples: deque[float] = field(
        default_factory=lambda: deque(maxlen=DEFAULT_LATENCY_SAMPLE_WINDOW)
    )

    def observe(self, latency_ms: float) -> None:
        self.sample_count += 1
        self.samples.append(max(0.0, latency_ms))

    def percentile(self, percentile: float) -> float | None:
        if not self.samples:
            return None
        if percentile < 0 or percentile > 100:
            raise ValueError("percentile must be from 0 to 100")
        ordered = sorted(self.samples)
        if percentile == 0:
            return ordered[0]
        index = math.ceil(len(ordered) * percentile / 100)
        return ordered[min(len(ordered) - 1, index - 1)]

    def as_metrics(self, *, prefix: str = "command_latency") -> dict[str, float | int | None]:
        return {
            f"{prefix}_sample_count": self.sample_count,
            f"{prefix}_p50_ms": self.percentile(50),
            f"{prefix}_p95_ms": self.percentile(95),
            f"{prefix}_p99_ms": self.percentile(99),
        }


@dataclass
class TelemetryRateObservation:
    sample_count: int = 0
    last_arrival_at: float | None = None
    last_interval_ms: float | None = None
    rate_hz: float | None = None
    jitter_ms: float | None = None

    def observe(self, arrived_at: float) -> None:
        self.sample_count += 1
        if self.last_arrival_at is None:
            self.last_arrival_at = arrived_at
            return

        interval_s = arrived_at - self.last_arrival_at
        self.last_arrival_at = arrived_at
        if interval_s <= 0:
            self.last_interval_ms = 0.0
            self.rate_hz = None
            self.jitter_ms = None if self.last_interval_ms is None else 0.0
            return

        interval_ms = interval_s * 1000
        previous_interval_ms = self.last_interval_ms
        self.last_interval_ms = interval_ms
        self.rate_hz = 1.0 / interval_s
        self.jitter_ms = (
            None
            if previous_interval_ms is None
            else abs(interval_ms - previous_interval_ms)
        )


@dataclass
class SystemState:
    estop_active: bool = False
    connected_count: int = 0

    def latch_estop(self) -> None:
        self.estop_active = True

    def operator_reset_estop(self) -> None:
        self.estop_active = False


@dataclass
class BoardState:
    board_id: str
    conn_state: BoardConnState = BoardConnState.DISCONNECTED
    estop_ack: bool = False
    rx_path_suspect: bool = False
    heartbeat_enabled: bool = False
    heartbeat_missed_count: int = 0
    last_heartbeat_sent_at: float | None = None
    last_heartbeat_ack_at: float | None = None
    last_telemetry: dict[str, Any] | None = None
    last_seen: float | None = None
    queue_depth: int = 0
    in_flight_board_seq: int | None = None
    schema: dict[str, Any] | None = None
    last_command_latency: CommandLatencyObservation | None = None
    command_latency_percentiles: LatencyPercentileObservation = field(
        default_factory=LatencyPercentileObservation
    )
    telemetry_rate: TelemetryRateObservation = field(default_factory=TelemetryRateObservation)

    def mark_estop_sent(self) -> None:
        self.estop_ack = False

    def mark_estop_ack(self) -> None:
        self.estop_ack = True

    def mark_heartbeat_disabled(self) -> None:
        self.heartbeat_enabled = False
        self.rx_path_suspect = False
        self.heartbeat_missed_count = 0
        self.last_heartbeat_sent_at = None
        self.last_heartbeat_ack_at = None

    def mark_heartbeat_sent(self, sent_at: float) -> None:
        self.heartbeat_enabled = True
        self.last_heartbeat_sent_at = sent_at

    def mark_heartbeat_ack(self, ack_at: float) -> None:
        self.heartbeat_enabled = True
        self.last_heartbeat_ack_at = ack_at
        self.heartbeat_missed_count = 0
        self.rx_path_suspect = False

    def mark_heartbeat_missed(self, *, suspect_after_misses: int) -> None:
        self.heartbeat_enabled = True
        self.heartbeat_missed_count += 1
        if self.heartbeat_missed_count >= suspect_after_misses:
            self.rx_path_suspect = True


@dataclass
class PendingCommand:
    board_id: str
    board_seq: int
    client_seq: int
    client: Any
    future: asyncio.Future[Any]
    command: dict[str, Any]
    enqueued_at: float
    queue_residency_cap_s: float = DEFAULT_QUEUE_RESIDENCY_CAP_S
    execution_timeout_s: float = DEFAULT_COMMAND_TIMEOUT_S
    written_at: float | None = None

    def __post_init__(self) -> None:
        if self.execution_timeout_s > MAX_COMMAND_TIMEOUT_S:
            raise ValueError("execution_timeout_s exceeds the 10 s hard ceiling")

    def queue_residency_expired(self, now: float) -> bool:
        return now - self.enqueued_at > self.queue_residency_cap_s


@dataclass(frozen=True)
class BoardStateRecord:
    """Redis mirror schema for board:state:<id>; Redis is not source of truth."""

    board_id: str
    conn_state: BoardConnState
    estop_ack: bool
    rx_path_suspect: bool = False
    heartbeat_enabled: bool = False
    heartbeat_missed_count: int = 0
    last_heartbeat_sent_at: float | None = None
    last_heartbeat_ack_at: float | None = None
    last_telemetry: dict[str, Any] | None = None
    last_seen: float | None = None
    queue_depth: int = 0
    in_flight_board_seq: int | None = None
    last_command_latency_ms: float | None = None
    last_board_proc_us: float | None = None
    command_latency_sample_count: int = 0
    command_latency_p50_ms: float | None = None
    command_latency_p95_ms: float | None = None
    command_latency_p99_ms: float | None = None
    telemetry_rate_hz: float | None = None
    telemetry_jitter_ms: float | None = None
    telemetry_interval_ms: float | None = None
    telemetry_sample_count: int = 0

    @classmethod
    def from_board_state(cls, state: BoardState) -> BoardStateRecord:
        return cls(
            board_id=state.board_id,
            conn_state=state.conn_state,
            estop_ack=state.estop_ack,
            rx_path_suspect=state.rx_path_suspect,
            heartbeat_enabled=state.heartbeat_enabled,
            heartbeat_missed_count=state.heartbeat_missed_count,
            last_heartbeat_sent_at=state.last_heartbeat_sent_at,
            last_heartbeat_ack_at=state.last_heartbeat_ack_at,
            last_telemetry=state.last_telemetry,
            last_seen=state.last_seen,
            queue_depth=state.queue_depth,
            in_flight_board_seq=state.in_flight_board_seq,
            last_command_latency_ms=(
                None if state.last_command_latency is None else state.last_command_latency.latency_ms
            ),
            last_board_proc_us=(
                None if state.last_command_latency is None else state.last_command_latency.board_proc_us
            ),
            command_latency_sample_count=state.command_latency_percentiles.sample_count,
            command_latency_p50_ms=state.command_latency_percentiles.percentile(50),
            command_latency_p95_ms=state.command_latency_percentiles.percentile(95),
            command_latency_p99_ms=state.command_latency_percentiles.percentile(99),
            telemetry_rate_hz=state.telemetry_rate.rate_hz,
            telemetry_jitter_ms=state.telemetry_rate.jitter_ms,
            telemetry_interval_ms=state.telemetry_rate.last_interval_ms,
            telemetry_sample_count=state.telemetry_rate.sample_count,
        )

    def as_hash(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "conn_state": self.conn_state.value,
            "estop_ack": self.estop_ack,
            "rx_path_suspect": self.rx_path_suspect,
            "heartbeat_enabled": self.heartbeat_enabled,
            "heartbeat_missed_count": self.heartbeat_missed_count,
            "last_heartbeat_sent_at": self.last_heartbeat_sent_at,
            "last_heartbeat_ack_at": self.last_heartbeat_ack_at,
            "last_telemetry": self.last_telemetry,
            "last_seen": self.last_seen,
            "queue_depth": self.queue_depth,
            "in_flight_board_seq": self.in_flight_board_seq,
            "last_command_latency_ms": self.last_command_latency_ms,
            "last_board_proc_us": self.last_board_proc_us,
            "command_latency_sample_count": self.command_latency_sample_count,
            "command_latency_p50_ms": self.command_latency_p50_ms,
            "command_latency_p95_ms": self.command_latency_p95_ms,
            "command_latency_p99_ms": self.command_latency_p99_ms,
            "telemetry_rate_hz": self.telemetry_rate_hz,
            "telemetry_jitter_ms": self.telemetry_jitter_ms,
            "telemetry_interval_ms": self.telemetry_interval_ms,
            "telemetry_sample_count": self.telemetry_sample_count,
        }


@dataclass(frozen=True)
class SystemStateRecord:
    """Redis mirror schema for system:state; Redis is not source of truth."""

    estop_active: bool
    connected_count: int

    @classmethod
    def from_system_state(cls, state: SystemState) -> SystemStateRecord:
        return cls(
            estop_active=state.estop_active,
            connected_count=state.connected_count,
        )

    def as_hash(self) -> dict[str, Any]:
        return {
            "estop_active": self.estop_active,
            "connected_count": self.connected_count,
        }


@dataclass
class BoardSeqCounter:
    """Per-board monotonic uint64 board_seq generator."""

    next_value: int = 1

    def next(self) -> int:
        if self.next_value > 2**64 - 1:
            raise OverflowError("board_seq exhausted uint64 range")
        value = self.next_value
        self.next_value += 1
        return value


@dataclass
class ControllerState:
    system: SystemState = field(default_factory=SystemState)
    boards: dict[str, BoardState] = field(default_factory=dict)

    def connected_count(self) -> int:
        return sum(1 for board in self.boards.values() if board.conn_state is BoardConnState.REGISTERED)

    def refresh_connected_count(self) -> None:
        self.system.connected_count = self.connected_count()
