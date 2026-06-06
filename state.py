"""Shared v1 controller state contract dataclasses.

These types describe the state shape used by later controller pieces. They do
not implement sockets, Redis access, firmware behavior, or GUI integration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
import asyncio


DEFAULT_COMMAND_FIFO_DEPTH = 6
DEFAULT_COMMAND_TIMEOUT_S = 2.0
MAX_COMMAND_TIMEOUT_S = 10.0
DEFAULT_QUEUE_RESIDENCY_CAP_S = 10.0


class BoardConnState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    REGISTERED = "REGISTERED"
    FAULTED = "FAULTED"


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
    last_telemetry: dict[str, Any] | None = None
    last_seen: float | None = None
    queue_depth: int = 0
    in_flight_board_seq: int | None = None
    schema: dict[str, Any] | None = None

    def mark_estop_sent(self) -> None:
        self.estop_ack = False

    def mark_estop_ack(self) -> None:
        self.estop_ack = True


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
    last_telemetry: dict[str, Any] | None = None
    last_seen: float | None = None
    queue_depth: int = 0
    in_flight_board_seq: int | None = None

    @classmethod
    def from_board_state(cls, state: BoardState) -> "BoardStateRecord":
        return cls(
            board_id=state.board_id,
            conn_state=state.conn_state,
            estop_ack=state.estop_ack,
            last_telemetry=state.last_telemetry,
            last_seen=state.last_seen,
            queue_depth=state.queue_depth,
            in_flight_board_seq=state.in_flight_board_seq,
        )

    def as_hash(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "conn_state": self.conn_state.value,
            "estop_ack": self.estop_ack,
            "last_telemetry": self.last_telemetry,
            "last_seen": self.last_seen,
            "queue_depth": self.queue_depth,
            "in_flight_board_seq": self.in_flight_board_seq,
        }


@dataclass(frozen=True)
class SystemStateRecord:
    """Redis mirror schema for system:state; Redis is not source of truth."""

    estop_active: bool
    connected_count: int

    @classmethod
    def from_system_state(cls, state: SystemState) -> "SystemStateRecord":
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

