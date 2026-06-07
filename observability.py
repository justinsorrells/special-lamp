"""Redis-backed observability queue for the v1 controller.

Redis is a read-replica and logging target only. This module owns a bounded
drop-oldest queue and an async worker that best-effort writes telemetry, state
snapshots, controller events, and command lifecycle records.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Protocol

from state import BoardState, BoardStateRecord, SystemState, SystemStateRecord

DEFAULT_OBS_QUEUE_SIZE = 20_000
DEFAULT_STREAM_MAXLEN = 100_000
TERMINAL_STATUSES = {"ok", "error", "timeout"}


LOGGER = logging.getLogger(__name__)


class AsyncRedisLike(Protocol):
    async def hset(self, name: str, mapping: dict[str, Any]) -> Any:
        ...

    async def xadd(
        self,
        name: str,
        fields: dict[str, Any],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> Any:
        ...

    async def publish(self, channel: str, message: str) -> Any:
        ...


@dataclass
class ObservabilityCounters:
    obs_dropped: int = 0
    redis_write_failures: int = 0
    records_written: int = 0


class ObservabilityQueue:
    """Single bounded drop-oldest observability queue."""

    def __init__(self, *, maxsize: int = DEFAULT_OBS_QUEUE_SIZE):
        if maxsize <= 0:
            raise ValueError("observability queue maxsize must be positive")
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self.counters = ObservabilityCounters()
        self._next_state_event_id = 1

    def enqueue(self, record: dict[str, Any]) -> bool:
        try:
            self.queue.put_nowait(record)
            return True
        except asyncio.QueueFull:
            try:
                self.queue.get_nowait()
                self.queue.task_done()
                self.counters.obs_dropped += 1
            except asyncio.QueueEmpty:
                pass
            self.queue.put_nowait(record)
            return True

    def enqueue_board_state(self, state: BoardState) -> bool:
        return self.enqueue(
            serialize_board_state_snapshot(
                state,
                event_id=self._state_event_id(),
            )
        )

    def enqueue_system_state(self, state: SystemState) -> bool:
        return self.enqueue(
            serialize_system_state_snapshot(
                state,
                event_id=self._state_event_id(),
            )
        )

    def enqueue_board_telemetry(self, message: dict[str, Any]) -> bool:
        return self.enqueue(serialize_board_telemetry(message))

    def enqueue_controller_event(self, event: dict[str, Any]) -> bool:
        return self.enqueue(serialize_controller_event(event))

    def enqueue_command_lifecycle(
        self,
        *,
        command_id: str,
        seq: int,
        board_id: str,
        phase: str,
        status: str | None = None,
        board_seq: int | None = None,
        error_code: str | None = None,
        command: str | None = None,
        controller_ts: float | None = None,
        latency_ms: float | None = None,
        board_proc_us: float | None = None,
    ) -> bool:
        return self.enqueue(
            serialize_command_lifecycle(
                command_id=command_id,
                seq=seq,
                board_id=board_id,
                phase=phase,
                status=status,
                board_seq=board_seq,
                error_code=error_code,
                command=command,
                controller_ts=controller_ts,
                latency_ms=latency_ms,
                board_proc_us=board_proc_us,
            )
        )

    def _state_event_id(self) -> int:
        event_id = self._next_state_event_id
        self._next_state_event_id += 1
        return event_id


class RedisTelemetryWorker:
    """Best-effort async Redis writer for observability records."""

    def __init__(
        self,
        *,
        redis: AsyncRedisLike | None,
        obs_queue: ObservabilityQueue,
        stream_maxlen: int = DEFAULT_STREAM_MAXLEN,
    ):
        self.redis = redis
        self.obs_queue = obs_queue
        self.stream_maxlen = stream_maxlen
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run())

    async def stop(self, *, drain_timeout_s: float = 0.2) -> None:
        self._stop.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.obs_queue.queue.join(), timeout=drain_timeout_s)
        except TimeoutError:
            pass
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    async def run(self) -> None:
        while not self._stop.is_set() or not self.obs_queue.queue.empty():
            try:
                record = await asyncio.wait_for(self.obs_queue.queue.get(), timeout=0.05)
            except TimeoutError:
                continue
            try:
                await self._write_record(record)
                self.obs_queue.counters.records_written += 1
            except Exception:
                self.obs_queue.counters.redis_write_failures += 1
                LOGGER.exception("redis observability write failed")
            finally:
                self.obs_queue.queue.task_done()

    async def _write_record(self, record: dict[str, Any]) -> None:
        if self.redis is None:
            return
        kind = record["kind"]
        if kind == "board_state":
            await self.redis.hset(record["key"], mapping=_redis_mapping(record["hash"]))
            await self.redis.publish(record["channel"], _json(record["message"]))
        elif kind == "system_state":
            await self.redis.hset(record["key"], mapping=_redis_mapping(record["hash"]))
            await self.redis.publish(record["channel"], _json(record["message"]))
        elif kind in {"board_telemetry", "controller_event", "command_lifecycle"}:
            await self.redis.xadd(
                record["stream"],
                _redis_mapping(record["fields"]),
                maxlen=self.stream_maxlen,
                approximate=True,
            )
        else:
            await self.redis.xadd(
                "controller:observability:unknown",
                {"record": _json(record)},
                maxlen=self.stream_maxlen,
                approximate=True,
            )


def serialize_board_state_snapshot(state: BoardState, *, event_id: int | None = None) -> dict[str, Any]:
    record = BoardStateRecord.from_board_state(state)
    payload = record.as_hash()
    if event_id is None:
        event_id = 0
    return {
        "kind": "board_state",
        "key": f"board:state:{record.board_id}",
        "channel": "board:state:updates",
        "hash": payload,
        "message": {
            "type": "board_state",
            "event_id": event_id,
            "board_id": record.board_id,
            "state": payload,
        },
    }


def serialize_system_state_snapshot(state: SystemState, *, event_id: int | None = None) -> dict[str, Any]:
    record = SystemStateRecord.from_system_state(state)
    payload = record.as_hash()
    if event_id is None:
        event_id = 0
    return {
        "kind": "system_state",
        "key": "system:state",
        "channel": "board:state:updates",
        "hash": payload,
        "message": {"type": "system_state", "event_id": event_id, "state": payload},
    }


def serialize_board_telemetry(message: dict[str, Any]) -> dict[str, Any]:
    board_id = message["source"]
    fields = {
        "board_id": board_id,
        "seq": message.get("seq"),
        "timestamp": message.get("timestamp"),
        "telemetry": message.get("telemetry", {}),
        "controller_received_at": message.get("controller_received_at"),
        "telemetry_rate_hz": message.get("telemetry_rate_hz"),
        "telemetry_jitter_ms": message.get("telemetry_jitter_ms"),
        "telemetry_interval_ms": message.get("telemetry_interval_ms"),
        "telemetry_sample_count": message.get("telemetry_sample_count"),
    }
    return {
        "kind": "board_telemetry",
        "stream": f"board:telemetry:{board_id}",
        "fields": fields,
    }


def serialize_controller_event(event: dict[str, Any]) -> dict[str, Any]:
    fields = {
        "source": event.get("source", "controller"),
        "event": event.get("event"),
        "event_id": event.get("event_id"),
        "details": event.get("details", {}),
    }
    return {"kind": "controller_event", "stream": "controller:events", "fields": fields}


def serialize_command_lifecycle(
    *,
    command_id: str,
    seq: int,
    board_id: str,
    phase: str,
    status: str | None = None,
    board_seq: int | None = None,
    error_code: str | None = None,
    command: str | None = None,
    controller_ts: float | None = None,
    latency_ms: float | None = None,
    board_proc_us: float | None = None,
) -> dict[str, Any]:
    if status is not None and status not in TERMINAL_STATUSES:
        raise ValueError(f"invalid terminal command status {status!r}")
    if controller_ts is None:
        controller_ts = time.monotonic()
    fields = {
        "command_id": command_id,
        "seq": seq,
        "board_id": board_id,
        "phase": phase,
        "status": status,
        "board_seq": board_seq,
        "error_code": error_code,
        "command": command,
        "controller_ts": controller_ts,
        "latency_ms": latency_ms,
        "board_proc_us": board_proc_us,
    }
    return {"kind": "command_lifecycle", "stream": "command:lifecycle", "fields": fields}


def _redis_mapping(mapping: dict[str, Any]) -> dict[str, str]:
    return {key: _redis_value(value) for key, value in mapping.items()}


def _redis_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | str):
        return str(value)
    return _json(value)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
