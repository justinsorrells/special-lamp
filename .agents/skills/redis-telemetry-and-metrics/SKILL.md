---
name: redis-telemetry-and-metrics
description: >
  Read before writing or changing observability, telemetry, metrics, or Redis
  mirroring for the Hyperloop controller. Covers the bounded drop-oldest
  observability queue, the best-effort async Redis writer, why Redis failures must
  never reach the command path, terminal-status validation on lifecycle records,
  and safe JSON/hash serialization of board and system snapshots. Use for
  observability.py and any telemetry-emitting controller code.
---

# Redis Telemetry and Metrics

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md`
> (v3 FROZEN) is the source of truth; also read the `asyncio-controller` and
> `project-networking-invariants` skills. Refs like (1.7) point at the governing
> contract decision. The contract wins on any conflict.

Redis is an **observability read-replica and logging target only** — never the
board command protocol and never a source of truth for board state (topology
invariant). Everything here is a side-channel: if it stops, board control must
keep working unchanged (1.7). The current implementation lives in
`observability.py`.

## The bounded, drop-oldest queue

* There is a single `ObservabilityQueue` wrapping an `asyncio.Queue` with a
  bounded `maxsize` (default `DEFAULT_OBS_QUEUE_SIZE = 20_000`).
* On overflow it is **drop-oldest**: `put_nowait`; on `QueueFull`, `get_nowait()`
  the oldest record, bump `counters.obs_dropped`, then enqueue the new record.
  `enqueue(...)` therefore (almost) always succeeds and **never blocks**.
* This is the **opposite** of the command FIFO, which is reject-newest
  (`BOARD_BUSY`). Do not confuse the two policies: losing a telemetry frame is
  fine; losing a control command is not.
* Enqueue is **synchronous and non-awaiting** so any command-path coroutine can
  emit a record without yielding or risking backpressure. Keep it that way.

## The best-effort writer

* `RedisTelemetryWorker` drains the queue in its own task and writes to Redis
  (`hset` + `publish` for state snapshots; `xadd` to capped streams for
  telemetry / controller events / command lifecycle).
* **Every write is wrapped in `try/except Exception`**: on failure it bumps
  `counters.redis_write_failures`, logs via `LOGGER.exception`, and continues. A
  Redis outage or latency spike must never raise into the dispatcher or stall
  e-stop. Preserve this guarantee in any change.
* `redis=None` is a first-class **disabled mode**: records are consumed and
  dropped. Use it for tests and local development — never gate it behind import
  of a real Redis client (the invariant checker forbids importing `redis`/
  `observability` into the command path).
* Shutdown drains best-effort: `stop()` sets the stop event, waits on
  `queue.join()` with a short `drain_timeout_s`, then cancels. A slow flush must
  not hang shutdown (see `graceful-shutdown-and-lifecycle`).

## Emitting records from the controller

* The controller holds an injected `observability` sink (it does **not** import
  `observability`) and calls `enqueue_*` helpers: board state, system state,
  board telemetry, controller events, and command lifecycle phases.
* `serialize_command_lifecycle` **validates `status` against the terminal set**
  `{ok, error, timeout}` and raises on anything else — telemetry must not invent
  a status the protocol forbids. Honor this; add error *codes*, never statuses.
* State snapshots go through `BoardStateRecord` / `SystemStateRecord`; they carry
  a monotonically increasing `event_id` so consumers can order updates.

## Serialization rules

* Redis hash values are coerced to strings by `_redis_value`: `None` -> `""`,
  bools -> `"true"`/`"false"`, scalars -> `str`, everything else -> compact JSON.
* JSON uses `separators=(",", ":")` and `sort_keys=True` for stable, compact
  output. Match this when adding fields.
* Snapshots reflect the two **orthogonal axes** (connection state vs.
  `system.estop_active`); never merge them into one field when serializing (2).

## Definition of done

Telemetry/metrics changes keep enqueue non-blocking and drop-oldest; Redis
failures are counted + logged, never raised into the command path; disabled
(`redis=None`) mode still works for tests; no new terminal `status` values; the
shutdown drain stays bounded. Add tests for queue overflow/drop counting, writer
failure handling, and lifecycle status validation.
