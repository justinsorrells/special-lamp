---
name: graceful-shutdown-and-lifecycle
description: >
  Read before implementing or changing controller shutdown / lifecycle behavior
  for the Hyperloop board networking stack. Covers SIGTERM/SIGINT handling via a
  callable shutdown path, stopping local request acceptance, the bounded in-flight
  drain window, failing remaining work with CONTROLLER_SHUTDOWN (never a new
  status), cancelling background tasks and handling CancelledError, closing board
  and client streams, best-effort Redis flush, and idempotency. Use for the
  controller lifecycle shutdown task.
---

# Graceful Shutdown and Lifecycle

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md`
> (v3 FROZEN) is the source of truth; also read the `asyncio-controller` and
> `project-networking-invariants` skills. Refs like (1.8) point at the governing
> contract decision. The contract wins on any conflict.

Shutdown must be **graceful, bounded, and terminal**: every accepted command
gets a terminal response, nothing is silently dropped, and the process never
hangs waiting on a slow peer or Redis.

## Shape: a callable shutdown path plus a thin signal handler

* Implement shutdown as an **`async def shutdown()` coroutine** on the controller.
  The signal handler does nothing but schedule it.
* Register handlers with `loop.add_signal_handler(signal.SIGTERM, ...)` and the
  same for `SIGINT`. Do **not** use blocking `signal.signal` handlers on the
  async path. The handler should `loop.create_task(controller.shutdown())` (or
  set an `asyncio.Event` the main coroutine awaits) — keep the handler trivial.
* Tests drive `await controller.shutdown()` directly; a single thin test covers
  the signal-handler wiring. Do not require a real OS signal in every test.

## Ordered shutdown sequence

1. **Stop accepting new local requests.** Close/stop the Unix socket server so no
   new client commands are admitted. A command that arrives during shutdown gets
   a structured `error` response with code `CONTROLLER_SHUTDOWN`.
2. **Quiesce board read loops** so no new commands are dispatched, but allow
   in-flight commands a chance to resolve.
3. **Bounded drain window (~2 s).** Wait up to the window for in-flight commands
   to resolve naturally (ok / timeout). Use `asyncio.wait_for` or
   `asyncio.wait(..., timeout=...)` — never an unbounded await.
4. **Fail the remainder** with `CONTROLLER_SHUTDOWN`: any command still pending,
   in-flight, or queued in a per-board FIFO resolves to an `error` response
   carrying that code. Resolve through the **pop-wins** path so a late board
   response cannot double-resolve a command already failed by shutdown (1.8).
5. **Close streams**: close board TCP writers and local client writers, then
   `await wait_closed()` with a short timeout so a dead socket cannot stall.
6. **Cancel background tasks** (telemetry writer, per-board tasks): `task.cancel()`
   then `await asyncio.gather(*tasks, return_exceptions=True)`.
7. **Flush Redis best-effort** with a short timeout; a flush failure or outage is
   logged and shutdown proceeds. Redis must never block or hang shutdown (1.7).

## Terminal status discipline

* The shutdown failure code is the existing `CONTROLLER_SHUTDOWN` error code; its
  `status` is `error` (or `timeout` where a command genuinely timed out during
  the drain window). **Do not add a `shutdown` terminal status** — statuses stay
  `ok` / `error` / `timeout` (3.12). The checker `tools/check_invariants.py`
  enforces this.
* Do not synthesize a fake `ok` result for an unresolved command.

## CancelledError and idempotency

* In any coroutine that may be cancelled, let `asyncio.CancelledError` propagate
  after cleanup — catch it only to run cleanup, then `raise`. Never swallow it.
* **Shutdown must be idempotent.** Guard with a flag/`Event` so calling it twice
  (e.g. SIGTERM then SIGINT) runs the sequence once and the second call is a
  no-op (or awaits the same completion).
* Wrap best-effort steps (stream close, Redis flush) so one failing step does not
  abort the rest of the sequence — collect and log errors, keep going.

## Things shutdown must NOT do

* Do not silently drop an accepted command without a terminal response + log.
* Do not block the event loop or wait unboundedly on a peer, socket, or Redis.
* Do not bypass the per-board writer lock when closing/draining (1.19).
* Do not touch firmware, GUI/webapp, the Redis command path, or add auto-restart
  / supervisor integration — out of scope per the backlog task.

## Definition of done

Tests cover: new requests rejected with `CONTROLLER_SHUTDOWN`; in-flight may
complete within the drain window; unresolved in-flight / queued commands fail
with `CONTROLLER_SHUTDOWN`; pending table cleared safely; streams and background
tasks closed/cancelled cleanly; late board response after shutdown does not
double-resolve (pop-wins); Redis flush failure is logged but does not hang;
shutdown is idempotent; no new terminal statuses. Tests use fake clocks / short
durations, not long real sleeps (see `testing-async-loops-and-mocks`). `pytest`
and `python -m compileall .` pass.
