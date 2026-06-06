---
name: asyncio-controller
description: >
  Read before writing or modifying the Python asyncio controller for the
  Hyperloop board networking stack. Covers the command path, per-board FIFO and
  one-in-flight rule, BOARD_BUSY, the two timeout clocks and 10 s ceiling, the
  per-board writer lock, out-of-band e-stop writes, and the bounded drop-oldest
  Redis/observability queue. Use for dispatcher, board connection manager, Unix
  socket server, timeout, and telemetry-writer work.
---

# Asyncio Controller

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md` (v3 FROZEN) is
> the source of truth; also read the `project-networking-invariants` skill. Refs
> like (1.5) point at the governing contract decision. The contract wins on any
> conflict.

The controller is the single authority for board communication. It uses Python
asyncio throughout. Avoid blocking calls on any async path. Prefer `asyncio`
streams. Keep parsing, routing, board-connection management, and Unix-socket
client handling in separate modules.

## Responsibilities

* Accept local client requests over a **full-duplex** Unix domain socket; clients
  also receive unsolicited `event` messages, not only `response`s (1.14).
* Maintain **persistent TCP connections to boards** (the controller connects; the
  board is the server) (1.3).
* Route commands to the correct board using the controller-owned `board_seq` (1.1).
* Enforce timeouts (see Timeouts below).
* Return a terminal status to the requester.
* Mirror board state to Redis and log telemetry/events via a bounded queue.
* Track board availability on the connection axis (2.1).
* Enforce the e-stop gate and perform out-of-band `estop` writes (1.13, 1.19).

## One command in flight per board

* The Teensy is single-threaded: **at most one command in flight per board**.
* Additional commands queue in a **bounded per-board FIFO** (default depth 6;
  tune to real burst pattern).
* FIFO is **reject-newest**: when full, reject the incoming command with
  `BOARD_BUSY`. Never drop-oldest a command; a control action must not vanish
  silently (1.2, 1.7).
* Telemetry and events from the board are unsolicited and are not subject to the
  in-flight rule; the reader dispatches them by `type`.

## Timeouts (two distinct clocks)

* **Execution timeout** starts **when the command is written to the board**, not
  when it is enqueued. Use per-command `asyncio.wait_for` (no central scanning
  task). Default 2 s. Per-command overrides allowed, but **assert no override
  exceeds the 10 s hard ceiling.** On expiry -> `COMMAND_TIMEOUT` (1.5).
* **Queue residency cap** applies while a command waits in the FIFO. On pop, if it
  has waited longer than its cap (default 10 s), reject with `COMMAND_TIMEOUT` and
  **never write it to the board** (stops late execution of a stale command) (1.5).

## Pending command resolution (pop-wins)

Both the timeout path and the reader path resolve a pending command via
`entry = pending.pop(board_seq, None)` with **no `await` between the lookup and
the pop**. The winner resolves the future (guard `if not future.done()`); the
loser gets `None` and drops. A board message with no matching `board_seq` is
dropped and logged (`unmatched_seq`), never raised. A late response after timeout
is normal (1.8).

## Per-board writer serialization

* Every board connection has **exactly one serialized write path**: a per-board
  writer lock or a single per-board writer task.
* Normal command writes and out-of-band `estop` writes both acquire it. **No two
  coroutines call `writer.write()` / `writer.drain()` on the same board stream
  concurrently** (1.19).
* `estop` bypasses the FIFO and the in-flight slot, **but not** the writer lock.

```
FIFO bypass:                    yes (estop)
in-flight command slot bypass:  yes (estop)
writer-lock bypass:             no  (never)
socket-byte interleaving:       no  (never)
```

## E-stop handling

* On an `estop_triggered` event: set `system.estop_active = true`; clear each
  per-board FIFO and fail those queued commands with `ESTOP_ACTIVE`; **leave any
  in-flight command to resolve naturally** (ok or timeout, do not synthesize a
  result); broadcast `estop` to all connected boards out-of-band via the writer
  lock; set each `estop_ack=false`, flip true on the board's `estop_ack` event;
  mirror to Redis; notify all local clients (1.13).
* The latch is global and operator-cleared only. Reconnect during e-stop: the
  board reaches `REGISTERED` normally, the controller re-sends `estop`, and
  dispatch stays blocked by the global flag (1.13).
* Command gating while `estop_active` is schema-driven (`blocked_by_estop`,
  absent -> blocked) (1.17).

## Redis / observability path

* Single bounded `obs_queue` (default maxsize 20000), **drop-oldest** on full
  (`put_nowait`; on `QueueFull` do one `get_nowait()`, bump `obs_dropped`, retry).
* A separate writer task drains it. **Redis latency or outage must never block
  the command path or e-stop.** Dropped writes increment a counter; board control
  continues (1.7).
* This drop-oldest policy is for observability only. It is the opposite of the
  command FIFO, which is reject-newest.

## Local client backpressure

Each client has a bounded outbound queue (default 1000). Drop oldest non-critical
events on overflow (`client_event_dropped`); **never drop `response` or `estop`/
safety events**. If saturated with critical messages, disconnect that client
rather than stall the broadcast loop (1.18).

## Lifecycle

On SIGTERM: stop accepting new local requests; drain in-flight commands for ~2 s;
fail the remainder with `CONTROLLER_SHUTDOWN`; close board connections; flush the
Redis queue best-effort with a short timeout (1.12-context, contract section 1).

## Definition of done

Tests cover the changed timeout / FIFO / disconnect / e-stop behavior; no
blocking calls on async paths; no new `status` values (error codes only);
invariants hold. If protocol behavior changes, update
`docs/contracts/V1_Networking_Decisions.md`, the relevant tests, and AGENTS.md
in the same change.
