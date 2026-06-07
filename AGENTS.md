# AGENTS.md

> **Authority.** `docs/contracts/V1_Networking_Decisions.md` is the authoritative
> networking/controller contract. `docs/contracts/Board_Developer_Guide.md`
> governs firmware. **This file is a navigation aid, not the source of truth.**
> Where this file is silent, defer to the contracts. Where it conflicts with the
> contracts, the contracts win and this file should be corrected. Section
> references like (1.8), (3.12) point at the governing contract decision.
>
> The skills live under `.agents/skills/<name>/SKILL.md` and are committed,
> present files; an agent should never have to chase a missing source of truth.

## Project summary

This repository implements the Hyperloop board networking stack.

* The mini PC controller is the single authority for board communication.
* Local applications, including the GUI, communicate with the controller over a
  Unix socket.
* Boards communicate with the controller over persistent TCP connections. **The
  board is the TCP server; the controller connects to it.** The board pushes its
  schema on connect and on every reconnect (contract 1.3).
* The v1 board protocol uses newline-delimited JSON messages.
* Redis may be used on the mini PC for telemetry, logs, observability, and
  debugging. Redis is **not** the board command protocol.

## Current architecture

The intended command path is:

```text
GUI / local client
    -> Unix socket
    -> Python asyncio controller on mini PC
    -> persistent TCP connection
    -> board
```

The intended telemetry/logging path is:

```text
board/controller events
    -> Python asyncio controller
    -> Redis / logs / observability tools
```

## Architecture invariants

Do not violate these without explicitly documenting the reason.

**Topology**
* The controller owns all board communication.
* The GUI does not talk directly to boards.
* Redis is not used as the board command protocol.
* Board communication uses TCP for v1; the board is the server, the controller
  connects, and connections are persistent.
* Message framing is newline-delimited JSON for v1.
* The controller uses Python asyncio and exposes a Unix socket for local clients.

**Command lifecycle**
* Every command must eventually produce a terminal status (see below).
* Resolution is single-owner (pop-wins): a late board response arriving after a
  timeout is dropped and logged, never raised, never double-resolved
  (contract 1.8).
* One command in flight per board; further commands queue in a bounded per-board
  FIFO and are rejected with `BOARD_BUSY` when full (contract 1.2).
* Do not silently drop commands.

**State**
* Single authority for board state: the controller's in-memory per-board object.
  Redis is a read-replica, never a source of truth.
* Connection state and safety state are **orthogonal axes**; do not merge them
  into one field/enum (contract 2).

**E-stop (convergence only)**
* Software e-stop is convergence only. The hardwired interlock and power cut are
  the safety layer. Software must never present itself as the safety guarantee.
* `system.estop_active` is a single global latch, cleared only by an operator
  `estop_reset`. Do not auto-clear it, and do not gate the latch on per-board
  acks (contract 1.13, 2.2).
* `estop` is written out-of-band (bypasses the FIFO and the in-flight slot) but
  **must acquire the same per-board writer lock** as normal writes. No two
  coroutines write the same board stream concurrently (contract 1.19).
* Command gating during e-stop is schema-driven (`blocked_by_estop`, absent =
  blocked) (contract 1.17).

**Robustness**
* Malformed messages must not crash the controller or the board.
* Board disconnects must be handled explicitly (contract 1.11).
* Redis failure must not prevent core board control (contract 1.7).
* Telemetry is one-way push from the board (~50 ms), never solicited
  (contract 1.6).

## Terminal command statuses

Every command resolves to exactly one terminal `status` (contract 3.12):

```text
ok
error
timeout
```

Rejections and disconnects are **not** separate statuses. They are `error` or
`timeout` responses carrying a specific error **code** (contract 3.11):

```text
INVALID_JSON  MISSING_FIELD  INVALID_TYPE  UNKNOWN_TARGET  UNKNOWN_COMMAND
INVALID_ARGUMENT  BOARD_UNAVAILABLE  BOARD_BUSY  COMMAND_TIMEOUT  ESTOP_ACTIVE
PROTOCOL_VERSION_MISMATCH  CONTROLLER_SHUTDOWN  INTERNAL_ERROR
```

Do not invent new `status` values; add an error code instead. Do not silently
drop commands.

## Coding rules

* Prefer simple, explicit code over clever abstractions.
* Keep protocol parsing separate from command routing.
* Keep board connection management separate from Unix socket client handling.
* Avoid blocking calls inside asyncio code.
* Prefer `asyncio` streams unless there is a clear reason not to.
* Add tests for protocol behavior, timeout behavior, and disconnect behavior.
* Update docs/comments (and `docs/contracts/V1_Networking_Decisions.md` if
  protocol behavior changed) in the same change.

### Definition of done (for any change)
* Tests cover the changed protocol / timeout / disconnect behavior.
* No blocking calls added to async paths.
* No new command `status` values (error codes only).
* The invariants section above still holds.
* If protocol behavior changed: `docs/contracts/V1_Networking_Decisions.md`,
  tests, and this file must be updated together.

## Build order (greenfield work)

Implement the shared contracts before feature work; everything builds against
them (contract 5):

```text
1. protocol.py     # message types, error codes, validation, framing, seq rules,
                   # pop-wins helper, blocked_by_estop default, estop_ack shape
2. state.py        # BoardConnState enum, SystemState (estop_active),
                   # per-board estop_ack, PendingCommand, Redis BoardStateRecord
3. interfaces.py   # board_down(board_id), pending-table ownership,
                   # orphaned-response handle, send_estop() out-of-band path,
                   # per-board writer lock / single-writer handle
```

## Skills

Eight repo skills encode the rules below. Read the relevant one before working
in that area; they defer to the frozen contract and must stay consistent with it.

* `project-networking-invariants` -- topology, the two orthogonal state axes,
  e-stop convergence model, terminal-status rules. Read for any change.
* `asyncio-controller` -- controller responsibilities: FIFO, timeouts, writer
  lock, Redis queue, e-stop gate.
* `newline-json-protocol` -- framing, line limits, seq vs board_seq, pop-wins,
  malformed-input handling, no new status values.
* `code-style-and-conventions` -- the repo's actual Python idioms; read for any
  code change so new code reads like the surrounding code.
* `testing-async-loops-and-mocks` -- deterministic asyncio tests, stream mocking,
  injectable clocks; read for anything under `tests/`.
* `graceful-shutdown-and-lifecycle` -- SIGTERM/SIGINT path, bounded in-flight
  drain, `CONTROLLER_SHUTDOWN`, idempotent shutdown.
* `redis-telemetry-and-metrics` -- the bounded drop-oldest observability queue
  and best-effort Redis writer; read for `observability.py`.
* `unix-sockets-and-backpressure` -- per-client outbound queues, critical vs
  non-critical event split, slow-client handling; read for `local_socket.py`.

## Forbidden changes

Do not make these unless the task explicitly asks for an architecture redesign:

* Do not make the GUI talk directly to boards.
* Do not put Redis in the command path between controller and boards.
* Do not make Redis a source of truth for board state.
* Do not replace newline-delimited framing without updating protocol tests.
* Do not introduce blocking socket loops into the asyncio controller.
* Do not allow malformed board/client messages to crash the controller.
* Do not create multiple authorities for board state, and do not merge
  connection state and safety state into one field.
* Do not bypass the per-board writer serialization for any write, including
  `estop`.
* Do not gate the global e-stop latch on per-board acks.
* Do not add new command `status` values.

## When working on controller code

> Use the `asyncio-controller` skill (`.agents/skills/asyncio-controller/SKILL.md`)
> and the `project-networking-invariants` skill.

The controller is responsible for:

* accepting local client requests over a Unix socket (full-duplex: clients
  receive unsolicited `event` messages as well as `response`s, contract 1.14)
* maintaining persistent board TCP connections (controller connects to boards)
* routing commands to the correct board (controller-owned `board_seq`,
  contract 1.1)
* enforcing timeouts (per-command, exec clock starts at board-write; queue
  residency cap; 10 s hard ceiling, contract 1.5)
* returning command status to the requester
* logging telemetry/events to Redis via a bounded, drop-oldest queue
  (contract 1.7)
* tracking board availability across the connection axis (contract 2.1)
* enforcing the e-stop gate and out-of-band `estop` writes (contract 1.13, 1.19)

## When working on protocol code

> Use the `newline-json-protocol` skill
> (`.agents/skills/newline-json-protocol/SKILL.md`) and the
> `project-networking-invariants` skill.

The protocol is responsible for:

* newline framing and receive-side line limits (8 KB controller, 1 KB board,
  contract 1.9)
* parsing messages (board side: fixed-capacity buffer, no dynamic allocation)
* validating required fields; rejecting malformed messages safely
* serializing responses consistently
* preserving the distinction between the client's `seq` and the controller-owned
  `board_seq` across request/response boundaries (never conflate them,
  contract 1.1, 3.1)
* the pop-wins resolution helper for unknown/late/duplicate seqs (contract 1.8)

## When unsure

Preserve the architecture invariants first. Ask whether the proposed change
keeps this shape:

```text
local client -> Unix socket -> controller -> TCP -> board
```

and whether it respects the two orthogonal axes (connection state vs
`system.estop_active`). If a change would alter protocol behavior, treat it as a
contract change: update `docs/contracts/V1_Networking_Decisions.md`, the tests,
and this file together, or stop and ask the operator before coding.
