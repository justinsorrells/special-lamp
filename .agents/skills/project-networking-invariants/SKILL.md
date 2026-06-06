---
name: project-networking-invariants
description: >
  Read before any change to the Hyperloop board networking stack. Encodes the
  frozen topology, the two orthogonal state axes (connection state vs e-stop
  state), the e-stop convergence model, and the terminal-status rules. Use this
  for controller, protocol, board, GUI, or observability work, and any time a
  change might touch architecture shape or board state.
---

# Project Networking Invariants

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md`
> (v3 FROZEN) is the source of truth. For firmware-specific behavior, defer to
> `docs/contracts/Board_Developer_Guide.md`. This skill summarizes the invariants an
> agent must not break. Where this skill is silent, defer to the contract; where
> it conflicts, the contract wins. Parenthetical refs like (1.13) point at the
> governing contract decision.

## Topology (frozen)

```
local client -> Unix socket -> asyncio controller -> persistent TCP -> board
```

* The controller is the single authority for board communication.
* The GUI / local clients never talk directly to boards.
* The local socket is full-duplex: clients receive unsolicited `event` messages
  as well as request-correlated `response`s (1.14).
* Board transport is TCP, newline-delimited JSON, persistent connections.

### Board-as-TCP-server (frozen decision)
* **The board listens. The controller connects to the board.**
* The board **pushes its schema** as the first message on connect and on every
  reconnect (1.3). There is no `get_schema` command.
* An unknown `board_id` at connect is rejected; a `protocol_version` mismatch
  FAULTs the board and it is not registered (1.16).

## Two orthogonal state axes (do not merge)

Connection state and safety state are **independent**. Never collapse them into a
single enum or field (2).

* **Connection axis (per board):**
  `DISCONNECTED -> CONNECTING -> CONNECTED -> REGISTERED`, with `FAULTED`
  returning to reconnect. (2.1)
* **Safety axis (global):** `system.estop_active` (bool, latched) plus per-board
  `estop_ack` (bool, observability only). (2.2)

A board can be `DISCONNECTED` while the system is in e-stop; a board can be
`REGISTERED` while `estop_active` blocks its commands. One enum cannot hold both
facts.

## Single authority for board state

* The controller's in-memory per-board object is authoritative.
* Redis is a **read-replica**: a hash `board:state:<id>` for current value plus a
  `board:state:updates` pub/sub channel for deltas. Never a source of truth (1.15).
* Redis being down must not affect board control or e-stop propagation (1.7).

## E-stop is convergence only

* **The hardwired interlock and power cut are the safety layer. Software e-stop
  is convergence/coordination and must never present itself as the guarantee** (1.13).
* `system.estop_active` is a single global latch. It is set on an
  `estop_triggered` event and cleared **only** by an operator `estop_reset` with
  the electrical condition cleared. **Do not auto-clear it. Do not gate the latch
  on per-board acks** (1.13, 2.2).
* While `estop_active`, command gating is **schema-driven**: reject commands with
  `blocked_by_estop=true` (or absent -> blocked) with `ESTOP_ACTIVE`; allow
  commands explicitly marked `blocked_by_estop=false` (status/fault/sensor reads)
  (1.17).
* `estop` is written **out-of-band** (bypasses the FIFO and the in-flight slot)
  but **must acquire the same per-board writer lock** as any other write (1.19).
* A board confirms safe state with an unsolicited `event: estop_ack`
  (`{"state":"safe"}`); this sets `board.estop_ack` but is observability, not a
  gate (1.20).

## Terminal statuses

Every command resolves to exactly one terminal `status` (3.12):

```
ok | error | timeout
```

Rejections, disconnects, busy, unavailable, e-stop, shutdown are **error codes**,
not statuses (3.11):

```
INVALID_JSON  MISSING_FIELD  INVALID_TYPE  UNKNOWN_TARGET  UNKNOWN_COMMAND
INVALID_ARGUMENT  BOARD_UNAVAILABLE  BOARD_BUSY  COMMAND_TIMEOUT  ESTOP_ACTIVE
PROTOCOL_VERSION_MISMATCH  CONTROLLER_SHUTDOWN  INTERNAL_ERROR
```

**Do not invent new status values; add an error code instead. Never silently
drop a command** (1.8).

## Robustness invariants

* Malformed messages must never crash the controller or the board (1.9).
* Board disconnects are handled explicitly; pending commands fail with
  `BOARD_UNAVAILABLE` (1.11).
* Telemetry is one-way push from the board (~50 ms), never solicited (1.6).

## Build order (greenfield)

Implement the shared contracts first; everything builds on them (contract 5):
`protocol.py`, then `state.py`, then `interfaces.py`.

## When unsure

Preserve invariants first. Confirm the change keeps the topology shape above and
respects the two orthogonal axes. If it would alter protocol behavior, treat it
as a contract change: update `docs/contracts/V1_Networking_Decisions.md`, the
relevant tests, and AGENTS.md together, or stop and ask the operator before
coding.
