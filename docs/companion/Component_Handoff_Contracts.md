# Component Handoff Contracts

> **Authority.** This is companion material for implementers. The frozen contract
> `docs/contracts/V1_Networking_Decisions.md` is authoritative for controller and
> networking behavior, and `docs/contracts/Board_Developer_Guide.md` is
> authoritative for firmware-facing behavior. If this file disagrees with either
> contract, the frozen contracts win.

This file describes ownership boundaries between repository modules. It is a
handoff guide, not a protocol extension.

## Shared Primitive Layer

`protocol.py`

- Owns newline-delimited JSON framing, receive-side line limits, message
  validation, terminal status and error-code vocabularies, client `seq` versus
  controller-owned `board_seq`, and the pop-wins helper.
- Does not open sockets, import Redis, route commands, or maintain board state.
- Must keep terminal statuses to `ok`, `error`, and `timeout`.

`state.py`

- Owns in-memory value/state shapes: `BoardConnState`, `SystemState`,
  `BoardState`, `PendingCommand`, `BoardStateRecord`, and `SystemStateRecord`.
- Keeps connection state separate from safety state:
  `conn_state`, `system.estop_active`, and `board.estop_ack` are distinct
  fields.
- Redis records are mirror schemas only. They are never source-of-truth objects.

`interfaces.py`

- Owns dependency seams shared by controller components: local client reply
  handles, board-down notification, and board writer handles.
- Owns the shared serialized writer boundary used by both command writes and
  out-of-band `estop` writes.
- `send_estop()` bypasses command FIFO and the in-flight command slot, but not
  the writer lock.

## Runtime Components

`controller.py`

- Owns dispatch decisions, per-board FIFO behavior, one command in flight per
  board, e-stop gating, pending command resolution, command timeout handling,
  queue residency rejection, and command lifecycle counters.
- Rejects commands to unregistered boards with `BOARD_UNAVAILABLE`.
- Rejects FIFO overflow with `BOARD_BUSY`.
- Rejects stale queued commands and execution timeouts with `COMMAND_TIMEOUT`.
- Rejects gated commands during e-stop with `ESTOP_ACTIVE`.
- Resolves pending commands by pop-wins: no `await` between pending lookup and
  removal.
- Receives observability through injection. It must not import Redis or make
  Redis part of command completion.

`board_connection.py`

- Owns persistent TCP connections to statically configured board endpoints. The
  board is the TCP server; the controller connects.
- Requires schema as the first message on every connect or reconnect.
- Moves a board to `REGISTERED` only after schema validation and protocol-version
  match.
- Emits `board_down(board_id)` on disconnect or read failure so the dispatcher
  can fail pending and queued commands with `BOARD_UNAVAILABLE`.
- Tracks pushed telemetry and event messages as unsolicited board input, not as
  command responses.
- Re-sends `estop` after a reconnect while `system.estop_active` is latched and
  tracks `estop_ack` only after the board's unsolicited `event: estop_ack`.

`local_socket.py`

- Owns the full-duplex Unix socket boundary for local clients.
- Clients send requests and continuously receive both `response` messages and
  unsolicited `event` messages.
- Per-client outbound queues are bounded. Non-critical event overflow drops the
  oldest non-critical event; responses and e-stop/safety events are not silently
  dropped.
- The local socket layer never imports board TCP code for direct GUI-to-board
  communication.

`observability.py`

- Owns best-effort telemetry, lifecycle records, Redis mirroring, and metrics
  export.
- Uses a single bounded observability queue with drop-oldest overflow.
- Redis failures are counted/logged and do not affect command routing, e-stop
  propagation, or shutdown progress.
- State hashes and update messages mirror controller-owned in-memory state.

## Failure Ownership

| Failure | Owner | Terminal response |
|---|---|---|
| Malformed local request | `local_socket.py` + `protocol.py` | `error` with `INVALID_JSON`, `MISSING_FIELD`, or `INVALID_TYPE` |
| Unknown target board | `controller.py` | `error` with `UNKNOWN_TARGET` or `BOARD_UNAVAILABLE` |
| Board not registered | `controller.py` | `error` with `BOARD_UNAVAILABLE` |
| FIFO full | `controller.py` | `error` with `BOARD_BUSY` |
| Queue residency expired | `controller.py` | `timeout` with `COMMAND_TIMEOUT` |
| Execution timeout | `controller.py` | `timeout` with `COMMAND_TIMEOUT` |
| Board disconnect | `board_connection.py` reports, `controller.py` resolves | `error` with `BOARD_UNAVAILABLE` |
| Protocol version mismatch | `board_connection.py` + `protocol.py` | board not registered; logged with `PROTOCOL_VERSION_MISMATCH` |
| E-stop gate | `controller.py` | `error` with `ESTOP_ACTIVE` |
| Shutdown after drain | controller lifecycle path | `error` with `CONTROLLER_SHUTDOWN` |
| Redis unavailable | `observability.py` | no command response impact |

## Boundary Rules

- Protocol parsing stays separate from command routing.
- Board connection management stays separate from local Unix socket handling.
- Command FIFO is reject-newest; observability and non-critical client events
  are drop-oldest.
- Pushed telemetry is a liveness signal and must not be solicited as a command.
- The global e-stop latch is not gated by per-board acks and is not auto-cleared.
