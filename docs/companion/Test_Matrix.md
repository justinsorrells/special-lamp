# Companion Test Matrix

> **Authority.** This is companion material for implementers and reviewers. The
> frozen contract `docs/contracts/V1_Networking_Decisions.md` is authoritative
> for controller and networking behavior, and
> `docs/contracts/Board_Developer_Guide.md` is authoritative for firmware-facing
> behavior. If this file disagrees with either contract, the frozen contracts win.

This matrix lists boundary cases that should stay covered as the implementation
and companion guides evolve.

## Protocol and Framing

- Newline-delimited JSON: exactly one JSON object per line.
- Controller receive limit: 8 KB.
- Board receive limit: 1 KB.
- Oversized input is rejected or discarded before JSON parsing according to the
  receiver.
- Large schema payloads over 1 KB but under 8 KB are valid board-to-controller
  messages.
- Malformed JSON, missing fields, and wrong types return structured errors and
  do not crash the controller or board.
- Terminal statuses remain exactly `ok`, `error`, and `timeout`.

## Sequence and Resolution

- Client `seq` is preserved in client-facing responses.
- Controller-owned `board_seq` is distinct from client `seq`.
- Two clients may use the same client `seq` while their board-facing
  `board_seq` values remain unique per board.
- Unknown, duplicate, and late board responses use pop-wins resolution and are
  dropped/logged without double-resolution.
- `controller_ts` is monotonic and used only for round-trip latency in the same
  controller process.

## Command Dispatch

- Commands to boards not in `REGISTERED` fail with `BOARD_UNAVAILABLE`.
- FIFO overflow fails the newest command with `BOARD_BUSY`.
- Queue residency expiry fails with `COMMAND_TIMEOUT` and is never written to
  the board.
- Execution timeout starts after write to the board and fails with
  `COMMAND_TIMEOUT`.
- No timeout override may exceed the 10 second hard ceiling.
- Board disconnect drains pending and queued commands with `BOARD_UNAVAILABLE`.

## E-Stop

- First `estop_triggered` latches `system.estop_active`.
- The latch is not auto-cleared and is not gated on any board's `estop_ack`.
- Queued gated commands fail with `ESTOP_ACTIVE`.
- In-flight commands resolve naturally after e-stop.
- Commands with `blocked_by_estop: false` remain dispatchable during e-stop.
- Missing `blocked_by_estop` defaults to blocked.
- Out-of-band `estop` bypasses FIFO and in-flight ownership but uses the same
  per-board writer lock as command writes.
- Reconnect during e-stop re-sends `estop` and resets the board's `estop_ack`
  until an unsolicited `event: estop_ack` with `details: {"state":"safe"}`
  arrives.

## Local Clients and Observability

- Local clients receive both `response` and unsolicited `event` messages.
- A disconnected client does not cancel an already written board command.
- Slow local clients cannot stall command routing or event delivery to other
  clients.
- Non-critical client events are drop-oldest on overflow.
- Responses and e-stop/safety events are never silently dropped.
- Redis queue overflow is drop-oldest and increments `obs_dropped`.
- Redis failure does not affect command routing, e-stop propagation, or state in
  controller memory.

## Lifecycle

- New requests during shutdown fail with `CONTROLLER_SHUTDOWN`.
- In-flight commands may complete during the bounded drain window.
- Remaining pending and queued commands resolve with `CONTROLLER_SHUTDOWN`.
- Shutdown closes streams, cancels background tasks, and flushes Redis
  best-effort without hanging.
- Late board responses after shutdown do not double-resolve a command.
