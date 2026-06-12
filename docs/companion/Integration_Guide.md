# Integration Guide

> **Authority.** This is companion material for integrators. The frozen contract
> `docs/contracts/V1_Networking_Decisions.md` is authoritative for controller and
> networking behavior, and `docs/contracts/Board_Developer_Guide.md` is
> authoritative for firmware-facing behavior. If this file disagrees with either
> contract, the frozen contracts win.

Use this guide when wiring the v1 stack into a local application, a mock board,
or a deployed mini-PC controller.

## Startup Shape

```text
local client -> Unix socket -> asyncio controller -> persistent TCP -> board
```

1. Start the controller with a static list of expected board IDs and board TCP
   endpoints.
2. The controller creates the Unix socket for local clients with owner-only
   permissions.
3. Each board listens as a TCP server.
4. The controller connects to each board and waits for the board-pushed schema.
5. A board becomes commandable only after it reaches `REGISTERED`.

Do not add a GUI-to-board path and do not put Redis between a client request and
the board.

## Adding a Board

1. Pick a stable board ID and add it to controller configuration.
2. Make the board push a schema as the first message on every connect.
3. Keep the schema under the controller receive limit and keep inbound command
   lines under the board receive limit.
4. Set `protocol_version` to the coordinated v1 value.
5. Include `blocked_by_estop` for every command. Missing means blocked.
6. Push telemetry from the board on its own timer. Do not wait for polling.
7. Implement local safe-state behavior for `estop` and controller loss.
8. Emit `event: estop_ack` with `details: {"state":"safe"}` only after local
   safe state is actually applied.

## Adding a Command

1. Register the command in the board schema.
2. Keep arguments small, flat, and typed with simple scalar names.
3. Mark motion or actuator-changing commands with `blocked_by_estop: true`.
4. Mark diagnostic/status reads with `blocked_by_estop: false` only when they
   are safe during e-stop.
5. Return quickly from board handlers. Long movement should start and report
   progress through telemetry or state, not block the command response.
6. Expect one in-flight command per board. Additional commands wait in the
   bounded FIFO or fail with `BOARD_BUSY`.

## Local Clients

- Local clients connect once to the Unix socket and stay connected.
- Clients must read continuously because the socket is full-duplex.
- Request-correlated replies are `response` messages using the client's original
  `seq`.
- Server-initiated notifications are `event` messages and may arrive without a
  matching request.
- Clients should treat e-stop and safety events as critical and update their UI
  from controller events, not by talking directly to boards.
- Clients discover configured boards and accepted board schemas with the
  controller-local `command: "get_schemas"` API documented in
  `Local_Client_API.md`.
- Client request lines are capped at the controller receive limit, 8 KiB.
  Schema discovery responses may be larger; clients should configure their
  stream reader to accept controller-to-client response lines of at least 64 KiB.

## E-Stop Operation

- Software e-stop is convergence only. The hardwired interlock and power cut are
  the safety layer.
- The controller sets the global `system.estop_active` latch on the first
  trigger and does not auto-clear it.
- Queued gated commands fail with `ESTOP_ACTIVE`; in-flight commands resolve
  naturally.
- The controller writes `estop` to connected boards out-of-band through the same
  per-board serialized writer path used by normal commands.
- A reconnecting board can reach `REGISTERED` while the global latch is still
  active. The controller re-sends `estop`, resets that board's `estop_ack` to
  false, and dispatch stays gated by the global latch.
- Reset requires an operator `estop_reset` after the electrical condition is
  cleared.

## Redis and Observability

- Redis is optional observability infrastructure.
- Redis mirrors current controller-owned state and receives capped telemetry and
  lifecycle history.
- Redis outage, latency, or write failure must not block command dispatch,
  e-stop propagation, local client events, or shutdown.

## Shutdown

1. Stop accepting new local requests.
2. Allow a bounded drain window for in-flight commands.
3. Resolve remaining queued or pending commands with `CONTROLLER_SHUTDOWN`.
4. Close local client and board streams with bounded waits.
5. Cancel background tasks and flush observability best-effort.

Shutdown must be idempotent and must not add a new terminal status.
