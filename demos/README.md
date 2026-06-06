# Demos

These demos are reference material only. They are useful for understanding early
experiments around command messages, schema lookup, and simple operator-facing
tools, but they are not authoritative implementation guidance.

The authoritative sources for production behavior are:

- `docs/contracts/V1_Networking_Decisions.md`
- `docs/contracts/Board_Developer_Guide.md`
- `AGENTS.md`
- `.agents/skills/*/SKILL.md`

The frozen v1 architecture is:

```text
local client -> Unix socket -> asyncio controller -> TCP -> board
```

The current demos do not fully model that architecture. They are earlier
prototypes that use UDP board communication, connect-per-command behavior in
places, ad hoc message fields, and direct webapp-to-board access. Treat them as
historical sketches, not as code to copy into the production controller.

## Demo Inventory

- `client/`: asyncio UDP client plus a temporary Unix socket command adapter.
- `server/`: blocking UDP command server with a small callable schema.
- `webapp.py`: FastAPI sketch that queries UDP demo servers and renders command
  buttons. Its documentation lives in `webapp/README.md`.

## How to Run The Prototype Set

From the repository root, start the UDP demo server:

```sh
python demos/server/server.py
```

In another terminal, start the demo client/controller sketch:

```sh
python demos/client/client.py
```

Optionally start the webapp sketch:

```sh
python demos/webapp.py
```

The webapp requires the packages in `requirements.txt`, including FastAPI and
Uvicorn.

## Local-Loop Integration Demo/Test

The contract-aligned local loop is exercised by an integration test rather than
the older prototype demos. It starts a fake local client, the real Unix socket
server, the real controller core, the real board TCP connection layer, and an
asyncio fake board TCP server:

```text
fake local client -> Unix socket -> real controller -> TCP -> fake board server
```

Run it from the repository root:

```sh
python3 -m unittest tests.test_local_loop_integration
```

The test covers one successful command round trip, newline JSON framing on both
socket hops, distinct client `seq` vs controller-owned `board_seq`, malformed
local input, board disconnect, board timeout, late board response drop,
`BOARD_BUSY`, and the e-stop command gate.

## Architecture Status

The demos currently model earlier prototypes, not the final intended
architecture. In particular:

- Board communication is UDP, not persistent TCP.
- The demo server acts like a UDP board/server, not a Teensy TCP server that
  pushes schema on connect.
- The client demo has a Unix socket adapter, but local clients are
  connect-per-command and not full-duplex persistent clients.
- The webapp talks toward UDP demo servers directly instead of going through the
  controller's Unix socket.
- Message fields use `sequence_no`, `cmd`, `args`, and `status_code` rather than
  the frozen v1 `seq`, `command`, `args`, `status`, and structured `error`
  contract.

## Contract Gaps Summary

- Transport differs: UDP prototypes vs v1 newline-delimited JSON over Unix
  socket and persistent TCP.
- Topology differs: webapp/direct UDP access vs controller-owned board
  communication.
- Schema differs: request/response `info` command vs board-pushed `schema` on
  connect/reconnect.
- State differs: no controller-authoritative per-board connection state, no
  separate global `system.estop_active`, and no per-board `estop_ack`.
- Command lifecycle differs: no bounded per-board FIFO, no one-in-flight rule
  aligned to `board_seq`, no queue residency cap, and no pop-wins pending-table
  resolution.
- Error model differs: integer `status_code` and prototype strings vs frozen
  statuses `ok`, `error`, `timeout` with contract error codes.
- E-stop behavior is absent: no latch, no schema-driven gating, no out-of-band
  serialized `estop` write, and no `estop_ack` event tracking.
- Robustness differs: malformed messages are caught in places, but line limits,
  structured validation, disconnect handling, and local-client backpressure do
  not match v1.
