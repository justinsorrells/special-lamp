# Client Demo

## Purpose

`demos/client/client.py` is an early asyncio prototype for talking to one or
more UDP demo boards and exposing a small Unix socket command adapter at
`/tmp/chudmail`.

It demonstrates polling a board-like UDP server, requesting an `info` schema,
queueing simple commands, and accepting local JSON requests that enqueue work.

## How To Run It Locally

Start the UDP demo server first:

```sh
python demos/server/server.py
```

Then run the client demo from the repository root:

```sh
python demos/client/client.py
```

The client creates `/tmp/chudmail`. A local one-shot request can be sent with a
Unix socket client, for example:

```sh
printf '{"client":1,"cmd":"hello","args":["demo"]}\n' | nc -U /tmp/chudmail
```

Use this command to ask for known board schemas:

```sh
printf '{"client":1,"cmd":"boards"}\n' | nc -U /tmp/chudmail
```

## Expected Behavior

The client periodically sends UDP JSON commands to `127.0.0.1:6767`, prints the
latest response, and updates its cached schema after receiving a successful
`info` response.

Local Unix socket requests receive immediate prototype responses such as
`{"status":"accepted"}` or `{"status":"rejected","reason":"unknown client"}`.
The actual board command is processed later by the polling loop.

## Important Files

- `demos/client/client.py`: UDP client class, command queue, polling loop, and
  temporary Unix socket adapter.

## Architecture Concept It Demonstrates

This demo sketches the idea of a local process that owns board communication and
accepts commands from local applications over a Unix socket.

That concept is aligned with the production topology at a high level, but the
implementation is not the v1 controller.

## Differences From The Frozen V1 Contract

- Uses UDP instead of persistent TCP.
- Uses `sequence_no`, `cmd`, `status_code`, and list-style `args` instead of the
  frozen v1 message fields.
- Polls `info` as a command to discover schema; v1 boards push `schema` on every
  connect/reconnect.
- Local socket clients are one-shot request/response connections, not
  persistent full-duplex clients that receive unsolicited `event` messages.
- Uses one queue per UDP client but does not implement the v1 bounded per-board
  FIFO depth, `BOARD_BUSY`, queue residency cap, or one-command-in-flight
  `board_seq` discipline.
- Timeouts are simple UDP receive timeouts, not separate queue-residency and
  execution clocks with a 10 s ceiling.
- No e-stop latch, schema-driven command gating, out-of-band `estop`, writer
  serialization, or `estop_ack` tracking.
- Removes `/tmp/chudmail` unconditionally if it exists; v1 must check whether an
  existing socket belongs to a live controller before unlinking it.

## What Future Production Code May Reuse

- The general idea of a controller-side adapter accepting local Unix socket
  requests.
- The use of asyncio for the local socket server and non-blocking network I/O.
- The rough pattern of caching board capabilities after learning them.

Any reuse should be reimplemented against the frozen v1 protocol, state model,
and controller interfaces rather than copied directly.

## What Production Code Must Not Copy Directly

- UDP board transport.
- The `info` command as schema discovery.
- The one-shot local socket behavior.
- The `sequence_no` and `status_code` message format.
- Unbounded command queue behavior.
- Unconditional stale-socket deletion.
- Generic broad exception handling as the main protocol error path.

