# Server Demo

## Purpose

`demos/server/server.py` is an early blocking UDP server that behaves like a
simple board simulator. It exposes a small Python function schema, validates
argument binding with `inspect.signature`, executes commands, and returns JSON
responses.

## How To Run It Locally

From the repository root:

```sh
python demos/server/server.py
```

The server binds UDP `0.0.0.0:6767`. In another terminal, run the client demo:

```sh
python demos/client/client.py
```

You can also send a direct UDP packet with a short script:

```sh
python -c 'import json,socket,time; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.settimeout(1); p={"type":"command","sequence_no":1,"timestamp":time.time(),"cmd":"hello","args":["demo"]}; s.sendto(json.dumps(p).encode(),("127.0.0.1",6767)); print(s.recvfrom(1024)[0].decode())'
```

## Expected Behavior

The server prints received command names and replies with JSON. The `info`
command returns a public schema containing available functions and a hostname.
The `hello` command prints and returns a greeting. Unknown commands or invalid
arguments return a response with `status_code: 500` and an error string.

## Important Files

- `demos/server/server.py`: UDP server, function registry, schema generation,
  command evaluation, and response generation.
- `demos/server/schema_ideas.txt`: early note listing response fields considered
  during prototype work.

## Architecture Concept It Demonstrates

This demo demonstrates a board-like endpoint with command names, argument
metadata, and JSON request/response behavior.

It is useful as a conceptual sketch for schema-backed command validation, not as
the production board transport or firmware protocol.

## Differences From The Frozen V1 Contract

- The server is UDP, while v1 board communication is persistent TCP with
  newline-delimited JSON.
- It is a blocking socket loop, while production controller paths must use
  asyncio and firmware must follow the board contract.
- Schema is returned by an `info` command, while v1 boards push a `schema`
  message first on connect and again on reconnect.
- Uses `sequence_no`, `cmd`, `status_code`, and `result` shapes that differ from
  v1 `seq`, `command`, `status`, structured `error`, and `result.board_seq`.
- Does not include `protocol_version`, `blocked_by_estop`, telemetry schema, or
  state schema fields required by v1 schema handling.
- Does not push telemetry at ~50 ms.
- Does not implement e-stop handling or unsolicited `event: estop_ack`.
- Does not enforce receive-side line limits, because UDP datagrams are used
  instead of newline framing over streams.

## What Future Production Code May Reuse

- The idea of a command registry with argument metadata.
- The use of callable signatures as a local simulation aid for validating demo
  command arguments.
- The distinction between public schema metadata and private callable objects.

Production code should translate those ideas into the v1 `schema` message and
contract error model.

## What Production Code Must Not Copy Directly

- Blocking UDP server loop.
- The `info` command as schema exchange.
- Integer `status_code` handling.
- Prototype message fields such as `sequence_no` and `cmd`.
- Dynamic Python callable execution as a model for firmware command dispatch.
- Broad exception handling that collapses protocol errors into ad hoc strings.

