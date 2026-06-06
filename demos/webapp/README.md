# Webapp Demo

## Purpose

`demos/webapp.py` is an early FastAPI sketch for a browser-facing debugger. It
queries demo UDP servers for schema information and renders HTML buttons for
commands.

The file currently lives at `demos/webapp.py`; this README is placed under
`demos/webapp/` only to match the demo documentation layout requested for this
repository.

## How To Run It Locally

Install dependencies from `requirements.txt` if needed, then start a UDP demo
server:

```sh
python demos/server/server.py
```

Start the webapp from the repository root:

```sh
python demos/webapp.py
```

Open `http://127.0.0.1:8000/`.

## Expected Behavior

The index route sends an `info` request to the UDP demo server at
`127.0.0.1:6767`, renders the raw response, and attempts to render command
buttons from cached schema data.

The command button path is prototype-only and is not currently production-ready:
it references an undefined `webAppClientSocket`, indexes `HOSTPORTS` with a
string path parameter, and sends a raw string instead of the server's expected
JSON command packet. Treat the route as a sketch of intended UI behavior rather
than a working command path.

## Important Files

- `demos/webapp.py`: FastAPI application, schema query helper, index route, and
  prototype command route.
- `requirements.txt`: FastAPI and Uvicorn dependencies used by the webapp demo.

## Architecture Concept It Demonstrates

This demo demonstrates the desire for an operator/debug UI that can discover
available subsystem commands and present them as browser controls.

The production version of that concept should communicate with the controller
over the local Unix socket, not directly with boards or UDP demo servers.

## Differences From The Frozen V1 Contract

- Talks directly toward UDP demo servers instead of using the controller's Unix
  socket.
- Uses connect-per-request UDP schema queries instead of a persistent full-duplex
  local client connection.
- Does not distinguish client-facing `response` messages from unsolicited
  `event` messages.
- Does not use the v1 command shape, sequence discipline, structured errors, or
  terminal status values.
- Does not observe controller-authoritative board state, Redis-mirrored state,
  e-stop events, or per-board connection state.
- Does not enforce schema-driven e-stop gating or operator `estop_reset`
  behavior.

## What Future Production Code May Reuse

- The general UI concept of discovering commands and rendering operator/debug
  controls from schema metadata.
- The use of FastAPI as a lightweight local debugging surface, if it remains
  behind the controller boundary.
- The pattern of showing recent command output while navigating back to the
  command list.

Production UI code should consume controller responses/events and state updates
through the v1 local-client interface.

## What Production Code Must Not Copy Directly

- Direct network access from the webapp to boards.
- UDP schema polling.
- Raw HTML string assembly for command execution paths.
- Global mutable state as the only command-result store.
- Undefined or incomplete command-sending logic.
- Any route that bypasses the controller as the single authority for board
  communication.

