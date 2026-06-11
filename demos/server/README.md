# TCP Mock Board

`demos/server/server.py` is a contract-faithful mock board for local controller
testing. It is an asyncio TCP server because v1 boards listen and the controller
connects.

Behavior:

- Sends a `schema` message first on every connection.
- Includes `protocol_version` and per-command `blocked_by_estop`.
- Pushes telemetry as unsolicited newline-JSON messages.
- Handles `command`, `estop`, and optional `heartbeat` messages.
- Emits unsolicited `event: estop_ack` after applying its local safe state.

Run it from the repo root:

```sh
python demos/server/server.py --board-id motor --host 127.0.0.1 --port 8767
```

The controller config should point the `motor` board endpoint at that host and
port. Local clients should still talk only to the controller Unix socket, never
to this mock board directly.
