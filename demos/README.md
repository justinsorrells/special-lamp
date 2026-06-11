# Demos

These demos model the frozen v1 topology:

```text
local client -> controller Unix socket -> asyncio controller -> TCP -> mock board
```

The mock board is a standalone TCP server. Local demo clients and the webapp talk
only to the controller Unix socket; they do not connect directly to boards.

## Pieces

- `server/`: contract-faithful TCP mock board. It pushes `schema` on connect,
  emits telemetry, handles commands, accepts out-of-band `estop`, and emits
  `event: estop_ack`.
- `client/`: persistent Unix-socket client for sending `command` and
  `estop_reset` messages to an already running controller.
- `webapp.py`: small FastAPI debugger that sends commands through the controller
  Unix socket.

## Run

Start the mock board:

```sh
python demos/server/server.py --board-id motor --host 127.0.0.1 --port 8767
```

Start the controller with a config whose board endpoint points at
`127.0.0.1:8767` and whose socket path is `/tmp/hyperloop-controller.sock`.

Send a command through the controller:

```sh
python demos/client/client.py --socket-path /tmp/hyperloop-controller.sock \
  --target motor --command move rpm=1200
```

Watch unsolicited events and responses on the same full-duplex local connection:

```sh
python demos/client/client.py --socket-path /tmp/hyperloop-controller.sock --watch
```

Optionally start the webapp:

```sh
python demos/webapp.py --socket-path /tmp/hyperloop-controller.sock
```

## Coverage

`tests/test_demos_contract.py` exercises the demo board and client with the real
controller core, real Unix-socket server, and real TCP board connection layer.
