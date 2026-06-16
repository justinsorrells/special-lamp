# Webapp dashboard (schema-driven GUI)

`demos/webapp_dashboard.py` is a small FastAPI operator GUI over the controller's
Unix socket. Unlike `webapp.py` (a one-shot debug surface), it holds one
persistent connection, builds forms from each board's schema, and renders a live
dashboard.

## Run

Commands go over the controller's Unix socket; **telemetry comes from Redis**
(the controller's observability read-replica), never the command socket. So the
full stack is Redis + a Redis-wired controller + the dashboard:

```sh
# 1. Redis (ephemeral cache is fine)
redis-server --daemonize yes --save "" --appendonly no

# 2. Controller WITH Redis observability (config must set [observability] enabled = true).
#    runtime.py wires redis=None, so use this launcher which passes a real client:
PYTHONPATH=. python demos/run_controller_redis.py --config <cfg.toml> \
    --redis-url redis://127.0.0.1:6379/0

# 3. Dashboard
PYTHONPATH=. python demos/webapp_dashboard.py \
    --socket-path /tmp/hyperloop-controller.sock --redis-url redis://127.0.0.1:6379/0
# open http://127.0.0.1:8000/
```

Flags: `--socket-path` (or `HYPERLOOP_CONTROLLER_SOCKET`), `--redis-url` (or
`HYPERLOOP_REDIS_URL`; pass `""` to disable telemetry), `--host` (default
`127.0.0.1`; use `0.0.0.0` to reach it from another device on the network),
`--port` (default `8000`). Without Redis the dashboard still works — forms,
commands, responses, and board state — just no telemetry panel.

## What it does

- **Schema-driven forms.** Reads `get_schemas` and renders one typed form per
  command. Argument types from the schema drive the inputs: `int`/`float` →
  number, `bool` → true/false select, `string` → text. Submitted strings are
  coerced to the declared type server-side before sending; invalid input returns
  `BAD_INPUT` and is never forwarded to a board.
- **Telemetry (from Redis).** A background reader tails each board's
  `board:telemetry:<id>` stream (`XREVRANGE … COUNT 1`) and shows the latest
  values plus the controller-measured rate / interval / jitter. Telemetry values
  also merge into the live-values store (source `telemetry:<id>`).
- **Live values (overwrite by field).** Every response and telemetry frame upserts
  a flat store keyed by bare field name, so common fields (`board_proc_us`,
  `latency_ms`, `echo_value`, …) collapse to one always-current row instead of a
  growing log. Internal `get_schemas` polls are excluded.
- **Board status** is polled via `get_schemas` once per second.
- **Auto `get_counters` (1 s)** checkbox drives the live panel continuously.
- **`estop_reset`** button sends the controller an `estop_reset`.

## HTTP API

| Method | Path | Body | Purpose |
|---|---|---|---|
| GET | `/` | — | HTML dashboard |
| GET | `/api/schema` | — | boards + per-command arg specs |
| GET | `/api/state` | — | board status, live values, recent events |
| POST | `/api/command` | `{target, command, args}` | send a command, return the response |
| POST | `/api/estop_reset` | — | send `estop_reset` to the controller |

## Architecture / boundaries

The GUI never talks to boards directly and never uses UDP. It uses two sources,
matching the stack's separation of concerns:

- **Command path → Unix socket.** Commands, responses, `estop_reset`, board
  state (`get_schemas`). The socket deliberately does not carry the 50 ms
  telemetry stream.
- **Observability → Redis.** Board telemetry is mirrored by the controller into
  capped `board:telemetry:<id>` streams; the dashboard reads them. This is the
  designed fan-out for any number of read-only observers and keeps telemetry off
  the command path. Redis is observability only — never the command path.

Note: `runtime.py` constructs the controller with `redis=None` (no mirroring),
so use `demos/run_controller_redis.py` (or otherwise pass a client to
`create_runtime(..., redis=...)`) and enable `[observability]` to populate Redis.
