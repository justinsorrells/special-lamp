# Hyperloop Board Networking Stack

The mini-PC controller and v1 networking layer for a Hyperloop pod's boards. A
single Python `asyncio` controller is the sole authority for board communication:
local applications (GUI/clients) talk to it over a Unix socket, and it maintains
persistent TCP connections to the boards.

## Topology (frozen)

```text
GUI / local client            board/controller events
    -> Unix socket                -> controller
    -> asyncio controller         -> Redis / logs / observability
    -> persistent TCP             (telemetry is one-way push from boards)
    -> board
```

- The controller owns all board communication; the GUI never talks to boards.
- Boards are TCP **servers**; the controller connects and the board pushes its
  schema on connect/reconnect.
- The wire format is newline-delimited JSON.
- Redis is observability / a read-replica only — never the command path, never a
  source of truth for board state.
- Connection state and e-stop state are orthogonal axes; terminal command
  statuses are exactly `ok` / `error` / `timeout` (new failure modes are error
  *codes*, not new statuses).

## Layout

| Path | Purpose |
|---|---|
| `protocol.py` | Message vocabulary, validation, framing, seq helpers, pop-wins |
| `state.py` | Connection/system state enums and records |
| `interfaces.py` | Injection seams (Protocols), serialized writer, e-stop path |
| `controller.py` | Dispatch, per-board FIFO, timeouts, e-stop gate |
| `board_connection.py` | Board TCP connect/read loops |
| `local_socket.py` | Local client Unix socket server + backpressure |
| `observability.py` | Bounded drop-oldest queue + best-effort Redis writer |
| `demos/` | Mock board server, mock client, and a status webapp |
| `tools/check_invariants.py` | Static architecture-invariant checker |
| `docs/contracts/` | **Frozen** networking + firmware contracts (source of truth) |
| `docs/companion/` | Subordinate implementation handoffs, integration guide, and test matrix |
| `.agents/skills/` | Per-area execution rules for agents |

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt        # runtime deps (FastAPI/uvicorn for the demo webapp)
pip install -r requirements-dev.txt    # pytest, ruff, mypy
```

## Verify (run from the repo root)

```bash
python -m pytest                  # full test suite
python tools/check_invariants.py  # static architecture guardrails
ruff check .                      # lint
mypy .                            # type check
```

Tests are stdlib `unittest` (with `IsolatedAsyncioTestCase` for async) and import
modules as top-level names, so always run them from the repo root.

## Where the rules live

- `docs/contracts/V1_Networking_Decisions.md` — the **frozen** authoritative
  contract. It wins on any conflict.
- `AGENTS.md` — navigation aid, invariants, and build order.
- `CLAUDE.md` — quick pointer + verification commands for agents.
- `.agents/skills/*/SKILL.md` — read the relevant skill before working in an area.
