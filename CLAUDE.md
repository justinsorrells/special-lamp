# CLAUDE.md

Hyperloop board networking stack. This file is a pointer; the real rules live
elsewhere — read them before changing code.

## Sources of truth (in order)
1. `docs/contracts/V1_Networking_Decisions.md` — **frozen** networking/controller
   contract. It wins on any conflict.
2. `docs/contracts/Board_Developer_Guide.md` — firmware-facing behavior.
3. `AGENTS.md` — navigation aid + invariants + build order.
4. `.agents/skills/*/SKILL.md` — execution rules per area. Read the relevant
   skill(s) before working: `project-networking-invariants`, `asyncio-controller`,
   `newline-json-protocol`, `code-style-and-conventions`,
   `testing-async-loops-and-mocks`, `graceful-shutdown-and-lifecycle`,
   `redis-telemetry-and-metrics`, `unix-sockets-and-backpressure`.

## Critical invariants (do not break without an explicit contract change)
- **Single authority:** the controller owns all board communication; the GUI
  never talks to boards directly.
- **Two orthogonal axes:** connection state vs. `system.estop_active` — never
  merge them into one field.
- **Terminal statuses are exactly `{ok, error, timeout}`** — new failure modes
  are error *codes*, never new statuses.
- **Redis is observability/read-replica only** — never in the command path,
  never a source of truth.

## Verification (run from the repo root)
```bash
python -m pytest                  # full suite
python tools/check_invariants.py  # static architecture guardrails
ruff check .                      # lint
mypy .                            # type check
```
Tests are stdlib `unittest`, imported as top-level modules — always run from the
repo root.
