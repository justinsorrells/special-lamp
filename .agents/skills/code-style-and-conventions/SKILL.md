---
name: code-style-and-conventions
description: >
  Read before writing or modifying any Python in the Hyperloop board networking
  stack. Codifies the repo's actual idioms: future annotations, StrEnum closed
  vocabularies, PEP-604 unions, frozen dataclasses for value objects and Protocol
  classes for injection seams, module separation and dependency direction, async
  rules, compact-JSON serialization, module-level logging, and the run-from-root
  unittest test style. Use for any code change so new code reads like the
  surrounding code.
---

# Code Style and Conventions

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md`
> (v3 FROZEN) and `AGENTS.md` govern *what* the code must do; this skill captures
> *how* the code is written so a change reads like the surrounding code. On any
> conflict the contract wins. For area-specific rules see the other skills
> (`asyncio-controller`, `newline-json-protocol`, `project-networking-invariants`,
> `testing-async-loops-and-mocks`, `graceful-shutdown-and-lifecycle`,
> `redis-telemetry-and-metrics`, `unix-sockets-and-backpressure`).

Target runtime is **Python 3.14**. The codebase is stdlib-first: `asyncio`,
`dataclasses`, `enum`, `typing`. The only third-party runtime deps (FastAPI /
uvicorn / pydantic) are confined to `demos/webapp.py`.

## Language idioms

* **`from __future__ import annotations`** is the first import in every module.
* **Closed string vocabularies are `enum.StrEnum`** (`MessageType`,
  `TerminalStatus`, `ErrorCode`). Adding a value is a contract decision. New
  failure modes are **error codes, never new terminal statuses** — the set stays
  `{ok, error, timeout}`, enforced by `tools/check_invariants.py`.
* **PEP-604 unions and builtin generics**: `int | float`, `X | None`,
  `dict[str, Any]`, `list[...]`. No `Optional`/`Union`/`Dict` from `typing`.
* **Keyword-only arguments** (`*,`) for optional/boolean/config parameters so
  call sites stay self-documenting (see `build_*` helpers in `protocol.py`).

## Data modelling

* **`@dataclass(frozen=True)` for value objects** — immutable messages/results
  (`ErrorObject`, `ParseResult`, `_OutboundMessage`). Mutable `@dataclass` for
  stateful objects (`ObservabilityCounters`, connection/state records).
* **`typing.Protocol` for injection seams**, never a concrete import: e.g.
  `ClientReplyHandle`, `BoardWriterHandle`, `BoardDownHandler`, `AsyncRedisLike`.
  This is *why* the command path can be kept decoupled from Redis/observability —
  dependencies are injected, so the core modules need not import them. Preserve
  this: inject, do not import (the invariant checker forbids `redis` /
  `observability` imports in the command-path modules).
* Validation failures raise `ProtocolValidationError(code, message)` carrying a
  structured `ErrorObject`; callers convert to a response rather than crashing.

## Module separation (keep these boundaries)

```
protocol.py        contract primitives: framing, validation, seq helpers, pop-wins
state.py           enums + state/record dataclasses (no I/O)
interfaces.py      Protocol seams + serialized writer + estop path (no sockets)
controller.py      dispatch, FIFO, timeouts, e-stop gate (injected observability)
board_connection.py  board TCP read/connect loops
local_socket.py    local client Unix socket server + per-client backpressure
observability.py   bounded drop-oldest queue + best-effort Redis writer
```

Dependency direction flows toward the contract primitives: everything may import
`protocol`/`state`; the command path must not import `observability`/`redis`;
`demos/` must not import controller-internal modules (only the shared contracts).

## Async rules

* No blocking calls on any async path; prefer `asyncio` streams.
* Per-command `asyncio.wait_for` for execution timeouts — no central scanner task.
* Let `asyncio.CancelledError` propagate after cleanup; never swallow it.
* Bounded queues everywhere data crosses a producer/consumer boundary, with an
  explicit overflow policy (command FIFO = reject-newest `BOARD_BUSY`;
  observability + client events = drop-oldest with a counter).

## Serialization & logging

* JSON is **compact and stable**: `json.dumps(..., separators=(",", ":"))`, with
  `sort_keys=True` for records meant to be compared/stored.
* One **module-level** logger: `LOGGER = logging.getLogger(__name__)`.
  Observability/Redis failures are logged (`LOGGER.exception`), never raised into
  the command path.

## Tests

* Stdlib **`unittest`**; async cases use `unittest.IsolatedAsyncioTestCase`.
  There is no pytest-asyncio.
* Tests import the modules under test as **top-level names and are run from the
  repo root** (`python -m pytest`). Do not add a `src/` layout or path hacks.
* Deterministic only — no arbitrary `asyncio.sleep`; synchronize on real signals;
  drive timeouts with injectable clocks (see `testing-async-loops-and-mocks`).
* Shared fakes (e.g. fake stream writer/reader) live in `tests/conftest.py`.

## Definition of done

New code uses future annotations, the union/generic syntax above, the right
dataclass/Protocol choice, and the module boundaries; no blocking calls on async
paths; no new terminal statuses; serialization/logging match the conventions;
`python -m pytest`, `python tools/check_invariants.py`, `ruff check .`, and
`mypy .` all pass.
