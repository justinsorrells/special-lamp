# Companion Contracts and Guides

> **Authority.** These files are companion material. The frozen contract
> `docs/contracts/V1_Networking_Decisions.md` remains the source of truth for
> controller/networking behavior, and `docs/contracts/Board_Developer_Guide.md`
> remains the firmware-facing source of truth. If a companion file disagrees
> with those contracts, the frozen contracts win and the companion file should be
> fixed.

This directory turns the frozen v1 decisions into implementation handoffs and
operator-facing checklists. It does not define new protocol behavior.

## Files

| File | Audience | Purpose |
|---|---|---|
| `Component_Handoff_Contracts.md` | Controller implementers | Ownership boundaries between protocol, state, controller, board TCP, local socket, and observability code. |
| `Codebase_Reference.html` | Implementers and reviewers | Study-grade tabbed reference with one tab for every Python file in the repository. |
| `Integration_Guide.md` | Integrators and operators | How to add boards, commands, local clients, Redis observability, and shutdown behavior without changing the topology. |
| `Local_Client_API.md` | Local client authors | Controller-local schema discovery requests, responses, events, validation, and local line limits. |
| `Test_Matrix.md` | Implementers and reviewers | Boundary conditions that companion changes and future feature work should keep covered. |

## Non-Negotiable Shape

```text
local client -> Unix socket -> asyncio controller -> persistent TCP -> board
```

- The controller is the only authority for board communication.
- Redis is observability/read-replica only, never the command path.
- Connection state and safety state are separate axes.
- Command terminal statuses are exactly `ok`, `error`, and `timeout`.
- New failure modes use existing or contract-added error codes, not new statuses.
- `estop` bypasses command FIFO and in-flight ownership, but still uses the
  per-board serialized writer path.
