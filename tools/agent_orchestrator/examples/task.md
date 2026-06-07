# Task: Implement Redis telemetry/logging integration

Read AGENTS.md, docs/contracts/V1_Networking_Decisions.md, and the relevant skills.

Implement Redis telemetry/logging integration only.

Scope:
- Redis is observability/read-replica only.
- Redis must not be in the command path.
- Core board control must work if Redis is unavailable.
- Add a bounded, drop-oldest telemetry/event queue.
- Add an async telemetry worker that consumes the queue.
- Serialize controller events, board telemetry, board state snapshots, and command lifecycle events according to the existing contracts.
- Ensure Redis write failures are logged/recorded but do not fail commands.
- Ensure shutdown drains or cancels the telemetry worker cleanly according to the contract.
- Use existing protocol/state/interfaces types where available.

Do not implement:
- GUI/webapp changes
- firmware changes
- new command routing
- new protocol status values
- Redis pub/sub command path
- architecture changes

Tests should cover:
- command path still works with Redis disabled/unavailable
- telemetry events are enqueued without blocking command completion
- queue is bounded
- queue uses drop-oldest behavior when full
- Redis write failure does not fail a command
- board state snapshot is read-replica data only
- command lifecycle events include command id / seq / board id / status / error code where applicable
- worker shutdown is clean

Do not modify docs/contracts unless you find a direct contradiction. If you find one, report it instead of editing the contract.

Run the relevant tests and report results.
