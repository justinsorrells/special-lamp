# Remaining V1 Networking Backlog

General rules for every task:

* Read `AGENTS.md`.
* Read `docs/contracts/V1_Networking_Decisions.md`.
* Read `docs/contracts/Board_Developer_Guide.md` if firmware-facing behavior is affected.
* Read the relevant `.agents/skills/*/SKILL.md` files.
* Preserve the frozen architecture:

  * local client -> Unix socket -> controller -> TCP -> board
  * controller is the single authority
  * Redis is observability/read-replica only
  * terminal statuses are only `ok`, `error`, and `timeout`
  * error cases use error codes, not new terminal status values
  * client `seq` and controller-owned `board_seq` are distinct
  * connection state and safety/e-stop state are orthogonal
* Do not modify `docs/contracts/`, `AGENTS.md`, or `.agents/skills/` unless the task explicitly grants permission.
* If a direct contradiction with the contracts is found, stop and report it instead of editing the contracts.
* Run relevant tests and report results.

---

* [x] Task: Implement Redis telemetry/logging integration

  ## Goal

  Implement Redis telemetry/logging as an observability side-channel only.

  Redis must remain read-replica / observability infrastructure. Redis must not be placed in the command path and must not become a source of truth for board state.

  ## Scope

  Implement:

  * bounded telemetry/event queue
  * drop-oldest behavior when the queue is full
  * async telemetry worker that consumes the queue
  * command lifecycle event emission
  * board telemetry event emission if supported by existing interfaces
  * board state snapshot serialization/mirroring if supported by existing state contracts
  * Redis write failure logging/recording
  * clean telemetry worker shutdown
  * optional no-op or disabled telemetry mode for tests/local development

  Redis events/snapshots should support observability of:

  * command received
  * command routed
  * command sent to board
  * command resolved
  * command timeout
  * board unavailable
  * board disconnect
  * malformed board/client message where appropriate
  * e-stop rejection where appropriate
  * board telemetry update where supported
  * board state snapshot where supported

  ## Do not implement

  Do not implement:

  * GUI/webapp changes
  * firmware changes
  * new command routing
  * new protocol status values
  * Redis pub/sub command path
  * Redis as source of truth
  * architecture changes
  * contract changes
  * local client event backpressure; that is a separate task
  * metrics dashboard; that is a separate task

  ## Tests should cover

  Add or update tests for:

  * command path still works with Redis disabled
  * command path still works when Redis is unavailable
  * Redis write failure does not fail a command
  * telemetry enqueue does not block command completion
  * telemetry queue is bounded
  * telemetry queue uses drop-oldest behavior when full
  * dropped telemetry increments or records an observability drop signal if metrics hooks already exist
  * worker handles Redis write exceptions without crashing the controller
  * worker shutdown is clean
  * worker shutdown drains or cancels according to the existing contract
  * board state snapshot is read-replica data only
  * command lifecycle events include relevant IDs/fields:

    * client `seq`
    * controller `board_seq` where applicable
    * board id
    * command name
    * terminal status
    * error code where applicable
    * timestamp fields where applicable
  * Redis integration does not add new terminal statuses
  * Redis integration does not route commands through Redis

  ## Validation

  Run:

  ```bash
  pytest
  python -m compileall .
  ```

  Also run if configured:

  ```bash
  ruff check .
  mypy .
  ```

  ## Files and contract reminder

  Do not modify:

  * `docs/contracts/`
  * `AGENTS.md`
  * `.agents/skills/`

  unless explicitly authorized by the operator.

---

* [x] Task: Implement local client event backpressure

  ## Goal

  Implement outbound event backpressure for local Unix socket clients.

  Local clients may receive unsolicited `event` messages in addition to command `response` messages. The controller must prevent slow or stuck local clients from causing unbounded memory growth or blocking the controller.

  ## Scope

  Implement:

  * per-local-client outbound event queue
  * queue depth of 1000 events per local client
  * drop-oldest behavior for non-critical events when the queue overflows
  * critical-event handling when the queue is saturated
  * disconnect behavior when critical events cannot be delivered due to saturation
  * event classification:

    * critical events
    * non-critical events
  * logging/metrics hooks for dropped events and disconnected saturated clients
  * clean cancellation/shutdown behavior for client event writer tasks
  * tests using fake local clients / Unix socket clients

  Critical events should not be silently dropped.

  Non-critical events may be dropped oldest-first when the queue is full.

  ## Do not implement

  Do not implement:

  * Redis telemetry queue changes except minimal hooks if already available
  * GUI/webapp changes
  * board TCP changes
  * firmware changes
  * new command routing
  * new protocol status values
  * architecture changes
  * contract changes
  * metrics dashboard

  Do not make local clients talk directly to boards.

  Do not put Redis in the local client event path.

  ## Tests should cover

  Add or update tests for:

  * each connected local client gets its own outbound event queue
  * outbound queue depth is capped at 1000
  * non-critical events use drop-oldest behavior on overflow
  * dropped non-critical events are logged or counted
  * critical events are not silently dropped
  * saturated critical event queue disconnects the affected client
  * disconnecting a saturated client does not affect other connected clients
  * command responses still reach the requesting client when possible
  * client disconnect during queued event delivery does not crash the controller
  * slow client does not block controller command processing
  * multiple clients can receive events independently
  * queue overflow does not introduce new terminal command statuses
  * queue overflow does not route through Redis
  * shutdown cancels/drains local client writer tasks cleanly

  ## Validation

  Run:

  ```bash
  pytest
  python -m compileall .
  ```

  Also run if configured:

  ```bash
  ruff check .
  mypy .
  ```

  ## Files and contract reminder

  Do not modify:

  * `docs/contracts/`
  * `AGENTS.md`
  * `.agents/skills/`

  unless explicitly authorized by the operator.

---

* [x] Task: Implement monotonic clock latency & telemetry rate tracking

  ## Goal

  Add latency and telemetry-rate tracking using monotonic controller time.

  The controller should measure round-trip command latency using a monotonic `controller_ts` and should optionally record board-reported processing duration via `board_proc_us` when the board provides it.

  The controller should also compute telemetry rate and jitter per board.

  ## Scope

  Implement:

  * monotonic controller timestamp capture at command send time
  * round-trip latency measurement when the board response is resolved
  * optional parsing/recording of `board_proc_us` from board responses when present
  * telemetry arrival-rate tracking per board
  * telemetry jitter tracking per board
  * storage of latency/rate/jitter observations in controller-owned in-memory state
  * observability hooks for Redis/events if Redis telemetry integration already exists
  * tests using deterministic/fake clocks where possible

  Use monotonic time for elapsed-duration calculations. Do not use wall-clock time for latency measurement.

  If wall-clock timestamps are needed for logs, keep them separate from monotonic elapsed-time calculations.

  ## Do not implement

  Do not implement:

  * firmware changes
  * GUI/webapp changes
  * Redis command path
  * new command routing
  * new terminal status values
  * architecture changes
  * contract changes
  * heartbeat behavior; that is a separate task
  * metrics dashboard; that is a separate task

  Do not require boards to send `board_proc_us`. It is optional.

  ## Tests should cover

  Add or update tests for:

  * command send records monotonic controller timestamp
  * command response computes round-trip duration from monotonic time
  * latency calculation does not use wall-clock time
  * missing `board_proc_us` is accepted
  * present `board_proc_us` is parsed and recorded
  * invalid `board_proc_us` is ignored or handled safely according to existing validation patterns
  * telemetry arrival rate is computed per board
  * telemetry jitter is computed per board
  * telemetry rate tracking handles first sample
  * telemetry rate tracking handles irregular intervals
  * telemetry rate tracking handles board disconnect/reconnect without crashing
  * latency/rate tracking does not alter terminal command statuses
  * latency/rate tracking does not affect command routing
  * latency/rate tracking does not make Redis a source of truth
  * fake/deterministic clock tests are stable and not sleep-based where avoidable

  ## Validation

  Run:

  ```bash
  pytest
  python -m compileall .
  ```

  Also run if configured:

  ```bash
  ruff check .
  mypy .
  ```

  ## Files and contract reminder

  Do not modify:

  * `docs/contracts/`
  * `AGENTS.md`
  * `.agents/skills/`

  unless explicitly authorized by the operator.

---

* [x] Task: Implement metrics and lifecycle counters

  ## Goal

  Add controller-owned metrics and lifecycle counters for debugging, observability, testing, and operational introspection.

  Metrics should be in-memory controller-owned state first. Redis may mirror metrics if Redis telemetry integration exists, but Redis must not become the source of truth.

  ## Scope

  Implement counters for contract-relevant lifecycle and failure events.

  Include counters such as:

  * `obs_dropped`
  * `unmatched_seq`
  * `orphaned_response`
  * `board_busy_rejections`
  * `estop_rejections`
  * `malformed_client_messages`
  * `malformed_board_messages`
  * `client_disconnects`
  * `board_disconnects`
  * `command_timeouts`
  * `controller_shutdown_failures`
  * `redis_write_failures` if Redis integration exists
  * `local_event_dropped` if local client event backpressure exists
  * `critical_event_disconnects` if local client event backpressure exists
  * `late_board_responses`
  * `duplicate_board_responses`
  * `commands_completed_ok`
  * `commands_completed_error`
  * `commands_completed_timeout`

  Implement:

  * in-memory metrics object or registry
  * safe increment/update helpers
  * metrics snapshot/export method for tests and observability
  * integration points in existing controller paths
  * tests for counter increments

  ## Do not implement

  Do not implement:

  * GUI dashboard
  * webapp changes
  * firmware changes
  * new command routing
  * new terminal status values
  * Redis as source of truth
  * architecture changes
  * contract changes
  * broad tracing framework
  * external metrics backend unless already specified by existing code/contracts

  Do not make metrics required for command completion.

  ## Tests should cover

  Add or update tests for:

  * metrics object initializes all expected counters to zero
  * successful command increments success/completion counters
  * error command increments error/completion counters
  * timeout increments timeout counter
  * board busy rejection increments `board_busy_rejections`
  * e-stop rejection increments `estop_rejections`
  * unmatched/unknown board response increments `unmatched_seq` or equivalent
  * late response after timeout increments late/orphan counter as appropriate
  * duplicate response does not double-resolve and increments duplicate/orphan counter as appropriate
  * malformed client message increments malformed client counter
  * malformed board message increments malformed board counter
  * board disconnect increments board disconnect counter
  * client disconnect increments client disconnect counter
  * Redis write failure increments Redis failure counter if Redis integration exists
  * local event drop increments event-drop counter if event backpressure exists
  * metrics snapshot is read-only/copy-safe
  * metrics do not introduce new terminal statuses
  * metrics do not affect command routing
  * metrics do not make Redis authoritative

  ## Validation

  Run:

  ```bash
  pytest
  python -m compileall .
  ```

  Also run if configured:

  ```bash
  ruff check .
  mypy .
  ```

  ## Files and contract reminder

  Do not modify:

  * `docs/contracts/`
  * `AGENTS.md`
  * `.agents/skills/`

  unless explicitly authorized by the operator.

---

* [x] Task: Implement optional heartbeat and RX-path insurance

  ## Goal

  Implement an optional, slow controller-to-board heartbeat/ack mechanism to detect a board whose TX telemetry path is still alive but whose RX command path may be wedged.

  This is RX-path insurance only. It must not replace normal command timeouts, telemetry rate tracking, or board disconnect detection.

  ## Scope

  Implement:

  * optional heartbeat configuration
  * slow controller-to-board heartbeat message
  * expected heartbeat acknowledgement handling
  * timeout/missed-ack tracking
  * per-board RX-path health indication
  * integration with board availability/health state according to existing state contracts
  * logging/metrics hooks for missed heartbeat acknowledgements
  * tests using fake board behavior

  The heartbeat should be slow and low-rate. It should not flood the board.

  The heartbeat should use the same board writer serialization rules as other writes unless the contract says otherwise.

  The heartbeat must not bypass the per-board writer lock.

  ## Do not implement

  Do not implement:

  * firmware changes unless explicitly requested
  * GUI/webapp changes
  * Redis command path
  * new terminal status values
  * new architecture decisions
  * contract changes
  * high-rate ping flooding
  * replacement of normal command timeouts
  * replacement of telemetry rate tracking
  * direct local-client-to-board heartbeat path

  Do not treat heartbeat as a hard safety guarantee.

  ## Tests should cover

  Add or update tests for:

  * heartbeat can be disabled
  * disabled heartbeat produces no heartbeat writes
  * enabled heartbeat sends heartbeat at configured slow interval
  * heartbeat writes use the existing per-board writer serialization path
  * heartbeat does not bypass the writer lock
  * heartbeat ack clears missed-heartbeat state
  * missed heartbeat ack increments/logs the appropriate metric or event
  * repeated missed heartbeat acks mark RX path as suspect/unhealthy according to existing state model
  * board telemetry continuing without heartbeat ack can be represented as TX alive but RX suspect
  * heartbeat does not create new terminal command statuses
  * heartbeat does not interfere with one-command-in-flight semantics
  * heartbeat does not starve normal commands
  * board disconnect stops heartbeat loop cleanly
  * controller shutdown cancels heartbeat loop cleanly
  * malformed heartbeat ack is handled safely
  * late heartbeat ack is handled safely

  ## Validation

  Run:

  ```bash
  pytest
  python -m compileall .
  ```

  Also run if configured:

  ```bash
  ruff check .
  mypy .
  ```

  ## Files and contract reminder

  Do not modify:

  * `docs/contracts/`
  * `AGENTS.md`
  * `.agents/skills/`

  unless explicitly authorized by the operator.

---

* [x] Task: Implement controller lifecycle shutdown draining

  ## Goal

  Implement graceful controller shutdown behavior.

  On shutdown, the controller should stop accepting new local requests, allow a bounded drain window for in-flight work, fail remaining unresolved work with `CONTROLLER_SHUTDOWN`, close streams, and flush/drain Redis observability queue where applicable.

  ## Scope

  Implement shutdown lifecycle behavior for:

  * SIGTERM handling
  * stopping local Unix socket request acceptance
  * stopping or quiescing board connection loops
  * 2 second in-flight drain window
  * failing remaining pending/in-flight/queued commands with error code `CONTROLLER_SHUTDOWN`
  * preserving terminal status model:

    * status must be `error` or `timeout` as appropriate
    * do not add `shutdown` as a new status
  * closing local client streams
  * closing board TCP streams
  * cancelling background tasks cleanly
  * flushing/draining Redis telemetry queue where Redis integration exists
  * ensuring shutdown is idempotent
  * logging/metrics hooks for shutdown behavior

  Shutdown should be explicit and testable without requiring an actual OS signal in every test. Prefer a callable shutdown path plus a thin signal handler.

  ## Do not implement

  Do not implement:

  * firmware changes
  * GUI/webapp changes
  * Redis command path
  * new terminal status values
  * architecture changes
  * contract changes
  * process supervisor/systemd integration
  * hardware interaction
  * auto-restart behavior

  Do not let shutdown silently drop accepted commands without terminal response/logging.

  ## Tests should cover

  Add or update tests for:

  * shutdown stops accepting new local client requests
  * new command during shutdown receives structured error with `CONTROLLER_SHUTDOWN`
  * in-flight command may complete during the 2 second drain window
  * unresolved in-flight command after drain window fails with `CONTROLLER_SHUTDOWN`
  * queued commands fail with `CONTROLLER_SHUTDOWN`
  * pending table is cleared/resolved safely
  * local client streams are closed cleanly
  * board TCP streams are closed cleanly
  * background tasks are cancelled or drained cleanly
  * Redis telemetry queue flushes/drains if Redis integration exists
  * Redis flush failure during shutdown is logged but does not hang shutdown indefinitely
  * shutdown is idempotent if called more than once
  * shutdown does not introduce new terminal statuses
  * shutdown preserves pop-wins behavior for late responses
  * late board response after shutdown does not double-resolve a command
  * signal handler invokes the same shutdown path or is covered by a thin integration test
  * tests avoid long real-time sleeps where possible by using fake clocks/timeouts or reduced test durations

  ## Validation

  Run:

  ```bash
  pytest
  python -m compileall .
  ```

  Also run if configured:

  ```bash
  ruff check .
  mypy .
  ```

  ## Files and contract reminder

  Do not modify:

  * `docs/contracts/`
  * `AGENTS.md`
  * `.agents/skills/`

  unless explicitly authorized by the operator.

---

* [x] Task: Implement subprocess timeouts in orchestrator command execution

  ## Goal

  Add execution timeouts around the subprocess execution (`run_cmd`) within the agent orchestrator to prevent a hung or wedged agent CLI command from hanging the entire orchestrator execution loop indefinitely.

---

* [ ] Task: Narrow orchestrator git commit scope

  ## Goal

  Update the orchestrator's git commit stage (`git_commit`) to stage and commit only the specific files that were reviewed, audited, and approved, rather than using `git add -A` which stages and commits the entire worktree indiscriminately.

---

* [ ] Task: Active enforcement of never_auto_push and never_auto_merge

  ## Goal

  Actively enforce the `never_auto_push` and `never_auto_merge` config flags in the orchestrator codebase. Currently, auto-push and auto-merge behavior is only prevented by omission (the orchestrator just does not implement them). They should be explicitly validated and strictly enforced in code.

---

* [x] Task: Make Antigravity (`agy`) audit yield a parseable verdict and relax verdict parsing

  ## Goal

  The orchestrator cannot currently complete a real backlog run because the Antigravity audit step fails to produce a parseable verdict. On the first live end-to-end run (controller shutdown-draining task), Codex implemented the change, all checks passed, and Claude reviewed PASS, but the run stopped with `STOP_ANTIGRAVITY_AUDIT_FAILED` / "Could not parse PASS/FAIL verdict from Antigravity audit" even though `agy`'s own audit concluded the work passed.

  Two distinct root causes, both in `tools/agent_orchestrator` (this is a high-trust, self-modifying orchestrator-maintenance task):

  1. **`agy` does not emit a parseable verdict to stdout.** Unlike `claude -p` and `codex exec`, `agy` runs as a full agentic tool: it explores the repo and writes its real audit report (with the verdict) to its own brain directory (`~/.gemini/antigravity-cli/brain/<id>/audit_report.md`), streaming only a narrative to stdout that never contains a `Final verdict:` line. The audit prompt (`prompts/antigravity_audit.md`) and/or invocation must force `agy` to print exactly `Final verdict: PASS` or `Final verdict: FAIL` as the final stdout line (or the orchestrator must read agy's report artifact).

  2. **The verdict parser is over-strict.** The anti-spoofing hardening locked parsing to `^Final verdict:\s*(PASS|FAIL)\s*$`, which rejects markdown emphasis (`**Final verdict:** PASS`), trailing punctuation, and headings — yet `README.md` still claims verdicts are "matched robustly regardless of Markdown emphasis." Relax the regex to tolerate leading/trailing markdown/whitespace while staying anchored to line start and taking the last match (preserving spoof-resistance), and reconcile the README claim.

  ## Acceptance

  * A real `agy` audit run yields a parseable PASS/FAIL verdict and can reach auto-commit.
  * Verdict parser accepts `**Final verdict:** PASS`, `Final verdict: PASS.`, etc., while still ignoring verdict-looking text embedded mid-line / inside diffs, and still taking the last match.
  * README verdict-robustness claim matches actual behavior.
  * Add tests for the relaxed-but-anchored parsing and the agy-verdict extraction path.


