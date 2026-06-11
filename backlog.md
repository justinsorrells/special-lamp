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

* [x] Task: Narrow orchestrator git commit scope

  ## Goal

  Update the orchestrator's git commit stage (`git_commit`) to stage and commit only the specific files that were reviewed, audited, and approved, rather than using `git add -A` which stages and commits the entire worktree indiscriminately.

---

* [x] Task: Active enforcement of never_auto_push and never_auto_merge

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

---

<!-- V1 Networking contract-coverage backlog (T1–T8). Seeded from a 5-review consensus
     audit (canonical source: ~/contract_FINAL_backlog.md), ratified by Codex + Antigravity.
     Frozen contract `docs/contracts/V1_Networking_Decisions.md` is unchanged. -->

* [x] Task: Implement exponential reconnect backoff with full jitter (contract 1.4)

  ## Goal

  Replace the fixed reconnect delay in `BoardTCPConnection` with contract-1.4 exponential backoff plus full jitter. Currently `board_connection.py` uses a fixed `reconnect_delay_s = 0.05` (`board_connection.py:84,122`) — no exponential growth, jitter, cap, or reset-on-registration.

  ## Files

  * `board_connection.py`
  * `tests/test_board_connection.py`

  ## Requirements

  * AWS full jitter: `sleep = uniform(0, min(5.0, 0.5 * (2 ** attempt)))` — base 500 ms, factor 2, cap 5 s.
  * Reset the attempt counter to base on successful registration (`REGISTERED`).
  * Retries are infinite (no give-up state in V1).
  * Stop/shutdown must cancel the backoff sleep cleanly (no hang).
  * Inject random/clock so tests are deterministic (no real sleeps).

  ## Tests should cover

  * Delay grows exponentially and is capped at 5 s.
  * Each delay is within full-jitter bounds.
  * Backoff resets to base after successful registration.
  * Stop during backoff cancels the sleep promptly.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```

---

* [x] Task: Implement telemetry-loss liveness FAULT watchdog (contract 1.6)

  ## Goal

  Telemetry doubles as the board->controller liveness signal (contract 1.6): mark a board `FAULTED` after ~5 missed 50 ms frames (~250 ms with no inbound message). Today `last_seen` is recorded (`controller.py:882`, `state.py`) but nothing watches it, so a board whose telemetry/RX wedges while TCP stays open remains `REGISTERED` and keeps receiving commands. `FAULTED` currently fires only on read error / registration timeout (`board_connection.py:144-166`).

  ## Files

  * `board_connection.py`, `controller.py`, `state.py`
  * `tests/test_board_connection.py`, `tests/test_controller_core.py`

  ## Requirements

  * Per-board watchdog: if `monotonic now - last_inbound > ~250 ms` (configurable), transition to `FAULTED` and call `board_down(board_id)` so pending + FIFO commands resolve with `BOARD_UNAVAILABLE` (pop-wins).
  * **Settled decision:** any valid inbound board packet (not telemetry only) resets the liveness deadline.
  * Keep orthogonal to the optional heartbeat / `rx_path_suspect` behavior.
  * Liveness decisions must not depend on Redis/observability.
  * Deterministic/injectable clock; no real sleeps.

  ## Do not

  * Solicit telemetry. Merge connection state with safety/e-stop state. Add new terminal statuses.

  ## Tests should cover

  * Healthy telemetry keeps the board `REGISTERED`.
  * Silence beyond threshold → `FAULTED` + pending/FIFO failed with `BOARD_UNAVAILABLE`.
  * Any inbound packet resets the deadline.
  * Redis down/disabled does not affect the liveness decision.
  * Reconnect recovers cleanly.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```

---

* [x] Task: Add controller runtime entrypoint, configuration, and signal handling (contract 0, 6.9)

  ## Goal

  There is no production controller runner: the stack exists as components (`ControllerCore`, `BoardTCPConnection`, `LocalUnixSocketServer`, observability) with no module that wires them, loads config, and installs OS signal handlers. (`shutdown()` exists but nothing calls it from a signal handler; non-test `__main__` exists only under `demos/`.)

  ## Files

  * New `tools/run_controller.py` (or `controller_main.py`)
  * New non-authoritative **Configuration Contract** doc (NOT under `docs/contracts/`)
  * `local_socket.py` (touch as needed)
  * Tests under `tests/`

  ## Requirements

  * Load static config: board ids + host/port (statically configured; unknown ids rejected, contract 0/1.3), Unix socket path, optional Redis params, heartbeat enablement, timeout/FIFO/backoff/liveness knobs.
  * Start one `BoardTCPConnection` per configured board, the `LocalUnixSocketServer`, and the Redis telemetry worker when configured.
  * Unix socket lifecycle (already implemented in `local_socket.py:193-226`, wire it): create mode `0600`; unlink a dead/stale socket; refuse to start if a live controller owns the path.
  * SIGTERM/SIGINT → existing graceful `shutdown()` (stop accepting, bounded in-flight drain, fail remainder `CONTROLLER_SHUTDOWN`, close board + client streams). Idempotent.

  ## Tests should cover

  * Stale dead socket unlinked and recreated; live controller on the path → refuse-to-start; socket mode `0600`.
  * SIGTERM path: new requests refused, in-flight drained, remainder `CONTROLLER_SHUTDOWN`.
  * Unknown/duplicate board id rejected.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```

---

* [x] Task: Complete section 4 metrics and latency percentiles (contract 4)

  ## Goal

  The minimum metrics set (contract 4) is incomplete: `reconnect_count` and `protocol_version_mismatches` are absent, and only the last command latency is tracked (`state.py:29,167`) instead of per-command p50/p95/p99.

  ## Files

  * `controller.py`, `state.py`, `observability.py`
  * `tests/test_controller_core.py`, `tests/test_observability.py`

  ## Requirements

  * Add to `metrics_snapshot()`: `reconnect_count`, `registration_timeouts`, `protocol_version_mismatches`.
  * **Settled semantics:** `reconnect_count` increments on a successful TCP open; `registration_timeouts` increments when a board connects but sends no schema within the 2 s registration window; `protocol_version_mismatches` on a 1.16 fault.
  * Track per-board command RTT in a bounded buffer (e.g. `deque(maxlen=1000)`) and expose p50/p95/p99 computed from the monotonic `controller_ts`.
  * Mirror added fields to Redis state/observability if state records are extended.
  * No new terminal statuses; must not affect the command path.

  ## Tests should cover

  * Percentiles correct over a known latency sample; latency window bounded.
  * Each counter increments per its defined trigger.
  * Metrics snapshot is read-only/copy-safe; command routing unaffected.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```

---

* [x] Task: Replace legacy UDP demos with a contract-faithful TCP mock board and Unix-socket client

  ## Goal

  The `demos/` code is legacy UDP / direct-board (`demos/server/server.py:81` uses `SOCK_DGRAM`; `demos/client/client.py` is a `UDPClient`; `demos/webapp.py` talks UDP to boards), contradicting the frozen topology. No contract-faithful mock board (Agent 7) exists for real TCP/timing integration.

  ## Files

  * `demos/server/server.py` (→ TCP mock board), `demos/client/client.py`, optionally `demos/webapp.py`, `demos/*/README.md`

  ## Requirements

  * Mock board = TCP **server**: pushes schema on connect/reconnect (`protocol_version` + per-command `blocked_by_estop`), pushes 50 ms telemetry, responds to `command`s with newline-delimited JSON, emits `event: estop_ack` on `estop`, and supports injectable RX-wedged + TCP-disconnect failure modes.
  * Local demo client: connects to the controller Unix socket, sends `command` / `estop_reset`, and continuously reads both `response` and unsolicited `event`. Never talks directly to a board.
  * Optional webapp: talks only to the controller Unix socket.

  ## Do not

  * Reintroduce UDP into the command path. Make the GUI/client talk directly to boards. Invent new protocol fields/statuses.

  ## Tests should cover

  * E2E smoke: `client -> Unix socket -> controller -> TCP mock board -> response`.
  * E-stop path: broadcast `estop` and receive `estop_ack`.
  * Unknown-target / timeout / board-disconnect smoke paths.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```

---

* [x] Task: Add e-stop and lifecycle integration proof tests (contract 1.7, 1.13, 6.9)

  ## Goal

  Core e-stop pieces are covered, but high-value *combinations* and runtime paths are not directly proven. (Note: two-client same-`seq`, two-board `board_seq=1`, and large-schema-accepted are already covered at the protocol/unit level in `tests/test_protocol_contracts.py` — do not re-add; expand only to runtime/e2e.)

  ## Files

  * `tests/` (likely `test_controller_core.py`, `test_board_connection.py`, `test_observability.py`, integration tests)

  ## Tests should cover

  * **Redis outage during `trigger_estop`:** latch sets, FIFOs clear with `ESTOP_ACTIVE`, boards receive `estop`, local clients notified, and no exception escapes — e-stop never depends on Redis (1.7/1.13).
  * **Reconnect during active e-stop:** board reaches `REGISTERED` while `estop_active`, controller re-sends out-of-band `estop`, gated commands rejected, a `blocked_by_estop=false` command still dispatches, `estop_ack` false until the event.
  * **Telemetry-loss → FAULTED end-to-end** (pairs with the liveness task).
  * **SIGTERM end-to-end** via the runtime entrypoint (pairs with the entrypoint task).
  * **Test-tier visibility:** make the socket-bind integration tests (which `raise unittest.SkipTest` on `PermissionError`) an explicit, reported tier so a CI skip is intentional and visible, not silent.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```

---

* [ ] Task: Author companion contracts and guides

  ## Goal

  Add the missing developer/operator docs that current implementation work needs. All non-authoritative; `docs/contracts/V1_Networking_Decisions.md` remains authoritative and is not edited.

  ## Scope (ranked)

  * **Must-have:** Configuration Contract (board endpoints, socket path, Redis, heartbeat, timeout/FIFO/backoff/liveness knobs — pairs with the entrypoint task); Contract Coverage Matrix (each contract section -> impl / unit test / integration test / static check / doc, gaps marked); E-stop Reset Clearance Contract; Contract-Level Integration Test Plan.
  * **Useful:** Local Client Developer Guide (full-duplex `response` vs `event`, seq semantics, error codes, backpressure); Redis Observability Schema (hash/stream/pub-sub field shapes, `event_id`).
  * **Not needed:** Error-Code Mapping Table (already in 3.11), Heartbeat Wire Contract. Defer Board Library API + QNEthernet notes to firmware work.

  ## Operator decision required (E-stop Reset Clearance Contract)

  Proposed (PENDING OPERATOR RATIFICATION — safety-adjacent): the controller does **not** poll boards for physical safety; it accepts `estop_reset` from the local client, and the client/GUI is responsible for confirming the physical interlock is cleared. Document ownership, sync/async behavior, and failure behavior. Do not implement reset-clearance logic until ratified.

  ## Validation

  Docs only; no runtime tests unless a docs/coverage checker is added.

---

* [ ] Task: Repo hygiene — root contract duplicate, pytest path, stray-file guard

  ## Goal

  Close out repo-cleanliness gaps surfaced during the contract audit.

  ## Requirements

  * Remove the divergent **root** `V1_Networking_Decisions.md` (or replace with a one-line pointer to the canonical `docs/contracts/V1_Networking_Decisions.md`); do not modify the canonical copy.
  * Add a `tools/check_invariants.py` rule that fails on a divergent duplicate contract file and on stray top-level review/brief markdown files.
  * Pin pytest's import path so every invocation collects+runs the same 191 tests: add `[tool.pytest.ini_options]\npythonpath = ["."]` to `pyproject.toml`. (Today bare `pytest` errors at collection without the repo root on `sys.path`.)

  ## Tests should cover

  * Invariant checker flags a divergent duplicate contract file.
  * `pytest -q` and `python -m pytest -q` both collect 191 tests from the repo root.
  * Existing `pytest` / `check_invariants` / `ruff` / `mypy` still pass.

  ## Validation

  ```bash
  python -m pytest
  python tools/check_invariants.py
  ruff check .
  mypy .
  ```


