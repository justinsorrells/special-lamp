# Project Manager Audit: Contract Compliance Report

This report evaluates the current Python asyncio implementation of the **Hyperloop board networking stack** at `~/special-lamp` against the authoritative contract **`docs/contracts/V1_Networking_Decisions.md` (v3 FROZEN)**.

Overall, the core structure, protocol parsing, and state boundaries are of extremely high quality, and all 88 existing tests pass. However, there are several load-bearing gaps that the next development agent (e.g., Codex) must address to achieve 100% compliance with the frozen specifications.

---

## 1. Compliance Matrix

| Decision Area | Status | Contract Reference | Notes / Findings |
| :--- | :--- | :--- | :--- |
| **Topology & Socket Lifecycle** | **Fully Compliant** | (0) | Unix socket unlinks dead sockets and binds with mode `0600` successfully. |
| **Sequence Number Rewriting** | **Fully Compliant** | (1.1, 3.1) | Clean split between opaque client `seq` and monotonic `board_seq` counters. |
| **Concurrency & FIFO Gating** | **Fully Compliant** | (1.2, 1.7) | Default FIFO depth `6` with reject-newest `BOARD_BUSY` behavior. |
| **Reconnect Backoff** | **Non-Compliant** | (1.4) | Uses a constant delay (`0.05` s) instead of exponential backoff with full jitter. |
| **Command Timeout Clocks** | **Fully Compliant** | (1.5) | Implements queue residency cap and execution clock starting at board write. |
| **Telemetry & Liveness** | **Partially Compliant** | (1.6) | Telemetry payloads are captured, but no liveness checker faults the board after missed frames. |
| **Observability Queue (Redis)** | **Fully Compliant** | (1.7, 1.15) | Bounded drop-oldest `obs_queue` (size `20,000`) with Redis pub/sub mirroring. |
| **Late Response Resolution** | **Fully Compliant** | (1.8) | Implements atomic `pop_pending` pop-wins resolution without yields. |
| **Out-of-band E-stop** | **Fully Compliant** | (1.13, 1.19) | E-stop writes bypass command FIFO/in-flight slots but serialize under a shared writer lock. |
| **Local Client Event Delivery** | **Non-Compliant** | (1.14, 1.18) | Unix socket server defines `broadcast_event`, but the controller events are not wired to it. |
| **Metrics / Counters** | **Partially Compliant** | (4) | Several required counters (specifically `orphaned_response`) are missing. |

---

## 2. Perfect Implementations (What is Done Well)

The codebase has executed several challenging async design patterns correctly:
1. **Orthogonal State Axes (2.1, 2.2):** `state.py` keeps connection state (`BoardConnState`) and safety state (`estop_active`, `estop_ack`) completely separate rather than merging them into a single enum.
2. **Atomic Pop-Wins (1.8):** `protocol.pop_pending` performs an atomic dict `pop` without any `await` yielded in between, ensuring that races between board readers and timeout schedulers never cause double resolution.
3. **Write Serialization (1.19):** Both `ControllerCore` command dispatch and out-of-band `send_estop` writes must acquire the `asyncio.Lock` owned by the `StreamBoardWriter`, preventing byte interleaving on the socket stream.
4. **Unix Socket Safety (0):** `local_socket.py` performs a liveness probe on startup to prevent spawning multiple controller instances, and safely unlinks dead socket paths.

---

## 3. Critical Gaps Identified (Requires Action)

These issues represent deviations from the frozen specification and must be resolved:

### 🔴 Gap 1: Missing Reconnect Jitter & Backoff (Decision 1.4)
* **Contract Specification:** Reconnect backoff must be exponential with full jitter: *Base 500 ms, factor 2, cap 5 s*. Retries are infinite.
* **Current Implementation:** In [board_connection.py](file:///Users/justinsorrells/special-lamp/board_connection.py#L98), the loop sleeps for a constant `self.reconnect_delay_s` (configured at `0.05` seconds in the test suite). There is no exponential multiplication or jitter logic applied.

### 🔴 Gap 2: Missing Board Liveness Watchdog (Decision 1.6)
* **Contract Specification:** Telemetry doubles as the board's liveness signal. The controller must mark a board as `FAULTED` after ~5 missed telemetry frames (~250 ms with no inbound message).
* **Current Implementation:** While [board_connection.py](file:///Users/justinsorrells/special-lamp/board_connection.py#L183) stamps the board's `last_seen` timestamp when telemetry is received, there is no background task or scheduling logic to check this timestamp and trigger a `FAULTED` state change when telemetry ceases.

### 🔴 Gap 3: Missing Local Socket Event Broadcast Loop (Decision 1.13, 1.14)
* **Contract Specification:** Local GUI clients must receive unsolicited event messages (e.g. `estop_triggered`, `estop_ack`, and state transitions).
* **Current Implementation:** `LocalUnixSocketServer` defines `broadcast_event`, but the controller has no reference to it and does not propagate events to socket clients. Events enqueued via `observe_controller_event` only enter the Redis observability queue, leaving local clients blind to E-stops or state shifts.

### 🔴 Gap 4: Missing `orphaned_response` Metrics & Unused Helper (Decision 1.12, 4)
* **Contract Specification:** Client disconnects must drop response delivery and increment the `orphaned_response` counter.
* **Current Implementation:** 
  * `deliver_client_response` is defined in [interfaces.py](file:///Users/justinsorrells/special-lamp/interfaces.py#L86) but never imported or used by `local_socket.py`.
  * `ControllerCounters` in [controller.py](file:///Users/justinsorrells/special-lamp/controller.py#L41) does not contain an `orphaned_response` metric, nor is it updated on client disconnection.

---

## 4. Proposed Backlog for Next Agent (Codex)

To achieve 100% compliance, the following implementation plan should be executed:

1. **Implement Exponential Backoff Jitter:**
   * Modify `BoardTCPConnection` to track a mutable `current_delay_s` starting at `0.5` seconds.
   * Apply exponential multiplication (factor 2, cap 5.0) and full jitter (`random.uniform(0, current_delay)`) on connect failure.
   * Reset the delay back to `0.5` upon successful schema registration.

2. **Add Liveness Watchdog Task:**
   * Spawn a background liveness check task in `BoardTCPConnection` when registering a board.
   * Periodically check `(loop.time() - board.last_seen) > 0.25` and force-fault the connection if exceeded.

3. **Bridge Controller Events to Unix Socket Server:**
   * Define an event observer callback interface in the controller or pass a callback from `LocalUnixSocketServer` to `ControllerCore`.
   * Ensure any controller-observed events (like state changes, E-stops) trigger `broadcast_event` to all active clients.

4. **Wire up `orphaned_response` counter:**
   * Add `orphaned_response` to `ControllerCounters` in `controller.py`.
   * Update Unix socket client reply logic to use `deliver_client_response` and increment the counter if client is gone.
