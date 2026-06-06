# V1 Networking Architecture: Locked Decisions & Message Contract (v3 FROZEN)

Companion to `Networking_Architecture_Proposal.pdf`. This file resolves the
"open questions" that define interfaces between agents and is the **authoritative
implementation contract**. Where this file and the proposal disagree, this file
wins.

v3 changelog (from v2): `estop` is out-of-band and bypasses the in-flight rule
(1.2/1.13); line limits clarified as receive-side (1.9); command gating during
e-stop is schema-driven via `blocked_by_estop`, absent = blocked/fail-safe
(1.17); `controller_ts` must be monotonic (1.10); local client event
backpressure pinned (1.18); board-id, socket-lifecycle, client-lifetime, and
Redis-retention defaults written into the contract (0).

v3 FROZEN: per-board write serialization pinned (1.19); `estop_ack` event shape
pinned (1.20 / 3.13). No scope changes beyond these two; the design is frozen
for implementation starting with `protocol.py`, `state.py`, `interfaces.py`.

System context that drives these choices: closed/trusted system, no external
network access; underground machine that moves slowly; command response budget
under 10 s; anything genuinely hazardous is handled by a hardwired interlock and
power cut, not software. Security is out of scope for V1 (single trusted host).
**Software e-stop is convergence and coordination, not the safety guarantee.**

---

## 0. Assumptions written into the contract

These are load-bearing. Implementations may rely on them.

- Single trusted host; no auth; security out of scope for V1.
- The hardwired interlock plus power cut is the safety guarantee. Software
  e-stop is convergence only and cannot guarantee delivery timing or preempt
  bytes already written to a socket.
- Command execution timeout starts when the command is **written to the board**,
  not when it is enqueued. Queued commands have a separate residency cap (1.5).
- Client `seq` values are unique among that client's own outstanding requests.
  The controller does not deduplicate across clients; it owns a separate
  board-facing sequence space.
- **Board IDs are statically configured.** The controller holds a config list of
  expected board ids. A board declares its id in `source`/schema; on connect the
  controller matches against config and rejects unknown ids. No dynamic
  discovery in V1.
- **Unix socket lifecycle:** on startup, if the socket path exists, the
  controller attempts a connection; if dead, it unlinks and recreates; if a live
  controller answers, it refuses to start (prevents two controllers). Socket is
  created mode 0600.
- **Local clients are long-lived**, persistent, full-duplex connections (1.14).
  No connect-per-command.
- Reconnect retries forever at the backoff cap. There is no give-up state in V1.
- Telemetry `timestamp` is board-local and is never used for latency. Only the
  `controller_ts` echo (1.10) yields latency.
- Connection state and safety state are orthogonal (2). No single enum encodes
  both.
- **Redis retention:** telemetry/history are observability and are capped, not
  grown forever. Defaults (judgment call for codex): telemetry in capped streams
  (`XADD MAXLEN ~ 100000` or time-based trim); command history retained ~7 days;
  state hashes are current-value (no retention needed).

---

## 1. Decision Sheet

### 1.1 Sequence number ownership
The controller owns a per-board monotonic counter (uint64, starts at 1) =
`board_seq`. The client's `seq` is opaque to the controller and is echoed back
unchanged in the client-facing response. Pending commands are keyed by
`board_seq`; each entry stores the originating client handle and the client's
original `seq` for the reply. Top-level `seq` on a client-facing `response` is
the client's seq; `result.board_seq` is the controller seq. **They are never
equal and must never be conflated.**
*Pins:* Agent 1 (contract), Agent 4 (dispatcher, pending table).

### 1.2 Concurrency per board
One command in flight per board (Teensy is single-threaded). Controller holds a
bounded per-board FIFO; reject with `BOARD_BUSY` when full. Telemetry and events
from the board are unsolicited and are not subject to this rule; the reader
dispatches them by `type`.
**FIFO depth = 6** (conservative default; tune to real burst pattern). With a
per-command budget up to 10 s, a deep queue is minutes of stale backlog; a
shallow queue plus the residency cap (1.5) keeps the system honest.

The one-command-in-flight rule and the per-board FIFO apply to `command`
messages only. **`estop` is an urgent out-of-band control message:** it is
written directly to the board socket without entering the FIFO and without
consuming the in-flight slot, and may be written while a normal command is still
pending. This does not unsend or preempt bytes already written; it only
guarantees `estop` is the next controller write to that board.
*Pins:* Agent 4, Agent 1 (`BOARD_BUSY`).

### 1.3 Schema exchange: push on connect
Board sends its schema as the first message after the TCP connection opens, and
again on every reconnect. No `get_schema` command exists. Connection axis is
`CONNECTED` until schema arrives, then `REGISTERED`. No schema within a 2 s
registration timeout -> `FAULTED`, then reconnect. Schema carries
`protocol_version`; on mismatch the controller logs and FAULTs the board rather
than registering it (1.16). Each command in the schema carries `blocked_by_estop`
(1.17).
*Pins:* Agent 3 (state machine), Agent 8 (firmware).

### 1.4 Reconnect backoff
Exponential with full jitter. Base 500 ms, factor 2, cap 5 s. Reset to base on
successful registration. Retries are infinite (no give-up state in V1).
*Pins:* Agent 3.

### 1.5 Command timeout: clocks and mechanism
Per-command `asyncio.wait_for` (no central scanning task). Two distinct timers:

- **Execution timeout** starts when the command is **written to the board**.
  Default 2 s; per-command overrides allowed but **no override may exceed the
  10 s hard ceiling** (assert in the dispatcher). On expiry -> `COMMAND_TIMEOUT`.
- **Queue residency cap** applies while a command waits in the per-board FIFO.
  On pop, if the entry has waited longer than its cap (default 10 s), it is
  rejected with `COMMAND_TIMEOUT` and **never written to the board**.

*Reason:* starting the execution clock at enqueue would time out commands that
never reached the board; the residency cap prevents stale commands from firing
late.
*Pins:* Agent 4.

### 1.6 Telemetry and liveness
Telemetry is **one-way push** from the board's own 50 ms hardware timer. It is
not solicited and is never a command response.
*Reason:* a board in a tight loop may only notice inbound data ~every 250 ms
(QNEthernet behaviour), so soliciting telemetry would collapse the rate.
Pushing from a local timer is independent of inbound-notice latency and avoids a
round trip per frame.
Telemetry doubles as the board->controller liveness signal: mark `FAULTED` after
~5 missed frames (~250 ms with no inbound message).
Optional: a slow controller->board heartbeat-ack purely as RX-path insurance
(catches a board whose receive path wedged while its timer still pushes). Not
50 ms; firmware discretion; not required for liveness.
*Pins:* Agent 3 (health tracker), Agent 8 (firmware telemetry timer).

### 1.7 Internal queue topology and bounds
**Single observability queue.** Path:
`board reader -> obs_queue -> redis writer -> Redis`. The reader enqueues
telemetry/log/event records; the writer drains, enriches, and writes.

- `obs_queue` `maxsize=20000`, **drop-oldest** on full, increment `obs_dropped`.

**drop-oldest mechanism (specify exactly so agents agree):** on `put_nowait`
raising `QueueFull`, perform one `get_nowait()` to evict the oldest, increment
the drop counter, then retry the `put_nowait`. (Equivalently, back the queue
with `collections.deque(maxlen=N)` plus an `asyncio.Event`. Pick one
project-wide; do not mix.)

Per-board command FIFOs (1.2) are separate, are **reject-newest** (`BOARD_BUSY`),
and are **not** drop-oldest. Dropping a queued command silently would lose a
control action; rejecting it surfaces the backpressure to the caller.
*Reason:* a slow consumer or down Redis must never OOM the controller or stall
the command path.
*Pins:* Agent 5 (writer), Agent 6 (counters).

### 1.8 Unknown / late / duplicate sequence numbers (atomicity)
A board message whose `board_seq` matches no pending entry is dropped and logged
(`unmatched_seq` counter). Never raise. A response arriving after its command
timed out is **normal**, not exceptional.

**Resolution is a single owner via pop-wins.** Both the timeout path and the
reader path resolve a pending command by:
`entry = pending.pop(board_seq, None)` with **no `await` between the lookup and
the pop**. Whoever pops first owns the outcome; the loser gets `None` and drops.
Only the winner calls `future.set_result(...)`, and it must still guard with
`if not future.done()`. The same discipline applies to the disconnect drain
(1.11) and e-stop (1.13).
*Pins:* Agent 1 (helpers), Agent 4 (pending table).

### 1.9 Max line length (receive-side)
Line limits are **receive-side**. The controller accepts inbound lines up to
8 KB; the board accepts inbound lines up to 1 KB. A sender's outbound message
must fit the **receiver's** limit: board outbound (schema, telemetry, responses)
must fit the controller's 8 KB; controller outbound commands must fit the
board's 1 KB. (Schema is the largest board-outbound payload and is sized against
the 8 KB ceiling, not 1 KB.) On overflow, the controller errors that connection
(catch asyncio `LimitOverrunError`); the board discards bytes until the next
newline and logs. Reject oversized JSON before parsing. The board parses into a
fixed-capacity `StaticJsonDocument`.
*Pins:* Agent 1, Agent 2, Agent 8.

### 1.10 Clock and latency semantics (monotonic)
Controller stamps `controller_ts` on each outbound command (board hop). The
board echoes that value back **untouched** in its response. Controller computes
RTT as `now - controller_ts` on its own clock; the board never interprets the
value, so there is no cross-device skew. Optional: board reports `board_proc_us`,
a receive-to-respond duration from local `micros()` (durations are skew-immune).
Pushed telemetry has no `controller_ts`; measure telemetry rate and jitter as
controller-side inter-arrival only. Do not compute one-way telemetry latency.

`controller_ts` must come from the controller's **monotonic** clock
(`time.monotonic()` / `loop.time()`), not wall-clock, so NTP steps or system
clock adjustments cannot produce negative or distorted latency. The value is an
opaque round-trip token: it is only ever compared to a later monotonic reading
**in the same controller process**, and must not be logged or interpreted as a
wall-clock time. (Human-readable log timestamps may use wall-clock separately.)
*Pins:* Agent 6 (metrics), Agent 1 (field meaning).

### 1.11 Commands to a not-yet-registered board; disconnect during in-flight
A command targeting a board not in `REGISTERED` (or blocked per 1.17 while
`estop_active`) is rejected immediately with `BOARD_UNAVAILABLE` (or
`ESTOP_ACTIVE`). Do not queue across a reconnect.

**Disconnect-during-in-flight handoff (cross-agent interface).** Disconnect is
detected by the connection manager (Agent 3); the pending table is owned by the
dispatcher (Agent 4). On detection, Agent 3 emits `board_down(board_id)`. Agent
4 drains that board's pending entries and its FIFO, failing each with
`BOARD_UNAVAILABLE` using the pop-wins discipline (1.8). This contract lives in
`interfaces.py` and must exist before either agent builds.
*Pins:* Agent 3, Agent 4.

### 1.12 Local client disconnect before response
If the client socket is gone when the board response (or timeout) resolves, the
reply is dropped and logged (`orphaned_response` counter). Client disconnect
does **not** cancel the in-flight board command; it may have physical side
effects and is allowed to complete.
*Pins:* Agent 2, Agent 4.

### 1.13 E-stop (software convergence only; honest about limits)
The hardwired interlock plus power cut is the safety layer. Software performs
convergence only, and the contract is explicit about its limits.

Software **can**: stop dispatching queued commands (clear controller-side
FIFOs), reject gated commands while `estop_active` (1.17), send an `estop`
message out-of-band as the next bytes written to each board (1.2), and converge
controller, Redis, and GUI state.

Software **cannot**: unsend bytes already written to a TCP socket (so a command
in flight may still execute and return `ok`); guarantee when `estop` reaches a
board (TCP plus the ~250 ms QNEthernet receive cadence); or stop a board whose
receive path has wedged.

Flow:
1. Board A trips (hardware has likely already cut A's drive) and sends an
   unsolicited `event: estop_triggered`.
2. Controller, in order: set `system.estop_active = true`; clear every per-board
   FIFO and fail those queued commands with `ESTOP_ACTIVE`; **leave any in-flight
   command to resolve naturally** (ok or timeout) -- do not synthesize a result
   for something that may have physically happened; broadcast `estop` to all
   connected boards (written out-of-band per 1.2: direct socket write, not a
   FIFO entry, does not wait on the in-flight slot); set each board's
   `estop_ack = false`, flip to `true` on the board's confirmation; mirror to
   Redis; notify all local clients via `event`.
3. Latching: controller never auto-clears. Reset requires the electrical
   condition cleared **and** an explicit operator `estop_reset` (with
   confirmation). Until then all gated commands (1.17) return `ESTOP_ACTIVE`.
4. Reconnect during e-stop: a reconnecting board traverses the connection axis
   normally to `REGISTERED`; the controller then sends it `estop` (sets its
   `estop_ack=false`). Dispatch remains blocked by the global `estop_active`
   flag, not by per-board state.
5. Idempotent: first trigger latches `estop_active`; concurrent triggers from
   multiple boards are logged no-ops.
*Pins:* Agent 1 (`event`/`estop`/`estop_reset`, `ESTOP_ACTIVE`), Agent 3
(reconnect re-assert, ack tracking), Agent 4 (dispatch gate).

### 1.14 Full-duplex local socket
The local socket is not pure request/response. The GUI reads continuously and
distinguishes `response` (correlated to its own request `seq`) from `event`
(server-initiated, no matching request). Required by e-stop and board-state
notifications.
*Pins:* Agent 1 (contract), Agent 2 (socket handler).

### 1.15 Board state: controller-authoritative, Redis-mirrored
The controller's in-memory per-board object is authoritative. Redis is a read
replica so other programs see state without polling the controller. The record
holds **connection state and safety state as separate fields** (2):
- Current value: hash `board:state:<id>` with `conn_state`
  (DISCONNECTED|CONNECTING|CONNECTED|REGISTERED|FAULTED), `estop_ack` (bool),
  `last_telemetry`, `last_seen`, `queue_depth`, `in_flight_board_seq`; and
  `system:state` with `estop_active` (bool) and `connected_count`.
- Live updates: on every transition, also `PUBLISH` the delta on
  `board:state:updates` (include a monotonic `event_id`). A late subscriber
  reads the hash once for current truth, then follows the channel.
If Redis is down, state still propagates to boards and GUI; Redis writes drop
per 1.7. **E-stop propagation never depends on Redis.**
*Pins:* Agent 5 (writer + pub/sub), Agent 6 (counters), Agent 3 (transitions).

### 1.16 Protocol-version mismatch
Schema (1.3) carries `protocol_version`. If it does not match the controller's
expected version, the controller logs the mismatch and moves the board to
`FAULTED` (then reconnect/backoff). It does **not** register or accept commands
for a mismatched board.
*Pins:* Agent 3, Agent 1.

### 1.17 Command gating during e-stop (schema-driven)
Each board command in the schema carries `blocked_by_estop: true|false`. While
`system.estop_active=true`, the dispatcher rejects any command with
`blocked_by_estop=true` (or absent) with `ESTOP_ACTIVE`, and allows commands
explicitly marked `blocked_by_estop=false` (status, fault, sensor reads) so the
operator can diagnose during an e-stop.
**Absent field defaults to `true` (fail safe):** a command that does not declare
itself is treated as motion and blocked. `estop_reset` is a client->controller
message, **not** a board command, and is never subject to this field; the
controller always accepts it (and acts only when the electrical condition is
cleared, per 1.13).
*Pins:* Agent 1 (schema field), Agent 3 (schema parse), Agent 4 (gate), Agent 8
(firmware declares per command).

### 1.18 Local client event backpressure
Each connected local client has a bounded outbound queue (default 1000). On
overflow, drop oldest **non-critical** events (state deltas, telemetry-derived)
and increment `client_event_dropped`. `response` and `estop`/safety events are
**never dropped**; if the queue is saturated with critical messages, the
controller disconnects that client rather than block the broadcast loop. One
slow GUI must never stall event delivery to others or the command path.
*Pins:* Agent 2.

### 1.19 Per-board write serialization
Every board connection has **exactly one serialized write path.** Normal
commands and out-of-band `estop` writes must both acquire the same per-board
writer lock, or both go through a single per-board writer task. `estop` bypasses
the command FIFO and the in-flight command slot, but it does **not** bypass
socket write serialization. No two coroutines may call `writer.write()` /
`writer.drain()` on the same board stream concurrently.

```
FIFO bypass:                    yes
in-flight command slot bypass:  yes
socket-byte interleaving:       no
```

*Reason:* out-of-band `estop` (1.2) can be written while a normal command is
mid-write; without a shared writer lock or single writer task, two coroutines
interleave bytes on one TCP stream and corrupt newline-JSON framing.
*Pins:* Agent 1 (writer helper / lock primitive), Agent 3 (owns the per-board
writer task/lock), Agent 4 (acquires it for command writes), e-stop path
(acquires it for `estop` writes).

### 1.20 estop_ack event contract
A board confirms receipt/handling of `estop` by sending an unsolicited
`event: estop_ack` (shape in 3.13). The controller sets `board.estop_ack=true`
**only** after receiving this event. A missing ack is logged and observable
(surfaced per-board) but does **not** prevent the system from being globally in
e-stop: `system.estop_active` latches independently of any board's ack (2.2).
*Pins:* Agent 1 (event shape), Agent 3 (ack tracking -> `estop_ack`), Agent 8
(firmware emits it), Agent 7 (mock emits it).

---

## 2. State model: two orthogonal axes

`ESTOPPED` is **not** a connection state. Connection state and safety state are
independent; a board can be `DISCONNECTED` while the system is in e-stop, and a
board can be `REGISTERED` while `estop_active` blocks its commands.

### 2.1 Connection axis (per board)
```
DISCONNECTED -> CONNECTING -> CONNECTED -> REGISTERED
      ^                                         |
      |                                         v
      +------------------ FAULTED <-------------+
```
- `CONNECTING`: TCP connect in progress.
- `CONNECTED`: TCP up, schema not yet received.
- `REGISTERED`: schema received, version OK; commands accepted (unless gated by
  the safety axis).
- `FAULTED`: registration timeout, liveness loss, protocol/version error, or
  read failure. Triggers reconnect with backoff (1.4), returning to
  `DISCONNECTED`/`CONNECTING`.

### 2.2 Safety axis (global)
```
system.estop_active : bool   (latched true; cleared only by operator estop_reset)
board.estop_ack     : bool   (per board: has this board confirmed safe state)
```
- The **dispatch gate** is: reject commands per 1.17 if `system.estop_active`,
  regardless of any board's connection state.
- `estop_ack` is observability/convergence, not a gate. The system does not wait
  on acks to consider itself in e-stop; a board may be unreachable.

---

## 3. Message Contract (newline-delimited JSON)

One JSON object per line, terminated by `\n`. Receive-side line limits: 8 KB
controller, 1 KB board (1.9).

### 3.1 Common fields
`type`, `seq`, `timestamp` (sender-local, informational only), `source`,
`target`, plus payload-specific fields.

`seq` discipline: on the client->controller `command`, `seq` is the client's own
value (unique among that client's outstanding requests). The controller rewrites
`source`, `target`, and `seq` (to `board_seq`) on the board hop, and restores the
client's `seq` on the client-facing `response`. `result.board_seq` exposes the
board seq for debugging. Top-level `seq` and `result.board_seq` are never equal.

### 3.2 Message types

| type           | direction                         | notes |
|----------------|-----------------------------------|-------|
| `command`      | client->controller->board         | board hop carries `controller_ts` (monotonic) |
| `response`     | board->controller->client         | echoes `controller_ts`; carries `status` |
| `telemetry`    | board->controller (push, 50 ms)   | unsolicited, one-way |
| `schema`       | board->controller (on connect)    | first message; carries `protocol_version`, `blocked_by_estop` per command |
| `event`        | server-initiated or board->controller | board_disconnected, estop_triggered, estop_ack, state change; carries `event_id` |
| `estop`        | controller->board (broadcast)     | out-of-band write; bypasses FIFO and in-flight slot |
| `estop_reset`  | client->controller                | operator-only; not a board command; clears latch if condition cleared |
| `heartbeat`    | controller->board (optional)      | RX-path insurance only; not for liveness |

### 3.3 Command, client -> controller
```json
{"type":"command","seq":12,"timestamp":1710000000.100,
 "source":"gui","target":"motor_controller",
 "command":"set_speed","args":{"rpm":1200}}
```

### 3.4 Command, controller -> board (rewritten hop)
```json
{"type":"command","seq":1042,"controller_ts":81234.567,
 "source":"controller","target":"motor_controller",
 "command":"set_speed","args":{"rpm":1200}}
```
`seq` here is the controller-owned `board_seq`. `controller_ts` is a monotonic
token (1.10), echoed untouched in the response. The client's original `seq` (12)
lives only in the pending entry and is restored on the response.

### 3.5 Response, board -> controller -> client
```json
{"type":"response","seq":12,"controller_ts":81234.567,
 "source":"controller","target":"gui","status":"ok",
 "result":{"accepted":true,"board":"motor_controller","board_seq":1042,
           "latency_ms":20.4,"board_proc_us":850},
 "error":null}
```
Top-level `seq` = client's seq (12). `result.board_seq` = controller seq (1042).
`latency_ms` is controller-measured (`now - controller_ts`, monotonic).
`board_proc_us` is optional, board-measured duration.

### 3.6 Telemetry (push)
```json
{"type":"telemetry","seq":441,"timestamp":1710000001.000,
 "source":"motor_controller","target":"controller",
 "telemetry":{"rpm":1180,"temperature_c":41.2,"voltage":24.1}}
```

### 3.7 Schema (on connect / reconnect)
```json
{"type":"schema","seq":1,"timestamp":1710000000.000,
 "source":"motor_controller","target":"controller",
 "protocol_version":"1",
 "schema":{"commands":{"set_speed":{"args":{"rpm":"int"},"blocked_by_estop":true},
                       "get_status":{"args":{},"blocked_by_estop":false}},
           "telemetry":{"rpm":"int","temperature_c":"float","voltage":"float"},
           "state":{"mode":"string","faulted":"bool"},
           "firmware_version":"..."}}
```
A command without `blocked_by_estop` defaults to blocked (1.17).

### 3.8 Event (server-initiated, incl. e-stop)
```json
{"type":"event","event_id":90412,"timestamp":1710000002.000,
 "source":"controller","event":"estop_triggered","origin_board":"board_a",
 "details":{"reason":"interlock_gpio"}}
```

### 3.9 E-stop reset (operator)
```json
{"type":"estop_reset","seq":7,"timestamp":1710000050.0,
 "source":"gui","target":"controller"}
```

### 3.13 E-stop ack (board -> controller, unsolicited)
```json
{"type":"event","timestamp":1710000002.100,
 "source":"motor_controller","target":"controller",
 "event":"estop_ack","details":{"state":"safe"}}
```
Sent by a board after it receives and applies `estop`. Sets `board.estop_ack`
(1.20). Unsolicited; not a command response; not gated by 1.17.

### 3.10 Error object
```json
{"code":"ESTOP_ACTIVE","message":"command rejected: system is in e-stop"}
```

### 3.11 Error codes
`INVALID_JSON`, `MISSING_FIELD`, `INVALID_TYPE`, `UNKNOWN_TARGET`,
`UNKNOWN_COMMAND`, `INVALID_ARGUMENT`, `BOARD_UNAVAILABLE`, `BOARD_BUSY`,
`COMMAND_TIMEOUT`, `ESTOP_ACTIVE`, `PROTOCOL_VERSION_MISMATCH`,
`CONTROLLER_SHUTDOWN`, `INTERNAL_ERROR`.

### 3.12 Status values
`ok`, `error`, `timeout`. (`timeout` carries `COMMAND_TIMEOUT`.)

---

## 4. Counters / metrics (minimum set)
`obs_dropped`, `unmatched_seq`, `orphaned_response`, `board_busy_rejections`,
`estop_rejections`, `stale_command_rejections`, `reconnect_count`,
`registration_timeouts`, `protocol_version_mismatches`, `client_event_dropped`,
plus per-command latency (p50/p95/p99 from monotonic `controller_ts`) and
telemetry inter-arrival rate/jitter per board. Surface per-board `conn_state`
and `estop_ack`.

---

## 5. Shared contracts to implement FIRST (merge-chaos prevention)

Conflict hotspots: Agent 3 <-> Agent 4 (pending lifecycle + board state),
Agent 4 <-> e-stop (global flag + dispatch gate + out-of-band estop write),
Agent 5 <-> Agent 6 (counters + queue). Ship these before any feature work:

1. **`protocol.py`** (Agent 1): message types, error codes, validation,
   line-length enforcement, seq rules, pop-wins resolution helper,
   `blocked_by_estop` parsing with absent=blocked default.
2. **`state.py`** (shared): `BoardConnState` enum; `SystemState` (with
   `estop_active`); per-board `estop_ack`; `PendingCommand` dataclass; the Redis
   `BoardStateRecord` schema with **separate** `conn_state` and `estop_ack`
   fields.
3. **`interfaces.py`** (shared): the `board_down(board_id)` notification (Agent 3
   -> Agent 4); pending-table ownership statement; the client-reply handle so
   orphaned responses (1.12) are handled in one place; the out-of-band
   `send_estop(board_id)` path (Agent 4/controller) that bypasses the FIFO and
   in-flight slot but acquires the per-board writer lock (1.19); the per-board
   writer-lock / single-writer-task handle shared by command and estop writes.

### Agent pin summary

| Agent | Owns | Pinned by |
|-------|------|-----------|
| 1 Protocol | message types, error codes, parse/validate, line + seq rules, pop-wins helper, blocked_by_estop default, writer-serialization primitive, estop_ack shape | 1.1, 1.8, 1.9, 1.10, 1.13, 1.14, 1.16, 1.17, 1.19, 1.20 |
| 2 Unix socket server | full-duplex local socket, response/event split, line limits, client-disconnect, client event backpressure, socket lifecycle | 1.9, 1.12, 1.14, 1.18 |
| 3 Board connection manager | connection axis, schema-on-connect, version check, backoff, liveness, board_down, estop re-assert + ack, blocked_by_estop parse, per-board writer task/lock | 1.3, 1.4, 1.6, 1.11, 1.13, 1.15, 1.16, 1.17, 1.19, 1.20 |
| 4 Command dispatcher | controller seq, per-board FIFO + residency cap, exec timeout, reject rules, pending pop-wins, e-stop gate, out-of-band estop write (via shared writer lock) | 1.1, 1.2, 1.5, 1.8, 1.11, 1.12, 1.13, 1.17, 1.19 |
| 5 Telemetry + Redis writer | single obs queue, drop-oldest mechanism, hash + pub/sub mirror, retention | 1.6, 1.7, 1.15 |
| 6 Logging + metrics | controller-measured (monotonic) latency, counters, jitter, surfaced state | 1.7, 1.8, 1.10, 1.15 |
| 7 Mock board | push telemetry, schema+version+blocked_by_estop, estop_ack event, delays, malformed, wedged-RX | 1.3, 1.6, 1.9, 1.13, 1.16, 1.17, 1.20 |
| 8 Teensy firmware | 50 ms telemetry timer, fixed JSON, fail-safe, estop + estop_ack emit, schema push, version, blocked_by_estop decl | 1.3, 1.6, 1.9, 1.12, 1.13, 1.16, 1.17, 1.20 |

**Ship `protocol.py`, `state.py`, `interfaces.py` first.** Everything else
builds against them.

---

## 6. Test coverage

### 6.1 Core (from proposal section 13, retained)
Single local request; single board TCP; sustained command; telemetry recording;
multi-board routing; malformed local request; malformed board message; board
disconnect; board reconnect; Redis outage; load.

### 6.2 Concurrency and race tests (must-have)
- **Timeout/late-response boundary:** response arrives at the timeout instant;
  assert no double-resolution (pop-wins), exactly one outcome delivered.
- **Two coroutines race the same pending entry:** assert pop-wins, loser drops.
- **Disconnect mid-in-flight:** `board_down` fires; pending + FIFO failed with
  `BOARD_UNAVAILABLE`; no orphaned futures.
- **Client disconnect before response:** `orphaned_response` logged; board
  command still completes.
- **Stale queued command:** command queued behind a slow peer past its residency
  cap; rejected with `COMMAND_TIMEOUT`, never written to the board.
- **FIFO full:** `BOARD_BUSY` on overflow; queued commands never silently
  dropped.

### 6.3 E-stop tests
- **E-stop with one command in flight and several queued:** in-flight resolves
  naturally (ok or timeout), queued rejected `ESTOP_ACTIVE`, flag latched, all
  connected boards sent `estop`.
- **Out-of-band write ordering:** `estop` is written to a board while a normal
  command is still pending; assert `estop` does not wait on the in-flight slot
  and is the next controller write to that board.
- **Non-motion command under e-stop:** a `blocked_by_estop=false` command (e.g.
  `get_status`) is still dispatched while `estop_active=true`; a
  `blocked_by_estop=true` and a field-absent command are both rejected.
- **E-stop idempotency:** 3 boards trigger near-simultaneously; single latch,
  others logged no-ops.
- **Reconnect during e-stop:** board reaches `REGISTERED`, dispatch still
  rejected by global flag, `estop` re-sent, `estop_ack` reset then set on confirm.
- **Reset gating:** `estop_reset` succeeds only with condition cleared; emits
  state events; gated commands accepted again afterward.
- **E-stop with Redis down:** boards + GUI still converge; Redis writes drop.
- **Write serialization under estop+command race:** force a normal command write
  and an out-of-band `estop` write to the same board concurrently; assert bytes
  are not interleaved (every received line is valid JSON, framing intact) and the
  shared per-board writer lock / single writer task is the serialization point.
- **estop_ack tracking:** board sends `event: estop_ack`; assert
  `board.estop_ack` flips true only after the event. A board that never acks:
  `estop_ack` stays false, is observable, and `system.estop_active` remains
  latched regardless.

### 6.4 Queue / backpressure
- Sustained drop-oldest on `obs_queue`: oldest evicted, newest retained,
  `obs_dropped` exact; no unbounded growth.
- Redis down for an extended period: command path and e-stop unaffected; queue
  caps at maxsize, counter climbs.
- **Slow local client:** non-critical events dropped (`client_event_dropped`
  climbs) while `response` and `estop` are still delivered; a client saturated
  with critical messages is disconnected, broadcast loop never stalls.

### 6.5 Protocol / framing
- Oversized line to controller (>8 KB): connection errored cleanly, no crash.
- Oversized line to board (>1 KB): discard-to-newline, board stays up.
- **Large schema (>1 KB, <8 KB) board-outbound:** accepted by the controller
  (receive-side limit), not rejected against the 1 KB board limit.
- Malformed JSON, missing fields, wrong types: correct error codes.
- Seq disambiguation: client `seq` and `board_seq` never conflated across a full
  round trip; two clients with colliding `seq` both matched correctly.
- Protocol-version mismatch: board FAULTed, `PROTOCOL_VERSION_MISMATCH`, not
  registered.
- Unknown board id on connect: rejected.

### 6.6 Redis mirroring
- On each transition: hash updated and delta published with monotonic `event_id`.
- Late subscriber: reads correct current state from the hash, then follows the
  channel without gaps.

### 6.7 QNEthernet / timing
- Measure actual telemetry inter-arrival under load; confirm 50 ms holds and has
  not collapsed toward 250 ms.
- If a solicited heartbeat-ack is used, confirm it tolerates the ~250 ms receive
  cadence without false FAULTs.
- Wedged-RX board (timer still pushes telemetry, receive path stalled): confirm
  optional heartbeat-ack catches it if enabled; document behaviour if not.

### 6.8 Clock
- **Monotonic latency under wall-clock step:** simulate an NTP/system-clock jump
  (forward and backward) mid-flight; assert `latency_ms` stays non-negative and
  undistorted (proves monotonic source).

### 6.9 Lifecycle
- SIGTERM mid-flight: new requests refused, drain window honored, remainder
  `CONTROLLER_SHUTDOWN`, boards fail-safe.
- **Stale socket on startup:** dead socket path unlinked and recreated; a live
  controller on the path causes refuse-to-start.

---

## 7. Open judgment calls (settle with codex, not silently)
1. **FIFO depth** (1.2): set to 6 as a conservative default; tune to real burst
   pattern.
2. **Queue topology** (1.7): collapsed to one `obs_queue`; revisit only if
   telemetry enrichment proves CPU-heavy enough to want a separate parse stage.
3. **Optional RX-insurance heartbeat** (1.6): include only if a wedged board
   receive path with a still-running send timer is a realistic firmware failure.
4. **Redis retention numbers** (0): 100k telemetry / 7-day history are starting
   points, not measured.
5. **Client outbound queue depth** (1.18): 1000 default, tune to event volume.

# Board Function and Schema Registration Guide (v2 FROZEN)

> **Authority note.** This guide is a developer-facing companion to the frozen
> contract `V1_Networking_Decisions.md` (v3 FROZEN). Where this guide and the
> frozen contract ever disagree, **the contract wins.** Section references like
> (1.6), (1.20) point at the contract decision that governs the rule.

## Purpose

This guide explains what low-level board developers need to provide so their
board can work with the V1 networking system.

Board developers do **not** need to write networking code.

The networking layer will handle:

* TCP communication
* JSON parsing and message framing (fixed-capacity, see the constraint box below)
* Schema transmission
* Command dispatch
* Response formatting
* Telemetry transmission (one-way push, see section 8)
* E-stop message handling and `estop_ack` framing (you still own the safe-state
  hook, see section 10)

Board developers only need to provide:

1. A board ID.
2. A protocol version.
3. A firmware version.
4. A schema describing available commands, telemetry, and state.
5. Functions that implement the registered commands.
6. Telemetry values.
7. Local safe behavior for e-stop or controller loss, where applicable.

The main rule is:

```text
The controller only interacts with the board through registered functions.
```

If a capability is not registered in the schema, the controller should not call it.

> **Embedded constraint box (read first).** On a Teensy there is no separate
> networking process: the "networking layer" is library code linked into your
> firmware. Two hard limits follow from the contract (1.9):
> * Inbound command lines are capped at **1 KB** and parsed into a
>   **fixed-capacity** buffer. **No dynamic JSON allocation.** Keep `args` small
>   and flat.
> * Your full schema is sent *outbound* and is sized against the controller's
>   **8 KB** receive limit, so the schema itself is not 1 KB-limited. Only
>   per-command `args` must stay minimal.

---

## 1. Board Identity

Each board must provide a static board ID. The board ID must match the
controller configuration.

Required identity fields:

```text
board_id
protocol_version
firmware_version
```

Example values:

```text
board_id: motor_controller
protocol_version: 1
firmware_version: 0.1.0
```

Do not change board IDs casually. The controller uses the board ID to route
commands, and the board ID appears in `source` on every outbound message.

> **Coordinated values / failure modes (contract 0, 1.16).**
> * An **unknown `board_id`** is rejected at connect; the board will not register.
> * `protocol_version` is a **coordinated** value. If it does not match what the
>   controller expects, the controller refuses to register the board and marks it
>   `FAULTED`, then reconnects in a loop. **Do not bump it unilaterally**;
>   change it only alongside the controller's expected version.

---

## 2. Command Registration

Every callable board behavior must be registered as a command.

Each command registration must include:

```text
command name
argument schema
blocked_by_estop
function handler
```

The networking layer will use this information to:

1. Build the board schema.
2. Tell the controller what commands exist.
3. Validate incoming command names.
4. Call the correct function when a command arrives.
5. Format the function result into a protocol response.

Board developers should think of the schema as the board's public API.

---

## 3. Schema Format

Each board must provide a schema with this general structure:

```json
{
  "type": "schema",
  "seq": 1,
  "timestamp": 1710000000.000,
  "source": "<board_id>",
  "target": "controller",
  "protocol_version": "1",
  "schema": {
    "commands": {
      "<command_name>": {
        "args": {
          "<arg_name>": "<arg_type>"
        },
        "blocked_by_estop": true
      }
    },
    "telemetry": {
      "<telemetry_field>": "<field_type>"
    },
    "state": {
      "<state_field>": "<field_type>"
    },
    "firmware_version": "<firmware_version>"
  }
}
```

Example schema:

```json
{
  "type": "schema",
  "seq": 1,
  "timestamp": 1710000000.000,
  "source": "motor_controller",
  "target": "controller",
  "protocol_version": "1",
  "schema": {
    "commands": {
      "set_speed": {
        "args": {
          "rpm": "int"
        },
        "blocked_by_estop": true
      },
      "get_status": {
        "args": {},
        "blocked_by_estop": false
      }
    },
    "telemetry": {
      "rpm": "int",
      "temperature_c": "float",
      "voltage": "float"
    },
    "state": {
      "mode": "string",
      "faulted": "bool"
    },
    "firmware_version": "0.1.0"
  }
}
```

> The library sends this schema as the **first message on every connect and
> reconnect** (contract 1.3). You provide the schema once; the library handles
> resending it.

---

## 4. `blocked_by_estop`

Every command must declare whether it is blocked during e-stop (contract 1.17).

Use:

```json
"blocked_by_estop": true
```

for commands that move hardware, change actuator output, or could affect
physical behavior.

Use:

```json
"blocked_by_estop": false
```

only for commands that are safe during e-stop, such as status reads, fault
reads, or sensor snapshots. Keeping these available is the point: the operator
diagnoses *during* an e-stop.

If `blocked_by_estop` is missing, the controller treats it as `true`
(fail-safe). When unsure, mark the command as blocked.

---

## 5. Argument Types

Keep argument schemas simple and flat (remember the 1 KB inbound line limit).

Preferred V1 argument types:

```text
int
float
bool
string
```

Example:

```json
"args": {
  "rpm": "int",
  "enabled": "bool"
}
```

Avoid complex nested structures. A command whose arguments cannot fit a small,
flat object inside a 1 KB line is a sign the command should be split.

---

## 6. Function Expectations

Each registered command should map to one board-local function.

A command function should:

* Accept the arguments declared in the schema.
* Validate its inputs.
* Perform one clear board-local action or query.
* Return success or error.
* **Not block.**
* Avoid communicating with the GUI, Redis, or other boards.

The controller owns orchestration. The board function owns local behavior.

> **Non-blocking is a hard requirement (contract 1.2, 1.5).** Handlers run under
> a **one-command-in-flight-per-board** model with a **2 s default timeout
> (10 s hard ceiling).** A handler that blocks stalls *every* other command to
> this board and will trip the execution timeout. For any action that is not
> near-instant (a motor ramp, a long move), **start the action and return `ok`
> immediately**; report progress and completion through telemetry/state, not by
> blocking the handler.
>
> **Normal handlers vs safety hooks (not a contradiction).** Normal command
> handlers must be non-blocking and start-and-return. The e-stop and
> controller-loss hooks in section 10 are different: they should do the *minimum*
> required to put the board into local safe state, and return only once that safe
> action has actually been applied. Different jobs, different rules.

> **You never touch `controller_ts` (contract 1.10).** You may see it in the
> wire format; it is a controller-owned round-trip token the library echoes back
> untouched. Do not read, write, or interpret it.

---

## 7. Example Function Shape

This is the only function pattern board developers need to follow conceptually:

```text
function command_name(args):
    validate args

    if args are invalid:
        return error(code, message)

    perform board-local action or query   # start-and-return if slow

    return ok(result)
```

Example (note: returns immediately after *commanding* the speed, does not wait
for the motor to reach it):

```text
function set_speed(args):
    rpm = args["rpm"]

    if rpm is not valid:
        return error("INVALID_ARGUMENT", "rpm is invalid")

    command speed locally   # non-blocking; actual rpm reported via telemetry

    return ok({
        "accepted": true,
        "rpm": rpm
    })
```

The exact function signature may depend on the low-level networking layer, but
the behavior should follow this pattern.

---

## 8. Telemetry Registration

Each board should declare the telemetry fields it provides.

Example:

```json
"telemetry": {
  "rpm": "int",
  "temperature_c": "float",
  "voltage": "float"
}
```

> **Telemetry is a one-way push on a free-running timer (contract 1.6).** The
> networking layer reads your telemetry **snapshot on its own ~50 ms timer and
> pushes it one-way** to the controller. It is **never** request/response. You do
> not wait to be asked. Provide a **fast, non-blocking getter** that returns the
> latest values from your control loop; do not poll, wait, or block inside it.
> Telemetry also serves as the board's **liveness signal**, so it must keep
> flowing. If telemetry stops, the controller treats the board as faulted.

---

## 9. State Registration

Each board should declare useful state fields.

Example:

```json
"state": {
  "mode": "string",
  "faulted": "bool"
}
```

State fields should describe board-local status that helps the controller, GUI,
or logs understand what the board is doing. Provide a **current-value getter**;
the controller reads/mirrors state on transitions, so the getter must be fast
and non-blocking like the telemetry getter.

---

## 10. E-stop and Controller Loss Hooks

Provide two idempotent local hooks:

```text
on_estop_received()    -> drive board to local safe state
on_controller_lost()   -> drive board to local safe state
```

Both must be **safe to call more than once.** A reconnect during an active
e-stop will re-invoke `on_estop_received()` (contract 1.13 step 4).

> **`estop_ack` timing (contract 1.20, 3.13).** The networking library formats
> and frames the `estop_ack` event, but emits it **only after
> `on_estop_received()` returns.** The ack confirms the board has actually
> applied its safe state. It is not a "bytes received" acknowledgement. So:
> * **Do not return from the hook until the board is actually safe.**
> * The ack payload is `{"state":"safe"}`.
> * If the hook cannot confirm safe state, return failure through the
>   library-provided hook result mechanism once that API is defined. **Do not
>   falsely report safe.** When the hook reports failure, the library can withhold
>   or qualify the ack. A missing ack is observable and does **not** prevent the
>   system from being globally in e-stop.

Remember: **software e-stop is convergence only.** The hardwired interlock and
power cut are the real safety layer; assume drive may already be physically cut
when your hook runs.

---

## 11. What Board Developers Should Not Implement

Board developers should not implement:

* TCP socket logic
* JSON parsing or message framing
* Redis communication
* GUI communication
* Cross-board routing
* Controller logic
* Protocol response formatting
* `estop_ack` framing/transport (you own the safe-state hook; the library owns
  the message)

Those are networking-layer responsibilities.

Board developers should implement:

* Registered command functions (non-blocking)
* Schema information
* Telemetry values (fast getter)
* State values (fast getter)
* Local e-stop behavior (idempotent hook)
* Local controller-loss behavior (idempotent hook)

---

## 12. Board Developer Checklist

For each board, provide:

* [ ] Static board ID (matches controller config)
* [ ] Protocol version (coordinated; do not bump unilaterally)
* [ ] Firmware version
* [ ] Schema with commands
* [ ] Schema with telemetry fields
* [ ] Schema with state fields
* [ ] `blocked_by_estop` for every command (absent = blocked)
* [ ] One function per registered command
* [ ] Command handlers are non-blocking (start-and-return for slow actions)
* [ ] Telemetry getter is non-blocking and fast (read under a ~50 ms timer)
* [ ] State value getter is non-blocking, if state is provided
* [ ] `args` stay small and flat (1 KB inbound line limit)
* [ ] e-stop / controller-loss hooks are idempotent
* [ ] `estop_ack` emitted only after safe state is applied (`{"state":"safe"}`;
      library sends it, you just make the hook return when truly safe)

Keep the board interface simple: register what the board can do, then let the
networking layer expose it to the controller.
