---
name: newline-json-protocol
description: >
  Read before writing or modifying message parsing, framing, validation, or
  serialization for the Hyperloop board networking stack. Covers newline-delimited
  JSON framing, receive-side line limits (8 KB controller / 1 KB board), the
  client seq vs controller-owned board_seq distinction, pop-wins resolution,
  dropping late/duplicate responses, structured-error-not-crash handling, and the
  fixed terminal status set. Use for protocol.py and any message-handling code.
---

# Newline-JSON Protocol (v1)

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md` (v3 FROZEN) is
> the source of truth; also read the `project-networking-invariants` skill. Refs
> like (1.9) point at the governing contract decision. The contract wins on any
> conflict.

The v1 protocol is **newline-delimited JSON**: one JSON object per line,
terminated by `\n`, over both the Unix socket and board TCP. Keep parsing
separate from routing. Malformed input produces a structured error; it never
crashes a process.

## Framing and line limits (receive-side)

* One JSON object per line, `\n`-terminated. TCP and Unix sockets are byte
  streams, so framing is required.
* **Line limits are receive-side** (1.9):
  * Controller accepts inbound lines up to **8 KB**. On overflow, error that
    connection (catch asyncio `LimitOverrunError`).
  * Board accepts inbound lines up to **1 KB**, parsed into a **fixed-capacity**
    buffer (no dynamic allocation). On overflow, discard bytes to the next
    newline and log.
* A sender's outbound message must fit the **receiver's** limit. Board-outbound
  schema (the largest board message) is sized against the controller's 8 KB, not
  1 KB. Controller-outbound commands must fit the board's 1 KB.
* Reject oversized JSON before parsing.

## Sequence numbers: `seq` vs `board_seq`

* The controller owns a per-board monotonic `board_seq` (uint64, from 1).
* The client's `seq` is opaque to the controller and is **echoed back unchanged**
  on the client-facing `response`.
* On the board hop the controller rewrites `source`, `target`, and `seq` (to
  `board_seq`), and carries a monotonic `controller_ts` token.
* On the client response: top-level `seq` = client's seq; `result.board_seq` =
  controller seq. **They are never equal and must never be conflated** (1.1, 3.1).
* `controller_ts` is an opaque round-trip token from the controller's **monotonic**
  clock; the board echoes it untouched, the controller measures RTT against it.
  Never interpret it as wall-clock or log it as a timestamp (1.10).

## Message types

`command`, `response`, `telemetry` (board push, ~50 ms, unsolicited), `schema`
(board push on connect/reconnect, carries `protocol_version` and per-command
`blocked_by_estop`), `event` (server-initiated or board->controller, e.g.
`estop_triggered`, `estop_ack`, with `event_id`), `estop` (controller->board,
out-of-band), `estop_reset` (client->controller, not a board command),
`heartbeat` (optional). See contract section 3 for exact shapes.

## Pop-wins resolution; late and duplicate responses

* A board message whose `board_seq` matches no pending entry is **dropped and
  logged** (`unmatched_seq`), **never raised**. A response arriving after its
  command timed out is normal, not exceptional.
* Resolution is single-owner: `entry = pending.pop(board_seq, None)` with **no
  `await` between lookup and pop**; the winner resolves the future (guard
  `if not future.done()`), the loser drops. Duplicates resolve to a drop (1.8).
* Provide this as a shared helper in `protocol.py` so every reader enforces it
  identically.

## Validation: structured errors, never crashes

* Validate `type`, the fields required for that message type, and the command/
  response routing fields such as `seq`, `source`, and `target` where the
  contract requires them. (Do not force command-style routing fields onto
  schema, telemetry, or event messages if the contract defines those
  differently.) Validate command names and arg types against the board schema.
* Any malformed or invalid input yields a structured error response with one of
  the contract error codes, not an exception that propagates to the event loop
  (1.9, 12-context).
* Error object shape: `{"code": "<ERROR_CODE>", "message": "<human text>"}`.

## Terminal statuses (fixed set)

Exactly three (3.12):

```
ok | error | timeout
```

**No new status values.** Rejections, disconnects, busy, unavailable, e-stop,
and shutdown are **error codes** carried on an `error`/`timeout` response (3.11):

```
INVALID_JSON  MISSING_FIELD  INVALID_TYPE  UNKNOWN_TARGET  UNKNOWN_COMMAND
INVALID_ARGUMENT  BOARD_UNAVAILABLE  BOARD_BUSY  COMMAND_TIMEOUT  ESTOP_ACTIVE
PROTOCOL_VERSION_MISMATCH  CONTROLLER_SHUTDOWN  INTERNAL_ERROR
```

## Serialization

* Serialize responses consistently; preserve the client `seq` on the way back and
  expose `result.board_seq` for debugging.
* One JSON object per line, `\n`-terminated, within the receiver's line limit.

## Tests this skill expects

Framing/partial-read at buffer boundary; oversized line (controller >8 KB errors
cleanly; board >1 KB discards-to-newline and stays up); large schema (>1 KB,
<8 KB) accepted by controller; malformed JSON / missing fields / wrong types ->
correct error codes, no crash; `seq` vs `board_seq` never conflated across a
round trip; two clients with colliding `seq` both matched; late/duplicate
`board_seq` dropped and logged with no double-resolution; monotonic `controller_ts`
stays non-negative across a simulated wall-clock step.

## Definition of done

Tests cover changed protocol behavior; malformed input never crashes; no new
`status` values; `seq`/`board_seq` distinction preserved. Protocol behavior
changes require updating `docs/contracts/V1_Networking_Decisions.md`, the
relevant tests, and AGENTS.md in the same change.
