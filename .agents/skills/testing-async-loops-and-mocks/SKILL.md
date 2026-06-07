---
name: testing-async-loops-and-mocks
description: >
  Read before writing or changing tests for the Hyperloop asyncio controller,
  protocol, board connection, or Unix socket code. Covers deterministic asyncio
  tests (no arbitrary sleeps), mocking asyncio StreamReader/StreamWriter for TCP
  and Unix sockets, driving timeouts with injectable clocks, and exercising
  disconnects, late/duplicate responses (pop-wins), and e-stop paths without
  flakiness. Use for anything under tests/.
---

# Testing Async Loops and Mocks

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md`
> (v3 FROZEN) is the source of truth; also read the `asyncio-controller` and
> `project-networking-invariants` skills. Refs like (1.5) point at the governing
> contract decision. The contract wins on any conflict.

These tests guard timeout, disconnect, pop-wins, and e-stop behavior. They must
be **deterministic**: a control-system test that passes 95% of the time is worse
than no test. The whole suite runs in well under a second today; keep it that way.

## Core rules

* **No arbitrary `asyncio.sleep()` to "wait for" something.** Sleeping to let a
  coroutine make progress is the #1 source of flake. Synchronize on a real
  signal instead: `await event.wait()`, `await queue.get()`, or `await` the
  future/task you actually care about.
* **Drive timeouts with an injectable clock, not wall-clock.** The controller's
  two timeout clocks (execution timeout from board-write; queue-residency cap,
  1.5) should take a `time_source` / clock callable so a test can advance time
  instantly. Never make a test sleep 2 s to observe a 2 s timeout.
* **Match the existing style.** Tests use `unittest` with `IsolatedAsyncioTestCase`
  for async cases and are imported as top-level modules (run `pytest` from the
  repo root). Follow the patterns already in `tests/`.
* **One assertion target per test.** Disconnect mid-flight, late response after
  timeout, and BOARD_BUSY are separate tests, not one mega-test.

## Mocking streams (TCP boards and Unix clients)

Both board TCP and the local Unix socket are newline-JSON over
`asyncio.StreamReader` / `asyncio.StreamWriter` (see `newline-json-protocol`).
Prefer the lightest fake that exercises the real framing:

* **Reader**: a real `asyncio.StreamReader` you `feed_data(...)` /
  `feed_eof()` is usually simpler and more faithful than a mock. Feeding raw
  bytes (with and without the trailing `\n`) tests the framing path for real.
* **Writer**: a small fake exposing `write(bytes)`, `drain()` (async, no-op or a
  controllable awaitable for backpressure tests), `close()`, and
  `wait_closed()`, capturing written bytes for assertions. Use the awaitable
  `drain()` to simulate a slow/stalled peer when testing backpressure (1.18).
* Decode captured writes back through `protocol.parse_message` and assert on the
  message dict, not on a brittle byte string.

## Exercising the behaviors that matter

* **Disconnect**: `feed_eof()` (clean) or raise from the read to simulate an
  abrupt drop (1.11). Assert in-flight commands resolve terminally and the
  connection axis flips — connection state and e-stop state stay orthogonal (2).
* **Timeout**: write the command, advance the injected clock past the limit,
  assert `COMMAND_TIMEOUT` with `timeout` status (never a new status), and assert
  a stale queued command is rejected on pop rather than written (1.5).
* **Pop-wins**: deliver a board response for a `board_seq` that was already
  timed out or never existed; assert it is dropped/logged (`unmatched_seq`),
  the future is not double-resolved, and nothing is raised (1.8).
* **E-stop**: assert queued commands fail `ESTOP_ACTIVE`, in-flight commands are
  left to resolve naturally, and the out-of-band `estop` write still goes
  through the per-board writer lock (1.13, 1.19).

## Anti-patterns (do not)

* Do not assert on timing (`elapsed < 0.1`) — assert on outcomes.
* Do not patch private internals when feeding bytes through the public framing
  path would prove the same thing.
* Do not leak tasks: cancel/await background tasks in teardown so one test's
  pending work cannot fail the next.
* Do not introduce real Redis or a real socket file in a unit test; use the
  disabled/no-op observability mode and in-memory fakes.

## Definition of done

The changed timeout / disconnect / pop-wins / e-stop behavior is covered;
tests are deterministic with no arbitrary sleeps; no real-time waits longer than
a few ms; no leaked tasks or sockets; `pytest` passes from the repo root.
