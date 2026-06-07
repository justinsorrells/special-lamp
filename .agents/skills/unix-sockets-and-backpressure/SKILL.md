---
name: unix-sockets-and-backpressure
description: >
  Read before writing or changing the local Unix socket server, per-client
  outbound queues, or event broadcast/backpressure for the Hyperloop controller.
  Covers the full-duplex client boundary, one bounded outbound queue per client,
  the critical vs non-critical event split (never drop response/estop/safety),
  drop-oldest-noncritical eviction, slow-client disconnect, and clean per-client
  shutdown. Use for local_socket.py and client broadcast code.
---

# Unix Sockets and Backpressure

> **Authority.** The frozen contract `docs/contracts/V1_Networking_Decisions.md`
> (v3 FROZEN) is the source of truth; also read the `asyncio-controller`,
> `newline-json-protocol`, and `project-networking-invariants` skills. Refs like
> (1.18) point at the governing contract decision. The contract wins on any
> conflict.

Local GUI/client processes connect to the controller over a **full-duplex** Unix
domain socket and exchange newline-delimited JSON. Clients receive both
`response`s and unsolicited `event`s (1.14). This boundary is `local_socket.py`;
it must never touch boards, Redis, or firmware directly.

## One bounded outbound queue per client

* Each `LocalClientConnection` owns its **own** bounded `asyncio.Queue`
  (`outbound_queue_size`, default 1000) drained by a dedicated `writer_loop`
  task. One slow client must not stall the broadcast loop or other clients.
* The reader and the writer are separate: `_handle_client` reads and dispatches;
  `writer_loop` serializes and `drain()`s. A read command is routed via a
  per-request task (`_route_and_reply`) so a slow board does not block the read
  loop.
* Broadcasts use `asyncio.gather(..., return_exceptions=True)` across a snapshot
  (`list(self.clients)`) so one failing client cannot break the fan-out.

## Critical vs non-critical: the core rule

Backpressure handling **depends on message criticality**:

* **Critical** = `response`s and safety events. Critical events are detected by
  name: anything starting with `estop` plus `safety_fault` / `safety_state`
  (`_is_critical_event`); when the name is unknown, **default to critical**.
* **Non-critical** = ordinary telemetry/status events.

On a full outbound queue (`_send`):

* **Non-critical**: evict the oldest non-critical item (`_evict_oldest_noncritical`),
  count `client_event_dropped`, and enqueue. If nothing can be evicted, drop the
  new event and count it. **Drop-oldest, like the observability queue.**
* **Critical**: try to make room by evicting non-critical items; if the queue is
  *still* full of critical messages, **disconnect that client**
  (`_record_critical_disconnect`, then `close(flush=False)`) rather than drop a
  response or safety event or stall the broadcast loop (1.18).

> Never silently drop a `response`, `estop`, or safety event. A client that cannot
> keep up with critical traffic is dropped, not lied to.

## Eviction mechanics (don't break these)

* `_evict_oldest_noncritical` drains the queue into a list, skips exactly **one**
  oldest non-critical item, and re-enqueues the rest **in order** — preserving
  FIFO order and the sentinel. Keep order-preservation if you touch it.
* The shutdown sentinel (`None`) and critical items must survive eviction; only a
  non-critical message is ever removed.

## Clean shutdown of a client

* `close()` enqueues a `None` sentinel so `writer_loop` exits, optionally flushes
  (await the writer task) or cancels it (`flush=False`), then `writer.close()` +
  `wait_closed()` swallowing `ConnectionError`/`OSError`.
* `_put_sentinel` makes room for the sentinel even on a saturated queue by
  evicting one item — shutdown must not block on a full client queue.
* The server `stop()` closes the listening socket, closes all clients
  concurrently, and unlinks the socket file. Make per-client close idempotent and
  exception-safe (see `graceful-shutdown-and-lifecycle`).

## Malformed input and limits

* Receive limit is `CONTROLLER_MAX_LINE_BYTES` (8 KB). An over-limit line
  (`LimitOverrunError`) yields an `INVALID_JSON` error response and a
  `malformed_client_message` controller event; a bad parse yields a structured
  error and continues. Malformed client input must **never** crash the server
  (see `newline-json-protocol`).
* Preserve the client `seq` / controller-owned `board_seq` distinction on
  responses; the socket layer echoes the client's `seq` and `source` back.

## Definition of done

Per-client queues stay bounded and independent; non-critical events drop-oldest
with a counter; `response`/`estop`/safety events are never dropped (slow clients
are disconnected instead); eviction preserves FIFO order and the sentinel;
malformed input never crashes the server; client close is idempotent and
exception-safe. Add tests for queue overflow (both criticalities), slow-client
disconnect, and clean shutdown using fake writers (see
`testing-async-loops-and-mocks`).
