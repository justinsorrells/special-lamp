# Local Client API

> **Authority.** This is companion material for local client integrators. The
> frozen contract `docs/contracts/V1_Networking_Decisions.md` is authoritative
> for controller and networking behavior, and
> `docs/contracts/Board_Developer_Guide.md` is authoritative for firmware-facing
> behavior. If this file disagrees with either contract, the frozen contracts win.

Local clients connect to the controller over the Unix socket. They never talk
directly to boards and Redis is not in the command path.

## Controller-Local Commands

Controller-local commands reuse the existing `command` message type with
`target: "controller"`. They are handled by an explicit allowlist in the
controller. Unknown controller-local command names return `UNKNOWN_COMMAND`.

Controller-local commands:

- never go to a board
- never allocate `board_seq`
- never enter a per-board FIFO
- never consume a board in-flight slot
- never acquire a board writer lock or touch TCP
- are not subject to board execution timeouts or queue residency caps
- reply with the normal `response` shape and the client's original `seq`

## Schema Discovery

All configured boards:

```json
{"type":"command","seq":41,"timestamp":1710000100.0,
 "source":"gui","target":"controller",
 "command":"get_schemas","args":{}}
```

One configured board:

```json
{"type":"command","seq":42,"timestamp":1710000100.0,
 "source":"gui","target":"controller",
 "command":"get_schemas","args":{"board_id":"motor_controller"}}
```

Generic command validation is unchanged. Missing `args` returns
`MISSING_FIELD`; `null` or non-object `args` returns `INVALID_TYPE`.

Handler-level validation for `get_schemas` is:

- `{}` returns all configured boards
- `{"board_id":"<id>"}` returns that one configured board
- unsupported extra keys return `INVALID_ARGUMENT`
- non-string `board_id` returns `INVALID_ARGUMENT`
- unknown `board_id` returns `UNKNOWN_TARGET`

The response always uses `result.boards`, sorted by `board_id` for all-board
queries. A filtered query returns a one-item list.

```json
{"type":"response","seq":42,"source":"controller","target":"gui",
 "status":"ok",
 "result":{"boards":[
   {"board_id":"motor_controller",
    "known":true,
    "available":false,
    "conn_state":"DISCONNECTED",
    "schema_revision":3,
    "protocol_version":"1",
    "firmware_version":"0.1.0",
    "schema":{"commands":{"set_speed":{"args":{"rpm":"int"},
                                      "blocked_by_estop":true}},
              "telemetry":{"rpm":"int"},
              "state":{"mode":"string"}}}]},
 "error":null}
```

`known` means the controller has accepted at least one valid schema for the
board during this controller process. `available` is connection state only:
`conn_state == "REGISTERED"`. E-stop state remains a separate safety axis and is
not folded into availability.

For a configured board that has not registered yet, `known` is `false`,
`available` is `false`, `schema_revision` is `0`, and `schema`,
`protocol_version`, and `firmware_version` are `null`.

The returned `schema` is an effective copy of the accepted schema body.
`blocked_by_estop` is present on every command; an omitted field is materialized
as `true`.

## Schema Revisions

`schema_revision` is an in-memory, per-board counter. It is `0` until the first
accepted schema, `1` after first acceptance, and increments when the effective
schema, `protocol_version`, or `firmware_version` changes. It does not increment
for reconnects with an identical effective record.

Revisions reset when the controller restarts. Clients must treat revisions as
opaque change markers valid only for one controller socket session. After
reconnecting to the controller socket, re-query schemas instead of comparing
against old revisions.

When a schema first becomes known or changes, the controller emits:

```json
{"type":"event","event_id":90413,"timestamp":1710000101.0,
 "source":"controller","event":"schema_updated",
 "details":{"board_id":"motor_controller","schema_revision":3}}
```

`schema_updated` is non-critical. A slow client may miss it under backpressure,
so clients should re-query after reconnecting or when local cache state is
uncertain.

## Line Limits

Client-to-controller request lines use the existing controller receive limit:
8 KiB.

Controller-to-client response/event lines use `LOCAL_RESPONSE_MAX_LINE`, default
64 KiB including the terminating newline. Clients should configure their stream
reader to accept response lines of at least 64 KiB.

If an all-board `get_schemas` response exceeds the local response limit, the
controller returns `INVALID_ARGUMENT` and the message tells the client to retry
with `args.board_id`. If a filtered response exceeds the limit, it returns
`INVALID_ARGUMENT` for that individual board schema.
