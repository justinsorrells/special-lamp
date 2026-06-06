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
