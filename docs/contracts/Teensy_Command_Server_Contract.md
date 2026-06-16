# Teensy 4.1 Command Server Contract — MOVED

The authoritative Teensy 4.1 command-server contract now lives in the
firmware repo:

```text
~/cuddly-train/docs/contracts/Teensy_Command_Server_Contract.md
```

It was authored here on 2026-06-11 and moved the same day when the firmware
implementation repo was bootstrapped. Do not recreate or edit a copy here —
the firmware repo's copy is hash-pinned and is the single source of truth.

The reverse dependency is snapshotted, not shared: `cuddly-train` carries
pinned read-only copies of this repo's `V1_Networking_Decisions.md` and
`Board_Developer_Guide.md` (see its `docs/contracts/UPSTREAM_SOURCES.md`).
If those documents change here, re-snapshot them there.
