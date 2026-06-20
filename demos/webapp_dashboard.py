"""Schema-driven FastAPI dashboard for the controller Unix socket.

A small operator GUI that:
  * holds one persistent Unix-socket connection to the controller,
  * reads each board's schema and renders a typed form per command,
  * sends commands and shows the correlated response,
  * keeps a flat "live values" store that OVERWRITES by field name, so the
    dashboard always shows the current value of each field (responses collapse
    onto common keys instead of growing a log).

Run from the special-lamp repo root (needs `protocol` on the path):

    PYTHONPATH=. python demos/webapp_dashboard.py --socket-path /tmp/hyperloop-controller.sock

Then open http://127.0.0.1:8000/.

Note: this controller forwards command *responses* and schema-change events to
local clients, but not board telemetry, so the live panel is driven by responses
(e.g. `get_counters`, which changes every poll) and polled board state.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import time
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from protocol import CONTROLLER_MAX_LINE_BYTES, MessageType, parse_message, serialize_message

DEFAULT_SOCKET_PATH = "/tmp/hyperloop-controller.sock"
CONTROLLER_TARGET = "controller"


def _coerce(value: Any, type_name: str) -> Any:
    """Coerce a raw form value to the schema-declared argument type."""
    if type_name == "int":
        return int(value)
    if type_name == "float":
        return float(value)
    if type_name == "bool":
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in ("1", "true", "on", "yes"):
            return True
        if normalized in ("0", "false", "off", "no"):
            return False
        raise ValueError(f"{value!r} is not a valid bool")
    return str(value)


class ControllerLink:
    """One persistent full-duplex connection with seq-correlated requests."""

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._no_absorb_seqs: set[int] = set()
        self._seq = 1000
        self._lock = asyncio.Lock()
        # dashboard state
        self.boards: list[dict[str, Any]] = []
        self.live_values: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.connected = False
        self.last_error: str | None = None
        # telemetry observability (Redis read-replica)
        self.telemetry: dict[str, dict[str, Any]] = {}
        self.system_state: dict[str, Any] = {}
        self.board_state: dict[str, dict[str, Any]] = {}
        self._telemetry_fresh: dict[str, tuple[Any, float]] = {}  # bid -> (last_seq, monotonic)
        self.redis_status = "disabled"
        self._redis: Any = None

    async def start(self, redis_url: str | None = None) -> None:
        await self._ensure_connection()
        asyncio.create_task(self._poll_loop())
        if redis_url:
            await self._start_redis(redis_url)

    async def _start_redis(self, url: str) -> None:
        try:
            import redis.asyncio as aioredis  # lazy: dashboard works without redis
            self._redis = aioredis.from_url(url, decode_responses=True)
            await self._redis.ping()
            self.redis_status = f"connected ({url})"
            asyncio.create_task(self._redis_loop())
        except Exception as exc:  # noqa: BLE001 - degrade gracefully
            self._redis = None
            self.redis_status = f"unavailable: {exc}"

    async def _redis_loop(self) -> None:
        """Read each board's telemetry stream and merge it into the live view.

        Telemetry is observability: it comes from the controller's Redis
        read-replica (`board:telemetry:<id>`), never the command socket.
        """
        while True:
            for board in list(self.boards):
                bid = board.get("board_id")
                if not bid:
                    continue
                try:
                    entries = await self._redis.xrevrange(f"board:telemetry:{bid}", count=1)
                except Exception as exc:  # noqa: BLE001
                    self.redis_status = f"read error: {exc}"
                    continue
                if not entries:
                    continue
                _id, fields = entries[0]
                try:
                    values = json.loads(fields.get("telemetry", "{}"))
                except (TypeError, ValueError):
                    values = {}
                seq = fields.get("seq")
                now_m = time.monotonic()
                prev = self._telemetry_fresh.get(bid)
                if prev is None or prev[0] != seq:
                    self._telemetry_fresh[bid] = (seq, now_m)
                age_s = round(now_m - self._telemetry_fresh[bid][1], 2)
                self.telemetry[bid] = {
                    "values": values,
                    "seq": seq,
                    "rate_hz": fields.get("telemetry_rate_hz"),
                    "interval_ms": fields.get("telemetry_interval_ms"),
                    "jitter_ms": fields.get("telemetry_jitter_ms"),
                    "samples": fields.get("telemetry_sample_count"),
                    "age_s": age_s,            # seconds since telemetry last advanced
                    "live": age_s < 1.5,       # board is pushing fresh telemetry
                    "updated": time.strftime("%H:%M:%S"),
                }
                self._absorb_values(values, source=f"telemetry:{bid}")
                try:
                    self.board_state[bid] = await self._redis.hgetall(f"board:state:{bid}")
                except Exception:  # noqa: BLE001
                    pass
            try:
                self.system_state = await self._redis.hgetall("system:state")
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(0.5)

    async def _ensure_connection(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        try:
            self._reader, self._writer = await asyncio.open_unix_connection(
                self.socket_path, limit=CONTROLLER_MAX_LINE_BYTES
            )
            self._reader_task = asyncio.create_task(self._read_loop())
            self.connected = True
            self.last_error = None
        except (OSError, asyncio.CancelledError) as exc:
            self.connected = False
            self.last_error = f"connect failed: {exc}"

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                line = await self._reader.readuntil(b"\n")
                parsed = parse_message(line, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)
                if not parsed.ok or parsed.message is None:
                    continue
                self._dispatch(parsed.message)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError):
            pass
        finally:
            self.connected = False
            self._reader = None
            self._writer = None

    def _dispatch(self, message: dict[str, Any]) -> None:
        mtype = message.get("type")
        if mtype == MessageType.RESPONSE.value:
            seq = message.get("seq")
            fut = self._pending.pop(seq, None) if isinstance(seq, int) else None
            if fut is not None and not fut.done():
                fut.set_result(message)
            if isinstance(seq, int) and seq in self._no_absorb_seqs:
                self._no_absorb_seqs.discard(seq)  # internal poll: metadata, not a board value
            else:
                self._absorb_values(message.get("result"), source=f"response:{seq}")
        elif mtype == MessageType.TELEMETRY.value:
            self._absorb_values(message.get("telemetry"), source="telemetry")
        elif mtype == MessageType.EVENT.value:
            self.events.insert(0, {"t": time.strftime("%H:%M:%S"), **message})
            del self.events[50:]
            self._absorb_values(message.get("details"), source=f"event:{message.get('event')}")

    def _absorb_values(self, payload: Any, *, source: str) -> None:
        """Overwrite live values by (bare) field name. Common fields collapse."""
        if not isinstance(payload, dict):
            return
        now = time.strftime("%H:%M:%S")
        for key, value in payload.items():
            if isinstance(value, dict):
                self._absorb_values(value, source=source)
                continue
            self.live_values[key] = {"value": value, "source": source, "updated": now}

    async def send(self, *, target: str, command: str, args: dict[str, Any],
                   absorb: bool = True) -> dict[str, Any]:
        return await self._request(
            {
                "type": MessageType.COMMAND.value,
                "source": "webapp",
                "target": target,
                "command": command,
                "args": args,
            },
            absorb=absorb,
        )

    async def send_estop_reset(self) -> dict[str, Any]:
        return await self._request(
            {
                "type": MessageType.ESTOP_RESET.value,
                "source": "webapp",
                "target": CONTROLLER_TARGET,
            },
            absorb=False,
        )

    async def _request(self, message: dict[str, Any], *, absorb: bool = True) -> dict[str, Any]:
        async with self._lock:
            await self._ensure_connection()
            if self._writer is None:
                return {"status": "error", "error": {"code": "NO_CONTROLLER",
                                                     "message": self.last_error or "not connected"}}
            self._seq += 1
            seq = self._seq
            message = {**message, "seq": seq}
            if not absorb:
                self._no_absorb_seqs.add(seq)
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[dict[str, Any]] = loop.create_future()
            self._pending[seq] = fut
            try:
                self._writer.write(serialize_message(message, max_line_bytes=CONTROLLER_MAX_LINE_BYTES))
                await self._writer.drain()
            except (ConnectionError, OSError) as exc:
                self._pending.pop(seq, None)
                self.connected = False
                self.last_error = f"send failed: {exc}"
                return {"status": "error", "error": {"code": "NO_CONTROLLER", "message": self.last_error}}
        try:
            return await asyncio.wait_for(fut, timeout=3.0)
        except (asyncio.TimeoutError, TimeoutError):
            self._pending.pop(seq, None)
            return {"status": "error", "error": {"code": "TIMEOUT",
                                                 "message": f"no response for seq {seq}"}}

    async def refresh_schema(self) -> None:
        resp = await self.send(target=CONTROLLER_TARGET, command="get_schemas", args={}, absorb=False)
        if resp.get("status") == "ok":
            self.boards = resp.get("result", {}).get("boards", [])

    async def _poll_loop(self) -> None:
        while True:
            with contextlib.suppress(Exception):
                await self.refresh_schema()
            await asyncio.sleep(1.0)


link: ControllerLink


def build_app(socket_path: str, redis_url: str | None = None) -> FastAPI:
    global link
    link = ControllerLink(socket_path)
    app = FastAPI(title="Hyperloop controller dashboard")

    @app.on_event("startup")
    async def _startup() -> None:
        await link.start(redis_url=redis_url)

    @app.get("/api/schema")
    async def api_schema() -> JSONResponse:
        if not link.boards:
            await link.refresh_schema()
        return JSONResponse({"connected": link.connected, "last_error": link.last_error,
                             "boards": link.boards})

    @app.get("/api/state")
    async def api_state() -> JSONResponse:
        return JSONResponse({
            "connected": link.connected,
            "boards": [{k: b.get(k) for k in
                        ("board_id", "conn_state", "available", "schema_revision",
                         "firmware_version", "protocol_version")} for b in link.boards],
            "live_values": link.live_values,
            "events": link.events[:15],
            "telemetry": link.telemetry,
            "system_state": link.system_state,
            "board_state": link.board_state,
            "redis_status": link.redis_status,
        })

    @app.get("/api/history")
    async def api_history(board: str, field: str = "echo_value", n: int = 160) -> JSONResponse:
        if link._redis is None:  # noqa: SLF001 - demo
            return JSONResponse({"points": [], "field": field, "board": board})
        try:
            entries = await link._redis.xrevrange(f"board:telemetry:{board}", count=n)  # noqa: SLF001
        except Exception:  # noqa: BLE001
            return JSONResponse({"points": [], "field": field, "board": board})
        points: list[dict[str, Any]] = []
        for entry_id, fields in entries:
            try:
                ts = int(str(entry_id).split("-")[0])
                values = json.loads(fields.get("telemetry", "{}"))
                if field in values and isinstance(values[field], (int, float)):
                    points.append({"t": ts, "v": float(values[field])})
                elif field in fields and fields[field] not in (None, ""):
                    points.append({"t": ts, "v": float(fields[field])})  # top-level stream field
            except (TypeError, ValueError):
                continue
        points.reverse()
        return JSONResponse({"points": points, "field": field, "board": board})

    @app.post("/api/command")
    async def api_command(payload: dict[str, Any]) -> JSONResponse:
        target = payload.get("target", "")
        command = payload.get("command", "")
        raw_args = payload.get("args", {}) or {}
        types = _arg_types(target, command)
        if types is None:
            return JSONResponse({"status": "error", "error": {
                "code": "BAD_INPUT", "message": "unknown target or command"}})
        extra_args = set(raw_args) - set(types)
        if extra_args:
            name = sorted(extra_args)[0]
            return JSONResponse({"status": "error", "error": {
                "code": "BAD_INPUT", "message": f"{name!r} is not a declared argument"}})
        coerced: dict[str, Any] = {}
        for name, raw in raw_args.items():
            try:
                coerced[name] = _coerce(raw, types.get(name, "string"))
            except (TypeError, ValueError):
                return JSONResponse({"status": "error", "error": {
                    "code": "BAD_INPUT", "message": f"{name!r} is not a valid {types.get(name)}"}})
        resp = await link.send(target=target, command=command, args=coerced)
        return JSONResponse(resp)

    @app.post("/api/estop_reset")
    async def api_estop_reset() -> JSONResponse:
        return JSONResponse(await link.send_estop_reset())

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return INDEX_HTML

    return app


def _arg_types(target: str, command: str) -> dict[str, str] | None:
    for board in link.boards:
        if board.get("board_id") == target:
            cmd = (board.get("schema") or {}).get("commands", {}).get(command, {})
            if not cmd:
                return None
            return dict(cmd.get("args", {}) or {})
    return None


INDEX_HTML = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hyperloop // Control</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
 --bg:#080C11;--bg-deep:#0B1118;--panel:#101821;--panel2:#141E29;--elev:#182431;--sel:#1D2B3A;
 --border:#263544;--border-strong:#3A4B5C;
 --t1:#E9F0F6;--t2:#A6B4C1;--t3:#6F7F8E;--t4:#465461;
 --accent:#4DA3FF;--accent2:#26C6DA;--ok:#38D996;--warn:#F6B94A;--crit:#FF5364;--purple:#8F7CFF;
 --sans:'Inter',system-ui,-apple-system,sans-serif;--mono:'JetBrains Mono',ui-monospace,monospace;
}
*{box-sizing:border-box}
html,body{margin:0;height:100%}
body{background:var(--bg);color:var(--t1);font-family:var(--sans);font-size:13px;-webkit-font-smoothing:antialiased}
.mono{font-family:var(--mono);font-variant-numeric:tabular-nums}
.shell{display:grid;grid-template-rows:auto auto 1fr;height:100vh}
/* ---- top bar ---- */
.topbar{display:flex;align-items:center;gap:18px;height:58px;padding:0 18px;background:linear-gradient(180deg,#0d141d,#0b1118);border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:10px}
.brand .mark{width:22px;height:22px;border-radius:5px;background:linear-gradient(135deg,var(--accent),var(--accent2));box-shadow:0 0 0 1px #2b4254 inset}
.brand b{font-weight:600;letter-spacing:.14em;font-size:13px}
.brand span{color:var(--t3);font-size:11px;letter-spacing:.22em}
.spacer{flex:1}
.pill{display:flex;align-items:center;gap:7px;height:30px;padding:0 11px;border:1px solid var(--border);background:var(--panel);border-radius:7px;font-size:11px;letter-spacing:.06em;color:var(--t2)}
.pill .dot{width:7px;height:7px;border-radius:50%;background:var(--t4)}
.pill.good .dot{background:var(--ok);box-shadow:0 0 8px #38d99680}.pill.good{color:var(--t1)}
.pill.bad .dot{background:var(--crit);box-shadow:0 0 8px #ff536480}.pill.bad{color:#ffd0d4}
.pill .v{font-family:var(--mono);color:var(--t1)}
.clock{font-family:var(--mono);color:var(--t2);font-size:12px;letter-spacing:.04em}
.toggle{display:flex;align-items:center;gap:8px;color:var(--t2);font-size:11px;letter-spacing:.05em;cursor:pointer;user-select:none}
.toggle input{appearance:none;width:32px;height:18px;border-radius:10px;background:var(--elev);border:1px solid var(--border);position:relative;cursor:pointer;transition:.16s}
.toggle input:checked{background:#13314d;border-color:var(--accent)}
.toggle input::after{content:"";position:absolute;top:1px;left:1px;width:14px;height:14px;border-radius:50%;background:var(--t2);transition:.16s}
.toggle input:checked::after{transform:translateX(14px);background:var(--accent)}
/* ---- safety banner ---- */
.safety{display:none;align-items:center;gap:14px;padding:11px 18px;background:linear-gradient(180deg,#2a1014,#230f12);border-bottom:1px solid var(--crit)}
.safety.on{display:flex}
.safety .ico{width:26px;height:26px;color:var(--crit)}
.safety b{color:#ffd9dc;letter-spacing:.1em;font-size:13px}
.safety small{color:#e39aa0;letter-spacing:.04em}
/* ---- body ---- */
.body{display:grid;grid-template-columns:228px 1fr 324px;min-height:0}
nav.nav{border-right:1px solid var(--border);background:var(--bg-deep);padding:14px 10px;display:flex;flex-direction:column;gap:3px}
.nav .grp{color:var(--t4);font-size:10px;letter-spacing:.18em;padding:14px 10px 6px}
.nav a{display:flex;align-items:center;gap:11px;padding:9px 11px;border-radius:7px;color:var(--t2);text-decoration:none;font-size:12.5px;letter-spacing:.02em;border:1px solid transparent;cursor:pointer}
.nav a svg{width:17px;height:17px;color:var(--t3)}
.nav a:hover{background:var(--panel);color:var(--t1)}
.nav a.active{background:var(--sel);color:var(--t1);border-color:var(--border)}
.nav a.active svg{color:var(--accent)}
.nav a.active{position:relative}
.nav a.active::before{content:"";position:absolute;left:-10px;top:8px;bottom:8px;width:2px;border-radius:2px;background:var(--accent)}
main.canvas{overflow:auto;padding:18px;min-width:0}
aside.rail{border-left:1px solid var(--border);background:var(--bg-deep);display:flex;flex-direction:column;min-height:0}
.rail h3{font-size:11px;letter-spacing:.16em;color:var(--t3);margin:0;padding:15px 16px 10px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
.rail .stream{overflow:auto;flex:1;padding:6px 0}
.evt{display:flex;gap:10px;padding:8px 16px;border-bottom:1px solid #18222e}
.evt .d{width:7px;height:7px;border-radius:50%;margin-top:4px;background:var(--accent);flex:none}
.evt .d.ok{background:var(--ok)}.evt .d.warn{background:var(--warn)}.evt .d.crit{background:var(--crit)}.evt .d.mut{background:var(--t4)}
.evt .body{min-width:0;flex:1}
.evt .tt{font-size:12px;color:var(--t1);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.evt .mt{font-size:11px;color:var(--t3);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.evt .ts{font-size:11px;color:var(--t2);font-family:var(--mono);flex:none;white-space:nowrap;padding-left:4px}
/* ---- grid + widgets ---- */
.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}
.sec-title{display:flex;align-items:center;gap:9px;margin:6px 2px 12px;color:var(--t2);font-size:13px;letter-spacing:.1em}
.sec-title:not(:first-child){margin-top:24px}
.sec-title .ln{flex:1;height:1px;background:var(--border)}
.w{background:var(--panel2);border:1px solid var(--border);border-radius:9px;padding:15px}
.w.head{display:flex;align-items:center;justify-content:space-between}
.w h2{font-size:12px;font-weight:600;letter-spacing:.07em;color:var(--t2);margin:0 0 2px;text-transform:uppercase}
.w .sub{font-size:11px;color:var(--t3);font-family:var(--mono)}
.metric .label{font-size:10.5px;letter-spacing:.13em;color:var(--t3);text-transform:uppercase}
.metric .val{font-family:var(--mono);font-size:30px;font-weight:500;color:var(--t1);margin:7px 0 3px;line-height:1}
.metric .ctx{font-size:11px;color:var(--t3);font-family:var(--mono)}
.metric .val.accent{color:var(--accent)}.metric .val.cyan{color:var(--accent2)}.metric .val.purple{color:var(--purple)}
.col3{grid-column:span 3}.col4{grid-column:span 4}.col5{grid-column:span 5}.col6{grid-column:span 6}.col7{grid-column:span 7}.col8{grid-column:span 8}.col12{grid-column:span 12}
/* device card */
.device{border:1px solid var(--border);border-left:3px solid var(--ok);border-radius:9px;background:var(--panel2);padding:16px}
.device.off{border-left-color:var(--crit);opacity:.62;filter:saturate(.5)}
.device .row1{display:flex;align-items:center;gap:10px}
.device .nm{font-weight:600;letter-spacing:.04em;font-size:14px}
.device .stat{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:11px;letter-spacing:.1em;color:var(--ok)}
.device.off .stat{color:var(--crit)}
.device .stat .d{width:8px;height:8px;border-radius:50%;background:currentColor;box-shadow:0 0 8px currentColor}
.device .meta{color:var(--t3);font-family:var(--mono);font-size:11px;margin-top:5px}
.device .reads{display:flex;gap:26px;margin-top:14px}
.device .reads .k{font-size:10px;letter-spacing:.12em;color:var(--t3);text-transform:uppercase}
.device .reads .v{font-family:var(--mono);font-size:21px;color:var(--t1);margin-top:3px}
.device .chips{display:flex;gap:8px;margin-top:14px;flex-wrap:wrap}
.chip{font-family:var(--mono);font-size:10.5px;color:var(--t2);background:var(--elev);border:1px solid var(--border);border-radius:6px;padding:3px 8px}
.chip.warn{color:var(--warn);border-color:#4a3a1c}.chip.crit{color:var(--crit);border-color:#4a1c22}.chip.ok{color:var(--ok);border-color:#1c4a32}
/* chart */
.chartwrap{position:relative;height:188px;margin-top:6px}
canvas{width:100%;height:100%;display:block}
.fieldbtns{display:flex;gap:6px}
.fb{font-family:var(--mono);font-size:10.5px;color:var(--t3);background:var(--panel);border:1px solid var(--border);border-radius:6px;padding:4px 9px;cursor:pointer;letter-spacing:.03em}
.fb.on{color:var(--accent);border-color:var(--accent);background:#0f2335}
/* registers table */
table.reg{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:11.5px;table-layout:fixed}
table.reg td{padding:4px 4px;border-bottom:1px solid #1a2531;overflow:hidden}
table.reg td.k{color:var(--t3);width:46%;word-break:break-word}
table.reg td.v{color:var(--t1);text-align:right;word-break:break-all}
table.reg td.s{color:var(--t2);text-align:right;font-size:10.5px;width:64px;white-space:nowrap}
/* commands */
.cmd{border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:11px;background:var(--panel)}
.cmd .ch{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.cmd .ch b{font-family:var(--mono);font-size:12.5px;color:var(--t1)}
.cmd .ch .tg{font-size:10px;color:var(--t3);letter-spacing:.06em}
.cmd.gated{border-color:#3a2a14}
.fieldrow{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-bottom:9px}
label.fl{display:block;font-size:10px;letter-spacing:.1em;color:var(--t3);text-transform:uppercase;margin-bottom:4px}
input.in,select.in{width:100%;background:var(--bg-deep);border:1px solid var(--border);color:var(--t1);font-family:var(--mono);font-size:12px;border-radius:6px;padding:7px 9px;outline:none}
input.in:focus,select.in:focus{border-color:var(--accent);box-shadow:0 0 0 3px #4da3ff22}
button.btn{height:34px;padding:0 14px;border-radius:7px;font-size:12px;font-weight:500;letter-spacing:.04em;cursor:pointer;border:1px solid var(--accent);background:#10243a;color:#cfe6ff;transition:.14s}
button.btn:hover{background:#143352}
button.btn.crit{border-color:var(--crit);background:#2a1014;color:#ffc9cd}
button.btn.crit:hover{background:#3a151b}
button.btn.ghost{border-color:var(--border);background:var(--panel);color:var(--t2)}
.cmd .resp{margin-top:9px;font-family:var(--mono);font-size:11px;border-radius:6px;padding:8px;background:var(--bg-deep);border:1px solid var(--border);color:var(--t2);white-space:pre-wrap;word-break:break-word;display:none}
.cmd .resp.show{display:block}
.cmd .resp.ok{border-color:#1c4a32}.cmd .resp.err{border-color:#4a1c22;color:#ffb3b8}
.muted{color:var(--t3)}
.empty{color:var(--t3);font-family:var(--mono);font-size:12px;display:flex;align-items:center;gap:9px;padding:18px 4px}
@media(max-width:1180px){.body{grid-template-columns:64px 1fr}.rail{display:none}.nav a span{display:none}.nav .grp{display:none}}
@media(prefers-reduced-motion:reduce){*{transition:none!important}}
.flash{animation:fl .9s ease-out}@keyframes fl{from{background:#16324d}to{background:transparent}}
/* board tabs + summary cards */
.tabs{display:flex;gap:7px;flex-wrap:wrap;margin:0 2px 14px}
.tab{font-family:var(--mono);font-size:12px;letter-spacing:.03em;color:var(--t2);background:var(--panel);border:1px solid var(--border);border-radius:7px;padding:8px 14px;cursor:pointer}
.tab:hover{color:var(--t1);border-color:var(--border-strong)}
.tab.on{color:var(--accent);border-color:var(--accent);background:#0f2335}
.sumcard{border:1px solid var(--border);border-left:3px solid var(--ok);border-radius:9px;background:var(--panel2);padding:14px;cursor:pointer;transition:.14s}
.sumcard:hover{border-color:var(--border-strong)}
.sumcard.off{border-left-color:var(--crit);opacity:.62;filter:saturate(.5)}
.sumcard.sel{box-shadow:0 0 0 1px var(--accent) inset;border-left-color:var(--accent)}
.sumcard .row1{display:flex;align-items:center;gap:10px}
.sumcard .nm{font-weight:600;letter-spacing:.03em;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sumcard .stat{margin-left:auto;display:flex;align-items:center;gap:6px;font-size:10px;letter-spacing:.1em;color:var(--ok);flex:none}
.sumcard.off .stat{color:var(--crit)}
.sumcard .stat .d{width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 8px currentColor}
.sumcard .meta{color:var(--t3);font-family:var(--mono);font-size:10.5px;margin:5px 0 10px}
.sumrow{display:flex;justify-content:space-between;font-size:11.5px;padding:3px 0;color:var(--t3);border-top:1px solid #1a2531}
.sumrow b{color:var(--t1);font-family:var(--mono);font-weight:500}
</style></head>
<body><div class="shell">
 <div class="topbar">
  <div class="brand"><div class="mark"></div><b>HYPERLOOP</b><span>CONTROL</span></div>
  <div class="spacer"></div>
  <div class="pill" id="p-ctl"><span class="dot"></span>CONTROLLER <span class="v" id="p-ctl-v">…</span></div>
  <div class="pill" id="p-redis"><span class="dot"></span>REDIS <span class="v" id="p-redis-v">…</span></div>
  <div class="pill" id="p-boards"><span class="dot"></span>BOARDS <span class="v" id="p-boards-v">0</span></div>
  <label class="toggle"><input type="checkbox" id="autopoll"> AUTO COUNTERS</label>
  <div class="clock" id="clock">--:--:--</div>
 </div>
 <div class="safety" id="safety">
  <svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 9v4m0 4h.01M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/></svg>
  <div><b>E-STOP ACTIVE</b> &nbsp;<small id="safety-sub">Command output is restricted — operator reset required</small></div>
  <div class="spacer"></div>
  <button class="btn ghost" id="reset2">ESTOP RESET</button>
 </div>
 <div class="body">
  <nav class="nav">
   <div class="grp">OPERATIONS</div>
   <a class="active" data-go="ov"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg><span>Overview</span></a>
   <a data-go="summary"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round"><path d="M4 6h16M4 12h16M4 18h10"/></svg><span>Summary</span></a>
   <a data-go="boards"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"><rect x="5" y="5" width="14" height="14" rx="2"/><rect x="9" y="9" width="6" height="6" rx="1"/><path d="M9 1v3M15 1v3M9 20v3M15 20v3M1 9h3M1 15h3M20 9h3M20 15h3"/></svg><span>Boards</span></a>
   <a data-go="tele"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linejoin="round" stroke-linecap="round"><path d="M3 12h4l2-6 4 12 2-6h6"/></svg><span>Telemetry</span></a>
   <a data-go="cmd"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linejoin="round" stroke-linecap="round"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="m7 9 3 3-3 3M13 15h4"/></svg><span>Commands</span></a>
   <div class="grp">SYSTEM</div>
   <a data-go="events"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linejoin="round" stroke-linecap="round"><path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.94 1.94 0 0 0 3.4 0"/></svg><span>Events</span></a>
  </nav>
  <main class="canvas">
   <div class="sec-title" id="ov">OVERVIEW <div class="ln"></div></div>
   <div class="grid">
    <div class="w metric col3"><div class="label">Telemetry rate</div><div class="val accent mono" id="m-rate">--</div><div class="ctx">target 20.0 Hz</div></div>
    <div class="w metric col3"><div class="label">Frame interval</div><div class="val mono" id="m-int">--</div><div class="ctx">nominal 50.0 ms</div></div>
    <div class="w metric col3"><div class="label">Jitter</div><div class="val cyan mono" id="m-jit">--</div><div class="ctx">peak-to-mean</div></div>
    <div class="w metric col3"><div class="label">Cmd latency p95</div><div class="val mono" id="m-lat">--</div><div class="ctx" id="m-lat-ctx">p50 -- · p99 --</div></div>
   </div>

   <div class="sec-title" id="summary">SUMMARY <div class="ln"></div></div>
   <div class="grid" id="board-summary"><div class="empty">Waiting for boards…</div></div>

   <div class="sec-title" id="boards">BOARDS &amp; SIGNAL <div class="ln"></div></div>
   <div class="tabs" id="board-tabs"></div>
   <div class="grid">
    <div class="col5"><div id="devices"><div class="empty"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.75"><rect x="5" y="5" width="14" height="14" rx="2"/><rect x="9" y="9" width="6" height="6" rx="1"/></svg> Waiting for board registration…</div></div></div>
    <div class="col7"><div class="w">
      <div class="w head" style="margin-bottom:8px"><div><h2 id="tele">Live telemetry</h2><div class="sub" id="chart-sub">—</div></div>
       <div class="fieldbtns" id="fieldbtns">
        <button class="fb on" data-f="telemetry_interval_ms">interval ms</button>
        <button class="fb" data-f="telemetry_rate_hz">rate hz</button>
        <button class="fb" data-f="echo_value">echo_value</button>
       </div></div>
      <div class="chartwrap"><canvas id="chart"></canvas></div>
    </div></div>
   </div>

   <div class="sec-title" id="cmd">COMMAND &amp; CONTROL <div class="ln"></div></div>
   <div class="grid">
    <div class="col7"><div class="w"><h2 style="margin-bottom:12px">Command console</h2><div id="forms"><div class="empty">loading schema…</div></div></div></div>
    <div class="col5"><div class="w"><div class="w head" style="margin-bottom:10px"><h2>Registers</h2><span class="sub" id="reg-count"></span></div>
      <table class="reg"><tbody id="reg"></tbody></table></div></div>
   </div>
  </main>
  <aside class="rail">
   <h3><svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linejoin="round" stroke-linecap="round"><path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.94 1.94 0 0 0 3.4 0"/></svg> ACTIVITY</h3>
   <div class="stream" id="events"><div class="empty" style="padding:16px">No activity yet</div></div>
  </aside>
 </div>
</div>
<script>
const ICONS={};
let schema=null, board=null, field='telemetry_interval_ms', activity=[], lastEstop=false, schemaSig=null, tabsSig=null, formsSig=null;
function el(id){return document.getElementById(id)}
function tnow(){return new Date().toLocaleTimeString('en-GB')}
function pill(id,good,val){const p=el(id);p.classList.toggle('good',!!good);p.classList.toggle('bad',!good);if(val!==undefined)el(id+'-v').textContent=val;}
function pushAct(kind,tt,mt){activity.unshift({d:kind,tt:tt,mt:mt||'',ts:tnow()});activity=activity.slice(0,60);renderActivity();}
function renderActivity(){const e=el('events');if(!activity.length){e.innerHTML='<div class="empty" style="padding:16px">No activity yet</div>';return;}
 e.innerHTML=activity.map(a=>'<div class="evt"><div class="d '+a.d+'"></div><div class="body"><div class="tt">'+a.tt+'</div>'+(a.mt?'<div class="mt">'+a.mt+'</div>':'')+'</div><div class="ts">'+a.ts+'</div></div>').join('');}

async function loadSchema(){
 try{schema=await (await fetch('/api/schema')).json();}catch(e){return;}
 renderForms();
}
// Command console renders ONE board at a time — the active tab. Only rebuild
// when the active board or its schema changes, so live typing isn't wiped.
function renderForms(force){
 const f=el('forms');if(!f)return;
 const bs=(schema&&schema.boards)||[];
 if(!bs.length){f.innerHTML='<div class="empty">no boards registered — waiting</div>';formsSig=null;return;}
 if(!board||!bs.find(b=>b.board_id===board))board=bs[0].board_id;
 const b=bs.find(x=>x.board_id===board)||bs[0];
 const cmds=(b.schema&&b.schema.commands)||{};
 const sig=JSON.stringify([b.board_id,b.schema_revision,cmds]);
 if(!force&&sig===formsSig)return;
 const firstLoad=formsSig===null;formsSig=sig;
 let html='';
 for(const name of Object.keys(cmds)){const args=cmds[name].args||{};const gated=cmds[name].blocked_by_estop;
  const crit=name.indexOf('estop')>=0;
  html+='<div class="cmd'+(gated?' gated':'')+'"><div class="ch"><b>'+name+'</b><span class="tg muted">→ '+b.board_id+(gated?' · gated':'')+'</span></div>';
  const ks=Object.keys(args);
  if(ks.length){html+='<div class="fieldrow">';for(const an of ks){const t=args[an];
    html+='<div><label class="fl">'+an+' · '+t+'</label>'+(t==='bool'
      ?'<select class="in" data-arg="'+an+'"><option>true</option><option>false</option></select>'
      :'<input class="in" data-arg="'+an+'" '+((t==='int'||t==='float')?'type="number" step="any" ':'')+'placeholder="'+t+'">')+'</div>';}
    html+='</div>';}
  html+='<button class="btn'+(crit?' crit':'')+'" data-send data-target="'+b.board_id+'" data-cmd="'+name+'">'+(crit?'TRIGGER':'SEND')+'</button>';
  html+='<div class="resp"></div></div>';
 }
 f.innerHTML=html||'<div class="empty">no commands for this board</div>';
 f.querySelectorAll('[data-send]').forEach(btn=>btn.addEventListener('click',()=>sendCmd(btn)));
 if(!firstLoad){f.classList.remove('flash');void f.offsetWidth;f.classList.add('flash');}
}
// Tab strip: one tab per registered board; click selects the active board.
function renderTabs(boards){
 boards=boards||[];const c=el('board-tabs');if(!c)return;
 if(boards.length&&(!board||!boards.find(b=>b.board_id===board)))board=boards[0].board_id;
 const sig=JSON.stringify([boards.map(b=>b.board_id),board]);
 if(sig===tabsSig)return;tabsSig=sig;
 c.innerHTML=boards.map(b=>'<button class="tab'+(b.board_id===board?' on':'')+'" data-tab="'+b.board_id+'">'+b.board_id+'</button>').join('');
 c.querySelectorAll('[data-tab]').forEach(btn=>btn.addEventListener('click',()=>selectBoard(btn.dataset.tab)));
}
function selectBoard(bid){
 if(!bid||board===bid)return;board=bid;tabsSig=null;
 renderTabs((schema&&schema.boards)||[]);renderForms(true);drawChart();poll();
}
// Summary section: a compact status card for EVERY board, always visible.
function renderSummary(s){
 const c=el('board-summary');if(!c)return;const bs=s.boards||[];
 if(!bs.length){c.innerHTML='<div class="empty">no boards registered — waiting</div>';return;}
 c.innerHTML=bs.map(b=>{const bid=b.board_id;const tel=(s.telemetry||{})[bid]||{};const bst=(s.board_state||{})[bid]||{};
  const live=tel.live&&b.conn_state==='REGISTERED';const ack=bst.estop_ack==='true';
  return '<div class="col4"><div class="sumcard'+(live?'':' off')+(bid===board?' sel':'')+'" data-tab="'+bid+'">'+
   '<div class="row1"><span class="nm">'+bid+'</span><span class="stat"><span class="d"></span>'+(live?'ONLINE':'OFFLINE')+'</span></div>'+
   '<div class="meta">'+(b.conn_state||'—')+' · fw '+(b.firmware_version||'—')+'</div>'+
   '<div class="sumrow"><span>rate</span><b>'+fmt(tel.rate_hz,1)+' Hz</b></div>'+
   '<div class="sumrow"><span>echo_value</span><b>'+((tel.values&&tel.values.echo_value)!==undefined?tel.values.echo_value:'--')+'</b></div>'+
   '<div class="sumrow"><span>estop_ack</span><b'+(ack?' style="color:var(--warn)"':'')+'>'+(ack?'true':'false')+'</b></div>'+
   '<div class="sumrow"><span>queue</span><b>'+(bst.queue_depth||'0')+'</b></div>'+
   '</div></div>';
 }).join('');
 c.querySelectorAll('[data-tab]').forEach(card=>card.addEventListener('click',()=>selectBoard(card.dataset.tab)));
}
async function sendCmd(btn){
 const card=btn.closest('.cmd');const args={};
 card.querySelectorAll('[data-arg]').forEach(i=>args[i.dataset.arg]=i.value);
 const target=btn.dataset.target, command=btn.dataset.cmd;
 pushAct('mut','▸ '+command, JSON.stringify(args));
 let r;try{r=await (await fetch('/api/command',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({target,command,args})})).json();}
 catch(e){r={status:'error',error:{message:String(e)}};}
 const box=card.querySelector('.resp');box.classList.add('show');
 const okk=r.status==='ok';box.classList.toggle('ok',okk);box.classList.toggle('err',!okk);
 box.textContent=JSON.stringify(okk?r.result:r.error,null,2);box.classList.remove('flash');void box.offsetWidth;box.classList.add('flash');
 pushAct(okk?'ok':'crit',(okk?'✓ ':'✕ ')+command,okk?JSON.stringify(r.result):(r.error&&r.error.code||'error'));
}
async function estopReset(){await fetch('/api/estop_reset',{method:'POST'});pushAct('warn','estop_reset sent','');}

function fmt(x,d){if(x===undefined||x===null||x==='')return '--';const n=+x;return isNaN(n)?x:n.toFixed(d===undefined?1:d);}
async function poll(){
 el('clock').textContent=tnow();
 let s;try{s=await (await fetch('/api/state')).json();}catch(e){pill('p-ctl',false,'down');return;}
 pill('p-ctl',s.connected,s.connected?'link':'down');
 const rok=(s.redis_status||'').startsWith('connected');pill('p-redis',rok,rok?'ok':'off');
 pill('p-boards',(s.boards||[]).length>0,(s.boards||[]).length);
 // safety banner
 const estop=(s.system_state||{}).estop_active==='true';el('safety').classList.toggle('on',estop);
 if(estop&&!lastEstop)pushAct('crit','E-STOP latched','system.estop_active=true');lastEstop=estop;
 // board tabs + all-board summary, then per-board (active tab) detail
 renderTabs(s.boards||[]);
 renderSummary(s);
 // telemetry + board metrics for the ACTIVE board (selected tab)
 const bid=board||(s.boards[0]&&s.boards[0].board_id);
 const tel=(s.telemetry||{})[bid]; const bst=(s.board_state||{})[bid]||{};
 const conn=((s.boards||[]).find(b=>b.board_id===bid)||{}).conn_state||'—';
 const live=tel&&tel.live&&conn==='REGISTERED';
 if(tel){
  el('m-rate').textContent=fmt(tel.rate_hz,1);
  el('m-int').textContent=fmt(tel.interval_ms,1);
  el('m-jit').textContent=fmt(tel.jitter_ms,2);
 }
 if(bst.command_latency_p95_ms){el('m-lat').textContent=fmt(bst.command_latency_p95_ms,2);
  el('m-lat-ctx').textContent='p50 '+fmt(bst.command_latency_p50_ms,2)+' · p99 '+fmt(bst.command_latency_p99_ms,2);}
 renderDevice(bid,conn,live,tel,bst);
 renderRegisters(tel);
 // controller events into activity (dedupe by id)
 (s.events||[]).forEach(ev=>{const key=(ev.event||'')+ (ev.t||'');if(!poll._seen)poll._seen={};if(poll._seen[key])return;poll._seen[key]=1;
   pushAct(ev.event&&ev.event.indexOf('estop')>=0?'crit':'mut',ev.event||ev.type,JSON.stringify(ev.details||{}));});
 // auto counters
 if(el('autopoll').checked&&schema&&schema.boards){for(const b of schema.boards){if((b.schema.commands||{}).get_counters)
   fetch('/api/command',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({target:b.board_id,command:'get_counters',args:{}})});}}
}
function renderDevice(bid,conn,live,tel,bst){
 const d=el('devices');if(!bid){return;}
 const ack=bst.estop_ack==='true';
 const chips=[];
 chips.push('<span class="chip">proto '+(bst.protocol_version||'1')+'</span>');
 chips.push('<span class="chip">fw '+( (schema&&(schema.boards.find(b=>b.board_id===bid)||{}).firmware_version)||'—')+'</span>');
 chips.push('<span class="chip '+(live?'ok':'crit')+'">'+(live?'telemetry live':'telemetry stale '+(tel?('· '+tel.age_s+'s'):''))+'</span>');
 chips.push('<span class="chip '+(ack?'warn':'')+'">estop_ack '+(ack?'true':'false')+'</span>');
 chips.push('<span class="chip">queue '+(bst.queue_depth||'0')+'</span>');
 d.innerHTML='<div class="device'+(live?'':' off')+'"><div class="row1"><span class="nm">'+bid+'</span>'+
  '<span class="stat"><span class="d"></span>'+(live?'ONLINE':'OFFLINE')+'</span></div>'+
  '<div class="meta">'+conn+' · '+(bst.last_seen?('seen '+(+bst.last_seen).toFixed(0)):'—')+'</div>'+
  '<div class="reads"><div><div class="k">Echo value</div><div class="v">'+((tel&&tel.values&&tel.values.echo_value)!==undefined?tel.values.echo_value:'--')+'</div></div>'+
  '<div><div class="k">Mode</div><div class="v">'+((tel&&tel.values&&tel.values.telemetry_mode)!==undefined?tel.values.telemetry_mode:'--')+'</div></div>'+
  '<div><div class="k">Rate</div><div class="v">'+fmt(tel&&tel.rate_hz,1)+' Hz</div></div></div>'+
  '<div class="chips">'+chips.join('')+'</div></div>';
}
function renderRegisters(tel){
 const vals=(tel&&tel.values)||{};const upd=(tel&&tel.updated)||'';
 const keys=Object.keys(vals).sort();el('reg-count').textContent=keys.length+' fields';
 el('reg').innerHTML=keys.map(k=>'<tr><td class="k">'+k+'</td><td class="v">'+JSON.stringify(vals[k])+'</td><td class="s">'+upd+'</td></tr>').join('')
  ||'<tr><td class="muted" colspan="3" style="padding:14px 4px">waiting for telemetry…</td></tr>';
}
/* ---- canvas chart ---- */
async function drawChart(){
 const bid=board;if(!bid){return;}
 let pts=[];try{pts=(await (await fetch('/api/history?board='+encodeURIComponent(bid)+'&field='+field+'&n=160')).json()).points||[];}catch(e){}
 const cv=el('chart'),wrap=cv.parentElement,dpr=devicePixelRatio||1,w=wrap.clientWidth,h=wrap.clientHeight;
 cv.width=w*dpr;cv.height=h*dpr;const x=cv.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.clearRect(0,0,w,h);
 const sub=el('chart-sub');
 if(pts.length<2){sub.textContent='waiting for data…';x.fillStyle='#6F7F8E';x.font='12px JetBrains Mono';x.fillText('no telemetry',12,24);return;}
 const ys=pts.map(p=>p.v);let mn=Math.min(...ys),mx=Math.max(...ys);if(mn===mx){mn-=1;mx+=1;}const r=mx-mn;mn-=r*0.18;mx+=r*0.18;
 const pl=6,pr=6,pt=12,pb=8;const X=i=>pl+i/(pts.length-1)*(w-pl-pr),Y=v=>pt+(1-(v-mn)/(mx-mn))*(h-pt-pb);
 x.strokeStyle='rgba(58,75,92,.30)';x.lineWidth=1;for(let g=0;g<=4;g++){const yy=pt+g/4*(h-pt-pb);x.beginPath();x.moveTo(pl,yy);x.lineTo(w-pr,yy);x.stroke();}
 const col=field==='telemetry_rate_hz'?'#26C6DA':(field==='echo_value'?'#8F7CFF':'#4DA3FF');
 x.beginPath();pts.forEach((p,i)=>{const xx=X(i),yy=Y(p.v);i?x.lineTo(xx,yy):x.moveTo(xx,yy);});
 x.lineTo(X(pts.length-1),h-pb);x.lineTo(X(0),h-pb);x.closePath();
 const g=x.createLinearGradient(0,pt,0,h);g.addColorStop(0,col+'2e');g.addColorStop(1,col+'00');x.fillStyle=g;x.fill();
 x.beginPath();pts.forEach((p,i)=>{const xx=X(i),yy=Y(p.v);i?x.lineTo(xx,yy):x.moveTo(xx,yy);});x.strokeStyle=col;x.lineWidth=1.8;x.stroke();
 const lx=X(pts.length-1),ly=Y(pts[pts.length-1].v);x.fillStyle=col;x.beginPath();x.arc(lx,ly,3,0,7);x.fill();
 const last=pts[pts.length-1].v;sub.textContent=field+'  cur '+last.toFixed(field==='echo_value'?0:2)+'  ·  min '+Math.min(...ys).toFixed(1)+'  max '+Math.max(...ys).toFixed(1);
}
// nav + field buttons + reset
document.querySelectorAll('.nav a').forEach(a=>a.addEventListener('click',()=>{document.querySelectorAll('.nav a').forEach(x=>x.classList.remove('active'));a.classList.add('active');const t=el(a.dataset.go);if(t)t.scrollIntoView({behavior:'smooth',block:'start'});}));
document.querySelectorAll('.fb').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.fb').forEach(x=>x.classList.remove('on'));b.classList.add('on');field=b.dataset.f;drawChart();}));
el('reset2').addEventListener('click',estopReset);
loadSchema();poll();drawChart();
setInterval(poll,1000);setInterval(drawChart,1000);setInterval(loadSchema,5000);
</script></body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Schema-driven controller dashboard")
    parser.add_argument("--socket-path", default=os.environ.get(
        "HYPERLOOP_CONTROLLER_SOCKET", DEFAULT_SOCKET_PATH))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--redis-url", default=os.environ.get(
        "HYPERLOOP_REDIS_URL", "redis://127.0.0.1:6379/0"),
        help="Redis URL for telemetry observability (empty string to disable)")
    args = parser.parse_args()
    redis_url = args.redis_url or None
    uvicorn.run(build_app(args.socket_path, redis_url=redis_url),
                host=args.host, port=args.port)


if __name__ == "__main__":
    main()
