"""Small FastAPI demo client for the controller Unix socket."""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from protocol import CONTROLLER_MAX_LINE_BYTES, MessageType, parse_message, serialize_message

DEFAULT_SOCKET_PATH = "/tmp/hyperloop-controller.sock"

app = FastAPI()
socket_path = os.environ.get("HYPERLOOP_CONTROLLER_SOCKET", DEFAULT_SOCKET_PATH)
last_result: dict[str, Any] | None = None


async def send_controller_command(
    *,
    target: str,
    command: str,
    args: dict[str, Any] | None = None,
    seq: int = 1,
) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(
        socket_path,
        limit=CONTROLLER_MAX_LINE_BYTES,
    )
    try:
        writer.write(
            serialize_message(
                {
                    "type": MessageType.COMMAND.value,
                    "seq": seq,
                    "source": "webapp",
                    "target": target,
                    "command": command,
                    "args": {} if args is None else args,
                },
                max_line_bytes=CONTROLLER_MAX_LINE_BYTES,
            )
        )
        await writer.drain()
        while True:
            line = await reader.readuntil(b"\n")
            parsed = parse_message(line, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)
            if parsed.ok and parsed.message is not None and parsed.message.get("type") == MessageType.RESPONSE.value:
                return parsed.message
    finally:
        writer.close()
        await writer.wait_closed()


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    result_html = ""
    if last_result is not None:
        result_html = f"<h2>Last response</h2><pre>{last_result}</pre>"
    return f"""
    <h1>Hyperloop Controller Demo</h1>
    <form method="get" action="/run">
      <label>Board <input name="target" value="motor"></label>
      <label>Command <input name="command" value="status"></label>
      <label>rpm <input name="rpm" value=""></label>
      <button type="submit">Send</button>
    </form>
    {result_html}
    """


@app.get("/run", response_class=HTMLResponse)
async def run(
    target: str = "motor",
    command: str = "status",
    rpm: str = "",
) -> str:
    global last_result
    args: dict[str, Any] = {}
    if rpm:
        args["rpm"] = int(rpm)
    last_result = await send_controller_command(target=target, command=command, args=args)
    return await index()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the controller Unix-socket demo webapp")
    parser.add_argument("--socket-path", default=DEFAULT_SOCKET_PATH)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser


def main() -> None:
    global socket_path
    args = build_parser().parse_args()
    socket_path = args.socket_path
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
