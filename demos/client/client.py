"""Persistent Unix-socket demo client for the v1 controller."""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from protocol import CONTROLLER_MAX_LINE_BYTES, MessageType, parse_message, serialize_message

LOGGER = logging.getLogger(__name__)
DEFAULT_SOCKET_PATH = "/tmp/hyperloop-controller.sock"
DEFAULT_CLIENT_SEQ = 1000


@dataclass
class UnixSocketDemoClient:
    socket_path: str = DEFAULT_SOCKET_PATH
    source: str = "demo_client"
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None
    next_seq: int = DEFAULT_CLIENT_SEQ
    inbound: asyncio.Queue[dict[str, Any]] = field(default_factory=asyncio.Queue)
    _reader_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        if self.reader is not None or self.writer is not None:
            return
        self.reader, self.writer = await asyncio.open_unix_connection(
            self.socket_path,
            limit=CONTROLLER_MAX_LINE_BYTES,
        )
        self._reader_task = asyncio.create_task(self._read_loop())

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()
        self.reader = None
        self.writer = None

    async def send_command(
        self,
        *,
        target: str,
        command: str,
        args: dict[str, Any] | None = None,
        seq: int | None = None,
    ) -> int:
        if seq is None:
            seq = self._next_seq()
        await self._send(
            {
                "type": MessageType.COMMAND.value,
                "seq": seq,
                "source": self.source,
                "target": target,
                "command": command,
                "args": {} if args is None else args,
            }
        )
        return seq

    async def send_estop_reset(self, *, seq: int | None = None) -> int:
        if seq is None:
            seq = self._next_seq()
        await self._send(
            {
                "type": MessageType.ESTOP_RESET.value,
                "seq": seq,
                "source": self.source,
                "target": "controller",
            }
        )
        return seq

    async def read_message(self, *, timeout_s: float | None = None) -> dict[str, Any]:
        if timeout_s is None:
            return await self.inbound.get()
        return await asyncio.wait_for(self.inbound.get(), timeout=timeout_s)

    async def read_response(self, seq: int, *, timeout_s: float = 2.0) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"no response for seq {seq}")
            message = await self.read_message(timeout_s=remaining)
            if message.get("type") == MessageType.RESPONSE.value and message.get("seq") == seq:
                return message
            LOGGER.info("event: %s", message)

    async def _send(self, message: dict[str, Any]) -> None:
        if self.writer is None:
            raise RuntimeError("client is not connected")
        self.writer.write(serialize_message(message, max_line_bytes=CONTROLLER_MAX_LINE_BYTES))
        await self.writer.drain()

    async def _read_loop(self) -> None:
        if self.reader is None:
            return
        try:
            while True:
                line = await self.reader.readuntil(b"\n")
                parsed = parse_message(line, max_line_bytes=CONTROLLER_MAX_LINE_BYTES)
                if parsed.ok:
                    await self.inbound.put(parsed.message)
                else:
                    LOGGER.warning("discarded malformed controller message: %s", parsed.error)
        except asyncio.IncompleteReadError:
            return
        except asyncio.CancelledError:
            raise

    def _next_seq(self) -> int:
        seq = self.next_seq
        self.next_seq += 1
        return seq


def _parse_args(raw_args: list[str]) -> dict[str, Any]:
    if not raw_args:
        return {}
    result: dict[str, Any] = {}
    for item in raw_args:
        name, _, value = item.partition("=")
        if not name or not _:
            raise ValueError(f"argument {item!r} must use name=value")
        result[name] = _coerce_value(value)
    return result


def _coerce_value(value: str) -> int | float | bool | str:
    if value == "true":
        return True
    if value == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


async def run_once(args: argparse.Namespace) -> dict[str, Any]:
    client = UnixSocketDemoClient(socket_path=args.socket_path, source=args.source)
    await client.connect()
    try:
        if args.estop_reset:
            seq = await client.send_estop_reset()
        else:
            seq = await client.send_command(
                target=args.target,
                command=args.command,
                args=_parse_args(args.args),
            )
        return await client.read_response(seq, timeout_s=args.timeout_s)
    finally:
        await client.close()


async def watch_events(args: argparse.Namespace) -> None:
    client = UnixSocketDemoClient(socket_path=args.socket_path, source=args.source)
    await client.connect()
    try:
        while True:
            print(await client.read_message())
    finally:
        await client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send v1 messages to the controller Unix socket")
    parser.add_argument("--socket-path", default=DEFAULT_SOCKET_PATH)
    parser.add_argument("--source", default="demo_client")
    parser.add_argument("--target", default="motor")
    parser.add_argument("--command", default="status")
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--estop-reset", action="store_true")
    parser.add_argument("--watch", action="store_true", help="print all responses/events until interrupted")
    parser.add_argument("args", nargs="*", help="command args as name=value")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args()
    if args.watch:
        asyncio.run(watch_events(args))
        return
    print(asyncio.run(run_once(args)))


if __name__ == "__main__":
    main()
