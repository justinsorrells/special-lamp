"""Contract-faithful asyncio TCP mock board for local demos.

The mock board is an external actor: it listens as a TCP server, pushes its
schema immediately on each connection, emits one-way telemetry, and answers v1
newline-JSON commands from the controller. It does not import controller
internals and is intentionally useful both as a runnable demo and as an
integration-test fixture.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from protocol import (
    BOARD_MAX_LINE_BYTES,
    MessageType,
    PROTOCOL_VERSION,
    TerminalStatus,
    parse_message,
    serialize_message,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_BOARD_ID = "motor"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8767
TELEMETRY_INTERVAL_S = 0.05


@dataclass(frozen=True)
class MockCommand:
    args: dict[str, str]
    blocked_by_estop: bool = True


@dataclass
class MockBoardState:
    mode: str = "idle"
    rpm: int = 0
    temperature_c: float = 32.0
    voltage: float = 24.0
    faulted: bool = False
    estop_received: bool = False


@dataclass
class MockBoardServer:
    board_id: str = DEFAULT_BOARD_ID
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    telemetry_interval_s: float = TELEMETRY_INTERVAL_S
    protocol_version: str = PROTOCOL_VERSION
    firmware_version: str = "demo-0.1.0"
    commands: dict[str, MockCommand] = field(
        default_factory=lambda: {
            "move": MockCommand(args={"rpm": "int"}, blocked_by_estop=True),
            "status": MockCommand(args={}, blocked_by_estop=False),
            "legacy_motion": MockCommand(args={}),
        }
    )
    state: MockBoardState = field(default_factory=MockBoardState)
    server: asyncio.AbstractServer | None = None
    writers: list[asyncio.StreamWriter] = field(default_factory=list)
    received_messages: asyncio.Queue[dict[str, Any]] = field(default_factory=asyncio.Queue)
    sent_messages: asyncio.Queue[dict[str, Any]] = field(default_factory=asyncio.Queue)

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
            limit=BOARD_MAX_LINE_BYTES,
        )
        socket = self.server.sockets[0]
        self.port = socket.getsockname()[1]

    async def stop(self) -> None:
        for writer in list(self.writers):
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
        self.writers.clear()
        if self.server is None:
            return
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    def schema_message(self) -> dict[str, Any]:
        return {
            "type": MessageType.SCHEMA.value,
            "seq": 1,
            "timestamp": time.time(),
            "source": self.board_id,
            "target": "controller",
            "protocol_version": self.protocol_version,
            "schema": {
                "commands": {
                    name: {
                        "args": command.args,
                        "blocked_by_estop": command.blocked_by_estop,
                    }
                    for name, command in self.commands.items()
                },
                "telemetry": {
                    "rpm": "int",
                    "temperature_c": "float",
                    "voltage": "float",
                },
                "state": {
                    "mode": "string",
                    "faulted": "bool",
                },
                "firmware_version": self.firmware_version,
            },
        }

    async def trigger_estop(self, writer: asyncio.StreamWriter, *, reason: str = "demo") -> None:
        event = {
            "type": MessageType.EVENT.value,
            "timestamp": time.time(),
            "source": self.board_id,
            "target": "controller",
            "event": "estop_triggered",
            "details": {"reason": reason},
        }
        await self._write_message(writer, event)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        telemetry_task: asyncio.Task[None] | None = None
        self.writers.append(writer)
        try:
            await self._write_message(writer, self.schema_message())
            telemetry_task = asyncio.create_task(self._telemetry_loop(writer))
            while not reader.at_eof():
                message = await self._read_one_message(reader)
                if message is None:
                    return
                await self.received_messages.put(message)
                await self._handle_message(message, writer)
        finally:
            if telemetry_task is not None:
                telemetry_task.cancel()
                try:
                    await telemetry_task
                except asyncio.CancelledError:
                    pass
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            if writer in self.writers:
                self.writers.remove(writer)

    async def _read_one_message(self, reader: asyncio.StreamReader) -> dict[str, Any] | None:
        try:
            line = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError:
            return None
        except asyncio.LimitOverrunError:
            await reader.readuntil(b"\n")
            LOGGER.warning("discarded oversized command line for board %s", self.board_id)
            return None
        parsed = parse_message(line, max_line_bytes=BOARD_MAX_LINE_BYTES)
        if parsed.ok:
            return parsed.message
        LOGGER.warning(
            "discarded malformed board-inbound message for %s: %s",
            self.board_id,
            parsed.error,
        )
        return None

    async def _handle_message(
        self,
        message: dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        message_type = message["type"]
        if message_type == MessageType.COMMAND.value:
            await self._handle_command(message, writer)
        elif message_type == MessageType.ESTOP.value:
            await self._handle_estop(writer)
        elif message_type == MessageType.HEARTBEAT.value:
            await self._write_message(writer, self._heartbeat_ack(message))

    async def _handle_command(
        self,
        message: dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        command = message["command"]
        if command not in self.commands:
            await self._write_message(
                writer,
                self._response(
                    message,
                    status=TerminalStatus.ERROR,
                    result=None,
                    error={
                        "code": "UNKNOWN_COMMAND",
                        "message": f"unknown command {command}",
                    },
                ),
            )
            return
        result = self._execute_command(command, message["args"])
        await self._write_message(
            writer,
            self._response(message, status=TerminalStatus.OK, result=result, error=None),
        )

    def _execute_command(self, command: str, args: dict[str, Any]) -> dict[str, Any]:
        if command == "move":
            rpm = args.get("rpm", 0)
            if isinstance(rpm, int) and not isinstance(rpm, bool):
                self.state.rpm = rpm
                self.state.mode = "moving"
            return {"accepted": True, "rpm": self.state.rpm}
        if command == "status":
            return {
                "mode": self.state.mode,
                "rpm": self.state.rpm,
                "faulted": self.state.faulted,
                "estop_received": self.state.estop_received,
            }
        if command == "legacy_motion":
            self.state.mode = "legacy_motion"
            return {"accepted": True}
        return {"accepted": True}

    async def _handle_estop(self, writer: asyncio.StreamWriter) -> None:
        self.state.estop_received = True
        self.state.mode = "estop"
        self.state.rpm = 0
        await self._write_message(
            writer,
            {
                "type": MessageType.EVENT.value,
                "timestamp": time.time(),
                "source": self.board_id,
                "target": "controller",
                "event": "estop_ack",
                "details": {"state": "safe"},
            },
        )

    def _response(
        self,
        message: dict[str, Any],
        *,
        status: TerminalStatus,
        result: dict[str, Any] | None,
        error: dict[str, str] | None,
    ) -> dict[str, Any]:
        return {
            "type": MessageType.RESPONSE.value,
            "seq": message["seq"],
            "controller_ts": message.get("controller_ts"),
            "source": self.board_id,
            "target": "controller",
            "status": status.value,
            "result": result,
            "error": error,
            "board_proc_us": 100,
        }

    def _heartbeat_ack(self, message: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": MessageType.HEARTBEAT.value,
            "seq": message.get("seq", 0),
            "source": self.board_id,
            "target": "controller",
        }

    async def _telemetry_loop(self, writer: asyncio.StreamWriter) -> None:
        seq = 1
        try:
            while not writer.is_closing():
                await asyncio.sleep(self.telemetry_interval_s)
                await self._write_message(
                    writer,
                    {
                        "type": MessageType.TELEMETRY.value,
                        "seq": seq,
                        "timestamp": time.time(),
                        "source": self.board_id,
                        "target": "controller",
                        "telemetry": {
                            "rpm": self.state.rpm,
                            "temperature_c": self.state.temperature_c,
                            "voltage": self.state.voltage,
                        },
                    },
                )
                seq += 1
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            return

    async def _write_message(
        self,
        writer: asyncio.StreamWriter,
        message: dict[str, Any],
    ) -> None:
        writer.write(serialize_message(message))
        await writer.drain()
        await self.sent_messages.put(message)


async def run_server(args: argparse.Namespace) -> None:
    server = MockBoardServer(
        board_id=args.board_id,
        host=args.host,
        port=args.port,
        telemetry_interval_s=args.telemetry_interval_s,
    )
    await server.start()
    LOGGER.info("mock board %s listening on %s:%s", server.board_id, server.host, server.port)
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a v1 TCP mock board")
    parser.add_argument("--board-id", default=DEFAULT_BOARD_ID)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--telemetry-interval-s", type=float, default=TELEMETRY_INTERVAL_S)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(run_server(build_parser().parse_args()))


if __name__ == "__main__":
    main()
