from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast

from board_connection import BoardEndpoint, BoardTCPConnection, LivenessConfig
from controller import ControllerCore
from demos.client.client import DEFAULT_CLIENT_SEQ, UnixSocketDemoClient
from demos.server.server import MockBoardServer
from demos.webapp_dashboard import ControllerLink, _coerce
from local_socket import LocalUnixSocketServer
from protocol import BOARD_MAX_LINE_BYTES, ErrorCode, MessageType, parse_message, serialize_message
from tests.conftest import FakeStreamWriter


async def read_json(reader: asyncio.StreamReader, *, timeout: float = 1.0) -> dict[str, Any]:
    line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=timeout)
    parsed = parse_message(line)
    if not parsed.ok:
        raise AssertionError(parsed.error)
    assert parsed.message is not None
    return parsed.message


class DemoMockBoardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.board = MockBoardServer(port=0, telemetry_interval_s=0.02)
        try:
            await self.board.start()
        except PermissionError as exc:
            raise unittest.SkipTest(f"TCP bind unavailable in this environment: {exc}") from exc

    async def asyncTearDown(self) -> None:
        await self.board.stop()

    async def test_mock_board_pushes_schema_then_unsolicited_telemetry(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.board.port)
        try:
            schema = await read_json(reader)
            telemetry = await read_json(reader)

            self.assertEqual(schema["type"], MessageType.SCHEMA.value)
            self.assertEqual(schema["source"], "motor")
            self.assertEqual(schema["protocol_version"], "1")
            self.assertTrue(schema["schema"]["commands"]["move"]["blocked_by_estop"])
            self.assertFalse(schema["schema"]["commands"]["status"]["blocked_by_estop"])
            self.assertEqual(telemetry["type"], MessageType.TELEMETRY.value)
            self.assertEqual(telemetry["source"], "motor")
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_mock_board_handles_command_and_estop_ack_without_udp(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.board.port)
        try:
            await read_json(reader)
            writer.write(
                serialize_message(
                    {
                        "type": MessageType.COMMAND.value,
                        "seq": 7,
                        "controller_ts": 123.0,
                        "source": "controller",
                        "target": "motor",
                        "command": "move",
                        "args": {"rpm": 900},
                    },
                    max_line_bytes=BOARD_MAX_LINE_BYTES,
                )
            )
            await writer.drain()
            response = await self._read_until(reader, MessageType.RESPONSE.value)

            writer.write(
                serialize_message(
                    {
                        "type": MessageType.ESTOP.value,
                        "source": "controller",
                        "target": "motor",
                    },
                    max_line_bytes=BOARD_MAX_LINE_BYTES,
                )
            )
            await writer.drain()
            ack = await self._read_until(reader, MessageType.EVENT.value, event="estop_ack")

            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["seq"], 7)
            self.assertEqual(response["controller_ts"], 123.0)
            self.assertEqual(response["result"]["rpm"], 900)
            self.assertEqual(ack["details"], {"state": "safe"})
            self.assertTrue(self.board.state.estop_received)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_mock_board_unknown_command_uses_contract_error_model(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.board.port)
        try:
            await read_json(reader)
            writer.write(
                serialize_message(
                    {
                        "type": MessageType.COMMAND.value,
                        "seq": 8,
                        "source": "controller",
                        "target": "motor",
                        "command": "missing",
                        "args": {},
                    },
                    max_line_bytes=BOARD_MAX_LINE_BYTES,
                )
            )
            await writer.drain()
            response = await self._read_until(reader, MessageType.RESPONSE.value)

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.UNKNOWN_COMMAND.value)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _read_until(
        self,
        reader: asyncio.StreamReader,
        message_type: str,
        *,
        event: str | None = None,
    ) -> dict[str, Any]:
        for _ in range(20):
            message = await read_json(reader)
            if message.get("type") != message_type:
                continue
            if event is not None and message.get("event") != event:
                continue
            return message
        self.fail(f"did not receive {message_type}")


class DemoMockBoardUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_schema_contains_required_contract_fields_without_socket(self) -> None:
        board = MockBoardServer()
        schema = board.schema_message()

        self.assertEqual(schema["type"], MessageType.SCHEMA.value)
        self.assertEqual(schema["protocol_version"], "1")
        self.assertIn("telemetry", schema["schema"])
        self.assertIn("state", schema["schema"])
        self.assertTrue(schema["schema"]["commands"]["move"]["blocked_by_estop"])
        self.assertFalse(schema["schema"]["commands"]["status"]["blocked_by_estop"])

    async def test_command_response_and_estop_ack_without_socket(self) -> None:
        board = MockBoardServer()
        writer = FakeStreamWriter()

        await board._handle_message(
            {
                "type": MessageType.COMMAND.value,
                "seq": 21,
                "controller_ts": 456.0,
                "source": "controller",
                "target": "motor",
                "command": "move",
                "args": {"rpm": 700},
            },
            cast(Any, writer),
        )
        await board._handle_message(
            {
                "type": MessageType.ESTOP.value,
                "source": "controller",
                "target": "motor",
            },
            cast(Any, writer),
        )
        messages = writer.messages()

        self.assertEqual(messages[0]["type"], MessageType.RESPONSE.value)
        self.assertEqual(messages[0]["status"], "ok")
        self.assertEqual(messages[0]["controller_ts"], 456.0)
        self.assertEqual(messages[0]["result"]["rpm"], 700)
        self.assertEqual(messages[1]["event"], "estop_ack")
        self.assertEqual(messages[1]["details"], {"state": "safe"})
        self.assertTrue(board.state.estop_received)


class DemoLocalLoopTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.tmp.name, "controller.sock")
        self.mock_board: MockBoardServer | None = None
        self.board_connection: BoardTCPConnection | None = None
        self.local_server: LocalUnixSocketServer | None = None
        self.controller: ControllerCore | None = None

    async def asyncTearDown(self) -> None:
        if self.local_server is not None:
            await self.local_server.stop()
        if self.board_connection is not None:
            await self.board_connection.stop()
        if self.mock_board is not None:
            await self.mock_board.stop()
        self.tmp.cleanup()

    async def start_stack(self) -> None:
        self.mock_board = MockBoardServer(port=0, telemetry_interval_s=0.02)
        try:
            await self.mock_board.start()
        except PermissionError as exc:
            raise unittest.SkipTest(f"TCP bind unavailable in this environment: {exc}") from exc
        self.controller = ControllerCore(expected_boards={"motor"}, default_execution_timeout_s=0.5)
        self.board_connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.mock_board.port),
            self.controller,
            reconnect_delay_s=0.02,
            registration_timeout_s=0.5,
            liveness=LivenessConfig(enabled=False),
        )
        self.board_connection.start()
        await self.board_connection.wait_registered(timeout=1.0)
        self.local_server = LocalUnixSocketServer(
            socket_path=self.socket_path,
            controller=self.controller,
        )
        try:
            await self.local_server.start()
        except PermissionError as exc:
            raise unittest.SkipTest(f"Unix socket bind unavailable in this environment: {exc}") from exc

    async def test_demo_client_routes_command_through_unix_socket_controller_and_tcp_board(self) -> None:
        await self.start_stack()
        assert self.mock_board is not None
        client = UnixSocketDemoClient(socket_path=self.socket_path)
        await client.connect()
        try:
            seq = await client.send_command(target="motor", command="move", args={"rpm": 1200})
            response = await client.read_response(seq, timeout_s=1.0)
            board_command = await asyncio.wait_for(self.mock_board.received_messages.get(), timeout=1.0)

            self.assertEqual(board_command["type"], MessageType.COMMAND.value)
            self.assertEqual(board_command["source"], "controller")
            self.assertEqual(board_command["target"], "motor")
            self.assertNotEqual(board_command["seq"], seq)
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["seq"], seq)
            self.assertEqual(response["result"]["board_seq"], board_command["seq"])
            self.assertNotEqual(response["seq"], response["result"]["board_seq"])
        finally:
            await client.close()

    async def test_demo_client_reads_unsolicited_event_on_full_duplex_socket(self) -> None:
        client = UnixSocketDemoClient(socket_path=self.socket_path)
        reader = asyncio.StreamReader()
        client.reader = reader
        client._reader_task = asyncio.create_task(client._read_loop())
        reader.feed_data(
            serialize_message(
                {
                    "type": MessageType.EVENT.value,
                    "source": "controller",
                    "event": "state",
                    "details": {"demo": True},
                }
            )
        )
        try:
            message = await client.read_message(timeout_s=1.0)

            self.assertEqual(message["type"], MessageType.EVENT.value)
            self.assertEqual(message["event"], "state")
        finally:
            await client.close()


class DemoClientUnitTests(unittest.TestCase):
    def test_auto_sequence_starts_outside_initial_board_sequence_range(self) -> None:
        client = UnixSocketDemoClient()

        self.assertEqual(client._next_seq(), DEFAULT_CLIENT_SEQ)
        self.assertNotEqual(DEFAULT_CLIENT_SEQ, 1)


class DashboardUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_estop_reset_uses_seq_correlated_request(self) -> None:
        link = ControllerLink("/tmp/not-used.sock")
        writer = FakeStreamWriter()
        link._writer = cast(Any, writer)
        link.connected = True

        task = asyncio.create_task(link.send_estop_reset())
        while not writer.messages():
            await asyncio.sleep(0)
        request = writer.messages()[0]
        link._dispatch(
            {
                "type": MessageType.RESPONSE.value,
                "seq": request["seq"],
                "source": "controller",
                "target": "webapp",
                "status": "ok",
                "result": {"estop_active": False},
                "error": None,
            }
        )
        response = await asyncio.wait_for(task, timeout=0.5)

        self.assertEqual(request["type"], MessageType.ESTOP_RESET.value)
        self.assertEqual(request["target"], "controller")
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["seq"], request["seq"])

    def test_bool_coercion_rejects_ambiguous_values_before_forwarding(self) -> None:
        self.assertTrue(_coerce("true", "bool"))
        self.assertFalse(_coerce("false", "bool"))
        with self.assertRaises(ValueError):
            _coerce("maybe", "bool")


class DemoSourceInvariantTests(unittest.TestCase):
    def test_demos_do_not_use_udp_or_direct_board_socket_patterns(self) -> None:
        demo_files = [
            Path("demos/server/server.py"),
            Path("demos/client/client.py"),
            Path("demos/webapp.py"),
        ]
        forbidden = ("SOCK_DGRAM", "AF_INET", "sendto(", "recvfrom(")
        for path in demo_files:
            source = path.read_text(encoding="utf-8")
            for pattern in forbidden:
                self.assertNotIn(pattern, source, f"{pattern} remains in {path}")


if __name__ == "__main__":
    unittest.main()
