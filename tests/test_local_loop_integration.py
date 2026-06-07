import asyncio
import json
import os
import tempfile
import unittest

from board_connection import BoardEndpoint, BoardTCPConnection
from controller import ControllerCore
from local_socket import LocalUnixSocketServer
from protocol import ErrorCode, MessageType
from tests.test_controller_core import client_command, schema_for

TERMINAL_STATUSES = {"ok", "error", "timeout"}


def encode(message):
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


def board_ok_response(board_command, *, board_id="motor"):
    return {
        "type": "response",
        "seq": board_command["seq"],
        "source": board_id,
        "target": "controller",
        "controller_ts": board_command.get("controller_ts"),
        "status": "ok",
        "result": {"accepted": True},
        "error": None,
    }


async def read_raw_json_line(reader, *, timeout=0.8):
    line = await asyncio.wait_for(reader.readline(), timeout=timeout)
    if not line:
        raise AssertionError("expected a newline JSON response, got EOF")
    if not line.endswith(b"\n"):
        raise AssertionError(f"response was not newline terminated: {line!r}")
    return line, json.loads(line.decode("utf-8"))


class FakeLoopBoardServer:
    def __init__(self, *, board_id="motor", auto_respond=False, close_on_command=False):
        self.board_id = board_id
        self.auto_respond = auto_respond
        self.close_on_command = close_on_command
        self.server = None
        self.port = None
        self.connections = 0
        self.commands = asyncio.Queue()
        self.raw_lines = asyncio.Queue()
        self.writers = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def close(self):
        for writer in list(self.writers):
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def send_to_latest(self, message):
        writer = self.writers[-1]
        writer.write(encode(message))
        await writer.drain()

    async def _handle(self, reader, writer):
        self.connections += 1
        self.writers.append(writer)
        writer.write(encode(schema_for(self.board_id)))
        await writer.drain()
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    return
                await self.raw_lines.put(line)
                if not line.endswith(b"\n"):
                    continue
                try:
                    message = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                if message.get("type") == MessageType.COMMAND.value:
                    await self.commands.put(message)
                    if self.close_on_command:
                        writer.close()
                        await writer.wait_closed()
                        return
                    if self.auto_respond:
                        writer.write(encode(board_ok_response(message, board_id=self.board_id)))
                        await writer.drain()
                elif message.get("type") == MessageType.ESTOP.value:
                    await self.commands.put(message)
        except ConnectionError:
            return


class LocalLoopIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.tmp.name, "controller.sock")
        self.board_server = None
        self.board_connection = None
        self.local_server = None

    async def asyncTearDown(self):
        if self.local_server is not None:
            await self.local_server.stop()
        if self.board_connection is not None:
            await self.board_connection.stop()
        if self.board_server is not None:
            await self.board_server.close()
        self.tmp.cleanup()

    async def start_stack(
        self,
        *,
        auto_respond=False,
        close_on_command=False,
        fifo_depth=6,
        default_execution_timeout_s=0.25,
    ):
        self.board_server = FakeLoopBoardServer(
            auto_respond=auto_respond,
            close_on_command=close_on_command,
        )
        await self.board_server.start()
        self.controller = ControllerCore(
            expected_boards={"motor"},
            fifo_depth=fifo_depth,
            default_execution_timeout_s=default_execution_timeout_s,
        )
        self.board_connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.board_server.port),
            self.controller,
            reconnect_delay_s=0.02,
            registration_timeout_s=0.5,
        )
        self.board_connection.start()
        await self.board_connection.wait_registered(timeout=1.0)
        self.local_server = LocalUnixSocketServer(
            socket_path=self.socket_path,
            controller=self.controller,
        )
        await self.local_server.start()

    async def connect_local_client(self):
        return await asyncio.open_unix_connection(self.socket_path)

    async def send_local(self, writer, message):
        payload = encode(message)
        self.assertTrue(payload.endswith(b"\n"))
        writer.write(payload)
        await writer.drain()

    async def read_until_seq(self, reader, seq):
        for _ in range(10):
            _, response = await read_raw_json_line(reader)
            if response.get("seq") == seq:
                return response
        self.fail(f"no response for client seq {seq}")

    async def test_valid_command_flows_local_socket_to_controller_to_tcp_board_and_back(self):
        await self.start_stack(auto_respond=True)
        reader, writer = await self.connect_local_client()
        try:
            await self.send_local(writer, client_command(seq=42))
            board_raw_line = await asyncio.wait_for(self.board_server.raw_lines.get(), timeout=0.8)
            board_command = await asyncio.wait_for(self.board_server.commands.get(), timeout=0.8)
            local_raw_line, response = await read_raw_json_line(reader)

            self.assertTrue(board_raw_line.endswith(b"\n"))
            self.assertTrue(local_raw_line.endswith(b"\n"))
            self.assertEqual(board_command["type"], MessageType.COMMAND.value)
            self.assertEqual(board_command["source"], "controller")
            self.assertEqual(board_command["target"], "motor")
            self.assertNotEqual(board_command["seq"], 42)
            self.assertIn(response["status"], TERMINAL_STATUSES)
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["seq"], 42)
            self.assertEqual(response["result"]["board_seq"], board_command["seq"])
            self.assertNotEqual(response["seq"], response["result"]["board_seq"])
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_malformed_local_client_message_returns_structured_error(self):
        await self.start_stack(auto_respond=True)
        reader, writer = await self.connect_local_client()
        try:
            writer.write(b"{bad-json\n")
            await writer.drain()
            _, response = await read_raw_json_line(reader)

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.INVALID_JSON.value)
            self.assertIn(response["status"], TERMINAL_STATUSES)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_board_disconnect_during_command_returns_board_unavailable(self):
        await self.start_stack(close_on_command=True)
        reader, writer = await self.connect_local_client()
        try:
            await self.send_local(writer, client_command(seq=43))
            response = await self.read_until_seq(reader, 43)

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
            self.assertIn(response["status"], TERMINAL_STATUSES)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_board_timeout_returns_terminal_timeout_to_local_client(self):
        await self.start_stack(default_execution_timeout_s=0.02)
        reader, writer = await self.connect_local_client()
        try:
            await self.send_local(writer, client_command(seq=44))
            board_command = await asyncio.wait_for(self.board_server.commands.get(), timeout=0.8)
            response = await self.read_until_seq(reader, 44)

            self.assertEqual(board_command["type"], MessageType.COMMAND.value)
            self.assertEqual(response["status"], "timeout")
            self.assertEqual(response["error"]["code"], ErrorCode.COMMAND_TIMEOUT.value)
            self.assertIn(response["status"], TERMINAL_STATUSES)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_late_board_response_after_timeout_is_dropped_logged(self):
        await self.start_stack(default_execution_timeout_s=0.02)
        reader, writer = await self.connect_local_client()
        try:
            await self.send_local(writer, client_command(seq=45))
            board_command = await asyncio.wait_for(self.board_server.commands.get(), timeout=0.8)
            response = await self.read_until_seq(reader, 45)
            await self.board_server.send_to_latest(board_ok_response(board_command))

            await self.wait_for(lambda: self.controller.counters.unmatched_seq == 1)
            self.assertEqual(response["status"], "timeout")
            self.assertEqual(response["error"]["code"], ErrorCode.COMMAND_TIMEOUT.value)
            self.assertEqual(self.controller.pending_count(), 0)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_board_busy_queue_full_surfaces_through_local_socket(self):
        await self.start_stack(fifo_depth=1, default_execution_timeout_s=0.3)
        reader, writer = await self.connect_local_client()
        try:
            await self.send_local(writer, client_command(seq=51))
            await asyncio.wait_for(self.board_server.commands.get(), timeout=0.8)
            await self.send_local(writer, client_command(seq=52))
            await self.wait_for(lambda: self.controller.fifo_depth_for("motor") == 1)
            await self.send_local(writer, client_command(seq=53))
            response = await self.read_until_seq(reader, 53)

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.BOARD_BUSY.value)
            self.assertEqual(self.controller.counters.board_busy_rejections, 1)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_estop_gate_surfaces_through_local_socket_without_board_write(self):
        await self.start_stack(auto_respond=True)
        self.controller.state.system.latch_estop()
        reader, writer = await self.connect_local_client()
        try:
            await self.send_local(writer, client_command(seq=61, command="move"))
            response = await self.read_until_seq(reader, 61)

            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
            self.assertTrue(self.board_server.commands.empty())
        finally:
            writer.close()
            await writer.wait_closed()

    async def wait_for(self, predicate, *, timeout=0.8):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.005)
        self.fail("condition was not met before timeout")


if __name__ == "__main__":
    unittest.main()
