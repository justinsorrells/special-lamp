import asyncio
import json
import unittest

from board_connection import BoardEndpoint, BoardTCPConnection
from controller import ControllerCore
from observability import ObservabilityQueue
from protocol import ErrorCode, MessageType, parse_message
from state import BoardConnState
from tests.test_controller_core import client_command, schema_for


def encode(message):
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


def ok_response(board_seq, board_id="motor"):
    return {
        "type": "response",
        "seq": board_seq,
        "source": board_id,
        "target": "controller",
        "status": "ok",
        "result": {"accepted": True},
        "error": None,
    }


class FakeBoardTCPServer:
    def __init__(self, board_id="motor"):
        self.board_id = board_id
        self.server = None
        self.port = None
        self.connections = 0
        self.commands = asyncio.Queue()
        self.writers = []
        self.readers = []
        self.send_malformed_before_schema = False
        self.close_after_schema = False
        self.auto_respond = False

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
        self.server.close()
        await self.server.wait_closed()

    async def send_to_latest(self, message):
        writer = self.writers[-1]
        writer.write(encode(message))
        await writer.drain()

    async def send_raw_to_latest(self, data):
        writer = self.writers[-1]
        writer.write(data)
        await writer.drain()

    async def close_latest_client(self):
        writer = self.writers[-1]
        writer.close()
        await writer.wait_closed()

    async def _handle(self, reader, writer):
        self.connections += 1
        self.readers.append(reader)
        self.writers.append(writer)
        if self.send_malformed_before_schema:
            writer.write(b"not-json\n")
            await writer.drain()
        writer.write(encode(schema_for(self.board_id)))
        await writer.drain()
        if self.close_after_schema:
            writer.close()
            await writer.wait_closed()
            return
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    return
                parsed = parse_message(line)
                if not parsed.ok:
                    continue
                await self.commands.put(parsed.message)
                if parsed.message["type"] == MessageType.COMMAND.value and self.auto_respond:
                    writer.write(encode(ok_response(parsed.message["seq"], self.board_id)))
                    await writer.drain()
        except ConnectionError:
            return


class BoardConnectionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = FakeBoardTCPServer()
        await self.server.start()
        self.controller = ControllerCore(expected_boards={"motor"})
        self.connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.server.port),
            self.controller,
            reconnect_delay_s=0.02,
        )

    async def asyncTearDown(self):
        await self.connection.stop()
        await self.server.close()

    def start_connection(self):
        self.connection.start()
        return self.connection

    async def wait_for(self, predicate, timeout=1.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.005)
        self.fail("condition was not met before timeout")

    async def test_controller_connects_to_fake_board_and_accepts_schema_push(self):
        self.start_connection()
        await self.connection.wait_registered()

        self.assertEqual(self.server.connections, 1)
        self.assertEqual(self.controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertEqual(self.controller.state.boards["motor"].schema["source"], "motor")

    async def test_controller_sends_command_line_to_board_and_response_resolves_pending(self):
        self.start_connection()
        await self.connection.wait_registered()

        task = asyncio.create_task(self.controller.route_command(client_command(seq=10)))
        board_command = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        self.assertEqual(board_command["type"], MessageType.COMMAND.value)
        self.assertEqual(board_command["source"], "controller")
        self.assertEqual(board_command["target"], "motor")

        await self.server.send_to_latest(ok_response(board_command["seq"]))
        response = await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["seq"], 10)
        self.assertEqual(response["result"]["board_seq"], board_command["seq"])

    async def test_malformed_board_message_is_handled_safely(self):
        obs = ObservabilityQueue(maxsize=20)
        self.controller.observability = obs
        self.server.send_malformed_before_schema = True
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.send_raw_to_latest(b"{bad-json\n")
        task = asyncio.create_task(self.controller.route_command(client_command(seq=11)))
        board_command = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        await self.server.send_to_latest(ok_response(board_command["seq"]))

        self.assertEqual((await asyncio.wait_for(task, timeout=0.5))["status"], "ok")
        self.assertEqual(self.controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        events = [item for item in list(obs.queue._queue) if item["kind"] == "controller_event"]
        self.assertTrue(
            any(
                event["fields"]["event"] == "malformed_board_message"
                and event["fields"]["details"]["error_code"] == ErrorCode.INVALID_JSON.value
                for event in events
            )
        )

    async def test_board_disconnect_calls_board_down(self):
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.close_latest_client()
        await self.wait_for(
            lambda: self.controller.state.boards["motor"].conn_state == BoardConnState.FAULTED
        )

    async def test_reconnect_accepts_schema_push_again(self):
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.close_latest_client()
        await self.wait_for(lambda: self.server.connections >= 2)
        await self.connection.wait_registered()

        self.assertEqual(self.controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertGreaterEqual(self.server.connections, 2)

    async def test_late_response_after_timeout_is_dropped_through_core(self):
        self.start_connection()
        await self.connection.wait_registered()

        task = asyncio.create_task(
            self.controller.route_command(client_command(seq=12), execution_timeout_s=0.01)
        )
        board_command = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        timed_out = await asyncio.wait_for(task, timeout=0.5)
        await self.server.send_to_latest(ok_response(board_command["seq"]))
        await self.wait_for(lambda: self.controller.counters.unmatched_seq == 1)

        self.assertEqual(timed_out["status"], "timeout")
        self.assertEqual(timed_out["error"]["code"], ErrorCode.COMMAND_TIMEOUT.value)

    async def test_writer_serialization_keeps_concurrent_writes_as_valid_lines(self):
        self.start_connection()
        await self.connection.wait_registered()

        command_task = asyncio.create_task(self.controller.route_command(client_command(seq=13)))
        estop_task = asyncio.create_task(self.controller.send_estop_to_board("motor"))

        first = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        second = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        self.assertEqual({first["type"], second["type"]}, {"command", "estop"})
        self.assertEqual(first["target"], "motor")
        self.assertEqual(second["target"], "motor")

        command = first if first["type"] == MessageType.COMMAND.value else second
        await self.server.send_to_latest(ok_response(command["seq"]))
        self.assertEqual((await asyncio.wait_for(command_task, timeout=0.5))["status"], "ok")
        await asyncio.wait_for(estop_task, timeout=0.5)

    async def test_board_telemetry_and_estop_ack_event_update_state(self):
        obs = ObservabilityQueue(maxsize=20)
        self.controller.observability = obs
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.send_to_latest(
            {
                "type": "telemetry",
                "seq": 2,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 100},
            }
        )
        await self.wait_for(lambda: self.controller.state.boards["motor"].last_telemetry == {"rpm": 100})
        snapshots = [item for item in list(obs.queue._queue) if item["kind"] == "board_state"]
        self.assertTrue(any(item["hash"]["last_telemetry"] == {"rpm": 100} for item in snapshots))

        await self.server.send_to_latest(
            {
                "type": "event",
                "source": "motor",
                "target": "controller",
                "event": "estop_ack",
                "details": {"state": "safe"},
            }
        )
        await self.wait_for(lambda: self.controller.state.boards["motor"].estop_ack)


if __name__ == "__main__":
    unittest.main()
