import asyncio
import json
import os
import tempfile
import unittest

from controller import ControllerCore
from local_socket import LocalUnixSocketServer
from protocol import ErrorCode, MessageType
from state import BoardConnState
from tests.test_controller_core import FakeBoardWriter, board_ok_response, client_command, schema_for


def encode(message):
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


async def read_json(reader):
    return json.loads((await asyncio.wait_for(reader.readline(), timeout=0.5)).decode("utf-8"))


class LocalSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.tmp.name, "controller.sock")
        self.controller = ControllerCore(expected_boards={"motor"})
        self.board_writer = FakeBoardWriter()
        self.controller.register_board("motor", writer=self.board_writer, schema=schema_for("motor"))
        self.server = LocalUnixSocketServer(socket_path=self.socket_path, controller=self.controller)
        await self.server.start()

    async def asyncTearDown(self):
        await self.server.stop()
        self.tmp.cleanup()

    async def connect_client(self):
        return await asyncio.open_unix_connection(self.socket_path)

    async def wait_for_board_messages(self, count):
        for _ in range(100):
            if len(self.board_writer.messages) >= count:
                return
            await asyncio.sleep(0)
        self.fail(f"board writer received {len(self.board_writer.messages)} messages, expected {count}")

    async def wait_for_clients(self, count):
        for _ in range(100):
            if len(self.server.clients) >= count:
                return
            await asyncio.sleep(0)
        self.fail(f"server has {len(self.server.clients)} clients, expected {count}")

    async def test_client_connects_to_unix_socket(self):
        reader, writer = await self.connect_client()
        try:
            await self.wait_for_clients(1)
            self.assertTrue(os.path.exists(self.socket_path))
            self.assertEqual(len(self.server.clients), 1)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_valid_command_routes_through_controller_and_returns_same_client_response(self):
        reader, writer = await self.connect_client()
        try:
            writer.write(encode(client_command(seq=10)))
            await writer.drain()

            await self.wait_for_board_messages(1)
            board_seq = self.board_writer.messages[0]["seq"]
            await self.controller.handle_board_response(board_ok_response(board_seq))
            response = await read_json(reader)

            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["seq"], 10)
            self.assertEqual(response["target"], "gui")
            self.assertEqual(response["result"]["board_seq"], board_seq)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_malformed_json_returns_structured_error(self):
        reader, writer = await self.connect_client()
        try:
            writer.write(b"{bad-json\n")
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.INVALID_JSON.value)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_missing_required_fields_return_structured_error(self):
        reader, writer = await self.connect_client()
        try:
            writer.write(encode({"type": "command", "seq": 5, "source": "gui"}))
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["seq"], 5)
            self.assertEqual(response["target"], "gui")
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.MISSING_FIELD.value)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_over_limit_client_line_returns_structured_error(self):
        reader, writer = await self.connect_client()
        try:
            writer.write(b'{"type":"command","seq":9,"source":"gui","target":"' + (b"x" * 9000) + b'"}\n')
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.INVALID_JSON.value)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_unknown_target_returns_error_code_not_new_status(self):
        reader, writer = await self.connect_client()
        try:
            writer.write(encode(client_command(seq=6, target="missing")))
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
            self.assertNotIn(response["status"], {"rejected", "disconnected"})
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_client_disconnect_before_response_does_not_crash_controller(self):
        reader, writer = await self.connect_client()
        try:
            writer.write(encode(client_command(seq=7)))
            await writer.drain()
            await self.wait_for_board_messages(1)
            board_seq = self.board_writer.messages[0]["seq"]

            writer.close()
            await writer.wait_closed()
            await self.controller.handle_board_response(board_ok_response(board_seq))
            await asyncio.sleep(0)

            self.assertEqual(self.controller.pending_count(), 0)
            self.assertEqual(self.controller.in_flight_for("motor"), None)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_multiple_local_clients_can_connect(self):
        reader_a, writer_a = await self.connect_client()
        reader_b, writer_b = await self.connect_client()
        try:
            writer_a.write(encode(client_command(seq=1, target="missing")))
            writer_b.write(encode(client_command(seq=2, target="missing")))
            await writer_a.drain()
            await writer_b.drain()

            response_a = await read_json(reader_a)
            response_b = await read_json(reader_b)
            self.assertEqual(response_a["seq"], 1)
            self.assertEqual(response_b["seq"], 2)
            self.assertEqual(response_a["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
            self.assertEqual(response_b["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
        finally:
            writer_a.close()
            writer_b.close()
            await writer_a.wait_closed()
            await writer_b.wait_closed()

    async def test_unsolicited_event_full_duplex_broadcast(self):
        reader, writer = await self.connect_client()
        try:
            await self.wait_for_clients(1)
            event = {
                "type": "event",
                "event_id": 1,
                "source": "controller",
                "event": "board_disconnected",
                "details": {"board_id": "motor"},
            }

            await self.server.broadcast_event(event)
            received = await read_json(reader)

            self.assertEqual(received["type"], MessageType.EVENT.value)
            self.assertEqual(received["event"], "board_disconnected")
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_valid_command_path_does_not_require_redis(self):
        self.assertFalse(hasattr(self.server, "redis"))
        reader, writer = await self.connect_client()
        try:
            self.controller.set_board_state("motor", BoardConnState.CONNECTED)
            writer.write(encode(client_command(seq=8)))
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
        finally:
            writer.close()
            await writer.wait_closed()


if __name__ == "__main__":
    unittest.main()
