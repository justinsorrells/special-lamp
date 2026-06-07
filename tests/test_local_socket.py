import asyncio
import json
import os
import tempfile
import unittest

from controller import ControllerCore
from local_socket import LocalClientConnection, LocalUnixSocketServer
from observability import ObservabilityQueue
from protocol import ErrorCode, MessageType
from state import BoardConnState
from tests.test_controller_core import FakeBoardWriter, board_ok_response, client_command, schema_for


def encode(message):
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


async def read_json(reader):
    return json.loads((await asyncio.wait_for(reader.readline(), timeout=0.5)).decode("utf-8"))


def event_message(event_id):
    return {
        "type": "event",
        "event_id": event_id,
        "source": "controller",
        "event": "board_state",
        "details": {"event_id": event_id},
    }


def ok_response(seq):
    return {
        "type": "response",
        "seq": seq,
        "source": "controller",
        "target": "gui",
        "status": "ok",
        "result": {},
        "error": None,
    }


def queued_payloads(client):
    return [item.message for item in list(client.outbound._queue) if item is not None]


class FakeLocalWriter:
    def __init__(self):
        self.closed = False
        self.writes = []

    def is_closing(self):
        return self.closed

    def write(self, data):
        self.writes.append(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class LocalSocketBackpressureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.controller = ControllerCore(expected_boards={"motor"})
        self.server = LocalUnixSocketServer(socket_path="/tmp/unbound-controller.sock", controller=self.controller)

    async def asyncTearDown(self):
        await asyncio.gather(*(client.close(flush=False) for client in list(self.server.clients)), return_exceptions=True)
        self.server.clients.clear()

    async def test_outbound_queue_depth_default_is_1000(self):
        self.assertEqual(self.server.outbound_queue_size, 1000)

    async def test_noncritical_event_backpressure_drops_oldest_and_counts(self):
        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=2,
            on_event_dropped=self.server._increment_client_event_dropped,
        )

        self.assertTrue(await client.send_event(event_message(1), critical=False))
        self.assertTrue(await client.send_event(event_message(2), critical=False))
        self.assertTrue(await client.send_event(event_message(3), critical=False))

        self.assertEqual(self.server.client_event_dropped, 1)
        self.assertEqual([message["event_id"] for message in queued_payloads(client)], [2, 3])
        self.assertTrue(client.is_connected)

    async def test_critical_backpressure_evicts_noncritical_before_disconnect(self):
        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=2,
            on_event_dropped=self.server._increment_client_event_dropped,
            on_critical_disconnect=self.server._increment_critical_event_disconnects,
        )

        self.assertTrue(await client.send_event(event_message(1), critical=False))
        self.assertTrue(await client.send_response(ok_response(2)))
        self.assertTrue(await client.send_response(ok_response(3)))

        self.assertEqual(self.server.client_event_dropped, 1)
        self.assertEqual(self.server.critical_event_disconnects, 0)
        self.assertEqual([message["seq"] for message in queued_payloads(client)], [2, 3])
        self.assertTrue(client.is_connected)

    async def test_critical_backpressure_disconnects_when_only_critical_messages_are_queued(self):
        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=1,
            on_critical_disconnect=self.server._increment_critical_event_disconnects,
        )

        self.assertTrue(await client.send_response(ok_response(1)))
        self.assertFalse(await client.send_response(ok_response(2)))

        self.assertFalse(client.connected)
        self.assertTrue(client.writer.closed)
        self.assertEqual(self.server.critical_event_disconnects, 1)

    async def test_broadcast_classifies_state_events_noncritical_by_default(self):
        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=1,
            on_event_dropped=self.server._increment_client_event_dropped,
            on_critical_disconnect=self.server._increment_critical_event_disconnects,
        )
        self.server.clients.add(client)

        await self.server.broadcast_event(event_message(1))
        await self.server.broadcast_event(event_message(2))

        self.assertEqual(self.server.client_event_dropped, 1)
        self.assertEqual([message["event_id"] for message in queued_payloads(client)], [2])
        self.assertEqual(self.server.critical_event_disconnects, 0)
        self.assertTrue(client.is_connected)

    async def test_broadcast_classifies_estop_events_critical_by_default(self):
        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=1,
            on_event_dropped=self.server._increment_client_event_dropped,
            on_critical_disconnect=self.server._increment_critical_event_disconnects,
        )
        self.server.clients.add(client)
        await client.send_response(ok_response(1))

        await self.server.broadcast_event(
            {
                "type": "event",
                "event_id": 2,
                "source": "controller",
                "event": "estop_triggered",
                "details": {"origin_board": "motor"},
            }
        )

        self.assertEqual(self.server.client_event_dropped, 0)
        self.assertEqual(self.server.critical_event_disconnects, 1)
        self.assertFalse(client.is_connected)

    async def test_saturated_critical_disconnect_does_not_affect_other_clients(self):
        saturated = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=1,
            on_critical_disconnect=self.server._increment_critical_event_disconnects,
        )
        receiving = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=1,
            on_critical_disconnect=self.server._increment_critical_event_disconnects,
        )
        self.server.clients.update({saturated, receiving})
        await saturated.send_response(ok_response(1))

        event = {
            "type": "event",
            "event_id": 2,
            "source": "controller",
            "event": "estop_triggered",
            "details": {"origin_board": "motor"},
        }
        await self.server.broadcast_event(event)

        self.assertFalse(saturated.is_connected)
        self.assertTrue(receiving.is_connected)
        self.assertEqual([message["event_id"] for message in queued_payloads(receiving)], [2])
        self.assertEqual(self.server.critical_event_disconnects, 1)


class LocalSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.tmp.name, "controller.sock")
        self.controller = ControllerCore(expected_boards={"motor"})
        self.board_writer = FakeBoardWriter()
        self.controller.register_board("motor", writer=self.board_writer, schema=schema_for("motor"))
        self.server = LocalUnixSocketServer(socket_path=self.socket_path, controller=self.controller)
        try:
            await self.server.start()
        except PermissionError as exc:
            self.tmp.cleanup()
            raise unittest.SkipTest(f"Unix socket bind unavailable in this environment: {exc}") from exc

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
        obs = ObservabilityQueue(maxsize=10)
        self.controller.observability = obs
        reader, writer = await self.connect_client()
        try:
            writer.write(b"{bad-json\n")
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.INVALID_JSON.value)
            event = [item for item in list(obs.queue._queue) if item["kind"] == "controller_event"][-1]
            self.assertEqual(event["fields"]["event"], "malformed_client_message")
            self.assertEqual(event["fields"]["details"]["error_code"], ErrorCode.INVALID_JSON.value)
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

    async def test_noncritical_event_backpressure_drops_oldest_and_counts(self):
        dropped = 0

        def count_drop():
            nonlocal dropped
            dropped += 1

        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=2,
            on_event_dropped=count_drop,
        )

        self.assertTrue(await client.send_event(event_message(1), critical=False))
        self.assertTrue(await client.send_event(event_message(2), critical=False))
        self.assertTrue(await client.send_event(event_message(3), critical=False))

        self.assertEqual(dropped, 1)
        self.assertEqual([message["event_id"] for message in queued_payloads(client)], [2, 3])
        self.assertTrue(client.is_connected)

    async def test_critical_backpressure_evicts_noncritical_before_disconnect(self):
        dropped = 0

        def count_drop():
            nonlocal dropped
            dropped += 1

        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=2,
            on_event_dropped=count_drop,
        )

        self.assertTrue(await client.send_event(event_message(1), critical=False))
        self.assertTrue(await client.send_response(ok_response(2)))
        self.assertTrue(await client.send_response(ok_response(3)))

        self.assertEqual(dropped, 1)
        self.assertEqual([message["seq"] for message in queued_payloads(client)], [2, 3])
        self.assertTrue(client.is_connected)

    async def test_critical_backpressure_disconnects_when_only_critical_messages_are_queued(self):
        disconnects = 0

        def count_disconnect():
            nonlocal disconnects
            disconnects += 1

        client = LocalClientConnection(
            reader=None,
            writer=FakeLocalWriter(),
            outbound_maxsize=1,
            on_critical_disconnect=count_disconnect,
        )

        self.assertTrue(await client.send_response(ok_response(1)))
        self.assertFalse(await client.send_response(ok_response(2)))

        self.assertFalse(client.connected)
        self.assertTrue(client.writer.closed)
        self.assertEqual(disconnects, 1)

    async def test_estop_reset_routes_to_controller_instead_of_unknown_command(self):
        self.controller.state.system.latch_estop()
        reader, writer = await self.connect_client()
        try:
            writer.write(
                encode(
                    {
                        "type": "estop_reset",
                        "seq": 11,
                        "source": "gui",
                        "target": "controller",
                    }
                )
            )
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "ok")
            self.assertEqual(response["seq"], 11)
            self.assertEqual(response["target"], "gui")
            self.assertEqual(response["result"]["estop_active"], False)
            self.assertFalse(self.controller.state.system.estop_active)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_estop_reset_preserves_latch_when_condition_is_not_cleared(self):
        await self.server.stop()
        self.server = LocalUnixSocketServer(
            socket_path=self.socket_path,
            controller=self.controller,
            estop_reset_condition_cleared=lambda: False,
        )
        await self.server.start()
        self.controller.state.system.latch_estop()
        reader, writer = await self.connect_client()
        try:
            writer.write(
                encode(
                    {
                        "type": "estop_reset",
                        "seq": 12,
                        "source": "gui",
                        "target": "controller",
                    }
                )
            )
            await writer.drain()

            response = await read_json(reader)
            self.assertEqual(response["status"], "error")
            self.assertEqual(response["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
            self.assertTrue(self.controller.state.system.estop_active)
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
