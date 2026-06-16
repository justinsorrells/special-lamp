import asyncio
import socket
import unittest
from collections import deque
from unittest.mock import patch

from board_connection import (
    BoardEndpoint,
    BoardTCPConnection,
    HeartbeatConfig,
    LivenessConfig,
    ReconnectBackoff,
)
from controller import ControllerCore
from observability import ObservabilityQueue
from protocol import ErrorCode, MessageType, parse_message
from state import BoardConnState
from tests.conftest import (
    FakeBoardWriter,
    FakeClock,
    async_wait_for,
    client_command,
    encode,
    ok_response,
    schema_for,
)


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
        self.suppress_schema = False
        self.auto_respond = False
        self.ack_heartbeats = False

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
        if self.suppress_schema:
            await reader.read()
            return
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
                elif parsed.message["type"] == MessageType.HEARTBEAT.value and self.ack_heartbeats:
                    writer.write(
                        encode(
                            {
                                "type": "heartbeat",
                                "seq": parsed.message["seq"],
                                "source": self.board_id,
                                "target": "controller",
                            }
                        )
                    )
                    await writer.drain()
        except ConnectionError:
            return


class BoardConnectionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = FakeBoardTCPServer()
        try:
            await self.server.start()
        except PermissionError as exc:
            raise unittest.SkipTest(f"TCP bind unavailable in this environment: {exc}") from exc
        self.controller = ControllerCore(expected_boards={"motor"})
        self.connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.server.port),
            self.controller,
            reconnect_backoff=ReconnectBackoff(base_delay_s=0.02, random_fraction=lambda: 1.0),
            liveness=LivenessConfig(enabled=False),
        )

    async def asyncTearDown(self):
        await self.connection.stop()
        await self.server.close()

    def start_connection(self):
        self.connection.start()
        return self.connection

    async def wait_for(self, predicate, timeout=1.0):
        await async_wait_for(predicate, timeout=timeout)

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
        self.assertGreaterEqual(self.controller.metrics_snapshot()["malformed_board_messages"], 1)

    async def test_board_disconnect_calls_board_down(self):
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.close_latest_client()
        await self.wait_for(
            lambda: self.controller.state.boards["motor"].conn_state == BoardConnState.FAULTED
        )
        self.assertEqual(self.controller.metrics_snapshot()["board_disconnects"], 1)

    async def test_reconnect_accepts_schema_push_again(self):
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.close_latest_client()
        await self.wait_for(lambda: self.server.connections >= 2)
        await self.connection.wait_registered()

        self.assertEqual(self.controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertGreaterEqual(self.server.connections, 2)
        self.assertEqual(
            self.controller.metrics_snapshot()["reconnect_count"], self.server.connections
        )

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
        events = []

        async def collect(event):
            events.append(event)

        self.controller.set_local_event_sink(collect)
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
        self.assertEqual(self.controller.state.boards["motor"].telemetry_rate.sample_count, 1)

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
        await self.wait_for(lambda: any(event["event"] == "estop_ack" for event in events))
        estop_ack_events = [event for event in events if event["event"] == "estop_ack"]
        self.assertEqual(len(estop_ack_events), 1)
        self.assertEqual(estop_ack_events[0]["details"], {"state": "safe"})

    async def test_board_telemetry_rate_tracking_updates_over_tcp(self):
        self.start_connection()
        await self.connection.wait_registered()

        await self.server.send_to_latest(
            {
                "type": "telemetry",
                "seq": 20,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 100},
            }
        )
        await self.wait_for(lambda: self.controller.state.boards["motor"].telemetry_rate.sample_count == 1)
        await asyncio.sleep(0.01)
        await self.server.send_to_latest(
            {
                "type": "telemetry",
                "seq": 21,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 120},
            }
        )
        await self.wait_for(lambda: self.controller.state.boards["motor"].telemetry_rate.sample_count == 2)

        rate = self.controller.state.boards["motor"].telemetry_rate
        self.assertIsNotNone(rate.rate_hz)
        self.assertGreater(rate.rate_hz, 0.0)
        self.assertIsNotNone(rate.last_interval_ms)

    async def test_disabled_heartbeat_produces_no_heartbeat_writes(self):
        self.start_connection()
        await self.connection.wait_registered()

        task = asyncio.create_task(self.controller.route_command(client_command(seq=30)))
        board_command = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        await self.server.send_to_latest(ok_response(board_command["seq"]))
        await task

        self.assertEqual(board_command["type"], MessageType.COMMAND.value)
        self.assertFalse(self.controller.state.boards["motor"].heartbeat_enabled)
        self.assertEqual(self.controller.metrics_snapshot()["heartbeat_acks_missed"], 0)
        self.assertTrue(self.server.commands.empty())

    async def test_enabled_heartbeat_sends_slow_message_and_ack_clears_suspect_state(self):
        await self.connection.stop()
        self.server.ack_heartbeats = True
        self.connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.server.port),
            self.controller,
            reconnect_delay_s=0.02,
            heartbeat=HeartbeatConfig(enabled=True, interval_s=0.01, ack_timeout_s=0.05),
        )
        self.start_connection()
        await self.connection.wait_registered()

        heartbeat = await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        await self.wait_for(lambda: self.controller.state.boards["motor"].last_heartbeat_ack_at is not None)

        board = self.controller.state.boards["motor"]
        self.assertEqual(heartbeat["type"], MessageType.HEARTBEAT.value)
        self.assertEqual(heartbeat["target"], "motor")
        self.assertTrue(board.heartbeat_enabled)
        self.assertFalse(board.rx_path_suspect)
        self.assertEqual(board.heartbeat_missed_count, 0)

    async def test_missed_heartbeat_marks_rx_path_suspect_while_telemetry_still_updates(self):
        await self.connection.stop()
        self.connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.server.port),
            self.controller,
            reconnect_delay_s=0.02,
            heartbeat=HeartbeatConfig(
                enabled=True,
                interval_s=0.01,
                ack_timeout_s=0.01,
                suspect_after_misses=2,
            ),
        )
        self.start_connection()
        await self.connection.wait_registered()

        await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        await self.wait_for(
            lambda: self.controller.metrics_snapshot()["heartbeat_acks_missed"] >= 1,
            timeout=0.5,
        )
        await asyncio.wait_for(self.server.commands.get(), timeout=0.5)
        await self.wait_for(lambda: self.controller.state.boards["motor"].rx_path_suspect)
        await self.server.send_to_latest(
            {
                "type": "telemetry",
                "seq": 40,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 100},
            }
        )
        await self.wait_for(lambda: self.controller.state.boards["motor"].last_telemetry == {"rpm": 100})

        board = self.controller.state.boards["motor"]
        self.assertTrue(board.rx_path_suspect)
        self.assertEqual(board.telemetry_rate.sample_count, 1)
        self.assertGreaterEqual(self.controller.metrics_snapshot()["heartbeat_acks_missed"], 2)

    async def test_late_and_malformed_heartbeat_ack_are_handled_safely(self):
        self.start_connection()
        await self.connection.wait_registered()

        await self.connection._handle_message(
            {
                "type": "heartbeat",
                "seq": 999,
                "source": "motor",
                "target": "controller",
            }
        )
        await self.connection._handle_message(
            {
                "type": "heartbeat",
                "source": "motor",
                "target": "controller",
            }
        )

        snapshot = self.controller.metrics_snapshot()
        self.assertEqual(snapshot["late_heartbeat_acks"], 1)
        self.assertEqual(snapshot["malformed_heartbeat_acks"], 1)

    async def test_liveness_watchdog_faults_stale_tcp_connection_and_reconnects(self):
        await self.connection.stop()
        self.connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.server.port),
            self.controller,
            reconnect_delay_s=0.01,
            liveness=LivenessConfig(timeout_s=0.03),
        )
        self.start_connection()
        await self.connection.wait_registered()

        await self.wait_for(
            lambda: self.controller.metrics_snapshot()["telemetry_liveness_timeouts"] >= 1,
            timeout=0.5,
        )
        await self.wait_for(lambda: self.server.connections >= 2, timeout=0.5)
        await self.connection.wait_registered(timeout=0.5)

        self.assertEqual(self.controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertGreaterEqual(self.server.connections, 2)

    async def test_registration_timeout_counter_increments(self):
        await self.connection.stop()
        self.server.suppress_schema = True
        self.connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", self.server.port),
            self.controller,
            reconnect_backoff=ReconnectBackoff(base_delay_s=0.01, random_fraction=lambda: 1.0),
            registration_timeout_s=0.01,
            liveness=LivenessConfig(enabled=False),
        )

        self.start_connection()
        await self.wait_for(
            lambda: self.controller.metrics_snapshot()["registration_timeouts"] >= 1,
            timeout=0.5,
        )

        self.assertEqual(self.controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)


class ReconnectBackoffTests(unittest.TestCase):
    def test_full_jitter_uses_exponential_ceiling_and_caps(self):
        fractions = deque([1.0, 0.5, 0.0, 1.0, 1.0])
        backoff = ReconnectBackoff(
            base_delay_s=0.5,
            factor=2.0,
            cap_delay_s=5.0,
            random_fraction=fractions.popleft,
        )

        self.assertEqual(backoff.next_delay_s(), 0.5)
        self.assertEqual(backoff.current_ceiling_s, 1.0)
        self.assertEqual(backoff.next_delay_s(), 0.5)
        self.assertEqual(backoff.current_ceiling_s, 2.0)
        self.assertEqual(backoff.next_delay_s(), 0.0)
        self.assertEqual(backoff.current_ceiling_s, 4.0)
        self.assertEqual(backoff.next_delay_s(), 4.0)
        self.assertEqual(backoff.current_ceiling_s, 5.0)
        self.assertEqual(backoff.next_delay_s(), 5.0)
        self.assertEqual(backoff.current_ceiling_s, 5.0)

    def test_reset_returns_ceiling_to_base(self):
        backoff = ReconnectBackoff(
            base_delay_s=0.5,
            factor=2.0,
            cap_delay_s=5.0,
            random_fraction=lambda: 1.0,
        )
        backoff.next_delay_s()
        backoff.next_delay_s()

        backoff.reset()

        self.assertEqual(backoff.current_ceiling_s, 0.5)
        self.assertEqual(backoff.next_delay_s(), 0.5)

    def test_validation_rejects_invalid_backoff_settings(self):
        with self.assertRaises(ValueError):
            ReconnectBackoff(base_delay_s=0.0)
        with self.assertRaises(ValueError):
            ReconnectBackoff(factor=1.0)
        with self.assertRaises(ValueError):
            ReconnectBackoff(base_delay_s=1.0, cap_delay_s=0.5)

        backoff = ReconnectBackoff(random_fraction=lambda: 1.01)
        with self.assertRaises(ValueError):
            backoff.next_delay_s()


class BoardConnectionUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_wait_is_interrupted_by_stop_event(self):
        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            reconnect_backoff=ReconnectBackoff(
                base_delay_s=10.0,
                cap_delay_s=10.0,
                random_fraction=lambda: 1.0,
            ),
        )

        wait_task = asyncio.create_task(connection._wait_before_reconnect())
        connection._stop.set()
        await asyncio.wait_for(wait_task, timeout=0.1)

    async def test_connect_attempt_is_bounded_by_connect_timeout(self):
        async def never_connect(*_args, **_kwargs):
            await asyncio.Event().wait()

        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            connect_timeout_s=0.01,
            liveness=LivenessConfig(enabled=False),
        )

        with patch("board_connection.asyncio.open_connection", side_effect=never_connect):
            await connection._connect_and_read_once()

        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)
        self.assertEqual(controller.metrics_snapshot()["board_disconnects"], 1)

    async def test_tcp_keepalive_is_enabled_on_connected_socket(self):
        class FakeSocket:
            def __init__(self):
                self.options = []

            def setsockopt(self, level, option, value):
                self.options.append((level, option, value))

        class FakeWriter:
            def __init__(self):
                self.raw_socket = FakeSocket()

            def get_extra_info(self, name):
                if name == "socket":
                    return self.raw_socket
                return None

        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(BoardEndpoint("motor", "127.0.0.1", 1), controller)
        writer = FakeWriter()

        connection._enable_tcp_keepalive(writer)

        self.assertIn((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), writer.raw_socket.options)

    async def test_successful_registration_resets_reconnect_backoff(self):
        server = FakeBoardTCPServer()
        try:
            await server.start()
        except PermissionError as exc:
            raise unittest.SkipTest(f"TCP bind unavailable in this environment: {exc}") from exc
        controller = ControllerCore(expected_boards={"motor"})
        backoff = ReconnectBackoff(
            base_delay_s=0.02,
            factor=2.0,
            cap_delay_s=1.0,
            random_fraction=lambda: 1.0,
        )
        backoff.next_delay_s()
        backoff.next_delay_s()
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", server.port),
            controller,
            reconnect_backoff=backoff,
        )
        try:
            connection.start()
            await connection.wait_registered()

            self.assertEqual(backoff.current_ceiling_s, 0.02)
            self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        finally:
            await connection.stop()
            await server.close()

    async def test_constructor_rejects_two_reconnect_delay_sources(self):
        controller = ControllerCore(expected_boards={"motor"})

        with self.assertRaises(ValueError):
            BoardTCPConnection(
                BoardEndpoint("motor", "127.0.0.1", 1),
                controller,
                reconnect_delay_s=0.1,
                reconnect_backoff=ReconnectBackoff(),
            )

    async def test_malformed_board_message_counter_increments_without_tcp_socket(self):
        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
        )
        reader = asyncio.StreamReader()
        reader.feed_data(b"{bad-json\n")
        reader.feed_eof()

        message = await connection._read_valid_message(reader)

        self.assertIsNone(message)
        self.assertEqual(controller.metrics_snapshot()["malformed_board_messages"], 1)

    async def test_heartbeat_write_uses_writer_lock_without_socket(self):
        class LockObservingWriter:
            def __init__(self):
                self.lock = asyncio.Lock()
                self.messages = []
                self.waiting_for_lock = asyncio.Event()

            async def write_message(self, message):
                self.waiting_for_lock.set()
                async with self.lock:
                    self.messages.append(message)

        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            heartbeat=HeartbeatConfig(enabled=True, ack_timeout_s=0.05),
        )
        writer = LockObservingWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        await writer.lock.acquire()
        try:
            task = asyncio.create_task(connection._send_one_heartbeat(writer))
            await asyncio.wait_for(writer.waiting_for_lock.wait(), timeout=0.5)
            self.assertEqual(writer.messages, [])
        finally:
            writer.lock.release()

        await async_wait_for(lambda: len(writer.messages) == 1, timeout=0.5)
        heartbeat = writer.messages[0]
        self.assertEqual(heartbeat["type"], MessageType.HEARTBEAT.value)
        await connection._handle_message(
            {
                "type": "heartbeat",
                "seq": heartbeat["seq"],
                "source": "motor",
                "target": "controller",
            }
        )
        await asyncio.wait_for(task, timeout=0.5)
        self.assertFalse(controller.state.boards["motor"].rx_path_suspect)

    async def test_heartbeat_miss_without_socket_marks_rx_path_suspect(self):
        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            heartbeat=HeartbeatConfig(
                enabled=True,
                ack_timeout_s=0.01,
                suspect_after_misses=1,
            ),
        )
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        await connection._send_one_heartbeat(writer)

        board = controller.state.boards["motor"]
        self.assertTrue(board.rx_path_suspect)
        self.assertEqual(board.heartbeat_missed_count, 1)
        self.assertEqual(controller.metrics_snapshot()["heartbeat_acks_missed"], 1)

    async def test_late_heartbeat_ack_after_miss_does_not_clear_suspect_state(self):
        controller = ControllerCore(expected_boards={"motor"})
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            heartbeat=HeartbeatConfig(
                enabled=True,
                ack_timeout_s=0.01,
                suspect_after_misses=1,
            ),
        )
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        await connection._send_one_heartbeat(writer)
        await connection._handle_message(
            {
                "type": "heartbeat",
                "seq": writer.messages[0]["seq"],
                "source": "motor",
                "target": "controller",
            }
        )

        board = controller.state.boards["motor"]
        self.assertTrue(board.rx_path_suspect)
        self.assertEqual(controller.metrics_snapshot()["late_heartbeat_acks"], 1)

    async def test_liveness_timeout_faults_board_and_fails_pending_commands(self):
        clock = FakeClock(100.0)
        controller = ControllerCore(expected_boards={"motor"}, monotonic_clock=clock)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            liveness=LivenessConfig(timeout_s=0.25),
        )
        connection._record_valid_inbound(
            {
                "type": "schema",
                "seq": 1,
                "source": "motor",
                "target": "controller",
                "protocol_version": "1",
                "schema": {"commands": {}, "telemetry": {}, "state": {}},
            }
        )
        first = asyncio.create_task(controller.route_command(client_command(seq=70)))
        await async_wait_for(lambda: len(writer.messages) == 1, timeout=0.5)
        second = asyncio.create_task(controller.route_command(client_command(seq=71)))
        await async_wait_for(lambda: controller.fifo_depth_for("motor") == 1, timeout=0.5)

        clock.advance(0.251)
        faulted = await connection._fault_if_liveness_expired()

        self.assertTrue(faulted)
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)
        first_response = await asyncio.wait_for(first, timeout=0.2)
        second_response = await asyncio.wait_for(second, timeout=0.2)
        self.assertEqual(first_response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
        self.assertEqual(second_response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["telemetry_liveness_timeouts"], 1)
        self.assertEqual(snapshot["board_disconnects"], 1)
        self.assertEqual(controller.pending_count(), 0)
        self.assertEqual(controller.fifo_depth_for("motor"), 0)

    async def test_healthy_telemetry_keeps_registered_before_liveness_threshold(self):
        clock = FakeClock(150.0)
        controller = ControllerCore(expected_boards={"motor"}, monotonic_clock=clock)
        controller.register_board("motor", writer=FakeBoardWriter(), schema=schema_for("motor"))
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            liveness=LivenessConfig(timeout_s=0.25),
        )
        telemetry = {
            "type": "telemetry",
            "seq": 1,
            "source": "motor",
            "target": "controller",
            "telemetry": {"rpm": 100},
        }

        connection._record_valid_inbound(telemetry)
        controller.record_board_telemetry(telemetry)
        clock.advance(0.249)

        self.assertFalse(await connection._fault_if_liveness_expired())
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertEqual(controller.state.boards["motor"].last_telemetry, {"rpm": 100})

    async def test_any_valid_inbound_packet_resets_liveness_deadline(self):
        clock = FakeClock(200.0)
        controller = ControllerCore(expected_boards={"motor"}, monotonic_clock=clock)
        controller.register_board("motor", writer=FakeBoardWriter(), schema=schema_for("motor"))
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            liveness=LivenessConfig(timeout_s=0.25),
        )
        connection._record_valid_inbound(
            {
                "type": "telemetry",
                "seq": 1,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 100},
            }
        )
        clock.advance(0.20)
        connection._record_valid_inbound(ok_response(42, board_id="motor"))
        clock.advance(0.20)

        self.assertFalse(await connection._fault_if_liveness_expired())
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertEqual(controller.metrics_snapshot()["telemetry_liveness_timeouts"], 0)

        clock.advance(0.051)
        self.assertTrue(await connection._fault_if_liveness_expired())
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)

    async def test_liveness_fault_does_not_depend_on_observability(self):
        clock = FakeClock(300.0)
        controller = ControllerCore(expected_boards={"motor"}, monotonic_clock=clock, observability=None)
        controller.register_board("motor", writer=FakeBoardWriter(), schema=schema_for("motor"))
        connection = BoardTCPConnection(
            BoardEndpoint("motor", "127.0.0.1", 1),
            controller,
            liveness=LivenessConfig(timeout_s=0.25),
        )
        connection._record_valid_inbound(
            {
                "type": "event",
                "source": "motor",
                "target": "controller",
                "event": "state_changed",
                "details": {},
            }
        )
        clock.advance(0.251)

        self.assertTrue(await connection._fault_if_liveness_expired())
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)
        self.assertEqual(controller.metrics_snapshot()["telemetry_liveness_timeouts"], 1)


if __name__ == "__main__":
    unittest.main()
