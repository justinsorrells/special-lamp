import asyncio
import copy
import unittest

from controller import ControllerCore
from protocol import (
    BOARD_MAX_LINE_BYTES,
    ErrorCode,
    MessageType,
    ProtocolValidationError,
    parse_message,
    validate_message,
)
from state import BoardConnState
from tests.conftest import (
    FakeBoardWriter,
    FakeClock,
    async_wait_for,
    client_command,
    ok_response,
    schema_for,
)


def schema_query(seq=90, args=None):
    return client_command(
        seq=seq,
        target="controller",
        command="get_schemas",
        args={} if args is None else args,
    )


class ControllerCoreTests(unittest.IsolatedAsyncioTestCase):
    def make_controller(self, *, boards=None, monotonic_clock=None):
        if boards is None:
            boards = {"motor"}
        return ControllerCore(expected_boards=set(boards), monotonic_clock=monotonic_clock)

    def register_motor(self, controller, writer=None):
        if writer is None:
            writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))
        return writer

    async def wait_for_messages(self, writer, count):
        try:
            await async_wait_for(lambda: len(writer.messages) >= count, timeout=0.5)
        except AssertionError:
            self.fail(f"writer only received {len(writer.messages)} messages, expected {count}")

    async def test_command_accepted_and_resolved_ok(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        task = asyncio.create_task(controller.route_command(client_command(seq=12)))
        await self.wait_for_messages(writer, 1)
        board_seq = writer.messages[0]["seq"]

        await controller.handle_board_response(ok_response(board_seq))
        response = await task

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["seq"], 12)
        self.assertEqual(response["result"]["board_seq"], board_seq)
        self.assertIsNone(controller.in_flight_for("motor"))
        self.assertEqual(controller.pending_count("motor"), 0)
        self.assertEqual(controller.metrics_snapshot()["commands_completed_ok"], 1)

    async def test_metrics_initialize_to_zero_and_snapshot_is_copy_safe(self):
        controller = self.make_controller()

        snapshot = controller.metrics_snapshot()
        expected_counters = {
            "obs_dropped",
            "unmatched_seq",
            "orphaned_response",
            "board_busy_rejections",
            "estop_rejections",
            "malformed_client_messages",
            "malformed_board_messages",
            "client_disconnects",
            "board_disconnects",
            "command_timeouts",
            "controller_shutdown_failures",
            "redis_write_failures",
            "local_event_dropped",
            "critical_event_disconnects",
            "late_board_responses",
            "duplicate_board_responses",
            "commands_completed_ok",
            "commands_completed_error",
            "commands_completed_timeout",
            "stale_command_rejections",
            "reconnect_count",
            "registration_timeouts",
            "protocol_version_mismatches",
            "client_event_dropped",
            "heartbeat_acks_missed",
            "malformed_heartbeat_acks",
            "late_heartbeat_acks",
            "telemetry_liveness_timeouts",
            "command_latency_sample_count",
            "command_latency_p50_ms",
            "command_latency_p95_ms",
            "command_latency_p99_ms",
            "boards",
        }

        self.assertEqual(set(snapshot), expected_counters)
        numeric_values = {key: value for key, value in snapshot.items() if key != "boards"}
        self.assertTrue(
            all(value in {0, None} for value in numeric_values.values())
        )
        self.assertEqual(
            snapshot["boards"]["motor"],
            {
                "conn_state": BoardConnState.DISCONNECTED.value,
                "estop_ack": False,
                "queue_depth": 0,
                "in_flight_board_seq": None,
                "telemetry_rate_hz": None,
                "telemetry_jitter_ms": None,
                "telemetry_interval_ms": None,
                "telemetry_sample_count": 0,
                "command_latency_sample_count": 0,
                "command_latency_p50_ms": None,
                "command_latency_p95_ms": None,
                "command_latency_p99_ms": None,
            },
        )
        snapshot["unmatched_seq"] = 99
        self.assertEqual(controller.metrics_snapshot()["unmatched_seq"], 0)

    async def test_heartbeat_state_is_observability_only_and_separate_from_connection_axis(self):
        controller = self.make_controller()
        self.register_motor(controller)

        controller.record_heartbeat_sent("motor", seq=1, sent_at=10.0)
        controller.record_heartbeat_missed("motor", seq=1, suspect_after_misses=1)
        board = controller.state.boards["motor"]

        self.assertEqual(board.conn_state, BoardConnState.REGISTERED)
        self.assertTrue(board.heartbeat_enabled)
        self.assertTrue(board.rx_path_suspect)
        self.assertEqual(controller.metrics_snapshot()["heartbeat_acks_missed"], 1)

        controller.record_heartbeat_ack("motor", seq=2, ack_at=11.0)
        self.assertEqual(board.conn_state, BoardConnState.REGISTERED)
        self.assertFalse(board.rx_path_suspect)
        self.assertEqual(board.heartbeat_missed_count, 0)

    async def test_command_send_records_monotonic_controller_timestamp(self):
        clock = FakeClock(500.25)
        controller = self.make_controller(monotonic_clock=clock)
        writer = self.register_motor(controller)

        task = asyncio.create_task(controller.route_command(client_command(seq=14)))
        await self.wait_for_messages(writer, 1)
        board_command = writer.messages[0]

        self.assertEqual(board_command["controller_ts"], 500.25)
        self.assertEqual(board_command["controller_ts"], controller._pending[board_command["seq"]].written_at)

        await controller.handle_board_response(ok_response(board_command["seq"]))
        await task

    async def test_command_response_records_monotonic_latency_and_board_proc_us(self):
        clock = FakeClock(100.0)
        controller = self.make_controller(monotonic_clock=clock)
        writer = self.register_motor(controller)

        task = asyncio.create_task(controller.route_command(client_command(seq=15)))
        await self.wait_for_messages(writer, 1)
        board_seq = writer.messages[0]["seq"]
        clock.advance(0.125)
        client_response = await controller.handle_board_response(
            ok_response(board_seq, board_proc_us=2400)
        )
        response = await task
        observation = controller.state.boards["motor"].last_command_latency

        self.assertEqual(client_response, response)
        self.assertEqual(response["status"], "ok")
        self.assertAlmostEqual(response["result"]["latency_ms"], 125.0)
        self.assertIsNotNone(observation)
        self.assertEqual(observation.board_seq, board_seq)
        self.assertAlmostEqual(observation.latency_ms, 125.0)
        self.assertEqual(observation.controller_ts, 100.0)
        self.assertEqual(observation.observed_at, 100.125)
        self.assertEqual(observation.board_proc_us, 2400.0)

    async def test_latency_calculation_does_not_use_wall_clock(self):
        clock = FakeClock(10.0)
        controller = self.make_controller(monotonic_clock=clock)
        writer = self.register_motor(controller)

        task = asyncio.create_task(controller.route_command(client_command(seq=16)))
        await self.wait_for_messages(writer, 1)
        board_seq = writer.messages[0]["seq"]
        import time

        original_time = time.time
        try:
            time.time = lambda: -1_000_000.0
            clock.advance(0.050)
            response = await controller.handle_board_response(ok_response(board_seq))
        finally:
            time.time = original_time
        await task

        self.assertAlmostEqual(response["result"]["latency_ms"], 50.0)
        self.assertGreaterEqual(response["result"]["latency_ms"], 0.0)

    async def test_missing_and_invalid_board_proc_us_are_accepted(self):
        for board_response, expected in (
            (ok_response(1), None),
            (ok_response(1, board_proc_us="bad"), None),
            (ok_response(1, board_proc_us=-1), None),
            (ok_response(1, result={"accepted": True, "board_proc_us": 12}), 12.0),
        ):
            clock = FakeClock(200.0)
            controller = self.make_controller(monotonic_clock=clock)
            writer = self.register_motor(controller)
            task = asyncio.create_task(controller.route_command(client_command(seq=17)))
            await self.wait_for_messages(writer, 1)
            board_response["seq"] = writer.messages[0]["seq"]
            clock.advance(0.001)
            response = await controller.handle_board_response(board_response)
            await task

            self.assertEqual(response["status"], "ok")
            self.assertEqual(controller.state.boards["motor"].last_command_latency.board_proc_us, expected)

    async def test_telemetry_rate_tracking_handles_first_and_irregular_samples(self):
        clock = FakeClock(20.0)
        controller = self.make_controller(monotonic_clock=clock)
        self.register_motor(controller)

        telemetry = {
            "type": "telemetry",
            "seq": 1,
            "source": "motor",
            "target": "controller",
            "telemetry": {"rpm": 100},
        }
        controller.observe_board_telemetry(telemetry)
        rate = controller.state.boards["motor"].telemetry_rate
        self.assertEqual(rate.sample_count, 1)
        self.assertIsNone(rate.rate_hz)
        self.assertIsNone(rate.jitter_ms)

        clock.advance(0.05)
        controller.observe_board_telemetry({**telemetry, "seq": 2, "telemetry": {"rpm": 110}})
        self.assertEqual(rate.sample_count, 2)
        self.assertAlmostEqual(rate.last_interval_ms, 50.0)
        self.assertAlmostEqual(rate.rate_hz, 20.0)
        self.assertIsNone(rate.jitter_ms)

        clock.advance(0.08)
        controller.observe_board_telemetry({**telemetry, "seq": 3, "telemetry": {"rpm": 120}})
        self.assertEqual(rate.sample_count, 3)
        self.assertAlmostEqual(rate.last_interval_ms, 80.0)
        self.assertAlmostEqual(rate.rate_hz, 12.5)
        self.assertAlmostEqual(rate.jitter_ms, 30.0)
        self.assertEqual(controller.state.boards["motor"].last_telemetry, {"rpm": 120})
        self.assertEqual(controller.state.boards["motor"].last_seen, 20.13)

    async def test_telemetry_tracking_tolerates_reconnect_state_changes(self):
        clock = FakeClock(30.0)
        controller = self.make_controller(monotonic_clock=clock)
        writer = self.register_motor(controller)
        telemetry = {
            "type": "telemetry",
            "seq": 1,
            "source": "motor",
            "target": "controller",
            "telemetry": {"rpm": 100},
        }

        controller.observe_board_telemetry(telemetry)
        await controller.board_down("motor")
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))
        clock.advance(0.05)
        controller.observe_board_telemetry({**telemetry, "seq": 2})

        rate = controller.state.boards["motor"].telemetry_rate
        self.assertEqual(rate.sample_count, 2)
        self.assertAlmostEqual(rate.rate_hz, 20.0)
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)

    async def test_unknown_board_returns_unknown_target(self):
        controller = self.make_controller()
        response = await controller.route_command(client_command(target="missing"))

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["commands_completed_error"], 1)

    async def test_board_unavailable_when_not_registered(self):
        controller = self.make_controller()
        response = await controller.route_command(client_command())

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)

    async def test_board_unavailable_when_conn_state_not_registered(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        controller.set_board_state("motor", BoardConnState.CONNECTED)

        response = await controller.route_command(client_command())

        self.assertEqual(response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
        self.assertEqual(writer.messages, [])

    async def test_protocol_version_mismatch_faults_board_before_raising(self):
        controller = self.make_controller()
        writer = FakeBoardWriter()
        bad_schema = schema_for("motor")
        bad_schema["protocol_version"] = "2"

        with self.assertRaises(ProtocolValidationError):
            controller.register_board("motor", writer=writer, schema=bad_schema)

        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)
        self.assertEqual(controller.metrics_snapshot()["protocol_version_mismatches"], 1)
        self.assertEqual(writer.messages, [])

    async def test_metrics_record_latency_percentiles_from_monotonic_samples(self):
        clock = FakeClock(1000.0)
        controller = self.make_controller(monotonic_clock=clock)
        writer = self.register_motor(controller)

        for seq, latency_s in enumerate((0.01, 0.02, 0.03, 0.04, 0.10), start=1):
            task = asyncio.create_task(controller.route_command(client_command(seq=seq)))
            await self.wait_for_messages(writer, seq)
            board_seq = writer.messages[-1]["seq"]
            clock.advance(latency_s)
            await controller.handle_board_response(ok_response(board_seq))
            response = await task
            self.assertEqual(response["status"], "ok")

        snapshot = controller.metrics_snapshot()
        board_metrics = snapshot["boards"]["motor"]
        self.assertEqual(snapshot["command_latency_sample_count"], 5)
        self.assertAlmostEqual(snapshot["command_latency_p50_ms"], 30.0)
        self.assertAlmostEqual(snapshot["command_latency_p95_ms"], 100.0)
        self.assertAlmostEqual(snapshot["command_latency_p99_ms"], 100.0)
        self.assertEqual(board_metrics["command_latency_sample_count"], 5)
        self.assertAlmostEqual(board_metrics["command_latency_p50_ms"], 30.0)
        self.assertAlmostEqual(board_metrics["command_latency_p95_ms"], 100.0)
        self.assertAlmostEqual(board_metrics["command_latency_p99_ms"], 100.0)

    async def test_unknown_command_returns_unknown_command_error(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        response = await controller.route_command(client_command(command="missing"))

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.UNKNOWN_COMMAND.value)
        self.assertEqual(writer.messages, [])

    async def test_execution_timeout_hard_ceiling_is_enforced(self):
        controller = self.make_controller()
        self.register_motor(controller)

        with self.assertRaises(ValueError):
            await controller.route_command(client_command(), execution_timeout_s=10.001)

    async def test_board_busy_when_fifo_is_full(self):
        controller = ControllerCore(expected_boards={"motor"}, fifo_depth=1)
        writer = self.register_motor(controller)

        first = asyncio.create_task(controller.route_command(client_command(seq=1)))
        await self.wait_for_messages(writer, 1)
        second = asyncio.create_task(controller.route_command(client_command(seq=2)))
        await asyncio.sleep(0)
        busy = await controller.route_command(client_command(seq=3))

        self.assertEqual(busy["error"]["code"], ErrorCode.BOARD_BUSY.value)
        self.assertEqual(controller.counters.board_busy_rejections, 1)
        self.assertEqual(controller.fifo_depth_for("motor"), 1)

        await controller.handle_board_response(ok_response(writer.messages[0]["seq"]))
        await self.wait_for_messages(writer, 2)
        await controller.handle_board_response(ok_response(writer.messages[1]["seq"]))

        self.assertEqual((await first)["status"], "ok")
        self.assertEqual((await second)["status"], "ok")

    async def test_timeout_after_board_write(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        task = asyncio.create_task(
            controller.route_command(client_command(seq=1), execution_timeout_s=0.01)
        )
        await self.wait_for_messages(writer, 1)
        response = await asyncio.wait_for(task, timeout=0.2)

        self.assertEqual(response["status"], "timeout")
        self.assertEqual(response["error"]["code"], ErrorCode.COMMAND_TIMEOUT.value)
        self.assertIsNone(controller.in_flight_for("motor"))
        self.assertEqual(controller.pending_count(), 0)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["commands_completed_timeout"], 1)
        self.assertEqual(snapshot["command_timeouts"], 1)

    async def test_queue_residency_timeout_cap(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        first = asyncio.create_task(controller.route_command(client_command(seq=1)))
        await self.wait_for_messages(writer, 1)
        second = asyncio.create_task(
            controller.route_command(client_command(seq=2), queue_residency_cap_s=0.0)
        )
        await asyncio.sleep(0)

        await controller.handle_board_response(ok_response(writer.messages[0]["seq"]))
        second_response = await asyncio.wait_for(second, timeout=0.2)

        self.assertEqual((await first)["status"], "ok")
        self.assertEqual(second_response["status"], "timeout")
        self.assertEqual(second_response["error"]["code"], ErrorCode.COMMAND_TIMEOUT.value)
        self.assertEqual(controller.counters.stale_command_rejections, 1)
        self.assertEqual(len(writer.messages), 1)

    async def test_late_board_response_after_timeout_is_dropped_logged(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        task = asyncio.create_task(
            controller.route_command(client_command(seq=1), execution_timeout_s=0.01)
        )
        await self.wait_for_messages(writer, 1)
        board_seq = writer.messages[0]["seq"]
        response = await asyncio.wait_for(task, timeout=0.2)
        late = await controller.handle_board_response(ok_response(board_seq))

        self.assertEqual(response["status"], "timeout")
        self.assertIsNone(late)
        self.assertEqual(controller.counters.unmatched_seq, 1)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["late_board_responses"], 1)
        self.assertEqual(snapshot["duplicate_board_responses"], 0)

    async def test_duplicate_board_response_does_not_double_resolve(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        task = asyncio.create_task(controller.route_command(client_command(seq=1)))
        await self.wait_for_messages(writer, 1)
        board_seq = writer.messages[0]["seq"]
        first = await controller.handle_board_response(ok_response(board_seq))
        duplicate = await controller.handle_board_response(ok_response(board_seq))
        response = await task

        self.assertEqual(first["status"], "ok")
        self.assertEqual(response["status"], "ok")
        self.assertIsNone(duplicate)
        self.assertEqual(controller.counters.unmatched_seq, 1)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["duplicate_board_responses"], 1)
        self.assertEqual(snapshot["late_board_responses"], 0)

    async def test_board_down_resolves_in_flight_and_queued_commands(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        first = asyncio.create_task(controller.route_command(client_command(seq=1)))
        await self.wait_for_messages(writer, 1)
        second = asyncio.create_task(controller.route_command(client_command(seq=2)))
        await asyncio.sleep(0)

        await controller.board_down("motor")

        first_response = await asyncio.wait_for(first, timeout=0.2)
        second_response = await asyncio.wait_for(second, timeout=0.2)
        self.assertEqual(first_response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
        self.assertEqual(second_response["error"]["code"], ErrorCode.BOARD_UNAVAILABLE.value)
        self.assertEqual(controller.pending_count(), 0)
        self.assertEqual(controller.fifo_depth_for("motor"), 0)
        self.assertIsNone(controller.in_flight_for("motor"))
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.FAULTED)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["board_disconnects"], 1)
        self.assertEqual(snapshot["commands_completed_error"], 2)

    async def test_shutdown_rejects_new_commands_with_controller_shutdown(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        await controller.shutdown(drain_timeout_s=0.0)
        response = await controller.route_command(client_command(seq=44))

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(writer.messages, [])
        self.assertEqual(controller.metrics_snapshot()["commands_completed_error"], 1)

    async def test_shutdown_allows_in_flight_command_to_complete_during_drain(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        command_task = asyncio.create_task(controller.route_command(client_command(seq=45)))
        await self.wait_for_messages(writer, 1)
        board_seq = writer.messages[0]["seq"]
        shutdown_task = asyncio.create_task(controller.shutdown(drain_timeout_s=0.2))
        await asyncio.sleep(0)

        await controller.handle_board_response(ok_response(board_seq))
        await asyncio.wait_for(shutdown_task, timeout=0.5)
        response = await asyncio.wait_for(command_task, timeout=0.2)

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["result"]["board_seq"], board_seq)
        self.assertEqual(controller.pending_count(), 0)
        self.assertIsNone(controller.in_flight_for("motor"))
        self.assertTrue(writer.closed)
        self.assertEqual(controller.metrics_snapshot()["commands_completed_ok"], 1)

    async def test_shutdown_quiesces_dispatch_and_fails_queued_command_after_drain(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        first = asyncio.create_task(controller.route_command(client_command(seq=46)))
        await self.wait_for_messages(writer, 1)
        second = asyncio.create_task(controller.route_command(client_command(seq=47)))
        await asyncio.sleep(0)
        shutdown_task = asyncio.create_task(controller.shutdown(drain_timeout_s=0.2))
        await async_wait_for(lambda: controller._shutting_down, timeout=0.2)

        await controller.handle_board_response(ok_response(writer.messages[0]["seq"]))
        await asyncio.wait_for(shutdown_task, timeout=0.5)
        first_response = await asyncio.wait_for(first, timeout=0.2)
        second_response = await asyncio.wait_for(second, timeout=0.2)

        self.assertEqual(first_response["status"], "ok")
        self.assertEqual(second_response["status"], "error")
        self.assertEqual(second_response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(len(writer.messages), 1)
        self.assertEqual(controller.pending_count(), 0)
        self.assertEqual(controller.fifo_depth_for("motor"), 0)

    async def test_shutdown_fails_unresolved_in_flight_and_queued_commands(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        first = asyncio.create_task(controller.route_command(client_command(seq=48)))
        await self.wait_for_messages(writer, 1)
        second = asyncio.create_task(controller.route_command(client_command(seq=49)))
        await asyncio.sleep(0)
        board_seq = writer.messages[0]["seq"]

        await controller.shutdown(drain_timeout_s=0.0)
        first_response = await asyncio.wait_for(first, timeout=0.2)
        second_response = await asyncio.wait_for(second, timeout=0.2)

        self.assertEqual(first_response["status"], "error")
        self.assertEqual(first_response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(second_response["status"], "error")
        self.assertEqual(second_response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(controller.pending_count(), 0)
        self.assertEqual(controller.fifo_depth_for("motor"), 0)
        self.assertIsNone(controller.in_flight_for("motor"))
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.DISCONNECTED)
        self.assertTrue(writer.closed)
        self.assertEqual(writer.close_count, 1)

        late = await controller.handle_board_response(ok_response(board_seq))
        self.assertIsNone(late)
        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["unmatched_seq"], 1)
        self.assertEqual(snapshot["duplicate_board_responses"], 1)
        self.assertEqual(snapshot["commands_completed_error"], 2)

    async def test_shutdown_during_board_write_keeps_pending_until_failure(self):
        controller = self.make_controller()
        writer = FakeBoardWriter()
        writer.use_gate = True
        self.register_motor(controller, writer)

        command_task = asyncio.create_task(controller.route_command(client_command(seq=51)))
        await writer.started.wait()
        shutdown_task = asyncio.create_task(controller.shutdown(drain_timeout_s=0.0))
        await async_wait_for(lambda: controller._shutting_down, timeout=0.2)

        writer.allow_finish.set()
        await asyncio.wait_for(shutdown_task, timeout=0.5)
        response = await asyncio.wait_for(command_task, timeout=0.2)

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(controller.pending_count(), 0)
        self.assertIsNone(controller.in_flight_for("motor"))
        self.assertTrue(writer.closed)

    async def test_shutdown_is_idempotent(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        task = asyncio.create_task(controller.route_command(client_command(seq=50)))
        await self.wait_for_messages(writer, 1)

        await asyncio.gather(
            controller.shutdown(drain_timeout_s=0.0),
            controller.shutdown(drain_timeout_s=0.0),
        )
        response = await asyncio.wait_for(task, timeout=0.2)

        self.assertEqual(response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(writer.close_count, 1)
        self.assertEqual(controller.pending_count(), 0)

    async def test_shutdown_observability_stop_failure_is_bounded_and_counted(self):
        class FailingObservability:
            async def stop(self):
                raise RuntimeError("redis flush failed")

        controller = ControllerCore(
            expected_boards={"motor"},
            observability=FailingObservability(),
        )
        writer = self.register_motor(controller)
        task = asyncio.create_task(controller.route_command(client_command(seq=52)))
        await self.wait_for_messages(writer, 1)

        await controller.shutdown(drain_timeout_s=0.0, close_timeout_s=0.05)
        response = await asyncio.wait_for(task, timeout=0.2)

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)
        self.assertEqual(controller.metrics_snapshot()["controller_shutdown_failures"], 1)
        self.assertEqual(controller.pending_count(), 0)
        self.assertTrue(writer.closed)

    async def test_estop_blocks_schema_blocked_commands_but_allows_unblocked(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        controller.state.system.latch_estop()

        blocked = await controller.route_command(client_command(seq=1, command="move"))
        absent_defaults_blocked = await controller.route_command(
            client_command(seq=2, command="legacy_motion")
        )
        allowed = asyncio.create_task(controller.route_command(client_command(seq=3, command="status")))
        await self.wait_for_messages(writer, 1)
        await controller.handle_board_response(ok_response(writer.messages[0]["seq"]))

        self.assertEqual(blocked["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
        self.assertEqual(absent_defaults_blocked["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
        self.assertEqual((await allowed)["status"], "ok")
        self.assertTrue(controller.state.system.estop_active)
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)

    async def test_estop_bypasses_fifo_but_uses_writer_lock(self):
        controller = self.make_controller()
        writer = FakeBoardWriter()
        writer.use_gate = True
        self.register_motor(controller, writer)

        command_task = asyncio.create_task(controller.route_command(client_command(seq=1)))
        await writer.started.wait()
        await asyncio.sleep(0)

        estop_task = asyncio.create_task(controller.send_estop_to_board("motor"))
        await asyncio.sleep(0)
        self.assertEqual(len(writer.messages), 1)
        self.assertEqual(writer.messages[0]["type"], MessageType.COMMAND.value)

        writer.allow_finish.set()
        await asyncio.wait_for(estop_task, timeout=0.2)
        self.assertEqual(len(writer.messages), 2)
        self.assertEqual(writer.messages[1]["type"], MessageType.ESTOP.value)
        self.assertEqual(controller.fifo_depth_for("motor"), 0)
        self.assertEqual(controller.in_flight_for("motor"), writer.messages[0]["seq"])

        await controller.handle_board_response(ok_response(writer.messages[0]["seq"]))
        self.assertEqual((await command_task)["status"], "ok")

    async def test_trigger_estop_clears_fifo_leaves_in_flight_and_sends_estop(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        first = asyncio.create_task(controller.route_command(client_command(seq=1)))
        await self.wait_for_messages(writer, 1)
        queued = asyncio.create_task(controller.route_command(client_command(seq=2)))
        await asyncio.sleep(0)

        await controller.trigger_estop(origin_board="motor")
        queued_response = await asyncio.wait_for(queued, timeout=0.2)

        self.assertEqual(queued_response["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
        self.assertTrue(controller.state.system.estop_active)
        self.assertEqual(writer.messages[1]["type"], MessageType.ESTOP.value)
        self.assertIsNotNone(controller.in_flight_for("motor"))

        await controller.handle_board_response(ok_response(writer.messages[0]["seq"]))
        self.assertEqual((await first)["status"], "ok")

    async def test_repeated_estop_trigger_is_noop(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)

        await controller.trigger_estop(origin_board="motor")
        await controller.trigger_estop(origin_board="motor")

        self.assertTrue(controller.state.system.estop_active)
        self.assertEqual(len(writer.messages), 1)
        self.assertEqual(writer.messages[0]["type"], MessageType.ESTOP.value)

    async def test_connection_state_and_estop_state_remain_separate(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        controller.state.system.latch_estop()
        controller.set_board_state("motor", BoardConnState.DISCONNECTED)

        self.assertTrue(controller.state.system.estop_active)
        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.DISCONNECTED)
        self.assertFalse(controller.state.boards["motor"].estop_ack)
        self.assertEqual(writer.messages, [])

    async def test_get_schemas_returns_all_configured_boards_sorted_with_never_registered(self):
        controller = self.make_controller(boards={"motor", "aux"})
        self.register_motor(controller)

        response = await controller.route_command(schema_query(seq=90))

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["seq"], 90)
        boards = response["result"]["boards"]
        self.assertEqual([record["board_id"] for record in boards], ["aux", "motor"])
        self.assertEqual(
            boards[0],
            {
                "board_id": "aux",
                "known": False,
                "available": False,
                "conn_state": BoardConnState.DISCONNECTED.value,
                "schema_revision": 0,
                "protocol_version": None,
                "firmware_version": None,
                "schema": None,
            },
        )
        self.assertTrue(boards[1]["known"])
        self.assertTrue(boards[1]["available"])
        self.assertEqual(boards[1]["schema_revision"], 1)
        self.assertEqual(boards[1]["protocol_version"], "1")
        self.assertTrue(boards[1]["schema"]["commands"]["legacy_motion"]["blocked_by_estop"])
        self.assertEqual(controller.pending_count(), 0)
        self.assertEqual(controller.in_flight_for("motor"), None)

    async def test_get_schemas_filter_returns_one_record_and_cached_schema_after_disconnect(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        await controller.board_down("motor")

        response = await controller.route_command(schema_query(args={"board_id": "motor"}))

        self.assertEqual(response["status"], "ok")
        boards = response["result"]["boards"]
        self.assertEqual(len(boards), 1)
        self.assertEqual(boards[0]["board_id"], "motor")
        self.assertTrue(boards[0]["known"])
        self.assertFalse(boards[0]["available"])
        self.assertEqual(boards[0]["conn_state"], BoardConnState.FAULTED.value)
        self.assertEqual(boards[0]["schema_revision"], 1)
        self.assertIsNotNone(boards[0]["schema"])
        self.assertEqual(writer.messages, [])

    async def test_get_schemas_validation_and_unknown_local_command(self):
        controller = self.make_controller()

        extra = await controller.route_command(schema_query(args={"extra": True}))
        non_string = await controller.route_command(schema_query(args={"board_id": 7}))
        unknown_board = await controller.route_command(schema_query(args={"board_id": "missing"}))
        unknown_command = await controller.route_command(
            client_command(seq=91, target="controller", command="missing", args={})
        )

        self.assertEqual(extra["error"]["code"], ErrorCode.INVALID_ARGUMENT.value)
        self.assertEqual(non_string["error"]["code"], ErrorCode.INVALID_ARGUMENT.value)
        self.assertEqual(unknown_board["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
        self.assertEqual(unknown_command["error"]["code"], ErrorCode.UNKNOWN_COMMAND.value)

    async def test_get_schemas_succeeds_during_estop_and_does_not_touch_board_writer_or_seq(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        controller.state.system.latch_estop()

        response = await controller.route_command(schema_query(seq=92))

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["seq"], 92)
        self.assertEqual(writer.messages, [])
        self.assertEqual(controller.pending_count(), 0)
        self.assertEqual(controller.fifo_depth_for("motor"), 0)
        self.assertEqual(controller.in_flight_for("motor"), None)
        self.assertEqual(controller._boards["motor"].seq_counter.next_value, 1)

    async def test_effective_schema_copy_materializes_estop_gate_without_mutating_stored_schema(self):
        controller = self.make_controller()
        raw_schema = schema_for("motor")
        stored_before = copy.deepcopy(raw_schema)
        controller.register_board("motor", writer=FakeBoardWriter(), schema=raw_schema)

        response = await controller.route_command(schema_query())
        returned_schema = response["result"]["boards"][0]["schema"]
        returned_schema["commands"]["legacy_motion"]["blocked_by_estop"] = False

        self.assertEqual(raw_schema, stored_before)
        self.assertNotIn(
            "blocked_by_estop",
            controller.state.boards["motor"].schema["schema"]["commands"]["legacy_motion"],
        )
        second = await controller.route_command(schema_query())
        self.assertTrue(
            second["result"]["boards"][0]["schema"]["commands"]["legacy_motion"]["blocked_by_estop"]
        )

    async def test_schema_revision_changes_only_for_effective_record_changes_and_emits_event(self):
        controller = self.make_controller()
        events = []

        async def collect(event):
            events.append(event)

        controller.set_local_event_sink(collect)
        writer = self.register_motor(controller)
        await async_wait_for(lambda: len(events) == 1)

        controller.register_board("motor", writer=writer, schema=schema_for("motor"))
        equivalent = schema_for("motor")
        equivalent["schema"]["commands"]["legacy_motion"]["blocked_by_estop"] = True
        controller.register_board("motor", writer=writer, schema=equivalent)
        changed = schema_for("motor")
        changed["schema"]["commands"]["new_status"] = {"args": {}, "blocked_by_estop": False}
        controller.register_board("motor", writer=writer, schema=changed)
        await async_wait_for(lambda: len(events) == 2)

        self.assertEqual(controller.state.boards["motor"].schema_revision, 2)
        self.assertEqual([event["event"] for event in events], ["schema_updated", "schema_updated"])
        self.assertEqual(events[0]["details"], {"board_id": "motor", "schema_revision": 1})
        self.assertEqual(events[1]["details"], {"board_id": "motor", "schema_revision": 2})
        for event in events:
            validate_message(event)

    async def test_firmware_version_change_increments_revision(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        changed = schema_for("motor")
        changed["schema"]["firmware_version"] = "0.2.0"

        controller.register_board("motor", writer=writer, schema=changed)
        response = await controller.route_command(schema_query())

        self.assertEqual(controller.state.boards["motor"].schema_revision, 2)
        self.assertEqual(response["result"]["boards"][0]["firmware_version"], "0.2.0")

    async def test_protocol_version_mismatch_leaves_schema_revision_untouched(self):
        controller = self.make_controller()
        writer = self.register_motor(controller)
        bad_schema = schema_for("motor")
        bad_schema["protocol_version"] = "2"

        with self.assertRaises(ProtocolValidationError):
            controller.register_board("motor", writer=writer, schema=bad_schema)

        response = await controller.route_command(schema_query())
        self.assertEqual(response["result"]["boards"][0]["schema_revision"], 1)
        self.assertEqual(response["result"]["boards"][0]["protocol_version"], "1")

    async def test_schema_query_size_limit_fails_all_board_response_but_allows_filtered(self):
        controller = self.make_controller(boards={"aux", "motor"})
        controller.register_board("motor", writer=FakeBoardWriter(), schema=schema_for("motor"))
        controller.register_board("aux", writer=FakeBoardWriter(), schema=schema_for("aux"))

        filtered = controller.route_controller_local_command(
            schema_query(args={"board_id": "motor"}),
            max_response_line_bytes=600,
        )
        aggregate = controller.route_controller_local_command(
            schema_query(),
            max_response_line_bytes=600,
        )
        filtered_too_large = controller.route_controller_local_command(
            schema_query(args={"board_id": "motor"}),
            max_response_line_bytes=200,
        )

        self.assertEqual(filtered["status"], "ok")
        self.assertEqual(aggregate["status"], "error")
        self.assertEqual(aggregate["error"]["code"], ErrorCode.INVALID_ARGUMENT.value)
        self.assertIn("retry with args.board_id", aggregate["error"]["message"])
        self.assertEqual(filtered_too_large["status"], "error")
        self.assertEqual(filtered_too_large["error"]["code"], ErrorCode.INVALID_ARGUMENT.value)
        self.assertIn("Individual board schema", filtered_too_large["error"]["message"])


class SerializedWriterSmokeTest(unittest.IsolatedAsyncioTestCase):
    async def test_fake_transport_messages_remain_newline_json_compatible(self):
        raw_writes = []

        async def write_bytes(data):
            raw_writes.append(data)

        from interfaces import SerializedBoardWriter, send_estop

        writer = SerializedBoardWriter(
            board_id="motor",
            write_bytes=write_bytes,
            max_line_bytes=BOARD_MAX_LINE_BYTES,
        )
        await send_estop("motor", writer)

        parsed = parse_message(raw_writes[0], max_line_bytes=BOARD_MAX_LINE_BYTES)
        self.assertTrue(parsed.ok)
        self.assertEqual(parsed.message["type"], MessageType.ESTOP.value)


if __name__ == "__main__":
    unittest.main()
