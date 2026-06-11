import asyncio
import json
import logging
import unittest

from controller import ControllerCore
from observability import (
    ObservabilityQueue,
    RedisTelemetryWorker,
    serialize_board_state_snapshot,
    serialize_command_lifecycle,
)
from protocol import ErrorCode, MessageType
from state import BoardConnState
from tests.conftest import (
    FakeBoardWriter,
    async_wait_for,
    client_command,
    schema_for,
)
from tests.conftest import (
    ok_response as board_ok_response,
)


class FakeRedis:
    def __init__(self, *, fail=False, delay=0.0):
        self.fail = fail
        self.delay = delay
        self.hsets = []
        self.xadds = []
        self.publishes = []

    async def hset(self, name, mapping):
        await self._maybe_fail()
        self.hsets.append((name, dict(mapping)))

    async def xadd(self, name, fields, *, maxlen=None, approximate=True):
        await self._maybe_fail()
        self.xadds.append((name, dict(fields), maxlen, approximate))

    async def publish(self, channel, message):
        await self._maybe_fail()
        self.publishes.append((channel, message))

    async def _maybe_fail(self):
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise RuntimeError("redis unavailable")


class ObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_requires_positive_bound(self):
        with self.assertRaises(ValueError):
            ObservabilityQueue(maxsize=0)

    async def test_command_path_still_works_with_redis_disabled(self):
        obs = ObservabilityQueue(maxsize=10)
        worker = RedisTelemetryWorker(redis=None, obs_queue=obs)
        worker.start()
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        command_task = asyncio.create_task(controller.route_command(client_command(seq=100)))
        await self.wait_for(lambda: len(writer.messages) == 1)
        await controller.handle_board_response(board_ok_response(writer.messages[0]["seq"]))
        response = await asyncio.wait_for(command_task, timeout=0.5)
        await worker.stop()

        self.assertEqual(response["status"], "ok")
        self.assertEqual(controller.pending_count(), 0)

    async def test_telemetry_events_are_enqueued_without_blocking_command_completion(self):
        obs = ObservabilityQueue(maxsize=20)
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        controller.observe_board_telemetry(
            {
                "type": "telemetry",
                "seq": 7,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 1000},
            }
        )
        command_task = asyncio.create_task(controller.route_command(client_command(seq=101)))
        await self.wait_for(lambda: len(writer.messages) == 1)
        await controller.handle_board_response(board_ok_response(writer.messages[0]["seq"]))
        response = await asyncio.wait_for(command_task, timeout=0.5)

        queued_kinds = [item["kind"] for item in list(obs.queue._queue)]
        self.assertEqual(response["status"], "ok")
        self.assertIn("board_telemetry", queued_kinds)
        self.assertIn("command_lifecycle", queued_kinds)

    async def test_queue_is_bounded_and_uses_drop_oldest(self):
        obs = ObservabilityQueue(maxsize=2)

        obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event_id": 1}})
        obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event_id": 2}})
        obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event_id": 3}})

        self.assertEqual(obs.queue.qsize(), 2)
        self.assertEqual(obs.counters.obs_dropped, 1)
        self.assertEqual([item["fields"]["event_id"] for item in list(obs.queue._queue)], [2, 3])

    async def test_controller_metrics_snapshot_mirrors_observability_counters(self):
        obs = ObservabilityQueue(maxsize=1)
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)

        obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event_id": 1}})
        obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event_id": 2}})
        obs.counters.redis_write_failures = 3

        snapshot = controller.metrics_snapshot()
        self.assertEqual(snapshot["obs_dropped"], 1)
        self.assertEqual(snapshot["redis_write_failures"], 3)

    async def test_redis_write_failure_does_not_fail_command(self):
        obs = ObservabilityQueue(maxsize=20)
        redis = FakeRedis(fail=True)
        worker = RedisTelemetryWorker(redis=redis, obs_queue=obs)
        worker.start()
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        command_task = asyncio.create_task(controller.route_command(client_command(seq=102)))
        await self.wait_for(lambda: len(writer.messages) == 1)
        await controller.handle_board_response(board_ok_response(writer.messages[0]["seq"]))
        response = await asyncio.wait_for(command_task, timeout=0.5)
        await self.wait_for(lambda: obs.counters.redis_write_failures > 0)
        await worker.stop()

        self.assertEqual(response["status"], "ok")
        self.assertGreater(controller.metrics_snapshot()["redis_write_failures"], 0)

    async def test_redis_write_failure_does_not_block_estop_convergence(self):
        obs = ObservabilityQueue(maxsize=40)
        redis = FakeRedis(fail=True)
        worker = RedisTelemetryWorker(redis=redis, obs_queue=obs)
        worker.start()
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        in_flight = asyncio.create_task(controller.route_command(client_command(seq=111)))
        await self.wait_for(lambda: len(writer.messages) == 1)
        queued = asyncio.create_task(controller.route_command(client_command(seq=112)))
        await self.wait_for(lambda: controller.fifo_depth_for("motor") == 1)

        await controller.trigger_estop(origin_board="motor")
        queued_response = await asyncio.wait_for(queued, timeout=0.5)
        await self.wait_for(lambda: obs.counters.redis_write_failures > 0)
        await controller.handle_board_response(board_ok_response(writer.messages[0]["seq"]))
        in_flight_response = await asyncio.wait_for(in_flight, timeout=0.5)
        await worker.stop()

        self.assertTrue(controller.state.system.estop_active)
        self.assertEqual(writer.messages[1]["type"], MessageType.ESTOP.value)
        self.assertEqual(queued_response["status"], "error")
        self.assertEqual(queued_response["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
        self.assertEqual(in_flight_response["status"], "ok")
        self.assertGreater(controller.metrics_snapshot()["redis_write_failures"], 0)

    async def test_worker_handles_redis_write_exceptions_without_crashing(self):
        obs = ObservabilityQueue(maxsize=10)
        redis = FakeRedis(fail=True)
        worker = RedisTelemetryWorker(redis=redis, obs_queue=obs)
        worker.start()

        with self.assertLogs("observability", level=logging.ERROR):
            obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event": "one"}})
            await self.wait_for(lambda: obs.counters.redis_write_failures == 1)
            redis.fail = False
            obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event": "two"}})
            await self.wait_for(lambda: obs.counters.records_written == 1)
        await worker.stop()

        self.assertEqual(len(redis.xadds), 1)
        self.assertEqual(redis.xadds[0][1]["event"], "two")

    async def test_board_state_snapshot_is_read_replica_only(self):
        obs = ObservabilityQueue(maxsize=10)
        redis = FakeRedis()
        worker = RedisTelemetryWorker(redis=redis, obs_queue=obs)
        worker.start()
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))
        await self.wait_for(lambda: any(name == "board:state:motor" for name, _ in redis.hsets))

        redis.hsets[-1][1]["conn_state"] = BoardConnState.FAULTED.value
        await worker.stop()

        self.assertEqual(controller.state.boards["motor"].conn_state, BoardConnState.REGISTERED)
        self.assertEqual(serialize_board_state_snapshot(controller.state.boards["motor"])["key"], "board:state:motor")

    async def test_state_update_publish_payloads_include_monotonic_event_ids(self):
        obs = ObservabilityQueue(maxsize=20)
        redis = FakeRedis()
        worker = RedisTelemetryWorker(redis=redis, obs_queue=obs)
        worker.start()
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()

        controller.set_board_state("motor", BoardConnState.CONNECTING)
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))
        await self.wait_for(lambda: len(redis.publishes) >= 4)
        await worker.stop()

        state_updates = [
            json.loads(message)
            for channel, message in redis.publishes
            if channel == "board:state:updates"
        ]
        event_ids = [message["event_id"] for message in state_updates]
        self.assertEqual(event_ids, sorted(event_ids))
        self.assertEqual(event_ids, list(range(1, len(event_ids) + 1)))
        self.assertTrue({"board_state", "system_state"}.issubset({message["type"] for message in state_updates}))

    async def test_command_lifecycle_event_includes_required_fields(self):
        obs = ObservabilityQueue(maxsize=20)
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        command_task = asyncio.create_task(controller.route_command(client_command(seq=103)))
        await self.wait_for(lambda: len(writer.messages) == 1)
        board_seq = writer.messages[0]["seq"]
        await controller.handle_board_response(board_ok_response(board_seq))
        await asyncio.wait_for(command_task, timeout=0.5)

        lifecycle_items = [item for item in list(obs.queue._queue) if item["kind"] == "command_lifecycle"]
        phases = [item["fields"]["phase"] for item in lifecycle_items]
        self.assertEqual(
            phases[-5:],
            ["received", "routed", "queued", "sent_to_board", "resolved"],
        )
        lifecycle = lifecycle_items[-1]
        fields = lifecycle["fields"]
        self.assertEqual(fields["command_id"], f"motor:{board_seq}")
        self.assertEqual(fields["seq"], 103)
        self.assertEqual(fields["board_id"], "motor")
        self.assertEqual(fields["phase"], "resolved")
        self.assertEqual(fields["status"], "ok")
        self.assertIsNone(fields["error_code"])
        self.assertIsInstance(fields["controller_ts"], float)
        self.assertIsInstance(fields["latency_ms"], float)
        self.assertIsNone(fields["board_proc_us"])

        busy = await controller.route_command(client_command(seq=104, target="missing"))
        lifecycle = [item for item in list(obs.queue._queue) if item["kind"] == "command_lifecycle"][-1]
        self.assertEqual(busy["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
        self.assertEqual(lifecycle["fields"]["phase"], "rejected_unknown_target")
        self.assertEqual(lifecycle["fields"]["status"], "error")
        self.assertEqual(lifecycle["fields"]["error_code"], ErrorCode.UNKNOWN_TARGET.value)

    async def test_command_lifecycle_timeout_and_estop_rejection_phases(self):
        obs = ObservabilityQueue(maxsize=30)
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        timeout_task = asyncio.create_task(
            controller.route_command(client_command(seq=201), execution_timeout_s=0.01)
        )
        await self.wait_for(lambda: len(writer.messages) == 1)
        timeout_response = await asyncio.wait_for(timeout_task, timeout=0.5)
        controller.state.system.latch_estop()
        estop_response = await controller.route_command(client_command(seq=202))

        lifecycle_items = [item for item in list(obs.queue._queue) if item["kind"] == "command_lifecycle"]
        by_seq = {}
        for item in lifecycle_items:
            by_seq.setdefault(item["fields"]["seq"], []).append(item["fields"])

        self.assertEqual(timeout_response["status"], "timeout")
        self.assertEqual(by_seq[201][-1]["phase"], "timeout")
        self.assertEqual(by_seq[201][-1]["error_code"], ErrorCode.COMMAND_TIMEOUT.value)
        self.assertEqual(estop_response["error"]["code"], ErrorCode.ESTOP_ACTIVE.value)
        self.assertEqual(by_seq[202][-1]["phase"], "estop_rejected")
        self.assertEqual(by_seq[202][-1]["error_code"], ErrorCode.ESTOP_ACTIVE.value)

    async def test_lifecycle_serializer_rejects_new_terminal_statuses(self):
        with self.assertRaises(ValueError):
            serialize_command_lifecycle(
                command_id="motor:1",
                seq=1,
                board_id="motor",
                phase="resolved",
                status="rejected",
            )

    async def test_latency_and_telemetry_observations_are_mirrored_as_read_replica_fields(self):
        obs = ObservabilityQueue(maxsize=40)
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)
        writer = FakeBoardWriter()
        controller.register_board("motor", writer=writer, schema=schema_for("motor"))

        command_task = asyncio.create_task(controller.route_command(client_command(seq=301)))
        await self.wait_for(lambda: len(writer.messages) == 1)
        board_seq = writer.messages[0]["seq"]
        await controller.handle_board_response(
            board_ok_response(board_seq, board_proc_us=1200)
        )
        await asyncio.wait_for(command_task, timeout=0.5)
        controller.observe_board_telemetry(
            {
                "type": "telemetry",
                "seq": 1,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 1000},
            }
        )
        controller.observe_board_telemetry(
            {
                "type": "telemetry",
                "seq": 2,
                "source": "motor",
                "target": "controller",
                "telemetry": {"rpm": 1010},
            }
        )

        board_states = [item for item in list(obs.queue._queue) if item["kind"] == "board_state"]
        latest_state = board_states[-1]["hash"]
        self.assertIsInstance(latest_state["last_command_latency_ms"], float)
        self.assertEqual(latest_state["last_board_proc_us"], 1200.0)
        self.assertEqual(latest_state["command_latency_sample_count"], 1)
        self.assertIsInstance(latest_state["command_latency_p50_ms"], float)
        self.assertIsInstance(latest_state["command_latency_p95_ms"], float)
        self.assertIsInstance(latest_state["command_latency_p99_ms"], float)
        self.assertEqual(latest_state["telemetry_sample_count"], 2)
        self.assertIn("telemetry_rate_hz", latest_state)
        self.assertIn("telemetry_jitter_ms", latest_state)

        telemetry_records = [item for item in list(obs.queue._queue) if item["kind"] == "board_telemetry"]
        latest_telemetry = telemetry_records[-1]["fields"]
        self.assertEqual(latest_telemetry["telemetry_sample_count"], 2)
        self.assertIn("telemetry_rate_hz", latest_telemetry)
        self.assertIn("telemetry_jitter_ms", latest_telemetry)

        lifecycle = [item for item in list(obs.queue._queue) if item["kind"] == "command_lifecycle"][-1]
        self.assertIsInstance(lifecycle["fields"]["latency_ms"], float)
        self.assertEqual(lifecycle["fields"]["board_proc_us"], 1200.0)

    async def test_malformed_message_events_can_be_enqueued(self):
        obs = ObservabilityQueue(maxsize=10)
        controller = ControllerCore(expected_boards={"motor"}, observability=obs)

        controller.observe_controller_event(
            {
                "type": MessageType.EVENT.value,
                "source": "controller",
                "event": "malformed_client_message",
                "details": {"error_code": ErrorCode.INVALID_JSON.value},
            }
        )

        event = [item for item in list(obs.queue._queue) if item["kind"] == "controller_event"][-1]
        self.assertEqual(event["fields"]["event"], "malformed_client_message")
        self.assertEqual(event["fields"]["details"], {"error_code": ErrorCode.INVALID_JSON.value})

    async def test_worker_shutdown_is_clean(self):
        obs = ObservabilityQueue(maxsize=5)
        redis = FakeRedis()
        worker = RedisTelemetryWorker(redis=redis, obs_queue=obs)
        obs.enqueue({"kind": "controller_event", "stream": "controller:events", "fields": {"event": "test"}})
        worker.start()
        await worker.stop()

        self.assertTrue(worker._task.done())
        self.assertEqual(obs.queue.qsize(), 0)
        self.assertEqual(len(redis.xadds), 1)

    async def wait_for(self, predicate, *, timeout=0.5):
        try:
            await async_wait_for(predicate, timeout=timeout)
        except AssertionError:
            self.fail("condition was not met before timeout")


if __name__ == "__main__":
    unittest.main()
