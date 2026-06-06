import asyncio
import unittest

from controller import ControllerCore
from observability import (
    ObservabilityQueue,
    RedisTelemetryWorker,
    serialize_board_state_snapshot,
)
from protocol import ErrorCode
from state import BoardConnState
from tests.test_controller_core import FakeBoardWriter, board_ok_response, client_command, schema_for


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

        lifecycle = [item for item in list(obs.queue._queue) if item["kind"] == "command_lifecycle"][-1]
        fields = lifecycle["fields"]
        self.assertEqual(fields["command_id"], f"motor:{board_seq}")
        self.assertEqual(fields["seq"], 103)
        self.assertEqual(fields["board_id"], "motor")
        self.assertEqual(fields["status"], "ok")
        self.assertIsNone(fields["error_code"])

        busy = await controller.route_command(client_command(seq=104, target="missing"))
        lifecycle = [item for item in list(obs.queue._queue) if item["kind"] == "command_lifecycle"][-1]
        self.assertEqual(busy["error"]["code"], ErrorCode.UNKNOWN_TARGET.value)
        self.assertEqual(lifecycle["fields"]["status"], "error")
        self.assertEqual(lifecycle["fields"]["error_code"], ErrorCode.UNKNOWN_TARGET.value)

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
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.005)
        self.fail("condition was not met before timeout")


if __name__ == "__main__":
    unittest.main()
