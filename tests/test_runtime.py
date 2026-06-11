from __future__ import annotations

import asyncio
import json
import os
import signal
import tempfile
import textwrap
import unittest
from typing import Any

from board_connection import LivenessConfig
from protocol import ErrorCode, MessageType
from runtime import (
    ControllerRuntime,
    RuntimeBoardConfig,
    RuntimeConfig,
    create_runtime,
    install_signal_handlers,
    load_runtime_config,
)
from tests.conftest import client_command, encode, ok_response, schema_for


class FakeComponentLog:
    def __init__(self) -> None:
        self.entries: list[str] = []


class FakeLocalServer:
    def __init__(self, log: FakeComponentLog) -> None:
        self.log = log
        self.started = 0
        self.stopped = 0

    async def start(self) -> None:
        self.started += 1
        self.log.entries.append("local.start")

    async def stop(self) -> None:
        self.stopped += 1
        self.log.entries.append("local.stop")


class FakeBoardConnection:
    def __init__(self, name: str, log: FakeComponentLog) -> None:
        self.name = name
        self.log = log
        self.started = 0
        self.stopped = 0

    def start(self) -> None:
        self.started += 1
        self.log.entries.append(f"{self.name}.start")

    async def stop(self) -> None:
        self.stopped += 1
        self.log.entries.append(f"{self.name}.stop")


class DelayedRegistrationBoardConnection(FakeBoardConnection):
    def __init__(self, name: str, log: FakeComponentLog) -> None:
        super().__init__(name, log)
        self.started_event = asyncio.Event()
        self.registration_released = asyncio.Event()

    def start(self) -> None:
        super().start()
        self.started_event.set()

    async def wait_registered(self, timeout: float = 1.0) -> None:
        self.log.entries.append(f"{self.name}.wait_registered")
        await asyncio.wait_for(self.registration_released.wait(), timeout=timeout)


class NeverRegisteredBoardConnection(FakeBoardConnection):
    async def wait_registered(self, timeout: float = 1.0) -> None:
        self.log.entries.append(f"{self.name}.wait_registered")
        await asyncio.wait_for(asyncio.Event().wait(), timeout=timeout)


class FakeController:
    def __init__(self, log: FakeComponentLog) -> None:
        self.log = log
        self.shutdown_calls: list[tuple[float, float]] = []

    async def shutdown(self, *, drain_timeout_s: float, close_timeout_s: float) -> None:
        self.shutdown_calls.append((drain_timeout_s, close_timeout_s))
        self.log.entries.append("controller.shutdown")


class FakeObservabilityWorker:
    def __init__(self, log: FakeComponentLog) -> None:
        self.log = log
        self.started = 0
        self.stop_timeouts: list[float] = []

    def start(self) -> None:
        self.started += 1
        self.log.entries.append("observability.start")

    async def stop(self, *, drain_timeout_s: float = 0.2) -> None:
        self.stop_timeouts.append(drain_timeout_s)
        self.log.entries.append("observability.stop")


class FakeSignalLoop:
    def __init__(self) -> None:
        self.handlers: dict[signal.Signals, Any] = {}
        self.tasks: list[asyncio.Task[None]] = []

    def add_signal_handler(self, sig, callback) -> None:
        self.handlers[sig] = callback

    def create_task(self, awaitable):
        task = asyncio.create_task(awaitable)
        self.tasks.append(task)
        return task


class FakeLoopBoardServer:
    def __init__(self, *, board_id: str = "motor") -> None:
        self.board_id = board_id
        self.server: asyncio.AbstractServer | None = None
        self.port: int | None = None
        self.commands: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.writers: list[Any] = []

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        assert self.server.sockets is not None
        self.port = self.server.sockets[0].getsockname()[1]

    async def close(self) -> None:
        for writer in list(self.writers):
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, reader, writer) -> None:
        self.writers.append(writer)
        writer.write(encode(schema_for(self.board_id)))
        await writer.drain()
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    return
                message = json.loads(line.decode("utf-8"))
                if message.get("type") == MessageType.COMMAND.value:
                    await self.commands.put(message)
                    writer.write(
                        encode(
                            ok_response(
                                message["seq"],
                                self.board_id,
                                controller_ts=message.get("controller_ts"),
                            )
                        )
                    )
                    await writer.drain()
        except ConnectionError:
            return


async def read_json_line(reader, *, timeout: float = 0.8):
    line = await asyncio.wait_for(reader.readline(), timeout=timeout)
    if not line:
        raise AssertionError("expected response line")
    return json.loads(line.decode("utf-8"))


class RuntimeConfigTests(unittest.TestCase):
    def test_load_runtime_config_parses_toml_and_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "controller.toml")
            with open(path, "w", encoding="utf-8") as config_file:
                config_file.write(
                    textwrap.dedent(
                        """
                        [controller]
                        socket_path = "/tmp/hyperloop.sock"
                        fifo_depth = 4
                        default_execution_timeout_s = 1.5
                        default_queue_residency_cap_s = 7.0
                        local_outbound_queue_size = 32
                        registration_timeout_s = 0.25
                        shutdown_drain_timeout_s = 0.1
                        shutdown_close_timeout_s = 0.05

                        [[boards]]
                        id = "motor"
                        host = "127.0.0.1"
                        port = 9001

                        [reconnect]
                        base_delay_s = 0.01
                        factor = 2.5
                        cap_delay_s = 0.5

                        [heartbeat]
                        enabled = true
                        interval_s = 2.0
                        ack_timeout_s = 0.25
                        suspect_after_misses = 2

                        [liveness]
                        enabled = false
                        timeout_s = 0.75

                        [observability]
                        enabled = false
                        queue_size = 10
                        stream_maxlen = 20
                        """
                    )
                )

            config = load_runtime_config(path)

        self.assertEqual(config.socket_path, "/tmp/hyperloop.sock")
        self.assertEqual(config.boards, (RuntimeBoardConfig("motor", "127.0.0.1", 9001),))
        self.assertEqual(config.fifo_depth, 4)
        self.assertEqual(config.default_execution_timeout_s, 1.5)
        self.assertEqual(config.default_queue_residency_cap_s, 7.0)
        self.assertEqual(config.local_outbound_queue_size, 32)
        self.assertEqual(config.registration_timeout_s, 0.25)
        self.assertEqual(config.shutdown_drain_timeout_s, 0.1)
        self.assertEqual(config.shutdown_close_timeout_s, 0.05)
        self.assertEqual(config.reconnect_base_delay_s, 0.01)
        self.assertEqual(config.reconnect_factor, 2.5)
        self.assertEqual(config.reconnect_cap_delay_s, 0.5)
        self.assertTrue(config.heartbeat.enabled)
        self.assertEqual(config.heartbeat.suspect_after_misses, 2)
        self.assertFalse(config.liveness.enabled)
        self.assertFalse(config.observability_enabled)

    def test_config_rejects_missing_boards_and_duplicate_board_ids(self):
        with self.assertRaisesRegex(ValueError, "boards"):
            RuntimeConfig.from_mapping({"controller": {"socket_path": "/tmp/x"}})
        with self.assertRaisesRegex(ValueError, "unique"):
            RuntimeConfig(
                socket_path="/tmp/x",
                boards=(
                    RuntimeBoardConfig("motor", "127.0.0.1", 1),
                    RuntimeBoardConfig("motor", "127.0.0.1", 2),
                ),
            )

    def test_config_rejects_invalid_board_port_and_nonpositive_bounds(self):
        with self.assertRaisesRegex(ValueError, "port"):
            RuntimeBoardConfig("motor", "127.0.0.1", 0)
        with self.assertRaisesRegex(ValueError, "fifo_depth"):
            RuntimeConfig(
                socket_path="/tmp/x",
                boards=(RuntimeBoardConfig("motor", "127.0.0.1", 1),),
                fifo_depth=0,
            )

    def test_create_runtime_wires_static_board_ids_and_observability(self):
        config = RuntimeConfig(
            socket_path="/tmp/hyperloop.sock",
            boards=(
                RuntimeBoardConfig("motor", "127.0.0.1", 1001),
                RuntimeBoardConfig("brake", "127.0.0.1", 1002),
            ),
            observability_enabled=True,
            observability_queue_size=3,
        )

        runtime = create_runtime(config)

        self.assertEqual(set(runtime.controller.state.boards), {"motor", "brake"})
        self.assertEqual(len(runtime.board_connections), 2)
        self.assertIsNotNone(runtime.observability_worker)
        self.assertEqual(runtime.controller.observability.queue.maxsize, 3)


class RuntimeLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def make_runtime(self):
        log = FakeComponentLog()
        config = RuntimeConfig(
            socket_path="/tmp/hyperloop.sock",
            boards=(RuntimeBoardConfig("motor", "127.0.0.1", 1),),
            shutdown_drain_timeout_s=0.11,
            shutdown_close_timeout_s=0.07,
        )
        local = FakeLocalServer(log)
        board = FakeBoardConnection("board", log)
        controller = FakeController(log)
        observability = FakeObservabilityWorker(log)
        runtime = ControllerRuntime(
            config=config,
            controller=controller,
            local_server=local,
            board_connections=(board,),
            observability_worker=observability,
        )
        return runtime, log, local, board, controller, observability

    async def test_start_starts_observability_boards_then_local_socket(self):
        runtime, log, local, board, _controller, observability = self.make_runtime()

        await runtime.start()
        await runtime.start()

        self.assertEqual(
            log.entries,
            ["observability.start", "board.start", "local.start"],
        )
        self.assertEqual(observability.started, 1)
        self.assertEqual(board.started, 1)
        self.assertEqual(local.started, 1)

    async def test_start_waits_for_initial_board_registration_before_local_socket(self):
        log = FakeComponentLog()
        config = RuntimeConfig(
            socket_path="/tmp/hyperloop.sock",
            boards=(RuntimeBoardConfig("motor", "127.0.0.1", 1),),
            registration_timeout_s=0.5,
        )
        local = FakeLocalServer(log)
        board = DelayedRegistrationBoardConnection("board", log)
        runtime = ControllerRuntime(
            config=config,
            controller=FakeController(log),
            local_server=local,
            board_connections=(board,),
        )

        start_task = asyncio.create_task(runtime.start())
        await asyncio.wait_for(board.started_event.wait(), timeout=0.2)
        self.assertEqual(local.started, 0)

        board.registration_released.set()
        await start_task

        self.assertEqual(
            log.entries,
            ["board.start", "board.wait_registered", "local.start"],
        )
        self.assertEqual(local.started, 1)

    async def test_start_opens_local_socket_after_registration_window_expires(self):
        log = FakeComponentLog()
        config = RuntimeConfig(
            socket_path="/tmp/hyperloop.sock",
            boards=(RuntimeBoardConfig("motor", "127.0.0.1", 1),),
            registration_timeout_s=0.01,
        )
        local = FakeLocalServer(log)
        board = NeverRegisteredBoardConnection("board", log)
        runtime = ControllerRuntime(
            config=config,
            controller=FakeController(log),
            local_server=local,
            board_connections=(board,),
        )

        await runtime.start()

        self.assertEqual(
            log.entries,
            ["board.start", "board.wait_registered", "local.start"],
        )
        self.assertEqual(local.started, 1)

    async def test_shutdown_stops_local_first_then_boards_then_controller_then_observability(self):
        runtime, log, local, board, controller, observability = self.make_runtime()

        await runtime.shutdown()
        await runtime.shutdown()

        self.assertEqual(
            log.entries,
            ["local.stop", "board.stop", "controller.shutdown", "observability.stop"],
        )
        self.assertEqual(local.stopped, 1)
        self.assertEqual(board.stopped, 1)
        self.assertEqual(controller.shutdown_calls, [(0.11, 0.07)])
        self.assertEqual(observability.stop_timeouts, [0.07])
        self.assertTrue(runtime.shutdown_complete.is_set())

    async def test_signal_handler_schedules_async_shutdown(self):
        shutdown_called = asyncio.Event()

        async def shutdown() -> None:
            shutdown_called.set()

        loop = FakeSignalLoop()
        installed = install_signal_handlers(loop, shutdown)

        self.assertTrue(installed)
        self.assertIn(signal.SIGTERM, loop.handlers)
        self.assertIn(signal.SIGINT, loop.handlers)

        loop.handlers[signal.SIGTERM]()
        await asyncio.wait_for(shutdown_called.wait(), timeout=0.5)
        await asyncio.gather(*loop.tasks)


class RuntimeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.board_server = FakeLoopBoardServer()
        try:
            await self.board_server.start()
        except PermissionError as exc:
            self.tmp.cleanup()
            raise unittest.SkipTest(f"TCP bind unavailable in this environment: {exc}") from exc
        self.runtime = None

    async def asyncTearDown(self):
        if self.runtime is not None:
            await self.runtime.shutdown()
        await self.board_server.close()
        self.tmp.cleanup()

    async def test_runtime_factory_starts_local_socket_and_board_transport(self):
        socket_path = os.path.join(self.tmp.name, "controller.sock")
        config = RuntimeConfig(
            socket_path=socket_path,
            boards=(RuntimeBoardConfig("motor", "127.0.0.1", self.board_port()),),
            registration_timeout_s=0.5,
            reconnect_base_delay_s=0.01,
            reconnect_cap_delay_s=0.01,
            liveness=LivenessConfig(enabled=False),
            observability_enabled=False,
        )
        self.runtime = create_runtime(config)
        await self.runtime.start()

        reader, writer = await asyncio.open_unix_connection(socket_path)
        try:
            writer.write(encode(client_command(seq=77)))
            await writer.drain()
            board_command = await asyncio.wait_for(self.board_server.commands.get(), timeout=0.8)
            response = await read_json_line(reader)
        finally:
            writer.close()
            await writer.wait_closed()

        self.assertEqual(board_command["type"], MessageType.COMMAND.value)
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["seq"], 77)
        self.assertNotEqual(response["seq"], response["result"]["board_seq"])

    async def test_runtime_shutdown_causes_later_local_connect_to_fail(self):
        socket_path = os.path.join(self.tmp.name, "controller.sock")
        config = RuntimeConfig(
            socket_path=socket_path,
            boards=(RuntimeBoardConfig("motor", "127.0.0.1", self.board_port()),),
            registration_timeout_s=0.5,
            reconnect_base_delay_s=0.01,
            reconnect_cap_delay_s=0.01,
            liveness=LivenessConfig(enabled=False),
            observability_enabled=False,
        )
        self.runtime = create_runtime(config)
        await self.runtime.start()
        await self.runtime.shutdown()

        with self.assertRaises((FileNotFoundError, ConnectionRefusedError, OSError)):
            await asyncio.open_unix_connection(socket_path)

    async def test_controller_rejects_new_commands_with_shutdown_code_after_runtime_shutdown(self):
        socket_path = os.path.join(self.tmp.name, "controller.sock")
        config = RuntimeConfig(
            socket_path=socket_path,
            boards=(RuntimeBoardConfig("motor", "127.0.0.1", self.board_port()),),
            registration_timeout_s=0.5,
            liveness=LivenessConfig(enabled=False),
            observability_enabled=False,
        )
        self.runtime = create_runtime(config)
        await self.runtime.start()
        await self.runtime.shutdown()

        response = await self.runtime.controller.route_command(client_command(seq=88))

        self.assertEqual(response["status"], "error")
        self.assertEqual(response["error"]["code"], ErrorCode.CONTROLLER_SHUTDOWN.value)

    def board_port(self) -> int:
        if self.board_server.port is None:
            raise AssertionError("board server did not bind a port")
        return self.board_server.port


if __name__ == "__main__":
    unittest.main()
