"""Runtime entrypoint and configuration for the v1 asyncio controller."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import tomllib
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from board_connection import (
    BoardEndpoint,
    BoardTCPConnection,
    HeartbeatConfig,
    LivenessConfig,
    ReconnectBackoff,
)
from controller import (
    DEFAULT_SHUTDOWN_CLOSE_TIMEOUT_S,
    DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_S,
    ControllerCore,
)
from local_socket import LocalUnixSocketServer
from observability import DEFAULT_OBS_QUEUE_SIZE, DEFAULT_STREAM_MAXLEN, ObservabilityQueue, RedisTelemetryWorker
from state import (
    DEFAULT_COMMAND_FIFO_DEPTH,
    DEFAULT_COMMAND_TIMEOUT_S,
    DEFAULT_QUEUE_RESIDENCY_CAP_S,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_CONFIG_ENV = "HYPERLOOP_CONTROLLER_CONFIG"


class BoardConnectionLike(Protocol):
    def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def wait_registered(self, timeout: float = 1.0) -> None:
        ...


class LocalServerLike(Protocol):
    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...


class ObservabilityWorkerLike(Protocol):
    def start(self) -> None:
        ...

    async def stop(self, *, drain_timeout_s: float = 0.2) -> None:
        ...


@dataclass(frozen=True)
class RuntimeBoardConfig:
    board_id: str
    host: str
    port: int

    def __post_init__(self) -> None:
        if not self.board_id:
            raise ValueError("board id must be non-empty")
        if not self.host:
            raise ValueError(f"board {self.board_id} host must be non-empty")
        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"board {self.board_id} port must be in 1..65535")


@dataclass(frozen=True)
class RuntimeConfig:
    socket_path: str
    boards: tuple[RuntimeBoardConfig, ...]
    fifo_depth: int = DEFAULT_COMMAND_FIFO_DEPTH
    default_execution_timeout_s: float = DEFAULT_COMMAND_TIMEOUT_S
    default_queue_residency_cap_s: float = DEFAULT_QUEUE_RESIDENCY_CAP_S
    local_outbound_queue_size: int = 1000
    registration_timeout_s: float = 2.0
    shutdown_drain_timeout_s: float = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_S
    shutdown_close_timeout_s: float = DEFAULT_SHUTDOWN_CLOSE_TIMEOUT_S
    reconnect_base_delay_s: float = 0.5
    reconnect_factor: float = 2.0
    reconnect_cap_delay_s: float = 5.0
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    liveness: LivenessConfig = field(default_factory=LivenessConfig)
    observability_enabled: bool = True
    observability_queue_size: int = DEFAULT_OBS_QUEUE_SIZE
    observability_stream_maxlen: int = DEFAULT_STREAM_MAXLEN

    def __post_init__(self) -> None:
        if not self.socket_path:
            raise ValueError("controller.socket_path must be non-empty")
        if not self.boards:
            raise ValueError("at least one configured board is required")
        if len({board.board_id for board in self.boards}) != len(self.boards):
            raise ValueError("board ids must be unique")
        _require_positive("fifo_depth", self.fifo_depth)
        _require_positive("default_execution_timeout_s", self.default_execution_timeout_s)
        _require_positive("default_queue_residency_cap_s", self.default_queue_residency_cap_s)
        _require_positive("local_outbound_queue_size", self.local_outbound_queue_size)
        _require_positive("registration_timeout_s", self.registration_timeout_s)
        _require_positive("shutdown_drain_timeout_s", self.shutdown_drain_timeout_s)
        _require_positive("shutdown_close_timeout_s", self.shutdown_close_timeout_s)
        _require_positive("observability_queue_size", self.observability_queue_size)
        _require_positive("observability_stream_maxlen", self.observability_stream_maxlen)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> RuntimeConfig:
        controller = _mapping(data.get("controller"), "controller")
        boards_data = data.get("boards")
        if not isinstance(boards_data, list):
            raise ValueError("boards must be a TOML array of tables")
        boards = tuple(_board_config(item) for item in boards_data)
        reconnect = _mapping(data.get("reconnect", {}), "reconnect")
        heartbeat = _mapping(data.get("heartbeat", {}), "heartbeat")
        liveness = _mapping(data.get("liveness", {}), "liveness")
        observability = _mapping(data.get("observability", {}), "observability")
        return cls(
            socket_path=_string(controller, "socket_path"),
            boards=boards,
            fifo_depth=_int(controller, "fifo_depth", DEFAULT_COMMAND_FIFO_DEPTH),
            default_execution_timeout_s=_float(
                controller,
                "default_execution_timeout_s",
                DEFAULT_COMMAND_TIMEOUT_S,
            ),
            default_queue_residency_cap_s=_float(
                controller,
                "default_queue_residency_cap_s",
                DEFAULT_QUEUE_RESIDENCY_CAP_S,
            ),
            local_outbound_queue_size=_int(controller, "local_outbound_queue_size", 1000),
            registration_timeout_s=_float(controller, "registration_timeout_s", 2.0),
            shutdown_drain_timeout_s=_float(
                controller,
                "shutdown_drain_timeout_s",
                DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_S,
            ),
            shutdown_close_timeout_s=_float(
                controller,
                "shutdown_close_timeout_s",
                DEFAULT_SHUTDOWN_CLOSE_TIMEOUT_S,
            ),
            reconnect_base_delay_s=_float(reconnect, "base_delay_s", 0.5),
            reconnect_factor=_float(reconnect, "factor", 2.0),
            reconnect_cap_delay_s=_float(reconnect, "cap_delay_s", 5.0),
            heartbeat=HeartbeatConfig(
                enabled=_bool(heartbeat, "enabled", False),
                interval_s=_float(heartbeat, "interval_s", 5.0),
                ack_timeout_s=_float(heartbeat, "ack_timeout_s", 1.0),
                suspect_after_misses=_int(heartbeat, "suspect_after_misses", 3),
            ),
            liveness=LivenessConfig(
                enabled=_bool(liveness, "enabled", True),
                timeout_s=_float(liveness, "timeout_s", 0.25),
            ),
            observability_enabled=_bool(observability, "enabled", True),
            observability_queue_size=_int(observability, "queue_size", DEFAULT_OBS_QUEUE_SIZE),
            observability_stream_maxlen=_int(
                observability,
                "stream_maxlen",
                DEFAULT_STREAM_MAXLEN,
            ),
        )


class ControllerRuntime:
    """Owns process-level startup, signal-driven shutdown, and component lifetime."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        controller: Any,
        local_server: LocalServerLike,
        board_connections: tuple[BoardConnectionLike, ...],
        observability_worker: ObservabilityWorkerLike | None = None,
    ):
        self.config = config
        self.controller = controller
        self.local_server = local_server
        self.board_connections = board_connections
        self.observability_worker = observability_worker
        self.shutdown_complete = asyncio.Event()
        self._shutdown_task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        if self.observability_worker is not None:
            self.observability_worker.start()
        for connection in self.board_connections:
            connection.start()
        await self._wait_for_initial_board_registration()
        await self.local_server.start()
        self._started = True

    async def shutdown(self) -> None:
        current_task = asyncio.current_task()
        if self._shutdown_task is not None:
            if self._shutdown_task is not current_task:
                await self._shutdown_task
            return
        self._shutdown_task = asyncio.create_task(self._run_shutdown())
        await self._shutdown_task

    async def wait_closed(self) -> None:
        await self.shutdown_complete.wait()

    async def _run_shutdown(self) -> None:
        try:
            await self._stop_local_server()
            await self._stop_board_connections()
            await self.controller.shutdown(
                drain_timeout_s=self.config.shutdown_drain_timeout_s,
                close_timeout_s=self.config.shutdown_close_timeout_s,
            )
            await self._stop_observability_worker()
        finally:
            self.shutdown_complete.set()

    async def _wait_for_initial_board_registration(self) -> None:
        waiters = []
        for connection in self.board_connections:
            wait_registered = getattr(connection, "wait_registered", None)
            if wait_registered is not None:
                waiters.append(wait_registered(timeout=self.config.registration_timeout_s))
        if not waiters:
            return
        await asyncio.gather(*waiters, return_exceptions=True)

    async def _stop_local_server(self) -> None:
        try:
            await self.local_server.stop()
        except Exception:
            LOGGER.exception("local socket server shutdown failed")

    async def _stop_board_connections(self) -> None:
        results = await asyncio.gather(
            *(connection.stop() for connection in self.board_connections),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                LOGGER.error(
                    "board connection shutdown failed",
                    exc_info=(type(result), result, result.__traceback__),
                )

    async def _stop_observability_worker(self) -> None:
        if self.observability_worker is None:
            return
        try:
            await self.observability_worker.stop(
                drain_timeout_s=self.config.shutdown_close_timeout_s,
            )
        except Exception:
            LOGGER.exception("observability worker shutdown failed")


def load_runtime_config(path: str | os.PathLike[str]) -> RuntimeConfig:
    with Path(path).open("rb") as config_file:
        data = tomllib.load(config_file)
    return RuntimeConfig.from_mapping(data)


def create_runtime(config: RuntimeConfig, *, redis: Any | None = None) -> ControllerRuntime:
    obs_queue = (
        ObservabilityQueue(maxsize=config.observability_queue_size)
        if config.observability_enabled
        else None
    )
    obs_worker = (
        RedisTelemetryWorker(
            redis=redis,
            obs_queue=obs_queue,
            stream_maxlen=config.observability_stream_maxlen,
        )
        if obs_queue is not None
        else None
    )
    controller = ControllerCore(
        expected_boards={board.board_id for board in config.boards},
        fifo_depth=config.fifo_depth,
        default_execution_timeout_s=config.default_execution_timeout_s,
        default_queue_residency_cap_s=config.default_queue_residency_cap_s,
        observability=obs_queue,
    )
    board_connections = tuple(
        BoardTCPConnection(
            BoardEndpoint(board.board_id, board.host, board.port),
            controller,
            reconnect_backoff=ReconnectBackoff(
                base_delay_s=config.reconnect_base_delay_s,
                factor=config.reconnect_factor,
                cap_delay_s=config.reconnect_cap_delay_s,
            ),
            registration_timeout_s=config.registration_timeout_s,
            heartbeat=config.heartbeat,
            liveness=config.liveness,
        )
        for board in config.boards
    )
    local_server = LocalUnixSocketServer(
        socket_path=config.socket_path,
        controller=controller,
        outbound_queue_size=config.local_outbound_queue_size,
    )
    return ControllerRuntime(
        config=config,
        controller=controller,
        local_server=local_server,
        board_connections=board_connections,
        observability_worker=obs_worker,
    )


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    shutdown: Callable[[], Coroutine[Any, Any, None]],
) -> bool:
    installed = True

    def schedule_shutdown() -> None:
        loop.create_task(shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, schedule_shutdown)
        except (NotImplementedError, RuntimeError):
            installed = False
    return installed


async def run_until_shutdown(runtime: ControllerRuntime) -> None:
    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, runtime.shutdown)
    await runtime.start()
    try:
        await runtime.wait_closed()
    finally:
        await runtime.shutdown()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Hyperloop v1 asyncio controller")
    parser.add_argument(
        "--config",
        default=os.environ.get(DEFAULT_CONFIG_ENV),
        help=f"path to controller TOML config, or ${DEFAULT_CONFIG_ENV}",
    )
    args = parser.parse_args(argv)
    if args.config is None:
        parser.error(f"--config is required unless {DEFAULT_CONFIG_ENV} is set")
    logging.basicConfig(level=logging.INFO)
    config = load_runtime_config(args.config)
    asyncio.run(run_until_shutdown(create_runtime(config)))
    return 0


def _board_config(data: Any) -> RuntimeBoardConfig:
    mapping = _mapping(data, "boards[]")
    return RuntimeBoardConfig(
        board_id=_string(mapping, "id"),
        host=_string(mapping, "host"),
        port=_int(mapping, "port"),
    )


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a TOML table")
    return value


def _string(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def _int(mapping: dict[str, Any], key: str, default: int | None = None) -> int:
    value = mapping.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{key} must be an integer")
    return value


def _float(mapping: dict[str, Any], key: str, default: float) -> float:
    value = mapping.get(key, default)
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"{key} must be a number")
    return float(value)


def _bool(mapping: dict[str, Any], key: str, default: bool) -> bool:
    value = mapping.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value


def _require_positive(name: str, value: int | float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


if __name__ == "__main__":
    raise SystemExit(main())
