"""Shared interfaces between future v1 controller components.

This file defines handoff points required by the frozen build order. It does
not open sockets, talk to Redis, implement firmware, or integrate a GUI.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from protocol import build_estop_message, serialize_message


class ClientReplyHandle(Protocol):
    """Client response boundary used to centralize orphaned-response handling."""

    @property
    def is_connected(self) -> bool:
        ...

    async def send_response(self, response: dict[str, Any]) -> None:
        ...

    async def send_event(self, event: dict[str, Any]) -> None:
        ...


class BoardDownHandler(Protocol):
    """Dispatcher-owned drain hook called by the board connection manager."""

    async def board_down(self, board_id: str) -> None:
        ...


class BoardWriterHandle(Protocol):
    """Serialized write boundary shared by normal commands and estop writes."""

    @property
    def lock(self) -> asyncio.Lock:
        ...

    async def write_message(self, message: dict[str, Any]) -> None:
        ...


WriteBytes = Callable[[bytes], Awaitable[None]]


@dataclass
class SerializedBoardWriter:
    """Single serialized write path for one board.

    The supplied write_bytes callback is provided by future socket code. This
    class only owns the per-board lock and newline-JSON serialization contract.
    """

    board_id: str
    write_bytes: WriteBytes
    max_line_bytes: int
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def write_message(self, message: dict[str, Any]) -> None:
        encoded = serialize_message(message, max_line_bytes=self.max_line_bytes)
        async with self.lock:
            await self.write_bytes(encoded)


async def send_estop(
    board_id: str,
    writer: BoardWriterHandle,
    *,
    source: str = "controller",
) -> None:
    """Out-of-band estop path.

    This bypasses command FIFO and in-flight ownership in future dispatcher code,
    but it still goes through the board writer handle so bytes are serialized
    with normal command writes.
    """

    await writer.write_message(build_estop_message(target=board_id, source=source))


async def deliver_client_response(
    client: ClientReplyHandle,
    response: dict[str, Any],
    *,
    orphaned_counter: Callable[[], None] | None = None,
) -> bool:
    """Send a response only if the local client is still connected."""

    if not client.is_connected:
        if orphaned_counter is not None:
            orphaned_counter()
        return False
    await client.send_response(response)
    return True

