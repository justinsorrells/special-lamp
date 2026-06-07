"""Shared test helpers for the Hyperloop controller test suite.

These are intentionally small, dependency-free helpers extracted to remove
duplication and to give async tests a deterministic toolkit (see the
`testing-async-loops-and-mocks` skill). They are additive: existing tests may
adopt them incrementally. `tests/test_board_connection.py` is the reference
consumer.

Run the suite from the repo root (`python -m pytest`); these import as
`from tests.conftest import ...`.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any


def encode(message: dict[str, Any]) -> bytes:
    """Encode a message as one newline-terminated, compact-JSON line."""

    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


def ok_response(board_seq: int, board_id: str = "motor") -> dict[str, Any]:
    """Build a minimal terminal `ok` board response for the given board_seq."""

    return {
        "type": "response",
        "seq": board_seq,
        "source": board_id,
        "target": "controller",
        "status": "ok",
        "result": {"accepted": True},
        "error": None,
    }


async def async_wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float = 1.0,
    interval: float = 0.005,
) -> None:
    """Poll `predicate` until true or raise AssertionError at `timeout`.

    Prefer this over a bare `asyncio.sleep(...)`: it synchronizes on the actual
    condition instead of a guessed duration, which keeps async tests fast and
    non-flaky.
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not met before timeout")


def make_stream_reader(*chunks: bytes) -> asyncio.StreamReader:
    """An `asyncio.StreamReader` pre-fed with `chunks`, then EOF.

    Feeding real bytes exercises the production newline-framing path rather than
    mocking it. Pass partial chunks to test split frames.
    """

    reader = asyncio.StreamReader()
    for chunk in chunks:
        reader.feed_data(chunk)
    reader.feed_eof()
    return reader


class FakeStreamWriter:
    """Minimal stand-in for `asyncio.StreamWriter`.

    Captures everything written so tests can decode and assert on the messages.
    `drain` is awaitable and controllable: set `drain_blocker` to an
    `asyncio.Event` to simulate a slow/stalled peer for backpressure tests, or set
    `fail_on_write`/`fail_on_drain` to simulate a dropped connection.
    """

    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.closed = False
        self.drain_blocker: asyncio.Event | None = None
        self.fail_on_write = False
        self.fail_on_drain = False

    def write(self, data: bytes) -> None:
        if self.fail_on_write:
            raise ConnectionResetError("fake writer write failed")
        self.writes.append(data)

    async def drain(self) -> None:
        if self.fail_on_drain:
            raise ConnectionResetError("fake writer drain failed")
        if self.drain_blocker is not None:
            await self.drain_blocker.wait()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed

    def messages(self) -> list[dict[str, Any]]:
        """Decode captured writes back into message dicts (one per line)."""

        decoded: list[dict[str, Any]] = []
        for chunk in self.writes:
            for line in chunk.splitlines():
                if line:
                    decoded.append(json.loads(line))
        return decoded
