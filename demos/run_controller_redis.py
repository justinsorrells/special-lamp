"""Run the controller with Redis observability wired in.

`runtime.main()` constructs the controller with `redis=None`, so telemetry/state
are not mirrored anywhere. This launcher uses the public `create_runtime(...,
redis=<client>)` seam to attach an async Redis client, so the controller mirrors
each board's telemetry to the `board:telemetry:<id>` stream (and board/system
state to hashes) for dashboards to read. The command path is unchanged — Redis
is observability only.

Run from the special-lamp repo root:

    PYTHONPATH=. python demos/run_controller_redis.py --config <cfg.toml> \
        --redis-url redis://127.0.0.1:6379/0

The config should enable observability:

    [observability]
    enabled = true
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import redis.asyncio as aioredis

from runtime import create_runtime, load_runtime_config, run_until_shutdown


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the controller with Redis observability")
    parser.add_argument("--config", required=True, help="controller TOML config path")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    config = load_runtime_config(args.config)
    if not config.observability_enabled:
        logging.warning("config has observability disabled; telemetry will NOT reach Redis")
    client = aioredis.from_url(args.redis_url, decode_responses=True)
    asyncio.run(run_until_shutdown(create_runtime(config, redis=client)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
