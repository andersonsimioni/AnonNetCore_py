from __future__ import annotations

import argparse
import asyncio
import signal

from core import CoreConfig, CoreEngine


async def run_node(engine: CoreEngine) -> None:
    await engine.start()
    local_node = engine.services.identity_service.get_local_physical_node_result()
    engine.services.log_service.info(
        "node_runtime",
        "core started",
        listen_port=engine.services.config.listen_port,
        physical_node_id=local_node.id if local_node else "unknown",
    )

    try:
        await _wait_for_shutdown_signal()
    finally:
        await engine.stop()


async def _wait_for_shutdown_signal() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        stop_event.set()

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, _stop)
        except NotImplementedError:
            pass

    await stop_event.wait()


def build_engine_from_args() -> CoreEngine:
    args = parse_args()
    engine = CoreEngine()
    engine.services.config.listen_port = args.listen_port
    return engine


def parse_args() -> argparse.Namespace:
    default_config = CoreConfig()
    parser = argparse.ArgumentParser(description="Sobe um physical node do AnonNetCore.")
    parser.add_argument(
        "--listen-port",
        type=int,
        default=default_config.listen_port,
        help="Porta TCP local do node.",
    )
    return parser.parse_args()


def main() -> None:
    asyncio.run(run_node(build_engine_from_args()))


if __name__ == "__main__":
    main()
