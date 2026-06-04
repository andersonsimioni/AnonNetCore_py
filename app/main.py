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
        listen_port=engine.services.config.physical_tcp_listen_port,
        physical_node_id=local_node.id if local_node else "unknown",
    )

    try:
        await _wait_for_shutdown_signal()
    finally:
        await engine.stop()


async def _wait_for_shutdown_signal() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handlers: dict[signal.Signals, object] = {}
    shutdown_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        shutdown_signals.append(signal.SIGBREAK)

    def _stop() -> None:
        loop.call_soon_threadsafe(stop_event.set)

    for signal_name in shutdown_signals:
        try:
            loop.add_signal_handler(signal_name, _stop)
        except NotImplementedError:
            previous_handlers[signal_name] = signal.getsignal(signal_name)
            signal.signal(signal_name, lambda *_: _stop())

    try:
        await stop_event.wait()
    finally:
        for signal_name, previous_handler in previous_handlers.items():
            signal.signal(signal_name, previous_handler)


def build_engine_from_args() -> CoreEngine:
    args = parse_args()
    engine = CoreEngine()
    engine.services.config.physical_tcp_listen_port = args.listen_port
    return engine


def parse_args() -> argparse.Namespace:
    default_config = CoreConfig()
    parser = argparse.ArgumentParser(description="Starts an AnonNetCore physical node.")
    parser.add_argument(
        "--listen-port",
        type=int,
        default=default_config.physical_tcp_listen_port,
        help="Porta TCP local do node.",
    )
    return parser.parse_args()


def main() -> None:
    try:
        asyncio.run(run_node(build_engine_from_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
