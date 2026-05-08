from __future__ import annotations

import argparse
import asyncio
import signal

from bootstrap.models import BootstrapEndpoint
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
    config = engine.services.config
    config.node_index = args.node_index
    config.node_name = _build_node_name(args.node_index)
    config.listen_port = args.listen_port
    config.advertised_tcp_host = args.advertised_host
    config.advertised_tcp_port = args.advertised_port
    config.bootstrap_endpoints = [_parse_host_port(item) for item in args.bootstrap_endpoint]
    engine.services.bootstrap_service.config.public_endpoints = [
        BootstrapEndpoint(host=host, port=port, source="runtime_config")
        for host, port in config.bootstrap_endpoints
    ]
    return engine


def parse_args() -> argparse.Namespace:
    default_config = CoreConfig()
    parser = argparse.ArgumentParser(description="Sobe um physical node do AnonNetCore.")
    parser.add_argument("--node-index", type=int, required=True, help="Indice numerico do node.")
    parser.add_argument(
        "--listen-port",
        type=int,
        default=default_config.listen_port,
        help="Porta TCP local do node.",
    )
    parser.add_argument(
        "--advertised-host",
        type=str,
        default=None,
        help="Host anunciado para outros peers alcançarem este node.",
    )
    parser.add_argument(
        "--advertised-port",
        type=int,
        default=None,
        help="Porta anunciada para outros peers alcançarem este node.",
    )
    parser.add_argument(
        "--bootstrap-endpoint",
        action="append",
        default=[],
        help="Endpoint bootstrap no formato host:port. Pode ser informado varias vezes.",
    )
    return parser.parse_args()


def _build_node_name(node_index: int) -> str:
    return f"node-{node_index:03d}"


def _parse_host_port(value: str) -> tuple[str, int]:
    host, separator, raw_port = value.rpartition(":")
    if not separator or not host or not raw_port:
        raise SystemExit(f"Bootstrap endpoint invalido: {value}. Use host:port.")

    try:
        port = int(raw_port)
    except ValueError as error:
        raise SystemExit(f"Porta bootstrap invalida em: {value}") from error

    if port <= 0:
        raise SystemExit(f"Porta bootstrap invalida em: {value}")

    return host, port


def main() -> None:
    asyncio.run(run_node(build_engine_from_args()))


if __name__ == "__main__":
    main()
