from __future__ import annotations

import argparse
from pathlib import Path
import socket
import sys


CLUSTER_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = CLUSTER_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

INTERNAL_TCP_PORT = 19001
HOST_PORT_BASE = 19001


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    state_dir = output_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

    for node_index in range(1, args.nodes + 1):
        (state_dir / build_node_name(node_index)).mkdir(parents=True, exist_ok=True)

    compose_path = output_dir / "docker-compose.generated.yml"
    compose_path.write_text(
        build_compose_text(
            node_count=args.nodes,
        ),
        encoding="utf-8",
    )
    print(f"Docker compose gerado em: {compose_path}")
    print(f"Quantidade de nodes: {args.nodes}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera o ambiente Docker do cluster AnonNetCore.")
    parser.add_argument(
        "--nodes",
        type=int,
        required=True,
        help="Quantidade total de physical nodes a subir.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("cluster"),
        help="Diretorio de saida do compose e do estado local do cluster.",
    )
    args = parser.parse_args()
    if args.nodes < 2:
        raise SystemExit("Use pelo menos 2 nodes para manter os bootstraps fixos.")
    return args


def build_compose_text(*, node_count: int) -> str:
    host_advertised_ip = detect_local_network_host()
    lines: list[str] = [
        'name: "anonnet-test-cluster"',
        "services:",
    ]

    for node_index in range(1, node_count + 1):
        lines.extend(
            build_service_lines(
                node_index=node_index,
                host_advertised_ip=host_advertised_ip,
            )
        )

    lines.extend(
        [
            "networks:",
            "  anonnet-test-net:",
            '    name: "anonnet-test-net"',
            '    driver: "bridge"',
        ]
    )
    return "\n".join(lines) + "\n"


def build_service_lines(
    *,
    node_index: int,
    host_advertised_ip: str,
) -> list[str]:
    node_name = build_node_name(node_index)
    advertised_port = build_host_port(node_index)
    lines = [
        f"  {node_name}:",
        "    build:",
        "      context: ..",
        "      dockerfile: Dockerfile",
        f'    container_name: "anonnet-{node_name}"',
        f'    hostname: "{node_name}"',
        "    command:",
        f"      - python",
        f"      - app/main.py",
        f"      - --listen-port",
        f'      - "{INTERNAL_TCP_PORT}"',
        "    environment:",
        f'      ANONNET_ADVERTISED_TCP_HOST: "{host_advertised_ip}"',
        f'      ANONNET_ADVERTISED_TCP_PORT: "{advertised_port}"',
    ]

    lines.extend(
        [
        "    volumes:",
        f'      - "./state/{node_name}:/app/data/local"',
        "    restart: unless-stopped",
        "    networks:",
        "      - anonnet-test-net",
        "    ports:",
        f'      - "{advertised_port}:{INTERNAL_TCP_PORT}"',
        ]
    )

    return lines


def build_node_name(node_index: int) -> str:
    return f"node-{node_index:03d}"


def build_host_port(node_index: int) -> int:
    return HOST_PORT_BASE + node_index - 1


def detect_local_network_host() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            local_host = sock.getsockname()[0]
            if local_host:
                return local_host
    except OSError:
        pass

    return "127.0.0.1"


if __name__ == "__main__":
    main()
