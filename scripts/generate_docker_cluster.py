from __future__ import annotations

import argparse
from pathlib import Path


INTERNAL_TCP_PORT = 19001
HOST_BOOTSTRAP_PORTS = {
    1: 19001,
    2: 19002,
}


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    state_dir = output_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

    for node_index in range(1, args.nodes + 1):
        (state_dir / build_node_name(node_index)).mkdir(parents=True, exist_ok=True)

    compose_path = output_dir / "docker-compose.generated.yml"
    compose_path.write_text(
        build_compose_text(node_count=args.nodes),
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
        default=Path("docker/cluster"),
        help="Diretorio de saida do compose e do estado local do cluster.",
    )
    args = parser.parse_args()
    if args.nodes < 2:
        raise SystemExit("Use pelo menos 2 nodes para manter os bootstraps fixos.")
    return args


def build_compose_text(*, node_count: int) -> str:
    lines: list[str] = [
        'name: "anonnet-test-cluster"',
        "services:",
    ]

    for node_index in range(1, node_count + 1):
        lines.extend(build_service_lines(node_index))

    lines.extend(
        [
            "networks:",
            "  anonnet-test-net:",
            '    name: "anonnet-test-net"',
            '    driver: "bridge"',
        ]
    )
    return "\n".join(lines) + "\n"


def build_service_lines(node_index: int) -> list[str]:
    node_name = build_node_name(node_index)
    lines = [
        f"  {node_name}:",
        "    build:",
        "      context: ../..",
        "      dockerfile: Dockerfile",
        f'    container_name: "anonnet-{node_name}"',
        f'    hostname: "{node_name}"',
        "    command:",
        f"      - python",
        f"      - main.py",
        f"      - --node-index",
        f'      - "{node_index}"',
        f"      - --listen-port",
        f'      - "{INTERNAL_TCP_PORT}"',
        "    volumes:",
        f'      - "./state/{node_name}:/app/data/local"',
        "    restart: unless-stopped",
        "    networks:",
        "      - anonnet-test-net",
    ]

    host_port = HOST_BOOTSTRAP_PORTS.get(node_index)
    if host_port is not None:
        lines.extend(
            [
                "    ports:",
                f'      - "{host_port}:{INTERNAL_TCP_PORT}"',
            ]
        )

    return lines


def build_node_name(node_index: int) -> str:
    return f"node-{node_index:03d}"


if __name__ == "__main__":
    main()
