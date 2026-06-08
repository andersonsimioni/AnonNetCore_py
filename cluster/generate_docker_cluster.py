from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import random
import socket
import sys


CLUSTER_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = CLUSTER_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

INTERNAL_TCP_PORT = 19001
INTERNAL_UDP_PORT = 19002
HOST_PORT_BASE = 19001
HOST_UDP_PORT_BASE = 29001
BOOTSTRAP_NODE_COUNT = 2


@dataclass(frozen=True, slots=True)
class ClusterNodeProfile:
    name: str
    reachability: str
    tcp_enabled: bool
    udp_enabled: bool

    @property
    def relay_enabled(self) -> bool:
        return self.reachability == "public" and self.tcp_enabled


BOOTSTRAP_PROFILE = ClusterNodeProfile(
    name="bootstrap_public_tcp",
    reachability="public",
    tcp_enabled=True,
    udp_enabled=False,
)
RANDOM_NODE_PROFILES = (
    ClusterNodeProfile(
        name="public_tcp_only",
        reachability="public",
        tcp_enabled=True,
        udp_enabled=False,
    ),
    ClusterNodeProfile(
        name="public_udp_only",
        reachability="public",
        tcp_enabled=False,
        udp_enabled=True,
    ),
    ClusterNodeProfile(
        name="public_tcp_udp",
        reachability="public",
        tcp_enabled=True,
        udp_enabled=True,
    ),
    ClusterNodeProfile(
        name="private_relay_client",
        reachability="private",
        tcp_enabled=True,
        udp_enabled=True,
    ),
)
PROFILE_BY_NAME = {
    BOOTSTRAP_PROFILE.name: BOOTSTRAP_PROFILE,
    **{profile.name: profile for profile in RANDOM_NODE_PROFILES},
}


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    state_dir = output_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    random_source = random.Random(args.seed)
    node_profiles = parse_profile_sequence(args.profiles, args.nodes)
    if node_profiles is None:
        node_profiles = build_node_profiles(args.nodes, random_source)

    for node_index in range(1, args.nodes + 1):
        (state_dir / build_node_name(node_index)).mkdir(parents=True, exist_ok=True)

    compose_path = output_dir / "docker-compose.generated.yml"
    compose_path.write_text(
        build_compose_text(
            node_profiles=node_profiles,
        ),
        encoding="utf-8",
    )
    print(f"Docker compose generated at: {compose_path}")
    print(f"Node count: {args.nodes}")
    print(f"Transport seed: {args.seed}")
    print_node_profile_summary(node_profiles)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generates the AnonNetCore Docker cluster environment.")
    parser.add_argument(
        "--nodes",
        type=int,
        required=True,
        help="Total number of physical nodes to start.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("cluster"),
        help="Output directory for compose and local cluster state.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=random.SystemRandom().randrange(1, 2**31),
        help="Seed used to randomize non-bootstrap node profiles.",
    )
    parser.add_argument(
        "--profiles",
        type=str,
        default=None,
        help=(
            "Comma-separated node profile names. When provided, the amount of "
            "profiles must match --nodes and bootstrap nodes must use "
            "bootstrap_public_tcp."
        ),
    )
    args = parser.parse_args()
    if args.nodes < 2:
        raise SystemExit("Use at least 2 nodes to keep fixed bootstrap nodes.")
    return args


def build_node_profiles(
    node_count: int,
    random_source: random.Random,
) -> list[ClusterNodeProfile]:
    profiles: list[ClusterNodeProfile] = []
    for node_index in range(1, node_count + 1):
        if node_index <= BOOTSTRAP_NODE_COUNT:
            profiles.append(BOOTSTRAP_PROFILE)
            continue
        profiles.append(random_source.choice(RANDOM_NODE_PROFILES))
    return profiles


def parse_profile_sequence(
    raw_profiles: str | None,
    node_count: int,
) -> list[ClusterNodeProfile] | None:
    if raw_profiles is None:
        return None

    profile_names = [name.strip() for name in raw_profiles.split(",") if name.strip()]
    if len(profile_names) != node_count:
        raise SystemExit(
            f"--profiles must contain exactly {node_count} profile names; "
            f"received {len(profile_names)}."
        )

    profiles: list[ClusterNodeProfile] = []
    for node_index, profile_name in enumerate(profile_names, start=1):
        profile = PROFILE_BY_NAME.get(profile_name)
        if profile is None:
            known_profiles = ", ".join(sorted(PROFILE_BY_NAME))
            raise SystemExit(f"Unknown node profile '{profile_name}'. Known profiles: {known_profiles}")

        if node_index <= BOOTSTRAP_NODE_COUNT and profile != BOOTSTRAP_PROFILE:
            raise SystemExit("The first two cluster nodes must use bootstrap_public_tcp.")

        profiles.append(profile)

    return profiles


def build_compose_text(*, node_profiles: list[ClusterNodeProfile]) -> str:
    host_advertised_ip = detect_local_network_host()
    lines: list[str] = [
        'name: "anonnet-test-cluster"',
        "services:",
    ]

    for node_index, node_profile in enumerate(node_profiles, start=1):
        lines.extend(
            build_service_lines(
                node_index=node_index,
                node_profile=node_profile,
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
    node_profile: ClusterNodeProfile,
    host_advertised_ip: str,
) -> list[str]:
    node_name = build_node_name(node_index)
    advertised_tcp_port = build_host_tcp_port(node_index)
    advertised_udp_port = build_host_udp_port(node_index)
    lines = [
        f"  {node_name}:",
        "    build:",
        "      context: ..",
        "      dockerfile: Dockerfile",
        f'    container_name: "anonnet-{node_name}"',
        f'    hostname: "{node_name}"',
        f'    labels:',
        f'      anonnet.node_profile: "{node_profile.name}"',
        "    command:",
        f"      - python",
        f"      - app/main.py",
        f"      - --listen-port",
        f'      - "{INTERNAL_TCP_PORT}"',
        f"      - --enable-log-error-reporting",
        f"      - --log-error-report-endpoint",
        f'      - "http://host.docker.internal:18999/v1/smoke-log-events"',
        "    environment:",
        f'      ANONNET_NODE_REACHABILITY: "{node_profile.reachability}"',
        f'      ANONNET_TCP_TRANSPORT_ENABLED: "{format_bool(node_profile.tcp_enabled)}"',
        f'      ANONNET_UDP_TRANSPORT_ENABLED: "{format_bool(node_profile.udp_enabled)}"',
        f'      ANONNET_RELAY_SERVICE_ENABLED: "{format_bool(node_profile.relay_enabled)}"',
        f'      ANONNET_BOOTSTRAP_HOST: "{host_advertised_ip}"',
        '      ANONNET_DOCKER_HOST_GATEWAY: "host.docker.internal"',
    ]
    if node_profile.tcp_enabled:
        lines.extend(
            [
                f'      ANONNET_ADVERTISED_TCP_HOST: "{host_advertised_ip}"',
                f'      ANONNET_ADVERTISED_TCP_PORT: "{advertised_tcp_port}"',
            ]
        )
    if node_profile.udp_enabled:
        lines.extend(
            [
                f'      ANONNET_ADVERTISED_UDP_HOST: "{host_advertised_ip}"',
                f'      ANONNET_ADVERTISED_UDP_PORT: "{advertised_udp_port}"',
            ]
        )

    lines.extend(
        [
        "    extra_hosts:",
        '      - "host.docker.internal:host-gateway"',
        "    volumes:",
        f'      - "./state/{node_name}:/app/data/local"',
        "    restart: unless-stopped",
        "    networks:",
        "      - anonnet-test-net",
        ]
    )
    port_mappings = build_port_mappings(
        node_profile=node_profile,
        advertised_tcp_port=advertised_tcp_port,
        advertised_udp_port=advertised_udp_port,
    )
    if port_mappings:
        lines.append("    ports:")
        lines.extend(f'      - "{mapping}"' for mapping in port_mappings)

    return lines


def build_node_name(node_index: int) -> str:
    return f"node-{node_index:03d}"


def build_host_tcp_port(node_index: int) -> int:
    return HOST_PORT_BASE + node_index - 1


def build_host_udp_port(node_index: int) -> int:
    return HOST_UDP_PORT_BASE + node_index - 1


def build_port_mappings(
    *,
    node_profile: ClusterNodeProfile,
    advertised_tcp_port: int,
    advertised_udp_port: int,
) -> list[str]:
    if node_profile.reachability == "private":
        return []

    mappings: list[str] = []
    if node_profile.tcp_enabled:
        mappings.append(f"{advertised_tcp_port}:{INTERNAL_TCP_PORT}/tcp")
    if node_profile.udp_enabled:
        mappings.append(f"{advertised_udp_port}:{INTERNAL_UDP_PORT}/udp")
    return mappings


def format_bool(value: bool) -> str:
    return "true" if value else "false"


def print_node_profile_summary(node_profiles: list[ClusterNodeProfile]) -> None:
    for node_index, node_profile in enumerate(node_profiles, start=1):
        print(
            f"{build_node_name(node_index)}: "
            f"profile={node_profile.name} "
            f"reachability={node_profile.reachability} "
            f"tcp={format_bool(node_profile.tcp_enabled)} "
            f"udp={format_bool(node_profile.udp_enabled)} "
            f"relay={format_bool(node_profile.relay_enabled)}"
        )


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
