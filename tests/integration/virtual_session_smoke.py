from __future__ import annotations

import argparse
import asyncio
import platform
from pathlib import Path
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from identity import VirtualNodeIdentityCreateInput

from core_helpers import create_isolated_core, reset_core_data_dir, stop_cores

TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "virtual-session-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
CORE_A_PORT = 19101
CORE_B_PORT = 19102
MIN_CLUSTER_NODES = 3
VIRTUAL_SESSION_KEEPALIVE_SECONDS = 4


async def main() -> None:
    args = parse_args()
    reset_core_data_dir(TEST_DATA_ROOT)
    print(f"reset test data: {TEST_DATA_ROOT}")
    start_cluster(minimum_remote_nodes=args.minimum_remote_nodes)
    wait_for_cluster_containers(minimum_remote_nodes=args.minimum_remote_nodes)

    core_a = create_test_core(
        data_dir=TEST_DATA_ROOT / "core-a",
        listen_port=CORE_A_PORT,
        log_dir=TEST_LOG_ROOT / "core-a",
    )
    core_b = create_test_core(
        data_dir=TEST_DATA_ROOT / "core-b",
        listen_port=CORE_B_PORT,
        log_dir=TEST_LOG_ROOT / "core-b",
    )

    try:
        await asyncio.gather(core_a.start(), core_b.start())
        print("cores A/B started")

        vn_a = create_local_virtual_node(core_a, kind="test-vn-a")
        vn_b = create_local_virtual_node(core_b, kind="test-vn-b")
        print(f"vn A created: {vn_a.id}")
        print(f"vn B created: {vn_b.id}")

        await wait_for_network_ready(core_a, minimum_remote_nodes=args.minimum_remote_nodes)
        await wait_for_network_ready(core_b, minimum_remote_nodes=args.minimum_remote_nodes)
        print("cores A/B know enough physical nodes")

        route_result = await create_route_for_virtual_node(core_a)
        initial_path_id = str(route_result["initial_path_id"])
        active_route = await wait_for_route_active(core_a, initial_path_id)
        print(f"route active: initial_path_id={initial_path_id} final_path_id={active_route.final_path_id}")

        core_b.services.identity_service.upsert_remote_virtual_node(
            node_id=vn_a.id,
            public_key=vn_a.public_key,
            kind=vn_a.kind,
            status="active",
            metadata_json='{"source":"integration_test_identity_exchange"}',
        )
        print("core B learned VN A identity")

        session_id = await core_b.services.protocol_clients.virtual.session.start_session_to_virtual_node(
            local_virtual_node_id=vn_b.id,
            remote_virtual_node_id=vn_a.id,
            keepalive_interval_seconds=VIRTUAL_SESSION_KEEPALIVE_SECONDS,
        )
        await wait_for_virtual_session_active(core_b, session_id)
        print(f"virtual session active: session_id={session_id}")
        await wait_for_virtual_keepalive_ack(core_b, session_id)
        print(f"virtual session keepalive ok: session_id={session_id}")
    finally:
        await stop_cores(core_b, core_a)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test: core A publica rota DRT e core B abre sessao virtual com VN A.",
    )
    parser.add_argument("--minimum-remote-nodes", type=int, default=2)
    return parser.parse_args()


def start_cluster(*, minimum_remote_nodes: int) -> None:
    node_count = max(minimum_remote_nodes + 1, MIN_CLUSTER_NODES)
    detach_argument = "-Detach" if platform.system().lower() == "windows" else "--detach"
    command = [
        sys.executable,
        str(PROJECT_ROOT / "cluster" / "up_nodes.py"),
        str(node_count),
        detach_argument,
    ]
    print(f"starting docker cluster: nodes={node_count}")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def wait_for_cluster_containers(
    *,
    minimum_remote_nodes: int,
    timeout_seconds: float = 120.0,
) -> None:
    expected_count = max(minimum_remote_nodes + 1, MIN_CLUSTER_NODES)
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        running_count = count_running_cluster_containers()
        if running_count >= expected_count:
            print(f"docker cluster running: containers={running_count}")
            return

        print(f"waiting docker cluster: running={running_count} expected={expected_count}")
        time.sleep(2.0)

    raise TimeoutError("Timed out waiting for docker cluster containers.")


def count_running_cluster_containers() -> int:
    completed = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            "name=anonnet-node-",
            "--format",
            "{{.Names}}",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return len([line for line in completed.stdout.splitlines() if line.strip()])


def create_test_core(
    *,
    data_dir: Path,
    listen_port: int,
    log_dir: Path,
):
    return create_isolated_core(
        data_dir=data_dir,
        listen_port=listen_port,
        log_dir=log_dir,
    )


def create_local_virtual_node(engine, *, kind: str):
    local_physical_node = engine.services.identity_service.get_local_physical_node_result()
    if local_physical_node is None:
        raise RuntimeError("Local physical node was not initialized.")

    return engine.services.identity_service.create_local_virtual_node(
        VirtualNodeIdentityCreateInput(
            kind=kind,
            owner_physical_node_id=local_physical_node.id,
            metadata_json='{"source":"virtual_session_smoke"}',
        )
    )


async def wait_for_network_ready(
    engine,
    *,
    minimum_remote_nodes: int,
    timeout_seconds: float = 60.0,
) -> None:
    async def is_ready() -> bool:
        await refresh_route_candidate_rtts(engine)
        candidates = engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
            limit=minimum_remote_nodes,
        )
        return len(candidates) >= minimum_remote_nodes

    await wait_until(is_ready, timeout_seconds=timeout_seconds, label="network ready")


async def refresh_route_candidate_rtts(engine) -> None:
    ping_candidates = engine.services.identity_service.list_remote_physical_nodes_for_ping(limit=32)
    for candidate in ping_candidates:
        has_rtt = any(
            route_candidate.node_id == candidate.node_id
            for route_candidate in engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
                limit=32,
            )
        )
        if has_rtt:
            continue

        try:
            ping_result = await engine.services.protocol_clients.physical.ping.ping_physical_node(
                remote_physical_node_id=candidate.node_id,
            )
        except Exception:
            continue

        observed_rtt_ms = ping_result.get("observed_rtt_ms")
        if isinstance(observed_rtt_ms, (int, float)):
            engine.services.identity_service.upsert_rtt_info(
                remote_physical_node_id=candidate.node_id,
                observed_rtt_ms=float(observed_rtt_ms),
            )


async def create_route_for_virtual_node(engine) -> dict[str, object]:
    candidates = engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(limit=16)
    if len(candidates) < 2:
        raise RuntimeError("Not enough physical route candidates with RTT.")

    first_hop = candidates[0]
    final_node = select_final_physical_node(candidates, first_hop.node_id)
    return await engine.services.protocol_clients.physical.route_build.start_random_walk_ttl_route(
        first_hop_physical_node_id=first_hop.node_id,
        final_physical_node_public_key=final_node.public_key,
        remaining_ttl_ms=30_000,
        expected_round_trip_ttl_ms=30_000,
    )


def select_final_physical_node(candidates, first_hop_node_id: str):
    for candidate in candidates:
        if candidate.node_id != first_hop_node_id:
            return candidate
    return candidates[0]


async def wait_for_route_active(
    engine,
    initial_path_id: str,
    *,
    timeout_seconds: float = 90.0,
):
    async def load_active_route():
        route = engine.services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=initial_path_id,
        )
        if route is not None and route.status == "active" and route.final_path_id:
            return route
        return None

    return await wait_until_value(load_active_route, timeout_seconds=timeout_seconds, label="route active")


async def wait_for_virtual_session_active(
    engine,
    session_id: str,
    *,
    timeout_seconds: float = 15.0,
) -> None:
    async def is_active() -> bool:
        session = engine.services.session_manager.get_session_by_session_id(session_id)
        return session is not None and session.session_state == "active"

    await wait_until(is_active, timeout_seconds=timeout_seconds, label="virtual session active")


async def wait_for_virtual_keepalive_ack(
    engine,
    session_id: str,
    *,
    timeout_seconds: float = 20.0,
) -> None:
    async def has_keepalive_ack() -> bool:
        session = engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.last_keepalive_sent_at is None:
            return False

        return session.last_activity_at > session.last_keepalive_sent_at

    await wait_until(has_keepalive_ack, timeout_seconds=timeout_seconds, label="virtual keepalive ack")


async def wait_until(predicate, *, timeout_seconds: float, label: str) -> None:
    async def value_predicate():
        return True if await predicate() else None

    await wait_until_value(value_predicate, timeout_seconds=timeout_seconds, label=label)


async def wait_until_value(loader, *, timeout_seconds: float, label: str):
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        value = await loader()
        if value is not None:
            return value
        await asyncio.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for {label}.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"virtual session smoke failed: {error}", file=sys.stderr)
        raise
