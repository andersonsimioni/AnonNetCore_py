from __future__ import annotations

import asyncio
import math
import platform
from pathlib import Path
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from crypto import sha512_hex
from identity import VirtualNodeIdentityCreateInput

from core_helpers import create_isolated_core


MIN_CLUSTER_NODES = 3
DEFAULT_READY_CLUSTER_RATIO = 0.6
NETWORK_READY_TIMEOUT_SECONDS = 180.0
FORCED_EXCHANGE_INTERVAL_SECONDS = 15.0


def resolve_required_ready_nodes(
    *,
    cluster_nodes: int,
    minimum_remote_nodes: int | None,
) -> int:
    if minimum_remote_nodes is not None:
        return max(1, minimum_remote_nodes)

    return max(1, math.ceil(cluster_nodes * DEFAULT_READY_CLUSTER_RATIO))


def reset_cluster() -> None:
    command = [
        sys.executable,
        str(PROJECT_ROOT / "cluster" / "down_nodes.py"),
    ]
    print("resetting docker cluster")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def start_cluster(*, node_count: int) -> None:
    node_count = max(node_count, MIN_CLUSTER_NODES)
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
    expected_count: int,
    timeout_seconds: float = 120.0,
) -> None:
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


def create_local_virtual_node(engine, *, kind: str, metadata_source: str):
    local_physical_node = engine.services.identity_service.get_local_physical_node_result()
    if local_physical_node is None:
        raise RuntimeError("Local physical node was not initialized.")

    return engine.services.identity_service.create_local_virtual_node(
        VirtualNodeIdentityCreateInput(
            kind=kind,
            owner_physical_node_id=local_physical_node.id,
            metadata_json=f'{{"source":"{metadata_source}"}}',
        )
    )


async def wait_for_network_ready(
    engine,
    *,
    minimum_remote_nodes: int,
    timeout_seconds: float = NETWORK_READY_TIMEOUT_SECONDS,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_ready_count = -1
    last_forced_exchange_at = 0.0

    while asyncio.get_running_loop().time() < deadline:
        now = asyncio.get_running_loop().time()
        if now - last_forced_exchange_at >= FORCED_EXCHANGE_INTERVAL_SECONDS:
            await request_known_nodes_from_active_peers(engine)
            last_forced_exchange_at = now

        await refresh_route_candidate_rtts(engine)
        candidates = engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
            limit=minimum_remote_nodes,
        )
        ready_count = len(candidates)
        if ready_count >= minimum_remote_nodes:
            return

        if ready_count != last_ready_count:
            print(
                "waiting network ready: "
                f"node={engine.get_runtime_node_name()} "
                f"ready_route_candidates={ready_count} "
                f"required={minimum_remote_nodes}"
            )
            last_ready_count = ready_count

        await asyncio.sleep(1.0)

    raise TimeoutError(
        f"Timed out waiting for network ready: required_ready_nodes={minimum_remote_nodes}."
    )


async def request_known_nodes_from_active_peers(engine) -> None:
    exchange_candidates = engine.services.identity_service.list_remote_physical_nodes_for_info_exchange(
        limit=4,
    )
    for candidate in exchange_candidates:
        try:
            session = engine.services.session_manager.get_active_physical_session_by_remote_node_id(
                candidate.node_id
            )
            if session is None:
                session_id = await engine.services.protocol_clients.physical.session.start_session(
                    remote_physical_node_id=candidate.node_id,
                )
                session = engine.services.session_manager.get_session_by_session_id(session_id)
            if session is None or session.session_state != "active":
                continue

            await engine.services.protocol_clients.physical.node_info_exchange.request_known_physical_nodes(
                session_id=session.session_id,
            )
        except Exception:
            continue


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


async def wait_for_drt_entry(
    engine,
    *,
    virtual_node_public_key: str,
    timeout_seconds: float = 60.0,
) -> dict[str, object]:
    logical_key = sha512_hex(virtual_node_public_key.encode("utf-8"))

    async def load_drt_entry():
        result = await engine.services.protocol_clients.physical.dht.query(
            namespace="drt",
            logical_key=logical_key,
        )
        if result.get("status") == "found":
            return result
        return None

    return await wait_until_value(load_drt_entry, timeout_seconds=timeout_seconds, label="DRT entry")


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
