from __future__ import annotations

import asyncio
import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


from core.network import detect_local_network_host  # noqa: E402
from core_helpers import create_isolated_core, reset_core_data_dir, stop_cores  # noqa: E402
from smoke_helpers import (  # noqa: E402
    request_known_nodes_from_active_peers,
    reset_cluster,
    resolve_required_ready_nodes,
    resolve_cluster_node_count,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_cluster_to_reach_local_core_ports,
    wait_for_network_ready,
)
from smokes_config import SMOKES_CONFIG  # noqa: E402


DATA_DIR = PROJECT_ROOT / "data" / "local" / "integration" / "physical-relay-smoke"
LOG_DIR = DATA_DIR / "logs"
LISTEN_HOST = "0.0.0.0"
LOCAL_CONNECT_HOST = "127.0.0.1"
RELAY_PORT = SMOKES_CONFIG.physical_relay_relay_port
REQUESTER_PORT = SMOKES_CONFIG.physical_relay_requester_port
PRIVATE_NODE_PORT = SMOKES_CONFIG.physical_relay_private_node_port


def main() -> None:
    asyncio.run(_run(parse_args()))
    print("physical relay smoke OK")


async def _run(args: argparse.Namespace) -> None:
    cluster_nodes = resolve_cluster_node_count(args.cluster_nodes)
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )
    advertised_host = detect_local_network_host()

    reset_core_data_dir(DATA_DIR)
    print(f"reset test data: {DATA_DIR}")
    reset_cluster()
    start_cluster(node_count=cluster_nodes)
    wait_for_cluster_containers(expected_count=cluster_nodes)

    previous_advertised_host = os.environ.get("ANONNET_ADVERTISED_TCP_HOST")
    os.environ["ANONNET_ADVERTISED_TCP_HOST"] = advertised_host

    relay = create_isolated_core(
        data_dir=DATA_DIR / "relay",
        listen_host=LISTEN_HOST,
        listen_port=RELAY_PORT,
        log_dir=LOG_DIR / "relay",
    )
    requester = create_isolated_core(
        data_dir=DATA_DIR / "requester",
        listen_host=LISTEN_HOST,
        listen_port=REQUESTER_PORT,
        log_dir=LOG_DIR / "requester",
    )
    private_b = create_isolated_core(
        data_dir=DATA_DIR / "private-b",
        listen_host=LISTEN_HOST,
        listen_port=PRIVATE_NODE_PORT,
        log_dir=LOG_DIR / "private-b",
        node_reachability="private",
    )

    try:
        await asyncio.gather(relay.start(), requester.start(), private_b.start())
        print(
            "local relay smoke cores started: "
            f"advertised_host={advertised_host} relay_port={RELAY_PORT} "
            f"requester_port={REQUESTER_PORT} private_port={PRIVATE_NODE_PORT}"
        )

        await _assert_private_tcp_listener_is_closed(PRIVATE_NODE_PORT)
        wait_for_cluster_to_reach_local_core_ports(RELAY_PORT, REQUESTER_PORT)

        relay_node = _require_local_node(relay, "relay")
        requester_node = _require_local_node(requester, "requester")
        private_b_node = _require_local_node(private_b, "private-b")

        await asyncio.gather(
            wait_for_network_ready(relay, minimum_remote_nodes=required_ready_nodes),
            wait_for_network_ready(requester, minimum_remote_nodes=required_ready_nodes),
            wait_for_network_ready(private_b, minimum_remote_nodes=required_ready_nodes),
        )
        await wait_for_cluster_network_maturity(
            relay,
            requester,
            private_b,
            required_ready_nodes=required_ready_nodes,
        )
        print(f"local cores joined docker cluster: required_ready_nodes={required_ready_nodes}")

        await asyncio.gather(
            _wait_until_knows_node(private_b, relay_node.id, "private-b learned relay from cluster"),
            _wait_until_knows_node(requester, relay_node.id, "requester learned relay from cluster"),
        )

        await private_b.services.protocol_clients.physical.relay.register_local_node_at_relay(
            relay_physical_node_id=relay_node.id,
        )
        await _wait_for_relay_registration(relay, private_b_node.id)
        print("private node registered at relay")

        relay_session_id = await requester.services.protocol_clients.physical.session.start_session(
            remote_physical_node_id=relay_node.id,
        )
        await requester.services.protocol_clients.physical.node_info_exchange.request_known_physical_nodes(
            session_id=relay_session_id,
            max_records=10,
        )
        await _wait_until_requester_knows_private_node(requester, private_b_node.id)
        print("requester learned private node relay endpoint from relay")

        session_id = await requester.services.protocol_clients.physical.session.start_session(
            remote_physical_node_id=private_b_node.id,
        )
        requester_session = requester.services.session_manager.get_session_by_session_id(session_id)
        if requester_session is None or requester_session.session_state != "active":
            raise RuntimeError("Requester did not establish an active physical session through relay.")
        if requester_session.transport != "relay_tcp":
            raise RuntimeError(f"Expected requester session over relay_tcp, got {requester_session.transport}.")

        private_session = await _wait_for_active_session(
            private_b,
            requester_node.id,
            expected_transport="relay_tcp",
        )
        if private_session.transport != "relay_tcp":
            raise RuntimeError(f"Expected private node session over relay_tcp, got {private_session.transport}.")

        await requester.services.protocol_clients.physical.session.send_keepalive(session_id=session_id)
        await asyncio.sleep(SMOKES_CONFIG.physical_relay_medium_poll_seconds)
        print(
            "relay physical session validated: "
            f"requester={requester_node.id} private={private_b_node.id} relay={relay_node.id}"
        )
    finally:
        await stop_cores(private_b, requester, relay)
        if previous_advertised_host is None:
            os.environ.pop("ANONNET_ADVERTISED_TCP_HOST", None)
        else:
            os.environ["ANONNET_ADVERTISED_TCP_HOST"] = previous_advertised_host


def _require_local_node(engine, name: str):
    local_node = engine.services.identity_service.get_local_physical_node_result()
    if local_node is None:
        raise RuntimeError(f"{name} local physical identity was not initialized.")
    return local_node


async def _assert_private_tcp_listener_is_closed(port: int) -> None:
    try:
        reader, writer = await asyncio.open_connection(LOCAL_CONNECT_HOST, port)
    except OSError:
        return

    writer.close()
    await writer.wait_closed()
    raise RuntimeError("Private node unexpectedly accepted a direct TCP connection.")


async def _wait_until_knows_node(engine, remote_node_id: str, label: str) -> None:
    deadline = asyncio.get_running_loop().time() + SMOKES_CONFIG.drt_entry_timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        remote_node = engine.services.identity_service.get_remote_physical_node_by_id(remote_node_id)
        endpoints = engine.services.identity_service.list_remote_physical_node_endpoints(remote_node_id)
        if remote_node is not None and endpoints:
            return

        await request_known_nodes_from_active_peers(engine)
        await asyncio.sleep(SMOKES_CONFIG.physical_relay_registration_poll_seconds)

    raise TimeoutError(f"Timed out waiting for {label}.")


async def _wait_for_relay_registration(relay, private_node_id: str) -> None:
    deadline = asyncio.get_running_loop().time() + SMOKES_CONFIG.virtual_session_active_timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        registration = relay.services.relay_service.get_active_registration(private_node_id)
        if registration is not None:
            return
        await asyncio.sleep(SMOKES_CONFIG.physical_relay_short_poll_seconds)
    raise TimeoutError("Relay did not keep the private node registration.")


async def _wait_until_requester_knows_private_node(requester, private_node_id: str) -> None:
    deadline = asyncio.get_running_loop().time() + SMOKES_CONFIG.virtual_session_active_timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        remote_node = requester.services.identity_service.get_remote_physical_node_by_id(private_node_id)
        endpoints = requester.services.identity_service.list_remote_physical_node_endpoints(private_node_id)
        if remote_node is not None and any(endpoint.transport == "relay_tcp" for endpoint in endpoints):
            return
        await asyncio.sleep(SMOKES_CONFIG.physical_relay_short_poll_seconds)
    raise TimeoutError("Requester did not learn the private node relay endpoint from R.")


async def _wait_for_active_session(
    engine,
    remote_physical_node_id: str,
    *,
    expected_transport: str | None = None,
):
    deadline = asyncio.get_running_loop().time() + SMOKES_CONFIG.virtual_session_active_timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        sessions = engine.services.session_manager.list_sessions()
        for session in sessions:
            if session.remote_identity_id != remote_physical_node_id:
                continue
            if session.session_state != "active":
                continue
            if expected_transport is not None and session.transport != expected_transport:
                continue
            return session
        await asyncio.sleep(SMOKES_CONFIG.physical_relay_short_poll_seconds)

    transport_suffix = f" over {expected_transport}" if expected_transport is not None else ""
    raise TimeoutError(f"Private node did not activate the physical session{transport_suffix}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real relay smoke: usa cluster Docker, relay publico local e node privado sem listener TCP.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=SMOKES_CONFIG.min_cluster_nodes)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    main()
