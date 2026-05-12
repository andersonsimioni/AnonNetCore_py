from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from core_helpers import reset_core_data_dir, stop_cores
from smoke_helpers import (
    MIN_CLUSTER_NODES,
    create_local_virtual_node,
    create_route_for_virtual_node,
    create_test_core,
    reset_cluster,
    resolve_required_ready_nodes,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_network_ready,
    wait_for_route_active,
    wait_for_virtual_keepalive_ack,
    wait_for_virtual_session_active,
)


TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "virtual-session-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
CORE_A_PORT = 19101
CORE_B_PORT = 19102
VIRTUAL_SESSION_KEEPALIVE_SECONDS = 4


async def main() -> None:
    args = parse_args()
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=args.cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )

    reset_core_data_dir(TEST_DATA_ROOT)
    print(f"reset test data: {TEST_DATA_ROOT}")
    reset_cluster()
    start_cluster(node_count=args.cluster_nodes)
    wait_for_cluster_containers(expected_count=args.cluster_nodes)

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

        vn_a = create_local_virtual_node(
            core_a,
            kind="test-vn-a",
            metadata_source="virtual_session_smoke",
        )
        vn_b = create_local_virtual_node(
            core_b,
            kind="test-vn-b",
            metadata_source="virtual_session_smoke",
        )
        print(f"vn A created: {vn_a.id}")
        print(f"vn B created: {vn_b.id}")

        await wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes)
        await wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes)
        print(f"cores A/B know enough physical nodes: required_ready_nodes={required_ready_nodes}")

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
        print(f"OK virtual session keepalive received: session_id={session_id}")
    finally:
        await stop_cores(core_b, core_a)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test: core A publica rota DRT e core B abre sessao virtual com VN A.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=MIN_CLUSTER_NODES)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"virtual session smoke failed: {error}", file=sys.stderr)
        raise
