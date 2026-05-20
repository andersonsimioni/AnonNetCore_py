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
    create_test_core,
    reset_cluster,
    resolve_cluster_node_count,
    resolve_required_ready_nodes,
    start_cluster,
    validate_virtual_message_roundtrip,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_network_ready,
    wait_for_runtime_route_active,
    wait_for_virtual_session_active,
)


TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "virtual-message-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
CORE_A_PORT = 19301
CORE_B_PORT = 19302
APP_MESSAGE_TYPE = "integration.virtual-message.echo"
APP_REPLY_MESSAGE_TYPE = "integration.virtual-message.echo.reply"


async def main() -> None:
    args = parse_args()
    cluster_nodes = resolve_cluster_node_count(args.cluster_nodes)
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )

    reset_core_data_dir(TEST_DATA_ROOT)
    print(f"reset test data: {TEST_DATA_ROOT}")
    reset_cluster()
    start_cluster(node_count=cluster_nodes)
    wait_for_cluster_containers(expected_count=cluster_nodes)

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
        print("checkpoint 1 OK: cores A/B started")

        vn_a = create_local_virtual_node(
            core_a,
            kind="virtual-message-vn-a",
            metadata_source="virtual_message_smoke",
        )
        vn_b = create_local_virtual_node(
            core_b,
            kind="virtual-message-vn-b",
            metadata_source="virtual_message_smoke",
        )
        print(f"checkpoint 2 OK: virtual nodes created: vn_a={vn_a.id} vn_b={vn_b.id}")

        await asyncio.gather(
            wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes),
            wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes),
        )
        print(f"checkpoint 3 OK: network ready: required_ready_nodes={required_ready_nodes}")

        await wait_for_cluster_network_maturity(
            core_a,
            core_b,
            required_ready_nodes=required_ready_nodes,
        )
        print("checkpoint 4 OK: cluster network maturity reached")

        active_route = await wait_for_runtime_route_active(core_a, local_virtual_node_id=vn_a.id)
        print(
            "checkpoint 5 OK: route active from runtime: "
            f"initial_path_id={active_route.initial_path_id} final_path_id={active_route.final_path_id}"
        )

        core_b.services.identity_service.upsert_remote_virtual_node(
            node_id=vn_a.id,
            public_key=vn_a.public_key,
            kind=vn_a.kind,
            status="active",
            metadata_json='{"source":"virtual_message_smoke_identity_exchange"}',
        )
        print("checkpoint 6 OK: core B learned VN A identity")

        session_id = await core_b.services.protocol_clients.virtual.session.start_session_to_virtual_node(
            local_virtual_node_id=vn_b.id,
            remote_virtual_node_id=vn_a.id,
        )
        await wait_for_virtual_session_active(core_b, session_id)
        print(f"checkpoint 7 OK: virtual session active: session_id={session_id}")

        await run_virtual_message_protocol_smoke(core_a, core_b, session_id)
        print("checkpoint 8 OK: virtual message roundtrip delivered")
        print("OK virtual message smoke passed")
    finally:
        await stop_cores(core_b, core_a)


async def run_virtual_message_protocol_smoke(core_a, core_b, session_id: str) -> None:
    await validate_virtual_message_roundtrip(
        sender_engine=core_b,
        receiver_engine=core_a,
        session_id=session_id,
        app_message_type=APP_MESSAGE_TYPE,
        reply_message_type=APP_REPLY_MESSAGE_TYPE,
        payload={
            "text": "hello from virtual message smoke",
            "sequence": 1,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test: VirtualMessageProtocolHandler entrega e responde mensagens virtuais.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=MIN_CLUSTER_NODES)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"virtual message smoke failed: {error}", file=sys.stderr)
        raise
