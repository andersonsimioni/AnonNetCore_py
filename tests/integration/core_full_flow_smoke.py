from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from sessions import VirtualSessionMessageReply

from core_helpers import reset_core_data_dir, stop_cores
from smoke_helpers import (
    create_local_virtual_node,
    create_route_for_virtual_node,
    create_test_core,
    reset_cluster,
    resolve_required_ready_nodes,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_drt_entry,
    wait_for_network_ready,
    wait_for_route_active,
    wait_for_virtual_keepalive_ack,
    wait_for_virtual_session_active,
    wait_until,
)


TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "core-full-flow-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
CORE_A_PORT = 19201
CORE_B_PORT = 19202
DEFAULT_CLUSTER_NODES = 5
VIRTUAL_SESSION_KEEPALIVE_SECONDS = 4
APP_MESSAGE_TYPE = "integration.echo"
APP_REPLY_MESSAGE_TYPE = "integration.echo.reply"


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
        print("checkpoint 1 OK: cores A/B started")

        vn_a = create_local_virtual_node(
            core_a,
            kind="full-flow-vn-a",
            metadata_source="core_full_flow_smoke",
        )
        vn_b = create_local_virtual_node(
            core_b,
            kind="full-flow-vn-b",
            metadata_source="core_full_flow_smoke",
        )
        print(f"checkpoint 2 OK: virtual nodes created: vn_a={vn_a.id} vn_b={vn_b.id}")

        await wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes)
        await wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes)
        print(f"checkpoint 3 OK: network ready: required_ready_nodes={required_ready_nodes}")

        route_result = await create_route_for_virtual_node(core_a)
        initial_path_id = str(route_result["initial_path_id"])
        active_route = await wait_for_route_active(core_a, initial_path_id)
        print(
            "checkpoint 4 OK: route active: "
            f"initial_path_id={initial_path_id} final_path_id={active_route.final_path_id}"
        )

        await wait_for_drt_entry(core_b, virtual_node_public_key=vn_a.public_key)
        print("checkpoint 5 OK: DRT entry discovered from core B")

        core_b.services.identity_service.upsert_remote_virtual_node(
            node_id=vn_a.id,
            public_key=vn_a.public_key,
            kind=vn_a.kind,
            status="active",
            metadata_json='{"source":"integration_test_identity_exchange"}',
        )
        print("checkpoint 6 OK: core B learned VN A identity")

        session_id = await core_b.services.protocol_clients.virtual.session.start_session_to_virtual_node(
            local_virtual_node_id=vn_b.id,
            remote_virtual_node_id=vn_a.id,
            keepalive_interval_seconds=VIRTUAL_SESSION_KEEPALIVE_SECONDS,
        )
        await wait_for_virtual_session_active(core_b, session_id)
        print(f"checkpoint 7 OK: virtual session active: session_id={session_id}")

        await wait_for_virtual_keepalive_ack(core_b, session_id)
        print("checkpoint 8 OK: virtual keepalive ack received")

        await validate_virtual_session_data_roundtrip(core_a, core_b, session_id)
        print("checkpoint 9 OK: virtual session data roundtrip delivered")
        print("OK core full flow smoke passed")
    finally:
        await stop_cores(core_b, core_a)


async def validate_virtual_session_data_roundtrip(core_a, core_b, session_id: str) -> None:
    loop = asyncio.get_running_loop()
    received_request = loop.create_future()
    received_reply = loop.create_future()

    def handle_request(message):
        if message.session_id != session_id:
            return None
        if not received_request.done():
            received_request.set_result(message)

        return VirtualSessionMessageReply(
            app_message_type=APP_REPLY_MESSAGE_TYPE,
            payload={
                "echo": message.payload,
                "received_by": message.local_virtual_node_id,
            },
            request_id=message.request_id,
        )

    def handle_reply(message):
        if message.session_id != session_id:
            return None
        if not received_reply.done():
            received_reply.set_result(message)
        return None

    core_a.services.session_manager.register_virtual_message_handler(
        APP_MESSAGE_TYPE,
        handle_request,
    )
    core_b.services.session_manager.register_virtual_message_handler(
        APP_REPLY_MESSAGE_TYPE,
        handle_reply,
    )

    request_id = await core_b.services.protocol_clients.virtual.session.send_message(
        session_id=session_id,
        app_message_type=APP_MESSAGE_TYPE,
        payload={"value": "hello-core"},
    )

    await wait_until(
        lambda: _future_done(received_request),
        timeout_seconds=20.0,
        label="virtual data request delivered",
    )
    await wait_until(
        lambda: _future_done(received_reply),
        timeout_seconds=20.0,
        label="virtual data reply delivered",
    )

    request_message = received_request.result()
    reply_message = received_reply.result()
    if request_message.request_id != request_id:
        raise RuntimeError("Virtual data request_id mismatch on receiver.")
    if reply_message.request_id != request_id:
        raise RuntimeError("Virtual data reply request_id mismatch.")
    if reply_message.payload.get("echo") != {"value": "hello-core"}:
        raise RuntimeError("Virtual data reply payload mismatch.")


async def _future_done(future: asyncio.Future) -> bool:
    return future.done()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Full integration smoke: cluster, route build, DRT, virtual session e data.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=DEFAULT_CLUSTER_NODES)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"core full flow smoke failed: {error}", file=sys.stderr)
        raise
