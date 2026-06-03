from __future__ import annotations

import argparse
import asyncio
import base64
from dataclasses import dataclass
from pathlib import Path
import random
import sys
import time
from urllib.parse import quote


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from core_helpers import reset_core_data_dir, stop_cores
from smoke_helpers import (
    create_test_core,
    reset_cluster,
    resolve_cluster_node_count,
    resolve_required_ready_nodes,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_stable_drt_online_route_count,
    wait_for_network_ready,
    wait_until_value,
)
from smokes_config import SMOKES_CONFIG
from virtual_api_stress_smoke import (
    JsonApiClient,
    find_free_tcp_port,
    random_text,
    resolve_local_core_port,
    run_async_step,
    run_sync_step,
    wait_for_api_download,
    wait_for_cluster_to_reach_local_core_ports,
    wait_for_publish_job_stored,
)


TEST_DATA_BASE_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "virtual-api-local-vn-stress-smoke"
APP_MESSAGE_TYPE = "integration.virtual-api-local-vn-stress.message"
DEFAULT_LOCAL_CORE_PORT = SMOKES_CONFIG.virtual_api_local_core_port


async def main() -> None:
    args = parse_args()
    random_source = random.Random(args.seed)
    test_data_root = TEST_DATA_BASE_ROOT / args.run_id
    test_log_root = test_data_root / "logs"
    ports = resolve_smoke_ports(args)
    cluster_nodes = resolve_cluster_node_count(args.cluster_nodes)
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )

    reset_core_data_dir(test_data_root)
    print(f"reset test data: {test_data_root}", flush=True)
    print(
        "resolved local-vn stress ports: "
        f"core={ports.core} api={ports.api}",
        flush=True,
    )
    await run_sync_step(
        "reset docker cluster",
        reset_cluster,
        timeout_seconds=SMOKES_CONFIG.virtual_api_step_reset_cluster_timeout_seconds,
    )
    await run_sync_step(
        f"start docker cluster: nodes={cluster_nodes}",
        lambda: start_cluster(node_count=cluster_nodes),
        timeout_seconds=SMOKES_CONFIG.virtual_api_step_start_cluster_timeout_seconds,
    )
    await run_sync_step(
        "wait docker cluster containers",
        lambda: wait_for_cluster_containers(expected_count=cluster_nodes),
        timeout_seconds=SMOKES_CONFIG.virtual_api_step_wait_cluster_timeout_seconds,
    )

    core = create_test_core(
        data_dir=test_data_root / "core",
        listen_port=ports.core,
        log_dir=test_log_root / "core",
        api_port=ports.api,
        virtual_route_expected_round_trip_ttl_ms=args.route_rtt_ms,
        virtual_route_pending_timeout_seconds=args.route_pending_timeout_seconds,
        virtual_route_min_online_routes=args.min_online_routes,
    )
    api = JsonApiClient(
        f"http://127.0.0.1:{ports.api}",
        timeout_seconds=SMOKES_CONFIG.virtual_api_client_timeout_seconds,
    )

    try:
        await run_async_step(
            "checkpoint 1: start single API core",
            core.start(),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_start_core_timeout_seconds,
        )
        await run_sync_step(
            "checkpoint 1b: docker can reach local core TCP port",
            lambda: wait_for_cluster_to_reach_local_core_ports(ports.core),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_reachability_timeout_seconds,
        )

        vn_a, vn_b = await run_async_step(
            "checkpoint 2: create two local virtual nodes through API",
            asyncio.gather(
                api.post(
                    "/v1/virtual-nodes",
                    {
                        "kind": "virtual-api-local-stress-a",
                        "metadata": {"source": "virtual_api_local_vn_stress_smoke"},
                    },
                ),
                api.post(
                    "/v1/virtual-nodes",
                    {
                        "kind": "virtual-api-local-stress-b",
                        "metadata": {"source": "virtual_api_local_vn_stress_smoke"},
                    },
                ),
            ),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_create_nodes_timeout_seconds,
        )
        print(
            f"checkpoint 2 details: vn_a={vn_a['id']} vn_b={vn_b['id']}",
            flush=True,
        )

        await run_async_step(
            f"checkpoint 3a: wait network ready: required_ready_nodes={required_ready_nodes}",
            wait_for_network_ready(core, minimum_remote_nodes=required_ready_nodes),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_network_ready_timeout_seconds,
        )
        await run_async_step(
            "checkpoint 3b: wait cluster network maturity",
            wait_for_cluster_network_maturity(
                core,
                required_ready_nodes=required_ready_nodes,
            ),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_maturity_timeout_seconds,
        )

        route_inventory = await run_async_step(
            f"checkpoint 4: wait both local VNs in DRT: min_online_routes={args.min_online_routes}",
            asyncio.gather(
                wait_for_stable_drt_online_route_count(
                    core,
                    virtual_node_public_key=str(vn_a["public_key"]),
                    minimum_routes=args.min_online_routes,
                    timeout_seconds=args.route_inventory_timeout_seconds,
                ),
                wait_for_stable_drt_online_route_count(
                    core,
                    virtual_node_public_key=str(vn_b["public_key"]),
                    minimum_routes=args.min_online_routes,
                    timeout_seconds=args.route_inventory_timeout_seconds,
                ),
            ),
            timeout_seconds=(
                args.route_inventory_timeout_seconds
                + SMOKES_CONFIG.virtual_api_step_route_inventory_extra_seconds
            ),
        )
        print(
            "checkpoint 4 details: "
            f"vn_a_routes={route_inventory[0]['online_route_count']} "
            f"vn_b_routes={route_inventory[1]['online_route_count']}",
            flush=True,
        )

        session_a_to_b = await run_async_step(
            "checkpoint 5a: start local VN session A->B through standard DRT path",
            api.post(
                "/v1/sessions/virtual",
                {
                    "local_virtual_node_id": vn_a["id"],
                    "remote_virtual_node_id": vn_b["id"],
                },
            ),
            timeout_seconds=SMOKES_CONFIG.virtual_api_local_session_timeout_seconds,
        )
        session_b_to_a = await run_async_step(
            "checkpoint 5b: start local VN session B->A through standard DRT path",
            api.post(
                "/v1/sessions/virtual",
                {
                    "local_virtual_node_id": vn_b["id"],
                    "remote_virtual_node_id": vn_a["id"],
                },
            ),
            timeout_seconds=SMOKES_CONFIG.virtual_api_local_session_timeout_seconds,
        )
        print(
            "checkpoint 5 details: "
            f"a_to_b={session_a_to_b['session_id']} b_to_a={session_b_to_a['session_id']}",
            flush=True,
        )

        await run_async_step(
            "checkpoint 6a: subscribe local API virtual inbox",
            api.post("/v1/messages/virtual/subscribe", {"app_message_type": APP_MESSAGE_TYPE}),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_subscribe_timeout_seconds,
        )
        await run_async_step(
            "checkpoint 6b: concurrent local VN message stress",
            exercise_concurrent_local_messages(
                api=api,
                session_a_to_b=str(session_a_to_b["session_id"]),
                session_b_to_a=str(session_b_to_a["session_id"]),
                rounds=args.message_rounds,
                concurrency=args.message_concurrency,
                random_source=random_source,
            ),
            timeout_seconds=max(
                SMOKES_CONFIG.virtual_api_local_message_timeout_min_seconds,
                args.message_rounds * SMOKES_CONFIG.virtual_api_local_message_timeout_per_round_seconds,
            ),
        )

        await run_async_step(
            "checkpoint 7: local VN content loop stress",
            exercise_local_content_loops(
                api=api,
                provider_virtual_node_id=str(vn_a["id"]),
                session_id=str(session_b_to_a["session_id"]),
                downloads=args.content_downloads,
                random_source=random_source,
            ),
            timeout_seconds=max(
                SMOKES_CONFIG.virtual_api_local_content_timeout_min_seconds,
                args.content_downloads * SMOKES_CONFIG.virtual_api_local_content_timeout_per_download_seconds,
            ),
        )

        await run_async_step(
            "checkpoint 8: verify sessions survived stress",
            verify_sessions_active(
                api=api,
                expected_session_ids={
                    str(session_a_to_b["session_id"]),
                    str(session_b_to_a["session_id"]),
                },
            ),
            timeout_seconds=SMOKES_CONFIG.virtual_api_step_identity_timeout_seconds,
        )

        print("OK virtual API local VN stress smoke passed", flush=True)
    finally:
        try:
            await asyncio.wait_for(stop_cores(core), timeout=30.0)
        except TimeoutError:
            print("warning: timed out while stopping local VN API stress core", flush=True)


async def exercise_concurrent_local_messages(
    *,
    api: JsonApiClient,
    session_a_to_b: str,
    session_b_to_a: str,
    rounds: int,
    concurrency: int,
    random_source: random.Random,
) -> None:
    expected_messages: dict[str, dict[str, object]] = {}
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def send_one(index: int) -> None:
        async with semaphore:
            direction = "a_to_b" if random_source.choice([True, False]) else "b_to_a"
            session_id = session_a_to_b if direction == "a_to_b" else session_b_to_a
            payload = {
                "direction": direction,
                "sequence": index,
                "body": random_text(random_source, min_size=8, max_size=2048),
                "nonce": random_source.getrandbits(96),
            }
            result = await api.post(
                f"/v1/sessions/virtual/{quote(session_id, safe='')}/messages",
                {
                    "app_message_type": APP_MESSAGE_TYPE,
                    "payload": payload,
                },
            )
            request_id = str(result["request_id"])
            expected_messages[request_id] = payload
            if (
                index == 1
                or index % SMOKES_CONFIG.virtual_api_local_message_progress_interval == 0
                or index == rounds
            ):
                print(
                    "local message sent: "
                    f"index={index}/{rounds} direction={direction} request_id={request_id}",
                    flush=True,
                )

    await asyncio.gather(*(send_one(index) for index in range(1, rounds + 1)))
    await wait_for_expected_messages(
        api=api,
        expected_messages=expected_messages,
        timeout_seconds=max(
            SMOKES_CONFIG.virtual_api_local_wait_messages_min_seconds,
            rounds * SMOKES_CONFIG.virtual_api_local_wait_messages_per_round_seconds,
        ),
    )


async def wait_for_expected_messages(
    *,
    api: JsonApiClient,
    expected_messages: dict[str, dict[str, object]],
    timeout_seconds: float,
) -> None:
    pending = dict(expected_messages)

    async def collect_messages() -> bool | None:
        messages = await api.get(
            f"/v1/messages/virtual?app_message_type={quote(APP_MESSAGE_TYPE, safe='')}&limit=1000&consume=false"
        )
        for message in messages:
            request_id = message.get("request_id")
            if not isinstance(request_id, str) or request_id not in pending:
                continue

            expected_payload = pending[request_id]
            if message.get("payload") != expected_payload:
                raise RuntimeError(f"Payload mismatch for request_id={request_id}.")
            pending.pop(request_id)

        print(
            "waiting local VN messages: "
            f"received={len(expected_messages) - len(pending)} "
            f"pending={len(pending)} total={len(expected_messages)}",
            flush=True,
        )
        return True if not pending else None

    await wait_until_value(
        collect_messages,
        timeout_seconds=timeout_seconds,
        label="all local VN virtual messages",
    )

    await api.get(
        f"/v1/messages/virtual?app_message_type={quote(APP_MESSAGE_TYPE, safe='')}&limit=1000&consume=true"
    )


async def exercise_local_content_loops(
    *,
    api: JsonApiClient,
    provider_virtual_node_id: str,
    session_id: str,
    downloads: int,
    random_source: random.Random,
) -> None:
    for index in range(1, downloads + 1):
        size_bytes = random_source.randint(
            SMOKES_CONFIG.virtual_api_local_content_min_bytes,
            SMOKES_CONFIG.virtual_api_local_content_max_bytes,
        )
        content_bytes = random_source.randbytes(size_bytes)
        stored = await api.post(
            "/v1/content",
            {
                "data_base64": base64.b64encode(content_bytes).decode("ascii"),
                "title": f"local-vn-stress-{index}",
                "content_type": "application/octet-stream",
                "tags": ["integration", "local-vn-stress"],
            },
        )
        content_id = str(stored["content_id"])
        print(
            "local content stored: "
            f"index={index}/{downloads} content_id={content_id} size_bytes={size_bytes}",
            flush=True,
        )

        publish_result = await api.post(
            f"/v1/content/{quote(content_id, safe='')}/providers/ddt",
            {
                "local_virtual_node_id": provider_virtual_node_id,
                "async_publish": True,
            },
        )
        await wait_for_publish_job_stored(api, str(publish_result["publish_result"]["job_id"]))

        await api.post(
            "/v1/downloads",
            {
                "session_id": session_id,
                "content_id": content_id,
            },
        )
        await wait_for_api_download(
            api,
            session_id=session_id,
            content_id=content_id,
            expected_size=size_bytes,
        )
        print(
            "local content downloaded: "
            f"index={index}/{downloads} content_id={content_id}",
            flush=True,
        )


async def verify_sessions_active(
    *,
    api: JsonApiClient,
    expected_session_ids: set[str],
) -> None:
    sessions = await api.get("/v1/sessions/virtual")
    active_session_ids = {
        str(session.get("session_id"))
        for session in sessions
        if session.get("session_state") == "active"
    }
    missing = expected_session_ids - active_session_ids
    if missing:
        raise RuntimeError(f"Expected sessions are not active after stress: {sorted(missing)}")


def resolve_smoke_ports(args: argparse.Namespace) -> LocalSmokePorts:
    used_ports: set[int] = set()
    core = resolve_local_core_port(
        requested_port=args.core_port,
        default_port=DEFAULT_LOCAL_CORE_PORT,
        used_ports=used_ports,
        label="local-vn-core",
    )
    api = args.api_port or find_free_tcp_port(used_ports)
    return LocalSmokePorts(core=core, api=api)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stress smoke: dois VNs locais no mesmo core se comunicando pela API "
            "usando DRT/route execute, sem atalho local."
        ),
    )
    parser.add_argument("--cluster-nodes", type=int, default=SMOKES_CONFIG.min_cluster_nodes)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    parser.add_argument(
        "--min-online-routes",
        type=int,
        default=SMOKES_CONFIG.virtual_api_min_online_routes,
    )
    parser.add_argument(
        "--route-inventory-timeout-seconds",
        type=float,
        default=SMOKES_CONFIG.virtual_api_local_route_inventory_timeout_seconds,
    )
    parser.add_argument(
        "--route-rtt-ms",
        type=int,
        default=SMOKES_CONFIG.virtual_api_route_expected_round_trip_ttl_ms,
    )
    parser.add_argument(
        "--route-pending-timeout-seconds",
        type=float,
        default=SMOKES_CONFIG.virtual_api_local_route_pending_timeout_seconds,
    )
    parser.add_argument(
        "--message-rounds",
        type=int,
        default=SMOKES_CONFIG.virtual_api_local_message_rounds,
    )
    parser.add_argument(
        "--message-concurrency",
        type=int,
        default=SMOKES_CONFIG.virtual_api_local_message_concurrency,
    )
    parser.add_argument(
        "--content-downloads",
        type=int,
        default=SMOKES_CONFIG.virtual_api_local_content_downloads,
    )
    parser.add_argument("--seed", type=int, default=SMOKES_CONFIG.virtual_api_local_seed)
    parser.add_argument("--run-id", default=f"run-{int(time.time())}")
    parser.add_argument("--core-port", type=int, default=None)
    parser.add_argument("--api-port", type=int, default=None)
    return parser.parse_args()


@dataclass(slots=True, frozen=True)
class LocalSmokePorts:
    core: int
    api: int


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"virtual API local VN stress smoke failed: {error}", file=sys.stderr)
        raise
