from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
INTEGRATION_ROOT = PROJECT_ROOT / "tests" / "integration"
for path in (APP_ROOT, INTEGRATION_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from core import CoreConfig, CoreEngine, EngineServices
from dht import DhtService
from identity import IdentityService
from route import RouteService
from storage import DatabaseConfig, DatabaseManager
from smoke_helpers import (
    create_local_virtual_node,
    resolve_cluster_node_count,
    resolve_required_ready_nodes,
    start_cluster,
    wait_for_drt_entry,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_network_ready,
    wait_for_runtime_route_active,
)


TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "poc-social-js-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
SOCIAL_SMOKE_SCRIPT = PROJECT_ROOT / "poc" / "smokes" / "social_flow.js"


async def main() -> int:
    args = parse_args()
    cluster_nodes = resolve_cluster_node_count(args.cluster_nodes)
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )

    reset_data_dir(TEST_DATA_ROOT)
    if not args.skip_cluster:
        start_cluster(node_count=cluster_nodes)
        wait_for_cluster_containers(expected_count=cluster_nodes)

    core_a = create_api_core(
        data_dir=TEST_DATA_ROOT / "core-a",
        listen_port=19601,
        api_port=18180,
        websocket_port=18181,
        log_dir=TEST_LOG_ROOT / "core-a",
    )
    core_b = create_api_core(
        data_dir=TEST_DATA_ROOT / "core-b",
        listen_port=19602,
        api_port=18280,
        websocket_port=18281,
        log_dir=TEST_LOG_ROOT / "core-b",
    )

    try:
        await asyncio.gather(core_a.start(), core_b.start())
        print("checkpoint 0 OK: API cores A/B started")

        await wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes)
        await wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes)
        await wait_for_cluster_network_maturity(
            core_a,
            core_b,
            required_ready_nodes=required_ready_nodes,
        )
        print("checkpoint 0 OK: physical network ready for social smoke")

        vn_a = create_local_virtual_node(
            core_a,
            kind="social",
            metadata_source="poc_social_js_smoke",
        )
        vn_b = create_local_virtual_node(
            core_b,
            kind="social",
            metadata_source="poc_social_js_smoke",
        )
        print(f"checkpoint 1 OK: social VNs prepared by core runtime: A={vn_a.id} B={vn_b.id}")

        active_route = await wait_for_runtime_route_active(core_a, local_virtual_node_id=vn_a.id)
        await wait_for_drt_entry(
            core_b,
            virtual_node_public_key=vn_a.public_key,
            expected_final_path_id=active_route.final_path_id,
        )
        print("checkpoint 2 OK: VN A route is active and visible from core B DRT")

        await run_js_smoke(vn_a=vn_a, vn_b=vn_b)
        return 0
    finally:
        await stop_core(core_b)
        await stop_core(core_a)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o smoke JS da PoC social com dois cores reais.",
    )
    parser.add_argument("cluster_nodes", type=int, nargs="?", default=8)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    parser.add_argument("--skip-cluster", action="store_true")
    return parser.parse_args()


def create_api_core(
    *,
    data_dir: Path,
    listen_port: int,
    api_port: int,
    websocket_port: int,
    log_dir: Path,
) -> CoreEngine:
    data_dir.mkdir(parents=True, exist_ok=True)
    database = DatabaseManager(DatabaseConfig(db_path=data_dir / "anonnetcore.db"))
    config = CoreConfig(
        listen_host="0.0.0.0",
        listen_port=listen_port,
        log_dir=log_dir,
    )
    config.api_enabled = True
    config.api_host = "127.0.0.1"
    config.api_port = api_port
    config.api_websocket_enabled = True
    config.api_websocket_host = "127.0.0.1"
    config.api_websocket_port = websocket_port
    config.content_storage_dir = data_dir / "content"
    config.virtual_route_maintenance_runtime_interval_seconds = 1.0
    config.virtual_route_maintenance_expected_round_trip_ttl_ms = 2000
    config.virtual_route_maintenance_candidate_limit = 16

    services = EngineServices(
        config=config,
        database=database,
        dht_service=DhtService(config=config, database=database),
        identity_service=IdentityService(database=database),
        route_service=RouteService(database=database),
    )
    return CoreEngine(services=services)


async def run_js_smoke(*, vn_a, vn_b) -> None:
    env = os.environ.copy()
    env["CORE_A_HTTP"] = "http://127.0.0.1:18180"
    env["CORE_B_HTTP"] = "http://127.0.0.1:18280"
    env["CORE_A_VN_ID"] = vn_a.id
    env["CORE_A_VN_PUBLIC_KEY"] = vn_a.public_key
    env["CORE_B_VN_ID"] = vn_b.id
    env["CORE_B_VN_PUBLIC_KEY"] = vn_b.public_key
    process = await asyncio.create_subprocess_exec(
        "node",
        str(SOCIAL_SMOKE_SCRIPT),
        cwd=PROJECT_ROOT,
        env=env,
    )
    exit_code = await process.wait()
    if exit_code != 0:
        raise RuntimeError(f"social JS smoke failed with exit code {exit_code}")


def reset_data_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


async def stop_core(engine: CoreEngine) -> None:
    try:
        await engine.stop()
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
