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
from smokes_config import SMOKES_CONFIG


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
        listen_port=SMOKES_CONFIG.social_core_listen_port,
        api_port=SMOKES_CONFIG.social_api_port,
        websocket_port=SMOKES_CONFIG.social_websocket_port,
        log_dir=TEST_LOG_ROOT / "core-a",
    )
    try:
        await core_a.start()
        print("checkpoint 0 OK: API core A started")

        await wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes)
        await wait_for_cluster_network_maturity(
            core_a,
            required_ready_nodes=required_ready_nodes,
        )
        print("checkpoint 0 OK: physical network ready for social smoke")

        local_vn_a = create_local_virtual_node(
            core_a,
            kind="social",
            metadata_source="poc_social_js_smoke_same_core",
        )
        local_vn_b = create_local_virtual_node(
            core_a,
            kind="social",
            metadata_source="poc_social_js_smoke_same_core",
        )
        print(
            "checkpoint 1 OK: social VNs prepared by core runtime: "
            f"localA={local_vn_a.id} localB={local_vn_b.id}"
        )

        active_route_local_a_task = asyncio.create_task(
            wait_for_runtime_route_active(
                core_a,
                local_virtual_node_id=local_vn_a.id,
                timeout_seconds=SMOKES_CONFIG.social_route_active_timeout_seconds,
            )
        )
        active_route_local_b_task = asyncio.create_task(
            wait_for_runtime_route_active(
                core_a,
                local_virtual_node_id=local_vn_b.id,
                timeout_seconds=SMOKES_CONFIG.social_route_active_timeout_seconds,
            )
        )
        active_route_local_a, active_route_local_b = await asyncio.gather(
            active_route_local_a_task,
            active_route_local_b_task,
        )
        print("checkpoint 2 OK: same-core runtime routes are active")

        await asyncio.gather(
            wait_for_drt_entry(
                core_a,
                virtual_node_public_key=local_vn_a.public_key,
                expected_final_path_id=active_route_local_a.final_path_id,
            ),
            wait_for_drt_entry(
                core_a,
                virtual_node_public_key=local_vn_b.public_key,
                expected_final_path_id=active_route_local_b.final_path_id,
            ),
        )
        print("checkpoint 3 OK: same-core routes are visible through DRT")

        await run_js_smoke(
            local_vn_a=local_vn_a,
            local_vn_b=local_vn_b,
        )
        return 0
    finally:
        await stop_core(core_a)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o smoke JS da PoC social com dois cores reais.",
    )
    parser.add_argument("cluster_nodes", type=int, nargs="?", default=SMOKES_CONFIG.social_cluster_nodes)
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
        physical_listen_host="0.0.0.0",
        physical_tcp_listen_port=listen_port,
        log_dir=log_dir,
    )
    config.api_enabled = True
    config.api_host = "127.0.0.1"
    config.api_port = api_port
    config.api_websocket_enabled = True
    config.api_websocket_port = websocket_port
    config.content_storage_dir = data_dir / "content"
    config.virtual_route_maintenance_interval_seconds = (
        SMOKES_CONFIG.test_core_route_runtime_interval_seconds
    )
    config.virtual_route_build_timeout_seconds = (
        SMOKES_CONFIG.test_core_route_pending_timeout_seconds
    )
    config.default_random_walk_ttl_ms = (
        SMOKES_CONFIG.test_core_route_expected_round_trip_ttl_ms
    )
    config.random_walk_candidate_limit = SMOKES_CONFIG.test_core_route_candidate_limit

    services = EngineServices(
        config=config,
        database=database,
        dht_service=DhtService(config=config, database=database),
        identity_service=IdentityService(database=database),
        route_service=RouteService(database=database),
    )
    return CoreEngine(services=services)


async def run_js_smoke(*, local_vn_a, local_vn_b) -> None:
    env = os.environ.copy()
    env["SOCIAL_SMOKE_MODE"] = "same-core"
    env["CORE_A_HTTP"] = f"http://127.0.0.1:{SMOKES_CONFIG.social_api_port}"
    env["CORE_A_LOCAL_VN_A_ID"] = local_vn_a.id
    env["CORE_A_LOCAL_VN_A_PUBLIC_KEY"] = local_vn_a.public_key
    env["CORE_A_LOCAL_VN_B_ID"] = local_vn_b.id
    env["CORE_A_LOCAL_VN_B_PUBLIC_KEY"] = local_vn_b.public_key
    js_log_path = TEST_LOG_ROOT / "social-flow-js.log"
    js_log_path.parent.mkdir(parents=True, exist_ok=True)
    with js_log_path.open("w", encoding="utf-8") as js_log:
        process = await asyncio.create_subprocess_exec(
            "node",
            str(SOCIAL_SMOKE_SCRIPT),
            cwd=PROJECT_ROOT,
            env=env,
            stdout=js_log,
            stderr=subprocess.STDOUT,
        )
        exit_code = await process.wait()

    if exit_code != 0:
        print(f"social JS smoke log: {js_log_path}")
        print(_tail_text(js_log_path, line_count=80))
        raise RuntimeError(f"social JS smoke failed with exit code {exit_code}")

    print(f"social JS smoke log: {js_log_path}")
    print("checkpoint 4 OK: social JS smoke passed")


def reset_data_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _tail_text(path: Path, *, line_count: int) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


async def stop_core(engine: CoreEngine) -> None:
    try:
        await engine.stop()
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
