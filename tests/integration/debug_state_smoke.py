from __future__ import annotations

import platform
from pathlib import Path
import subprocess
import sys
import time
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.debug_console import DebugNodeRegistry


CLUSTER_NODE_COUNT = 4
DOCKER_FILTER = "anonnet-node-"
STARTUP_TIMEOUT_SECONDS = 120.0
STABILIZATION_SECONDS = 35.0
POLL_SECONDS = 2.0
MAX_ACTIVE_SESSIONS_PER_NODE = 12
MAX_TOTAL_SESSIONS_PER_NODE = 20


def main() -> int:
    try:
        start_cluster(node_count=CLUSTER_NODE_COUNT)
        wait_for_running_containers(expected_count=CLUSTER_NODE_COUNT)
        snapshot = wait_for_stable_debug_snapshot()
        validate_debug_snapshot(snapshot)
        print("debug_state_smoke OK")
        return 0
    finally:
        stop_cluster()


def start_cluster(*, node_count: int) -> None:
    detach_argument = "-Detach" if platform.system().lower() == "windows" else "--detach"
    run_command(
        [
            sys.executable,
            str(PROJECT_ROOT / "cluster" / "up_nodes.py"),
            str(node_count),
            detach_argument,
        ]
    )


def stop_cluster() -> None:
    run_command(
        [
            sys.executable,
            str(PROJECT_ROOT / "cluster" / "down_nodes.py"),
        ],
        allow_failure=True,
    )


def wait_for_running_containers(*, expected_count: int) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        running_count = count_running_containers()
        print(f"waiting cluster containers: running={running_count} expected={expected_count}")
        if running_count == expected_count:
            return
        time.sleep(POLL_SECONDS)
    raise TimeoutError("Timed out waiting for debug smoke cluster containers.")


def wait_for_stable_debug_snapshot() -> dict[str, Any]:
    deadline = time.monotonic() + STABILIZATION_SECONDS
    last_snapshot: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last_snapshot = collect_debug_snapshot()
        print_snapshot_summary(last_snapshot)
        time.sleep(POLL_SECONDS)

    if last_snapshot is None:
        raise RuntimeError("No debug snapshot was collected.")
    return last_snapshot


def collect_debug_snapshot() -> dict[str, Any]:
    registry = DebugNodeRegistry(
        api_urls=[],
        include_docker=True,
        docker_filter=DOCKER_FILTER,
        timeout_seconds=5.0,
    )
    return registry.collect()


def validate_debug_snapshot(snapshot: dict[str, Any]) -> None:
    nodes = snapshot.get("nodes")
    if not isinstance(nodes, list):
        raise AssertionError("Debug snapshot did not return a node list.")

    healthy_nodes = [node for node in nodes if node.get("ok") is True]
    if len(healthy_nodes) != CLUSTER_NODE_COUNT:
        raise AssertionError(f"Expected {CLUSTER_NODE_COUNT} healthy nodes, got {len(healthy_nodes)}.")

    failures: list[str] = []
    for node in healthy_nodes:
        source = str(node.get("source") or "unknown")
        state = node.get("state") if isinstance(node.get("state"), dict) else {}
        failures.extend(validate_node_state(source, state))

    if failures:
        raise AssertionError("\n".join(failures))


def validate_node_state(source: str, state: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    peer_count = int(state.get("peers", {}).get("diagnostics", {}).get("route_ready_nodes") or 0)
    sessions = state.get("sessions", {})
    active_sessions = int(sessions.get("active") or 0)
    total_sessions = int(sessions.get("total") or 0)
    dht = state.get("dht", {})
    dht_rows = int(dht.get("total_rows") or 0)
    dht_unique_keys = int(dht.get("total_unique_keys") or 0)
    duplicate_key_count = int(dht.get("duplicate_key_count") or 0)

    if peer_count > CLUSTER_NODE_COUNT - 1:
        failures.append(f"{source}: route_ready_nodes inflated: {peer_count}")
    if active_sessions > MAX_ACTIVE_SESSIONS_PER_NODE:
        failures.append(f"{source}: too many active sessions: {active_sessions}")
    if total_sessions > MAX_TOTAL_SESSIONS_PER_NODE:
        failures.append(f"{source}: too many total sessions: {total_sessions}")
    if duplicate_key_count:
        failures.append(f"{source}: duplicate DHT keys: {duplicate_key_count}")
    if dht_rows != dht_unique_keys:
        failures.append(f"{source}: DHT rows differ from unique keys: rows={dht_rows} unique={dht_unique_keys}")

    return failures


def print_snapshot_summary(snapshot: dict[str, Any]) -> None:
    nodes = snapshot.get("nodes") if isinstance(snapshot.get("nodes"), list) else []
    healthy_nodes = [node for node in nodes if node.get("ok") is True]
    print(f"debug snapshot: healthy={len(healthy_nodes)}/{len(nodes)}")
    for node in healthy_nodes:
        source = str(node.get("source") or "unknown")
        state = node.get("state") if isinstance(node.get("state"), dict) else {}
        peers = state.get("peers", {}).get("diagnostics", {}).get("route_ready_nodes")
        sessions = state.get("sessions", {})
        dht = state.get("dht", {})
        print(
            "  "
            f"{source}: peers={peers} "
            f"sessions={sessions.get('active')}/{sessions.get('total')} "
            f"dht={dht.get('total_unique_keys')}/{dht.get('total_rows')} "
            f"duplicates={dht.get('duplicate_key_count')}"
        )


def count_running_containers() -> int:
    completed = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"name={DOCKER_FILTER}",
            "--format",
            "{{.Names}}",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return len([line for line in completed.stdout.splitlines() if line.strip()])


def run_command(command: list[str], *, allow_failure: bool = False) -> None:
    completed = subprocess.run(command, cwd=PROJECT_ROOT)
    if completed.returncode != 0 and not allow_failure:
        raise subprocess.CalledProcessError(completed.returncode, command)


if __name__ == "__main__":
    raise SystemExit(main())
