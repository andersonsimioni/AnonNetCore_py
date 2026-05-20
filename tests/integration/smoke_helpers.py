from __future__ import annotations

import asyncio
from datetime import datetime, timezone
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
from dht import DrtRecordPayload, parse_record
from identity import VirtualNodeIdentityCreateInput
from sessions import VirtualSessionMessageReply

from core_helpers import create_isolated_core


MIN_CLUSTER_NODES = 8
DEFAULT_READY_CLUSTER_RATIO = 0.6
NETWORK_READY_TIMEOUT_SECONDS = 180.0
FORCED_EXCHANGE_INTERVAL_SECONDS = 15.0
CLUSTER_NETWORK_MATURITY_SECONDS = 25.0
CLUSTER_NETWORK_MATURITY_TICK_SECONDS = 2.0
CLUSTER_NETWORK_MATURITY_STABLE_TICKS = 3


def resolve_required_ready_nodes(
    *,
    cluster_nodes: int,
    minimum_remote_nodes: int | None,
) -> int:
    if minimum_remote_nodes is not None:
        return max(1, minimum_remote_nodes)

    return max(1, math.ceil(cluster_nodes * DEFAULT_READY_CLUSTER_RATIO))


def resolve_cluster_node_count(node_count: int) -> int:
    return max(node_count, MIN_CLUSTER_NODES)


def reset_cluster() -> None:
    command = [
        sys.executable,
        str(PROJECT_ROOT / "cluster" / "down_nodes.py"),
    ]
    print("resetting docker cluster")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def start_cluster(*, node_count: int) -> None:
    node_count = resolve_cluster_node_count(node_count)
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


async def wait_for_cluster_network_maturity(
    *engines,
    required_ready_nodes: int | None = None,
    warmup_seconds: float = CLUSTER_NETWORK_MATURITY_SECONDS,
    tick_seconds: float = CLUSTER_NETWORK_MATURITY_TICK_SECONDS,
) -> None:
    if not engines:
        return

    minimum_ready_nodes = max(1, required_ready_nodes or 1)
    stable_ticks = 0
    deadline = asyncio.get_running_loop().time() + warmup_seconds
    while asyncio.get_running_loop().time() < deadline:
        snapshots = await asyncio.gather(
            *(_refresh_engine_network_context(engine) for engine in engines),
            return_exceptions=True,
        )
        ready_counts = [
            snapshot.ready_route_candidates
            for snapshot in snapshots
            if isinstance(snapshot, NetworkMaturitySnapshot)
        ]
        network_is_ready = (
            len(ready_counts) == len(engines)
            and all(count >= minimum_ready_nodes for count in ready_counts)
        )
        stable_ticks = stable_ticks + 1 if network_is_ready else 0

        print(
            "waiting cluster network maturity: "
            f"ready_route_candidates={ready_counts} "
            f"required={minimum_ready_nodes} "
            f"stable_ticks={stable_ticks}/{CLUSTER_NETWORK_MATURITY_STABLE_TICKS} "
            f"remaining_seconds={max(0.0, deadline - asyncio.get_running_loop().time()):.1f}"
        )

        if stable_ticks >= CLUSTER_NETWORK_MATURITY_STABLE_TICKS:
            return

        await asyncio.sleep(tick_seconds)

    raise TimeoutError("Timed out waiting for cluster network maturity.")


class NetworkMaturitySnapshot:
    def __init__(self, *, ready_route_candidates: int) -> None:
        self.ready_route_candidates = ready_route_candidates


async def _refresh_engine_network_context(engine) -> NetworkMaturitySnapshot:
    await request_known_nodes_from_active_peers(engine)
    await refresh_route_candidate_rtts(engine)
    candidates = engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
        limit=32,
    )
    return NetworkMaturitySnapshot(ready_route_candidates=len(candidates))


async def request_known_nodes_from_active_peers(engine) -> None:
    exchange_candidates = engine.services.identity_service.list_remote_physical_nodes_for_info_exchange(
        limit=16,
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
    route_candidates = engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
        limit=32,
    )
    route_candidate_ids = {candidate.node_id for candidate in route_candidates}
    missing_rtt_candidates = [
        candidate
        for candidate in ping_candidates
        if candidate.node_id not in route_candidate_ids
    ]
    semaphore = asyncio.Semaphore(8)

    async def refresh_candidate(candidate) -> None:
        try:
            async with semaphore:
                ping_result = await engine.services.protocol_clients.physical.ping.ping_physical_node(
                    remote_physical_node_id=candidate.node_id,
                )
        except Exception:
            return

        observed_rtt_ms = ping_result.get("observed_rtt_ms")
        if isinstance(observed_rtt_ms, (int, float)):
            engine.services.identity_service.upsert_rtt_info(
                remote_physical_node_id=candidate.node_id,
                observed_rtt_ms=float(observed_rtt_ms),
            )

    await asyncio.gather(*(refresh_candidate(candidate) for candidate in missing_rtt_candidates))


async def wait_for_runtime_route_active(
    engine,
    *,
    local_virtual_node_id: str,
    timeout_seconds: float = 120.0,
):
    async def load_active_route():
        return engine.services.route_service.get_active_initiator_resolution_for_local_virtual_node(
            local_virtual_node_id=local_virtual_node_id,
        )

    return await wait_until_value(
        load_active_route,
        timeout_seconds=timeout_seconds,
        label="runtime-created route active",
    )


async def wait_for_drt_entry(
    engine,
    *,
    virtual_node_public_key: str,
    expected_final_path_id: str | None = None,
    timeout_seconds: float = 60.0,
) -> dict[str, object]:
    logical_key = sha512_hex(virtual_node_public_key.encode("utf-8"))

    async def load_drt_entry():
        result = await engine.services.protocol_clients.physical.dht.query(
            namespace="drt",
            logical_key=logical_key,
        )
        if result.get("status") != "found":
            return None
        if expected_final_path_id is None:
            return result

        record_json = result.get("record_json")
        if not isinstance(record_json, str) or not record_json:
            return None
        try:
            record = parse_record("drt", record_json)
        except Exception:
            return None
        if not isinstance(record, DrtRecordPayload):
            return None

        has_expected_route = any(
            entry.final_path_id == expected_final_path_id
            for entry in record.route_entries
        )
        if has_expected_route:
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
    timeout_seconds: float = 70.0,
) -> None:
    observed_after = datetime.now(timezone.utc)

    async def has_keepalive_ack() -> bool:
        session = engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            return False

        if session.last_keepalive_sent_at is not None:
            return session.last_activity_at > session.last_keepalive_sent_at

        return session.last_activity_at > observed_after

    await wait_until(has_keepalive_ack, timeout_seconds=timeout_seconds, label="virtual keepalive ack")


async def validate_virtual_message_roundtrip(
    *,
    sender_engine,
    receiver_engine,
    session_id: str,
    app_message_type: str = "integration.virtual.message",
    reply_message_type: str = "integration.virtual.message.reply",
    payload: dict[str, object] | None = None,
    timeout_seconds: float = 20.0,
) -> None:
    """Valida entrega e resposta via VIRTUAL_SESSION_DATA sobre sessao virtual ativa."""

    message_payload = payload or {"value": "hello-virtual-message"}
    loop = asyncio.get_running_loop()
    received_request = loop.create_future()
    received_reply = loop.create_future()

    def handle_request(message):
        if message.session_id != session_id:
            return None
        if not received_request.done():
            received_request.set_result(message)

        return VirtualSessionMessageReply(
            app_message_type=reply_message_type,
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

    receiver_engine.services.session_manager.register_virtual_message_handler(
        app_message_type,
        handle_request,
    )
    sender_engine.services.session_manager.register_virtual_message_handler(
        reply_message_type,
        handle_reply,
    )

    request_id = await sender_engine.services.protocol_clients.virtual.session.send_message(
        session_id=session_id,
        app_message_type=app_message_type,
        payload=message_payload,
    )

    await wait_until(
        lambda: _future_done(received_request),
        timeout_seconds=timeout_seconds,
        label="virtual message request delivered",
    )
    await wait_until(
        lambda: _future_done(received_reply),
        timeout_seconds=timeout_seconds,
        label="virtual message reply delivered",
    )

    request_message = received_request.result()
    reply_message = received_reply.result()
    if request_message.request_id != request_id:
        raise RuntimeError("Virtual message request_id mismatch on receiver.")
    if reply_message.request_id != request_id:
        raise RuntimeError("Virtual message reply request_id mismatch.")
    if reply_message.payload.get("echo") != message_payload:
        raise RuntimeError("Virtual message reply payload mismatch.")


async def _future_done(future: asyncio.Future) -> bool:
    return future.done()


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
