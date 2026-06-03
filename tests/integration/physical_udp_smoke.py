from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


from common import utc_now  # noqa: E402
from core.network import detect_local_network_host  # noqa: E402
from core.protocols.physical import PhysicalNodeInfoProtocolHandler  # noqa: E402
from core_helpers import create_isolated_core, reset_core_data_dir, stop_cores  # noqa: E402
from dht import DpntRecordPayload, parse_record, serialize_record  # noqa: E402
from smoke_helpers import (  # noqa: E402
    reset_cluster,
    resolve_required_ready_nodes,
    resolve_cluster_node_count,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_network_ready,
    wait_until,
    wait_until_value,
)
from smokes_config import SMOKES_CONFIG  # noqa: E402


DATA_DIR = PROJECT_ROOT / "data" / "local" / "integration" / "physical-udp-smoke"
LOG_DIR = DATA_DIR / "logs"
LISTEN_HOST = "0.0.0.0"
LOCAL_CONNECT_HOST = "127.0.0.1"
CORE_A_TCP_PORT = SMOKES_CONFIG.physical_udp_core_a_tcp_port
CORE_B_TCP_PORT = SMOKES_CONFIG.physical_udp_core_b_tcp_port
CORE_A_UDP_PORT = SMOKES_CONFIG.physical_udp_core_a_udp_port
CORE_B_UDP_PORT = SMOKES_CONFIG.physical_udp_core_b_udp_port
UDP_CHUNK_PAYLOAD_SIZE = SMOKES_CONFIG.physical_udp_chunk_payload_size
RANDOM_SEED = SMOKES_CONFIG.physical_udp_seed
MAX_STRESS_PAYLOAD_SIZE = SMOKES_CONFIG.physical_udp_max_stress_payload_size


def main() -> None:
    asyncio.run(_run(parse_args()))
    print("physical udp smoke OK")


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

    previous_tcp_host = os.environ.get("ANONNET_ADVERTISED_TCP_HOST")
    previous_udp_host = os.environ.get("ANONNET_ADVERTISED_UDP_HOST")
    os.environ["ANONNET_ADVERTISED_TCP_HOST"] = advertised_host
    os.environ["ANONNET_ADVERTISED_UDP_HOST"] = advertised_host

    core_a = create_isolated_core(
        data_dir=DATA_DIR / "core-a",
        listen_host=LISTEN_HOST,
        listen_port=CORE_A_TCP_PORT,
        udp_enabled=True,
        physical_udp_listen_port=CORE_A_UDP_PORT,
        log_dir=LOG_DIR / "core-a",
    )
    udp_only_b = create_isolated_core(
        data_dir=DATA_DIR / "udp-only-b",
        listen_host=LISTEN_HOST,
        listen_port=CORE_B_TCP_PORT,
        tcp_transport_enabled=False,
        udp_enabled=True,
        physical_udp_listen_port=CORE_B_UDP_PORT,
        log_dir=LOG_DIR / "udp-only-b",
    )
    _tune_udp_for_fragmentation(core_a)
    _tune_udp_for_fragmentation(udp_only_b)

    try:
        await asyncio.gather(core_a.start(), udp_only_b.start())
        node_a = _require_local_node(core_a, "core-a")
        node_b = _require_local_node(udp_only_b, "udp-only-b")
        print(
            "udp smoke local cores started: "
            f"advertised_host={advertised_host} "
            f"core_a_node_id={node_a.id} udp_only_b_node_id={node_b.id} "
            f"core_a_tcp={CORE_A_TCP_PORT} core_a_udp={CORE_A_UDP_PORT} "
            f"udp_only_b_tcp={CORE_B_TCP_PORT} udp_only_b_udp={CORE_B_UDP_PORT}"
        )

        await _assert_tcp_listener_is_closed(CORE_B_TCP_PORT)
        await asyncio.gather(
            wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes),
            wait_for_network_ready(udp_only_b, minimum_remote_nodes=required_ready_nodes),
        )
        await wait_for_cluster_network_maturity(
            core_a,
            udp_only_b,
            required_ready_nodes=required_ready_nodes,
        )
        print(f"udp smoke cores joined docker cluster: required_ready_nodes={required_ready_nodes}")

        await _pause_peer_autodiscovery(core_a, udp_only_b)
        _close_physical_sessions_between(core_a, node_b.id)
        _close_physical_sessions_between(udp_only_b, node_a.id)
        print("udp smoke isolated target peers before UDP-only session")

        await _publish_udp_only_dpnt(udp_only_b, node_b)
        dpnt_result = await _wait_for_udp_only_dpnt(core_a, node_b.id)
        _assert_dpnt_has_only_udp_endpoint(dpnt_result)
        print("core A resolved UDP-only peer from DPNT")

        session_id = await core_a.services.protocol_clients.physical.session.start_session(
            remote_physical_node_id=node_b.id,
        )
        session = core_a.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active":
            raise RuntimeError("UDP physical session was not activated on initiator.")
        if session.transport != "udp":
            raise RuntimeError(f"Expected UDP session on initiator, got {session.transport}.")

        inbound_session_id = await _wait_for_active_udp_session(udp_only_b, node_a.id)
        await _run_fragmentation_stress(
            label="A-to-B",
            engine=core_a,
            session_id=session_id,
            max_payload_size=args.max_payload_size,
        )
        await _run_fragmentation_stress(
            label="B-to-A",
            engine=udp_only_b,
            session_id=inbound_session_id,
            max_payload_size=args.max_payload_size,
        )
        await _run_concurrent_fragmentation_stress(
            core_a=core_a,
            core_a_session_id=session_id,
            udp_only_b=udp_only_b,
            udp_only_b_session_id=inbound_session_id,
        )
        await core_a.services.protocol_clients.physical.session.send_keepalive(session_id=session_id)
        await _wait_for_keepalive_ack(core_a, session_id)
        print("UDP-only peer handled fragmented reliable traffic and keepalive")
    finally:
        await stop_cores(udp_only_b, core_a)
        _restore_env("ANONNET_ADVERTISED_TCP_HOST", previous_tcp_host)
        _restore_env("ANONNET_ADVERTISED_UDP_HOST", previous_udp_host)


def _tune_udp_for_fragmentation(engine) -> None:
    engine.services.config.udp_chunk_payload_size = UDP_CHUNK_PAYLOAD_SIZE
    engine.services.config.udp_max_datagram_size = SMOKES_CONFIG.physical_udp_datagram_size
    engine.services.config.udp_reassembly_timeout_seconds = (
        SMOKES_CONFIG.physical_udp_reassembly_timeout_seconds
    )
    engine.services.config.udp_max_frame_size = SMOKES_CONFIG.physical_udp_max_frame_size


def _restore_env(name: str, previous_value: str | None) -> None:
    if previous_value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = previous_value


def _require_local_node(engine, name: str):
    local_node = engine.services.identity_service.get_local_physical_node_result()
    if local_node is None:
        raise RuntimeError(f"{name} local physical identity was not initialized.")
    return local_node


async def _assert_tcp_listener_is_closed(port: int) -> None:
    try:
        reader, writer = await asyncio.open_connection(LOCAL_CONNECT_HOST, port)
    except OSError:
        return

    writer.close()
    await writer.wait_closed()
    raise RuntimeError("UDP-only peer unexpectedly accepted a direct TCP connection.")


async def _pause_peer_autodiscovery(*engines) -> None:
    for engine in engines:
        runtimes = engine.services.runtime_services
        if runtimes is None:
            continue
        await runtimes.physical_node_validation.stop()
        await runtimes.physical_node_info_exchange.stop()
        await runtimes.physical_ping.stop()


def _close_physical_sessions_between(engine, remote_physical_node_id: str) -> None:
    for session in engine.services.session_manager.list_sessions(session_scope="physical"):
        if session.remote_identity_id != remote_physical_node_id:
            continue
        engine.services.session_manager.close_session(
            session.session_id,
            close_reason="physical_udp_smoke_reset_target_session",
        )


async def _publish_udp_only_dpnt(engine, local_node) -> None:
    endpoints = engine.build_local_physical_endpoints()
    _assert_only_udp_endpoints(endpoints)
    protocol_version = "1"
    signature = PhysicalNodeInfoProtocolHandler.sign_dpnt_descriptor(
        physical_node_public_key=local_node.public_key,
        endpoints=endpoints,
        reachability_class="direct",
        relay_capable=False,
        hole_punch_capable=False,
        protocol_version=protocol_version,
        feature_flags=["udp"],
        status=local_node.status,
        private_key_pem=local_node.private_key_pem,
    )
    record_json = serialize_record(
        DpntRecordPayload(
            pk_physical_node=local_node.public_key,
            endpoints=endpoints,
            transport_methods=["udp"],
            reachability_class="direct",
            relay_capable=False,
            hole_punch_capable=False,
            protocol_version=protocol_version,
            feature_flags=["udp"],
            last_validated_at=utc_now().isoformat(),
            status=local_node.status,
            signature=signature,
        )
    )
    result = await engine.services.protocol_clients.physical.dht.publish(
        namespace="dpnt",
        logical_key=local_node.id,
        record_json=record_json,
    )
    if result.get("status") != "stored":
        raise RuntimeError(f"UDP-only DPNT publish failed: {result!r}")


async def _wait_for_udp_only_dpnt(engine, physical_node_id: str) -> dict[str, object]:
    async def query_dpnt():
        result = await engine.services.protocol_clients.physical.dht.query(
            namespace="dpnt",
            logical_key=physical_node_id,
        )
        if result.get("status") != "found":
            return None
        return result

    return await wait_until_value(
        query_dpnt,
        timeout_seconds=SMOKES_CONFIG.physical_udp_dpnt_timeout_seconds,
        label="UDP-only DPNT record",
    )


def _assert_dpnt_has_only_udp_endpoint(result: dict[str, object]) -> None:
    record_json = result.get("record_json")
    if not isinstance(record_json, str):
        raise RuntimeError("DPNT query returned no record_json.")
    record = parse_record("dpnt", record_json)
    if not isinstance(record, DpntRecordPayload):
        raise RuntimeError("DPNT query did not return a DPNT record.")
    _assert_only_udp_endpoints(record.endpoints)


def _assert_only_udp_endpoints(endpoints: list[dict[str, object]]) -> None:
    if not endpoints:
        raise RuntimeError("Expected at least one UDP endpoint.")
    transports = {endpoint.get("transport") for endpoint in endpoints}
    if transports != {"udp"}:
        raise RuntimeError(f"Expected UDP-only endpoints, got {endpoints!r}.")


async def _wait_for_active_udp_session(engine, remote_physical_node_id: str) -> str:
    async def load_udp_session_id() -> str | None:
        for session in engine.services.session_manager.list_sessions(session_scope="physical"):
            if (
                session.remote_identity_id == remote_physical_node_id
                and session.session_state == "active"
                and session.transport == "udp"
            ):
                return session.session_id
        return None

    return await wait_until_value(
        load_udp_session_id,
        timeout_seconds=SMOKES_CONFIG.physical_udp_session_timeout_seconds,
        label="inbound UDP physical session",
    )


async def _run_fragmentation_stress(
    *,
    label: str,
    engine,
    session_id: str,
    max_payload_size: int,
) -> None:
    random_source = random.Random(RANDOM_SEED)
    payload_sizes = _build_stress_payload_sizes(random_source, max_payload_size)
    for index, size in enumerate(payload_sizes, start=1):
        await _send_udp_stress_payload(
            engine=engine,
            session_id=session_id,
            label=label,
            index=index,
            size=size,
        )
        await _wait_for_reliable_pending_empty(engine, session_id)
        print(f"udp fragmentation payload delivered: label={label} index={index} size={size}")


async def _run_concurrent_fragmentation_stress(
    *,
    core_a,
    core_a_session_id: str,
    udp_only_b,
    udp_only_b_session_id: str,
) -> None:
    batch_sizes = SMOKES_CONFIG.physical_udp_concurrent_batch_sizes
    tasks = []
    for index, size in enumerate(batch_sizes, start=1):
        tasks.append(
            _send_udp_stress_payload(
                engine=core_a,
                session_id=core_a_session_id,
                label="A-to-B-concurrent",
                index=index,
                size=size,
            )
        )
        tasks.append(
            _send_udp_stress_payload(
                engine=udp_only_b,
                session_id=udp_only_b_session_id,
                label="B-to-A-concurrent",
                index=index,
                size=size,
            )
        )

    await asyncio.gather(*tasks)
    await asyncio.gather(
        _wait_for_reliable_pending_empty(core_a, core_a_session_id),
        _wait_for_reliable_pending_empty(udp_only_b, udp_only_b_session_id),
    )
    print(f"udp concurrent fragmentation batch delivered: messages={len(tasks)}")


def _build_stress_payload_sizes(random_source: random.Random, max_payload_size: int) -> list[int]:
    boundary_sizes = [
        *SMOKES_CONFIG.physical_udp_boundary_payload_sizes,
        UDP_CHUNK_PAYLOAD_SIZE - 1,
        UDP_CHUNK_PAYLOAD_SIZE,
        UDP_CHUNK_PAYLOAD_SIZE + 1,
        max_payload_size,
    ]
    random_sizes = [
        random_source.randint(SMOKES_CONFIG.physical_udp_random_payload_min_size, max_payload_size)
        for _ in range(SMOKES_CONFIG.physical_udp_random_payload_count)
    ]
    return sorted({size for size in [*boundary_sizes, *random_sizes] if 0 < size <= max_payload_size})


async def _send_udp_stress_payload(
    *,
    engine,
    session_id: str,
    label: str,
    index: int,
    size: int,
) -> None:
    await engine.services.protocol_clients.physical.session.send_reliable_protocol_message(
        session_id=session_id,
        inner_message_type="UDP_FRAGMENTATION_STRESS",
        inner_payload={
            "label": label,
            "index": index,
            "size": size,
            "body": _build_repeating_payload(size),
        },
    )


def _build_repeating_payload(size: int) -> str:
    pattern = "anonnet-udp-fragmentation-stress|"
    repeats = (size // len(pattern)) + 1
    return (pattern * repeats)[:size]


async def _wait_for_reliable_pending_empty(engine, session_id: str) -> None:
    async def is_empty() -> bool:
        return engine.services.session_manager.count_pending_reliable_outbound(session_id) == 0

    await wait_until(
        is_empty,
        timeout_seconds=SMOKES_CONFIG.physical_udp_reliable_ack_timeout_seconds,
        label="UDP reliable ACK",
    )


async def _wait_for_keepalive_ack(engine, session_id: str) -> None:
    session = engine.services.session_manager.get_session_by_session_id(session_id)
    if session is None:
        raise RuntimeError("Session disappeared before keepalive.")
    observed_activity = session.last_activity_at

    async def has_new_activity() -> bool:
        current = engine.services.session_manager.get_session_by_session_id(session_id)
        return current is not None and current.last_activity_at > observed_activity

    await wait_until(
        has_new_activity,
        timeout_seconds=SMOKES_CONFIG.physical_udp_keepalive_ack_timeout_seconds,
        label="UDP keepalive ack",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real UDP smoke: cluster Docker, UDP-only peer, fragmented reliable traffic.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=SMOKES_CONFIG.min_cluster_nodes)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    parser.add_argument("--max-payload-size", type=int, default=MAX_STRESS_PAYLOAD_SIZE)
    return parser.parse_args()


if __name__ == "__main__":
    main()
