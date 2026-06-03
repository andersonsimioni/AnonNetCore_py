from __future__ import annotations

import asyncio
import json
import random
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


from common import utc_now  # noqa: E402
from core.models import PacketContext, ProtocolEnvelope  # noqa: E402
from core.runtime.session_runtime import SessionRuntime  # noqa: E402
from core_helpers import create_isolated_core, reset_core_data_dir, stop_cores  # noqa: E402
from sessions import SessionCreateInput, SessionManager  # noqa: E402
from smoke_helpers import create_local_virtual_node, validate_virtual_message_roundtrip  # noqa: E402
from smokes_config import SMOKES_CONFIG  # noqa: E402


RANDOM_SEED = SMOKES_CONFIG.reliable_seed
REAL_PROTOCOL_DATA_DIR = PROJECT_ROOT / "data" / "test" / "reliable_session_smoke"


class _SilentLogService:
    def debug(self, *args, **kwargs) -> None:
        pass

    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass

    def error(self, *args, **kwargs) -> None:
        pass


class _RouteRttService:
    def __init__(self, rtt_by_route_id: dict[str, float] | None = None) -> None:
        self._rtt_by_route_id = rtt_by_route_id or {}

    def resolve_route_rtt_ms(self, *, route_id: str) -> float | None:
        return self._rtt_by_route_id.get(route_id)


def main() -> None:
    manager = SessionManager()
    runtime = _build_session_runtime(
        manager,
        route_rtt_by_id={"route-from-service": SMOKES_CONFIG.reliable_route_service_rtt_ms},
    )

    _verify_ack_cleanup(manager)
    _verify_ordering_buffer_and_dedup(manager)
    _verify_randomized_out_of_order_delivery(manager)
    _verify_session_state_isolation(manager)
    _verify_physical_hop_by_hop_retry(manager, runtime)
    _verify_virtual_retry_uses_metadata_route_rtt(manager, runtime)
    _verify_virtual_retry_uses_route_service_rtt(manager, runtime)
    _verify_virtual_retry_fallback_without_route_rtt(manager, runtime)
    _verify_max_attempts_marks_message_failed(manager, runtime)
    asyncio.run(_verify_real_virtual_protocol_roundtrip())

    print("reliable session smoke OK")


def _verify_ack_cleanup(manager: SessionManager) -> None:
    session_id = _create_virtual_session(manager, "session-ack")
    first = _prepare_virtual_message(manager, session_id, "first")
    second = _prepare_virtual_message(manager, session_id, "second")

    manager.mark_reliable_outbound_sent(
        session_id=session_id,
        sequence_number=first.sequence_number,
    )
    wrong_ack = manager.mark_reliable_outbound_acked(
        session_id=session_id,
        sequence_number=first.sequence_number,
        reliable_message_id=second.reliable_message_id,
    )
    if wrong_ack is not None:
        raise RuntimeError("Reliable ACK accepted a mismatched message id.")

    manager.mark_reliable_outbound_acked(
        session_id=session_id,
        sequence_number=first.sequence_number,
        reliable_message_id=first.reliable_message_id,
    )
    if manager.count_pending_reliable_outbound(session_id) != 1:
        raise RuntimeError("ACK did not remove only the expected reliable outbound message.")

    manager.mark_reliable_outbound_acked(
        session_id=session_id,
        sequence_number=second.sequence_number,
        reliable_message_id=second.reliable_message_id,
    )
    if manager.count_pending_reliable_outbound(session_id) != 0:
        raise RuntimeError("Reliable pending counter did not reach zero after ACKs.")


def _verify_ordering_buffer_and_dedup(manager: SessionManager) -> None:
    session_id = _create_virtual_session(manager, "session-order")
    first = _prepare_virtual_message(manager, session_id, "first")
    second = _prepare_virtual_message(manager, session_id, "second")

    future_result = manager.receive_reliable_inbound(
        session_id=session_id,
        payload=second.to_reliable_payload(),
    )
    if future_result.deliveries or not future_result.buffered:
        raise RuntimeError("Out-of-order reliable message was not buffered.")

    ordered_result = manager.receive_reliable_inbound(
        session_id=session_id,
        payload=first.to_reliable_payload(),
    )
    delivered_values = [delivery.inner_payload["value"] for delivery in ordered_result.deliveries]
    if delivered_values != ["first", "second"]:
        raise RuntimeError(f"Reliable delivery order mismatch: {delivered_values!r}")

    duplicate_result = manager.receive_reliable_inbound(
        session_id=session_id,
        payload=first.to_reliable_payload(),
    )
    if duplicate_result.deliveries or not duplicate_result.duplicate:
        raise RuntimeError("Duplicate reliable message was delivered again.")


def _verify_randomized_out_of_order_delivery(manager: SessionManager) -> None:
    session_id = _create_virtual_session(manager, "session-random-order")
    messages = [
        _prepare_virtual_message(manager, session_id, f"value-{index:02d}")
        for index in range(1, SMOKES_CONFIG.reliable_random_message_count + 1)
    ]
    shuffled_messages = messages[:]
    random.Random(RANDOM_SEED).shuffle(shuffled_messages)

    delivered_values: list[str] = []
    for message in shuffled_messages:
        result = manager.receive_reliable_inbound(
            session_id=session_id,
            payload=message.to_reliable_payload(),
        )
        delivered_values.extend(delivery.inner_payload["value"] for delivery in result.deliveries)

    expected_values = [
        f"value-{index:02d}"
        for index in range(1, SMOKES_CONFIG.reliable_random_message_count + 1)
    ]
    if delivered_values != expected_values:
        raise RuntimeError("Randomized reliable delivery did not preserve sequence order.")

    duplicate_message = random.choice(messages)
    duplicate_result = manager.receive_reliable_inbound(
        session_id=session_id,
        payload=duplicate_message.to_reliable_payload(),
    )
    if duplicate_result.deliveries or not duplicate_result.duplicate:
        raise RuntimeError("Random duplicate reliable message was delivered again.")


def _verify_session_state_isolation(manager: SessionManager) -> None:
    session_a = _create_virtual_session(manager, "session-isolated-a")
    session_b = _create_virtual_session(manager, "session-isolated-b")
    message_a = _prepare_virtual_message(manager, session_a, "a-1")
    message_b = _prepare_virtual_message(manager, session_b, "b-1")

    delivered_a = manager.receive_reliable_inbound(
        session_id=session_a,
        payload=message_a.to_reliable_payload(),
    )
    delivered_b = manager.receive_reliable_inbound(
        session_id=session_b,
        payload=message_b.to_reliable_payload(),
    )

    if [item.inner_payload["value"] for item in delivered_a.deliveries] != ["a-1"]:
        raise RuntimeError("Reliable session A did not deliver its own sequence.")
    if [item.inner_payload["value"] for item in delivered_b.deliveries] != ["b-1"]:
        raise RuntimeError("Reliable session B did not deliver its own sequence.")


def _verify_physical_hop_by_hop_retry(manager: SessionManager, runtime: SessionRuntime) -> None:
    session_id = _create_physical_session(manager, "session-physical-retry")
    message = _prepare_physical_message(manager, session_id, "physical")
    _mark_sent_at(manager, message, seconds_ago=1.5)
    if message in runtime._list_due_reliable_messages():
        raise RuntimeError("Physical hop-by-hop reliable retry fired before its fixed interval.")

    _mark_sent_at(manager, message, seconds_ago=2.5)
    due_messages = runtime._list_due_reliable_messages()
    if message not in due_messages or round(message.retry_after_seconds, 1) != 2.0:
        raise RuntimeError("Physical hop-by-hop reliable retry did not use the fixed interval.")


def _verify_virtual_retry_uses_metadata_route_rtt(
    manager: SessionManager,
    runtime: SessionRuntime,
) -> None:
    session_id = _create_virtual_session(
        manager,
        "session-virtual-metadata-rtt",
        bound_route_id="route-from-metadata",
        metadata_json=f'{{"entry_point_rtt":{SMOKES_CONFIG.reliable_metadata_route_rtt_ms}}}',
    )
    message = _prepare_virtual_message(manager, session_id, "virtual-metadata-rtt")
    _mark_sent_at(manager, message, seconds_ago=6)
    if message in runtime._list_due_reliable_messages():
        raise RuntimeError("Virtual reliable retry ignored metadata RTT and retried too early.")

    _mark_sent_at(manager, message, seconds_ago=13)
    due_messages = runtime._list_due_reliable_messages()
    if message not in due_messages or round(message.retry_after_seconds) != 12:
        raise RuntimeError("Virtual reliable retry did not follow metadata route RTT.")


def _verify_virtual_retry_uses_route_service_rtt(
    manager: SessionManager,
    runtime: SessionRuntime,
) -> None:
    session_id = _create_virtual_session(
        manager,
        "session-virtual-service-rtt",
        bound_route_id="route-from-service",
    )
    message = _prepare_virtual_message(manager, session_id, "virtual-service-rtt")
    _mark_sent_at(manager, message, seconds_ago=10)
    if message in runtime._list_due_reliable_messages():
        raise RuntimeError("Virtual reliable retry ignored route service RTT and retried too early.")

    _mark_sent_at(manager, message, seconds_ago=19)
    due_messages = runtime._list_due_reliable_messages()
    if message not in due_messages or round(message.retry_after_seconds) != 18:
        raise RuntimeError("Virtual reliable retry did not follow route service RTT.")


def _verify_virtual_retry_fallback_without_route_rtt(
    manager: SessionManager,
    runtime: SessionRuntime,
) -> None:
    session_id = _create_virtual_session(
        manager,
        "session-virtual-fallback",
        bound_route_id="route-without-rtt",
    )
    message = _prepare_virtual_message(manager, session_id, "virtual-fallback")
    _mark_sent_at(manager, message, seconds_ago=4)
    if message in runtime._list_due_reliable_messages():
        raise RuntimeError("Virtual reliable retry fallback fired too early.")

    _mark_sent_at(manager, message, seconds_ago=6)
    due_messages = runtime._list_due_reliable_messages()
    if message not in due_messages or round(message.retry_after_seconds) != 5:
        raise RuntimeError("Virtual reliable retry fallback was not used without route RTT.")


def _verify_max_attempts_marks_message_failed(
    manager: SessionManager,
    runtime: SessionRuntime,
) -> None:
    session_id = _create_physical_session(manager, "session-max-attempts")
    message = _prepare_physical_message(manager, session_id, "max-attempts", max_attempts=2)
    message.attempts = 2
    _mark_sent_at(manager, message, seconds_ago=60)

    if message in runtime._list_due_reliable_messages():
        raise RuntimeError("Reliable message with max attempts was scheduled for resend.")
    if message.pending or message.failure_reason != "max_attempts_exceeded":
        raise RuntimeError("Reliable message was not marked failed after max attempts.")


def _create_virtual_session(
    manager: SessionManager,
    session_id: str,
    *,
    bound_route_id: str | None = None,
    metadata_json: str | None = None,
) -> str:
    manager.create_session(
        SessionCreateInput(
            session_id=session_id,
            session_scope="virtual",
            local_identity_type="virtual_node",
            local_identity_id=f"local-{session_id}",
            remote_identity_type="virtual_node",
            remote_identity_id=f"remote-{session_id}",
            direction="outbound",
            initiator_side="initiator",
            handshake_state="ready",
            session_state="active",
            bound_route_id=bound_route_id,
            metadata_json=metadata_json,
        )
    )
    return session_id


def _create_physical_session(manager: SessionManager, session_id: str) -> str:
    manager.create_session(
        SessionCreateInput(
            session_id=session_id,
            session_scope="physical",
            local_identity_type="physical_node",
            local_identity_id=f"local-{session_id}",
            remote_identity_type="physical_node",
            remote_identity_id=f"remote-{session_id}",
            direction="outbound",
            initiator_side="initiator",
            handshake_state="ready",
            session_state="active",
            transport="tcp",
            remote_host="127.0.0.1",
            remote_port=19001,
        )
    )
    return session_id


def _prepare_virtual_message(
    manager: SessionManager,
    session_id: str,
    value: str,
    *,
    max_attempts: int = SMOKES_CONFIG.reliable_default_max_attempts,
):
    return manager.prepare_reliable_outbound(
        session_id=session_id,
        inner_message_type="VIRTUAL_SESSION_DATA",
        inner_payload={"value": value},
        retry_after_seconds=SMOKES_CONFIG.reliable_virtual_retry_fallback_seconds,
        max_attempts=max_attempts,
    )


def _prepare_physical_message(
    manager: SessionManager,
    session_id: str,
    value: str,
    *,
    max_attempts: int = SMOKES_CONFIG.reliable_default_max_attempts,
):
    return manager.prepare_reliable_outbound(
        session_id=session_id,
        inner_message_type="PHYSICAL_TEST_DATA",
        inner_payload={"value": value},
        retry_after_seconds=SMOKES_CONFIG.reliable_physical_retry_seconds,
        max_attempts=max_attempts,
    )


def _mark_sent_at(manager: SessionManager, message, *, seconds_ago: float) -> None:
    manager.mark_reliable_outbound_sent(
        session_id=message.session_id,
        sequence_number=message.sequence_number,
    )
    message.last_sent_at = utc_now() - timedelta(seconds=seconds_ago)


def _build_session_runtime(
    manager: SessionManager,
    *,
    route_rtt_by_id: dict[str, float] | None = None,
) -> SessionRuntime:
    runtime = SessionRuntime.__new__(SessionRuntime)
    runtime.engine = SimpleNamespace(
        services=SimpleNamespace(
            config=SimpleNamespace(
                physical_session_reliable_retry_after_seconds=(
                    SMOKES_CONFIG.reliable_physical_retry_seconds
                ),
                virtual_session_reliable_retry_fallback_seconds=(
                    SMOKES_CONFIG.reliable_virtual_retry_fallback_seconds
                ),
                virtual_session_reliable_retry_rtt_multiplier=(
                    SMOKES_CONFIG.reliable_virtual_retry_rtt_multiplier
                ),
                virtual_session_reliable_retry_min_seconds=(
                    SMOKES_CONFIG.reliable_virtual_retry_min_seconds
                ),
                virtual_session_reliable_retry_max_seconds=(
                    SMOKES_CONFIG.reliable_virtual_retry_max_seconds
                ),
            ),
            session_manager=manager,
            route_service=_RouteRttService(route_rtt_by_id),
            log_service=_SilentLogService(),
        )
    )
    return runtime


async def _verify_real_virtual_protocol_roundtrip() -> None:
    reset_core_data_dir(REAL_PROTOCOL_DATA_DIR)
    sender_engine = create_isolated_core(
        data_dir=REAL_PROTOCOL_DATA_DIR / "sender",
        listen_port=SMOKES_CONFIG.reliable_real_sender_port,
        log_dir=REAL_PROTOCOL_DATA_DIR / "logs" / "sender",
        bootstrap_public_endpoints=[],
        bootstrap_dns_seeds=[],
    )
    receiver_engine = create_isolated_core(
        data_dir=REAL_PROTOCOL_DATA_DIR / "receiver",
        listen_port=SMOKES_CONFIG.reliable_real_receiver_port,
        log_dir=REAL_PROTOCOL_DATA_DIR / "logs" / "receiver",
        bootstrap_public_endpoints=[],
        bootstrap_dns_seeds=[],
    )
    _prepare_engine_without_transport(sender_engine, node_name="reliable-sender")
    _prepare_engine_without_transport(receiver_engine, node_name="reliable-receiver")

    try:
        sender_virtual_node = create_local_virtual_node(
            sender_engine,
            kind="reliable-smoke",
            metadata_source="reliable-sender",
        )
        receiver_virtual_node = create_local_virtual_node(
            receiver_engine,
            kind="reliable-smoke",
            metadata_source="reliable-receiver",
        )
        physical_session_id = "real-physical-hop-session"
        virtual_session_id = "real-virtual-e2e-session"
        _create_real_physical_session_pair(
            sender_engine.services.session_manager,
            receiver_engine.services.session_manager,
            session_id=physical_session_id,
        )
        await _verify_real_physical_protocol_roundtrip(
            sender_engine=sender_engine,
            receiver_engine=receiver_engine,
            session_id=physical_session_id,
        )
        _create_real_virtual_session_pair(
            sender_engine.services.session_manager,
            receiver_engine.services.session_manager,
            session_id=virtual_session_id,
            sender_virtual_node_id=sender_virtual_node.id,
            receiver_virtual_node_id=receiver_virtual_node.id,
        )
        _install_direct_virtual_bridge(
            sender_engine=sender_engine,
            receiver_engine=receiver_engine,
            physical_session_id=physical_session_id,
        )

        await validate_virtual_message_roundtrip(
            sender_engine=sender_engine,
            receiver_engine=receiver_engine,
            session_id=virtual_session_id,
            payload={"value": "real-reliable-protocol-path"},
            timeout_seconds=SMOKES_CONFIG.reliable_short_timeout_seconds,
        )
        if sender_engine.services.session_manager.count_pending_reliable_outbound(virtual_session_id) != 0:
            raise RuntimeError("Real virtual reliable roundtrip did not clear outbound pending state.")
    finally:
        await stop_cores(sender_engine, receiver_engine)


def _prepare_engine_without_transport(engine, *, node_name: str) -> None:
    engine.services.log_service.configure(
        node_name=node_name,
        log_file_path=REAL_PROTOCOL_DATA_DIR / "logs" / f"{node_name}.log",
    )
    engine.services.database.create_schema()
    engine.services.identity_service.ensure_local_physical_node()


def _create_real_physical_session_pair(
    sender_manager: SessionManager,
    receiver_manager: SessionManager,
    *,
    session_id: str,
) -> None:
    sender_manager.create_session(
        SessionCreateInput(
            session_id=session_id,
            session_scope="physical",
            local_identity_type="physical_node",
            local_identity_id="physical-sender",
            remote_identity_type="physical_node",
            remote_identity_id="physical-receiver",
            direction="outbound",
            initiator_side="initiator",
            handshake_state="ready",
            session_state="active",
            transport="direct",
            remote_host="127.0.0.1",
            remote_port=SMOKES_CONFIG.reliable_real_receiver_port,
        )
    )
    receiver_manager.create_session(
        SessionCreateInput(
            session_id=session_id,
            session_scope="physical",
            local_identity_type="physical_node",
            local_identity_id="physical-receiver",
            remote_identity_type="physical_node",
            remote_identity_id="physical-sender",
            direction="inbound",
            initiator_side="responder",
            handshake_state="ready",
            session_state="active",
            transport="direct",
            remote_host="127.0.0.1",
            remote_port=SMOKES_CONFIG.reliable_real_sender_port,
        )
    )


def _create_real_virtual_session_pair(
    sender_manager: SessionManager,
    receiver_manager: SessionManager,
    *,
    session_id: str,
    sender_virtual_node_id: str,
    receiver_virtual_node_id: str,
) -> None:
    sender_manager.create_session(
        SessionCreateInput(
            session_id=session_id,
            session_scope="virtual",
            local_identity_type="virtual_node",
            local_identity_id=sender_virtual_node_id,
            remote_identity_type="virtual_node",
            remote_identity_id=receiver_virtual_node_id,
            direction="outbound",
            initiator_side="initiator",
            handshake_state="ready",
            session_state="active",
            bound_route_id="direct-route-sender-to-receiver",
            metadata_json=f'{{"entry_point_rtt":{SMOKES_CONFIG.reliable_direct_route_rtt_ms}}}',
        )
    )
    receiver_manager.create_session(
        SessionCreateInput(
            session_id=session_id,
            session_scope="virtual",
            local_identity_type="virtual_node",
            local_identity_id=receiver_virtual_node_id,
            remote_identity_type="virtual_node",
            remote_identity_id=sender_virtual_node_id,
            direction="inbound",
            initiator_side="responder",
            handshake_state="ready",
            session_state="active",
            bound_route_id="direct-route-receiver-to-sender",
            metadata_json=f'{{"entry_point_rtt":{SMOKES_CONFIG.reliable_direct_route_rtt_ms}}}',
        )
    )


async def _verify_real_physical_protocol_roundtrip(
    *,
    sender_engine,
    receiver_engine,
    session_id: str,
) -> None:
    original_send_packet = sender_engine.send_packet

    async def send_direct_packet(message) -> None:
        receiver_context = PacketContext(
            transport_name=message.transport_name,
            payload=message.payload,
            remote_host="127.0.0.1",
            remote_port=SMOKES_CONFIG.reliable_real_sender_port,
            local_host="127.0.0.1",
            local_port=SMOKES_CONFIG.reliable_real_receiver_port,
        )
        receiver_result = await receiver_engine.process_received_packet(receiver_context)
        if not receiver_result.handled:
            raise RuntimeError(
                f"Real physical reliable DATA was not handled: {receiver_result.metadata!r}"
            )
        if receiver_result.response_payload is None:
            raise RuntimeError("Real physical reliable DATA did not produce ACK payload.")

        sender_context = PacketContext(
            transport_name=message.transport_name,
            payload=receiver_result.response_payload,
            remote_host="127.0.0.1",
            remote_port=SMOKES_CONFIG.reliable_real_receiver_port,
            local_host="127.0.0.1",
            local_port=SMOKES_CONFIG.reliable_real_sender_port,
        )
        sender_result = await sender_engine.process_received_packet(sender_context)
        if not sender_result.handled:
            raise RuntimeError(f"Real physical reliable ACK was not handled: {sender_result.metadata!r}")

    sender_engine.send_packet = send_direct_packet
    try:
        await sender_engine.services.protocol_clients.physical.session.send_reliable_protocol_message(
            session_id=session_id,
            inner_message_type="PHYSICAL_SESSION_KEEPALIVE",
            inner_payload={},
        )
        if sender_engine.services.session_manager.count_pending_reliable_outbound(session_id) != 0:
            raise RuntimeError("Real physical reliable roundtrip did not clear outbound pending state.")
    finally:
        sender_engine.send_packet = original_send_packet


def _install_direct_virtual_bridge(
    *,
    sender_engine,
    receiver_engine,
    physical_session_id: str,
) -> None:
    async def send_from_sender_to_receiver(
        *,
        session,
        message_type: str,
        payload: dict[str, object],
        virtual_envelope_ciphered: bool,
    ) -> None:
        sender_engine.services.log_service.info(
            "reliable_session_smoke",
            "direct bridge sending virtual envelope through real reliable handlers",
            session_id=session.session_id,
            message_type=message_type,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )
        response = await _deliver_virtual_envelope(
            source_engine=sender_engine,
            target_engine=receiver_engine,
            physical_session_id=physical_session_id,
            virtual_session_id=session.session_id,
            message_type=message_type,
            payload=payload,
        )
        if response is not None:
            await _deliver_virtual_response(
                target_engine=sender_engine,
                physical_session_id=physical_session_id,
                response_envelope=response,
            )

    sender_engine.services.protocol_clients.virtual.session._send_virtual_envelope = (
        send_from_sender_to_receiver
    )


async def _deliver_virtual_envelope(
    *,
    source_engine,
    target_engine,
    physical_session_id: str,
    virtual_session_id: str,
    message_type: str,
    payload: dict[str, object],
) -> dict[str, object] | None:
    header = source_engine.build_message_header(
        message_type=message_type,
        physical_session_id=physical_session_id,
        virtual_session_id=virtual_session_id,
    )
    envelope = _build_envelope(header=header, payload=payload)
    context = _build_direct_context(envelope)
    result = await target_engine.process_protocol_envelope(envelope, context)
    if not result.handled:
        raise RuntimeError(f"Direct virtual reliable delivery was not handled: {result.metadata!r}")

    response = result.metadata.get("virtual_response_envelope")
    if response is None:
        return None
    if not isinstance(response, dict):
        raise RuntimeError("Virtual reliable handler returned an invalid response envelope.")
    return response


async def _deliver_virtual_response(
    *,
    target_engine,
    physical_session_id: str,
    response_envelope: dict[str, object],
) -> None:
    header = response_envelope.get("header")
    payload = response_envelope.get("payload")
    if not isinstance(header, dict) or not isinstance(payload, dict):
        raise RuntimeError("Direct virtual response envelope is invalid.")

    header = {
        **header,
        "physical_session_id": header.get("physical_session_id") or physical_session_id,
    }
    envelope = _build_envelope(header=header, payload=payload)
    context = _build_direct_context(envelope)
    result = await target_engine.process_protocol_envelope(envelope, context)
    if not result.handled:
        raise RuntimeError(f"Direct virtual reliable ACK was not handled: {result.metadata!r}")


def _build_envelope(*, header: dict[str, object], payload: dict[str, object]) -> ProtocolEnvelope:
    raw_payload = json.dumps(
        {"header": header, "payload": payload},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    message_type = header.get("message_type")
    return ProtocolEnvelope(
        protocol_name="json",
        message_type=message_type if isinstance(message_type, str) else None,
        payload=payload,
        raw_payload=raw_payload,
        header=header,
    )


def _build_direct_context(envelope: ProtocolEnvelope) -> PacketContext:
    return PacketContext(
        transport_name="direct-smoke",
        payload=envelope.raw_payload,
        remote_host="127.0.0.1",
        remote_port=19701,
        local_host="127.0.0.1",
        local_port=19702,
        metadata={
            "route_path_id": "direct-route-sender-to-receiver",
            "route_direction": "vn_to_pn",
            "route_message_type": "ROUTE_DATA",
            "virtual_session_id": envelope.header.get("virtual_session_id"),
            "virtual_envelope_ciphered": False,
        },
    )


if __name__ == "__main__":
    main()
