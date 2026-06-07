from __future__ import annotations

from transport import RelayTcpTransportAdapter, TransportEndpoint

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler
from ..helpers import as_payload_dict, read_physical_session_id, read_string_or_none


class PhysicalRelayProtocolHandler(ProtocolMessageHandler):
    protocol_family = "physical_relay"
    supported_message_types = {"PHYSICAL_RELAY_DATA"}

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type != "PHYSICAL_RELAY_DATA":
            return self._build_invalid_result(envelope, "unsupported_physical_relay_message_type")

        return await self._handle_data(envelope, context, services)

    async def _handle_data(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        target_physical_node_id = read_string_or_none(payload, "target_physical_node_id")
        payload_hex = read_string_or_none(payload, "payload_hex")
        session_id = read_physical_session_id(envelope)
        if target_physical_node_id is None or payload_hex is None or session_id is None:
            return self._invalid_with_log(
                envelope,
                services,
                "invalid relay data payload",
                reason="invalid_relay_data",
                has_target=target_physical_node_id is not None,
                has_payload_hex=payload_hex is not None,
                has_session_id=session_id is not None,
            )

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active":
            return self._invalid_with_log(
                envelope,
                services,
                "relay data rejected because physical session is not active",
                reason="inactive_relay_session",
                session_id=session_id,
                session_exists=session is not None,
                session_state=getattr(session, "session_state", None),
            )

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is not None and target_physical_node_id == local_node.id:
            return await self._deliver_to_local_node(
                envelope,
                context,
                services,
                relay_physical_node_id=session.remote_identity_id,
                sender_physical_node_id=_read_sender_id(payload, fallback=session.remote_identity_id),
                payload_hex=payload_hex,
            )

        if not _can_act_as_relay(services):
            return self._invalid_with_log(
                envelope,
                services,
                "relay data rejected because local node cannot act as relay",
                reason="local_node_not_relay_capable",
                target_physical_node_id=target_physical_node_id,
            )

        target_session = services.session_manager.get_active_physical_session_by_remote_node_id(
            target_physical_node_id
        )
        if target_session is None:
            return self._invalid_with_log(
                envelope,
                services,
                "relay data target has no active session with this relay",
                reason="relay_target_session_not_found",
                target_physical_node_id=target_physical_node_id,
            )

        relay_node_id = local_node.id if local_node is not None else None
        forward_payload = {
            "target_physical_node_id": target_physical_node_id,
            "sender_physical_node_id": session.remote_identity_id,
            "relay_physical_node_id": relay_node_id,
            "payload_hex": payload_hex,
        }
        services.log_service.debug(
            "physical_relay",
            "forwarding relay data to active target session",
            sender_physical_node_id=session.remote_identity_id,
            target_physical_node_id=target_physical_node_id,
            relay_physical_node_id=relay_node_id,
            target_session_id=target_session.session_id,
            payload_size_bytes=len(payload_hex) // 2,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": "forward_message",
                "target_physical_session_id": target_session.session_id,
                "forward_message_type": "PHYSICAL_RELAY_DATA",
                "forward_payload": forward_payload,
            },
        )

    async def _deliver_to_local_node(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
        *,
        relay_physical_node_id: str,
        sender_physical_node_id: str,
        payload_hex: str,
    ) -> PacketProcessingResult:
        try:
            inner_payload = bytes.fromhex(payload_hex)
        except ValueError:
            return self._invalid_with_log(
                envelope,
                services,
                "relay data payload_hex is invalid",
                reason="invalid_payload_hex",
                sender_physical_node_id=sender_physical_node_id,
                relay_physical_node_id=relay_physical_node_id,
            )

        adapter = services.transport.adapters.get("relay_tcp")
        if not isinstance(adapter, RelayTcpTransportAdapter):
            return self._invalid_with_log(
                envelope,
                services,
                "cannot deliver relay packet because relay_tcp adapter is missing",
                reason="relay_tcp_adapter_missing",
            )

        relay_endpoint = _build_relay_endpoint_from_session(
            services,
            relay_physical_node_id=relay_physical_node_id,
            fallback_host=context.remote_host,
            fallback_port=context.remote_port,
        )
        metadata = {
            "relay_physical_node_id": relay_physical_node_id,
            "target_physical_node_id": sender_physical_node_id,
        }
        await adapter.inject_inbound_packet(
            payload=inner_payload,
            remote_endpoint=TransportEndpoint(
                transport_name="relay_tcp",
                host=relay_endpoint["host"],
                port=relay_endpoint["port"],
                metadata=metadata,
            ),
            metadata=metadata,
        )
        services.log_service.debug(
            "physical_relay",
            "delivered relay data to local relay_tcp adapter",
            sender_physical_node_id=sender_physical_node_id,
            relay_physical_node_id=relay_physical_node_id,
            payload_size_bytes=len(inner_payload),
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={"protocol_family": self.protocol_family, "action": "deliver_relay_data"},
        )

    def _invalid_with_log(
        self,
        envelope: ProtocolEnvelope,
        services: EngineServices,
        message: str,
        **metadata: object,
    ) -> PacketProcessingResult:
        services.log_service.warning("physical_relay", message, **metadata)
        return self._build_invalid_result(envelope, str(metadata.get("reason", "invalid_physical_relay_message")))


def _can_act_as_relay(services: EngineServices) -> bool:
    return services.engine is not None and services.engine.can_act_as_physical_relay()


def _read_sender_id(payload: dict[str, object], *, fallback: str) -> str:
    sender_physical_node_id = payload.get("sender_physical_node_id")
    if isinstance(sender_physical_node_id, str) and sender_physical_node_id:
        return sender_physical_node_id
    return fallback


def _build_relay_endpoint_from_session(
    services: EngineServices,
    *,
    relay_physical_node_id: str,
    fallback_host: str | None,
    fallback_port: int | None,
) -> dict[str, object]:
    endpoints = services.identity_service.list_remote_physical_node_endpoints(
        relay_physical_node_id,
        only_active=True,
    )
    for endpoint in endpoints:
        if endpoint.transport == "tcp":
            return {"host": endpoint.host, "port": endpoint.port}

    return {
        "host": fallback_host or "",
        "port": int(fallback_port or 0),
    }
