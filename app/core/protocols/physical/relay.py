from __future__ import annotations

import json

from common import canonical_payload_hex, compact_json_bytes
from crypto import dilithium_verify_hex, sha512_hex
from transport import RelayTcpTransportAdapter, TransportEndpoint

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler
from ..helpers import (
    as_payload_dict,
    build_response_header,
    read_physical_session_id,
    read_string_or_none,
)


class PhysicalRelayProtocolHandler(ProtocolMessageHandler):
    protocol_family = "physical_relay"
    supported_message_types = {
        "PHYSICAL_RELAY_REGISTER_REQUEST",
        "PHYSICAL_RELAY_REGISTER_CHALLENGE",
        "PHYSICAL_RELAY_REGISTER_PROOF",
        "PHYSICAL_RELAY_REGISTER_OK",
        "PHYSICAL_RELAY_OPEN",
        "PHYSICAL_RELAY_OPEN_OK",
        "PHYSICAL_RELAY_OPEN_FAIL",
        "PHYSICAL_RELAY_DATA",
        "PHYSICAL_RELAY_CLOSE",
    }

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "PHYSICAL_RELAY_REGISTER_REQUEST":
            return await self._handle_register_request(envelope, context, services)
        if envelope.message_type == "PHYSICAL_RELAY_REGISTER_CHALLENGE":
            return await self._handle_client_response(envelope, context, services, "challenge")
        if envelope.message_type == "PHYSICAL_RELAY_REGISTER_PROOF":
            return await self._handle_register_proof(envelope, context, services)
        if envelope.message_type == "PHYSICAL_RELAY_REGISTER_OK":
            return await self._handle_client_response(envelope, context, services, "register_ok")
        if envelope.message_type == "PHYSICAL_RELAY_OPEN":
            return await self._handle_open(envelope, context, services)
        if envelope.message_type in {"PHYSICAL_RELAY_OPEN_OK", "PHYSICAL_RELAY_OPEN_FAIL"}:
            return await self._handle_client_response(envelope, context, services, "open")
        if envelope.message_type == "PHYSICAL_RELAY_DATA":
            return await self._handle_data(envelope, context, services)
        if envelope.message_type == "PHYSICAL_RELAY_CLOSE":
            return await self._handle_close(envelope, context, services)

        return self._build_invalid_result(envelope, "unsupported_physical_relay_message_type")

    async def _handle_register_request(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        target_physical_node_id = read_string_or_none(payload, "target_physical_node_id")
        target_session_id = read_physical_session_id(envelope)
        local_node = services.identity_service.get_local_physical_node_result()
        if target_physical_node_id is None or target_session_id is None or local_node is None:
            return self._invalid_with_log(
                envelope,
                services,
                "invalid relay register request",
                reason="invalid_register_request",
            )

        challenge = services.relay_service.create_challenge(
            target_physical_node_id=target_physical_node_id,
            target_session_id=target_session_id,
            relay_physical_node_id=local_node.id,
        )
        relay_endpoint = _build_relay_endpoint(services)
        services.log_service.info(
            "physical_relay",
            "issued relay registration challenge",
            target_physical_node_id=target_physical_node_id,
            target_session_id=target_session_id,
            relay_physical_node_id=local_node.id,
            relay_endpoint=relay_endpoint,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=_packet_bytes(
                header=build_response_header(envelope.header, "PHYSICAL_RELAY_REGISTER_CHALLENGE"),
                payload={
                    "target_physical_node_id": target_physical_node_id,
                    "relay_physical_node_id": local_node.id,
                    "challenge_nonce": challenge.nonce,
                    "expires_at": challenge.expires_at.isoformat(),
                    "relay_endpoint": relay_endpoint,
                },
            ),
            metadata={"protocol_family": self.protocol_family},
        )

    async def _handle_register_proof(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        target_session_id = read_physical_session_id(envelope)
        target_physical_node_id = read_string_or_none(payload, "target_physical_node_id")
        target_public_key = read_string_or_none(payload, "target_public_key")
        challenge_nonce = read_string_or_none(payload, "challenge_nonce")
        signature_hex = read_string_or_none(payload, "signature_hex")
        relay_physical_node_id = read_string_or_none(payload, "relay_physical_node_id")
        expires_at = read_string_or_none(payload, "expires_at")

        if (
            target_session_id is None
            or target_physical_node_id is None
            or target_public_key is None
            or challenge_nonce is None
            or signature_hex is None
            or relay_physical_node_id is None
            or expires_at is None
        ):
            return self._invalid_with_log(
                envelope,
                services,
                "invalid relay register proof",
                reason="invalid_register_proof",
            )

        challenge = services.relay_service.get_active_challenge(challenge_nonce)
        if (
            challenge is None
            or challenge.target_physical_node_id != target_physical_node_id
            or challenge.target_session_id != target_session_id
            or challenge.relay_physical_node_id != relay_physical_node_id
        ):
            return self._invalid_with_log(
                envelope,
                services,
                "relay registration challenge mismatch",
                reason="relay_challenge_mismatch",
                target_physical_node_id=target_physical_node_id,
            )

        if sha512_hex(target_public_key.encode("utf-8")) != target_physical_node_id:
            return self._invalid_with_log(
                envelope,
                services,
                "relay registration public key does not match target id",
                reason="target_public_key_mismatch",
                target_physical_node_id=target_physical_node_id,
            )

        signed_payload = build_register_signature_payload(
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=target_physical_node_id,
            challenge_nonce=challenge_nonce,
            expires_at=expires_at,
            relay_endpoint=_build_relay_endpoint(services),
        )
        try:
            signature_valid = dilithium_verify_hex(
                canonical_payload_hex(signed_payload),
                signature_hex,
                target_public_key,
            )
        except Exception as error:
            services.log_service.warning(
                "physical_relay",
                "relay registration signature verification failed",
                target_physical_node_id=target_physical_node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            signature_valid = False

        if not signature_valid:
            return self._invalid_with_log(
                envelope,
                services,
                "relay registration signature is invalid",
                reason="invalid_register_signature",
                target_physical_node_id=target_physical_node_id,
            )

        registration = services.relay_service.register_target(
            target_physical_node_id=target_physical_node_id,
            target_public_key=target_public_key,
            target_session_id=target_session_id,
            challenge_nonce=challenge_nonce,
            signature_hex=signature_hex,
        )
        _announce_relayed_node(services, target_physical_node_id, target_public_key)
        services.log_service.info(
            "physical_relay",
            "registered relayed physical node",
            target_physical_node_id=target_physical_node_id,
            target_session_id=target_session_id,
            expires_at=registration.expires_at.isoformat(),
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=_packet_bytes(
                header=build_response_header(envelope.header, "PHYSICAL_RELAY_REGISTER_OK"),
                payload={
                    "status": "registered",
                    "target_physical_node_id": target_physical_node_id,
                    "relay_physical_node_id": relay_physical_node_id,
                    "relay_endpoint": _build_relay_endpoint(services),
                    "expires_at": registration.expires_at.isoformat(),
                },
            ),
            metadata={"protocol_family": self.protocol_family},
        )

    async def _handle_open(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        requester_session_id = read_physical_session_id(envelope)
        target_physical_node_id = read_string_or_none(payload, "target_physical_node_id")
        if requester_session_id is None or target_physical_node_id is None:
            return self._build_open_fail(envelope, "invalid_open_request")

        registration = services.relay_service.get_active_registration(target_physical_node_id)
        if registration is None:
            services.log_service.warning(
                "physical_relay",
                "relay open failed because target is not registered",
                target_physical_node_id=target_physical_node_id,
                requester_session_id=requester_session_id,
            )
            return self._build_open_fail(envelope, "target_not_registered")

        channel = services.relay_service.create_channel(
            target_physical_node_id=target_physical_node_id,
            requester_session_id=requester_session_id,
            target_session_id=registration.target_session_id,
        )
        services.log_service.info(
            "physical_relay",
            "opened relay channel",
            relay_channel_id=channel.relay_channel_id,
            target_physical_node_id=target_physical_node_id,
            requester_session_id=requester_session_id,
            target_session_id=registration.target_session_id,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=_packet_bytes(
                header=build_response_header(envelope.header, "PHYSICAL_RELAY_OPEN_OK"),
                payload={
                    "relay_channel_id": channel.relay_channel_id,
                    "target_physical_node_id": target_physical_node_id,
                    "expires_at": channel.expires_at.isoformat(),
                },
            ),
            metadata={"protocol_family": self.protocol_family},
        )

    async def _handle_data(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        session_id = read_physical_session_id(envelope)
        relay_channel_id = read_string_or_none(payload, "relay_channel_id")
        if session_id is None or relay_channel_id is None:
            return self._invalid_with_log(
                envelope,
                services,
                "invalid relay data payload",
                reason="invalid_relay_data",
            )

        channel = services.relay_service.get_active_channel(relay_channel_id)
        if channel is None:
            return await self._deliver_data_to_local_client(envelope, context, services, relay_channel_id)

        target_session_id = _select_forward_session_id(channel, session_id)
        if target_session_id is None:
            return self._invalid_with_log(
                envelope,
                services,
                "relay data arrived from session outside channel",
                reason="relay_channel_session_mismatch",
                relay_channel_id=relay_channel_id,
                session_id=session_id,
            )

        forward_payload = _build_forward_relay_payload(
            services,
            payload=payload,
            from_session_id=session_id,
        )
        services.log_service.debug(
            "physical_relay",
            "forwarding relay data through channel",
            relay_channel_id=relay_channel_id,
            from_session_id=session_id,
            to_session_id=target_session_id,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": "forward_message",
                "target_physical_session_id": target_session_id,
                "forward_message_type": "PHYSICAL_RELAY_DATA",
                "forward_payload": forward_payload,
            },
        )

    async def _handle_close(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        relay_channel_id = read_string_or_none(payload, "relay_channel_id")
        session_id = read_physical_session_id(envelope)
        channel = services.relay_service.close_channel(relay_channel_id) if relay_channel_id else None
        target_session_id = _select_forward_session_id(channel, session_id) if channel and session_id else None
        services.log_service.info(
            "physical_relay",
            "closed relay channel",
            relay_channel_id=relay_channel_id,
            from_session_id=session_id,
            notify_session_id=target_session_id,
        )
        metadata: dict[str, object] = {"protocol_family": self.protocol_family}
        if target_session_id is not None:
            metadata.update(
                {
                    "action": "forward_message",
                    "target_physical_session_id": target_session_id,
                    "forward_message_type": "PHYSICAL_RELAY_CLOSE",
                    "forward_payload": payload,
                }
            )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata=metadata,
        )

    async def _handle_client_response(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
        response_kind: str,
    ) -> PacketProcessingResult:
        response_to_message_id = envelope.header.get("response_to_message_id")
        if isinstance(response_to_message_id, str) and services.protocol_clients is not None:
            services.protocol_clients.physical.relay.complete_response(
                response_to_message_id=response_to_message_id,
                message_type=envelope.message_type or "",
                payload=as_payload_dict(envelope),
            )
        services.log_service.debug(
            "physical_relay",
            "processed relay client response",
            response_kind=response_kind,
            response_to_message_id=response_to_message_id,
            message_type=envelope.message_type,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={"protocol_family": self.protocol_family, "response_kind": response_kind},
        )

    async def _deliver_data_to_local_client(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
        relay_channel_id: str,
    ) -> PacketProcessingResult:
        payload = as_payload_dict(envelope)
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("payload_hex"), str):
            adapter = services.transport.adapters.get("relay_tcp")
            if isinstance(adapter, RelayTcpTransportAdapter):
                await adapter.inject_inbound_packet(
                    payload=bytes.fromhex(data["payload_hex"]),
                    remote_endpoint=TransportEndpoint(
                        transport_name="relay_tcp",
                        host=str(data.get("relay_host") or context.remote_host or ""),
                        port=int(data.get("relay_port") or context.remote_port or 0),
                        metadata={
                            "relay_channel_id": relay_channel_id,
                            "relay_physical_node_id": data.get("relay_physical_node_id"),
                            "target_physical_node_id": data.get("sender_physical_node_id"),
                        },
                    ),
                    metadata={
                        "relay_channel_id": relay_channel_id,
                        "relay_physical_node_id": data.get("relay_physical_node_id"),
                        "target_physical_node_id": data.get("sender_physical_node_id"),
                    },
                )
            else:
                services.log_service.warning(
                    "physical_relay",
                    "cannot inject relay transport packet because relay_tcp adapter is missing",
                    relay_channel_id=relay_channel_id,
                )
        elif services.protocol_clients is not None:
            services.protocol_clients.physical.relay.handle_inbound_data(
                relay_channel_id=relay_channel_id,
                payload=payload,
            )
        services.log_service.debug(
            "physical_relay",
            "delivered relay data to local client",
            relay_channel_id=relay_channel_id,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={"protocol_family": self.protocol_family, "action": "deliver_relay_data"},
        )

    def _build_open_fail(self, envelope: ProtocolEnvelope, reason: str) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            response_payload=_packet_bytes(
                header=build_response_header(envelope.header, "PHYSICAL_RELAY_OPEN_FAIL"),
                payload={"status": "failed", "reason": reason},
            ),
            metadata={"protocol_family": self.protocol_family, "reason": reason},
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


def build_register_signature_payload(
    *,
    relay_physical_node_id: str,
    target_physical_node_id: str,
    challenge_nonce: str,
    expires_at: str,
    relay_endpoint: dict[str, object],
) -> dict[str, object]:
    return {
        "purpose": "physical_relay_register",
        "relay_physical_node_id": relay_physical_node_id,
        "target_physical_node_id": target_physical_node_id,
        "challenge_nonce": challenge_nonce,
        "expires_at": expires_at,
        "relay_endpoint": relay_endpoint,
    }


def _packet_bytes(*, header: dict[str, object], payload: dict[str, object]) -> bytes:
    return compact_json_bytes({"header": header, "payload": payload})


def _build_relay_endpoint(services: EngineServices) -> dict[str, object]:
    if services.engine is None:
        return {"transport": "relay_tcp", "host": "", "port": 0, "priority": 50, "metadata": {}}
    local_node = services.identity_service.get_local_physical_node_result()
    metadata = {"relay_physical_node_id": local_node.id} if local_node is not None else {}
    return {
        "transport": "relay_tcp",
        "host": services.engine.get_advertised_tcp_host(),
        "port": services.engine.get_advertised_tcp_port(),
        "priority": 50,
        "metadata": metadata,
    }


def _announce_relayed_node(
    services: EngineServices,
    target_physical_node_id: str,
    target_public_key: str,
) -> None:
    relay_endpoint = _build_relay_endpoint(services)
    services.identity_service.upsert_remote_physical_node(
        node_id=target_physical_node_id,
        public_key=target_public_key,
        protocol_version="1",
        status="active",
        endpoints=[relay_endpoint],
        mark_validated=True,
        reachability_class="relay",
        relay_capable=False,
        hole_punch_capable=False,
        notes_json=json.dumps(
            {
                "source": "physical_relay_registration",
                "relay_endpoint": relay_endpoint,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def _select_forward_session_id(channel, incoming_session_id: str) -> str | None:
    if channel.requester_session_id == incoming_session_id:
        return channel.target_session_id
    if channel.target_session_id == incoming_session_id:
        return channel.requester_session_id
    return None


def _build_forward_relay_payload(
    services: EngineServices,
    *,
    payload: dict[str, object],
    from_session_id: str,
) -> dict[str, object]:
    forward_payload = dict(payload)
    data = forward_payload.get("data")
    if not isinstance(data, dict):
        return forward_payload

    sender_session = services.session_manager.get_session_by_session_id(from_session_id)
    local_node = services.identity_service.get_local_physical_node_result()
    enriched_data = dict(data)
    if sender_session is not None:
        enriched_data["sender_physical_node_id"] = sender_session.remote_identity_id
    if local_node is not None:
        enriched_data["relay_physical_node_id"] = local_node.id
    forward_payload["data"] = enriched_data
    return forward_payload
