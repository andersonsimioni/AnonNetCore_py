from __future__ import annotations

import json
from uuid import uuid4

from crypto import (
    dilithium_sign_hex,
    dilithium_verify_hex,
    generate_kyber_key_pair,
    kyber_decapsulate_hex,
    kyber_encapsulate_hex,
)

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class VirtualSessionProtocolHandler(ProtocolMessageHandler):
    protocol_family = "virtual_session"
    supported_message_types = {
        "VIRTUAL_SESSION_INIT",
        "VIRTUAL_SESSION_INIT_OK",
        "VIRTUAL_SESSION_KEY_CONFIRM",
        "VIRTUAL_SESSION_READY",
        "VIRTUAL_SESSION_KEEPALIVE",
        "VIRTUAL_SESSION_KEEPALIVE_ACK",
        "VIRTUAL_SESSION_CLOSE",
    }

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "VIRTUAL_SESSION_INIT":
            return await self._handle_virtual_session_init(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_INIT_OK":
            return await self._handle_virtual_session_init_ok(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_KEY_CONFIRM":
            return await self._handle_virtual_session_key_confirm(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_READY":
            return await self._handle_virtual_session_ready(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_KEEPALIVE":
            return await self._handle_virtual_session_keepalive(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_KEEPALIVE_ACK":
            return await self._handle_virtual_session_keepalive_ack(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_CLOSE":
            return await self._handle_virtual_session_close(envelope, context, services)

        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_virtual_session_init(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        initiator_virtual_node_id = payload.get("initiator_virtual_node_id")
        target_virtual_node_id = payload.get("target_virtual_node_id")
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )

        if (
            not session_id
            or not isinstance(initiator_virtual_node_id, str)
            or not isinstance(target_virtual_node_id, str)
        ):
            return self._build_invalid_result(envelope, "invalid_virtual_session_init")

        local_virtual_node = services.identity_service.get_local_virtual_node_by_id(target_virtual_node_id)
        if local_virtual_node is None:
            return self._build_invalid_result(envelope, "local_virtual_node_not_found")

        remote_virtual_node = services.identity_service.get_remote_virtual_node_by_id(
            initiator_virtual_node_id
        )
        route_path_id = context.metadata.get("route_path_id")
        session = services.session_manager.create_inbound_virtual_session(
            session_id=session_id,
            local_virtual_node_id=target_virtual_node_id,
            remote_virtual_node_id=initiator_virtual_node_id,
            remote_public_key=remote_virtual_node.public_key if remote_virtual_node else None,
            bound_route_id=route_path_id if isinstance(route_path_id, str) and route_path_id else None,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )

        ephemeral_key_pair = generate_kyber_key_pair()
        services.session_manager.store_local_ephemeral_keypair(
            session_id,
            private_key_pem=ephemeral_key_pair.private_key_pem,
            public_key_pem=ephemeral_key_pair.public_key_pem,
            handshake_state="init_ok_sent",
        )

        signature_hex = self._sign_virtual_session_init_ok(
            session_id=session_id,
            responder_ephemeral_public_key=ephemeral_key_pair.public_key_pem,
            keepalive_interval_seconds=keepalive_interval_seconds,
            local_private_key_pem=local_virtual_node.private_key_encrypted,
        )
        return self._build_virtual_response_result(
            envelope,
            response_message_type="VIRTUAL_SESSION_INIT_OK",
            payload={
                "responder_ephemeral_public_key": ephemeral_key_pair.public_key_pem,
                "signature_hex": signature_hex,
                "keepalive_interval_seconds": keepalive_interval_seconds,
            },
            extra_metadata={
                "action": "virtual_session_init_ok_sent",
                "session_id": session.session_id,
            },
        )

    async def _handle_virtual_session_init_ok(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        responder_ephemeral_public_key = payload.get("responder_ephemeral_public_key")
        signature_hex = payload.get("signature_hex")
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )

        if (
            not session_id
            or not isinstance(responder_ephemeral_public_key, str)
            or not isinstance(signature_hex, str)
        ):
            return self._build_invalid_result(envelope, "invalid_virtual_session_init_ok")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "virtual_session_not_found")

        remote_public_key = session.remote_public_key
        if not remote_public_key:
            return self._build_invalid_result(envelope, "remote_virtual_node_public_key_not_found")

        signature_valid = self._verify_virtual_session_init_ok(
            session_id=session_id,
            responder_ephemeral_public_key=responder_ephemeral_public_key,
            keepalive_interval_seconds=keepalive_interval_seconds,
            signature_hex=signature_hex,
            remote_public_key_pem=remote_public_key,
        )
        if not signature_valid:
            services.session_manager.close_session(session_id, close_reason="invalid_init_ok_signature")
            return self._build_invalid_result(envelope, "invalid_virtual_session_init_ok_signature")

        services.session_manager.store_remote_ephemeral_public_key(
            session_id,
            public_key_pem=responder_ephemeral_public_key,
            handshake_state="init_ok_verified",
        )
        services.session_manager.touch_session(
            session_id,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )

        encapsulation = kyber_encapsulate_hex(responder_ephemeral_public_key)
        services.session_manager.store_shared_secret(
            session_id,
            shared_secret_hex=encapsulation.shared_secret_hex,
            handshake_state="key_confirm_sent",
            session_state="pending",
        )
        return self._build_virtual_response_result(
            envelope,
            response_message_type="VIRTUAL_SESSION_KEY_CONFIRM",
            payload={
                "encapsulated_key_hex": encapsulation.ciphertext_hex,
            },
            extra_metadata={
                "action": "virtual_session_key_confirm_sent",
                "session_id": session_id,
            },
        )

    async def _handle_virtual_session_key_confirm(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        encapsulated_key_hex = payload.get("encapsulated_key_hex")

        if not session_id or not isinstance(encapsulated_key_hex, str):
            return self._build_invalid_result(envelope, "invalid_virtual_session_key_confirm")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or not session.local_ephemeral_private_key:
            return self._build_invalid_result(envelope, "virtual_session_ephemeral_key_not_found")

        shared_secret_hex = kyber_decapsulate_hex(
            encapsulated_key_hex,
            session.local_ephemeral_private_key,
        )
        services.session_manager.store_shared_secret(
            session_id,
            shared_secret_hex=shared_secret_hex,
            handshake_state="ready_sent",
            session_state="pending",
        )
        services.session_manager.activate_session(session_id)
        return self._build_virtual_response_result(
            envelope,
            response_message_type="VIRTUAL_SESSION_READY",
            payload={
                "status": "active",
                "keepalive_interval_seconds": session.keepalive_interval_seconds,
            },
            extra_metadata={
                "action": "virtual_session_ready_sent",
                "session_id": session_id,
            },
        )

    async def _handle_virtual_session_ready(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )

        if not session_id:
            return self._build_invalid_result(envelope, "invalid_virtual_session_ready")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or not session.shared_secret_hex:
            return self._build_invalid_result(envelope, "virtual_session_shared_secret_not_found")

        services.session_manager.touch_session(
            session_id,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )
        services.session_manager.activate_session(session_id)
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": "virtual_session_activated",
                "session_id": session_id,
            },
        )

    async def _handle_virtual_session_keepalive(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_id = _read_virtual_session_id(envelope)
        if not session_id:
            return self._build_invalid_result(envelope, "invalid_virtual_session_keepalive")

        session = services.session_manager.touch_session(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "virtual_session_not_found")
        return self._build_virtual_response_result(
            envelope,
            response_message_type="VIRTUAL_SESSION_KEEPALIVE_ACK",
            payload={},
            extra_metadata={
                "action": "virtual_session_keepalive_ack_sent",
                "session_id": session_id,
            },
        )

    async def _handle_virtual_session_keepalive_ack(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_id = _read_virtual_session_id(envelope)
        if not session_id:
            return self._build_invalid_result(envelope, "invalid_virtual_session_keepalive_ack")

        session = services.session_manager.touch_session(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "virtual_session_not_found")
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": "virtual_session_keepalive_ack_received",
                "session_id": session_id,
            },
        )

    async def _handle_virtual_session_close(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        close_reason = payload.get("close_reason")

        if not session_id:
            return self._build_invalid_result(envelope, "invalid_virtual_session_close")

        session = services.session_manager.close_session(
            session_id,
            close_reason=close_reason if isinstance(close_reason, str) else "remote_closed",
        )
        if session is None:
            return self._build_invalid_result(envelope, "virtual_session_not_found")

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": "virtual_session_closed",
                "session_id": session_id,
            },
        )

    def _sign_virtual_session_init_ok(
        self,
        *,
        session_id: str,
        responder_ephemeral_public_key: str,
        keepalive_interval_seconds: int,
        local_private_key_pem: str,
    ) -> str:
        signed_payload = {
            "session_id": session_id,
            "responder_ephemeral_public_key": responder_ephemeral_public_key,
            "keepalive_interval_seconds": keepalive_interval_seconds,
        }
        return dilithium_sign_hex(_canonical_payload_hex(signed_payload), local_private_key_pem)

    def _verify_virtual_session_init_ok(
        self,
        *,
        session_id: str,
        responder_ephemeral_public_key: str,
        keepalive_interval_seconds: int,
        signature_hex: str,
        remote_public_key_pem: str,
    ) -> bool:
        signed_payload = {
            "session_id": session_id,
            "responder_ephemeral_public_key": responder_ephemeral_public_key,
            "keepalive_interval_seconds": keepalive_interval_seconds,
        }
        try:
            return dilithium_verify_hex(
                _canonical_payload_hex(signed_payload),
                signature_hex,
                remote_public_key_pem,
            )
        except Exception:
            return False

    def _build_invalid_result(
        self,
        envelope: ProtocolEnvelope,
        reason: str,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "reason": reason,
            },
        )

    def _build_not_implemented_result(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "virtual_session",
                "session_count": len(services.session_manager.list_sessions()),
                "next_step": "implement_virtual_session_protocol_flow",
            },
        )

    @staticmethod
    def _build_virtual_response_result(
        envelope: ProtocolEnvelope,
        *,
        response_message_type: str,
        payload: dict[str, object],
        extra_metadata: dict[str, object] | None = None,
    ) -> PacketProcessingResult:
        metadata = {
            "protocol_family": "virtual_session",
            "virtual_response_envelope": {
                "header": _build_response_header(envelope.header, response_message_type),
                "payload": payload,
            },
        }
        if extra_metadata:
            metadata.update(extra_metadata)

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata=metadata,
        )


def _build_response_header(
    request_header: dict[str, object],
    message_type: str,
) -> dict[str, object]:
    return {
        "version": request_header.get("version", 1),
        "message_type": message_type,
        "message_id": str(uuid4()),
        "message_sequence": request_header.get("message_sequence"),
        "physical_session_id": request_header.get("physical_session_id"),
        "virtual_session_id": request_header.get("virtual_session_id"),
        "response_to_message_id": request_header.get("message_id"),
    }


def _canonical_payload_hex(payload: dict[str, object]) -> str:
    raw_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return raw_bytes.hex()


def _as_payload_dict(envelope: ProtocolEnvelope) -> dict[str, object]:
    return envelope.payload if isinstance(envelope.payload, dict) else {}


def _read_virtual_session_id(envelope: ProtocolEnvelope) -> str | None:
    session_id = envelope.header.get("virtual_session_id")
    if isinstance(session_id, str) and session_id:
        return session_id
    return None


def _read_keepalive_interval(
    payload: dict[str, object],
    default_value: int,
) -> int:
    value = payload.get("keepalive_interval_seconds")
    if isinstance(value, int) and value > 0:
        return value
    return default_value
