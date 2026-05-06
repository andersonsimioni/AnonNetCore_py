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


class SessionProtocolHandler(ProtocolMessageHandler):
    protocol_family = "session"
    supported_message_types = {
        "PHYSICAL_SESSION_INIT",
        "PHYSICAL_SESSION_INIT_OK",
        "PHYSICAL_SESSION_KEY_CONFIRM",
        "PHYSICAL_SESSION_READY",
        "PHYSICAL_SESSION_KEEPALIVE",
        "PHYSICAL_SESSION_KEEPALIVE_ACK",
        "PHYSICAL_SESSION_CLOSE",
        
        "OVERLAY_SESSION_INIT",
        "OVERLAY_SESSION_INIT_OK",
        "OVERLAY_SESSION_KEY_CONFIRM",
        "OVERLAY_SESSION_READY",
        "OVERLAY_SESSION_CLOSE",
    }

    @staticmethod
    def build_physical_session_init_payload(
        *,
        header: dict[str, object],
        initiator_physical_node_id: str,
        keepalive_interval_seconds: int,
    ) -> bytes:
        return _build_packet_bytes(
            header=header,
            payload={
                "initiator_physical_node_id": initiator_physical_node_id,
                "keepalive_interval_seconds": keepalive_interval_seconds,
            },
        )

    @staticmethod
    def build_physical_session_init_ok_payload(
        *,
        request_header: dict[str, object],
        responder_ephemeral_public_key: str,
        signature_hex: str,
        keepalive_interval_seconds: int,
    ) -> bytes:
        return _build_packet_bytes(
            header=_build_response_header(request_header, "PHYSICAL_SESSION_INIT_OK"),
            payload={
                "responder_ephemeral_public_key": responder_ephemeral_public_key,
                "signature_hex": signature_hex,
                "keepalive_interval_seconds": keepalive_interval_seconds,
            },
        )

    @staticmethod
    def build_physical_session_key_confirm_payload(
        *,
        request_header: dict[str, object],
        encapsulated_key_hex: str,
    ) -> bytes:
        return _build_packet_bytes(
            header=_build_response_header(request_header, "PHYSICAL_SESSION_KEY_CONFIRM"),
            payload={
                "encapsulated_key_hex": encapsulated_key_hex,
            },
        )

    @staticmethod
    def build_physical_session_ready_payload(
        *,
        request_header: dict[str, object],
        keepalive_interval_seconds: int,
    ) -> bytes:
        return _build_packet_bytes(
            header=_build_response_header(request_header, "PHYSICAL_SESSION_READY"),
            payload={
                "status": "active",
                "keepalive_interval_seconds": keepalive_interval_seconds,
            },
        )

    @staticmethod
    def build_physical_session_keepalive_payload(
        *,
        header: dict[str, object],
    ) -> bytes:
        return _build_packet_bytes(
            header=header,
            payload={},
        )

    @staticmethod
    def build_physical_session_keepalive_ack_payload(
        *,
        request_header: dict[str, object],
    ) -> bytes:
        return _build_packet_bytes(
            header=_build_response_header(request_header, "PHYSICAL_SESSION_KEEPALIVE_ACK"),
            payload={},
        )

    @staticmethod
    def build_physical_session_close_payload(
        *,
        header: dict[str, object],
        close_reason: str,
    ) -> bytes:
        return _build_packet_bytes(
            header=header,
            payload={
                "close_reason": close_reason,
            },
        )

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "PHYSICAL_SESSION_INIT":
            return await self._handle_physical_session_init(envelope, context, services)

        if envelope.message_type == "PHYSICAL_SESSION_INIT_OK":
            return await self._handle_physical_session_init_ok(envelope, context, services)

        if envelope.message_type == "PHYSICAL_SESSION_KEY_CONFIRM":
            return await self._handle_physical_session_key_confirm(envelope, context, services)

        if envelope.message_type == "PHYSICAL_SESSION_READY":
            return await self._handle_physical_session_ready(envelope, context, services)

        if envelope.message_type == "PHYSICAL_SESSION_KEEPALIVE":
            return await self._handle_physical_session_keepalive(envelope, context, services)

        if envelope.message_type == "PHYSICAL_SESSION_KEEPALIVE_ACK":
            return await self._handle_physical_session_keepalive_ack(envelope, context, services)

        if envelope.message_type == "PHYSICAL_SESSION_CLOSE":
            return await self._handle_physical_session_close(envelope, context, services)

        if envelope.message_type == "OVERLAY_SESSION_INIT":
            return await self._handle_overlay_session_init(envelope, context, services)

        if envelope.message_type == "OVERLAY_SESSION_INIT_OK":
            return await self._handle_overlay_session_init_ok(envelope, context, services)

        if envelope.message_type == "OVERLAY_SESSION_KEY_CONFIRM":
            return await self._handle_overlay_session_key_confirm(envelope, context, services)

        if envelope.message_type == "OVERLAY_SESSION_READY":
            return await self._handle_overlay_session_ready(envelope, context, services)

        if envelope.message_type == "OVERLAY_SESSION_CLOSE":
            return await self._handle_overlay_session_close(envelope, context, services)

        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_physical_session_init(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_session_id(envelope)
        initiator_physical_node_id = payload.get("initiator_physical_node_id")
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )
        local_node = services.identity_service.get_local_physical_node_result()

        if local_node is None:
            return self._build_invalid_result(envelope, "local_physical_node_not_initialized")

        if not session_id or not isinstance(initiator_physical_node_id, str):
            return self._build_invalid_result(envelope, "invalid_physical_session_init")

        remote_node = services.identity_service.get_remote_physical_node_by_id(initiator_physical_node_id)
        session = services.session_manager.create_inbound_physical_session(
            session_id=session_id,
            local_physical_node_id=local_node.id,
            remote_physical_node_id=initiator_physical_node_id,
            remote_public_key=remote_node.public_key if remote_node else None,
            transport=context.transport_name,
            remote_host=context.remote_host,
            remote_port=context.remote_port,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )

        ephemeral_key_pair = generate_kyber_key_pair()
        services.session_manager.store_local_ephemeral_keypair(
            session_id,
            private_key_pem=ephemeral_key_pair.private_key_pem,
            public_key_pem=ephemeral_key_pair.public_key_pem,
            handshake_state="init_ok_sent",
        )

        signature_hex = self._sign_physical_session_init_ok(
            session_id=session_id,
            responder_ephemeral_public_key=ephemeral_key_pair.public_key_pem,
            keepalive_interval_seconds=keepalive_interval_seconds,
            local_private_key_pem=local_node.private_key_pem,
        )
        response_payload = self.build_physical_session_init_ok_payload(
            request_header=envelope.header,
            responder_ephemeral_public_key=ephemeral_key_pair.public_key_pem,
            signature_hex=signature_hex,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "physical_session_init_ok_sent",
                "session_id": session.session_id,
            },
        )

    async def _handle_physical_session_init_ok(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_session_id(envelope)
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
            return self._build_invalid_result(envelope, "invalid_physical_session_init_ok")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "physical_session_not_found")

        services.session_manager.bind_remote_endpoint(
            session_id,
            transport=context.transport_name,
            host=context.remote_host,
            port=context.remote_port,
        )

        remote_public_key = session.remote_public_key
        if not remote_public_key:
            return self._build_invalid_result(envelope, "remote_physical_node_public_key_not_found")

        signature_valid = self._verify_physical_session_init_ok(
            session_id=session_id,
            responder_ephemeral_public_key=responder_ephemeral_public_key,
            keepalive_interval_seconds=keepalive_interval_seconds,
            signature_hex=signature_hex,
            remote_public_key_pem=remote_public_key,
        )
        if not signature_valid:
            services.session_manager.close_session(session_id, close_reason="invalid_init_ok_signature")
            return self._build_invalid_result(envelope, "invalid_physical_session_init_ok_signature")

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
        response_payload = self.build_physical_session_key_confirm_payload(
            request_header=envelope.header,
            encapsulated_key_hex=encapsulation.ciphertext_hex,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "physical_session_key_confirm_sent",
                "session_id": session_id,
            },
        )

    async def _handle_physical_session_key_confirm(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_session_id(envelope)
        encapsulated_key_hex = payload.get("encapsulated_key_hex")

        if not session_id or not isinstance(encapsulated_key_hex, str):
            return self._build_invalid_result(envelope, "invalid_physical_session_key_confirm")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or not session.local_ephemeral_private_key:
            return self._build_invalid_result(envelope, "physical_session_ephemeral_key_not_found")

        services.session_manager.bind_remote_endpoint(
            session_id,
            transport=context.transport_name,
            host=context.remote_host,
            port=context.remote_port,
        )

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
        response_payload = self.build_physical_session_ready_payload(
            request_header=envelope.header,
            keepalive_interval_seconds=session.keepalive_interval_seconds,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "physical_session_ready_sent",
                "session_id": session_id,
            },
        )

    async def _handle_physical_session_ready(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_session_id(envelope)
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )

        if not session_id:
            return self._build_invalid_result(envelope, "invalid_physical_session_ready")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or not session.shared_secret_hex:
            return self._build_invalid_result(envelope, "physical_session_shared_secret_not_found")

        services.session_manager.bind_remote_endpoint(
            session_id,
            transport=context.transport_name,
            host=context.remote_host,
            port=context.remote_port,
        )

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
                "transport_name": context.transport_name,
                "action": "physical_session_activated",
                "session_id": session_id,
            },
        )

    async def _handle_physical_session_keepalive(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_id = _read_session_id(envelope)
        if not session_id:
            return self._build_invalid_result(envelope, "invalid_physical_session_keepalive")

        session = services.session_manager.touch_session(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "physical_session_not_found")

        services.session_manager.bind_remote_endpoint(
            session_id,
            transport=context.transport_name,
            host=context.remote_host,
            port=context.remote_port,
        )

        response_payload = self.build_physical_session_keepalive_ack_payload(
            request_header=envelope.header,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "physical_session_keepalive_ack_sent",
                "session_id": session_id,
            },
        )

    async def _handle_physical_session_keepalive_ack(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_id = _read_session_id(envelope)
        if not session_id:
            return self._build_invalid_result(envelope, "invalid_physical_session_keepalive_ack")

        session = services.session_manager.touch_session(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "physical_session_not_found")

        services.session_manager.bind_remote_endpoint(
            session_id,
            transport=context.transport_name,
            host=context.remote_host,
            port=context.remote_port,
        )

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "physical_session_keepalive_ack_received",
                "session_id": session_id,
            },
        )

    async def _handle_physical_session_close(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_session_id(envelope)
        close_reason = payload.get("close_reason")

        if not session_id:
            return self._build_invalid_result(envelope, "invalid_physical_session_close")

        session = services.session_manager.close_session(
            session_id,
            close_reason=close_reason if isinstance(close_reason, str) else "remote_closed",
        )
        if session is None:
            return self._build_invalid_result(envelope, "physical_session_not_found")

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "physical_session_closed",
                "session_id": session_id,
            },
        )

    async def _handle_overlay_session_init(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_overlay_session_init_ok(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_overlay_session_key_confirm(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_overlay_session_ready(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_overlay_session_close(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._build_not_implemented_result(envelope, context, services)

    def _sign_physical_session_init_ok(
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

    def _verify_physical_session_init_ok(
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
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "session_count": len(services.session_manager.list_sessions()),
                "next_step": "implement_overlay_session_protocol_flow",
            },
        )


def _build_packet_bytes(
    *,
    header: dict[str, object],
    payload: dict[str, object],
) -> bytes:
    packet = {"header": header, "payload": payload}
    return json.dumps(packet, separators=(",", ":"), sort_keys=True).encode("utf-8")


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
    }


def _canonical_payload_hex(payload: dict[str, object]) -> str:
    raw_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return raw_bytes.hex()


def _as_payload_dict(envelope: ProtocolEnvelope) -> dict[str, object]:
    return envelope.payload if isinstance(envelope.payload, dict) else {}


def _read_session_id(envelope: ProtocolEnvelope) -> str | None:
    session_id = envelope.header.get("physical_session_id")
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
