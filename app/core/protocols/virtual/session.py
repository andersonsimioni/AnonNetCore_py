from __future__ import annotations

from crypto import (
    dilithium_sign_hex,
    dilithium_verify_hex,
    generate_kyber_key_pair,
    kyber_decapsulate_hex,
    kyber_encapsulate_hex,
    sha512_hex,
)

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler
from ..helpers import (
    as_payload_dict as _as_payload_dict,
    build_response_header,
    canonical_payload_hex as _canonical_payload_hex,
    read_positive_keepalive_interval as _read_keepalive_interval,
    read_virtual_session_id as _read_virtual_session_id,
)


class VirtualSessionProtocolHandler(ProtocolMessageHandler):
    protocol_family = "virtual_session"
    supported_message_types = {
        "VIRTUAL_SESSION_INIT",
        "VIRTUAL_SESSION_INIT_OK",
        "VIRTUAL_SESSION_KEY_CONFIRM",
        "VIRTUAL_SESSION_READY",
        "VIRTUAL_SESSION_KEEPALIVE",
        "VIRTUAL_SESSION_KEEPALIVE_ACK",
        "VIRTUAL_SESSION_RELIABLE_DATA",
        "VIRTUAL_SESSION_RELIABLE_ACK",
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

        if envelope.message_type == "VIRTUAL_SESSION_RELIABLE_DATA":
            return await self._handle_virtual_session_reliable_data(envelope, context, services)

        if envelope.message_type == "VIRTUAL_SESSION_RELIABLE_ACK":
            return await self._handle_virtual_session_reliable_ack(envelope, context, services)

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
        initiator_virtual_node_public_key = payload.get("initiator_virtual_node_public_key")
        target_virtual_node_id = payload.get("target_virtual_node_id")
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )

        if (
            not session_id
            or not isinstance(initiator_virtual_node_id, str)
            or not isinstance(initiator_virtual_node_public_key, str)
            or not isinstance(target_virtual_node_id, str)
        ):
            return self._build_invalid_result(envelope, "invalid_virtual_session_init")
        if sha512_hex(initiator_virtual_node_public_key.encode("utf-8")) != initiator_virtual_node_id:
            return self._build_invalid_result(envelope, "invalid_initiator_virtual_node_public_key")

        local_virtual_node = services.identity_service.get_local_virtual_node_by_id(target_virtual_node_id)
        if local_virtual_node is None:
            return self._build_invalid_result(envelope, "local_virtual_node_not_found")

        services.identity_service.upsert_remote_virtual_node(
            node_id=initiator_virtual_node_id,
            public_key=initiator_virtual_node_public_key,
            kind="virtual_session_peer",
            status="active",
            metadata_json='{"source":"virtual_session_init"}',
        )
        route_path_id = context.metadata.get("route_path_id")
        session = services.session_manager.create_inbound_virtual_session(
            session_id=session_id,
            local_virtual_node_id=target_virtual_node_id,
            remote_virtual_node_id=initiator_virtual_node_id,
            remote_public_key=initiator_virtual_node_public_key,
            bound_route_id=route_path_id if isinstance(route_path_id, str) and route_path_id else None,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )
        session.remote_public_key = initiator_virtual_node_public_key
        services.log_service.info(
            "virtual_session",
            "accepted virtual session init",
            session_id=session_id,
            initiator_virtual_node_id=initiator_virtual_node_id,
            target_virtual_node_id=target_virtual_node_id,
            route_path_id=route_path_id,
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
                "responder_virtual_node_public_key": local_virtual_node.public_key,
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
        responder_virtual_node_public_key = payload.get("responder_virtual_node_public_key")
        responder_ephemeral_public_key = payload.get("responder_ephemeral_public_key")
        signature_hex = payload.get("signature_hex")
        keepalive_interval_seconds = _read_keepalive_interval(
            payload,
            services.config.physical_session_keepalive_seconds,
        )

        if (
            not session_id
            or not isinstance(responder_virtual_node_public_key, str)
            or not isinstance(responder_ephemeral_public_key, str)
            or not isinstance(signature_hex, str)
        ):
            return self._build_invalid_result(envelope, "invalid_virtual_session_init_ok")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            return self._build_invalid_result(envelope, "virtual_session_not_found")

        if sha512_hex(responder_virtual_node_public_key.encode("utf-8")) != session.remote_identity_id:
            services.session_manager.close_session(session_id, close_reason="invalid_responder_public_key")
            return self._build_invalid_result(envelope, "invalid_responder_virtual_node_public_key")

        session.remote_public_key = responder_virtual_node_public_key
        services.identity_service.upsert_remote_virtual_node(
            node_id=session.remote_identity_id,
            public_key=responder_virtual_node_public_key,
            kind="virtual_session_peer",
            status="active",
            metadata_json='{"source":"virtual_session_init_ok"}',
        )

        signature_valid = self._verify_virtual_session_init_ok(
            session_id=session_id,
            responder_ephemeral_public_key=responder_ephemeral_public_key,
            keepalive_interval_seconds=keepalive_interval_seconds,
            signature_hex=signature_hex,
            remote_public_key_pem=responder_virtual_node_public_key,
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
        services.log_service.info(
            "virtual_session",
            "validated init ok and sent key confirm",
            session_id=session_id,
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
        services.log_service.info(
            "virtual_session",
            "processed key confirm and activated inbound virtual session",
            session_id=session_id,
        )
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
        services.log_service.info(
            "virtual_session",
            "virtual session became active after ready message",
            session_id=session_id,
        )
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

    async def _handle_virtual_session_reliable_data(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        if not session_id:
            services.log_service.warning(
                "virtual_session_reliable",
                "received reliable data without virtual session id",
            )
            return self._build_invalid_result(envelope, "invalid_virtual_reliable_session_id")

        session = services.session_manager.touch_session(session_id)
        if session is None or session.session_scope != "virtual":
            services.log_service.warning(
                "virtual_session_reliable",
                "received reliable data for unknown virtual session",
                session_id=session_id,
            )
            return self._build_invalid_result(envelope, "virtual_session_not_found")

        try:
            receive_result = services.session_manager.receive_reliable_inbound(
                session_id=session_id,
                payload=payload,
            )
        except ValueError as error:
            services.log_service.warning(
                "virtual_session_reliable",
                "received invalid reliable data payload",
                session_id=session_id,
                reason=str(error),
            )
            return self._build_invalid_result(envelope, str(error))

        delivered_count = 0
        last_inner_result: PacketProcessingResult | None = None
        inner_response_envelopes: list[dict[str, object]] = []
        for inbound in receive_result.deliveries:
            inner_envelope = ProtocolEnvelope(
                protocol_name=envelope.protocol_name,
                message_type=inbound.inner_message_type,
                payload=inbound.inner_payload,
                raw_payload=envelope.raw_payload,
                header={
                    **envelope.header,
                    "message_type": inbound.inner_message_type,
                    "reliable_message_id": inbound.reliable_message_id,
                    "reliable_sequence_number": inbound.sequence_number,
                },
            )
            if services.engine is None:
                services.log_service.error(
                    "virtual_session_reliable",
                    "cannot dispatch reliable virtual payload without engine",
                    session_id=session_id,
                    reliable_message_id=inbound.reliable_message_id,
                    sequence_number=inbound.sequence_number,
                )
                continue

            last_inner_result = await services.engine.process_protocol_envelope(inner_envelope, context)
            delivered_count += 1
            if "virtual_response_envelope" in last_inner_result.metadata:
                candidate_response = last_inner_result.metadata.get("virtual_response_envelope")
                if isinstance(candidate_response, dict):
                    inner_response_envelopes.append(candidate_response)
                services.log_service.debug(
                    "virtual_session_reliable",
                    "inner reliable virtual payload produced a synchronous reply carried by reliable ack",
                    session_id=session_id,
                    reliable_message_id=inbound.reliable_message_id,
                    sequence_number=inbound.sequence_number,
                    inner_message_type=inbound.inner_message_type,
                )

        ack_sequence = receive_result.ack_payload.get("ack_for_sequence_number")
        ack_message_id = receive_result.ack_payload.get("ack_for_message_id")
        ack_payload = dict(receive_result.ack_payload)
        if inner_response_envelopes:
            ack_payload["inner_response_envelopes"] = inner_response_envelopes
        services.log_service.info(
            "virtual_session_reliable",
            "processed virtual reliable data and prepared ack",
            session_id=session_id,
            ack_for_sequence_number=ack_sequence,
            ack_for_message_id=ack_message_id,
            delivered_count=delivered_count,
            inner_response_count=len(inner_response_envelopes),
            duplicate=receive_result.duplicate,
            buffered=receive_result.buffered,
            next_pending_count=services.session_manager.count_pending_reliable_outbound(session_id),
        )
        return self._build_virtual_response_result(
            envelope,
            response_message_type="VIRTUAL_SESSION_RELIABLE_ACK",
            payload=ack_payload,
            extra_metadata={
                "action": "virtual_reliable_ack_sent",
                "session_id": session_id,
                "delivered_count": delivered_count,
                "inner_action": last_inner_result.metadata.get("action") if last_inner_result else None,
            },
        )

    async def _handle_virtual_session_reliable_ack(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        session_id = _read_virtual_session_id(envelope)
        ack_for_sequence = payload.get("ack_for_sequence_number")
        ack_for_message_id = payload.get("ack_for_message_id")
        if (
            not session_id
            or not isinstance(ack_for_sequence, int)
            or not isinstance(ack_for_message_id, str)
            or not ack_for_message_id
        ):
            services.log_service.warning(
                "virtual_session_reliable",
                "received invalid reliable ack",
                session_id=session_id,
                ack_for_sequence_number=ack_for_sequence,
                ack_for_message_id=ack_for_message_id,
            )
            return self._build_invalid_result(envelope, "invalid_virtual_reliable_ack")

        acked = services.session_manager.mark_reliable_outbound_acked(
            session_id=session_id,
            sequence_number=ack_for_sequence,
            reliable_message_id=ack_for_message_id,
        )
        services.session_manager.touch_session(session_id)
        if acked is None:
            services.log_service.warning(
                "virtual_session_reliable",
                "received reliable ack without matching pending outbound message",
                session_id=session_id,
                ack_for_sequence_number=ack_for_sequence,
                ack_for_message_id=ack_for_message_id,
            )
        else:
            services.log_service.info(
                "virtual_session_reliable",
                "received virtual reliable ack",
                session_id=session_id,
                ack_for_sequence_number=ack_for_sequence,
                ack_for_message_id=ack_for_message_id,
                attempts=acked.attempts,
                pending_count=services.session_manager.count_pending_reliable_outbound(session_id),
            )

        inner_response_envelopes = payload.get("inner_response_envelopes")
        if not isinstance(inner_response_envelopes, list):
            legacy_inner_response = payload.get("inner_response_envelope")
            inner_response_envelopes = [legacy_inner_response] if isinstance(legacy_inner_response, dict) else []

        inner_results: list[PacketProcessingResult] = []
        for inner_response_envelope in inner_response_envelopes:
            if not isinstance(inner_response_envelope, dict):
                services.log_service.warning(
                    "virtual_session_reliable",
                    "reliable ack carried a non-object inner response envelope",
                    session_id=session_id,
                )
                continue
            inner_result = await self._dispatch_inner_virtual_response_from_ack(
                envelope=envelope,
                context=context,
                services=services,
                session_id=session_id,
                inner_response_envelope=inner_response_envelope,
            )
            if inner_result is not None:
                inner_results.append(inner_result)

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": "virtual_reliable_ack_received",
                "session_id": session_id,
                "ack_matched": acked is not None,
                "inner_response_count": len(inner_results),
                "inner_action": inner_results[-1].metadata.get("action") if inner_results else None,
            },
        )

    async def _dispatch_inner_virtual_response_from_ack(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
        session_id: str,
        inner_response_envelope: dict[str, object],
    ) -> PacketProcessingResult | None:
        header = inner_response_envelope.get("header")
        payload = inner_response_envelope.get("payload")
        if not isinstance(header, dict) or not isinstance(payload, dict):
            services.log_service.warning(
                "virtual_session_reliable",
                "reliable ack carried invalid inner response envelope",
                session_id=session_id,
            )
            return None

        message_type = header.get("message_type")
        if not isinstance(message_type, str) or not message_type:
            services.log_service.warning(
                "virtual_session_reliable",
                "reliable ack carried inner response without message type",
                session_id=session_id,
            )
            return None
        if services.engine is None:
            services.log_service.error(
                "virtual_session_reliable",
                "cannot dispatch reliable ack inner response without engine",
                session_id=session_id,
                inner_message_type=message_type,
            )
            return None

        inner_envelope = ProtocolEnvelope(
            protocol_name=envelope.protocol_name,
            message_type=message_type,
            payload=payload,
            raw_payload=envelope.raw_payload,
            header=header,
        )
        result = await services.engine.process_protocol_envelope(inner_envelope, context)
        services.log_service.info(
            "virtual_session_reliable",
            "dispatched inner virtual response carried by reliable ack",
            session_id=session_id,
            inner_message_type=message_type,
            inner_action=result.metadata.get("action"),
        )
        return result

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
        services.log_service.info(
            "virtual_session",
            "received virtual session keepalive and sent ack",
            session_id=session_id,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            bound_route_id=session.bound_route_id,
        )
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
        services.log_service.info(
            "virtual_session",
            "received virtual session keepalive ack",
            session_id=session_id,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            bound_route_id=session.bound_route_id,
        )
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
    return build_response_header(request_header, message_type)
