from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import monotonic
from uuid import uuid4

from common import load_json_object
from crypto import (
    aes_decrypt_text,
    aes_encrypt_text,
    dilithium_sign_hex,
    dilithium_verify_hex,
    generate_kyber_key_pair,
    kyber_decapsulate_hex,
    kyber_encapsulate_hex,
    sha512_hex,
)
from dht import DrtRecordPayload, parse_record

from ..models import PacketProcessingResult
from .base import RouteStrategy


class RandomWalkTtlRouteStrategy(RouteStrategy):
    """Constroi rotas por random walk usando um budget temporal aproximado."""

    strategy_name = "random_walk_ttl_based"

    def find_valid_nonce(
        self,
        *,
        pk_final_physical_node: str,
        difficulty_bits: int,
    ) -> int:
        nonce = 0
        while True:
            route_create = RandomWalkTtlRouteCreate(
                pk_final_physical_node=_require_non_empty_string(
                    pk_final_physical_node,
                    field_name="pk_final_physical_node",
                ),
                remaining_ttl_ms=1,
                path_id="pow-probe",
                nonce=nonce,
            )
            if _is_valid_route_pow(
                route_create=route_create,
                difficulty_bits=difficulty_bits,
            ):
                return nonce
            nonce += 1

    def build_initial_route_create(
        self,
        *,
        pk_final_physical_node: str,
        remaining_ttl_ms: int,
        path_id: str,
        nonce: int,
    ) -> dict[str, object]:
        route_create = RandomWalkTtlRouteCreate(
            pk_final_physical_node=_require_non_empty_string(
                pk_final_physical_node,
                field_name="pk_final_physical_node",
            ),
            remaining_ttl_ms=_require_positive_int(
                remaining_ttl_ms,
                field_name="remaining_ttl_ms",
            ),
            path_id=_require_non_empty_string(path_id, field_name="path_id"),
            nonce=_require_non_negative_int(nonce, field_name="nonce"),
        )
        return route_create.to_payload(strategy_name=self.strategy_name)

    def build_route_pow_details(
        self,
        *,
        pk_final_physical_node: str,
        nonce: int | None,
        difficulty_bits: int,
    ) -> dict[str, object]:
        if nonce is None:
            return _build_route_pow_details(
                route_create=None,
                difficulty_bits=difficulty_bits,
            )

        route_create = RandomWalkTtlRouteCreate(
            pk_final_physical_node=_require_non_empty_string(
                pk_final_physical_node,
                field_name="pk_final_physical_node",
            ),
            remaining_ttl_ms=1,
            path_id="pow-probe",
            nonce=_require_non_negative_int(nonce, field_name="nonce"),
        )
        return _build_route_pow_details(
            route_create=route_create,
            difficulty_bits=difficulty_bits,
        )

    async def handle_route_create(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_create = self._parse_route_create(envelope.payload)
        pow_details = _build_route_pow_details(
            route_create=route_create,
            difficulty_bits=services.config.network_pow_difficulty_bits,
        )
        if not pow_details["is_valid"]:
            services.log_service.warning(
                "route_build",
                "received route create with invalid proof of work",
                path_id=route_create.path_id,
                route_nonce=route_create.nonce,
                remaining_ttl_ms=route_create.remaining_ttl_ms,
                pow_difficulty_bits=services.config.network_pow_difficulty_bits,
                pow_canonical_hash=pow_details["canonical_hash"],
                pow_proof_hash_prefix=pow_details["proof_hash_prefix"],
            )
            return self._build_invalid_result(envelope, reason="invalid_route_create_pow")

        services.log_service.debug(
            "route_build",
            "validated route create proof of work",
            path_id=route_create.path_id,
            route_nonce=route_create.nonce,
            remaining_ttl_ms=route_create.remaining_ttl_ms,
            pow_difficulty_bits=services.config.network_pow_difficulty_bits,
            pow_canonical_hash=pow_details["canonical_hash"],
            pow_proof_hash_prefix=pow_details["proof_hash_prefix"],
        )

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return self._build_invalid_result(envelope, reason="local_physical_node_not_initialized")

        final_physical_node_id = _build_physical_node_id(route_create.pk_final_physical_node)
        local_is_final_physical_node = local_node.public_key == route_create.pk_final_physical_node
        services.log_service.info(
            "route_build",
            "route create final node decision",
            path_id=route_create.path_id,
            route_nonce=route_create.nonce,
            local_physical_node_id=local_node.id,
            expected_final_physical_node_id=final_physical_node_id,
            local_is_final_physical_node=local_is_final_physical_node,
            physical_session_id=_read_header_value(envelope, "physical_session_id"),
            message_id=_read_header_value(envelope, "message_id"),
            message_sequence=_read_header_value(envelope, "message_sequence"),
            remaining_ttl_ms=route_create.remaining_ttl_ms,
        )

        if local_is_final_physical_node:
            return await self._handle_route_create_as_final_node(
                envelope=envelope,
                services=services,
                route_create=route_create,
            )

        return await self._handle_route_create_as_intermediary(
            envelope=envelope,
            services=services,
            route_create=route_create,
        )

    async def handle_route_create_kem_info(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_path_id = self._read_route_path_id(envelope.payload)
        kem_info = self._parse_route_create_kem_info(envelope.payload)
        reverse_path = self._resolve_reverse_path(
            services=services,
            path_id=route_path_id,
        )
        if reverse_path is not None:
            return self._build_forward_result(
                envelope=envelope,
                target_remote_physical_node_id=reverse_path.target_remote_physical_node_id,
                target_physical_session_id=reverse_path.target_physical_session_id,
                forward_message_type="ROUTE_CREATE_KEM_INFO",
                forward_payload=kem_info.to_payload(
                    strategy_name=self.strategy_name,
                    path_id=reverse_path.next_path_id,
                ),
                route_build_action=reverse_path.action,
            )

        return self._handle_local_route_create_kem_info(
            envelope=envelope,
            services=services,
            route_path_id=route_path_id,
            kem_info=kem_info,
        )

    async def handle_route_create_validate_and_publish(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        validation_request = self._parse_route_create_validate_and_publish(envelope.payload)
        forward_path = self._resolve_forward_path(
            services=services,
            path_id=validation_request.path_id,
        )
        if forward_path is not None:
            return self._build_forward_result(
                envelope=envelope,
                target_remote_physical_node_id=forward_path.target_remote_physical_node_id,
                forward_message_type="ROUTE_CREATE_VALIDATE_AND_PUBLISH",
                forward_payload=validation_request.to_payload(
                    strategy_name=self.strategy_name,
                    path_id=forward_path.next_path_id,
                ),
                route_build_action=forward_path.action,
            )

        return self._handle_local_route_create_validate_and_publish(
            envelope=envelope,
            services=services,
            validation_request=validation_request,
        )

    async def handle_route_create_ok(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_create_ok = self._parse_route_create_ok(envelope.payload)
        reverse_path = self._resolve_reverse_path(
            services=services,
            path_id=route_create_ok.path_id,
        )
        if reverse_path is not None:
            return self._build_forward_result(
                envelope=envelope,
                target_remote_physical_node_id=reverse_path.target_remote_physical_node_id,
                target_physical_session_id=reverse_path.target_physical_session_id,
                forward_message_type="ROUTE_CREATE_OK",
                forward_payload=route_create_ok.to_payload(
                    strategy_name=self.strategy_name,
                    path_id=reverse_path.next_path_id,
                ),
                route_build_action=reverse_path.action,
            )

        return await self._handle_local_route_create_ok(
            envelope=envelope,
            services=services,
            route_create_ok=route_create_ok,
        )

    async def handle_route_create_fail(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_create_fail = self._parse_route_create_fail(envelope.payload)
        reverse_path = self._resolve_reverse_path(
            services=services,
            path_id=route_create_fail.path_id,
        )
        if reverse_path is not None:
            invalidated = services.route_service.invalidate_hop_resolution_by_path_id(
                path_id=route_create_fail.path_id,
                reason=route_create_fail.reason,
            )
            services.log_service.info(
                "route_build",
                "route create fail invalidated hop and will continue reverse path",
                path_id=route_create_fail.path_id,
                next_path_id=reverse_path.next_path_id,
                target_remote_physical_node_id=reverse_path.target_remote_physical_node_id,
                reason=route_create_fail.reason,
                invalidated=invalidated is not None,
            )
            return self._build_forward_result(
                envelope=envelope,
                target_remote_physical_node_id=reverse_path.target_remote_physical_node_id,
                target_physical_session_id=reverse_path.target_physical_session_id,
                forward_message_type="ROUTE_CREATE_FAIL",
                forward_payload=route_create_fail.to_payload(
                    strategy_name=self.strategy_name,
                    path_id=reverse_path.next_path_id,
                ),
                route_build_action=reverse_path.action,
            )

        return self._handle_local_route_create_fail(
            envelope=envelope,
            services=services,
            route_create_fail=route_create_fail,
        )

    async def handle_route_create_ping(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_create_ping = self._parse_route_create_ping(envelope.payload)
        reverse_path = self._resolve_reverse_path(
            services=services,
            path_id=route_create_ping.path_id,
        )
        if reverse_path is not None:
            return self._build_forward_result(
                envelope=envelope,
                target_remote_physical_node_id=reverse_path.target_remote_physical_node_id,
                target_physical_session_id=reverse_path.target_physical_session_id,
                forward_message_type="ROUTE_CREATE_PING",
                forward_payload=route_create_ping.to_payload(
                    strategy_name=self.strategy_name,
                    path_id=reverse_path.next_path_id,
                ),
                route_build_action=reverse_path.action,
            )

        return self._handle_local_route_create_ping(
            envelope=envelope,
            services=services,
            route_create_ping=route_create_ping,
        )

    async def handle_route_create_pong(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_create_pong = self._parse_route_create_pong(envelope.payload)
        forward_path = self._resolve_forward_path(
            services=services,
            path_id=route_create_pong.path_id,
        )
        if forward_path is not None:
            services.log_service.debug(
                "route_build",
                "resolved route create pong forward path",
                current_path_id=route_create_pong.path_id,
                next_path_id=forward_path.next_path_id,
                target_remote_physical_node_id=forward_path.target_remote_physical_node_id,
                route_build_action=forward_path.action,
            )
            return self._build_forward_result(
                envelope=envelope,
                target_remote_physical_node_id=forward_path.target_remote_physical_node_id,
                forward_message_type="ROUTE_CREATE_PONG",
                forward_payload=route_create_pong.to_payload(
                    strategy_name=self.strategy_name,
                    path_id=forward_path.next_path_id,
                ),
                route_build_action=forward_path.action,
            )

        services.log_service.debug(
            "route_build",
            "route create pong has no forward mapping; trying local endpoint validation",
            path_id=route_create_pong.path_id,
            ping_id=route_create_pong.ping_id,
        )
        return await self._handle_local_route_create_pong(
            envelope=envelope,
            services=services,
            route_create_pong=route_create_pong,
        )

    async def _handle_route_create_as_intermediary(
        self,
        *,
        envelope,
        services,
        route_create: "RandomWalkTtlRouteCreate",
    ) -> PacketProcessingResult:
        from_physical_node_id = self._read_sender_physical_node_id(envelope, services)
        if from_physical_node_id is None:
            return self._build_invalid_result(envelope, reason="missing_sender_physical_node_id")

        forward_result = self._forward_route_create(
            from_physical_node_id=from_physical_node_id,
            from_physical_session_id=self._read_physical_session_id(envelope),
            services=services,
            route_create=route_create,
        )
        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=forward_result.next_remote_physical_node_id,
            forward_message_type="ROUTE_CREATE",
            forward_payload=forward_result.payload,
            route_build_action="forward_route_create",
            extra_metadata={
                "forward_result": {
                    "previous_path_id": forward_result.previous_path_id,
                    "selected_average_rtt_ms": forward_result.selected_average_rtt_ms,
                    "selected_one_way_rtt_ms": forward_result.selected_one_way_rtt_ms,
                    "next_remote_physical_node_id": forward_result.next_remote_physical_node_id,
                }
            },
        )

    async def _handle_route_create_as_final_node(
        self,
        *,
        envelope,
        services,
        route_create: "RandomWalkTtlRouteCreate",
    ) -> PacketProcessingResult:
        previous_physical_node_id = self._read_sender_physical_node_id(envelope, services)
        if previous_physical_node_id is None:
            return self._build_invalid_result(envelope, reason="missing_sender_physical_node_id")

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return self._build_invalid_result(envelope, reason="local_physical_node_not_initialized")

        services.log_service.info(
            "route_build",
            "route build trace reached final physical node",
            path_id=route_create.path_id,
            route_nonce=route_create.nonce,
            previous_physical_node_id=previous_physical_node_id,
            final_physical_node_id=local_node.id,
            remaining_ttl_ms=route_create.remaining_ttl_ms,
        )

        kyber_key_pair = generate_kyber_key_pair()
        physical_node_signature = _sign_route_kem_public_key_offer(
            kyber_public_key_pem=kyber_key_pair.public_key_pem,
            signing_private_key_pem=local_node.private_key_pem,
        )

        services.route_service.create_endpoint_resolution(
            previous_physical_node_id=previous_physical_node_id,
            route_strategy=self.strategy_name,
            route_nonce=route_create.nonce,
            route_path_id=route_create.path_id,
            kyber_private_key_pem=kyber_key_pair.private_key_pem,
            kyber_public_key_pem=kyber_key_pair.public_key_pem,
        )

        kem_info = RandomWalkTtlRouteCreateKemInfo(
            kyber_public_key_pem=kyber_key_pair.public_key_pem,
            physical_node_signature=physical_node_signature,
        )
        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=previous_physical_node_id,
            forward_message_type="ROUTE_CREATE_KEM_INFO",
            forward_payload=kem_info.to_payload(
                strategy_name=self.strategy_name,
                path_id=route_create.path_id,
            ),
            route_build_action="send_route_create_kem_info",
        )

    def _handle_local_route_create_kem_info(
        self,
        *,
        envelope,
        services,
        route_path_id: str,
        kem_info: "RandomWalkTtlRouteCreateKemInfo",
    ) -> PacketProcessingResult:
        initiator_state = services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=route_path_id,
        )
        if initiator_state is None:
            return self._build_invalid_result(envelope, reason="route_initiator_state_not_found")

        if not _is_valid_route_kem_public_key_offer_signature(
            kyber_public_key_pem=kem_info.kyber_public_key_pem,
            signature_hex=kem_info.physical_node_signature,
            physical_node_public_key_pem=initiator_state.final_physical_node_public_key,
        ):
            return self._build_invalid_result(
                envelope,
                reason="invalid_route_kem_public_key_offer_signature",
            )

        local_virtual_node = _select_route_local_virtual_node(
            services,
            initiator_state.local_virtual_node_id,
        )
        if local_virtual_node is None:
            return self._build_invalid_result(envelope, reason="local_virtual_node_not_initialized")

        final_path_id = str(uuid4())
        final_physical_node_id = _build_physical_node_id(
            initiator_state.final_physical_node_public_key
        )
        initiator_metadata = load_json_object(initiator_state.metadata_json)
        expected_round_trip_ttl_ms = initiator_metadata.get("expected_round_trip_ttl_ms")
        if not isinstance(expected_round_trip_ttl_ms, int) or expected_round_trip_ttl_ms <= 0:
            return self._build_invalid_result(
                envelope,
                reason="missing_expected_round_trip_ttl_ms",
            )
        virtual_node_signature = _sign_final_path_id(
            final_path_id=final_path_id,
            final_physical_node_id=final_physical_node_id,
            local_virtual_node_private_key_pem=local_virtual_node.private_key_encrypted,
        )
        encapsulation = kyber_encapsulate_hex(kem_info.kyber_public_key_pem)
        updated_state = services.route_service.update_initiator_resolution_validation_payload(
            initial_path_id=route_path_id,
            local_virtual_node_id=local_virtual_node.id,
            final_path_id=final_path_id,
            virtual_node_signature=virtual_node_signature,
            shared_secret_hex=encapsulation.shared_secret_hex,
        )
        if updated_state is None:
            return self._build_invalid_result(envelope, reason="route_initiator_state_not_found")

        services.log_service.info(
            "route_build",
            "route build trace received kem info at initiator",
            path_id=route_path_id,
            final_path_id=final_path_id,
            local_virtual_node_id=local_virtual_node.id,
            first_hop_physical_node_id=initiator_state.first_hop_physical_node_id,
            final_physical_node_id=final_physical_node_id,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            initiator_status=updated_state.status,
        )

        encrypted_payload = aes_encrypt_text(
            json.dumps(
                {
                    "virtual_node_id": local_virtual_node.id,
                    "virtual_node_public_key": local_virtual_node.public_key,
                    "final_path_id": final_path_id,
                    "final_physical_node_id": final_physical_node_id,
                    "expected_round_trip_ttl_ms": expected_round_trip_ttl_ms,
                    "virtual_node_signature": virtual_node_signature,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            encapsulation.shared_secret_hex,
            aad=_build_route_encrypted_payload_aad(
                message_type="ROUTE_CREATE_VALIDATE_AND_PUBLISH",
            ),
        )
        validation_request = RandomWalkTtlRouteCreateValidateAndPublish(
            path_id=route_path_id,
            kem_ciphertext_hex=encapsulation.ciphertext_hex,
            encrypted_payload_hex=encrypted_payload.payload_hex,
        )
        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=initiator_state.first_hop_physical_node_id,
            forward_message_type="ROUTE_CREATE_VALIDATE_AND_PUBLISH",
            forward_payload=validation_request.to_payload(
                strategy_name=self.strategy_name,
                path_id=route_path_id,
            ),
            route_build_action="send_route_create_validate_and_publish",
        )

    def _handle_local_route_create_validate_and_publish(
        self,
        *,
        envelope,
        services,
        validation_request: "RandomWalkTtlRouteCreateValidateAndPublish",
    ) -> PacketProcessingResult:
        route_endpoint_state = services.route_service.get_endpoint_resolution_by_path_id(
            route_path_id=validation_request.path_id,
        )
        if route_endpoint_state is None:
            return self._build_invalid_result(envelope, reason="route_endpoint_state_not_found")

        services.log_service.info(
            "route_build",
            "route build trace validation request reached final physical node",
            path_id=validation_request.path_id,
            endpoint_status=route_endpoint_state.status,
            previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
            route_nonce=route_endpoint_state.route_nonce,
        )

        shared_secret_hex = kyber_decapsulate_hex(
            validation_request.kem_ciphertext_hex,
            route_endpoint_state.kyber_private_key_pem,
        )
        decrypted_payload_json = aes_decrypt_text(
            validation_request.encrypted_payload_hex,
            shared_secret_hex,
            aad=_build_route_encrypted_payload_aad(
                message_type="ROUTE_CREATE_VALIDATE_AND_PUBLISH",
            ),
        )
        try:
            decrypted_payload = json.loads(decrypted_payload_json)
        except json.JSONDecodeError as error:
            return self._build_invalid_result(
                envelope,
                reason=f"invalid_route_validation_request_payload:{error}",
            )

        virtual_node_id = _read_required_string(decrypted_payload, "virtual_node_id")
        virtual_node_public_key = _read_required_string(decrypted_payload, "virtual_node_public_key")
        final_path_id = _read_required_string(decrypted_payload, "final_path_id")
        final_physical_node_id = _read_required_string(decrypted_payload, "final_physical_node_id")
        expected_round_trip_ttl_ms = _read_required_positive_int(
            decrypted_payload,
            "expected_round_trip_ttl_ms",
        )
        virtual_node_signature = _read_required_string(decrypted_payload, "virtual_node_signature")

        if _build_virtual_node_id(virtual_node_public_key) != virtual_node_id:
            return self._build_invalid_result(envelope, reason="virtual_node_id_public_key_mismatch")

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return self._build_invalid_result(envelope, reason="local_physical_node_not_initialized")

        expected_final_physical_node_id = _build_physical_node_id(local_node.public_key)
        if final_physical_node_id != expected_final_physical_node_id:
            return self._build_invalid_result(envelope, reason="invalid_final_physical_node_id")

        if not _is_valid_virtual_node_route_signature(
            final_path_id=final_path_id,
            final_physical_node_id=final_physical_node_id,
            signature_hex=virtual_node_signature,
            virtual_node_public_key_pem=virtual_node_public_key,
        ):
            return self._build_invalid_result(envelope, reason="invalid_virtual_node_route_signature")

        physical_node_signature = _sign_route_entry_point_acceptance(
            virtual_node_id=virtual_node_id,
            final_path_id=final_path_id,
            virtual_node_signature=virtual_node_signature,
            local_physical_node_private_key_pem=local_node.private_key_pem,
        )
        public_route_acceptance_signature = _sign_public_route_acceptance(
            route_strategy=route_endpoint_state.route_strategy,
            final_physical_node_public_key=local_node.public_key,
            route_nonce=route_endpoint_state.route_nonce,
            local_physical_node_private_key_pem=local_node.private_key_pem,
        )

        ping_id = str(uuid4())
        updated_endpoint_resolution = services.route_service.update_endpoint_resolution_validation_context(
            route_path_id=validation_request.path_id,
            shared_secret_hex=shared_secret_hex,
            final_path_id=final_path_id,
            remote_virtual_node_public_key=virtual_node_public_key,
            virtual_node_signature=virtual_node_signature,
            physical_node_signature=physical_node_signature,
            public_route_acceptance_signature=public_route_acceptance_signature,
            remote_virtual_node_id=virtual_node_id,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            ping_id=ping_id,
            ping_sent_at_monotonic_ms=monotonic() * 1000.0,
        )
        if updated_endpoint_resolution is None:
            return self._build_invalid_result(envelope, reason="route_endpoint_state_not_found")

        services.log_service.info(
            "route_build",
            "route build trace validation accepted by final physical node",
            path_id=validation_request.path_id,
            final_path_id=final_path_id,
            ping_id=ping_id,
            virtual_node_id=virtual_node_id,
            final_physical_node_id=final_physical_node_id,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            endpoint_status=updated_endpoint_resolution.status,
        )

        services.log_service.debug(
            "route_build",
            "prepared route ping validation",
            path_id=validation_request.path_id,
            final_path_id=final_path_id,
            ping_id=ping_id,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
            remote_virtual_node_id=virtual_node_id,
            endpoint_status=updated_endpoint_resolution.status,
        )

        route_create_ping = RandomWalkTtlRouteCreatePing(
            path_id=validation_request.path_id,
            ping_id=ping_id,
        )
        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=route_endpoint_state.previous_physical_node_id,
            forward_message_type="ROUTE_CREATE_PING",
            forward_payload=route_create_ping.to_payload(
                strategy_name=self.strategy_name,
                path_id=validation_request.path_id,
            ),
            route_build_action="send_route_create_ping",
        )

    async def _handle_local_route_create_ok(
        self,
        *,
        envelope,
        services,
        route_create_ok: "RandomWalkTtlRouteCreateOk",
    ) -> PacketProcessingResult:
        initiator_state = services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=route_create_ok.path_id,
        )
        if initiator_state is None or not initiator_state.shared_secret_hex:
            return self._build_invalid_result(envelope, reason="route_initiator_shared_secret_not_found")

        decrypted_payload_json = aes_decrypt_text(
            route_create_ok.encrypted_payload_hex,
            initiator_state.shared_secret_hex,
            aad=_build_route_encrypted_payload_aad(
                message_type="ROUTE_CREATE_OK",
            ),
        )
        try:
            decrypted_payload = json.loads(decrypted_payload_json)
        except json.JSONDecodeError as error:
            return self._build_invalid_result(
                envelope,
                reason=f"invalid_route_create_ok_payload:{error}",
            )

        virtual_node_id = _read_required_string(decrypted_payload, "virtual_node_id")
        final_path_id = _read_required_string(decrypted_payload, "final_path_id")
        virtual_node_signature = _read_required_string(decrypted_payload, "virtual_node_signature")
        physical_node_signature = _read_required_string(decrypted_payload, "physical_node_signature")

        if initiator_state.local_virtual_node_id and virtual_node_id != initiator_state.local_virtual_node_id:
            return self._build_invalid_result(envelope, reason="invalid_route_create_ok_virtual_node_id")

        if initiator_state.final_path_id and final_path_id != initiator_state.final_path_id:
            return self._build_invalid_result(envelope, reason="invalid_route_create_ok_final_path_id")

        services.log_service.info(
            "route_build",
            "route build trace route ok reached initiator",
            path_id=route_create_ok.path_id,
            final_path_id=final_path_id,
            virtual_node_id=virtual_node_id,
            initiator_status=initiator_state.status,
        )

        if (
            initiator_state.virtual_node_signature
            and virtual_node_signature != initiator_state.virtual_node_signature
        ):
            return self._build_invalid_result(
                envelope,
                reason="invalid_route_create_ok_virtual_node_signature",
            )

        if not _is_valid_route_entry_point_acceptance_signature(
            virtual_node_id=virtual_node_id,
            final_path_id=final_path_id,
            virtual_node_signature=virtual_node_signature,
            signature_hex=physical_node_signature,
            physical_node_public_key_pem=initiator_state.final_physical_node_public_key,
        ):
            return self._build_invalid_result(
                envelope,
                reason="invalid_route_create_ok_physical_node_signature",
            )

        if not initiator_state.route_strategy or initiator_state.route_nonce is None:
            return self._build_invalid_result(
                envelope,
                reason="route_initiator_public_route_context_not_found",
            )

        if not _is_valid_public_route_acceptance_signature(
            route_strategy=initiator_state.route_strategy,
            final_physical_node_public_key=initiator_state.final_physical_node_public_key,
            route_nonce=initiator_state.route_nonce,
            signature_hex=route_create_ok.public_route_acceptance_signature,
            physical_node_public_key_pem=initiator_state.final_physical_node_public_key,
        ):
            return self._build_invalid_result(
                envelope,
                reason="invalid_public_route_acceptance_signature",
            )

        drt_ready = await self._is_route_visible_in_drt(
            services=services,
            virtual_node_id=virtual_node_id,
            final_path_id=final_path_id,
        )
        if not drt_ready:
            services.route_service.invalidate_initiator_resolution(
                initial_path_id=route_create_ok.path_id,
                reason="route_create_ok_drt_entry_not_visible",
            )
            services.log_service.warning(
                "route_build",
                "route create ok rejected because published drt route is not visible",
                path_id=route_create_ok.path_id,
                virtual_node_id=virtual_node_id,
                final_path_id=final_path_id,
            )
            return self._build_invalid_result(
                envelope,
                reason="route_create_ok_drt_entry_not_visible",
            )

        services.route_service.mark_initiator_resolution_active(
            initial_path_id=route_create_ok.path_id,
            final_path_id=final_path_id,
            virtual_node_signature=virtual_node_signature,
            physical_node_signature=physical_node_signature,
            public_route_acceptance_signature=route_create_ok.public_route_acceptance_signature,
        )
        services.log_service.info(
            "route_build",
            "route build trace initiator route marked active",
            path_id=route_create_ok.path_id,
            final_path_id=final_path_id,
            virtual_node_id=virtual_node_id,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "route_build",
                "route_build_action": "deliver_local",
                "route_strategy": self.strategy_name,
                "path_id": route_create_ok.path_id,
                "virtual_node_id": virtual_node_id,
                "final_path_id": final_path_id,
                "virtual_node_signature": virtual_node_signature,
                "physical_node_signature": physical_node_signature,
                "public_route_acceptance_signature": route_create_ok.public_route_acceptance_signature,
                "next_step": "route_ready_after_drt_publish",
            },
        )

    def _handle_local_route_create_fail(
        self,
        *,
        envelope,
        services,
        route_create_fail: "RandomWalkTtlRouteCreateFail",
    ) -> PacketProcessingResult:
        invalidated = services.route_service.invalidate_initiator_resolution(
            initial_path_id=route_create_fail.path_id,
            reason=route_create_fail.reason,
        )
        services.log_service.warning(
            "route_build",
            "route create fail reached initiator",
            path_id=route_create_fail.path_id,
            reason=route_create_fail.reason,
            invalidated=invalidated is not None,
        )
        if invalidated is None:
            return self._build_invalid_result(envelope, reason="route_initiator_state_not_found")

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "route_build",
                "route_build_action": "route_create_failed",
                "route_strategy": self.strategy_name,
                "path_id": route_create_fail.path_id,
                "reason": route_create_fail.reason,
            },
        )

    async def _is_route_visible_in_drt(
        self,
        *,
        services,
        virtual_node_id: str,
        final_path_id: str,
    ) -> bool:
        deadline = monotonic() + services.config.route_create_ok_drt_visibility_timeout_seconds
        retry_seconds = services.config.route_create_ok_drt_visibility_retry_seconds
        last_result: dict[str, object] | None = None
        attempt = 0

        while monotonic() < deadline:
            attempt += 1
            result = await services.protocol_clients.physical.dht.query(
                namespace="drt",
                logical_key=virtual_node_id,
            )
            last_result = result
            if self._drt_query_result_contains_route(
                services=services,
                result=result,
                virtual_node_id=virtual_node_id,
                final_path_id=final_path_id,
                attempt=attempt,
            ):
                return True

            await asyncio.sleep(retry_seconds)

        services.log_service.warning(
            "route_build",
            "drt route visibility timed out",
            virtual_node_id=virtual_node_id,
            final_path_id=final_path_id,
            timeout_seconds=services.config.route_create_ok_drt_visibility_timeout_seconds,
            retry_seconds=retry_seconds,
            attempts=attempt,
            last_status=last_result.get("status") if last_result else None,
            last_reason=last_result.get("reason") if last_result else None,
        )
        return False

    def _drt_query_result_contains_route(
        self,
        *,
        services,
        result: dict[str, object],
        virtual_node_id: str,
        final_path_id: str,
        attempt: int,
    ) -> bool:
        if result.get("status") != "found":
            services.log_service.debug(
                "route_build",
                "drt route visibility check did not find record",
                virtual_node_id=virtual_node_id,
                final_path_id=final_path_id,
                attempt=attempt,
                dht_status=result.get("status"),
                dht_reason=result.get("reason"),
            )
            return False

        record_json = result.get("record_json")
        if not isinstance(record_json, str) or not record_json:
            services.log_service.warning(
                "route_build",
                "drt route visibility check found invalid record json",
                virtual_node_id=virtual_node_id,
                final_path_id=final_path_id,
                attempt=attempt,
            )
            return False

        try:
            record = parse_record("drt", record_json)
        except ValueError as error:
            services.log_service.warning(
                "route_build",
                "drt route visibility check failed to parse record",
                virtual_node_id=virtual_node_id,
                final_path_id=final_path_id,
                attempt=attempt,
                error=str(error),
            )
            return False

        if not isinstance(record, DrtRecordPayload):
            return False
        if _build_virtual_node_id(record.pk_virtual_node) != virtual_node_id:
            services.log_service.warning(
                "route_build",
                "drt route visibility check found record for another virtual node",
                expected_virtual_node_id=virtual_node_id,
                record_virtual_node_id=_build_virtual_node_id(record.pk_virtual_node),
                final_path_id=final_path_id,
                attempt=attempt,
            )
            return False

        route_found = any(entry.final_path_id == final_path_id for entry in record.route_entries)
        services.log_service.debug(
            "route_build",
            "drt route visibility check completed",
            virtual_node_id=virtual_node_id,
            final_path_id=final_path_id,
            attempt=attempt,
            route_found=route_found,
            route_entry_count=len(record.route_entries),
        )
        return route_found

    def _handle_local_route_create_ping(
        self,
        *,
        envelope,
        services,
        route_create_ping: "RandomWalkTtlRouteCreatePing",
    ) -> PacketProcessingResult:
        initiator_state = services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=route_create_ping.path_id,
        )
        if initiator_state is None or not initiator_state.first_hop_physical_node_id:
            return self._build_invalid_result(envelope, reason="route_initiator_state_not_found")

        route_create_pong = RandomWalkTtlRouteCreatePong(
            path_id=route_create_ping.path_id,
            ping_id=route_create_ping.ping_id,
        )
        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=initiator_state.first_hop_physical_node_id,
            forward_message_type="ROUTE_CREATE_PONG",
            forward_payload=route_create_pong.to_payload(
                strategy_name=self.strategy_name,
                path_id=route_create_ping.path_id,
            ),
            route_build_action="send_route_create_pong",
        )

    async def _handle_local_route_create_pong(
        self,
        *,
        envelope,
        services,
        route_create_pong: "RandomWalkTtlRouteCreatePong",
    ) -> PacketProcessingResult:
        services.log_service.debug(
            "route_build",
            "received local route create pong",
            path_id=route_create_pong.path_id,
            ping_id=route_create_pong.ping_id,
        )
        route_endpoint_state = services.route_service.get_endpoint_resolution_by_path_id(
            route_path_id=route_create_pong.path_id,
        )
        if route_endpoint_state is None or not route_endpoint_state.shared_secret_hex:
            services.log_service.warning(
                "route_build",
                "route create pong rejected because endpoint state is missing",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
                has_endpoint_state=route_endpoint_state is not None,
                has_shared_secret=(
                    route_endpoint_state.shared_secret_hex is not None
                    if route_endpoint_state is not None
                    else False
                ),
            )
            return self._build_invalid_result(envelope, reason="route_endpoint_state_not_found")

        metadata = load_json_object(route_endpoint_state.metadata_json)
        last_ping_id = metadata.get("last_ping_id")
        started_at_monotonic_ms = metadata.get("last_ping_sent_at_monotonic_ms")
        expected_round_trip_ttl_ms = metadata.get("expected_round_trip_ttl_ms")
        remote_virtual_node_id = metadata.get("remote_virtual_node_id")

        services.log_service.debug(
            "route_build",
            "loaded route pong validation context",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            route_status=route_endpoint_state.status,
            previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
            received_ping_id=route_create_pong.ping_id,
            expected_ping_id=last_ping_id,
            ping_started_at_monotonic_ms=started_at_monotonic_ms,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            remote_virtual_node_id=remote_virtual_node_id,
            has_virtual_node_signature=bool(route_endpoint_state.virtual_node_signature),
            has_physical_node_signature=bool(route_endpoint_state.physical_node_signature),
            has_public_route_acceptance_signature=bool(
                route_endpoint_state.public_route_acceptance_signature
            ),
        )
        services.log_service.info(
            "route_build",
            "route build trace pong reached final physical node",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            ping_id=route_create_pong.ping_id,
            expected_ping_id=last_ping_id,
            endpoint_status=route_endpoint_state.status,
            previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            remote_virtual_node_id=remote_virtual_node_id,
        )

        if last_ping_id != route_create_pong.ping_id:
            services.log_service.warning(
                "route_build",
                "route create pong rejected because ping id does not match",
                path_id=route_create_pong.path_id,
                received_ping_id=route_create_pong.ping_id,
                expected_ping_id=last_ping_id,
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="unexpected_route_create_pong",
            )
        if not isinstance(started_at_monotonic_ms, (int, float)):
            services.log_service.warning(
                "route_build",
                "route create pong rejected because ping start time is missing",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
                ping_started_at_monotonic_ms=started_at_monotonic_ms,
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="missing_route_ping_start_time",
            )
        if not isinstance(remote_virtual_node_id, str) or not remote_virtual_node_id:
            services.log_service.warning(
                "route_build",
                "route create pong rejected because remote virtual node id is missing",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
                remote_virtual_node_id=remote_virtual_node_id,
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="missing_remote_virtual_node_id",
            )

        observed_round_trip_ms = (monotonic() * 1000.0) - float(started_at_monotonic_ms)
        if not isinstance(expected_round_trip_ttl_ms, (int, float)):
            services.log_service.warning(
                "route_build",
                "route create pong rejected because expected ttl is missing",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
                expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
                observed_round_trip_ms=round(observed_round_trip_ms, 3),
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="missing_expected_round_trip_ttl_ms",
            )

        route_error_ms = services.config.random_walk_ttl_acceptance_error_ms
        lower = float(expected_round_trip_ttl_ms - route_error_ms)
        upper = float(expected_round_trip_ttl_ms + route_error_ms)
        is_within_expected_ttl = observed_round_trip_ms <= upper
        services.log_service.debug(
            "route_build",
            "calculated route pong rtt window",
            path_id=route_create_pong.path_id,
            ping_id=route_create_pong.ping_id,
            observed_round_trip_ms=round(observed_round_trip_ms, 3),
            observed_round_trip_ms_rounded=int(round(observed_round_trip_ms)),
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            allowed_error_ms=route_error_ms,
            allowed_lower_ms=lower,
            allowed_upper_ms=upper,
            is_within_expected_ttl=is_within_expected_ttl,
        )
        if not is_within_expected_ttl:
            services.log_service.warning(
                "route_build",
                "route create pong rejected because observed rtt exceeded ttl window",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
                observed_round_trip_ms=round(observed_round_trip_ms, 3),
                expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
                allowed_lower_ms=lower,
                allowed_upper_ms=upper,
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="route_round_trip_ttl_exceeded",
            )

        route_endpoint_state = services.route_service.mark_endpoint_resolution_active(
            route_path_id=route_create_pong.path_id,
        )
        if route_endpoint_state is None:
            services.log_service.warning(
                "route_build",
                "route create pong could not activate endpoint resolution",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
            )
            return self._build_invalid_result(envelope, reason="route_endpoint_state_not_found")
        services.log_service.debug(
            "route_build",
            "endpoint route resolution activated after pong",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            status=route_endpoint_state.status,
            previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
        )

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            services.log_service.warning(
                "route_build",
                "route create pong rejected because local physical node is missing",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
            )
            return self._build_invalid_result(envelope, reason="local_physical_node_not_initialized")

        drt_expires_at = _build_drt_entry_expires_at()
        observed_round_trip_ms_rounded = int(round(observed_round_trip_ms))
        services.log_service.debug(
            "route_build",
            "signing route rtt for drt publication",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            physical_node_id=local_node.id,
            rtt=observed_round_trip_ms_rounded,
            expires_at=drt_expires_at,
        )
        drt_rtt_physical_node_signature = _sign_drt_route_rtt(
            pk_physical_node=local_node.public_key,
            expires_at=drt_expires_at,
            rtt=observed_round_trip_ms_rounded,
            local_physical_node_private_key_pem=local_node.private_key_pem,
        )
        drt_publish_request = services.route_service.build_drt_publish_request_from_endpoint_resolution(
            route_path_id=route_create_pong.path_id,
            physical_node_public_key=local_node.public_key,
            rtt_physical_node_signature=drt_rtt_physical_node_signature,
            observed_round_trip_ms=observed_round_trip_ms_rounded,
            expires_at=drt_expires_at,
        )
        if drt_publish_request is None:
            services.log_service.warning(
                "route_build",
                "route create pong could not build drt publish request",
                path_id=route_create_pong.path_id,
                ping_id=route_create_pong.ping_id,
                final_path_id=route_endpoint_state.final_path_id,
                remote_virtual_node_id=remote_virtual_node_id,
                has_virtual_node_signature=bool(route_endpoint_state.virtual_node_signature),
                has_physical_node_signature=bool(route_endpoint_state.physical_node_signature),
                has_final_path_id=bool(route_endpoint_state.final_path_id),
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="drt_publish_request_not_available",
            )

        services.log_service.debug(
            "route_build",
            "built drt publish request after pong",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            namespace=drt_publish_request["namespace"],
            logical_key=drt_publish_request["logical_key"],
            expires_at=drt_publish_request["expires_at"],
            record_json_size=len(drt_publish_request["record_json"]),
            rtt=observed_round_trip_ms_rounded,
        )
        services.log_service.info(
            "route_build",
            "publishing route in drt before route ok",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            virtual_node_id=remote_virtual_node_id,
            observed_round_trip_ms=observed_round_trip_ms_rounded,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            logical_key=drt_publish_request["logical_key"],
        )
        drt_key = services.dht_service.build_key(
            drt_publish_request["namespace"],
            drt_publish_request["logical_key"],
        )
        drt_publish_result = await self._publish_drt_route_after_pong(
            services=services,
            drt_publish_request=drt_publish_request,
            drt_key=drt_key,
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            virtual_node_id=remote_virtual_node_id,
        )
        if drt_publish_result is None or drt_publish_result.get("status") != "stored":
            services.log_service.warning(
                "route_build",
                "route create pong rejected because drt publication did not complete",
                path_id=route_create_pong.path_id,
                final_path_id=route_endpoint_state.final_path_id,
                virtual_node_id=remote_virtual_node_id,
                drt_status=(
                    drt_publish_result.get("status")
                    if isinstance(drt_publish_result, dict)
                    else None
                ),
                drt_reason=(
                    drt_publish_result.get("reason")
                    if isinstance(drt_publish_result, dict)
                    else None
                ),
            )
            return self._build_route_create_fail_result(
                envelope=envelope,
                services=services,
                route_path_id=route_create_pong.path_id,
                route_endpoint_state=route_endpoint_state,
                reason="drt_publish_failed",
            )

        services.log_service.info(
            "route_build",
            "route drt publication confirmed before route ok",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            virtual_node_id=remote_virtual_node_id,
            key=drt_publish_result.get("key"),
            stored_count=drt_publish_result.get("stored_count"),
            required_stored_count=drt_publish_result.get("required_stored_count"),
        )

        encrypted_ok_payload = aes_encrypt_text(
            json.dumps(
                {
                    "virtual_node_id": remote_virtual_node_id,
                    "final_path_id": route_endpoint_state.final_path_id,
                    "virtual_node_signature": route_endpoint_state.virtual_node_signature,
                    "physical_node_signature": route_endpoint_state.physical_node_signature,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            route_endpoint_state.shared_secret_hex,
            aad=_build_route_encrypted_payload_aad(
                message_type="ROUTE_CREATE_OK",
            ),
        )
        route_create_ok = RandomWalkTtlRouteCreateOk(
            path_id=route_create_pong.path_id,
            encrypted_payload_hex=encrypted_ok_payload.payload_hex,
            public_route_acceptance_signature=route_endpoint_state.public_route_acceptance_signature,
        )
        services.log_service.debug(
            "route_build",
            "built route create ok after pong",
            path_id=route_create_pong.path_id,
            final_path_id=route_endpoint_state.final_path_id,
            target_previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
            encrypted_payload_size=len(encrypted_ok_payload.payload_hex),
            has_public_route_acceptance_signature=bool(
                route_endpoint_state.public_route_acceptance_signature
            ),
            drt_status="stored",
        )

        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=route_endpoint_state.previous_physical_node_id,
            forward_message_type="ROUTE_CREATE_OK",
            forward_payload=route_create_ok.to_payload(
                strategy_name=self.strategy_name,
                path_id=route_create_pong.path_id,
            ),
            route_build_action="send_route_create_ok",
            extra_metadata={
                "observed_round_trip_ms": observed_round_trip_ms,
                "expected_round_trip_ttl_ms": expected_round_trip_ttl_ms,
                "is_within_expected_ttl": True,
                "final_path_id": route_endpoint_state.final_path_id,
            },
        )

    def _build_route_create_fail_result(
        self,
        *,
        envelope,
        services,
        route_path_id: str,
        route_endpoint_state,
        reason: str,
    ) -> PacketProcessingResult:
        if not route_endpoint_state.previous_physical_node_id:
            services.log_service.warning(
                "route_build",
                "route create fail cannot be sent because previous hop is missing",
                path_id=route_path_id,
                reason=reason,
            )
            return self._build_invalid_result(envelope, reason=reason)

        route_create_fail = RandomWalkTtlRouteCreateFail(
            path_id=route_path_id,
            reason=reason,
        )
        invalidated_endpoint = services.route_service.invalidate_endpoint_resolution(
            route_path_id=route_path_id,
            reason=reason,
        )
        services.log_service.warning(
            "route_build",
            "sending route create fail through reverse path",
            path_id=route_path_id,
            final_path_id=route_endpoint_state.final_path_id,
            previous_physical_node_id=route_endpoint_state.previous_physical_node_id,
            reason=reason,
            invalidated_endpoint=invalidated_endpoint is not None,
        )
        return self._build_forward_result(
            envelope=envelope,
            target_remote_physical_node_id=route_endpoint_state.previous_physical_node_id,
            forward_message_type="ROUTE_CREATE_FAIL",
            forward_payload=route_create_fail.to_payload(
                strategy_name=self.strategy_name,
                path_id=route_path_id,
            ),
            route_build_action="send_route_create_fail",
        )

    async def _publish_drt_route_after_pong(
        self,
        *,
        services,
        drt_publish_request: dict[str, object],
        drt_key: str,
        path_id: str,
        final_path_id: str | None,
        virtual_node_id: str,
    ) -> dict[str, object] | None:
        try:
            trace_context = {
                "source": "route_create_drt_publish",
                "path_id": path_id,
                "final_path_id": final_path_id,
                "virtual_node_id": virtual_node_id,
                "drt_key": drt_key,
            }
            services.log_service.debug(
                "route_build",
                "route build trace publishing drt through dht",
                **trace_context,
            )
            publish_result = await services.protocol_clients.physical.dht.publish(
                namespace=str(drt_publish_request["namespace"]),
                logical_key=str(drt_publish_request["logical_key"]),
                record_json=str(drt_publish_request["record_json"]),
                expires_at=str(drt_publish_request["expires_at"]),
                trace_context=trace_context,
            )
        except asyncio.TimeoutError:
            services.log_service.warning(
                "route_build",
                "drt publish ack timed out after route pong validation",
                path_id=path_id,
                final_path_id=final_path_id,
                virtual_node_id=virtual_node_id,
                key=drt_key,
            )
            return None
        except Exception as error:
            services.log_service.warning(
                "route_build",
                "drt publish failed after route pong validation",
                path_id=path_id,
                final_path_id=final_path_id,
                virtual_node_id=virtual_node_id,
                key=drt_key,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return None

        services.log_service.info(
            "route_build",
            "published route in drt",
            path_id=path_id,
            final_path_id=final_path_id,
            virtual_node_id=virtual_node_id,
            status=publish_result.get("status"),
            key=publish_result.get("key"),
            stored_count=publish_result.get("stored_count"),
            required_stored_count=publish_result.get("required_stored_count"),
            stored_by=publish_result.get("stored_by"),
            reason=publish_result.get("reason"),
        )
        return publish_result

    def _forward_route_create(
        self,
        *,
        from_physical_node_id: str,
        from_physical_session_id: str | None,
        services,
        route_create: "RandomWalkTtlRouteCreate",
    ) -> "ForwardRouteCreateResult":
        final_physical_node_id = _build_physical_node_id(route_create.pk_final_physical_node)
        selected_candidate = self._select_next_candidate(
            services=services,
            final_physical_node_id=final_physical_node_id,
            previous_physical_node_id=from_physical_node_id,
            remaining_ttl_ms=route_create.remaining_ttl_ms,
        )

        selected_one_way_rtt_ms = _build_one_way_rtt_ms(selected_candidate.average_rtt_ms)
        next_remaining_ttl_ms = max(1, int(route_create.remaining_ttl_ms - selected_one_way_rtt_ms))
        next_path_id = str(uuid4())
        services.log_service.debug(
            "route_build",
            "selected random walk next hop",
            current_path_id=route_create.path_id,
            next_path_id=next_path_id,
            previous_physical_node_id=from_physical_node_id,
            selected_physical_node_id=selected_candidate.node_id,
            final_physical_node_id=final_physical_node_id,
            selection_reason=selected_candidate.selection_reason,
            selected_average_rtt_ms=selected_candidate.average_rtt_ms,
            selected_one_way_rtt_ms=selected_one_way_rtt_ms,
            remaining_ttl_ms=route_create.remaining_ttl_ms,
            next_remaining_ttl_ms=next_remaining_ttl_ms,
        )

        services.route_service.create_hop_resolution(
            route_strategy=self.strategy_name,
            from_physical_node_id=from_physical_node_id,
            to_physical_node_id=selected_candidate.node_id,
            received_path_id=route_create.path_id,
            generated_path_id=next_path_id,
            from_physical_session_id=from_physical_session_id,
        )

        next_payload = self.build_initial_route_create(
            pk_final_physical_node=route_create.pk_final_physical_node,
            remaining_ttl_ms=next_remaining_ttl_ms,
            path_id=next_path_id,
            nonce=route_create.nonce,
        )
        return ForwardRouteCreateResult(
            next_remote_physical_node_id=selected_candidate.node_id,
            previous_path_id=route_create.path_id,
            selected_average_rtt_ms=selected_candidate.average_rtt_ms,
            selected_one_way_rtt_ms=selected_one_way_rtt_ms,
            payload=next_payload,
        )

    def _parse_route_create(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreate":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreate(
            pk_final_physical_node=_read_required_string(payload_dict, "pk_final_physical_node"),
            remaining_ttl_ms=_read_required_positive_int(payload_dict, "remaining_ttl_ms"),
            path_id=_read_required_string(payload_dict, "path_id"),
            nonce=_read_required_nonce(payload_dict, "nonce"),
        )

    def _parse_route_create_kem_info(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreateKemInfo":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreateKemInfo(
            kyber_public_key_pem=_read_required_string(payload_dict, "kyber_public_key_pem"),
            physical_node_signature=_read_required_string(payload_dict, "physical_node_signature"),
        )

    def _read_route_path_id(
        self,
        payload: object,
    ) -> str:
        payload_dict = payload if isinstance(payload, dict) else {}
        return _read_required_string(payload_dict, "path_id")

    def _parse_route_create_validate_and_publish(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreateValidateAndPublish":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreateValidateAndPublish(
            path_id=_read_required_string(payload_dict, "path_id"),
            kem_ciphertext_hex=_read_required_string(payload_dict, "kem_ciphertext_hex"),
            encrypted_payload_hex=_read_required_string(payload_dict, "encrypted_payload_hex"),
        )

    def _parse_route_create_ok(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreateOk":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreateOk(
            path_id=_read_required_string(payload_dict, "path_id"),
            encrypted_payload_hex=_read_required_string(payload_dict, "encrypted_payload_hex"),
            public_route_acceptance_signature=_read_required_string(
                payload_dict,
                "public_route_acceptance_signature",
            ),
        )

    def _parse_route_create_fail(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreateFail":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreateFail(
            path_id=_read_required_string(payload_dict, "path_id"),
            reason=_read_required_string(payload_dict, "reason"),
        )

    def _parse_route_create_ping(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreatePing":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreatePing(
            path_id=_read_required_string(payload_dict, "path_id"),
            ping_id=_read_required_string(payload_dict, "ping_id"),
        )

    def _parse_route_create_pong(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreatePong":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreatePong(
            path_id=_read_required_string(payload_dict, "path_id"),
            ping_id=_read_required_string(payload_dict, "ping_id"),
        )

    def _resolve_forward_path(self, *, services, path_id: str) -> "BuildPathResolution | None":
        mapping = services.route_service.get_resolution_by_received_path_id(
            received_path_id=path_id,
        )
        if mapping is None:
            services.log_service.debug(
                "route_build",
                "route forward path not found; treating message as local endpoint",
                path_id=path_id,
            )
            return None

        services.log_service.debug(
            "route_build",
            "route forward path resolved",
            current_path_id=path_id,
            next_path_id=mapping.generated_path_id,
            target_remote_physical_node_id=mapping.to_physical_node_id,
            received_path_id=mapping.received_path_id,
            generated_path_id=mapping.generated_path_id,
            from_physical_node_id=mapping.from_physical_node_id,
            to_physical_node_id=mapping.to_physical_node_id,
            status=mapping.status,
            metadata=load_json_object(mapping.metadata_json),
        )
        return BuildPathResolution(
            action="forward_vn_to_pn",
            next_path_id=mapping.generated_path_id,
            target_remote_physical_node_id=mapping.to_physical_node_id,
            target_physical_session_id=None,
        )

    def _resolve_reverse_path(self, *, services, path_id: str) -> "BuildPathResolution | None":
        mapping = services.route_service.get_resolution_by_generated_path_id(
            generated_path_id=path_id,
        )
        if mapping is None:
            services.log_service.debug(
                "route_build",
                "route reverse path not found; treating message as local initiator/final endpoint",
                path_id=path_id,
            )
            return None

        from_physical_session_id = _read_optional_metadata_string(
            mapping.metadata_json,
            "from_physical_session_id",
        )
        services.log_service.debug(
            "route_build",
            "route reverse path resolved",
            current_path_id=path_id,
            next_path_id=mapping.received_path_id,
            target_remote_physical_node_id=mapping.from_physical_node_id,
            target_physical_session_id=from_physical_session_id,
            received_path_id=mapping.received_path_id,
            generated_path_id=mapping.generated_path_id,
            from_physical_node_id=mapping.from_physical_node_id,
            to_physical_node_id=mapping.to_physical_node_id,
            status=mapping.status,
            metadata=load_json_object(mapping.metadata_json),
        )
        return BuildPathResolution(
            action="forward_pn_to_vn",
            next_path_id=mapping.received_path_id,
            target_remote_physical_node_id=mapping.from_physical_node_id,
            target_physical_session_id=from_physical_session_id,
        )

    def _select_next_candidate(
        self,
        *,
        services,
        final_physical_node_id: str,
        previous_physical_node_id: str,
        remaining_ttl_ms: int,
    ) -> "RandomWalkTtlRouteCandidateSelection":
        route_candidates = services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
            limit=services.config.random_walk_candidate_limit,
        )
        services.log_service.debug(
            "route_build",
            "random walk candidate inventory",
            final_physical_node_id=final_physical_node_id,
            previous_physical_node_id=previous_physical_node_id,
            remaining_ttl_ms=remaining_ttl_ms,
            candidate_count=len(route_candidates),
            route_ready_candidates=[
                {
                    "node_id": candidate.node_id,
                    "average_rtt_ms": candidate.average_rtt_ms,
                    "one_way_rtt_ms": _build_one_way_rtt_ms(candidate.average_rtt_ms),
                    "session_count": getattr(candidate, "session_count", None),
                    "endpoint_count": getattr(candidate, "endpoint_count", None),
                }
                for candidate in route_candidates[:20]
            ],
        )

        eligible_intermediaries = [
            candidate
            for candidate in route_candidates
            if candidate.node_id != final_physical_node_id
            and candidate.node_id != previous_physical_node_id
            and _build_one_way_rtt_ms(candidate.average_rtt_ms) < remaining_ttl_ms
        ]
        if eligible_intermediaries:
            selected = random.choice(eligible_intermediaries)
            services.log_service.debug(
                "route_build",
                "random walk selected eligible intermediary",
                selected_physical_node_id=selected.node_id,
                final_physical_node_id=final_physical_node_id,
                previous_physical_node_id=previous_physical_node_id,
                remaining_ttl_ms=remaining_ttl_ms,
                eligible_count=len(eligible_intermediaries),
                selected_average_rtt_ms=selected.average_rtt_ms,
            )
            return _as_route_candidate_selection(
                selected,
                selection_reason="eligible_intermediary",
            )

        final_candidate = next(
            (
                candidate
                for candidate in route_candidates
                if candidate.node_id == final_physical_node_id
            ),
            None,
        )
        if final_candidate is not None:
            services.log_service.debug(
                "route_build",
                "random walk selected final candidate",
                final_physical_node_id=final_physical_node_id,
                previous_physical_node_id=previous_physical_node_id,
                remaining_ttl_ms=remaining_ttl_ms,
                final_average_rtt_ms=final_candidate.average_rtt_ms,
            )
            return _as_route_candidate_selection(
                final_candidate,
                selection_reason="final_candidate",
            )

        if previous_physical_node_id == final_physical_node_id:
            services.log_service.debug(
                "route_build",
                "random walk selected final fallback because previous is final",
                final_physical_node_id=final_physical_node_id,
                remaining_ttl_ms=remaining_ttl_ms,
            )
            return RandomWalkTtlRouteCandidateSelection(
                node_id=final_physical_node_id,
                average_rtt_ms=0.0,
                selection_reason="final_fallback",
            )

        previous_candidate = next(
            (
                candidate
                for candidate in route_candidates
                if candidate.node_id == previous_physical_node_id
                and candidate.node_id != final_physical_node_id
                and _build_one_way_rtt_ms(candidate.average_rtt_ms) < remaining_ttl_ms
            ),
            None,
        )
        if previous_candidate is not None:
            services.log_service.debug(
                "route_build",
                "random walk revisiting previous hop",
                previous_physical_node_id=previous_physical_node_id,
                final_physical_node_id=final_physical_node_id,
                remaining_ttl_ms=remaining_ttl_ms,
                previous_average_rtt_ms=previous_candidate.average_rtt_ms,
            )
            return _as_route_candidate_selection(
                previous_candidate,
                selection_reason="previous_hop_revisit",
            )

        fallback_previous_hop_rtt_ms = services.config.random_walk_previous_hop_fallback_rtt_ms
        if previous_physical_node_id != final_physical_node_id and fallback_previous_hop_rtt_ms < remaining_ttl_ms:
            services.log_service.debug(
                "route_build",
                "random walk revisiting previous hop using fallback rtt",
                previous_physical_node_id=previous_physical_node_id,
                final_physical_node_id=final_physical_node_id,
                remaining_ttl_ms=remaining_ttl_ms,
                fallback_previous_hop_rtt_ms=fallback_previous_hop_rtt_ms,
            )
            return RandomWalkTtlRouteCandidateSelection(
                node_id=previous_physical_node_id,
                average_rtt_ms=fallback_previous_hop_rtt_ms,
                selection_reason="previous_hop_revisit_without_rtt",
            )

        services.log_service.debug(
            "route_build",
            "random walk selected final fallback without candidate",
            final_physical_node_id=final_physical_node_id,
            previous_physical_node_id=previous_physical_node_id,
            remaining_ttl_ms=remaining_ttl_ms,
            candidate_count=len(route_candidates),
        )
        return RandomWalkTtlRouteCandidateSelection(
            node_id=final_physical_node_id,
            average_rtt_ms=0.0,
            selection_reason="final_fallback",
        )

    def _read_sender_physical_node_id(
        self,
        envelope,
        services,
    ) -> str | None:
        session_id = envelope.header.get("physical_session_id")
        if not isinstance(session_id, str) or not session_id:
            return None

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.remote_identity_type != "physical_node":
            return None

        return session.remote_identity_id

    @staticmethod
    def _read_physical_session_id(envelope) -> str | None:
        session_id = envelope.header.get("physical_session_id")
        if isinstance(session_id, str) and session_id:
            return session_id
        return None

    @staticmethod
    def _build_forward_result(
        *,
        envelope,
        target_remote_physical_node_id: str,
        target_physical_session_id: str | None = None,
        forward_message_type: str,
        forward_payload: dict[str, object],
        route_build_action: str,
        extra_metadata: dict[str, object] | None = None,
    ) -> PacketProcessingResult:
        metadata = {
            "action": "forward_message",
            "protocol_family": "route_build",
            "route_build_action": route_build_action,
            "target_remote_physical_node_id": target_remote_physical_node_id,
            "target_physical_session_id": target_physical_session_id,
            "forward_message_type": forward_message_type,
            "forward_payload": forward_payload,
        }
        if extra_metadata:
            metadata.update(extra_metadata)

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata=metadata,
        )

    @staticmethod
    def _build_invalid_result(envelope, *, reason: str) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "route_build",
                "reason": reason,
            },
        )


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreate:
    pk_final_physical_node: str
    remaining_ttl_ms: int
    path_id: str
    nonce: int

    def to_payload(self, *, strategy_name: str) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "pk_final_physical_node": self.pk_final_physical_node,
            "remaining_ttl_ms": self.remaining_ttl_ms,
            "path_id": self.path_id,
            "nonce": self.nonce,
        }


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreateKemInfo:
    kyber_public_key_pem: str
    physical_node_signature: str

    def to_payload(self, *, strategy_name: str, path_id: str) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "path_id": path_id,
            "kyber_public_key_pem": self.kyber_public_key_pem,
            "physical_node_signature": self.physical_node_signature,
        }


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreateValidateAndPublish:
    path_id: str
    kem_ciphertext_hex: str
    encrypted_payload_hex: str

    def to_payload(self, *, strategy_name: str, path_id: str) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "path_id": path_id,
            "kem_ciphertext_hex": self.kem_ciphertext_hex,
            "encrypted_payload_hex": self.encrypted_payload_hex,
        }


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreateOk:
    path_id: str
    encrypted_payload_hex: str
    public_route_acceptance_signature: str

    def to_payload(self, *, strategy_name: str, path_id: str) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "path_id": path_id,
            "encrypted_payload_hex": self.encrypted_payload_hex,
            "public_route_acceptance_signature": self.public_route_acceptance_signature,
        }


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreateFail:
    path_id: str
    reason: str

    def to_payload(self, *, strategy_name: str, path_id: str) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "path_id": path_id,
            "reason": self.reason,
        }


@dataclass(slots=True, frozen=True)
class _RandomWalkTtlPingPongPayloadMixin:
    path_id: str
    ping_id: str

    def to_payload(self, *, strategy_name: str, path_id: str) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "path_id": path_id,
            "ping_id": self.ping_id,
        }


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreatePing(_RandomWalkTtlPingPongPayloadMixin):
    pass


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreatePong(_RandomWalkTtlPingPongPayloadMixin):
    pass


@dataclass(slots=True, frozen=True)
class BuildPathResolution:
    action: str
    next_path_id: str
    target_remote_physical_node_id: str
    target_physical_session_id: str | None


@dataclass(slots=True, frozen=True)
class ForwardRouteCreateResult:
    next_remote_physical_node_id: str
    previous_path_id: str
    selected_average_rtt_ms: float
    selected_one_way_rtt_ms: float
    payload: dict[str, object]


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCandidateSelection:
    node_id: str
    average_rtt_ms: float
    selection_reason: str


def _as_route_candidate_selection(
    candidate,
    *,
    selection_reason: str,
) -> RandomWalkTtlRouteCandidateSelection:
    return RandomWalkTtlRouteCandidateSelection(
        node_id=candidate.node_id,
        average_rtt_ms=candidate.average_rtt_ms,
        selection_reason=selection_reason,
    )


def _build_physical_node_id(public_key: str) -> str:
    return sha512_hex(public_key.encode("utf-8"))


def _build_virtual_node_id(public_key: str) -> str:
    return sha512_hex(public_key.encode("utf-8"))


def _build_one_way_rtt_ms(observed_rtt_ms: float) -> float:
    return max(1.0, observed_rtt_ms / 2.0)


def _read_required_string(payload: dict[str, object], field_name: str) -> str:
    return _require_non_empty_string(payload.get(field_name), field_name=field_name)


def _read_required_positive_int(payload: dict[str, object], field_name: str) -> int:
    return _require_positive_int(payload.get(field_name), field_name=field_name)


def _read_required_nonce(payload: dict[str, object], field_name: str) -> int:
    return _require_non_negative_int(payload.get(field_name), field_name=field_name)


def _require_non_empty_string(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
        raise ValueError(f"The '{field_name}' field is required and must be a non-empty string.")


def _require_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, int) and value > 0:
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser um inteiro positivo.")


def _require_non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, int) and value >= 0:
        return value
        raise ValueError(f"The '{field_name}' field is required and must be a non-negative integer.")


def _is_valid_route_pow(
    *,
    route_create: RandomWalkTtlRouteCreate,
    difficulty_bits: int,
) -> bool:
    return _build_route_pow_details(
        route_create=route_create,
        difficulty_bits=difficulty_bits,
    )["is_valid"]


def _build_route_pow_details(
    *,
    route_create: RandomWalkTtlRouteCreate | None,
    difficulty_bits: int,
) -> dict[str, object]:
    if route_create is None:
        return {
            "canonical_hash": None,
            "proof_hash": None,
            "proof_hash_prefix": None,
            "difficulty_bits": difficulty_bits,
            "nonce": None,
            "is_valid": False,
        }

    canonical_payload_bytes = _build_route_pow_canonical_payload(route_create)
    canonical_hash = sha512_hex(canonical_payload_bytes)
    pow_material = canonical_payload_bytes + b"|" + str(route_create.nonce).encode("utf-8")
    proof_hash = sha512_hex(pow_material)
    proof_bits = bin(int(proof_hash, 16))[2:].zfill(512)
    return {
        "canonical_hash": canonical_hash,
        "proof_hash": proof_hash,
        "proof_hash_prefix": proof_hash[:16],
        "difficulty_bits": difficulty_bits,
        "nonce": route_create.nonce,
        "is_valid": difficulty_bits <= 0 or proof_bits.startswith("0" * difficulty_bits),
    }


def _build_route_pow_hash_bits(route_create: RandomWalkTtlRouteCreate) -> str:
    proof_hash = _build_route_pow_details(
        route_create=route_create,
        difficulty_bits=0,
    )["proof_hash"]
    return bin(int(str(proof_hash), 16))[2:].zfill(512)


def _build_route_pow_canonical_payload(route_create: RandomWalkTtlRouteCreate) -> bytes:
    canonical_payload = {
        "route_strategy": "random_walk_ttl_based",
        "pk_final_physical_node": route_create.pk_final_physical_node,
    }
    return json.dumps(
        canonical_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sign_route_kem_public_key_offer(
    *,
    kyber_public_key_pem: str,
    signing_private_key_pem: str,
) -> str:
    signed_payload = {
        "kyber_public_key_pem": kyber_public_key_pem,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_sign_hex(canonical_bytes.hex(), signing_private_key_pem)


def _is_valid_route_kem_public_key_offer_signature(
    *,
    kyber_public_key_pem: str,
    signature_hex: str,
    physical_node_public_key_pem: str,
) -> bool:
    signed_payload = {
        "kyber_public_key_pem": kyber_public_key_pem,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_verify_hex(
        canonical_bytes.hex(),
        signature_hex,
        physical_node_public_key_pem,
    )


def _sign_final_path_id(
    *,
    final_path_id: str,
    final_physical_node_id: str,
    local_virtual_node_private_key_pem: str,
) -> str:
    signed_payload = {
        "final_path_id": final_path_id,
        "final_physical_node_id": final_physical_node_id,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_sign_hex(canonical_bytes.hex(), local_virtual_node_private_key_pem)


def _is_valid_virtual_node_route_signature(
    *,
    final_path_id: str,
    final_physical_node_id: str,
    signature_hex: str,
    virtual_node_public_key_pem: str,
) -> bool:
    signed_payload = {
        "final_path_id": final_path_id,
        "final_physical_node_id": final_physical_node_id,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_verify_hex(
        canonical_bytes.hex(),
        signature_hex,
        virtual_node_public_key_pem,
    )


def _sign_route_entry_point_acceptance(
    *,
    virtual_node_id: str,
    final_path_id: str,
    virtual_node_signature: str,
    local_physical_node_private_key_pem: str,
) -> str:
    signed_payload = {
        "virtual_node_id": virtual_node_id,
        "final_path_id": final_path_id,
        "virtual_node_signature": virtual_node_signature,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_sign_hex(canonical_bytes.hex(), local_physical_node_private_key_pem)


def _is_valid_route_entry_point_acceptance_signature(
    *,
    virtual_node_id: str,
    final_path_id: str,
    virtual_node_signature: str,
    signature_hex: str,
    physical_node_public_key_pem: str,
) -> bool:
    signed_payload = {
        "virtual_node_id": virtual_node_id,
        "final_path_id": final_path_id,
        "virtual_node_signature": virtual_node_signature,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_verify_hex(
        canonical_bytes.hex(),
        signature_hex,
        physical_node_public_key_pem,
    )


def _sign_public_route_acceptance(
    *,
    route_strategy: str,
    final_physical_node_public_key: str,
    route_nonce: int,
    local_physical_node_private_key_pem: str,
) -> str:
    signed_payload = {
        "route_strategy": route_strategy,
        "pk_final_physical_node": final_physical_node_public_key,
        "nonce": route_nonce,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_sign_hex(canonical_bytes.hex(), local_physical_node_private_key_pem)


def _is_valid_public_route_acceptance_signature(
    *,
    route_strategy: str,
    final_physical_node_public_key: str,
    route_nonce: int,
    signature_hex: str,
    physical_node_public_key_pem: str,
) -> bool:
    signed_payload = {
        "route_strategy": route_strategy,
        "pk_final_physical_node": final_physical_node_public_key,
        "nonce": route_nonce,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_verify_hex(
        canonical_bytes.hex(),
        signature_hex,
        physical_node_public_key_pem,
    )


def _sign_drt_route_rtt(
    *,
    pk_physical_node: str,
    expires_at: str,
    rtt: int,
    local_physical_node_private_key_pem: str,
) -> str:
    signed_payload = {
        "pk_physical_node": pk_physical_node,
        "expires_at": expires_at,
        "rtt": rtt,
    }
    canonical_bytes = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return dilithium_sign_hex(canonical_bytes.hex(), local_physical_node_private_key_pem)


def _build_drt_entry_expires_at() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()


def _select_default_local_virtual_node(services):
    local_virtual_nodes = services.identity_service.list_local_virtual_nodes(only_active=True)
    return local_virtual_nodes[0] if local_virtual_nodes else None


def _select_route_local_virtual_node(services, local_virtual_node_id: str | None):
    if local_virtual_node_id:
        return services.identity_service.get_local_virtual_node_by_id(local_virtual_node_id)

    return _select_default_local_virtual_node(services)


def _read_optional_metadata_string(metadata_json: str | None, key: str) -> str | None:
    value = load_json_object(metadata_json).get(key)
    if isinstance(value, str) and value:
        return value
    return None


def _read_header_value(envelope, key: str) -> object:
    header = getattr(envelope, "header", None)
    if not isinstance(header, dict):
        return None
    return header.get(key)


def _build_route_encrypted_payload_aad(*, message_type: str) -> bytes:
    return json.dumps(
        {
            "scope": "route_build_encrypted_payload",
            "message_type": message_type,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
