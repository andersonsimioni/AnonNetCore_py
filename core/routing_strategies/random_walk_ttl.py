from __future__ import annotations

import json
import random
from dataclasses import dataclass
from uuid import uuid4

from crypto import sha512_hex

from ..models import PacketProcessingResult
from .base import RouteStrategy


class RandomWalkTtlRouteStrategy(RouteStrategy):
    """Estrategia de rota por random walk orientado a budget temporal."""

    strategy_name = "random_walk_ttl_based"

    def build_initial_route_create(
        self,
        *,
        pk_final_physical_node: str,
        remaining_ttl_ms: int,
        kem_ciphertext_for_final_physical_node: str,
        encrypted_payload_for_final_physical_node: str,
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
            kem_ciphertext_for_final_physical_node=_require_non_empty_string(
                kem_ciphertext_for_final_physical_node,
                field_name="kem_ciphertext_for_final_physical_node",
            ),
            encrypted_payload_for_final_physical_node=_require_non_empty_string(
                encrypted_payload_for_final_physical_node,
                field_name="encrypted_payload_for_final_physical_node",
            ),
            path_id=_require_non_empty_string(path_id, field_name="path_id"),
            nonce=_require_non_negative_int(nonce, field_name="nonce"),
        )
        return route_create.to_payload(strategy_name=self.strategy_name)

    async def handle_route_create(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context

        route_create = self._parse_route_create(envelope.payload)
        if not _is_valid_route_pow(
            route_create=route_create,
            difficulty_bits=services.config.route_pow_difficulty_bits,
        ):
            return self._build_invalid_result(envelope, reason="invalid_route_create_pow")

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return self._build_invalid_result(envelope, reason="local_physical_node_not_initialized")

        if local_node.public_key == route_create.pk_final_physical_node:
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

    async def handle_route_create_return(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_create_return")

    async def handle_route_create_ok(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_create_ok")

    async def handle_route_create_fail(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_create_fail")

    async def handle_route_data(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_data")

    async def handle_route_data_ack(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_data_ack")

    async def handle_route_keepalive(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_keepalive")

    async def handle_route_keepalive_ack(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_keepalive_ack")

    async def handle_route_close(
        self,
        *,
        envelope,
        context,
        services,
    ) -> PacketProcessingResult:
        del context, services
        return self._build_not_implemented_result(envelope, next_step="implement_route_close")

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
            services=services,
            route_create=route_create,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "action": "forward_message",
                "protocol_family": "routing",
                "target_remote_physical_node_id": forward_result["next_remote_physical_node_id"],
                "forward_message_type": "ROUTE_CREATE",
                "forward_payload": forward_result["payload"],
                "forward_result": forward_result,
            },
        )

    async def _handle_route_create_as_final_node(
        self,
        *,
        envelope,
        services,
        route_create: "RandomWalkTtlRouteCreate",
    ) -> PacketProcessingResult:
        del services
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "routing",
                "route_strategy": self.strategy_name,
                "path_id": route_create.path_id,
                "remaining_ttl_ms": route_create.remaining_ttl_ms,
                "next_step": "implement_route_create_return",
            },
        )

    def _forward_route_create(
        self,
        *,
        from_physical_node_id: str,
        services,
        route_create: "RandomWalkTtlRouteCreate",
    ) -> dict[str, object]:
        final_physical_node_id = _build_physical_node_id(route_create.pk_final_physical_node)
        selected_candidate = self._select_next_candidate(
            services=services,
            final_physical_node_id=final_physical_node_id,
            remaining_ttl_ms=route_create.remaining_ttl_ms,
        )

        one_way_rtt_ms = _build_one_way_rtt_ms(selected_candidate.average_rtt_ms)
        next_remaining_ttl_ms = max(1, int(route_create.remaining_ttl_ms - one_way_rtt_ms))
        next_path_id = str(uuid4())

        services.route_state_service.create_path_id_mapping(
            from_physical_node_id=from_physical_node_id,
            to_physical_node_id=selected_candidate.node_id,
            received_path_id=route_create.path_id,
            generated_path_id=next_path_id,
        )

        next_payload = self.build_initial_route_create(
            pk_final_physical_node=route_create.pk_final_physical_node,
            remaining_ttl_ms=next_remaining_ttl_ms,
            kem_ciphertext_for_final_physical_node=route_create.kem_ciphertext_for_final_physical_node,
            encrypted_payload_for_final_physical_node=route_create.encrypted_payload_for_final_physical_node,
            path_id=next_path_id,
            nonce=route_create.nonce,
        )

        return {
            "route_strategy": self.strategy_name,
            "next_remote_physical_node_id": selected_candidate.node_id,
            "previous_path_id": route_create.path_id,
            "selected_average_rtt_ms": selected_candidate.average_rtt_ms,
            "selected_one_way_rtt_ms": one_way_rtt_ms,
            "payload": next_payload,
        }

    def _parse_route_create(
        self,
        payload: object,
    ) -> "RandomWalkTtlRouteCreate":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RandomWalkTtlRouteCreate(
            pk_final_physical_node=_read_required_string(payload_dict, "pk_final_physical_node"),
            remaining_ttl_ms=_read_required_positive_int(payload_dict, "remaining_ttl_ms"),
            kem_ciphertext_for_final_physical_node=_read_required_string(
                payload_dict,
                "kem_ciphertext_for_final_physical_node",
            ),
            encrypted_payload_for_final_physical_node=_read_required_string(
                payload_dict,
                "encrypted_payload_for_final_physical_node",
            ),
            path_id=_read_required_string(payload_dict, "path_id"),
            nonce=_read_required_nonce(payload_dict, "nonce"),
        )

    def _select_next_candidate(
        self,
        *,
        services,
        final_physical_node_id: str,
        remaining_ttl_ms: int,
    ):
        route_candidates = services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
            limit=services.config.random_walk_ttl_route_candidate_limit,
        )

        eligible_intermediaries = [
            candidate
            for candidate in route_candidates
            if candidate.node_id != final_physical_node_id
            and _build_one_way_rtt_ms(candidate.average_rtt_ms) < remaining_ttl_ms
        ]
        if eligible_intermediaries:
            return random.choice(eligible_intermediaries)

        final_candidate = next(
            (
                candidate
                for candidate in route_candidates
                if candidate.node_id == final_physical_node_id
            ),
            None,
        )
        if final_candidate is not None:
            return final_candidate

        return _FallbackRouteCandidate(node_id=final_physical_node_id, average_rtt_ms=0.0)

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
    def _build_invalid_result(envelope, *, reason: str) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "routing",
                "reason": reason,
            },
        )

    @staticmethod
    def _build_not_implemented_result(envelope, *, next_step: str) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "routing",
                "route_strategy": "random_walk_ttl_based",
                "next_step": next_step,
            },
        )


@dataclass(slots=True, frozen=True)
class RandomWalkTtlRouteCreate:
    pk_final_physical_node: str
    remaining_ttl_ms: int
    kem_ciphertext_for_final_physical_node: str
    encrypted_payload_for_final_physical_node: str
    path_id: str
    nonce: int

    def to_payload(
        self,
        *,
        strategy_name: str,
    ) -> dict[str, object]:
        return {
            "route_strategy": strategy_name,
            "pk_final_physical_node": self.pk_final_physical_node,
            "remaining_ttl_ms": self.remaining_ttl_ms,
            "kem_ciphertext_for_final_physical_node": self.kem_ciphertext_for_final_physical_node,
            "encrypted_payload_for_final_physical_node": self.encrypted_payload_for_final_physical_node,
            "path_id": self.path_id,
            "nonce": self.nonce,
        }


@dataclass(slots=True, frozen=True)
class _FallbackRouteCandidate:
    node_id: str
    average_rtt_ms: float


def _build_physical_node_id(public_key: str) -> str:
    return sha512_hex(public_key.encode("utf-8"))


def _build_one_way_rtt_ms(observed_rtt_ms: float) -> float:
    return max(0.0, observed_rtt_ms / 2.0)


def _read_required_string(payload: dict[str, object], field_name: str) -> str:
    return _require_non_empty_string(payload.get(field_name), field_name=field_name)


def _read_required_positive_int(payload: dict[str, object], field_name: str) -> int:
    return _require_positive_int(payload.get(field_name), field_name=field_name)


def _read_required_nonce(payload: dict[str, object], field_name: str) -> str:
    return _require_non_negative_int(payload.get(field_name), field_name=field_name)


def _require_non_empty_string(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser uma string nao vazia.")


def _require_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, int) and value > 0:
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser um inteiro positivo.")


def _require_non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, int) and value >= 0:
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser um inteiro nao negativo.")


def _is_valid_route_pow(
    *,
    route_create: RandomWalkTtlRouteCreate,
    difficulty_bits: int,
) -> bool:
    if difficulty_bits <= 0:
        return True

    hash_bits = _build_route_pow_hash_bits(route_create)
    return hash_bits.startswith("0" * difficulty_bits)


def _build_route_pow_hash_bits(route_create: RandomWalkTtlRouteCreate) -> str:
    canonical_payload_bytes = _build_route_pow_canonical_payload(route_create)
    pow_material = canonical_payload_bytes + b"|" + str(route_create.nonce).encode("utf-8")
    return bin(int(sha512_hex(pow_material), 16))[2:].zfill(512)


def _build_route_pow_canonical_payload(route_create: RandomWalkTtlRouteCreate) -> bytes:
    canonical_payload = {
        "route_strategy": "random_walk_ttl_based",
        "pk_final_physical_node": route_create.pk_final_physical_node,
        "kem_ciphertext_for_final_physical_node": route_create.kem_ciphertext_for_final_physical_node,
        "encrypted_payload_for_final_physical_node": route_create.encrypted_payload_for_final_physical_node,
    }
    return json.dumps(
        canonical_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
