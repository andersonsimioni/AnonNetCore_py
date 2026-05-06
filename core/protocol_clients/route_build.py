from __future__ import annotations

from time import monotonic
from uuid import uuid4


class RouteBuildClient:
    """Orquestra a construcao e a validacao inicial de rotas."""

    def __init__(self, engine) -> None:
        self.engine = engine

    async def start_random_walk_ttl_route(
        self,
        *,
        first_hop_physical_node_id: str,
        final_physical_node_public_key: str,
        remaining_ttl_ms: int,
        nonce: int,
        expected_round_trip_ttl_ms: int | None = None,
    ) -> dict[str, object]:
        initial_path_id = str(uuid4())
        self.engine.services.route_service.create_initiator_resolution(
            first_hop_physical_node_id=first_hop_physical_node_id,
            initial_path_id=initial_path_id,
            final_physical_node_public_key=final_physical_node_public_key,
            expected_round_trip_ttl_ms=expected_round_trip_ttl_ms,
            route_strategy="random_walk_ttl_based",
            route_nonce=nonce,
        )

        payload = self.engine.services.route_strategies.require(
            "random_walk_ttl_based"
        ).build_initial_route_create(
            pk_final_physical_node=final_physical_node_public_key,
            remaining_ttl_ms=remaining_ttl_ms,
            path_id=initial_path_id,
            nonce=nonce,
        )
        await self.engine.forward_message_to_remote_physical_node(
            remote_physical_node_id=first_hop_physical_node_id,
            message_type="ROUTE_CREATE",
            payload=payload,
        )
        return {
            "route_strategy": "random_walk_ttl_based",
            "initial_path_id": initial_path_id,
            "first_hop_physical_node_id": first_hop_physical_node_id,
            "final_physical_node_public_key": final_physical_node_public_key,
            "remaining_ttl_ms": remaining_ttl_ms,
            "nonce": nonce,
            "expected_round_trip_ttl_ms": expected_round_trip_ttl_ms,
        }

    async def send_route_create_ping(
        self,
        *,
        initial_path_id: str,
    ) -> dict[str, object]:
        initiator_resolution = self.engine.services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=initial_path_id,
        )
        if initiator_resolution is None:
            raise ValueError("A rota informada nao existe no estado local do initiator.")
        if not initiator_resolution.first_hop_physical_node_id:
            raise ValueError("A rota informada nao possui first hop associado.")
        if not initiator_resolution.route_strategy:
            raise ValueError("A rota informada nao possui strategy registrada.")

        ping_id = str(uuid4())
        self.engine.services.route_service.mark_initiator_resolution_ping_started(
            initial_path_id=initial_path_id,
            ping_id=ping_id,
            started_at_monotonic_ms=monotonic() * 1000.0,
        )
        payload = {
            "route_strategy": initiator_resolution.route_strategy,
            "path_id": initial_path_id,
            "ping_id": ping_id,
        }
        await self.engine.forward_message_to_remote_physical_node(
            remote_physical_node_id=initiator_resolution.first_hop_physical_node_id,
            message_type="ROUTE_CREATE_PING",
            payload=payload,
        )
        return {
            "initial_path_id": initial_path_id,
            "ping_id": ping_id,
            "route_strategy": initiator_resolution.route_strategy,
        }
