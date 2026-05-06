from __future__ import annotations

from ..models import PacketProcessingResult
from .base import RouteStrategy


class RandomWalkMaxHopRouteStrategy(RouteStrategy):
    """Estrategia de rota por random walk com limite maximo de hops."""

    strategy_name = "random_walk_max_hop_based"

    def build_initial_route_create(
        self,
        **route_fields: object,
    ) -> dict[str, object]:
        return {
            "route_strategy": self.strategy_name,
            **route_fields,
        }

    async def handle_route_create(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create")

    async def handle_route_create_kem_info(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_kem_info")

    async def handle_route_create_validate_and_publish(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_validate_and_publish")

    async def handle_route_create_ok(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_ok")

    async def handle_route_create_ping(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_ping")

    async def handle_route_create_pong(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_pong")

    def _not_implemented(self, envelope, next_step: str) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "route_build",
                "route_strategy": self.strategy_name,
                "next_step": next_step,
            },
        )
