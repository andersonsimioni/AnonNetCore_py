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

    async def handle_route_create_return(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_return")

    async def handle_route_create_ok(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_ok")

    async def handle_route_create_fail(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_fail")

    async def handle_route_data(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_data")

    async def handle_route_data_ack(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_data_ack")

    async def handle_route_keepalive(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_keepalive")

    async def handle_route_keepalive_ack(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_keepalive_ack")

    async def handle_route_close(self, *, envelope, context, services) -> PacketProcessingResult:
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_close")

    def _not_implemented(self, envelope, next_step: str) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "routing",
                "route_strategy": self.strategy_name,
                "next_step": next_step,
            },
        )
