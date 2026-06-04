from __future__ import annotations

from .base import RouteStrategy


class RandomWalkMaxHopRouteStrategy(RouteStrategy):
    """Random-walk route strategy with a maximum hop limit."""

    strategy_name = "random_walk_max_hop_based"

    async def handle_route_create(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create")

    async def handle_route_create_kem_info(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_kem_info")

    async def handle_route_create_validate_and_publish(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_validate_and_publish")

    async def handle_route_create_ok(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_ok")

    async def handle_route_create_ping(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_ping")

    async def handle_route_create_pong(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_random_walk_max_hop_route_create_pong")
