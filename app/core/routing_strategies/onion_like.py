from __future__ import annotations

from .base import RouteStrategy


class OnionLikeRouteStrategy(RouteStrategy):
    """Route strategy with composition inspired by onion layers."""

    strategy_name = "onion_like_based"

    async def handle_route_create(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_onion_like_route_create")

    async def handle_route_create_kem_info(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_onion_like_route_create_kem_info")

    async def handle_route_create_validate_and_publish(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_onion_like_route_create_validate_and_publish")

    async def handle_route_create_ok(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_onion_like_route_create_ok")

    async def handle_route_create_ping(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_onion_like_route_create_ping")

    async def handle_route_create_pong(self, *, envelope, context, services):
        del context, services
        return self._not_implemented(envelope, "implement_onion_like_route_create_pong")
