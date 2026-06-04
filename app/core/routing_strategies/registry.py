from __future__ import annotations

from .base import RouteStrategy
from .onion_like import OnionLikeRouteStrategy
from .random_walk_max_hop import RandomWalkMaxHopRouteStrategy
from .random_walk_ttl import RandomWalkTtlRouteStrategy


class RouteStrategyRegistry:
    """Central registry for route composition strategies."""

    def __init__(self) -> None:
        strategies = [
            RandomWalkMaxHopRouteStrategy(),
            RandomWalkTtlRouteStrategy(),
            OnionLikeRouteStrategy(),
        ]
        self._strategies: dict[str, RouteStrategy] = {
            strategy.strategy_name: strategy
            for strategy in strategies
        }

    def get(self, strategy_name: str | None) -> RouteStrategy | None:
        if strategy_name is None:
            return None
        return self._strategies.get(strategy_name)

    def require(self, strategy_name: str) -> RouteStrategy:
        strategy = self.get(strategy_name)
        if strategy is not None:
            return strategy

        raise ValueError(f"Unsupported route strategy: {strategy_name}")

    def list_names(self) -> list[str]:
        return sorted(self._strategies.keys())
