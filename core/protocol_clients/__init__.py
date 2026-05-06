from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .physical import (
    PhysicalDhtClient,
    PhysicalPingClient,
    PhysicalNodeInfoClient,
    PhysicalNodeInfoExchangeClient,
    PhysicalSessionClient,
)
from .route_build import RouteBuildClient
from .route_execute import RouteExecuteClient

if TYPE_CHECKING:
    from ..engine import CoreEngine


@dataclass(slots=True)
class PhysicalProtocolClients:
    dht: PhysicalDhtClient
    ping: PhysicalPingClient
    node_info: PhysicalNodeInfoClient
    node_info_exchange: PhysicalNodeInfoExchangeClient
    session: PhysicalSessionClient

    def __init__(self, engine: CoreEngine) -> None:
        self.dht = PhysicalDhtClient(engine)
        self.ping = PhysicalPingClient(engine)
        self.node_info = PhysicalNodeInfoClient(engine)
        self.node_info_exchange = PhysicalNodeInfoExchangeClient(engine)
        self.session = PhysicalSessionClient(engine)


@dataclass(slots=True)
class OverlayProtocolClients:
    def __init__(self, engine: CoreEngine) -> None:
        del engine


@dataclass(slots=True)
class ProtocolClients:
    physical: PhysicalProtocolClients
    overlay: OverlayProtocolClients
    route_build: RouteBuildClient
    route_execute: RouteExecuteClient

    def __init__(self, engine: CoreEngine) -> None:
        self.physical = PhysicalProtocolClients(engine)
        self.overlay = OverlayProtocolClients(engine)
        self.route_build = RouteBuildClient(engine)
        self.route_execute = RouteExecuteClient(engine)


__all__ = [
    "OverlayProtocolClients",
    "PhysicalDhtClient",
    "PhysicalPingClient",
    "PhysicalNodeInfoClient",
    "PhysicalNodeInfoExchangeClient",
    "PhysicalProtocolClients",
    "PhysicalSessionClient",
    "ProtocolClients",
    "RouteBuildClient",
    "RouteExecuteClient",
]
