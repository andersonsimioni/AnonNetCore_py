from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .physical import (
    PhysicalDhtClient,
    PhysicalPingClient,
    PhysicalNodeInfoClient,
    PhysicalNodeInfoExchangeClient,
    PhysicalRelayClient,
    PhysicalSessionClient,
    RouteBuildClient,
    RouteExecuteClient,
)
from .virtual import VirtualSessionClient

if TYPE_CHECKING:
    from ..engine import CoreEngine


@dataclass(slots=True)
class PhysicalProtocolClients:
    dht: PhysicalDhtClient
    ping: PhysicalPingClient
    node_info: PhysicalNodeInfoClient
    node_info_exchange: PhysicalNodeInfoExchangeClient
    relay: PhysicalRelayClient
    session: PhysicalSessionClient
    route_build: RouteBuildClient
    route_execute: RouteExecuteClient

    def __init__(self, engine: CoreEngine) -> None:
        self.dht = PhysicalDhtClient(engine)
        self.ping = PhysicalPingClient(engine)
        self.node_info = PhysicalNodeInfoClient(engine)
        self.node_info_exchange = PhysicalNodeInfoExchangeClient(engine)
        self.relay = PhysicalRelayClient(engine)
        self.session = PhysicalSessionClient(engine)
        self.route_build = RouteBuildClient(engine)
        self.route_execute = RouteExecuteClient(engine)


@dataclass(slots=True)
class VirtualProtocolClients:
    session: VirtualSessionClient

    def __init__(self, engine: CoreEngine) -> None:
        self.session = VirtualSessionClient(engine)


@dataclass(slots=True)
class ProtocolClients:
    physical: PhysicalProtocolClients
    virtual: VirtualProtocolClients

    def __init__(self, engine: CoreEngine) -> None:
        self.physical = PhysicalProtocolClients(engine)
        self.virtual = VirtualProtocolClients(engine)


__all__ = [
    "VirtualProtocolClients",
    "PhysicalDhtClient",
    "PhysicalPingClient",
    "PhysicalNodeInfoClient",
    "PhysicalNodeInfoExchangeClient",
    "PhysicalProtocolClients",
    "PhysicalRelayClient",
    "PhysicalSessionClient",
    "ProtocolClients",
    "RouteBuildClient",
    "RouteExecuteClient",
    "VirtualSessionClient",
]
