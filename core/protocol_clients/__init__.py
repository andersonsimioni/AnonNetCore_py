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

    def __init__(self, engine: CoreEngine) -> None:
        self.physical = PhysicalProtocolClients(engine)
        self.overlay = OverlayProtocolClients(engine)


__all__ = [
    "OverlayProtocolClients",
    "PhysicalDhtClient",
    "PhysicalPingClient",
    "PhysicalNodeInfoClient",
    "PhysicalNodeInfoExchangeClient",
    "PhysicalProtocolClients",
    "PhysicalSessionClient",
    "ProtocolClients",
]
