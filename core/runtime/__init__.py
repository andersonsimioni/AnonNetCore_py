from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .dht_maintenance import DhtMaintenanceRuntime
from .physical_node_info_exchange import PhysicalNodeInfoExchangeRuntime
from .physical_ping import PhysicalPingRuntime
from .physical_node_validation import PhysicalNodeValidationRuntime
from .physical_session_runtime import PhysicalSessionRuntime

if TYPE_CHECKING:
    from ..engine import CoreEngine


@dataclass(slots=True)
class RuntimeServices:
    dht_maintenance: DhtMaintenanceRuntime
    physical_node_info_exchange: PhysicalNodeInfoExchangeRuntime
    physical_ping: PhysicalPingRuntime
    physical_node_validation: PhysicalNodeValidationRuntime
    physical_session: PhysicalSessionRuntime

    def __init__(self, engine: CoreEngine) -> None:
        self.dht_maintenance = DhtMaintenanceRuntime(engine)
        self.physical_node_info_exchange = PhysicalNodeInfoExchangeRuntime(engine)
        self.physical_ping = PhysicalPingRuntime(engine)
        self.physical_node_validation = PhysicalNodeValidationRuntime(engine)
        self.physical_session = PhysicalSessionRuntime(engine)

    async def start(self) -> None:
        await self.dht_maintenance.start()
        await self.physical_node_info_exchange.start()
        await self.physical_ping.start()
        await self.physical_node_validation.start()
        await self.physical_session.start()

    async def stop(self) -> None:
        await self.dht_maintenance.stop()
        await self.physical_node_info_exchange.stop()
        await self.physical_ping.stop()
        await self.physical_node_validation.stop()
        await self.physical_session.stop()


__all__ = [
    "DhtMaintenanceRuntime",
    "PhysicalNodeInfoExchangeRuntime",
    "PhysicalPingRuntime",
    "PhysicalNodeValidationRuntime",
    "PhysicalSessionRuntime",
    "RuntimeServices",
]
