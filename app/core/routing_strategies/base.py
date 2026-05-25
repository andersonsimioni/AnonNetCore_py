from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ..models import PacketProcessingResult

if TYPE_CHECKING:
    from ..models import PacketContext, ProtocolEnvelope
    from ..services import EngineServices


class RouteStrategy(ABC):
    """Contrato comum das estrategias de construcao de rota."""

    strategy_name: str

    def build_initial_route_create(
        self,
        **route_fields: object,
    ) -> dict[str, object]:
        """Monta o payload inicial de ROUTE_CREATE."""
        return {
            "route_strategy": self.strategy_name,
            **route_fields,
        }

    def _not_implemented(self, envelope: "ProtocolEnvelope", next_step: str) -> PacketProcessingResult:
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

    @abstractmethod
    async def handle_route_create(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE para esta estrategia."""

    @abstractmethod
    async def handle_route_create_kem_info(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_KEM_INFO para esta estrategia."""

    @abstractmethod
    async def handle_route_create_validate_and_publish(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_VALIDATE_AND_PUBLISH para esta estrategia."""

    @abstractmethod
    async def handle_route_create_ok(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_OK para esta estrategia."""

    @abstractmethod
    async def handle_route_create_ping(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_PING para esta estrategia."""

    @abstractmethod
    async def handle_route_create_pong(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_PONG para esta estrategia."""
