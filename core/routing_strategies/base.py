from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import PacketContext, PacketProcessingResult, ProtocolEnvelope
    from ..services import EngineServices


class RouteStrategy(ABC):
    """Contrato comum das estrategias de composicao e execucao de rota."""

    strategy_name: str

    @abstractmethod
    def build_initial_route_create(
        self,
        **route_fields: object,
    ) -> dict[str, object]:
        """Monta o payload inicial de ROUTE_CREATE."""

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
    async def handle_route_create_return(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_RETURN para esta estrategia."""

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
    async def handle_route_create_fail(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CREATE_FAIL para esta estrategia."""

    @abstractmethod
    async def handle_route_data(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_DATA para esta estrategia."""

    @abstractmethod
    async def handle_route_data_ack(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_DATA_ACK para esta estrategia."""

    @abstractmethod
    async def handle_route_keepalive(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_KEEPALIVE para esta estrategia."""

    @abstractmethod
    async def handle_route_keepalive_ack(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_KEEPALIVE_ACK para esta estrategia."""

    @abstractmethod
    async def handle_route_close(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processa ROUTE_CLOSE para esta estrategia."""
