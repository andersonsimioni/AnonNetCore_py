from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import PacketContext, PacketProcessingResult, ProtocolEnvelope
    from ..services import EngineServices


class RouteStrategy(ABC):
    """Contrato comum das estrategias de construcao de rota."""

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
