from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ..models import PacketProcessingResult

if TYPE_CHECKING:
    from ..models import PacketContext, ProtocolEnvelope
    from ..services import EngineServices


class RouteStrategy(ABC):
    """Common contract for route construction strategies."""

    strategy_name: str

    def build_initial_route_create(
        self,
        **route_fields: object,
    ) -> dict[str, object]:
        """Builds the initial ROUTE_CREATE payload."""
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
        """Processes ROUTE_CREATE for this strategy."""

    @abstractmethod
    async def handle_route_create_kem_info(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processes ROUTE_CREATE_KEM_INFO for this strategy."""

    @abstractmethod
    async def handle_route_create_validate_and_publish(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processes ROUTE_CREATE_VALIDATE_AND_PUBLISH for this strategy."""

    @abstractmethod
    async def handle_route_create_ok(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processes ROUTE_CREATE_OK for this strategy."""

    @abstractmethod
    async def handle_route_create_ping(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processes ROUTE_CREATE_PING for this strategy."""

    @abstractmethod
    async def handle_route_create_pong(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        """Processes ROUTE_CREATE_PONG for this strategy."""
