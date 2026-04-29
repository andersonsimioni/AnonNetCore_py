from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ..services import EngineServices


class ProtocolMessageHandler(ABC):
    """Handler de alto nivel para uma familia de mensagens de protocolo."""

    protocol_family: str
    supported_message_types: set[str]

    def can_handle(self, message_type: str | None) -> bool:
        return message_type is not None and message_type in self.supported_message_types

    @abstractmethod
    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        raise NotImplementedError
