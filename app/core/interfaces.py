from __future__ import annotations

from abc import ABC, abstractmethod

from .models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from .services import EngineServices


class PacketProcessor(ABC):
    """Processador do pacote JSON ja decodificado."""

    @abstractmethod
    async def process(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        raise NotImplementedError
