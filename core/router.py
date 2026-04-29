from __future__ import annotations

from .message_registry import MessageDefinition, MessageRegistry
from .models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from .services import EngineServices


class MessageRouter:
    """Encaminha mensagens usando o registry central do core."""

    def __init__(self) -> None:
        self.registry = MessageRegistry()

    def get_definition(self, message_type: str | None) -> MessageDefinition | None:
        return self.registry.get_definition(message_type)

    async def route(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return await self.registry.route(envelope, context, services)
