from __future__ import annotations

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class ContentProtocolHandler(ProtocolMessageHandler):
    protocol_family = "content"
    supported_message_types = {
        "OBJECT_GET",
        "OBJECT_CHUNK",
        "OBJECT_END",
        "OBJECT_PUT_ANNOUNCE",
        "OBJECT_PUT_CONFIRM",
        "APP_MESSAGE_SEND",
        "APP_MESSAGE_DELIVERED",
    }

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "has_crypto_service": services.crypto_service is not None,
                "next_step": "implement_object_transfer_and_app_delivery",
            },
        )
