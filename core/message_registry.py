from __future__ import annotations

from dataclasses import dataclass

from .models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from .protocols import (
    ContentProtocolHandler,
    DhtProtocolHandler,
    PingProtocolHandler,
    PhysicalNodeInfoExchangeProtocolHandler,
    PhysicalNodeInfoProtocolHandler,
    ProtocolMessageHandler,
    RoutingProtocolHandler,
    SessionProtocolHandler,
)
from .services import EngineServices


@dataclass(slots=True, frozen=True)
class MessageDefinition:
    message_type: str
    handler: ProtocolMessageHandler
    layer: str
    requires_physical_session: bool
    requires_virtual_session: bool = False


class MessageRegistry:
    """Fonte central das regras de roteamento e sessao de cada message_type."""

    def __init__(self) -> None:
        physical_node_info = PhysicalNodeInfoProtocolHandler()
        physical_ping = PingProtocolHandler()
        physical_session = SessionProtocolHandler()
        physical_node_info_exchange = PhysicalNodeInfoExchangeProtocolHandler()
        physical_dht = DhtProtocolHandler()
        physical_routing = RoutingProtocolHandler()
        overlay_content = ContentProtocolHandler()

        self._definitions: dict[str, MessageDefinition] = {}
        self._register_many(
            [
                MessageDefinition(
                    message_type="PING",
                    handler=physical_ping,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PONG",
                    handler=physical_ping,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_NODE_INFO_REQUEST",
                    handler=physical_node_info,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_NODE_INFO_RESPONSE",
                    handler=physical_node_info,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_INIT",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_INIT_OK",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_KEY_CONFIRM",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_READY",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=False,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_KEEPALIVE",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_KEEPALIVE_ACK",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_SESSION_CLOSE",
                    handler=physical_session,
                    layer="physical",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_NODE_INFO_EXCHANGE_REQUEST",
                    handler=physical_node_info_exchange,
                    layer="physical",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_NODE_INFO_EXCHANGE_RESPONSE",
                    handler=physical_node_info_exchange,
                    layer="physical",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="PHYSICAL_NODE_INFO_ANNOUNCE",
                    handler=physical_node_info_exchange,
                    layer="physical",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="OVERLAY_SESSION_INIT",
                    handler=physical_session,
                    layer="overlay",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="OVERLAY_SESSION_INIT_OK",
                    handler=physical_session,
                    layer="overlay",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="OVERLAY_SESSION_KEY_CONFIRM",
                    handler=physical_session,
                    layer="overlay",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="OVERLAY_SESSION_READY",
                    handler=physical_session,
                    layer="overlay",
                    requires_physical_session=True,
                ),
                MessageDefinition(
                    message_type="OVERLAY_SESSION_CLOSE",
                    handler=physical_session,
                    layer="overlay",
                    requires_physical_session=True,
                ),
            ]
        )
        self._register_handler(
            physical_dht,
            layer="physical",
            requires_physical_session=True,
        )
        self._register_handler(
            physical_routing,
            layer="physical",
            requires_physical_session=True,
        )
        self._register_handler(
            overlay_content,
            layer="overlay",
            requires_physical_session=True,
        )

    def get_definition(self, message_type: str | None) -> MessageDefinition | None:
        if message_type is None:
            return None
        return self._definitions.get(message_type)

    async def route(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        definition = self.get_definition(envelope.message_type)
        if definition is None:
            return self._build_unmapped_result(envelope, context)

        return await definition.handler.handle(envelope, context, services)

    def _register_handler(
        self,
        handler: ProtocolMessageHandler,
        *,
        layer: str,
        requires_physical_session: bool,
        requires_virtual_session: bool = False,
    ) -> None:
        self._register_many(
            [
                MessageDefinition(
                    message_type=message_type,
                    handler=handler,
                    layer=layer,
                    requires_physical_session=requires_physical_session,
                    requires_virtual_session=requires_virtual_session,
                )
                for message_type in handler.supported_message_types
            ]
        )

    def _register_many(self, definitions: list[MessageDefinition]) -> None:
        for definition in definitions:
            self._definitions[definition.message_type] = definition

    @staticmethod
    def _build_unmapped_result(
        envelope: ProtocolEnvelope,
        context: PacketContext,
    ) -> PacketProcessingResult:
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "unmapped",
                "transport_name": context.transport_name,
                "recognized_keys": sorted(payload.keys()),
                "next_step": "register_message_handler",
            },
        )
