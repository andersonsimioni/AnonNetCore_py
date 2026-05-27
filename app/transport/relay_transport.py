from __future__ import annotations

from collections.abc import Awaitable, Callable

from .interfaces import InboundPacketHandler, TransportAdapter
from .models import OutboundMessage, TransportEndpoint, TransportPacket, TransportState


RelayPacketSender = Callable[[OutboundMessage], Awaitable[None]]


class RelayTcpTransportAdapter(TransportAdapter):
    """Transporte virtual que encapsula pacotes fisicos dentro de um relay TCP."""

    transport_name = "relay_tcp"

    def __init__(self, relay_sender: RelayPacketSender | None = None) -> None:
        self._relay_sender = relay_sender
        self._inbound_packet_handler: InboundPacketHandler | None = None
        self._state = TransportState.STOPPED

    @property
    def state(self) -> TransportState:
        return self._state

    def set_relay_sender(self, relay_sender: RelayPacketSender) -> None:
        self._relay_sender = relay_sender

    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        self._inbound_packet_handler = handler

    async def start(self) -> None:
        self._state = TransportState.STARTED

    async def stop(self) -> None:
        self._state = TransportState.STOPPED

    async def send(self, message: OutboundMessage) -> None:
        if self._relay_sender is None:
            raise RuntimeError("relay_tcp transport sender was not configured.")
        if message.remote_endpoint.metadata:
            message = OutboundMessage(
                transport_name=message.transport_name,
                payload=message.payload,
                remote_endpoint=message.remote_endpoint,
                local_endpoint=message.local_endpoint,
                metadata={**message.remote_endpoint.metadata, **message.metadata},
            )
        await self._relay_sender(message)

    async def inject_inbound_packet(
        self,
        *,
        payload: bytes,
        remote_endpoint: TransportEndpoint,
        local_endpoint: TransportEndpoint | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        if self._inbound_packet_handler is None:
            return

        await self._inbound_packet_handler(
            TransportPacket(
                transport_name=self.transport_name,
                payload=payload,
                remote_endpoint=remote_endpoint,
                local_endpoint=local_endpoint,
                metadata=metadata or {},
            )
        )
