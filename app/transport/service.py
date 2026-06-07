from __future__ import annotations

from .interfaces import InboundPacketHandler, TransportAdapter
from .models import OutboundMessage


class TransportService:
    """Central service for sending and receiving transport messages."""

    def __init__(self) -> None:
        self._adapters: dict[str, TransportAdapter] = {}
        self._inbound_packet_handler: InboundPacketHandler | None = None

    def register_adapter(self, adapter: TransportAdapter) -> None:
        if adapter.transport_name in self._adapters:
            raise ValueError(f"Transport '{adapter.transport_name}' is already registered.")

        adapter.set_inbound_packet_handler(self._dispatch_inbound_packet)
        self._adapters[adapter.transport_name] = adapter

    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        self._inbound_packet_handler = handler

    @property
    def adapters(self) -> dict[str, TransportAdapter]:
        return self._adapters

    def has_adapter(self, transport_name: str) -> bool:
        return transport_name in self._adapters

    async def start(self) -> None:
        for adapter in self._adapters.values():
            await adapter.start()

    async def stop(self) -> None:
        for adapter in self._adapters.values():
            await adapter.stop()

    async def send(self, message: OutboundMessage) -> None:
        adapter = self._adapters.get(message.transport_name)
        if adapter is None:
            raise ValueError(f"Transport '{message.transport_name}' is not registered.")

        await adapter.send(message)

    async def _dispatch_inbound_packet(self, packet) -> None:
        if self._inbound_packet_handler is None:
            return

        await self._inbound_packet_handler(packet)
