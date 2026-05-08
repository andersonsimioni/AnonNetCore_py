from __future__ import annotations

from .interfaces import InboundPacketHandler, TransportAdapter
from .models import OutboundMessage


class TransportService:
    """Servico central para envio e recebimento de mensagens de transporte."""

    def __init__(self) -> None:
        self._adapters: dict[str, TransportAdapter] = {}
        self._inbound_packet_handler: InboundPacketHandler | None = None

    def register_adapter(self, adapter: TransportAdapter) -> None:
        if adapter.transport_name in self._adapters:
            raise ValueError(f"Transporte '{adapter.transport_name}' ja registrado.")

        adapter.set_inbound_packet_handler(self._dispatch_inbound_packet)
        self._adapters[adapter.transport_name] = adapter

    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        self._inbound_packet_handler = handler

    @property
    def adapters(self) -> dict[str, TransportAdapter]:
        return self._adapters

    async def start(self) -> None:
        for adapter in self._adapters.values():
            await adapter.start()

    async def stop(self) -> None:
        for adapter in self._adapters.values():
            await adapter.stop()

    async def send(self, message: OutboundMessage) -> None:
        adapter = self._adapters.get(message.transport_name)
        if adapter is None:
            raise ValueError(f"Transporte '{message.transport_name}' nao registrado.")

        await adapter.send(message)

    async def _dispatch_inbound_packet(self, packet) -> None:
        if self._inbound_packet_handler is None:
            return

        await self._inbound_packet_handler(packet)
