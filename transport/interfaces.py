from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from .models import OutboundMessage, TransportPacket, TransportState


InboundPacketHandler = Callable[[TransportPacket], Awaitable[None]]


class TransportAdapter(ABC):
    """Contrato para qualquer transporte de rede classica."""

    transport_name: str

    @property
    @abstractmethod
    def state(self) -> TransportState:
        raise NotImplementedError

    @abstractmethod
    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send(self, message: OutboundMessage) -> None:
        raise NotImplementedError
