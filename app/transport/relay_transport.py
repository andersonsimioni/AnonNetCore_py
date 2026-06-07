from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

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
        self.debug_logger: Callable[[str, dict[str, Any]], None] | None = None

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
        self._log_debug(
            "sending relay transport packet",
            {
                "relay_host": message.remote_endpoint.host,
                "relay_port": message.remote_endpoint.port,
                "metadata": message.metadata,
                "payload_size_bytes": len(message.payload),
            },
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
            self._log_debug(
                "dropped injected relay packet because inbound handler is missing",
                {
                    "remote_host": remote_endpoint.host,
                    "remote_port": remote_endpoint.port,
                    "metadata": metadata or {},
                    "payload_size_bytes": len(payload),
                },
            )
            return

        self._log_debug(
            "injecting inbound relay transport packet",
            {
                "remote_host": remote_endpoint.host,
                "remote_port": remote_endpoint.port,
                "remote_metadata": remote_endpoint.metadata,
                "packet_metadata": metadata or {},
                "payload_size_bytes": len(payload),
            },
        )
        await self._inbound_packet_handler(
            TransportPacket(
                transport_name=self.transport_name,
                payload=payload,
                remote_endpoint=remote_endpoint,
                local_endpoint=local_endpoint,
                metadata=metadata or {},
            )
        )

    def _log_debug(self, message: str, metadata: dict[str, Any]) -> None:
        if self.debug_logger is not None:
            self.debug_logger(message, metadata)
