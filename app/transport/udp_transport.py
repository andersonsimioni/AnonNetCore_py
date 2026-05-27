from __future__ import annotations

import asyncio
import base64
import json
import os
from dataclasses import dataclass
from uuid import uuid4

from common import utc_now
from .interfaces import InboundPacketHandler, TransportAdapter
from .models import OutboundMessage, TransportEndpoint, TransportPacket, TransportState


UDP_FRAME_MAGIC = "ANUDP1"


@dataclass(slots=True, frozen=True)
class UdpTransportConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 29001
    listen_enabled: bool = True
    max_datagram_size: int = 1200
    chunk_payload_size: int = 512
    max_frame_size: int = 1024 * 1024
    reassembly_timeout_seconds: float = 10.0


@dataclass(slots=True)
class _PartialUdpFrame:
    total_size: int
    chunk_count: int
    chunks: dict[int, bytes]
    created_at: float

    def is_complete(self) -> bool:
        return len(self.chunks) == self.chunk_count

    def build_payload(self) -> bytes:
        return b"".join(self.chunks[index] for index in range(self.chunk_count))


@dataclass(slots=True, frozen=True)
class _DecodedUdpChunk:
    frame_id: str
    chunk_index: int
    chunk_count: int
    total_size: int
    payload: bytes


class UdpTransportAdapter(TransportAdapter, asyncio.DatagramProtocol):
    """UDP transport with JSON chunking and in-memory reassembly."""

    transport_name = "udp"

    def __init__(self, config: UdpTransportConfig | None = None) -> None:
        self.config = config or UdpTransportConfig()
        self._state = TransportState.STOPPED
        self._inbound_packet_handler: InboundPacketHandler | None = None
        self._transport: asyncio.DatagramTransport | None = None
        self._partials: dict[tuple[str, int, str], _PartialUdpFrame] = {}

    @property
    def state(self) -> TransportState:
        return self._state

    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        self._inbound_packet_handler = handler

    async def start(self) -> None:
        if self._state == TransportState.STARTED:
            return

        self._state = TransportState.STARTING
        if self.config.listen_enabled:
            loop = asyncio.get_running_loop()
            transport, _ = await loop.create_datagram_endpoint(
                lambda: self,
                local_addr=(self.config.listen_host, self.config.listen_port),
            )
            self._transport = transport
        self._state = TransportState.STARTED

    async def stop(self) -> None:
        self._state = TransportState.STOPPING
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        self._partials.clear()
        self._state = TransportState.STOPPED

    async def send(self, message: OutboundMessage) -> None:
        if self._transport is None:
            raise RuntimeError("UDP transport is not listening and cannot send datagrams.")

        remote = message.remote_endpoint
        for datagram in self._encode_payload(message.payload):
            if len(datagram) > self.config.max_datagram_size:
                raise ValueError(
                    "UDP encoded datagram exceeds configured limit: "
                    f"size={len(datagram)} max={self.config.max_datagram_size}"
                )
            self._transport.sendto(
                datagram,
                (self._resolve_dial_host(remote.host), remote.port),
            )

    def datagram_received(self, data: bytes, addr) -> None:
        host, port = str(addr[0]), int(addr[1])
        try:
            completed_payload = self._decode_datagram(data, host, port)
        except ValueError:
            return
        if completed_payload is None:
            return
        if self._inbound_packet_handler is None:
            return

        packet = TransportPacket(
            transport_name=self.transport_name,
            payload=completed_payload,
            local_endpoint=TransportEndpoint(
                transport_name=self.transport_name,
                host=self.config.listen_host,
                port=self.config.listen_port,
            ),
            remote_endpoint=TransportEndpoint(
                transport_name=self.transport_name,
                host=host,
                port=port,
            ),
            received_at=utc_now(),
        )
        asyncio.create_task(self._inbound_packet_handler(packet))

    def error_received(self, exc: Exception) -> None:
        return

    def _encode_payload(self, payload: bytes) -> list[bytes]:
        frame_id = str(uuid4())
        chunk_size = max(1, self.config.chunk_payload_size)
        chunks = [
            payload[index:index + chunk_size]
            for index in range(0, len(payload), chunk_size)
        ] or [b""]

        return [
            self._encode_chunk(
                frame_id=frame_id,
                chunk_index=index,
                chunk_count=len(chunks),
                total_size=len(payload),
                payload=chunk,
            )
            for index, chunk in enumerate(chunks)
        ]

    @staticmethod
    def _encode_chunk(
        *,
        frame_id: str,
        chunk_index: int,
        chunk_count: int,
        total_size: int,
        payload: bytes,
    ) -> bytes:
        frame = {
            "magic": UDP_FRAME_MAGIC,
            "version": 1,
            "frame_id": frame_id,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "total_size": total_size,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }
        return json.dumps(frame, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def _decode_datagram(self, datagram: bytes, host: str, port: int) -> bytes | None:
        self._prune_expired_partials()
        chunk = self._decode_chunk(datagram)
        if chunk.total_size > self.config.max_frame_size:
            raise ValueError("UDP frame exceeds max_frame_size.")

        key = (host, port, chunk.frame_id)
        partial = self._partials.get(key)
        if partial is None:
            partial = _PartialUdpFrame(
                total_size=chunk.total_size,
                chunk_count=chunk.chunk_count,
                chunks={},
                created_at=asyncio.get_running_loop().time(),
            )
            self._partials[key] = partial

        if partial.total_size != chunk.total_size or partial.chunk_count != chunk.chunk_count:
            self._partials.pop(key, None)
            raise ValueError("UDP frame metadata changed during reassembly.")

        partial.chunks.setdefault(chunk.chunk_index, chunk.payload)
        if not partial.is_complete():
            return None

        self._partials.pop(key, None)
        payload = partial.build_payload()
        if len(payload) != partial.total_size:
            raise ValueError("UDP reassembled payload size mismatch.")
        return payload

    @staticmethod
    def _decode_chunk(datagram: bytes) -> _DecodedUdpChunk:
        try:
            frame = json.loads(datagram.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("Invalid UDP JSON frame.") from error

        if not isinstance(frame, dict) or frame.get("magic") != UDP_FRAME_MAGIC:
            raise ValueError("Invalid UDP frame magic.")

        frame_id = frame.get("frame_id")
        chunk_index = frame.get("chunk_index")
        chunk_count = frame.get("chunk_count")
        total_size = frame.get("total_size")
        payload_b64 = frame.get("payload_b64")
        if (
            not isinstance(frame_id, str)
            or not isinstance(chunk_index, int)
            or not isinstance(chunk_count, int)
            or not isinstance(total_size, int)
            or not isinstance(payload_b64, str)
            or chunk_index < 0
            or chunk_count <= 0
            or chunk_index >= chunk_count
            or total_size < 0
        ):
            raise ValueError("Invalid UDP frame fields.")

        try:
            payload = base64.b64decode(payload_b64.encode("ascii"), validate=True)
        except ValueError as error:
            raise ValueError("Invalid UDP frame payload.") from error

        return _DecodedUdpChunk(
            frame_id=frame_id,
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            total_size=total_size,
            payload=payload,
        )

    def _prune_expired_partials(self) -> None:
        now = asyncio.get_running_loop().time()
        expired_keys = [
            key
            for key, partial in self._partials.items()
            if now - partial.created_at >= self.config.reassembly_timeout_seconds
        ]
        for key in expired_keys:
            self._partials.pop(key, None)

    @staticmethod
    def _resolve_dial_host(advertised_host: str) -> str:
        local_advertised_host = os.getenv("ANONNET_ADVERTISED_TCP_HOST")
        docker_host_gateway = os.getenv("ANONNET_DOCKER_HOST_GATEWAY")
        if (
            local_advertised_host
            and docker_host_gateway
            and advertised_host == local_advertised_host
        ):
            return docker_host_gateway

        return advertised_host
