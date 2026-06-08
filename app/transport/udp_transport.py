from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Callable

from common import utc_now
from .interfaces import InboundPacketHandler, TransportAdapter
from .models import OutboundMessage, TransportEndpoint, TransportPacket, TransportState


UDP_KEEPALIVE_PAYLOAD = b"ANUDP_KEEPALIVE_V1"
UINT32_MAX = 4_294_967_295
UDP_DATA_HEADER_SIZE = 12


@dataclass(slots=True)
class _PartialUdpFrame:
    frame_id: int
    total_parts: int
    parts: dict[int, bytes]
    created_at: float

    def is_complete(self) -> bool:
        return len(self.parts) == self.total_parts

    def build_payload(self) -> bytes:
        return b"".join(self.parts[index] for index in range(1, self.total_parts + 1))


class UdpTransportAdapter(TransportAdapter, asyncio.DatagramProtocol):
    """Minimal UDP transport with fixed-size binary fragmentation headers."""

    transport_name = "udp"

    def __init__(
        self,
        *,
        listen_host: str,
        listen_port: int,
        listen_enabled: bool,
        max_datagram_size: int,
        keepalive_interval_seconds: float,
        fragment_reassembly_timeout_seconds: float,
    ) -> None:
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.listen_enabled = listen_enabled
        self.max_datagram_size = max_datagram_size
        self.keepalive_interval_seconds = keepalive_interval_seconds
        self.fragment_reassembly_timeout_seconds = fragment_reassembly_timeout_seconds
        self._state = TransportState.STOPPED
        self._inbound_packet_handler: InboundPacketHandler | None = None
        self._transport: asyncio.DatagramTransport | None = None
        self._local_endpoint: TransportEndpoint | None = None
        self._known_peers: set[tuple[str, int]] = set()
        self._partial_frames: dict[tuple[str, int, int], _PartialUdpFrame] = {}
        self._keepalive_task: asyncio.Task[None] | None = None
        self.debug_logger: Callable[[str, dict[str, Any]], None] | None = None

    @property
    def state(self) -> TransportState:
        return self._state

    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        self._inbound_packet_handler = handler

    async def start(self) -> None:
        if self._state == TransportState.STARTED:
            return

        self._state = TransportState.STARTING
        bind_port = self.listen_port if self.listen_enabled else 0
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: self,
            local_addr=(self.listen_host, bind_port),
        )
        self._transport = transport

        bound_host, bound_port = transport.get_extra_info("sockname")[:2]
        self._local_endpoint = TransportEndpoint(
            transport_name=self.transport_name,
            host=str(bound_host),
            port=int(bound_port),
            metadata={"advertised_listener": self.listen_enabled},
        )
        self._state = TransportState.STARTED
        if self.keepalive_interval_seconds > 0:
            self._keepalive_task = asyncio.create_task(self._run_keepalive_loop())
        self._log_debug(
            "udp transport started",
            {
                "listen_host": self.listen_host,
                "listen_port": self.listen_port,
                "listen_enabled": self.listen_enabled,
                "bound_host": str(bound_host),
                "bound_port": int(bound_port),
                "max_datagram_size": self.max_datagram_size,
                "keepalive_interval_seconds": self.keepalive_interval_seconds,
                "fragment_header_size": UDP_DATA_HEADER_SIZE,
                "max_fragment_payload_size": self._max_fragment_payload_size(),
                "fragment_reassembly_timeout_seconds": self.fragment_reassembly_timeout_seconds,
            },
        )

    async def stop(self) -> None:
        self._state = TransportState.STOPPING
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._keepalive_task
            self._keepalive_task = None
        if self._transport is not None:
            self._transport.close()
        self._transport = None
        self._local_endpoint = None
        self._known_peers.clear()
        self._partial_frames.clear()
        self._state = TransportState.STOPPED

    async def send(self, message: OutboundMessage) -> None:
        if self._transport is None:
            raise RuntimeError("UDP transport is not started and cannot send datagrams.")

        remote = message.remote_endpoint
        self._remember_peer(remote.host, remote.port)
        datagrams = self._build_data_datagrams(message.payload)
        self._log_debug(
            "sending udp payload",
            {
                "remote_host": remote.host,
                "remote_port": remote.port,
                "payload_size_bytes": len(message.payload),
                "datagram_count": len(datagrams),
                "max_datagram_size": self.max_datagram_size,
                "fragment_header_size": UDP_DATA_HEADER_SIZE,
                "max_fragment_payload_size": self._max_fragment_payload_size(),
            },
        )
        for datagram in datagrams:
            self._transport.sendto(
                datagram,
                (self._resolve_dial_host(remote.host), remote.port),
            )

    def datagram_received(self, data: bytes, addr) -> None:
        host, port = str(addr[0]), int(addr[1])
        self._remember_peer(host, port)
        if data == UDP_KEEPALIVE_PAYLOAD:
            self._log_debug(
                "received udp keepalive",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "known_peer_count": len(self._known_peers),
                },
            )
            return

        self._prune_expired_frames()
        payload = self._try_reassemble_data_frame(data, host, port)
        if payload is None:
            return

        if self._inbound_packet_handler is None:
            self._log_debug(
                "dropped udp datagram because inbound handler is missing",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "payload_size_bytes": len(payload),
                },
            )
            return

        self._log_debug(
            "received udp datagram",
            {
                "remote_host": host,
                "remote_port": port,
                "payload_size_bytes": len(payload),
            },
        )
        packet = TransportPacket(
            transport_name=self.transport_name,
            payload=payload,
            local_endpoint=self._local_endpoint
            or TransportEndpoint(
                transport_name=self.transport_name,
                host=self.listen_host,
                port=0,
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
        self._log_debug(
            "udp transport received socket error",
            {"error_type": type(exc).__name__, "error": repr(exc)},
        )

    def _build_data_datagrams(self, payload: bytes) -> list[bytes]:
        chunk_size = self._max_fragment_payload_size()
        chunks = [
            payload[index:index + chunk_size]
            for index in range(0, len(payload), chunk_size)
        ] or [b""]
        if len(chunks) > UINT32_MAX:
            raise ValueError(
                "UDP payload requires more fragments than uint32 can represent: "
                f"fragments={len(chunks)} max={UINT32_MAX}"
            )

        frame_id = _new_udp_frame_id()
        datagrams = [
            self._encode_data_frame(
                frame_id=frame_id,
                part=index,
                total_parts=len(chunks),
                payload=chunk,
            )
            for index, chunk in enumerate(chunks, start=1)
        ]
        for datagram in datagrams:
            if len(datagram) > self.max_datagram_size:
                raise ValueError(
                    "UDP fragment exceeds configured datagram limit: "
                    f"size={len(datagram)} max={self.max_datagram_size}"
                )
        return datagrams

    def _max_fragment_payload_size(self) -> int:
        if self.max_datagram_size <= UDP_DATA_HEADER_SIZE:
            raise ValueError(
                "UDP max datagram size must be larger than the binary fragment header: "
                f"size={self.max_datagram_size} header_size={UDP_DATA_HEADER_SIZE}"
            )
        return self.max_datagram_size - UDP_DATA_HEADER_SIZE

    @staticmethod
    def _encode_data_frame(
        *,
        frame_id: int,
        part: int,
        total_parts: int,
        payload: bytes,
    ) -> bytes:
        if not _is_valid_uint32(frame_id):
            raise ValueError(f"UDP frame id must fit uint32: frame_id={frame_id}")
        if not _is_valid_uint32_fragment_position(part, total_parts):
            raise ValueError(
                "UDP fragment position must fit uint32 and be within total parts: "
                f"part={part} total_parts={total_parts}"
            )
        return (
            frame_id.to_bytes(4, byteorder="big", signed=False)
            + part.to_bytes(4, byteorder="big", signed=False)
            + total_parts.to_bytes(4, byteorder="big", signed=False)
            + payload
        )

    def _try_reassemble_data_frame(self, datagram: bytes, host: str, port: int) -> bytes | None:
        if len(datagram) < UDP_DATA_HEADER_SIZE:
            self._log_debug(
                "dropped short udp data frame",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "datagram_size_bytes": len(datagram),
                },
            )
            return None

        frame_id = int.from_bytes(datagram[0:4], byteorder="big", signed=False)
        part = int.from_bytes(datagram[4:8], byteorder="big", signed=False)
        total_parts = int.from_bytes(datagram[8:12], byteorder="big", signed=False)
        if not _is_valid_uint32(frame_id):
            self._log_debug(
                "dropped invalid udp data frame id",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "frame_id": frame_id,
                },
            )
            return None
        if not _is_valid_uint32_fragment_position(part, total_parts):
            self._log_debug(
                "dropped invalid udp data frame",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "frame_id": frame_id,
                    "part": part,
                    "total_parts": total_parts,
                },
            )
            return None

        payload_part = datagram[UDP_DATA_HEADER_SIZE:]
        if total_parts == 1:
            return payload_part

        partial_key = (host, port, frame_id)
        if part == 1 and partial_key in self._partial_frames:
            self._partial_frames.pop(partial_key, None)

        partial_frame = self._partial_frames.get(partial_key)
        if partial_frame is None:
            partial_frame = _PartialUdpFrame(
                frame_id=frame_id,
                total_parts=total_parts,
                parts={},
                created_at=asyncio.get_running_loop().time(),
            )
            self._partial_frames[partial_key] = partial_frame
        elif partial_frame.total_parts != total_parts:
            self._partial_frames.pop(partial_key, None)
            self._log_debug(
                "dropped udp data frame with changed total parts",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "frame_id": frame_id,
                    "expected_total_parts": partial_frame.total_parts,
                    "received_total_parts": total_parts,
                },
            )
            return None

        partial_frame.parts.setdefault(part, payload_part)
        if not partial_frame.is_complete():
            self._log_debug(
                "received partial udp data frame",
                {
                    "remote_host": host,
                    "remote_port": port,
                    "frame_id": frame_id,
                    "received_parts": len(partial_frame.parts),
                    "total_parts": partial_frame.total_parts,
                },
            )
            return None

        self._partial_frames.pop(partial_key, None)
        payload = partial_frame.build_payload()
        self._log_debug(
            "reassembled udp payload",
            {
                "remote_host": host,
                "remote_port": port,
                "frame_id": frame_id,
                "total_parts": total_parts,
                "payload_size_bytes": len(payload),
            },
        )
        return payload

    def _prune_expired_frames(self) -> None:
        now = asyncio.get_running_loop().time()
        expired_items = [
            (key, frame)
            for key, frame in self._partial_frames.items()
            if now - frame.created_at >= self.fragment_reassembly_timeout_seconds
        ]
        for key, _frame in expired_items:
            self._partial_frames.pop(key, None)
        if not expired_items:
            return

        self._log_debug(
            "expired partial udp frames",
            {
                "expired_count": len(expired_items),
                "frames": [
                    {
                        "remote_host": key[0],
                        "remote_port": key[1],
                        "frame_id": key[2],
                        "received_parts": len(frame.parts),
                        "total_parts": frame.total_parts,
                    }
                    for key, frame in expired_items[:5]
                ],
            },
        )

    async def _run_keepalive_loop(self) -> None:
        while self._state in {TransportState.STARTING, TransportState.STARTED}:
            await asyncio.sleep(self.keepalive_interval_seconds)
            await self._send_keepalives()

    async def _send_keepalives(self) -> None:
        if self._transport is None or not self._known_peers:
            return

        peers = sorted(self._known_peers)
        for host, port in peers:
            self._transport.sendto(
                UDP_KEEPALIVE_PAYLOAD,
                (self._resolve_dial_host(host), port),
            )
        self._log_debug(
            "sent udp keepalives",
            {
                "peer_count": len(peers),
                "interval_seconds": self.keepalive_interval_seconds,
            },
        )

    def _remember_peer(self, host: str, port: int) -> None:
        if not host or port <= 0:
            return

        self._known_peers.add((host, port))

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

    def _log_debug(self, message: str, metadata: dict[str, Any]) -> None:
        if self.debug_logger is not None:
            self.debug_logger(message, metadata)


def _is_valid_uint32_fragment_position(part: int, total_parts: int) -> bool:
    return (
        1 <= part <= UINT32_MAX
        and 1 <= total_parts <= UINT32_MAX
        and part <= total_parts
    )


def _is_valid_uint32(value: int) -> bool:
    return 1 <= value <= UINT32_MAX


def _new_udp_frame_id() -> int:
    return int.from_bytes(os.urandom(4), byteorder="big", signed=False) or 1
