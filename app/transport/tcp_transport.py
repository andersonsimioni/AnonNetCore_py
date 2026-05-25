from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime

from common import utc_now
from .frame_codec import LengthPrefixedFrameCodec
from .interfaces import InboundPacketHandler, TransportAdapter
from .models import OutboundMessage, TransportEndpoint, TransportPacket, TransportState


@dataclass(slots=True)
class TcpConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    local_endpoint: TransportEndpoint
    remote_endpoint: TransportEndpoint
    reader_task: asyncio.Task
    last_activity_at: datetime

    def touch(self) -> None:
        self.last_activity_at = utc_now()


@dataclass(slots=True, frozen=True)
class TcpTransportConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 9000
    backlog: int = 100
    idle_timeout_seconds: int = 60


class TcpTransportAdapter(TransportAdapter):
    transport_name = "tcp"

    def __init__(self, config: TcpTransportConfig | None = None) -> None:
        self.config = config or TcpTransportConfig()
        self._state = TransportState.STOPPED
        self._inbound_packet_handler: InboundPacketHandler | None = None
        self._server: asyncio.AbstractServer | None = None
        self._connections: dict[str, TcpConnection] = {}

    @property
    def state(self) -> TransportState:
        return self._state

    def set_inbound_packet_handler(self, handler: InboundPacketHandler) -> None:
        self._inbound_packet_handler = handler

    async def start(self) -> None:
        if self._state == TransportState.STARTED:
            return

        self._state = TransportState.STARTING
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self.config.listen_host,
            port=self.config.listen_port,
            backlog=self.config.backlog,
        )
        self._state = TransportState.STARTED

    async def stop(self) -> None:
        self._state = TransportState.STOPPING
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        for connection in list(self._connections.values()):
            await self._close_connection(connection)

        self._state = TransportState.STOPPED

    async def send(self, message: OutboundMessage) -> None:
        if message.metadata.get("force_new_connection") is True:
            await self._close_existing_connection(message.remote_endpoint)

        connection = await self._get_or_create_connection(message.remote_endpoint)
        try:
            await LengthPrefixedFrameCodec.write_frame(connection.writer, message.payload)
            connection.touch()
            return
        except (ConnectionError, OSError, RuntimeError):
            await self._close_connection(connection)

        connection = await self._get_or_create_connection(message.remote_endpoint)
        await LengthPrefixedFrameCodec.write_frame(connection.writer, message.payload)
        connection.touch()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        connection = self._register_connection(reader, writer)
        try:
            await connection.reader_task
        finally:
            await self._close_connection(connection)

    async def _get_or_create_connection(
        self,
        remote_endpoint: TransportEndpoint,
    ) -> TcpConnection:
        connection_key = self._build_connection_key(remote_endpoint)
        existing_connection = self._connections.get(connection_key)
        if existing_connection is not None and not existing_connection.writer.is_closing():
            return existing_connection

        reader, writer = await asyncio.open_connection(
            host=self._resolve_dial_host(remote_endpoint.host),
            port=remote_endpoint.port,
        )
        return self._register_connection(reader, writer)

    def _register_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> TcpConnection:
        local_host, local_port = writer.get_extra_info("sockname")[:2]
        peer_host, peer_port = writer.get_extra_info("peername")[:2]

        local_endpoint = TransportEndpoint(
            transport_name=self.transport_name,
            host=local_host,
            port=local_port,
        )
        remote_endpoint = TransportEndpoint(
            transport_name=self.transport_name,
            host=peer_host,
            port=peer_port,
        )

        connection = TcpConnection(
            reader=reader,
            writer=writer,
            local_endpoint=local_endpoint,
            remote_endpoint=remote_endpoint,
            reader_task=asyncio.create_task(self._read_packets(reader, writer)),
            last_activity_at=utc_now(),
        )
        self._connections[self._build_connection_key(remote_endpoint)] = connection
        return connection

    async def _close_existing_connection(
        self,
        remote_endpoint: TransportEndpoint,
    ) -> None:
        existing_connection = self._connections.get(self._build_connection_key(remote_endpoint))
        if existing_connection is not None:
            await self._close_connection(existing_connection)

    async def _read_packets(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        local_host, local_port = writer.get_extra_info("sockname")[:2]
        peer_host, peer_port = writer.get_extra_info("peername")[:2]

        local_endpoint = TransportEndpoint(
            transport_name=self.transport_name,
            host=local_host,
            port=local_port,
        )
        remote_endpoint = TransportEndpoint(
            transport_name=self.transport_name,
            host=peer_host,
            port=peer_port,
        )
        connection_key = self._build_connection_key(remote_endpoint)

        try:
            while True:
                payload = await asyncio.wait_for(
                    LengthPrefixedFrameCodec.read_frame(reader),
                    timeout=self.config.idle_timeout_seconds,
                )
                connection = self._connections.get(connection_key)
                if connection is not None:
                    connection.touch()

                packet = TransportPacket(
                    transport_name=self.transport_name,
                    payload=payload,
                    local_endpoint=local_endpoint,
                    remote_endpoint=remote_endpoint,
                )
                if self._inbound_packet_handler is not None:
                    await self._inbound_packet_handler(packet)
        except (asyncio.IncompleteReadError, TimeoutError):
            return

    async def _close_connection(self, connection: TcpConnection) -> None:
        connection_key = self._build_connection_key(connection.remote_endpoint)
        self._connections.pop(connection_key, None)

        if connection.reader_task is not asyncio.current_task() and not connection.reader_task.done():
            connection.reader_task.cancel()
            try:
                await connection.reader_task
            except asyncio.CancelledError:
                pass

        if not connection.writer.is_closing():
            connection.writer.close()
            await connection.writer.wait_closed()

    @staticmethod
    def _build_connection_key(endpoint: TransportEndpoint) -> str:
        return f"{endpoint.transport_name}:{endpoint.host}:{endpoint.port}"

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
