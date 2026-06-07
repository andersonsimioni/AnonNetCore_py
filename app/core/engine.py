from __future__ import annotations

import asyncio
import json
import os
import re
import socket
from pathlib import Path
from uuid import uuid4

from bootstrap import BootstrapResolutionResult
from common import load_json_object
from crypto import aes_decrypt_hex, aes_encrypt_hex
from .network import detect_local_network_host
from .models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from .router import MessageRouter
from .services import EngineServices
from transport import (
    OutboundMessage,
    RelayTcpTransportAdapter,
    TcpTransportAdapter,
    TransportEndpoint,
    TransportPacket,
    TransportService,
    UdpTransportAdapter,
)


def _should_force_new_forward_connection(message_type: str) -> bool:
    return message_type.startswith("ROUTE_CREATE")


def _read_metadata_string(metadata: dict[str, object], field_name: str) -> str | None:
    value = metadata.get(field_name)
    return value if isinstance(value, str) and value else None


def _read_payload_path_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("path_id")
    return value if isinstance(value, str) and value else None


def _can_forward_through_observed_only_session(message_type: str) -> bool:
    return message_type == "PHYSICAL_RELAY_DATA"


class CoreEngine:
    """Nucleo generico para identificar e processar pacotes recebidos."""

    _PLAINTEXT_SESSION_MESSAGE_TYPES = {
        "PHYSICAL_SESSION_INIT",
        "PHYSICAL_SESSION_INIT_OK",
        "PHYSICAL_SESSION_KEY_CONFIRM",
        "PHYSICAL_SESSION_READY",
    }

    def __init__(self, services: EngineServices | None = None) -> None:
        self.services = services or EngineServices()
        self.services.ensure_defaults()
        self.services.bind_engine(self)
        self._configure_relay_transport_adapter()
        self.services.transport.set_inbound_packet_handler(self.handle_transport_packet)
        self.bootstrap_result: BootstrapResolutionResult | None = None
        self.message_router = MessageRouter()
        self._message_sequence = 0
        self._api_http_server = None
        self._api_websocket_server = None
        self._local_relay_endpoint: dict[str, object] | None = None

    async def start(self) -> None:
        self._configure_runtime_environment()
        self.services.database.create_schema()
        self.services.identity_service.ensure_local_physical_node()
        self.bootstrap_result = await self._run_bootstrap()
        await self.services.transport.start()
        self.services.log_service.info("engine", "engine started")
        self._log_loaded_bootstrap_targets()
        await self._request_bootstrap_physical_node_info()
        if self.services.runtime_services is not None:
            await self.services.runtime_services.start()
        await self._start_api_server_if_enabled()

    async def stop(self) -> None:
        await self._stop_api_server()
        if self.services.runtime_services is not None:
            await self.services.runtime_services.stop()
        await self.services.transport.stop()
        self.services.log_service.info("engine", "engine stopped")

    async def send_packet(self, message: OutboundMessage) -> None:
        prepared_message = self._prepare_outbound_message(message)
        await self.services.transport.send(prepared_message)

    def _configure_relay_transport_adapter(self) -> None:
        adapter = self.services.transport.adapters.get("relay_tcp")
        if adapter is None:
            relay_adapter = RelayTcpTransportAdapter(self._send_relay_transport_packet)
            self._configure_relay_adapter_logging(relay_adapter)
            self.services.transport.register_adapter(relay_adapter)
            return

        if isinstance(adapter, RelayTcpTransportAdapter):
            adapter.set_relay_sender(self._send_relay_transport_packet)
            self._configure_relay_adapter_logging(adapter)

    def _configure_relay_adapter_logging(self, adapter: RelayTcpTransportAdapter) -> None:
        adapter.debug_logger = lambda message, metadata: self.services.log_service.debug(
            "relay_tcp_transport",
            message,
            **metadata,
        )

    async def _send_relay_transport_packet(self, message: OutboundMessage) -> None:
        if self.services.protocol_clients is None:
            raise RuntimeError("Protocol clients are not initialized.")

        relay_metadata = {
            **message.remote_endpoint.metadata,
            **message.metadata,
        }
        target_physical_node_id = _read_metadata_string(relay_metadata, "target_physical_node_id")
        relay_physical_node_id = _read_metadata_string(relay_metadata, "relay_physical_node_id")
        if target_physical_node_id is None:
            self.services.log_service.warning(
                "engine",
                "relay tcp packet is missing target physical node id",
                transport=message.transport_name,
                relay_host=message.remote_endpoint.host,
                relay_port=message.remote_endpoint.port,
                endpoint_metadata_keys=sorted(message.remote_endpoint.metadata.keys()),
                message_metadata_keys=sorted(message.metadata.keys()),
            )
            raise ValueError("relay_tcp message requires target_physical_node_id metadata.")
        if relay_physical_node_id is None:
            relay_physical_node_id = self._resolve_relay_node_id_from_endpoint(message.remote_endpoint)
        if relay_physical_node_id is None:
            self.services.log_service.warning(
                "engine",
                "relay tcp packet is missing relay physical node id",
                transport=message.transport_name,
                target_physical_node_id=target_physical_node_id,
                relay_host=message.remote_endpoint.host,
                relay_port=message.remote_endpoint.port,
                endpoint_metadata_keys=sorted(message.remote_endpoint.metadata.keys()),
                message_metadata_keys=sorted(message.metadata.keys()),
            )
            raise ValueError("relay_tcp message requires relay_physical_node_id metadata.")

        relay_session = await self._ensure_active_physical_session(relay_physical_node_id)
        await self._send_message_over_physical_session(
            session=relay_session,
            message_type="PHYSICAL_RELAY_DATA",
            payload={
                "target_physical_node_id": target_physical_node_id,
                "payload_hex": message.payload.hex(),
            },
        )
        self.services.log_service.debug(
            "engine",
            "sent relay transport packet",
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=target_physical_node_id,
            relay_session_id=relay_session.session_id,
            payload_size_bytes=len(message.payload),
        )

    def _resolve_relay_node_id_from_endpoint(self, endpoint: TransportEndpoint) -> str | None:
        for transport in (endpoint.transport_name, "tcp"):
            relay_node_id = self.services.identity_service.find_remote_physical_node_id_by_endpoint(
                transport=transport,
                host=endpoint.host,
                port=endpoint.port,
            )
            if relay_node_id is not None:
                return relay_node_id
        return None

    async def handle_transport_packet(self, packet: TransportPacket) -> None:
        context = PacketContext(
            transport_name=packet.transport_name,
            payload=packet.payload,
            remote_host=packet.remote_endpoint.host,
            remote_port=packet.remote_endpoint.port,
            local_host=packet.local_endpoint.host if packet.local_endpoint else None,
            local_port=packet.local_endpoint.port if packet.local_endpoint else None,
            received_at=packet.received_at,
            metadata=packet.metadata,
        )
        result = await self.process_received_packet(context)
        if result.response_payload is not None:
            try:
                response_endpoint = self._build_response_endpoint(
                    packet.remote_endpoint,
                    packet.metadata,
                )
                await self.send_packet(
                    OutboundMessage(
                        transport_name=packet.transport_name,
                        payload=result.response_payload,
                        remote_endpoint=response_endpoint,
                        metadata=packet.metadata,
                    )
                )
            except (ConnectionError, OSError) as error:
                self.services.log_service.warning(
                    "engine",
                    "failed to send direct response packet",
                    transport=packet.transport_name,
                    remote_host=packet.remote_endpoint.host,
                    remote_port=packet.remote_endpoint.port,
                    error_type=type(error).__name__,
                    error=repr(error),
                )
            except Exception as error:
                self.services.log_service.warning(
                    "engine",
                    "failed to send direct response packet",
                    transport=packet.transport_name,
                    remote_host=packet.remote_endpoint.host,
                    remote_port=packet.remote_endpoint.port,
                    packet_metadata=packet.metadata,
                    error_type=type(error).__name__,
                    error=repr(error),
                )
        try:
            await self._execute_processing_result_actions(result)
        except (ConnectionError, OSError) as error:
            self.services.log_service.warning(
                "engine",
                "failed to execute packet result action",
                action=result.metadata.get("action"),
                message_type=result.message_type,
                error_type=type(error).__name__,
                error=repr(error),
            )
        except Exception as error:
            self.services.log_service.warning(
                "engine",
                "failed to execute packet result action",
                action=result.metadata.get("action"),
                message_type=result.message_type,
                error_type=type(error).__name__,
                error=repr(error),
            )

    async def process_received_packet(self, context: PacketContext) -> PacketProcessingResult:
        try:
            envelope = self._decode_json_packet(context)
        except ValueError as error:
            self.services.log_service.warning(
                "engine",
                "failed to decode inbound packet",
                transport=context.transport_name,
                remote_host=context.remote_host,
                remote_port=context.remote_port,
                local_host=context.local_host,
                local_port=context.local_port,
                metadata=context.metadata,
                error=str(error),
            )
            return PacketProcessingResult(
                protocol_name="json",
                handled=False,
                message_type=None,
                metadata={
                    "reason": "invalid_json_packet",
                    "transport_name": context.transport_name,
                    "error": str(error),
                },
            )

        if _should_force_new_forward_connection(envelope.message_type or ""):
            self.services.log_service.info(
                "engine",
                "route packet decoded",
                message_type=envelope.message_type,
                path_id=_read_payload_path_id(envelope.payload),
                physical_session_id=envelope.header.get("physical_session_id"),
                message_id=envelope.header.get("message_id"),
                message_sequence=envelope.header.get("message_sequence"),
                payload_encrypted=envelope.header.get("payload_encrypted") is True,
                transport=context.transport_name,
                remote_host=context.remote_host,
                remote_port=context.remote_port,
                local_host=context.local_host,
                local_port=context.local_port,
                context_metadata=context.metadata,
            )
        return await self.process_protocol_envelope(envelope, context)

    async def process_protocol_envelope(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
    ) -> PacketProcessingResult:
        session_validation = self._validate_message_policy(envelope)
        if session_validation is not None:
            return session_validation

        return await self.message_router.route(envelope, context, self.services)

    async def _run_bootstrap(self) -> BootstrapResolutionResult:
        return await self.services.bootstrap_service.load_bootstrap_targets(
            dns_seeds=self.services.config.bootstrap_dns_seeds,
            public_endpoints=self.services.config.bootstrap_public_endpoints,
        )

    async def _start_api_server_if_enabled(self) -> None:
        config = self.services.config
        if not config.api_enabled:
            return
        if self.services.api_service is None:
            return

        from api import CoreHttpApiServer, CoreWebSocketApiServer

        self._api_http_server = CoreHttpApiServer(
            self.services.api_service,
            host=config.api_host,
            port=config.api_port,
            cors_allow_origin=config.api_cors_allow_origin,
        )
        await self._api_http_server.start()
        self.services.log_service.info(
            "core_api",
            "http api server started",
            host=config.api_host,
            port=config.api_port,
        )
        if config.api_websocket_enabled:
            self._api_websocket_server = CoreWebSocketApiServer(
                self.services.api_service,
                host=config.api_host,
                port=config.api_websocket_port,
                path=config.api_websocket_path,
            )
            await self._api_websocket_server.start()
            self.services.log_service.info(
                "core_api",
                "websocket api server started",
                host=config.api_host,
                port=config.api_websocket_port,
                path=config.api_websocket_path,
            )

    async def _stop_api_server(self) -> None:
        if self._api_websocket_server is not None:
            await self._api_websocket_server.stop()
            self._api_websocket_server = None
            self.services.log_service.info("core_api", "websocket api server stopped")
        if self._api_http_server is not None:
            await self._api_http_server.stop()
            self._api_http_server = None
            self.services.log_service.info("core_api", "http api server stopped")

    def _configure_runtime_environment(self) -> None:
        self._configure_transport_listener()
        self._configure_log_service()

    def _configure_transport_listener(self) -> None:
        config = self.services.config
        transport_service = TransportService()
        transport_service.register_adapter(
            TcpTransportAdapter(
                listen_host=config.physical_listen_host,
                listen_port=config.physical_tcp_listen_port,
                listen_enabled=config.tcp_transport_enabled and not self.is_private_physical_node(),
                backlog=config.physical_tcp_backlog,
                idle_timeout_seconds=config.physical_tcp_idle_timeout_seconds,
            )
        )
        transport_service.register_adapter(
            UdpTransportAdapter(
                listen_host=config.physical_listen_host,
                listen_port=self.get_configured_udp_listen_port(),
                listen_enabled=config.udp_transport_enabled and not self.is_private_physical_node(),
                max_datagram_size=config.udp_max_datagram_size,
                keepalive_interval_seconds=config.udp_keepalive_interval_seconds,
                fragment_payload_size=config.udp_fragment_payload_size,
                fragment_send_delay_seconds=config.udp_fragment_send_delay_seconds,
                fragment_reassembly_timeout_seconds=config.udp_fragment_reassembly_timeout_seconds,
            )
        )
        udp_adapter = transport_service.adapters.get("udp")
        if isinstance(udp_adapter, UdpTransportAdapter):
            udp_adapter.debug_logger = lambda message, metadata: self.services.log_service.debug(
                "udp_transport",
                message,
                **metadata,
            )
        transport_service.register_adapter(
            RelayTcpTransportAdapter(self._send_relay_transport_packet)
        )
        relay_adapter = transport_service.adapters.get("relay_tcp")
        if isinstance(relay_adapter, RelayTcpTransportAdapter):
            self._configure_relay_adapter_logging(relay_adapter)
        transport_service.set_inbound_packet_handler(self.handle_transport_packet)
        self.services.transport = transport_service
        self.services.log_service.debug(
            "engine",
            "transport adapters configured",
            registered_transports=list(transport_service.adapters.keys()),
            tcp_listener_enabled=config.tcp_transport_enabled and not self.is_private_physical_node(),
            udp_listener_enabled=config.udp_transport_enabled and not self.is_private_physical_node(),
        )

    def _configure_log_service(self) -> None:
        node_name = self.get_runtime_node_name()
        log_file_path = Path(self.services.config.log_dir) / (
            f"{node_name}-{self.services.config.physical_tcp_listen_port}.log"
        )
        self.services.log_service.configure(
            node_name=node_name,
            log_file_path=log_file_path,
        )

    def _log_loaded_bootstrap_targets(self) -> None:
        if self.bootstrap_result is None:
            self.services.log_service.warning("bootstrap", "no bootstrap result loaded")
            return

        endpoints = [
            f"{endpoint.transport}://{endpoint.host}:{endpoint.port}"
            for endpoint in self.bootstrap_result.all_endpoints
        ]
        self.services.log_service.info(
            "bootstrap",
            "bootstrap targets loaded",
            endpoint_count=len(endpoints),
            endpoints=endpoints,
        )

    async def request_bootstrap_physical_node_info(self) -> None:
        bootstrap_endpoints = self._get_bootstrap_endpoints()
        if not bootstrap_endpoints:
            return

        for attempt in range(1, self.services.config.bootstrap_request_retries + 1):
            for endpoint in bootstrap_endpoints:
                await self._request_single_bootstrap_endpoint(endpoint, attempt)

            if attempt < self.services.config.bootstrap_request_retries:
                await asyncio.sleep(self.services.config.bootstrap_request_delay_seconds)

    async def _request_bootstrap_physical_node_info(self) -> None:
        await self.request_bootstrap_physical_node_info()

    async def _request_single_bootstrap_endpoint(
        self,
        endpoint,
        attempt: int,
    ) -> None:
        try:
            await self.services.protocol_clients.physical.node_info.send_request_to_bootstrap_endpoint(
                endpoint
            )
            self._log_bootstrap_event(
                f"bootstrap request sent target={endpoint.host}:{endpoint.port} attempt={attempt}"
            )
        except Exception as error:
            self._log_bootstrap_event(
                f"bootstrap request failed target={endpoint.host}:{endpoint.port} "
                f"attempt={attempt} error={error}"
            )

    def _get_bootstrap_endpoints(self) -> list:
        if self.bootstrap_result is None:
            return []

        return [
            endpoint
            for endpoint in self.bootstrap_result.all_endpoints
            if not self._is_local_bootstrap_endpoint(endpoint)
        ]

    def _log_bootstrap_event(self, message: str) -> None:
        self.services.log_service.info("bootstrap", message)

    def _is_local_bootstrap_endpoint(self, endpoint) -> bool:
        tcp_adapter = self.services.transport.adapters.get("tcp")
        if tcp_adapter is None:
            return False

        local_port = getattr(tcp_adapter, "listen_port", None)
        if endpoint.port != local_port:
            return False

        local_host_aliases = self._get_local_bootstrap_host_aliases()
        return endpoint.host.lower() in local_host_aliases

    @staticmethod
    def _get_local_bootstrap_host_aliases() -> set[str]:
        hostname = socket.gethostname().lower()
        fqdn = socket.getfqdn().lower()
        return {
            hostname,
            fqdn,
            "localhost",
            "127.0.0.1",
            "0.0.0.0",
        }

    def get_runtime_node_name(self) -> str:
        runtime_node_name = socket.gethostname().lower()
        if runtime_node_name:
            return runtime_node_name

        return "node"

    def get_advertised_tcp_host(self) -> str:
        advertised_host_from_env = os.getenv("ANONNET_ADVERTISED_TCP_HOST")
        if advertised_host_from_env:
            return advertised_host_from_env

        return detect_local_network_host()

    def get_advertised_tcp_port(self) -> int:
        advertised_port_from_env = os.getenv("ANONNET_ADVERTISED_TCP_PORT")
        if advertised_port_from_env:
            try:
                return int(advertised_port_from_env)
            except ValueError:
                pass

        node_name = self.get_runtime_node_name().lower()
        match = re.fullmatch(r"node-(\d{3})", node_name)
        if match is None:
            return self.services.config.physical_tcp_listen_port

        node_index = int(match.group(1))
        return 19001 + node_index - 1

    def get_configured_udp_listen_port(self) -> int:
        if self.services.config.physical_udp_listen_port is not None:
            return self.services.config.physical_udp_listen_port
        return self.services.config.physical_tcp_listen_port + 10000

    def get_advertised_udp_host(self) -> str:
        advertised_host_from_env = os.getenv("ANONNET_ADVERTISED_UDP_HOST")
        if advertised_host_from_env:
            return advertised_host_from_env
        return self.get_advertised_tcp_host()

    def get_advertised_udp_port(self) -> int:
        advertised_port_from_env = os.getenv("ANONNET_ADVERTISED_UDP_PORT")
        if advertised_port_from_env:
            try:
                return int(advertised_port_from_env)
            except ValueError:
                pass
        return self.get_configured_udp_listen_port()

    def is_private_physical_node(self) -> bool:
        return self.services.config.node_reachability.lower() == "private"

    def can_act_as_physical_relay(self) -> bool:
        config = self.services.config
        return (
            config.physical_relay_enabled
            and not self.is_private_physical_node()
            and config.tcp_transport_enabled
        )

    def build_local_physical_endpoints(self, transport_name: str | None = None) -> list[dict[str, object]]:
        if self.is_private_physical_node():
            if self._local_relay_endpoint is None:
                return []
            if transport_name not in {None, "relay_tcp"}:
                return []
            return [dict(self._local_relay_endpoint)]

        endpoints: list[dict[str, object]] = []
        if self.services.config.tcp_transport_enabled and transport_name in {None, "tcp"}:
            endpoints.append(
                {
                    "transport": "tcp",
                    "host": self.get_advertised_tcp_host(),
                    "port": self.get_advertised_tcp_port(),
                    "priority": 0,
                }
            )
        if self.services.config.udp_transport_enabled and transport_name in {None, "udp"}:
            endpoints.append(
                {
                    "transport": "udp",
                    "host": self.get_advertised_udp_host(),
                    "port": self.get_advertised_udp_port(),
                    "priority": 10,
                }
            )
        return endpoints

    def set_local_relay_endpoint(
        self,
        *,
        relay_physical_node_id: str,
        target_physical_node_id: str,
        host: str,
        port: int,
    ) -> None:
        self._local_relay_endpoint = {
            "transport": "relay_tcp",
            "host": host,
            "port": port,
            "priority": 50,
            "metadata": {
                "relay_physical_node_id": relay_physical_node_id,
                "target_physical_node_id": target_physical_node_id,
            },
        }
        self.services.log_service.info(
            "engine",
            "local relay endpoint selected",
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=target_physical_node_id,
            relay_host=host,
            relay_port=port,
        )

    def clear_local_relay_endpoint(self) -> None:
        if self._local_relay_endpoint is None:
            return

        self.services.log_service.warning(
            "engine",
            "local relay endpoint cleared",
            relay_endpoint=self._local_relay_endpoint,
        )
        self._local_relay_endpoint = None

    def build_message_header(
        self,
        *,
        message_type: str,
        physical_session_id: str | None = None,
        virtual_session_id: str | None = None,
    ) -> dict[str, object]:
        self._message_sequence += 1
        return {
            "version": 1,
            "message_type": message_type,
            "message_id": str(uuid4()),
            "message_sequence": self._message_sequence,
            "physical_session_id": physical_session_id,
            "virtual_session_id": virtual_session_id,
        }

    async def forward_message_to_remote_physical_node(
        self,
        *,
        remote_physical_node_id: str,
        message_type: str,
        payload: dict[str, object],
        force_new_connection: bool = False,
    ) -> bool:
        force_new_connection = force_new_connection or _should_force_new_forward_connection(message_type)
        if _should_force_new_forward_connection(message_type):
            self.services.log_service.debug(
                "engine",
                "route forward trace opening physical session",
                remote_physical_node_id=remote_physical_node_id,
                message_type=message_type,
                path_id=payload.get("path_id") if isinstance(payload, dict) else None,
                force_new_connection=force_new_connection,
            )
        try:
            session = await self._ensure_active_physical_session(remote_physical_node_id)
        except Exception as error:
            self.services.log_service.warning(
                "engine",
                "failed to open physical session for forward",
                remote_physical_node_id=remote_physical_node_id,
                message_type=message_type,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return False
        if _should_force_new_forward_connection(message_type):
            self.services.log_service.info(
                "engine",
                "route forward trace resolved physical session",
                requested_remote_physical_node_id=remote_physical_node_id,
                session_id=session.session_id,
                session_remote_identity_id=session.remote_identity_id,
                remote_identity_matches=session.remote_identity_id == remote_physical_node_id,
                session_transport=session.transport,
                session_remote_host=session.remote_host,
                session_remote_port=session.remote_port,
                session_metadata=load_json_object(session.metadata_json),
                message_type=message_type,
                path_id=_read_payload_path_id(payload),
            )
        try:
            await self._send_message_over_physical_session(
                session=session,
                message_type=message_type,
                payload=payload,
                force_new_connection=force_new_connection,
            )
            self.services.log_service.debug(
                "engine",
                "forwarded message to remote physical node",
                session_id=session.session_id,
                remote_physical_node_id=remote_physical_node_id,
                message_type=message_type,
                force_new_connection=force_new_connection,
            )
            if _should_force_new_forward_connection(message_type):
                self.services.log_service.info(
                    "engine",
                    "route forward trace delivered over physical session",
                    session_id=session.session_id,
                    remote_physical_node_id=remote_physical_node_id,
                    message_type=message_type,
                    path_id=payload.get("path_id") if isinstance(payload, dict) else None,
                    transport=session.transport,
                    remote_host=session.remote_host,
                    remote_port=session.remote_port,
                )
            return True
        except (ConnectionError, OSError) as error:
            self.services.log_service.warning(
                "engine",
                "physical session send failed; reopening session and retrying once",
                session_id=session.session_id,
                remote_physical_node_id=remote_physical_node_id,
                message_type=message_type,
                error_type=type(error).__name__,
                error=repr(error),
            )
            self.services.session_manager.close_session(
                session.session_id,
                close_reason="transport_send_failed",
            )

        try:
            retry_session = await self._ensure_active_physical_session(remote_physical_node_id)
        except Exception as error:
            self.services.log_service.warning(
                "engine",
                "failed to reopen physical session for forward retry",
                remote_physical_node_id=remote_physical_node_id,
                message_type=message_type,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return False
        await self._send_message_over_physical_session(
            session=retry_session,
            message_type=message_type,
            payload=payload,
            force_new_connection=True,
        )
        self.services.log_service.debug(
            "engine",
            "forwarded message to remote physical node after retry",
            session_id=retry_session.session_id,
            remote_physical_node_id=remote_physical_node_id,
            message_type=message_type,
            force_new_connection=True,
        )
        return True

    def _prepare_outbound_message(self, message: OutboundMessage) -> OutboundMessage:
        packet = self._load_json_packet(message.payload)
        if packet is None:
            return message

        header = packet.get("header")
        payload = packet.get("payload")
        if not isinstance(header, dict) or not isinstance(payload, dict):
            return message

        session_id = header.get("physical_session_id")
        message_type = header.get("message_type")
        if not isinstance(session_id, str) or not isinstance(message_type, str):
            return message

        if message_type in self._PLAINTEXT_SESSION_MESSAGE_TYPES:
            return message

        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            return message

        plaintext_hex = self._encode_payload_hex(payload)
        encrypted_payload = aes_encrypt_hex(
            plaintext_hex,
            session.shared_secret_hex,
            aad=self._build_physical_payload_aad(header),
        )
        protected_packet = {
            "header": {
                **header,
                "payload_encrypted": True,
            },
            "payload": {
                "ciphertext_hex": encrypted_payload.payload_hex,
            },
        }
        self.services.session_manager.touch_session(session_id)
        return OutboundMessage(
            transport_name=message.transport_name,
            payload=json.dumps(protected_packet, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            remote_endpoint=message.remote_endpoint,
            local_endpoint=message.local_endpoint,
            metadata=message.metadata,
        )

    async def _execute_processing_result_actions(
        self,
        result: PacketProcessingResult,
    ) -> None:
        action = result.metadata.get("action")
        if not isinstance(action, str) or not action:
            return

        if action == "forward_message":
            await self._execute_forward_message_action(result.metadata)
        elif action == "send_payload_to_physical_session":
            await self._execute_send_payload_to_physical_session_action(result.metadata)

    async def _execute_forward_message_action(
        self,
        metadata: dict[str, object],
    ) -> None:
        remote_physical_node_id = metadata.get("target_remote_physical_node_id")
        physical_session_id = metadata.get("target_physical_session_id")
        message_type = metadata.get("forward_message_type")
        payload = metadata.get("forward_payload")

        if not isinstance(message_type, str) or not message_type:
            return
        if not isinstance(payload, dict):
            return

        if isinstance(physical_session_id, str) and physical_session_id:
            session = self.services.session_manager.get_session_by_session_id(physical_session_id)
            if _should_force_new_forward_connection(message_type):
                self.services.log_service.info(
                    "engine",
                    "route forward trace inspecting mapped physical session",
                    mapped_session_id=physical_session_id,
                    mapped_session_exists=session is not None,
                    mapped_session_state=getattr(session, "session_state", None),
                    mapped_session_remote_identity_id=getattr(session, "remote_identity_id", None),
                    mapped_session_transport=getattr(session, "transport", None),
                    mapped_session_remote_host=getattr(session, "remote_host", None),
                    mapped_session_remote_port=getattr(session, "remote_port", None),
                    mapped_session_metadata=(
                        load_json_object(session.metadata_json) if session is not None else None
                    ),
                    target_remote_physical_node_id=remote_physical_node_id,
                    message_type=message_type,
                    path_id=payload.get("path_id"),
                )
            if (
                session is not None
                and session.session_state == "active"
                and (
                    not self._is_observed_only_physical_session(session)
                    or _can_forward_through_observed_only_session(message_type)
                )
            ):
                await self._send_message_over_physical_session(
                    session=session,
                    message_type=message_type,
                    payload=payload,
                )
                self.services.log_service.debug(
                    "engine",
                    "forwarded message through mapped physical session",
                    session_id=session.session_id,
                    remote_physical_node_id=session.remote_identity_id,
                    message_type=message_type,
                )
                if _should_force_new_forward_connection(message_type):
                    self.services.log_service.info(
                        "engine",
                        "route forward trace delivered through mapped physical session",
                        session_id=session.session_id,
                        remote_physical_node_id=session.remote_identity_id,
                        message_type=message_type,
                        path_id=payload.get("path_id"),
                        transport=session.transport,
                        remote_host=session.remote_host,
                        remote_port=session.remote_port,
                    )
                return

            if session is not None and self._is_observed_only_physical_session(session):
                self.services.log_service.debug(
                    "engine",
                    "mapped physical session is observed-only; falling back to node endpoint",
                    session_id=physical_session_id,
                    target_remote_physical_node_id=remote_physical_node_id,
                    message_type=message_type,
                )
            else:
                self.services.log_service.warning(
                    "engine",
                    "mapped physical session is not active for forward",
                    session_id=physical_session_id,
                    target_remote_physical_node_id=remote_physical_node_id,
                    message_type=message_type,
                    path_id=payload.get("path_id"),
                    session_exists=session is not None,
                    session_state=getattr(session, "session_state", None),
                    session_remote_identity_id=getattr(session, "remote_identity_id", None),
                    session_transport=getattr(session, "transport", None),
                    session_remote_host=getattr(session, "remote_host", None),
                    session_remote_port=getattr(session, "remote_port", None),
                )

        if not isinstance(remote_physical_node_id, str) or not remote_physical_node_id:
            if _should_force_new_forward_connection(message_type):
                self.services.log_service.warning(
                    "engine",
                    "route forward trace missing target remote physical node",
                    message_type=message_type,
                    path_id=payload.get("path_id"),
                    metadata=metadata,
                )
            return

        forwarded = await self.forward_message_to_remote_physical_node(
            remote_physical_node_id=remote_physical_node_id,
            message_type=message_type,
            payload=payload,
            force_new_connection=_should_force_new_forward_connection(message_type),
        )
        if _should_force_new_forward_connection(message_type) and not forwarded:
            self.services.log_service.warning(
                "engine",
                "route forward trace failed to reach remote physical node",
                target_remote_physical_node_id=remote_physical_node_id,
                message_type=message_type,
                path_id=payload.get("path_id"),
                metadata=metadata,
            )

    async def _execute_send_payload_to_physical_session_action(
        self,
        metadata: dict[str, object],
    ) -> None:
        session_id = metadata.get("target_physical_session_id")
        payload = metadata.get("payload")

        if not isinstance(session_id, str) or not session_id:
            return
        if not isinstance(payload, bytes):
            return

        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            self.services.log_service.warning(
                "engine",
                "cannot send payload because target physical session is unknown",
                target_physical_session_id=session_id,
                payload_size_bytes=len(payload),
            )
            return
        if session.session_state != "active":
            session = await self._reopen_session_for_payload_return(
                previous_session=session,
                payload_size_bytes=len(payload),
            )
            if session is None:
                return
            payload = self._replace_payload_physical_session_id(
                payload,
                session.session_id,
            )

        endpoint = self._resolve_session_send_endpoint(session)
        if endpoint is None:
            self.services.log_service.warning(
                "engine",
                "cannot send payload because target physical session has no endpoint",
                session_id=session.session_id,
                remote_physical_node_id=session.remote_identity_id,
                session_state=session.session_state,
                payload_size_bytes=len(payload),
            )
            return

        try:
            await self.send_packet(
                OutboundMessage(
                    transport_name=endpoint.transport_name,
                    payload=payload,
                    remote_endpoint=endpoint,
                    metadata={
                        **load_json_object(session.metadata_json),
                        "target_physical_node_id": session.remote_identity_id,
                    },
                )
            )
            self.services.log_service.debug(
                "engine",
                "sent payload to physical session",
                session_id=session.session_id,
                remote_physical_node_id=session.remote_identity_id,
                remote_host=session.remote_host,
                remote_port=session.remote_port,
                send_host=endpoint.host,
                send_port=endpoint.port,
                send_transport=endpoint.transport_name,
                send_metadata=endpoint.metadata,
                payload_size_bytes=len(payload),
            )
        except (ConnectionError, OSError) as error:
            if not self._is_observed_only_physical_session(session):
                self.services.session_manager.close_session(
                    session.session_id,
                    close_reason="transport_send_failed",
                )
            self.services.log_service.warning(
                "engine",
                "failed to send payload to physical session",
                session_id=session.session_id,
                remote_physical_node_id=session.remote_identity_id,
                remote_host=session.remote_host,
                remote_port=session.remote_port,
                send_host=endpoint.host,
                send_port=endpoint.port,
                send_transport=endpoint.transport_name,
                send_metadata=endpoint.metadata,
                session_metadata=session.metadata_json,
                error_type=type(error).__name__,
                error=repr(error),
            )

    @staticmethod
    def _build_response_endpoint(
        remote_endpoint: TransportEndpoint,
        packet_metadata: dict[str, object],
    ) -> TransportEndpoint:
        if not packet_metadata:
            return remote_endpoint
        return TransportEndpoint(
            transport_name=remote_endpoint.transport_name,
            host=remote_endpoint.host,
            port=remote_endpoint.port,
            metadata={**remote_endpoint.metadata, **packet_metadata},
        )

    async def _reopen_session_for_payload_return(
        self,
        *,
        previous_session,
        payload_size_bytes: int,
    ):
        remote_physical_node_id = previous_session.remote_identity_id
        if not isinstance(remote_physical_node_id, str) or not remote_physical_node_id:
            self.services.log_service.warning(
                "engine",
                "cannot reopen physical session for payload return without remote identity",
                previous_session_id=previous_session.session_id,
                previous_session_state=previous_session.session_state,
                payload_size_bytes=payload_size_bytes,
            )
            return None

        self.services.log_service.warning(
            "engine",
            "target physical session is inactive; reopening for payload return",
            previous_session_id=previous_session.session_id,
            previous_session_state=previous_session.session_state,
            remote_physical_node_id=remote_physical_node_id,
            previous_transport=previous_session.transport,
            previous_remote_host=previous_session.remote_host,
            previous_remote_port=previous_session.remote_port,
            payload_size_bytes=payload_size_bytes,
        )
        try:
            new_session_id = await self.services.protocol_clients.physical.session.start_session(
                remote_physical_node_id=remote_physical_node_id,
            )
        except Exception as error:
            self.services.log_service.warning(
                "engine",
                "failed to reopen physical session for payload return",
                previous_session_id=previous_session.session_id,
                remote_physical_node_id=remote_physical_node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return None

        new_session = self.services.session_manager.get_session_by_session_id(new_session_id)
        if new_session is None or new_session.session_state != "active":
            self.services.log_service.warning(
                "engine",
                "reopened physical session is not active for payload return",
                previous_session_id=previous_session.session_id,
                new_session_id=new_session_id,
                new_session_state=(
                    new_session.session_state if new_session is not None else None
                ),
                remote_physical_node_id=remote_physical_node_id,
            )
            return None

        self.services.log_service.info(
            "engine",
            "reopened physical session for payload return",
            previous_session_id=previous_session.session_id,
            new_session_id=new_session.session_id,
            remote_physical_node_id=remote_physical_node_id,
            transport=new_session.transport,
            remote_host=new_session.remote_host,
            remote_port=new_session.remote_port,
        )
        return new_session

    def _replace_payload_physical_session_id(
        self,
        payload: bytes,
        physical_session_id: str,
    ) -> bytes:
        packet = self._load_json_packet(payload)
        if packet is None:
            return payload

        header = packet.get("header")
        if not isinstance(header, dict):
            return payload

        packet["header"] = {
            **header,
            "physical_session_id": physical_session_id,
        }
        return json.dumps(packet, separators=(",", ":"), sort_keys=True).encode("utf-8")

    async def _send_message_over_physical_session(
        self,
        *,
        session,
        message_type: str,
        payload: dict[str, object],
        force_new_connection: bool = False,
    ) -> None:
        endpoint = self._build_session_remote_endpoint(session)
        header = self.build_message_header(
            message_type=message_type,
            physical_session_id=session.session_id,
        )
        packet_bytes = self._build_json_packet_bytes(
            header=header,
            payload=payload,
        )
        if _should_force_new_forward_connection(message_type):
            self.services.log_service.info(
                "engine",
                "route packet sending over physical session",
                session_id=session.session_id,
                session_remote_identity_id=session.remote_identity_id,
                message_type=message_type,
                path_id=_read_payload_path_id(payload),
                transport=session.transport,
                remote_host=session.remote_host,
                remote_port=session.remote_port,
                session_metadata=load_json_object(session.metadata_json),
                force_new_connection=force_new_connection,
            )
        await self.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=packet_bytes,
                remote_endpoint=endpoint,
                metadata={
                    **load_json_object(session.metadata_json),
                    "target_physical_node_id": session.remote_identity_id,
                    "force_new_connection": force_new_connection,
                },
            )
        )

    async def _ensure_active_physical_session(
        self,
        remote_physical_node_id: str,
    ):
        active_session = self.services.session_manager.get_active_physical_session_by_remote_node_id(
            remote_physical_node_id
        )
        if active_session is not None and not self._is_observed_only_physical_session(active_session):
            return active_session
        if active_session is not None:
            self.services.log_service.debug(
                "engine",
                "ignoring observed-only physical session for outbound forward",
                session_id=active_session.session_id,
                remote_physical_node_id=remote_physical_node_id,
                remote_host=active_session.remote_host,
                remote_port=active_session.remote_port,
            )

        session_id = await self.services.protocol_clients.physical.session.start_session(
            remote_physical_node_id=remote_physical_node_id,
        )
        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise RuntimeError("The physical session was created but is not available in memory.")
        return session

    @staticmethod
    def _build_session_remote_endpoint(session) -> TransportEndpoint:
        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("The physical session has no associated remote endpoint.")

        return TransportEndpoint(
            transport_name=session.transport,
            host=session.remote_host,
            port=session.remote_port,
            metadata=load_json_object(session.metadata_json),
        )

    def _resolve_session_send_endpoint(self, session) -> TransportEndpoint | None:
        if not self._is_observed_only_physical_session(session):
            return self._build_session_remote_endpoint(session)

        endpoints = self.services.identity_service.list_remote_physical_node_endpoints(
            session.remote_identity_id,
            only_active=True,
        )
        if not endpoints:
            self.services.log_service.warning(
                "engine",
                "observed-only session has no advertised endpoint; skipping direct session payload",
                session_id=session.session_id,
                remote_physical_node_id=session.remote_identity_id,
                observed_host=session.remote_host,
                observed_port=session.remote_port,
            )
            return None

        endpoint = endpoints[0]
        self.services.log_service.debug(
            "engine",
            "using advertised endpoint for observed-only physical session",
            session_id=session.session_id,
            remote_physical_node_id=session.remote_identity_id,
            observed_host=session.remote_host,
            observed_port=session.remote_port,
            observed_transport=session.transport,
            advertised_transport=endpoint.transport,
            advertised_host=endpoint.host,
            advertised_port=endpoint.port,
            advertised_metadata=endpoint.metadata_json,
        )
        return TransportEndpoint(
            transport_name=endpoint.transport,
            host=endpoint.host,
            port=endpoint.port,
            metadata=load_json_object(endpoint.metadata_json),
        )

    @staticmethod
    def _is_observed_only_physical_session(session) -> bool:
        if session.session_scope != "physical":
            return False
        return load_json_object(session.metadata_json).get("physical_endpoint_source") == "observed"

    def _validate_message_policy(
        self,
        envelope: ProtocolEnvelope,
    ) -> PacketProcessingResult | None:
        definition = self.message_router.get_definition(envelope.message_type)
        if definition is None:
            return None

        if definition.requires_physical_session:
            session_id = envelope.header.get("physical_session_id")
            if not isinstance(session_id, str) or not session_id:
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=False,
                    message_type=envelope.message_type,
                    metadata={
                        "reason": "physical_session_required",
                        "layer": definition.layer,
                    },
                )

            session = self.services.session_manager.get_session_by_session_id(session_id)
            if session is None:
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=False,
                    message_type=envelope.message_type,
                    metadata={
                        "reason": "physical_session_not_found",
                        "layer": definition.layer,
                    },
                )

            if session.session_state != "active":
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=False,
                    message_type=envelope.message_type,
                    metadata={
                        "reason": "physical_session_not_active",
                        "layer": definition.layer,
                    },
                )

        return None

    def _decode_json_packet(self, context: PacketContext) -> ProtocolEnvelope:
        packet = self._load_json_packet(context.payload)
        if packet is None:
            raise ValueError("The received payload does not contain a valid JSON packet.")

        if not isinstance(packet, dict):
            raise ValueError("The JSON packet must be an object.")

        header = packet.get("header")
        if not isinstance(header, dict):
            raise ValueError("The 'header' field must be a JSON object.")

        payload = packet.get("payload")
        if payload is None:
            payload = {}

        message_type = header.get("message_type")
        if message_type is not None and not isinstance(message_type, str):
            raise ValueError("The 'header.message_type' field must be a string.")

        payload = self._restore_inbound_payload(header, payload)
        return ProtocolEnvelope(
            protocol_name="json",
            message_type=message_type,
            payload=payload,
            raw_payload=context.payload,
            header=header,
        )

    def _restore_inbound_payload(
        self,
        header: dict[str, object],
        payload: object,
    ) -> object:
        payload_encrypted = header.get("payload_encrypted")
        if payload_encrypted is not True:
            return payload

        session_id = header.get("physical_session_id")
        message_type = header.get("message_type")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("Encrypted packet without physical_session_id.")

        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            self.services.log_service.warning(
                "engine",
                "cannot decrypt inbound physical packet without active session",
                session_id=session_id,
                message_type=message_type,
                session_exists=session is not None,
                session_state=getattr(session, "session_state", None),
                has_shared_secret=bool(getattr(session, "shared_secret_hex", None)),
            )
            raise ValueError("Encrypted packet received for an inactive or unknown physical session.")

        if not isinstance(payload, dict):
            raise ValueError("The encrypted payload must be a JSON object.")

        ciphertext_hex = payload.get("ciphertext_hex")
        if not isinstance(ciphertext_hex, str) or not ciphertext_hex:
            raise ValueError("The encrypted payload does not contain a valid ciphertext_hex.")

        try:
            plaintext_json = bytes.fromhex(
                aes_decrypt_hex(
                    ciphertext_hex,
                    session.shared_secret_hex,
                    aad=self._build_physical_payload_aad(header),
                )
            ).decode("utf-8")
        except Exception as error:
            self.services.log_service.warning(
                "engine",
                "failed to decrypt inbound physical packet",
                session_id=session_id,
                message_type=message_type,
                remote_physical_node_id=session.remote_identity_id,
                transport=session.transport,
                remote_host=session.remote_host,
                remote_port=session.remote_port,
                metadata_json=session.metadata_json,
                error_type=type(error).__name__,
                error=repr(error),
            )
            raise
        restored_payload = json.loads(plaintext_json)
        self.services.session_manager.touch_session(session_id)
        return restored_payload

    @staticmethod
    def _encode_payload_hex(payload: dict[str, object]) -> str:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8").hex()

    @staticmethod
    def _build_physical_payload_aad(header: dict[str, object]) -> bytes:
        protected_header = {
            key: value
            for key, value in header.items()
            if key != "payload_encrypted"
        }
        return json.dumps(
            {
                "scope": "physical_payload",
                "header": protected_header,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def _build_json_packet_bytes(
        *,
        header: dict[str, object],
        payload: dict[str, object],
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": payload,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def _load_json_packet(raw_payload: bytes) -> object | None:
        try:
            return json.loads(raw_payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
