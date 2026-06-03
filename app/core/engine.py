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
    TcpTransportConfig,
    TransportEndpoint,
    TransportPacket,
    TransportService,
    UdpTransportAdapter,
    UdpTransportConfig,
)


def _should_force_new_forward_connection(message_type: str) -> bool:
    return message_type.startswith("ROUTE_CREATE")


def _read_metadata_string(metadata: dict[str, object], field_name: str) -> str | None:
    value = metadata.get(field_name)
    return value if isinstance(value, str) and value else None


def _can_forward_through_observed_only_session(message_type: str) -> bool:
    return message_type in {"PHYSICAL_RELAY_DATA", "PHYSICAL_RELAY_CLOSE"}


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
            self.services.transport.register_adapter(relay_adapter)
            return

        if isinstance(adapter, RelayTcpTransportAdapter):
            adapter.set_relay_sender(self._send_relay_transport_packet)

    async def _send_relay_transport_packet(self, message: OutboundMessage) -> None:
        if self.services.protocol_clients is None:
            raise RuntimeError("Protocol clients are not initialized.")

        target_physical_node_id = _read_metadata_string(message.metadata, "target_physical_node_id")
        relay_physical_node_id = _read_metadata_string(message.metadata, "relay_physical_node_id")
        if target_physical_node_id is None:
            raise ValueError("relay_tcp message requires target_physical_node_id metadata.")
        if relay_physical_node_id is None:
            relay_physical_node_id = self._resolve_relay_node_id_from_endpoint(message.remote_endpoint)
        if relay_physical_node_id is None:
            raise ValueError("relay_tcp message requires relay_physical_node_id metadata.")

        await self.services.protocol_clients.physical.relay.send_transport_packet(
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=target_physical_node_id,
            payload=message.payload,
            relay_endpoint=message.remote_endpoint,
            relay_channel_id=_read_metadata_string(message.metadata, "relay_channel_id"),
        )

    def _resolve_relay_node_id_from_endpoint(self, endpoint: TransportEndpoint) -> str | None:
        return self.services.identity_service.find_remote_physical_node_id_by_endpoint(
            transport="tcp",
            host=endpoint.host,
            port=endpoint.port,
        )

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
                await self.send_packet(
                    OutboundMessage(
                        transport_name=packet.transport_name,
                        payload=result.response_payload,
                        remote_endpoint=packet.remote_endpoint,
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
            cors_allow_origin="*",
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
                path="/v1/events",
            )
            await self._api_websocket_server.start()
            self.services.log_service.info(
                "core_api",
                "websocket api server started",
                host=config.api_host,
                port=config.api_websocket_port,
                path="/v1/events",
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
                TcpTransportConfig(
                    listen_host=config.physical_listen_host,
                    listen_port=config.physical_tcp_listen_port,
                    listen_enabled=config.tcp_transport_enabled and not self.is_private_physical_node(),
                )
            )
        )
        if config.udp_transport_enabled:
            transport_service.register_adapter(
                UdpTransportAdapter(
                    UdpTransportConfig(
                        listen_host=config.physical_listen_host,
                        listen_port=self.get_configured_udp_listen_port(),
                        listen_enabled=not self.is_private_physical_node(),
                        max_datagram_size=config.udp_max_datagram_size,
                        chunk_payload_size=config.udp_chunk_payload_size,
                        max_frame_size=config.udp_max_frame_size,
                        reassembly_timeout_seconds=config.udp_reassembly_timeout_seconds,
                    )
                )
            )
        transport_service.register_adapter(
            RelayTcpTransportAdapter(self._send_relay_transport_packet)
        )
        transport_service.set_inbound_packet_handler(self.handle_transport_packet)
        self.services.transport = transport_service

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

    async def _request_bootstrap_physical_node_info(self) -> None:
        bootstrap_endpoints = self._get_bootstrap_endpoints()
        if not bootstrap_endpoints:
            return

        for attempt in range(1, self.services.config.bootstrap_request_retries + 1):
            for endpoint in bootstrap_endpoints:
                await self._request_single_bootstrap_endpoint(endpoint, attempt)

            if attempt < self.services.config.bootstrap_request_retries:
                await asyncio.sleep(self.services.config.bootstrap_request_delay_seconds)

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

        local_port = getattr(tcp_adapter.config, "listen_port", None)
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
            config.relay_service_enabled
            and not self.is_private_physical_node()
            and config.tcp_transport_enabled
        )

    def build_local_physical_endpoints(self, transport_name: str | None = None) -> list[dict[str, object]]:
        if self.is_private_physical_node():
            return []

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
                )

        if not isinstance(remote_physical_node_id, str) or not remote_physical_node_id:
            return

        await self.forward_message_to_remote_physical_node(
            remote_physical_node_id=remote_physical_node_id,
            message_type=message_type,
            payload=payload,
            force_new_connection=_should_force_new_forward_connection(message_type),
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
        if session is None or session.session_state != "active":
            return

        endpoint = self._resolve_session_send_endpoint(session)
        if endpoint is None:
            return

        try:
            await self.send_packet(
                OutboundMessage(
                    transport_name=endpoint.transport_name,
                    payload=payload,
                    remote_endpoint=endpoint,
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
                error_type=type(error).__name__,
                error=repr(error),
            )

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
            raise RuntimeError("A physical session foi criada, mas nao esta disponivel em memoria.")
        return session

    @staticmethod
    def _build_session_remote_endpoint(session) -> TransportEndpoint:
        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("A physical session nao possui endpoint remoto associado.")

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
            advertised_host=endpoint.host,
            advertised_port=endpoint.port,
        )
        return TransportEndpoint(
            transport_name=endpoint.transport,
            host=endpoint.host,
            port=endpoint.port,
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
            raise ValueError("O payload recebido nao contem um pacote JSON valido.")

        if not isinstance(packet, dict):
            raise ValueError("O pacote JSON precisa ser um objeto.")

        header = packet.get("header")
        if not isinstance(header, dict):
            raise ValueError("O campo 'header' precisa ser um objeto JSON.")

        payload = packet.get("payload")
        if payload is None:
            payload = {}

        message_type = header.get("message_type")
        if message_type is not None and not isinstance(message_type, str):
            raise ValueError("O campo 'header.message_type' precisa ser uma string.")

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
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("Pacote cifrado sem physical_session_id.")

        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            raise ValueError("Pacote cifrado recebido para uma sessao fisica inativa ou desconhecida.")

        if not isinstance(payload, dict):
            raise ValueError("O payload cifrado precisa ser um objeto JSON.")

        ciphertext_hex = payload.get("ciphertext_hex")
        if not isinstance(ciphertext_hex, str) or not ciphertext_hex:
            raise ValueError("O payload cifrado nao contem ciphertext_hex valido.")

        plaintext_json = bytes.fromhex(
            aes_decrypt_hex(
                ciphertext_hex,
                session.shared_secret_hex,
                aad=self._build_physical_payload_aad(header),
            )
        ).decode("utf-8")
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
