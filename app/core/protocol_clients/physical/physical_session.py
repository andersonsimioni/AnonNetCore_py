from __future__ import annotations

import asyncio
import json

from crypto import sha512_hex
from dht import DpntRecordPayload, parse_record
from sessions import build_remote_endpoint_from_session, is_observed_only_physical_session
from transport import OutboundMessage, TransportEndpoint

from ...protocols import SessionProtocolHandler
from ..helpers import (
    close_failed_handshake_session,
    verify_dilithium_payload_signature,
)


class PhysicalSessionClient:
    """Inicia e mantem sessoes fisicas entre peers adjacentes."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._handshake_timeout_seconds = (
            self.engine.services.config.physical_session_handshake_timeout_seconds
        )
        self._handshake_poll_interval_seconds = (
            self.engine.services.config.physical_session_handshake_poll_interval_seconds
        )

    async def start_session(
        self,
        *,
        remote_physical_node_id: str,
    ) -> str:
        existing_session = self.engine.services.session_manager.get_active_physical_session_by_remote_node_id(
            remote_physical_node_id
        )
        if existing_session is not None and not is_observed_only_physical_session(existing_session):
            self.engine.services.log_service.debug(
                "physical_session_client",
                "reusing active physical session",
                session_id=existing_session.session_id,
                remote_physical_node_id=remote_physical_node_id,
            )
            return existing_session.session_id

        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        if local_node is None:
            raise ValueError("A identidade fisica local ainda nao foi inicializada.")

        remote_node = await self._load_remote_physical_node(remote_physical_node_id)
        if remote_node is None:
            raise ValueError("O physical node remoto nao foi encontrado localmente nem na DPNT.")

        endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
            remote_physical_node_id
        )
        if not endpoints:
            raise ValueError("O physical node remoto nao possui endpoints conhecidos.")

        self.engine.services.log_service.debug(
            "physical_session_client",
            "loaded candidate endpoints for physical session",
            remote_physical_node_id=remote_physical_node_id,
            endpoint_count=len(endpoints),
        )
        for endpoint_data in endpoints:
            endpoint = TransportEndpoint(
                transport_name=endpoint_data.transport,
                host=endpoint_data.host,
                port=endpoint_data.port,
            )
            self.engine.services.log_service.info(
                "physical_session_client",
                "trying to establish physical session",
                remote_physical_node_id=remote_physical_node_id,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
            )
            session_id = await self._start_session_with_endpoint(
                local_physical_node_id=local_node.id,
                local_public_key=local_node.public_key,
                remote_physical_node_id=remote_physical_node_id,
                remote_public_key=remote_node.public_key,
                endpoint=endpoint,
            )
            if session_id is None:
                continue

            if await self._wait_for_activation(session_id):
                self.engine.services.log_service.info(
                    "physical_session_client",
                    "physical session established",
                    session_id=session_id,
                    remote_physical_node_id=remote_physical_node_id,
                )
                return session_id

            self.engine.services.log_service.warning(
                "physical_session_client",
                "physical session endpoint did not activate",
                session_id=session_id,
                remote_physical_node_id=remote_physical_node_id,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
            )
            self._register_endpoint_failure(
                remote_physical_node_id=remote_physical_node_id,
                endpoint=endpoint,
            )
            close_failed_handshake_session(self.engine, session_id)

        raise RuntimeError("Nao foi possivel estabelecer physical session com nenhum endpoint conhecido.")

    async def _load_remote_physical_node(self, remote_physical_node_id: str):
        remote_node = self.engine.services.identity_service.get_remote_physical_node_by_id(
            remote_physical_node_id
        )
        if remote_node is not None:
            return remote_node

        self.engine.services.log_service.info(
            "physical_session_client",
            "remote physical node not found locally, querying dpnt",
            remote_physical_node_id=remote_physical_node_id,
        )
        return await self._resolve_remote_physical_node_from_dpnt(remote_physical_node_id)

    async def _resolve_remote_physical_node_from_dpnt(self, remote_physical_node_id: str):
        result = await self.engine.services.protocol_clients.physical.dht.query(
            namespace="dpnt",
            logical_key=remote_physical_node_id,
        )
        if result.get("status") != "found":
            self.engine.services.log_service.warning(
                "physical_session_client",
                "dpnt lookup did not find remote physical node",
                remote_physical_node_id=remote_physical_node_id,
                status=result.get("status"),
            )
            return None

        record = self._parse_valid_dpnt_record(
            remote_physical_node_id=remote_physical_node_id,
            record_json=result.get("record_json"),
        )
        if record is None:
            return None

        endpoints = self._normalize_dpnt_endpoints(record.endpoints)
        if not endpoints:
            self.engine.services.log_service.warning(
                "physical_session_client",
                "dpnt record has no usable endpoints",
                remote_physical_node_id=remote_physical_node_id,
            )
            return None

        self.engine.services.identity_service.upsert_discovered_remote_physical_node(
            node_id=remote_physical_node_id,
            public_key=record.pk_physical_node,
            protocol_version=record.protocol_version,
            endpoints=endpoints,
            status=record.status,
            reachability_class=record.reachability_class,
            relay_capable=record.relay_capable,
            hole_punch_capable=record.hole_punch_capable,
            notes_json=json.dumps(
                {
                    "source": "physical_session_dpnt_lookup",
                    "dpnt_signature": record.signature,
                    "dpnt_feature_flags": record.feature_flags,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        self.engine.services.log_service.info(
            "physical_session_client",
            "remote physical node resolved from dpnt",
            remote_physical_node_id=remote_physical_node_id,
            endpoint_count=len(endpoints),
        )
        return self.engine.services.identity_service.get_remote_physical_node_by_id(
            remote_physical_node_id
        )

    def _parse_valid_dpnt_record(
        self,
        *,
        remote_physical_node_id: str,
        record_json: object,
    ) -> DpntRecordPayload | None:
        if not isinstance(record_json, str) or not record_json:
            self.engine.services.log_service.warning(
                "physical_session_client",
                "dpnt lookup returned invalid record_json",
                remote_physical_node_id=remote_physical_node_id,
            )
            return None

        try:
            record = parse_record("dpnt", record_json)
        except ValueError as error:
            self.engine.services.log_service.warning(
                "physical_session_client",
                "dpnt lookup returned malformed record",
                remote_physical_node_id=remote_physical_node_id,
                error=str(error),
            )
            return None

        if not isinstance(record, DpntRecordPayload):
            return None

        if sha512_hex(record.pk_physical_node.encode("utf-8")) != remote_physical_node_id:
            self.engine.services.log_service.warning(
                "physical_session_client",
                "dpnt record public key does not match requested node id",
                remote_physical_node_id=remote_physical_node_id,
            )
            return None

        if not self._is_valid_dpnt_record(remote_physical_node_id, record):
            self.engine.services.log_service.warning(
                "physical_session_client",
                "dpnt record signature is invalid",
                remote_physical_node_id=remote_physical_node_id,
            )
            return None

        return record

    def _is_valid_dpnt_record(
        self,
        remote_physical_node_id: str,
        record: DpntRecordPayload,
    ) -> bool:
        key_hex = self.engine.services.dht_service.build_key("dpnt", remote_physical_node_id)
        payload = {
            "key": key_hex,
            "pk_physical_node": record.pk_physical_node,
            "endpoints": record.endpoints,
            "transport_methods": record.transport_methods,
            "reachability_class": record.reachability_class,
            "relay_capable": record.relay_capable,
            "hole_punch_capable": record.hole_punch_capable,
            "protocol_version": record.protocol_version,
            "feature_flags": record.feature_flags,
            "status": record.status,
        }
        return verify_dilithium_payload_signature(payload, record.signature, record.pk_physical_node)

    @staticmethod
    def _normalize_dpnt_endpoints(endpoints: list[dict[str, object]]) -> list[dict[str, object]]:
        valid_endpoints: list[dict[str, object]] = []
        for endpoint in endpoints:
            transport = endpoint.get("transport")
            host = endpoint.get("host")
            port = endpoint.get("port")
            priority = endpoint.get("priority", 0)
            if not isinstance(transport, str) or not transport:
                continue
            if not isinstance(host, str) or not host:
                continue
            if not isinstance(port, int):
                continue

            valid_endpoints.append(
                {
                    "transport": transport,
                    "host": host,
                    "port": port,
                    "priority": priority if isinstance(priority, int) else 0,
                }
            )
        return valid_endpoints

    async def send_keepalive(
        self,
        *,
        session_id: str,
    ) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("A physical session informada nao existe em memoria.")

        endpoint = self._build_remote_endpoint(session)
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_KEEPALIVE",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_keepalive_payload(header=header)
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        self.engine.services.session_manager.mark_keepalive_sent(session.session_id)
        self.engine.services.log_service.debug(
            "physical_session_client",
            "sent physical session keepalive",
            session_id=session.session_id,
        )

    async def close_session(
        self,
        *,
        session_id: str,
        close_reason: str = "local_closed",
    ) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("A physical session informada nao existe em memoria.")

        endpoint = self._build_remote_endpoint(session)
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_CLOSE",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_close_payload(
            header=header,
            close_reason=close_reason,
        )
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        self.engine.services.log_service.info(
            "physical_session_client",
            "sent physical session close",
            session_id=session.session_id,
            close_reason=close_reason,
        )

    async def _start_session_with_endpoint(
        self,
        *,
        local_physical_node_id: str,
        local_public_key: str,
        remote_physical_node_id: str,
        remote_public_key: str,
        endpoint: TransportEndpoint,
    ) -> str | None:
        keepalive_interval_seconds = self.engine.services.config.physical_session_keepalive_seconds
        session = self.engine.services.session_manager.create_outbound_physical_session(
            local_physical_node_id=local_physical_node_id,
            remote_physical_node_id=remote_physical_node_id,
            remote_public_key=remote_public_key,
            transport=endpoint.transport_name,
            remote_host=endpoint.host,
            remote_port=endpoint.port,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_INIT",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_init_payload(
            header=header,
            initiator_physical_node_id=local_physical_node_id,
            initiator_public_key=local_public_key,
            initiator_endpoints=self._build_local_advertised_endpoints(endpoint.transport_name),
            keepalive_interval_seconds=keepalive_interval_seconds,
        )

        try:
            await self.engine.send_packet(
                OutboundMessage(
                    transport_name=endpoint.transport_name,
                    payload=payload,
                    remote_endpoint=endpoint,
                )
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_session_client",
                "failed to send physical session init",
                remote_physical_node_id=remote_physical_node_id,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
                error_type=type(error).__name__,
                error=repr(error),
            )
            self._register_endpoint_failure(
                remote_physical_node_id=remote_physical_node_id,
                endpoint=endpoint,
            )
            close_failed_handshake_session(self.engine, session.session_id)
            return None

        self.engine.services.log_service.info(
            "physical_session_client",
            "sent physical session init",
            session_id=session.session_id,
            remote_physical_node_id=remote_physical_node_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
        )
        return session.session_id

    async def _wait_for_activation(self, session_id: str) -> bool:
        deadline = asyncio.get_running_loop().time() + self._handshake_timeout_seconds

        while asyncio.get_running_loop().time() < deadline:
            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is None:
                self.engine.services.log_service.warning(
                    "physical_session_client",
                    "session disappeared while waiting for activation",
                    session_id=session_id,
                )
                return False
            if session.session_state == "active":
                return True
            if session.session_state == "closed":
                self.engine.services.log_service.warning(
                    "physical_session_client",
                    "session closed before activation",
                    session_id=session_id,
                )
                return False

            await asyncio.sleep(self._handshake_poll_interval_seconds)

        self.engine.services.log_service.warning(
            "physical_session_client",
            "session activation timed out",
            session_id=session_id,
            timeout_seconds=self._handshake_timeout_seconds,
        )
        return False

    def _register_endpoint_failure(
        self,
        *,
        remote_physical_node_id: str,
        endpoint: TransportEndpoint,
    ) -> None:
        self.engine.services.identity_service.mark_remote_physical_node_validation_failure(
            node_id=remote_physical_node_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
        )
        self.engine.services.log_service.warning(
            "physical_session_client",
            "registered endpoint failure",
            remote_physical_node_id=remote_physical_node_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
        )

    def _build_local_advertised_endpoints(self, transport_name: str) -> list[dict[str, object]]:
        if transport_name != "tcp":
            return []

        return [
            {
                "transport": "tcp",
                "host": self.engine.get_advertised_tcp_host(),
                "port": self.engine.get_advertised_tcp_port(),
                "priority": 0,
            }
        ]

    @staticmethod
    def _build_remote_endpoint(session) -> TransportEndpoint:
        return build_remote_endpoint_from_session(session)
