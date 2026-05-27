from __future__ import annotations

import asyncio
import json

from crypto import sha512_hex
from dht import DpntRecordPayload, parse_record
from sessions import (
    SessionStateUpdateInput,
    build_remote_endpoint_from_session,
    is_observed_only_physical_session,
)
from transport import (
    OutboundMessage,
    TransportEndpoint,
    build_transport_endpoint_from_result,
    normalize_endpoint_list,
)

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
            if not self.engine.services.transport.has_adapter(endpoint_data.transport):
                self.engine.services.log_service.debug(
                    "physical_session_client",
                    "skipping physical session endpoint without transport adapter",
                    remote_physical_node_id=remote_physical_node_id,
                    transport=endpoint_data.transport,
                    host=endpoint_data.host,
                    port=endpoint_data.port,
                )
                continue
            endpoint = build_transport_endpoint_from_result(endpoint_data)
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
        return normalize_endpoint_list(endpoints)

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

    async def send_reliable_protocol_message(
        self,
        *,
        session_id: str,
        inner_message_type: str,
        inner_payload: dict[str, object],
    ) -> str:
        if not inner_message_type:
            raise ValueError("inner_message_type nao pode ser vazio.")
        if not isinstance(inner_payload, dict):
            raise ValueError("inner_payload precisa ser um objeto.")

        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_scope != "physical":
            raise ValueError("A physical session informada nao existe em memoria.")
        if session.session_state != "active":
            raise ValueError("A physical session informada nao esta ativa.")

        reliable_message = self.engine.services.session_manager.prepare_reliable_outbound(
            session_id=session.session_id,
            inner_message_type=inner_message_type,
            inner_payload=inner_payload,
            retry_after_seconds=self.engine.services.config.physical_session_reliable_retry_after_seconds,
            max_attempts=self.engine.services.config.session_reliable_max_attempts,
        )
        await self.resend_reliable_message(reliable_message)
        self.engine.services.log_service.info(
            "physical_session_client",
            "sent reliable physical protocol message",
            session_id=session.session_id,
            reliable_message_id=reliable_message.reliable_message_id,
            sequence_number=reliable_message.sequence_number,
            inner_message_type=inner_message_type,
            remote_physical_node_id=session.remote_identity_id,
        )
        return reliable_message.reliable_message_id

    async def resend_reliable_message(self, reliable_message) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(
            reliable_message.session_id
        )
        if session is None or session.session_scope != "physical":
            raise ValueError("A physical session informada nao existe em memoria.")
        if session.session_state != "active":
            raise ValueError("A physical session informada nao esta ativa.")

        endpoint = self._build_remote_endpoint(session)
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_RELIABLE_DATA",
            physical_session_id=session.session_id,
        )
        payload = _build_packet_bytes(
            header=header,
            payload=reliable_message.to_reliable_payload(),
        )
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        marked = self.engine.services.session_manager.mark_reliable_outbound_sent(
            session_id=reliable_message.session_id,
            sequence_number=reliable_message.sequence_number,
        )
        self.engine.services.log_service.debug(
            "physical_session_client",
            "sent physical reliable envelope",
            session_id=session.session_id,
            reliable_message_id=reliable_message.reliable_message_id,
            sequence_number=reliable_message.sequence_number,
            attempts=marked.attempts if marked else reliable_message.attempts,
            inner_message_type=reliable_message.inner_message_type,
            remote_physical_node_id=session.remote_identity_id,
            pending_count=self.engine.services.session_manager.count_pending_reliable_outbound(
                session.session_id
            ),
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
        session_metadata = _build_session_transport_metadata(
            target_physical_node_id=remote_physical_node_id,
            endpoint_metadata=endpoint.metadata,
        )
        self.engine.services.session_manager.update_session_state(
            session.session_id,
            SessionStateUpdateInput(metadata_json=json.dumps(session_metadata, separators=(",", ":"))),
        )
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_INIT",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_init_payload(
            header=header,
            initiator_physical_node_id=local_physical_node_id,
            initiator_public_key=local_public_key,
            initiator_endpoints=self.engine.build_local_physical_endpoints(),
            keepalive_interval_seconds=keepalive_interval_seconds,
        )

        try:
            await self.engine.send_packet(
                OutboundMessage(
                    transport_name=endpoint.transport_name,
                    payload=payload,
                    remote_endpoint=endpoint,
                    metadata=session_metadata,
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

    @staticmethod
    def _build_remote_endpoint(session) -> TransportEndpoint:
        return build_remote_endpoint_from_session(session)


def _build_packet_bytes(
    *,
    header: dict[str, object],
    payload: dict[str, object],
) -> bytes:
    return json.dumps(
        {"header": header, "payload": payload},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _build_session_transport_metadata(
    *,
    target_physical_node_id: str,
    endpoint_metadata: dict[str, object],
) -> dict[str, object]:
    metadata = dict(endpoint_metadata)
    metadata["target_physical_node_id"] = target_physical_node_id
    return metadata
