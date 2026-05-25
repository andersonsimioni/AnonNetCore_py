from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from crypto import sha512_hex
from dht import DpntRecordPayload, DrtRecordPayload, DrtRouteEntryRecord, parse_record
from ..helpers import (
    close_failed_handshake_session,
    is_expired_iso_datetime,
    verify_dilithium_payload_signature,
)


class VirtualSessionClient:
    """Estabelece e mantem sessoes virtuais sobre uma rota ja criada."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._drt_lookup_timeout_seconds = (
            self.engine.services.config.virtual_session_drt_lookup_timeout_seconds
        )
        self._drt_lookup_retry_seconds = (
            self.engine.services.config.virtual_session_drt_lookup_retry_seconds
        )
        self._handshake_timeout_seconds = (
            self.engine.services.config.physical_session_handshake_timeout_seconds
        )
        self._handshake_poll_interval_seconds = (
            self.engine.services.config.physical_session_handshake_poll_interval_seconds
        )

    async def start_session_to_virtual_node(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
    ) -> str:
        local_virtual_node = self.engine.services.identity_service.get_local_virtual_node_by_id(
            local_virtual_node_id
        )
        if local_virtual_node is None:
            raise ValueError("O virtual node local informado nao existe.")

        remote_is_local = (
            self.engine.services.identity_service.get_local_virtual_node_by_id(
                remote_virtual_node_id
            )
            is not None
        )
        self.engine.services.log_service.info(
            "virtual_session_client",
            "starting virtual session via drt route resolution",
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
            remote_is_local=remote_is_local,
        )
        remote_virtual_node = self.engine.services.identity_service.get_remote_virtual_node_by_id(
            remote_virtual_node_id
        )
        entry_point = await self._resolve_entry_point(remote_virtual_node_id)
        await self._ensure_entry_point_physical_node(entry_point)
        return await self._start_session_over_entry_point(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
            remote_public_key=remote_virtual_node.public_key if remote_virtual_node else None,
            entry_point=entry_point,
        )

    async def start_session(
        self,
        *,
        local_route_path_id: str,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
    ) -> str:
        existing_session = self._get_active_virtual_session(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
        )
        if existing_session is not None:
            self.engine.services.log_service.debug(
                "virtual_session_client",
                "reusing active virtual session for local route",
                session_id=existing_session.session_id,
                local_virtual_node_id=local_virtual_node_id,
                remote_virtual_node_id=remote_virtual_node_id,
                existing_bound_route_id=existing_session.bound_route_id,
                requested_bound_route_id=local_route_path_id,
            )
            return existing_session.session_id

        local_virtual_node = self.engine.services.identity_service.get_local_virtual_node_by_id(
            local_virtual_node_id
        )
        if local_virtual_node is None:
            raise ValueError("O virtual node local informado nao existe.")

        remote_virtual_node = self.engine.services.identity_service.get_remote_virtual_node_by_id(
            remote_virtual_node_id
        )

        keepalive_seconds = self.engine.services.config.physical_session_keepalive_seconds
        session = self.engine.services.session_manager.create_outbound_virtual_session(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
            remote_public_key=remote_virtual_node.public_key if remote_virtual_node else None,
            bound_route_id=local_route_path_id,
            keepalive_interval_seconds=keepalive_seconds,
        )

        try:
            await self._send_virtual_envelope(
                session=session,
                message_type="VIRTUAL_SESSION_INIT",
                payload={
                    "initiator_virtual_node_id": local_virtual_node_id,
                    "initiator_virtual_node_public_key": local_virtual_node.public_key,
                    "target_virtual_node_id": remote_virtual_node_id,
                    "keepalive_interval_seconds": keepalive_seconds,
                },
                virtual_envelope_ciphered=False,
            )
            if await self._wait_for_activation(session.session_id):
                return session.session_id
        except Exception:
            close_failed_handshake_session(self.engine, session.session_id)
            raise

        close_failed_handshake_session(self.engine, session.session_id)
        raise RuntimeError("Nao foi possivel estabelecer a virtual session pela rota informada.")

    async def _start_session_over_entry_point(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
        remote_public_key: str | None,
        entry_point: "VirtualRouteEntryPoint",
    ) -> str:
        existing_session = self._get_active_virtual_session(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
        )
        if existing_session is not None:
            self.engine.services.log_service.debug(
                "virtual_session_client",
                "reusing active virtual session for drt entry point",
                session_id=existing_session.session_id,
                local_virtual_node_id=local_virtual_node_id,
                remote_virtual_node_id=remote_virtual_node_id,
                existing_bound_route_id=existing_session.bound_route_id,
                requested_bound_route_id=entry_point.final_path_id,
                entry_point_physical_node_id=entry_point.physical_node_id,
            )
            return existing_session.session_id

        local_virtual_node = self.engine.services.identity_service.get_local_virtual_node_by_id(
            local_virtual_node_id
        )
        if local_virtual_node is None:
            raise ValueError("O virtual node local informado nao existe.")

        keepalive_seconds = self.engine.services.config.physical_session_keepalive_seconds
        session = self.engine.services.session_manager.create_outbound_virtual_session(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
            remote_public_key=remote_public_key,
            bound_route_id=entry_point.final_path_id,
            keepalive_interval_seconds=keepalive_seconds,
        )
        session.metadata_json = json.dumps(
            {
                "route_source": "drt",
                "entry_point_physical_node_id": entry_point.physical_node_id,
                "entry_point_public_key": entry_point.physical_node_public_key,
                "entry_point_rtt": entry_point.rtt,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        try:
            await self._send_virtual_envelope(
                session=session,
                message_type="VIRTUAL_SESSION_INIT",
                payload={
                    "initiator_virtual_node_id": local_virtual_node_id,
                    "initiator_virtual_node_public_key": local_virtual_node.public_key,
                    "target_virtual_node_id": remote_virtual_node_id,
                    "keepalive_interval_seconds": keepalive_seconds,
                },
                virtual_envelope_ciphered=False,
            )
            if await self._wait_for_activation(session.session_id):
                self.engine.services.log_service.info(
                    "virtual_session_client",
                    "virtual session activated via drt",
                    session_id=session.session_id,
                    local_virtual_node_id=local_virtual_node_id,
                    remote_virtual_node_id=remote_virtual_node_id,
                    bound_route_id=session.bound_route_id,
                    entry_point_physical_node_id=entry_point.physical_node_id,
                )
                return session.session_id
        except Exception as error:
            self.engine.services.log_service.warning(
                "virtual_session_client",
                "virtual session init via drt failed while sending",
                session_id=session.session_id,
                local_virtual_node_id=local_virtual_node_id,
                remote_virtual_node_id=remote_virtual_node_id,
                bound_route_id=session.bound_route_id,
                entry_point_physical_node_id=entry_point.physical_node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            close_failed_handshake_session(self.engine, session.session_id)
            raise

        self.engine.services.log_service.warning(
            "virtual_session_client",
            "virtual session init via drt timed out waiting activation",
            session_id=session.session_id,
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
            bound_route_id=session.bound_route_id,
            entry_point_physical_node_id=entry_point.physical_node_id,
            timeout_seconds=self._handshake_timeout_seconds,
        )
        close_failed_handshake_session(self.engine, session.session_id)
        raise RuntimeError("Nao foi possivel estabelecer a virtual session via DRT.")

    async def send_keepalive(
        self,
        *,
        session_id: str,
    ) -> None:
        session = self._require_active_session(session_id)
        await self._send_virtual_envelope(
            session=session,
            message_type="VIRTUAL_SESSION_KEEPALIVE",
            payload={},
            virtual_envelope_ciphered=True,
        )
        self.engine.services.session_manager.mark_keepalive_sent(session.session_id)
        self.engine.services.log_service.info(
            "virtual_session_client",
            "sent virtual session keepalive",
            session_id=session.session_id,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            bound_route_id=session.bound_route_id,
        )

    async def send_message(
        self,
        *,
        session_id: str,
        app_message_type: str,
        payload: dict[str, object] | None = None,
        request_id: str | None = None,
    ) -> str:
        if not app_message_type:
            raise ValueError("app_message_type nao pode ser vazio.")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload precisa ser um objeto.")

        session = self._require_active_session(session_id)
        message_request_id = request_id or str(uuid4())
        await self._send_virtual_envelope(
            session=session,
            message_type="VIRTUAL_SESSION_DATA",
            payload={
                "app_message_type": app_message_type,
                "request_id": message_request_id,
                "payload": payload or {},
            },
            virtual_envelope_ciphered=True,
        )
        self.engine.services.log_service.info(
            "virtual_session_client",
            "sent virtual session data",
            session_id=session.session_id,
            app_message_type=app_message_type,
            request_id=message_request_id,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            bound_route_id=session.bound_route_id,
        )
        return message_request_id

    async def send_protocol_message(
        self,
        *,
        session_id: str,
        message_type: str,
        payload: dict[str, object] | None = None,
        virtual_envelope_ciphered: bool = True,
    ) -> None:
        if not message_type:
            raise ValueError("message_type nao pode ser vazio.")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload precisa ser um objeto.")

        session = self._require_active_session(session_id)
        await self._send_virtual_envelope(
            session=session,
            message_type=message_type,
            payload=payload or {},
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )
        self.engine.services.log_service.info(
            "virtual_session_client",
            "sent virtual protocol message",
            session_id=session.session_id,
            message_type=message_type,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            bound_route_id=session.bound_route_id,
        )

    async def close_session(
        self,
        *,
        session_id: str,
        close_reason: str = "local_closed",
    ) -> None:
        session = self._require_session(session_id)
        await self._send_virtual_envelope(
            session=session,
            message_type="VIRTUAL_SESSION_CLOSE",
            payload={
                "close_reason": close_reason,
            },
            virtual_envelope_ciphered=True,
        )

    async def _send_virtual_envelope(
        self,
        *,
        session,
        message_type: str,
        payload: dict[str, object],
        virtual_envelope_ciphered: bool,
    ) -> None:
        if not session.bound_route_id:
            raise ValueError("A virtual session nao possui rota local associada.")

        virtual_envelope = {
            "header": self.engine.build_message_header(
                message_type=message_type,
                virtual_session_id=session.session_id,
            ),
            "payload": payload,
        }
        entry_point_physical_node_id = self._read_session_entry_point_physical_node_id(session)
        if entry_point_physical_node_id:
            self.engine.services.log_service.debug(
                "virtual_session_client",
                "sending virtual envelope through drt entry point",
                session_id=session.session_id,
                message_type=message_type,
                bound_route_id=session.bound_route_id,
                entry_point_physical_node_id=entry_point_physical_node_id,
                virtual_envelope_ciphered=virtual_envelope_ciphered,
            )
            await self.engine.services.protocol_clients.physical.route_execute.send_to_entry_point(
                entry_point_physical_node_id=entry_point_physical_node_id,
                route_path_id=session.bound_route_id,
                virtual_session_id=session.session_id,
                virtual_envelope=virtual_envelope,
                virtual_envelope_ciphered=virtual_envelope_ciphered,
            )
            return

        self.engine.services.log_service.debug(
            "virtual_session_client",
            "sending virtual envelope through local route",
            session_id=session.session_id,
            message_type=message_type,
            bound_route_id=session.bound_route_id,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )
        await self.engine.services.protocol_clients.physical.route_execute.send_from_local_route(
            local_route_path_id=session.bound_route_id,
            virtual_session_id=session.session_id,
            virtual_envelope=virtual_envelope,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )

    async def _wait_for_activation(self, session_id: str) -> bool:
        deadline = asyncio.get_running_loop().time() + self._handshake_timeout_seconds

        while asyncio.get_running_loop().time() < deadline:
            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is None:
                return False
            if session.session_state == "active":
                return True
            if session.session_state == "closed":
                return False

            await asyncio.sleep(self._handshake_poll_interval_seconds)

        return False

    def _require_active_session(self, session_id: str):
        session = self._require_session(session_id)
        if session.session_state != "active":
            raise ValueError("A virtual session informada nao esta ativa.")
        return session

    def _require_session(self, session_id: str):
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_scope != "virtual":
            raise ValueError("A virtual session informada nao existe em memoria.")
        return session

    async def _resolve_entry_point(
        self,
        remote_virtual_node_id: str,
    ) -> "VirtualRouteEntryPoint":
        deadline = asyncio.get_running_loop().time() + self._drt_lookup_timeout_seconds
        last_error = "not_found"

        while asyncio.get_running_loop().time() < deadline:
            result = await self.engine.services.protocol_clients.physical.dht.query(
                namespace="drt",
                logical_key=remote_virtual_node_id,
            )
            if result.get("status") != "found":
                last_error = str(result.get("status") or "not_found")
                await asyncio.sleep(self._drt_lookup_retry_seconds)
                continue

            try:
                entry_point = self._select_entry_point_from_drt_result(
                    result,
                    remote_virtual_node_id,
                )
            except ValueError as error:
                last_error = str(error)
                await asyncio.sleep(self._drt_lookup_retry_seconds)
                continue

            self.engine.services.log_service.info(
                "virtual_session_client",
                "selected drt entry point",
                entry_point_physical_node_id=entry_point.physical_node_id,
                final_path_id=entry_point.final_path_id,
                rtt=entry_point.rtt,
            )
            return entry_point

        raise RuntimeError(
            "Nenhuma rota DRT valida foi encontrada para o virtual node remoto "
            f"dentro de {self._drt_lookup_timeout_seconds:.1f}s. Ultimo estado: {last_error}."
        )

    def _get_active_virtual_session(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
    ):
        session_manager = self.engine.services.session_manager
        if hasattr(session_manager, "get_active_virtual_session_by_local_and_remote_node_id"):
            return session_manager.get_active_virtual_session_by_local_and_remote_node_id(
                local_virtual_node_id=local_virtual_node_id,
                remote_virtual_node_id=remote_virtual_node_id,
            )
        return session_manager.get_active_session_by_remote_virtual_node_id(remote_virtual_node_id)

    def _select_entry_point_from_drt_result(
        self,
        result: dict[str, object],
        remote_virtual_node_id: str,
    ) -> "VirtualRouteEntryPoint":
        record_json = result.get("record_json")
        if not isinstance(record_json, str) or not record_json:
            raise ValueError("drt_record_json_invalid")

        record = parse_record("drt", record_json)
        if not isinstance(record, DrtRecordPayload):
            raise ValueError("drt_payload_invalid")
        if sha512_hex(record.pk_virtual_node.encode("utf-8")) != remote_virtual_node_id:
            raise ValueError("drt_record_belongs_to_another_virtual_node")

        valid_entries = [
            self._build_valid_entry_point(record.pk_virtual_node, entry)
            for entry in record.route_entries
        ]
        valid_entries = [entry for entry in valid_entries if entry is not None]
        if not valid_entries:
            raise ValueError("drt_has_no_valid_entry_point")

        valid_entries.sort(
            key=lambda entry: (
                _expires_at_sort_key(entry.expires_at),
                -entry.rtt,
            ),
            reverse=True,
        )
        return valid_entries[0]

    async def _ensure_entry_point_physical_node(
        self,
        entry_point: "VirtualRouteEntryPoint",
    ) -> None:
        existing_node = self.engine.services.identity_service.get_remote_physical_node_by_id(
            entry_point.physical_node_id
        )
        if existing_node is not None:
            endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
                entry_point.physical_node_id
            )
            if endpoints:
                self.engine.services.log_service.debug(
                    "virtual_session_client",
                    "entry point already known locally",
                    entry_point_physical_node_id=entry_point.physical_node_id,
                    endpoint_count=len(endpoints),
                )
                return

        result = await self.engine.services.protocol_clients.physical.dht.query(
            namespace="dpnt",
            logical_key=entry_point.physical_node_id,
        )
        if result.get("status") != "found":
            raise RuntimeError("Nao foi possivel resolver o DPNT do entry point fisico.")

        record_json = result.get("record_json")
        if not isinstance(record_json, str) or not record_json:
            raise RuntimeError("A resposta DPNT nao contem record_json valido.")

        record = parse_record("dpnt", record_json)
        if not isinstance(record, DpntRecordPayload):
            raise RuntimeError("A resposta DPNT nao contem um payload DPNT valido.")
        if record.pk_physical_node != entry_point.physical_node_public_key:
            raise RuntimeError("O DPNT encontrado nao pertence ao entry point da DRT.")
        if not self._is_valid_dpnt_record(entry_point.physical_node_id, record):
            raise RuntimeError("O DPNT do entry point possui assinatura invalida.")

        self.engine.services.identity_service.upsert_discovered_remote_physical_node(
            node_id=entry_point.physical_node_id,
            public_key=record.pk_physical_node,
            protocol_version=record.protocol_version,
            endpoints=record.endpoints,
            status=record.status,
            reachability_class=record.reachability_class,
            relay_capable=record.relay_capable,
            hole_punch_capable=record.hole_punch_capable,
            notes_json=json.dumps(
                {
                    "source": "drt_entry_point_dpnt",
                    "dpnt_signature": record.signature,
                    "dpnt_feature_flags": record.feature_flags,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        self.engine.services.log_service.info(
            "virtual_session_client",
            "entry point resolved from dpnt",
            entry_point_physical_node_id=entry_point.physical_node_id,
            endpoint_count=len(record.endpoints),
        )

    def _build_valid_entry_point(
        self,
        virtual_node_public_key: str,
        route_entry: DrtRouteEntryRecord,
    ) -> "VirtualRouteEntryPoint | None":
        if not self._is_valid_drt_route_entry(route_entry, virtual_node_public_key):
            return None

        return VirtualRouteEntryPoint(
            physical_node_id=sha512_hex(route_entry.pk_physical_node.encode("utf-8")),
            physical_node_public_key=route_entry.pk_physical_node,
            final_path_id=route_entry.final_path_id,
            rtt=route_entry.rtt,
            expires_at=route_entry.expires_at,
        )

    def _is_valid_drt_route_entry(
        self,
        route_entry: DrtRouteEntryRecord,
        virtual_node_public_key: str,
    ) -> bool:
        if is_expired_iso_datetime(route_entry.expires_at):
            return False

        physical_node_id = sha512_hex(route_entry.pk_physical_node.encode("utf-8"))
        virtual_node_id = sha512_hex(virtual_node_public_key.encode("utf-8"))
        virtual_payload = {
            "final_path_id": route_entry.final_path_id,
            "final_physical_node_id": physical_node_id,
        }
        physical_payload = {
            "virtual_node_id": virtual_node_id,
            "final_path_id": route_entry.final_path_id,
            "virtual_node_signature": route_entry.virtual_node_signature,
        }
        rtt_payload = {
            "pk_physical_node": route_entry.pk_physical_node,
            "expires_at": route_entry.expires_at,
            "rtt": route_entry.rtt,
        }

        return (
            route_entry.virtual_node_signature == route_entry.entry_point_virtual_node_signature
            and verify_dilithium_payload_signature(
                virtual_payload,
                route_entry.virtual_node_signature,
                virtual_node_public_key,
            )
            and verify_dilithium_payload_signature(
                physical_payload,
                route_entry.physical_node_signature,
                route_entry.pk_physical_node,
            )
            and verify_dilithium_payload_signature(
                physical_payload,
                route_entry.entry_point_physical_node_signature,
                route_entry.pk_physical_node,
            )
            and verify_dilithium_payload_signature(
                rtt_payload,
                route_entry.rtt_physical_node_signature,
                route_entry.pk_physical_node,
            )
        )

    def _is_valid_dpnt_record(
        self,
        physical_node_id: str,
        record: DpntRecordPayload,
    ) -> bool:
        key_hex = self.engine.services.dht_service.build_key("dpnt", physical_node_id)
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

    def _read_session_entry_point_physical_node_id(self, session) -> str | None:
        if not session.metadata_json:
            return None

        try:
            metadata = json.loads(session.metadata_json)
        except json.JSONDecodeError:
            return None

        if not isinstance(metadata, dict):
            return None

        entry_point_physical_node_id = metadata.get("entry_point_physical_node_id")
        if isinstance(entry_point_physical_node_id, str) and entry_point_physical_node_id:
            return entry_point_physical_node_id
        return None

@dataclass(slots=True, frozen=True)
class VirtualRouteEntryPoint:
    physical_node_id: str
    physical_node_public_key: str
    final_path_id: str
    rtt: int
    expires_at: str


def _expires_at_sort_key(expires_at: str) -> float:
    try:
        parsed = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError:
        return 0.0

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()
