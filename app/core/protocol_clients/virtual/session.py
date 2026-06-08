from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from common import load_json_object
from crypto import sha512_hex
from dht import DpntRecordPayload, DrtRecordPayload, DrtRouteEntryRecord, parse_record
from transport import canonical_endpoint_list
from ..helpers import (
    close_failed_handshake_session,
    is_expired_iso_datetime,
    verify_dilithium_payload_signature,
)


class VirtualSessionClient:
    """Establishes and maintains virtual sessions over an existing route."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._drt_lookup_timeout_seconds = (
            self.engine.services.config.virtual_session_drt_lookup_timeout_seconds
        )
        self._drt_lookup_retry_seconds = (
            self.engine.services.config.virtual_session_drt_lookup_retry_seconds
        )
        self._handshake_timeout_seconds = (
            self.engine.services.config.virtual_session_handshake_timeout_seconds
        )
        self._handshake_poll_interval_seconds = (
            self.engine.services.config.session_handshake_poll_interval_seconds
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
            raise ValueError("The provided local virtual node does not exist.")

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
        entry_points = await self._resolve_entry_points(remote_virtual_node_id)
        last_error: Exception | None = None

        for attempt, entry_point in enumerate(entry_points, start=1):
            try:
                await self._ensure_entry_point_physical_node(entry_point)
                timeout_seconds = self._entry_point_handshake_timeout_seconds(entry_point)
                self.engine.services.log_service.debug(
                    "virtual_session_client",
                    "calculated virtual session drt handshake timeout",
                    local_virtual_node_id=local_virtual_node_id,
                    remote_virtual_node_id=remote_virtual_node_id,
                    entry_point_physical_node_id=entry_point.physical_node_id,
                    final_path_id=entry_point.final_path_id,
                    route_rtt_ms=entry_point.rtt,
                    timeout_seconds=timeout_seconds,
                    handshake_timeout_seconds=self._handshake_timeout_seconds,
                    virtual_timeout_min_seconds=(
                        self.engine.services.config.virtual_session_timeout_min_seconds
                    ),
                    rtt_multiplier=(
                        self.engine.services.config.virtual_session_timeout_rtt_multiplier
                    ),
                )
                return await self._start_session_over_entry_point(
                    local_virtual_node_id=local_virtual_node_id,
                    remote_virtual_node_id=remote_virtual_node_id,
                    remote_public_key=remote_virtual_node.public_key if remote_virtual_node else None,
                    entry_point=entry_point,
                    timeout_seconds=timeout_seconds,
                    attempt=attempt,
                    max_attempts=len(entry_points),
                )
            except Exception as error:
                last_error = error
                self.engine.services.log_service.warning(
                    "virtual_session_client",
                    "virtual session drt entry point attempt failed",
                    local_virtual_node_id=local_virtual_node_id,
                    remote_virtual_node_id=remote_virtual_node_id,
                    attempt=attempt,
                    max_attempts=len(entry_points),
                    entry_point_physical_node_id=entry_point.physical_node_id,
                    final_path_id=entry_point.final_path_id,
                    rtt=entry_point.rtt,
                    error_type=type(error).__name__,
                    error=repr(error),
                )

        raise RuntimeError(
            "Could not establish the virtual session through any DRT entry point. "
            f"Last error: {last_error!r}"
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
            raise ValueError("The provided local virtual node does not exist.")

        remote_virtual_node = self.engine.services.identity_service.get_remote_virtual_node_by_id(
            remote_virtual_node_id
        )

        keepalive_seconds = self.engine.services.config.session_keepalive_seconds
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
        raise RuntimeError("Could not establish the virtual session over the provided route.")

    async def _start_session_over_entry_point(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
        remote_public_key: str | None,
        entry_point: "VirtualRouteEntryPoint",
        timeout_seconds: float | None = None,
        attempt: int = 1,
        max_attempts: int = 1,
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
            raise ValueError("The provided local virtual node does not exist.")

        keepalive_seconds = self.engine.services.config.session_keepalive_seconds
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
            if await self._wait_for_activation(
                session.session_id,
                timeout_seconds=timeout_seconds,
            ):
                self.engine.services.log_service.info(
                    "virtual_session_client",
                    "virtual session activated via drt",
                    session_id=session.session_id,
                    local_virtual_node_id=local_virtual_node_id,
                    remote_virtual_node_id=remote_virtual_node_id,
                    bound_route_id=session.bound_route_id,
                    entry_point_physical_node_id=entry_point.physical_node_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
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
            timeout_seconds=timeout_seconds or self._handshake_timeout_seconds,
            attempt=attempt,
            max_attempts=max_attempts,
        )
        close_failed_handshake_session(self.engine, session.session_id)
        raise RuntimeError("Could not establish the virtual session through DRT.")

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
            raise ValueError("app_message_type cannot be empty.")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload must be an object.")

        session = self._require_active_session(session_id)
        message_request_id = request_id or str(uuid4())
        reliable_message = self.engine.services.session_manager.prepare_reliable_outbound(
            session_id=session.session_id,
            inner_message_type="VIRTUAL_SESSION_DATA",
            inner_payload={
                "app_message_type": app_message_type,
                "request_id": message_request_id,
                "payload": payload or {},
            },
            retry_after_seconds=self.engine.services.config.virtual_reliable_retry_fallback_seconds,
            max_attempts=self.engine.services.config.reliable_delivery_max_attempts,
        )
        await self._send_reliable_outbound_message(reliable_message)
        self.engine.services.log_service.info(
            "virtual_session_client",
            "sent reliable virtual session data",
            session_id=session.session_id,
            app_message_type=app_message_type,
            request_id=message_request_id,
            reliable_message_id=reliable_message.reliable_message_id,
            sequence_number=reliable_message.sequence_number,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            bound_route_id=session.bound_route_id,
        )
        return message_request_id

    async def resend_reliable_message(
        self,
        reliable_message,
    ) -> None:
        await self._send_reliable_outbound_message(reliable_message)

    async def _send_reliable_outbound_message(self, reliable_message) -> None:
        session = self._require_active_session(reliable_message.session_id)
        await self._send_virtual_envelope(
            session=session,
            message_type="VIRTUAL_SESSION_RELIABLE_DATA",
            payload=reliable_message.to_reliable_payload(),
            virtual_envelope_ciphered=True,
        )
        marked = self.engine.services.session_manager.mark_reliable_outbound_sent(
            session_id=reliable_message.session_id,
            sequence_number=reliable_message.sequence_number,
        )
        self.engine.services.log_service.debug(
            "virtual_session_client",
            "sent virtual reliable envelope",
            session_id=session.session_id,
            reliable_message_id=reliable_message.reliable_message_id,
            sequence_number=reliable_message.sequence_number,
            attempts=marked.attempts if marked else reliable_message.attempts,
            inner_message_type=reliable_message.inner_message_type,
            pending_count=self.engine.services.session_manager.count_pending_reliable_outbound(
                session.session_id
            ),
        )

    async def send_protocol_message(
        self,
        *,
        session_id: str,
        message_type: str,
        payload: dict[str, object] | None = None,
        virtual_envelope_ciphered: bool = True,
    ) -> None:
        if not message_type:
            raise ValueError("message_type cannot be empty.")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload must be an object.")

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
            raise ValueError("The virtual session has no associated local route.")

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

    async def _wait_for_activation(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> bool:
        deadline = asyncio.get_running_loop().time() + (
            timeout_seconds or self._handshake_timeout_seconds
        )

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
            raise ValueError("The provided virtual session is not active.")
        return session

    def _require_session(self, session_id: str):
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_scope != "virtual":
            raise ValueError("The provided virtual session does not exist in memory.")
        return session

    async def _resolve_entry_points(
        self,
        remote_virtual_node_id: str,
    ) -> list["VirtualRouteEntryPoint"]:
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
                entry_points = self._select_entry_points_from_drt_result(
                    result,
                    remote_virtual_node_id,
                )
            except ValueError as error:
                last_error = str(error)
                await asyncio.sleep(self._drt_lookup_retry_seconds)
                continue

            self.engine.services.log_service.info(
                "virtual_session_client",
                "resolved drt entry point candidates",
                entry_point_count=len(entry_points),
                first_entry_point_physical_node_id=entry_points[0].physical_node_id,
                first_final_path_id=entry_points[0].final_path_id,
                first_rtt=entry_points[0].rtt,
            )
            return entry_points

        raise RuntimeError(
            "No valid DRT route was found for the remote virtual node "
            f"within {self._drt_lookup_timeout_seconds:.1f}s. Last state: {last_error}."
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

    def _select_entry_points_from_drt_result(
        self,
        result: dict[str, object],
        remote_virtual_node_id: str,
    ) -> list["VirtualRouteEntryPoint"]:
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
                -self._entry_point_reachability_score(entry),
                _expires_at_sort_key(entry.expires_at),
                -entry.rtt,
            ),
            reverse=True,
        )
        return valid_entries

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
            raise RuntimeError("Could not resolve the physical entry point DPNT.")

        record_json = result.get("record_json")
        if not isinstance(record_json, str) or not record_json:
            raise RuntimeError("The DPNT response does not contain a valid record_json.")

        record = parse_record("dpnt", record_json)
        if not isinstance(record, DpntRecordPayload):
            raise RuntimeError("The DPNT response does not contain a valid DPNT payload.")
        if record.pk_physical_node != entry_point.physical_node_public_key:
            raise RuntimeError("The resolved DPNT does not belong to the DRT entry point.")
        if not self._is_valid_dpnt_record(entry_point.physical_node_id, record):
            raise RuntimeError("The entry point DPNT has an invalid signature.")

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
        endpoints = canonical_endpoint_list(record.endpoints)
        payload = {
            "key": key_hex,
            "pk_physical_node": record.pk_physical_node,
            "endpoints": endpoints,
            "transport_methods": sorted(
                {
                    endpoint["transport"]
                    for endpoint in endpoints
                    if isinstance(endpoint.get("transport"), str)
                }
            ),
            "reachability_class": record.reachability_class,
            "relay_capable": record.relay_capable,
            "hole_punch_capable": record.hole_punch_capable,
            "protocol_version": record.protocol_version,
            "feature_flags": record.feature_flags,
            "status": record.status,
        }
        return verify_dilithium_payload_signature(payload, record.signature, record.pk_physical_node)

    def _read_session_entry_point_physical_node_id(self, session) -> str | None:
        entry_point_physical_node_id = load_json_object(session.metadata_json).get(
            "entry_point_physical_node_id"
        )
        if isinstance(entry_point_physical_node_id, str) and entry_point_physical_node_id:
            return entry_point_physical_node_id
        return None

    def _entry_point_reachability_score(self, entry_point: "VirtualRouteEntryPoint") -> int:
        endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
            entry_point.physical_node_id
        )
        if not endpoints:
            return 3
        return min(self._transport_preference(endpoint.transport) for endpoint in endpoints)

    def _entry_point_handshake_timeout_seconds(
        self,
        entry_point: "VirtualRouteEntryPoint",
    ) -> float:
        route_rtt_seconds = max(0.001, entry_point.rtt / 1000.0)
        route_timeout_seconds = (
            route_rtt_seconds
            * self.engine.services.config.virtual_session_timeout_rtt_multiplier
        )
        return max(
            self._handshake_timeout_seconds,
            self.engine.services.config.virtual_session_timeout_min_seconds,
            route_timeout_seconds,
        )

    @staticmethod
    def _transport_preference(transport_name: str | None) -> int:
        if transport_name == "tcp":
            return 0
        if transport_name == "relay_tcp":
            return 1
        if transport_name == "udp":
            return 2
        return 3

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
