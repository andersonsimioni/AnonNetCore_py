from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from common import load_json_object
from dht import parse_record, serialize_record, validate_and_merge
from storage.models import DhtRecord

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler
from ..helpers import as_payload_dict as _as_payload_dict
from ..helpers import read_string_or_none as _read_required_string
from ..helpers import read_string_or_none as _read_optional_string


class DhtProtocolHandler(ProtocolMessageHandler):
    protocol_family = "dht"
    _pending_forward_ttl_seconds = 120.0

    supported_message_types = {
        "DHT_PUBLISH",
        "DHT_QUERY",
        "DHT_RESULT",
    }

    def __init__(self) -> None:
        self._pending_forwards: dict[str, PendingDhtForward] = {}

    @staticmethod
    def build_query_payload(
        *,
        header: dict[str, object],
        namespace: str,
        logical_key: str,
        ttl: int | None = None,
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": {
                    "namespace": namespace,
                    "logical_key": logical_key,
                    "ttl": ttl,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def build_publish_payload(
        *,
        header: dict[str, object],
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None,
        ttl: int | None = None,
        stored_by: list[str] | None = None,
        pow_nonce: int | None = None,
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": {
                    "namespace": namespace,
                    "logical_key": logical_key,
                    "record_json": record_json,
                    "expires_at": expires_at,
                    "ttl": ttl,
                    "stored_by": stored_by or [],
                    "pow_nonce": pow_nonce,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def build_result_payload(
        *,
        request_header: dict[str, object],
        status: str,
        key_hex: str,
        responsible_nodes: list[dict[str, object]],
        stored_locally: bool,
        record_json: str | None = None,
        expires_at: str | None = None,
        stored_by: list[str] | None = None,
        required_stored_count: int | None = None,
    ) -> bytes:
        resolved_stored_by = stored_by or []
        resolved_required_count = (
            max(0, required_stored_count)
            if isinstance(required_stored_count, int)
            else len(resolved_stored_by)
        )
        return json.dumps(
            {
                "header": {
                    "version": request_header.get("version", 1),
                    "message_type": "DHT_RESULT",
                    "message_id": str(uuid4()),
                    "response_to_message_id": request_header.get("message_id"),
                    "message_sequence": request_header.get("message_sequence"),
                    "physical_session_id": request_header.get("physical_session_id"),
                    "virtual_session_id": request_header.get("virtual_session_id"),
                },
                "payload": {
                    "status": status,
                    "key": key_hex,
                    "stored_locally": stored_locally,
                    "responsible_nodes": responsible_nodes,
                    "record_json": record_json,
                    "expires_at": expires_at,
                    "stored_by": resolved_stored_by,
                    "stored_count": len(resolved_stored_by),
                    "required_stored_count": resolved_required_count,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "DHT_PUBLISH":
            return await self._handle_publish(envelope, context, services)

        if envelope.message_type == "DHT_QUERY":
            return await self._handle_query(envelope, context, services)

        if envelope.message_type == "DHT_RESULT":
            return await self._handle_result(envelope, context, services)

        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_publish(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        namespace = _read_required_string(payload, "namespace")
        logical_key = _read_required_string(payload, "logical_key")
        record_json = _read_required_string(payload, "record_json")
        expires_at = _read_optional_datetime(payload, "expires_at")
        stored_by = _read_string_list(payload, "stored_by")
        pow_nonce = _read_optional_int(payload, "pow_nonce")

        if namespace is None or logical_key is None or record_json is None:
            services.log_service.warning(
                "dht",
                "received invalid dht publish payload",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
            )
            return self._build_invalid_result(envelope, "invalid_dht_publish_payload")

        key_hex = services.dht_service.build_key(namespace, logical_key)
        if pow_nonce is None or not services.dht_service.validate_publish_pow(
            key_hex=key_hex,
            record_json=record_json,
            nonce=pow_nonce,
            difficulty_bits=services.config.dht_publish_pow_difficulty_bits,
        ):
            services.log_service.warning(
                "dht",
                "received dht publish with invalid proof of work",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                pow_nonce=pow_nonce,
                pow_difficulty_bits=services.config.dht_publish_pow_difficulty_bits,
            )
            return self._build_invalid_result(envelope, "invalid_dht_publish_pow")

        closest_nodes_result = services.dht_service.select_k_closest_nodes(key_hex)
        responsible_nodes = closest_nodes_result["nodes"]
        required_stored_count = len(responsible_nodes)
        services.log_service.debug(
            "dht",
            "handling dht publish",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            local_node_is_responsible=closest_nodes_result["local_node_is_responsible"],
            responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
            responsible_count=len(responsible_nodes),
            replication_factor=closest_nodes_result.get("replication_factor"),
            stored_by=stored_by,
            stored_count=len(stored_by),
            required_stored_count=required_stored_count,
            pow_nonce=pow_nonce,
            pow_difficulty_bits=services.config.dht_publish_pow_difficulty_bits,
        )

        if not closest_nodes_result["local_node_is_responsible"]:
            return await self._forward_or_return_failure(
                envelope=envelope,
                context=context,
                services=services,
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                failure_status="not_routable",
                stored_by=stored_by,
                required_stored_count=required_stored_count,
            )

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return self._build_invalid_result(envelope, "local_physical_node_not_initialized")

        if local_node.id not in stored_by:
            self._upsert_local_record(
                services=services,
                key_hex=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                record_json=record_json,
                expires_at=expires_at,
                source="dht_publish",
            )
            stored_by = [*stored_by, local_node.id]

        services.log_service.info(
            "dht",
            "stored dht record locally from publish",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            stored_count=len(stored_by),
            required_stored_count=required_stored_count,
        )

        if len(stored_by) < required_stored_count:
            return await self._forward_or_return_failure(
                envelope=envelope,
                context=context,
                services=services,
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                failure_status="partially_stored",
                stored_by=stored_by,
                required_stored_count=required_stored_count,
            )

        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status="stored",
            key_hex=key_hex,
            responsible_nodes=[],
            stored_locally=True,
            stored_by=stored_by,
            required_stored_count=required_stored_count,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "store_local_dht_record",
                "key": key_hex,
            },
        )

    async def _handle_query(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        namespace = _read_required_string(payload, "namespace")
        logical_key = _read_required_string(payload, "logical_key")

        if namespace is None or logical_key is None:
            services.log_service.warning(
                "dht",
                "received invalid dht query payload",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
            )
            return self._build_invalid_result(envelope, "invalid_dht_query_payload")

        key_hex = services.dht_service.build_key(namespace, logical_key)
        closest_nodes_result = services.dht_service.select_k_closest_nodes(key_hex)
        responsible_nodes = closest_nodes_result["nodes"]
        services.log_service.debug(
            "dht",
            "handling dht query",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            local_node_is_responsible=closest_nodes_result["local_node_is_responsible"],
            responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
        )

        if not closest_nodes_result["local_node_is_responsible"]:
            return await self._forward_or_return_failure(
                envelope=envelope,
                context=context,
                services=services,
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                failure_status="not_routable",
            )

        dht_record = self._load_validated_local_record(
            services=services,
            key_hex=key_hex,
        )
        if dht_record is None:
            query_cursor_distance_hex = self._resolve_local_distance_hex(
                services=services,
                key_hex=key_hex,
            )
            services.log_service.info(
                "dht",
                "dht record not found locally, trying another responsible node",
                key=key_hex,
                responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
                query_cursor_distance_hex=query_cursor_distance_hex,
            )
            return await self._forward_or_return_failure(
                envelope=envelope,
                context=context,
                services=services,
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                failure_status="not_found",
                query_cursor_distance_hex=query_cursor_distance_hex,
            )

        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status="found",
            key_hex=key_hex,
            responsible_nodes=[],
            stored_locally=True,
            record_json=dht_record.record_json,
            expires_at=dht_record.expires_at.isoformat() if dht_record.expires_at is not None else None,
        )
        services.log_service.info(
            "dht",
            "returned validated dht record",
            key=key_hex,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "return_validated_dht_record",
                "key": key_hex,
            },
        )

    async def _handle_result(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        response_to_message_id = envelope.header.get("response_to_message_id")
        if isinstance(response_to_message_id, str):
            forwarded_result = self._pop_pending_forward(response_to_message_id)
            if forwarded_result is not None:
                response_payload = self.build_result_payload(
                    request_header={
                        **envelope.header,
                        "message_id": forwarded_result.previous_message_id,
                        "physical_session_id": forwarded_result.previous_session_id,
                    },
                    status=str(payload.get("status") or "invalid_result"),
                    key_hex=str(payload.get("key") or ""),
                    responsible_nodes=[],
                    stored_locally=payload.get("stored_locally") is True,
                    record_json=payload.get("record_json") if isinstance(payload.get("record_json"), str) else None,
                    expires_at=payload.get("expires_at") if isinstance(payload.get("expires_at"), str) else None,
                    stored_by=_read_string_list(payload, "stored_by"),
                    required_stored_count=_read_optional_int(payload, "required_stored_count"),
                )
                services.log_service.debug(
                    "dht",
                    "forwarding dht result to previous hop",
                    response_to_message_id=response_to_message_id,
                    previous_session_id=forwarded_result.previous_session_id,
                    previous_message_id=forwarded_result.previous_message_id,
                    status=payload.get("status"),
                    key=payload.get("key"),
                    stored_count=payload.get("stored_count"),
                    required_stored_count=payload.get("required_stored_count"),
                )
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=True,
                    message_type=envelope.message_type,
                    metadata={
                        "protocol_family": self.protocol_family,
                        "transport_name": context.transport_name,
                        "action": "send_payload_to_physical_session",
                        "target_physical_session_id": forwarded_result.previous_session_id,
                        "payload": response_payload,
                    },
                )
            services.log_service.debug(
                "dht",
                "dht result has no pending forward on this hop",
                response_to_message_id=response_to_message_id,
                status=payload.get("status"),
                key=payload.get("key"),
                stored_count=payload.get("stored_count"),
                required_stored_count=payload.get("required_stored_count"),
            )

        if isinstance(response_to_message_id, str) and services.protocol_clients is not None:
            services.protocol_clients.physical.dht.complete_result(
                response_to_message_id=response_to_message_id,
                result_data={
                    "status": payload.get("status"),
                    "key": payload.get("key"),
                    "stored_locally": payload.get("stored_locally"),
                    "responsible_nodes": [],
                    "record_json": payload.get("record_json"),
                    "expires_at": payload.get("expires_at"),
                    "stored_by": _read_string_list(payload, "stored_by"),
                    "stored_count": payload.get("stored_count"),
                    "required_stored_count": payload.get("required_stored_count"),
                    "remote_host": context.remote_host,
                    "remote_port": context.remote_port,
                    "transport_name": context.transport_name,
                },
            )
            services.log_service.debug(
                "dht",
                "resolved pending dht result",
                response_to_message_id=response_to_message_id,
                status=payload.get("status"),
                key=payload.get("key"),
            )

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "resolve_pending_dht_result",
                "response_to_message_id": response_to_message_id,
            },
        )

    async def _forward_or_return_failure(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
        key_hex: str,
        responsible_nodes: list[dict[str, object]],
        failure_status: str,
        stored_by: list[str] | None = None,
        required_stored_count: int | None = None,
        query_cursor_distance_hex: str | None = None,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        resolved_stored_by = stored_by if stored_by is not None else _read_string_list(payload, "stored_by")
        resolved_query_cursor_distance_hex = (
            query_cursor_distance_hex
            if query_cursor_distance_hex is not None
            else _read_optional_string(payload, "query_cursor_distance_hex")
        )
        resolved_required_count = (
            required_stored_count
            if required_stored_count is not None
            else len(responsible_nodes)
        )
        ttl = _read_ttl(payload, services.config.dht_client_max_hops)
        if ttl <= 0:
            services.log_service.warning(
                "dht",
                "dht request ttl expired",
                key=key_hex,
                message_type=envelope.message_type,
                stored_count=len(resolved_stored_by),
                required_stored_count=resolved_required_count,
            )
            return self._build_result_response(
                envelope=envelope,
                context=context,
                status=_select_failed_publish_status(
                    envelope.message_type,
                    resolved_stored_by,
                    "ttl_expired",
                ),
                key_hex=key_hex,
                stored_by=resolved_stored_by,
                required_stored_count=resolved_required_count,
            )

        previous_session = self._get_previous_session(envelope, services)
        next_session = await self._select_next_session(
            services=services,
            responsible_nodes=responsible_nodes,
            previous_remote_physical_node_id=(
                previous_session.remote_identity_id if previous_session is not None else None
            ),
            stored_by=resolved_stored_by,
            query_cursor_distance_hex=(
                resolved_query_cursor_distance_hex
                if envelope.message_type == "DHT_QUERY"
                else None
            ),
        )
        if next_session is None:
            services.log_service.warning(
                "dht",
                "dht request could not be forwarded",
                key=key_hex,
                message_type=envelope.message_type,
                responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
                responsible_count=len(responsible_nodes),
                previous_remote_physical_node_id=(
                    previous_session.remote_identity_id if previous_session is not None else None
                ),
                stored_count=len(resolved_stored_by),
                required_stored_count=resolved_required_count,
                query_cursor_distance_hex=resolved_query_cursor_distance_hex,
            )
            return self._build_result_response(
                envelope=envelope,
                context=context,
                status=_select_failed_publish_status(
                    envelope.message_type,
                    resolved_stored_by,
                    failure_status,
                ),
                key_hex=key_hex,
                stored_by=resolved_stored_by,
                required_stored_count=resolved_required_count,
            )

        forward_header = {
            **envelope.header,
            "message_id": str(uuid4()),
            "physical_session_id": next_session.session_id,
        }
        forward_payload = {
            **payload,
            "ttl": ttl - 1,
            "stored_by": resolved_stored_by,
        }
        if envelope.message_type == "DHT_QUERY" and resolved_query_cursor_distance_hex is not None:
            forward_payload["query_cursor_distance_hex"] = resolved_query_cursor_distance_hex
        packet_bytes = json.dumps(
            {
                "header": forward_header,
                "payload": forward_payload,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        previous_message_id = envelope.header.get("message_id")
        previous_session_id = envelope.header.get("physical_session_id")
        if isinstance(previous_message_id, str) and isinstance(previous_session_id, str):
            self._remember_pending_forward(
                forward_message_id=forward_header["message_id"],
                previous_session_id=previous_session_id,
                previous_message_id=previous_message_id,
            )

        services.log_service.info(
            "dht",
            "forwarding dht request to next hop",
            key=key_hex,
            message_type=envelope.message_type,
            next_remote_physical_node_id=next_session.remote_identity_id,
            next_session_id=next_session.session_id,
            ttl=ttl - 1,
            stored_count=len(resolved_stored_by),
            required_stored_count=resolved_required_count,
            remaining_responsible_count=max(0, resolved_required_count - len(resolved_stored_by)),
            query_cursor_distance_hex=resolved_query_cursor_distance_hex,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "send_payload_to_physical_session",
                "target_physical_session_id": next_session.session_id,
                "payload": packet_bytes,
                "key": key_hex,
            },
        )

    def _build_result_response(
        self,
        *,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        status: str,
        key_hex: str,
        stored_by: list[str] | None = None,
        required_stored_count: int | None = None,
    ) -> PacketProcessingResult:
        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status=status,
            key_hex=key_hex,
            responsible_nodes=[],
            stored_locally=False,
            stored_by=stored_by,
            required_stored_count=required_stored_count,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": f"return_dht_{status}",
                "key": key_hex,
            },
        )

    async def _select_next_session(
        self,
        *,
        services: EngineServices,
        responsible_nodes: list[dict[str, object]],
        previous_remote_physical_node_id: str | None,
        stored_by: list[str],
        query_cursor_distance_hex: str | None,
    ):
        query_cursor_distance = _parse_distance_hex(query_cursor_distance_hex)
        for node in responsible_nodes:
            if node.get("is_local") is True:
                continue

            node_id = node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue
            if query_cursor_distance is not None:
                node_distance = node.get("distance_int")
                if isinstance(node_distance, int) and node_distance <= query_cursor_distance:
                    continue
            if node_id == previous_remote_physical_node_id:
                continue
            if node_id in stored_by:
                continue

            existing_session = services.session_manager.get_active_physical_session_by_remote_node_id(node_id)
            if existing_session is not None:
                if not _is_observed_only_physical_session(existing_session):
                    return existing_session
                services.log_service.debug(
                    "dht",
                    "ignoring observed-only physical session for dht forward",
                    session_id=existing_session.session_id,
                    remote_physical_node_id=node_id,
                    remote_host=existing_session.remote_host,
                    remote_port=existing_session.remote_port,
                )

            if services.protocol_clients is None:
                continue

            try:
                session_id = await services.protocol_clients.physical.session.start_session(
                    remote_physical_node_id=node_id,
                )
            except Exception as error:
                services.log_service.warning(
                    "dht",
                    "failed to open dht forward session",
                    remote_physical_node_id=node_id,
                    responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
                    previous_remote_physical_node_id=previous_remote_physical_node_id,
                    stored_by=stored_by,
                    error_type=type(error).__name__,
                    error=repr(error),
                )
                continue

            session = services.session_manager.get_session_by_session_id(session_id)
            if session is not None and session.session_state == "active":
                return session

        return None

    def _resolve_local_distance_hex(
        self,
        *,
        services: EngineServices,
        key_hex: str,
    ) -> str | None:
        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return None

        distance = services.dht_service.xor_distance_hex(key_hex, local_node.id)
        return format(distance, "0128x")

    @staticmethod
    def _get_previous_session(envelope: ProtocolEnvelope, services: EngineServices):
        session_id = envelope.header.get("physical_session_id")
        if not isinstance(session_id, str) or not session_id:
            return None
        return services.session_manager.get_session_by_session_id(session_id)

    def _remember_pending_forward(
        self,
        *,
        forward_message_id: str,
        previous_session_id: str,
        previous_message_id: str,
    ) -> None:
        self._cleanup_pending_forwards()
        self._pending_forwards[forward_message_id] = PendingDhtForward(
            previous_session_id=previous_session_id,
            previous_message_id=previous_message_id,
            created_at=time.monotonic(),
        )

    def _pop_pending_forward(self, forward_message_id: str) -> "PendingDhtForward | None":
        self._cleanup_pending_forwards()
        return self._pending_forwards.pop(forward_message_id, None)

    def _cleanup_pending_forwards(self) -> None:
        now = time.monotonic()
        expired = [
            message_id
            for message_id, pending in self._pending_forwards.items()
            if now - pending.created_at > self._pending_forward_ttl_seconds
        ]
        for message_id in expired:
            self._pending_forwards.pop(message_id, None)

    @classmethod
    def _upsert_local_record(
        cls,
        *,
        services: EngineServices,
        key_hex: str,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: datetime | None,
        source: str,
    ) -> None:
        effective_record_json = record_json
        merge_status = "new"
        with services.database.session_scope() as session:
            existing_record = (
                session.query(DhtRecord)
                .filter(DhtRecord.key == key_hex)
                .filter(DhtRecord.last_validated_at.is_not(None))
                .order_by(DhtRecord.last_validated_at.desc(), DhtRecord.updated_at.desc())
                .first()
            )
            if existing_record is not None:
                try:
                    parent_payload = parse_record(namespace, existing_record.record_json)
                    fragment_payload = parse_record(namespace, record_json)
                    merged_payload = validate_and_merge(
                        namespace,
                        key_hex,
                        parent_payload,
                        fragment_payload,
                    )
                    effective_record_json = serialize_record(merged_payload)
                    merge_status = "merged"
                except Exception as error:
                    merge_status = "replace_after_merge_failure"
                    services.log_service.warning(
                        "dht",
                        "failed to merge incoming dht fragment; storing incoming record",
                        key=key_hex,
                        namespace=namespace,
                        logical_key=logical_key,
                        source=source,
                        error_type=type(error).__name__,
                        error=repr(error),
                    )

            now = datetime.now(timezone.utc)
            if existing_record is None:
                dht_record = DhtRecord(
                    key=key_hex,
                    namespace=namespace,
                    logical_key=logical_key,
                    record_json=effective_record_json,
                    source=source,
                    last_validated_at=now,
                    expires_at=expires_at,
                )
                session.add(dht_record)
            else:
                existing_record.namespace = namespace
                existing_record.logical_key = logical_key
                existing_record.record_json = effective_record_json
                existing_record.source = source
                existing_record.last_validated_at = now
                existing_record.expires_at = expires_at
                dht_record = existing_record
                cls._delete_older_records_for_key(session, key_hex, keep_record_id=existing_record.id)
            services.log_service.debug(
                "dht",
                "upserted local dht record",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                source=source,
                merge_status=merge_status,
                record_json_size=len(effective_record_json),
            )

    @staticmethod
    def _delete_older_records_for_key(session, key_hex: str, *, keep_record_id: int) -> None:
        session.query(DhtRecord).filter(
            DhtRecord.key == key_hex,
            DhtRecord.id != keep_record_id,
        ).delete(synchronize_session=False)

    @staticmethod
    def _load_validated_local_record(
        *,
        services: EngineServices,
        key_hex: str,
    ) -> DhtRecord | None:
        with services.database.session_scope() as session:
            return (
                session.query(DhtRecord)
                .filter(DhtRecord.key == key_hex)
                .filter(DhtRecord.last_validated_at.is_not(None))
                .order_by(DhtRecord.last_validated_at.desc(), DhtRecord.updated_at.desc())
                .first()
            )

    def _build_not_implemented_result(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "database_path": str(services.database.config.db_path),
                "next_step": "implement_dht_query_and_result_flow",
            },
        )


def _read_optional_datetime(payload: dict[str, object], field_name: str) -> datetime | None:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _read_ttl(payload: dict[str, object], default_ttl: int) -> int:
    value = payload.get("ttl")
    if isinstance(value, int):
        return max(0, value)
    return max(0, default_ttl)


def _read_string_list(payload: dict[str, object], field_name: str) -> list[str]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        return []

    result: list[str] = []
    for item in value:
        if isinstance(item, str) and item and item not in result:
            result.append(item)
    return result


def _read_optional_int(payload: dict[str, object], field_name: str) -> int | None:
    value = payload.get(field_name)
    if isinstance(value, int):
        return max(0, value)
    return None


def _parse_distance_hex(value: str | None) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(value, 16)
    except ValueError:
        return None


def _select_failed_publish_status(
    message_type: str,
    stored_by: list[str],
    fallback_status: str,
) -> str:
    if message_type == "DHT_PUBLISH" and stored_by:
        return "partially_stored"
    return fallback_status


def _summarize_responsible_nodes(nodes: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "node_id": node.get("node_id"),
            "is_local": node.get("is_local"),
            "distance_hex": node.get("distance_hex"),
            "endpoint_count": len(node.get("endpoints") or []),
        }
        for node in nodes
    ]


def _is_observed_only_physical_session(session) -> bool:
    return load_json_object(session.metadata_json).get("physical_endpoint_source") == "observed"


@dataclass(slots=True, frozen=True)
class PendingDhtForward:
    previous_session_id: str
    previous_message_id: str
    created_at: float
