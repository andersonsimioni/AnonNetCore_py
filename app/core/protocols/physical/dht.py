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
        required_stored_count: int | None = None,
        pow_nonce: int | None = None,
        trace_context: dict[str, object] | None = None,
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
                    "required_stored_count": required_stored_count,
                    "pow_nonce": pow_nonce,
                    "trace_context": trace_context or {},
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
        trace_context: dict[str, object] | None = None,
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
                    "trace_context": trace_context or {},
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
        trace_context = _read_trace_context(payload)

        if namespace is None or logical_key is None or record_json is None:
            services.log_service.warning(
                "dht",
                "received invalid dht publish payload",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
                trace_context=trace_context,
            )
            return self._build_invalid_result(envelope, "invalid_dht_publish_payload")

        key_hex = services.dht_service.build_key(namespace, logical_key)
        pow_details = services.dht_service.build_publish_pow_details(
            key_hex=key_hex,
            record_json=record_json,
            nonce=pow_nonce,
            difficulty_bits=services.config.network_pow_difficulty_bits,
        )
        if not pow_details["is_valid"]:
            services.log_service.warning(
                "dht",
                "received dht publish with invalid proof of work",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                pow_nonce=pow_nonce,
                pow_difficulty_bits=services.config.network_pow_difficulty_bits,
                pow_canonical_hash=pow_details["canonical_hash"],
                pow_proof_hash_prefix=pow_details["proof_hash_prefix"],
                record_json_size=len(record_json),
                trace_context=trace_context,
            )
            return self._build_invalid_result(envelope, "invalid_dht_publish_pow")

        if not services.dht_service.validate_record_payload_pow(
            namespace=namespace,
            key_hex=key_hex,
            record_json=record_json,
            difficulty_bits=services.config.network_pow_difficulty_bits,
        ):
            services.log_service.warning(
                "dht",
                "received dht publish with invalid semantic payload proof of work",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                pow_difficulty_bits=services.config.network_pow_difficulty_bits,
                record_json_size=len(record_json),
                trace_context=trace_context,
            )
            return self._build_invalid_result(envelope, "invalid_dht_payload_pow")

        closest_nodes_result = services.dht_service.select_k_closest_nodes(key_hex)
        responsible_nodes = closest_nodes_result["nodes"]
        required_stored_count = _read_optional_int(payload, "required_stored_count")
        if required_stored_count is None:
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
            pow_difficulty_bits=services.config.network_pow_difficulty_bits,
            pow_canonical_hash=pow_details["canonical_hash"],
            pow_proof_hash_prefix=pow_details["proof_hash_prefix"],
            record_json_size=len(record_json),
            trace_context=trace_context,
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
                trace_context=trace_context,
            )

        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return self._build_invalid_result(envelope, "local_physical_node_not_initialized")

        if local_node.id not in stored_by:
            services.log_service.debug(
                "dht",
                "storing dht publish on responsible local node",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                local_physical_node_id=local_node.id,
                stored_by_before=stored_by,
                required_stored_count=required_stored_count,
                trace_context=trace_context,
            )
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
            local_physical_node_id=local_node.id,
            stored_by=stored_by,
            stored_count=len(stored_by),
            required_stored_count=required_stored_count,
            trace_context=trace_context,
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
                trace_context=trace_context,
            )

        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status="stored",
            key_hex=key_hex,
            responsible_nodes=[],
            stored_locally=True,
            stored_by=stored_by,
            required_stored_count=required_stored_count,
            trace_context=trace_context,
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
        accumulated_record_json = _read_optional_string(payload, "query_record_json")
        accumulated_expires_at = _read_optional_string(payload, "query_expires_at")
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
            has_accumulated_record=bool(accumulated_record_json),
        )

        if not closest_nodes_result["local_node_is_responsible"]:
            return await self._forward_or_return_failure(
                envelope=envelope,
                context=context,
                services=services,
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                failure_status="not_routable",
                query_record_json=accumulated_record_json,
                query_expires_at=accumulated_expires_at,
            )

        dht_record = self._load_validated_local_record(
            services=services,
            key_hex=key_hex,
        )
        query_cursor_distance_hex = self._resolve_local_distance_hex(
            services=services,
            key_hex=key_hex,
        )
        if dht_record is None:
            services.log_service.info(
                "dht",
                "dht record not found locally, trying another responsible node",
                key=key_hex,
                responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
                query_cursor_distance_hex=query_cursor_distance_hex,
                has_accumulated_record=bool(accumulated_record_json),
            )
            return await self._forward_or_return_failure(
                envelope=envelope,
                context=context,
                services=services,
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                failure_status=("found" if accumulated_record_json else "not_found"),
                query_cursor_distance_hex=query_cursor_distance_hex,
                query_record_json=accumulated_record_json,
                query_expires_at=accumulated_expires_at,
            )

        merged_record_json = self._merge_query_record_json(
            services=services,
            namespace=namespace,
            logical_key=logical_key,
            key_hex=key_hex,
            accumulated_record_json=accumulated_record_json,
            local_record_json=dht_record.record_json,
        )
        merged_expires_at = (
            dht_record.expires_at.isoformat()
            if dht_record.expires_at is not None
            else accumulated_expires_at
        )
        services.log_service.info(
            "dht",
            "dht query found local record and will continue responsible merge",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
            query_cursor_distance_hex=query_cursor_distance_hex,
            had_accumulated_record=bool(accumulated_record_json),
            merged_record_json_size=len(merged_record_json),
        )
        forward_result = await self._forward_or_return_failure(
            envelope=envelope,
            context=context,
            services=services,
            key_hex=key_hex,
            responsible_nodes=responsible_nodes,
            failure_status="found",
            query_cursor_distance_hex=query_cursor_distance_hex,
            query_record_json=merged_record_json,
            query_expires_at=merged_expires_at,
        )
        if forward_result.metadata.get("action") != "return_dht_found":
            return forward_result

        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status="found",
            key_hex=key_hex,
            responsible_nodes=[],
            stored_locally=True,
            record_json=merged_record_json,
            expires_at=merged_expires_at,
        )
        services.log_service.info(
            "dht",
            "returned validated dht record",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            merged_record_json_size=len(merged_record_json),
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
            forwarded_result = self._pop_pending_forward(response_to_message_id, services)
            if forwarded_result is not None:
                trace_context = _read_trace_context(payload)
                response_payload = self.build_result_payload(
                    request_header={
                        **envelope.header,
                        "message_id": forwarded_result.previous_message_id,
                        "physical_session_id": forwarded_result.previous_session_id,
                    },
                    status=str(payload.get("status") or "invalid_result"),
                    key_hex=str(payload.get("key") or ""),
                    responsible_nodes=_read_responsible_nodes(payload),
                    stored_locally=payload.get("stored_locally") is True,
                    record_json=payload.get("record_json") if isinstance(payload.get("record_json"), str) else None,
                    expires_at=payload.get("expires_at") if isinstance(payload.get("expires_at"), str) else None,
                    stored_by=_read_string_list(payload, "stored_by"),
                    required_stored_count=_read_optional_int(payload, "required_stored_count"),
                    trace_context=trace_context,
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
                    trace_context=trace_context,
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
                trace_context=_read_trace_context(payload),
            )

        if isinstance(response_to_message_id, str) and services.protocol_clients is not None:
            services.protocol_clients.physical.dht.complete_result(
                response_to_message_id=response_to_message_id,
                result_data={
                    "status": payload.get("status"),
                    "key": payload.get("key"),
                    "stored_locally": payload.get("stored_locally"),
                    "responsible_nodes": _read_responsible_nodes(payload),
                    "record_json": payload.get("record_json"),
                    "expires_at": payload.get("expires_at"),
                    "stored_by": _read_string_list(payload, "stored_by"),
                    "stored_count": payload.get("stored_count"),
                    "required_stored_count": payload.get("required_stored_count"),
                    "remote_host": context.remote_host,
                    "remote_port": context.remote_port,
                    "transport_name": context.transport_name,
                    "trace_context": _read_trace_context(payload),
                },
            )
            services.log_service.debug(
                "dht",
                "resolved pending dht result",
                response_to_message_id=response_to_message_id,
                status=payload.get("status"),
                key=payload.get("key"),
                trace_context=_read_trace_context(payload),
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
        query_record_json: str | None = None,
        query_expires_at: str | None = None,
        trace_context: dict[str, object] | None = None,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        resolved_trace_context = trace_context if trace_context is not None else _read_trace_context(payload)
        resolved_stored_by = stored_by if stored_by is not None else _read_string_list(payload, "stored_by")
        resolved_query_record_json = (
            query_record_json
            if query_record_json is not None
            else _read_optional_string(payload, "query_record_json")
        )
        resolved_query_expires_at = (
            query_expires_at
            if query_expires_at is not None
            else _read_optional_string(payload, "query_expires_at")
        )
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
        ttl = _read_ttl(payload, services.config.dht_request_max_forward_hops)
        if ttl <= 0:
            services.log_service.warning(
                "dht",
                "dht request ttl expired",
                key=key_hex,
                message_type=envelope.message_type,
                stored_count=len(resolved_stored_by),
                required_stored_count=resolved_required_count,
                trace_context=resolved_trace_context,
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
                responsible_nodes=responsible_nodes,
                stored_by=resolved_stored_by,
                required_stored_count=resolved_required_count,
                record_json=resolved_query_record_json,
                expires_at=resolved_query_expires_at,
                trace_context=resolved_trace_context,
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
                trace_context=resolved_trace_context,
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
                responsible_nodes=responsible_nodes,
                stored_by=resolved_stored_by,
                required_stored_count=resolved_required_count,
                record_json=resolved_query_record_json,
                expires_at=resolved_query_expires_at,
                trace_context=resolved_trace_context,
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
            "required_stored_count": resolved_required_count,
            "trace_context": resolved_trace_context,
        }
        if envelope.message_type == "DHT_QUERY" and resolved_query_record_json:
            forward_payload["query_record_json"] = resolved_query_record_json
            forward_payload["query_expires_at"] = resolved_query_expires_at
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
                services=services,
            )
            services.log_service.debug(
                "dht",
                "registered pending dht forward return path",
                key=key_hex,
                message_type=envelope.message_type,
                forward_message_id=forward_header["message_id"],
                previous_message_id=previous_message_id,
                previous_session_id=previous_session_id,
                next_session_id=next_session.session_id,
                next_remote_physical_node_id=next_session.remote_identity_id,
                stored_count=len(resolved_stored_by),
                required_stored_count=resolved_required_count,
                trace_context=resolved_trace_context,
            )
        else:
            services.log_service.warning(
                "dht",
                "dht forward has no previous return path in envelope header",
                key=key_hex,
                message_type=envelope.message_type,
                header_message_id=previous_message_id,
                header_physical_session_id=previous_session_id,
                next_session_id=next_session.session_id,
                next_remote_physical_node_id=next_session.remote_identity_id,
                trace_context=resolved_trace_context,
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
            trace_context=resolved_trace_context,
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
        responsible_nodes: list[dict[str, object]] | None = None,
        stored_by: list[str] | None = None,
        required_stored_count: int | None = None,
        record_json: str | None = None,
        expires_at: str | None = None,
        trace_context: dict[str, object] | None = None,
    ) -> PacketProcessingResult:
        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status=status,
            key_hex=key_hex,
            responsible_nodes=_summarize_responsible_nodes(responsible_nodes or []),
            stored_locally=False,
            record_json=record_json,
            expires_at=expires_at,
            stored_by=stored_by,
            required_stored_count=required_stored_count,
            trace_context=trace_context,
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
                services.log_service.debug(
                    "dht",
                    "skipping local responsible node during dht forward selection",
                    remote_physical_node_id=node.get("node_id"),
                    stored_by=stored_by,
                    query_cursor_distance_hex=query_cursor_distance_hex,
                )
                continue

            node_id = node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue
            endpoint_state = _summarize_known_endpoint_state(services, node_id)
            services.log_service.debug(
                "dht",
                "evaluating dht responsible node for forward",
                remote_physical_node_id=node_id,
                node_distance_hex=node.get("distance_hex"),
                node_endpoint_count=len(node.get("endpoints") or []),
                known_endpoint_state=endpoint_state,
                previous_remote_physical_node_id=previous_remote_physical_node_id,
                stored_by=stored_by,
                query_cursor_distance_hex=query_cursor_distance_hex,
            )
            if query_cursor_distance is not None:
                node_distance = node.get("distance_int")
                if isinstance(node_distance, int) and node_distance <= query_cursor_distance:
                    services.log_service.debug(
                        "dht",
                        "skipping dht responsible node behind query cursor",
                        remote_physical_node_id=node_id,
                        node_distance_hex=node.get("distance_hex"),
                        known_endpoint_state=endpoint_state,
                        query_cursor_distance_hex=query_cursor_distance_hex,
                    )
                    continue
            if node_id == previous_remote_physical_node_id:
                services.log_service.debug(
                    "dht",
                    "skipping previous dht forward node",
                    remote_physical_node_id=node_id,
                    previous_remote_physical_node_id=previous_remote_physical_node_id,
                    known_endpoint_state=endpoint_state,
                )
                continue
            if node_id in stored_by:
                services.log_service.debug(
                    "dht",
                    "skipping already stored dht responsible node",
                    remote_physical_node_id=node_id,
                    stored_by=stored_by,
                    known_endpoint_state=endpoint_state,
                )
                continue

            existing_session = services.session_manager.get_active_physical_session_by_remote_node_id(node_id)
            if existing_session is not None:
                if not _is_observed_only_physical_session(existing_session):
                    services.log_service.debug(
                        "dht",
                        "selected active dht forward session",
                        remote_physical_node_id=node_id,
                        session_id=existing_session.session_id,
                        transport=existing_session.transport,
                        remote_host=existing_session.remote_host,
                        remote_port=existing_session.remote_port,
                        known_endpoint_state=endpoint_state,
                    )
                    return existing_session
                services.log_service.debug(
                    "dht",
                    "ignoring observed-only physical session for dht forward",
                    session_id=existing_session.session_id,
                    remote_physical_node_id=node_id,
                    remote_host=existing_session.remote_host,
                    remote_port=existing_session.remote_port,
                    known_endpoint_state=endpoint_state,
                )

            if services.protocol_clients is None:
                continue

            try:
                services.log_service.debug(
                    "dht",
                    "opening dht forward session to responsible node",
                    remote_physical_node_id=node_id,
                    responsible_nodes=_summarize_responsible_nodes(responsible_nodes),
                    previous_remote_physical_node_id=previous_remote_physical_node_id,
                    stored_by=stored_by,
                    known_endpoint_state=endpoint_state,
                )
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
                    known_endpoint_state=endpoint_state,
                )
                continue

            session = services.session_manager.get_session_by_session_id(session_id)
            if session is not None and session.session_state == "active":
                services.log_service.info(
                    "dht",
                    "opened dht forward session to responsible node",
                    remote_physical_node_id=node_id,
                    session_id=session.session_id,
                    transport=session.transport,
                    remote_host=session.remote_host,
                    remote_port=session.remote_port,
                    known_endpoint_state=endpoint_state,
                )
                return session
            services.log_service.warning(
                "dht",
                "dht forward session did not become active",
                remote_physical_node_id=node_id,
                session_id=session_id,
                session_state=(session.session_state if session is not None else None),
                transport=(session.transport if session is not None else None),
                known_endpoint_state=endpoint_state,
            )

        return None

    def _merge_query_record_json(
        self,
        *,
        services: EngineServices,
        namespace: str,
        logical_key: str,
        key_hex: str,
        accumulated_record_json: str | None,
        local_record_json: str,
    ) -> str:
        if not accumulated_record_json:
            return local_record_json

        try:
            accumulated_payload = parse_record(namespace, accumulated_record_json)
            local_payload = parse_record(namespace, local_record_json)
            merged_payload = validate_and_merge(
                namespace,
                key_hex,
                accumulated_payload,
                local_payload,
                services.config.network_pow_difficulty_bits,
            )
            merged_record_json = serialize_record(merged_payload)
        except Exception as error:
            services.log_service.warning(
                "dht",
                "failed to merge dht query records; keeping local record",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                accumulated_record_json_size=len(accumulated_record_json),
                local_record_json_size=len(local_record_json),
                error_type=type(error).__name__,
                error=repr(error),
            )
            return local_record_json

        services.log_service.debug(
            "dht",
            "merged accumulated dht query record with local record",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            accumulated_record_json_size=len(accumulated_record_json),
            local_record_json_size=len(local_record_json),
            merged_record_json_size=len(merged_record_json),
        )
        return merged_record_json

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
        services: EngineServices,
    ) -> None:
        self._cleanup_pending_forwards(services)
        self._pending_forwards[forward_message_id] = PendingDhtForward(
            previous_session_id=previous_session_id,
            previous_message_id=previous_message_id,
            created_at=time.monotonic(),
        )

    def _pop_pending_forward(
        self,
        forward_message_id: str,
        services: EngineServices,
    ) -> "PendingDhtForward | None":
        self._cleanup_pending_forwards(services)
        return self._pending_forwards.pop(forward_message_id, None)

    def _cleanup_pending_forwards(self, services: EngineServices | None = None) -> None:
        now = time.monotonic()
        ttl_seconds = self._get_pending_forward_ttl_seconds(services)
        expired: list[tuple[str, PendingDhtForward, float]] = []
        for message_id, pending in self._pending_forwards.items():
            age_seconds = now - pending.created_at
            if age_seconds > ttl_seconds:
                expired.append((message_id, pending, age_seconds))
        for message_id, pending, age_seconds in expired:
            self._pending_forwards.pop(message_id, None)
            if services is not None:
                services.log_service.warning(
                    "dht",
                    "expired pending dht forward return path",
                    forward_message_id=message_id,
                    previous_session_id=pending.previous_session_id,
                    previous_message_id=pending.previous_message_id,
                    age_seconds=round(age_seconds, 3),
                    ttl_seconds=ttl_seconds,
                    pending_forward_count=len(self._pending_forwards),
                )

    @staticmethod
    def _get_pending_forward_ttl_seconds(
        services: EngineServices | None,
    ) -> float:
        if services is None:
            return 120.0

        return (
            services.config.dht_request_timeout_seconds
            * services.config.dht_query_attempt_count
            + services.config.dht_forward_return_path_grace_seconds
        )

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
        if not services.dht_service.validate_record_payload_pow(
            namespace=namespace,
            key_hex=key_hex,
            record_json=record_json,
            difficulty_bits=services.config.network_pow_difficulty_bits,
        ):
            services.log_service.warning(
                "dht",
                "ignored local dht record with invalid semantic payload proof of work",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                source=source,
                record_json_size=len(record_json),
            )
            return

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
                        services.config.network_pow_difficulty_bits,
                    )
                    effective_record_json = serialize_record(merged_payload)
                    merge_status = "merged"
                except Exception as error:
                    services.log_service.warning(
                        "dht",
                        "failed to merge incoming dht fragment; ignoring incoming record",
                        key=key_hex,
                        namespace=namespace,
                        logical_key=logical_key,
                        source=source,
                        error_type=type(error).__name__,
                        error=repr(error),
                    )
                    return

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


def _read_responsible_nodes(payload: dict[str, object]) -> list[dict[str, object]]:
    value = payload.get("responsible_nodes")
    if not isinstance(value, list):
        return []

    return [item for item in value if isinstance(item, dict)]


def _read_optional_int(payload: dict[str, object], field_name: str) -> int | None:
    value = payload.get(field_name)
    if isinstance(value, int):
        return max(0, value)
    return None


def _read_trace_context(payload: dict[str, object]) -> dict[str, object]:
    value = payload.get("trace_context")
    if isinstance(value, dict):
        return value
    return {}


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


def _summarize_known_endpoint_state(
    services: EngineServices,
    node_id: str,
) -> dict[str, object]:
    endpoints = services.identity_service.list_remote_physical_node_endpoints(
        node_id,
        only_active=False,
    )
    return {
        "known_endpoint_count": len(endpoints),
        "active_endpoint_count": sum(1 for endpoint in endpoints if endpoint.is_active),
        "inactive_endpoint_count": sum(1 for endpoint in endpoints if not endpoint.is_active),
        "endpoints": [
            {
                "transport": endpoint.transport,
                "host": endpoint.host,
                "port": endpoint.port,
                "is_active": endpoint.is_active,
                "failure_count": endpoint.failure_count,
                "last_success_at": (
                    endpoint.last_success_at.isoformat()
                    if endpoint.last_success_at is not None
                    else None
                ),
                "last_failure_at": (
                    endpoint.last_failure_at.isoformat()
                    if endpoint.last_failure_at is not None
                    else None
                ),
                "metadata": endpoint.metadata_json,
            }
            for endpoint in endpoints
        ],
    }


def _is_observed_only_physical_session(session) -> bool:
    return load_json_object(session.metadata_json).get("physical_endpoint_source") == "observed"


@dataclass(slots=True, frozen=True)
class PendingDhtForward:
    previous_session_id: str
    previous_message_id: str
    created_at: float
