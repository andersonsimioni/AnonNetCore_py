from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from storage.models import DhtRecord

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class DhtProtocolHandler(ProtocolMessageHandler):
    protocol_family = "dht"
    supported_message_types = {
        "DHT_PUBLISH",
        "DHT_QUERY",
        "DHT_RESULT",
    }

    @staticmethod
    def build_query_payload(
        *,
        header: dict[str, object],
        namespace: str,
        logical_key: str,
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": {
                    "namespace": namespace,
                    "logical_key": logical_key,
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
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": {
                    "namespace": namespace,
                    "logical_key": logical_key,
                    "record_json": record_json,
                    "expires_at": expires_at,
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
    ) -> bytes:
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

        if namespace is None or logical_key is None or record_json is None:
            services.log_service.warning(
                "dht",
                "received invalid dht publish payload",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
            )
            return self._build_invalid_result(envelope, "invalid_dht_publish_payload")

        key_hex = services.dht_service.build_key(namespace, logical_key)
        closest_nodes_result = services.dht_service.select_k_closest_nodes(key_hex)
        responsible_nodes = closest_nodes_result["nodes"]

        if not closest_nodes_result["local_node_is_responsible"]:
            services.log_service.info(
                "dht",
                "publish request not local, returning closest nodes",
                key=key_hex,
                responsible_node_count=len(responsible_nodes),
            )
            response_payload = self.build_result_payload(
                request_header=envelope.header,
                status="closest_nodes",
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                stored_locally=False,
            )
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=True,
                message_type=envelope.message_type,
                response_payload=response_payload,
                metadata={
                    "protocol_family": self.protocol_family,
                    "transport_name": context.transport_name,
                    "action": "return_closest_dht_nodes",
                    "key": key_hex,
                },
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
        services.log_service.info(
            "dht",
            "stored dht record locally from publish",
            key=key_hex,
            namespace=namespace,
            logical_key=logical_key,
        )

        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status="stored",
            key_hex=key_hex,
            responsible_nodes=responsible_nodes,
            stored_locally=True,
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

        if not closest_nodes_result["local_node_is_responsible"]:
            services.log_service.info(
                "dht",
                "query request not local, returning closest nodes",
                key=key_hex,
                responsible_node_count=len(responsible_nodes),
            )
            response_payload = self.build_result_payload(
                request_header=envelope.header,
                status="closest_nodes",
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                stored_locally=False,
            )
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=True,
                message_type=envelope.message_type,
                response_payload=response_payload,
                metadata={
                    "protocol_family": self.protocol_family,
                    "transport_name": context.transport_name,
                    "action": "return_closest_dht_nodes",
                    "key": key_hex,
                },
            )

        dht_record = self._load_validated_local_record(
            services=services,
            key_hex=key_hex,
        )
        if dht_record is None:
            services.log_service.info(
                "dht",
                "dht record not found locally",
                key=key_hex,
            )
            response_payload = self.build_result_payload(
                request_header=envelope.header,
                status="not_found",
                key_hex=key_hex,
                responsible_nodes=responsible_nodes,
                stored_locally=False,
            )
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=True,
                message_type=envelope.message_type,
                response_payload=response_payload,
                metadata={
                    "protocol_family": self.protocol_family,
                    "transport_name": context.transport_name,
                    "action": "return_dht_not_found",
                    "key": key_hex,
                },
            )

        response_payload = self.build_result_payload(
            request_header=envelope.header,
            status="found",
            key_hex=key_hex,
            responsible_nodes=responsible_nodes,
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
        if isinstance(response_to_message_id, str) and services.protocol_clients is not None:
            services.protocol_clients.physical.dht.complete_result(
                response_to_message_id=response_to_message_id,
                result_data={
                    "status": payload.get("status"),
                    "key": payload.get("key"),
                    "stored_locally": payload.get("stored_locally"),
                    "responsible_nodes": payload.get("responsible_nodes", []),
                    "record_json": payload.get("record_json"),
                    "expires_at": payload.get("expires_at"),
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

    @staticmethod
    def _upsert_local_record(
        *,
        services: EngineServices,
        key_hex: str,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: datetime | None,
        source: str,
    ) -> None:
        with services.database.session_scope() as session:
            dht_record = DhtRecord(
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                record_json=record_json,
                source=source,
                last_validated_at=datetime.now(timezone.utc),
                expires_at=expires_at,
            )
            session.add(dht_record)

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

    def _build_invalid_result(
        self,
        envelope: ProtocolEnvelope,
        reason: str,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "reason": reason,
            },
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


def _as_payload_dict(envelope: ProtocolEnvelope) -> dict[str, object]:
    return envelope.payload if isinstance(envelope.payload, dict) else {}


def _read_required_string(payload: dict[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    if isinstance(value, str) and value:
        return value
    return None


def _read_optional_datetime(payload: dict[str, object], field_name: str) -> datetime | None:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None
