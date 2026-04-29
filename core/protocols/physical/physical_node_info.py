from __future__ import annotations

import json
from uuid import uuid4

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class PhysicalNodeInfoProtocolHandler(ProtocolMessageHandler):
    protocol_family = "physical_node_info"
    supported_message_types = {
        "PHYSICAL_NODE_INFO_REQUEST",
        "PHYSICAL_NODE_INFO_RESPONSE",
    }

    @staticmethod
    def build_request_payload(
        *,
        header: dict[str, object],
        requester_node_id: str | None,
        requester_public_key: str | None,
    ) -> bytes:
        payload = {
            "header": header,
            "payload": {
                "requester_node_id": requester_node_id,
                "requester_public_key": requester_public_key,
            },
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def build_response_payload(
        *,
        request_header: dict[str, object],
        physical_node_id: str,
        public_key: str,
        key_algorithm: str,
        status: str,
        endpoints: list[dict[str, object]],
    ) -> bytes:
        payload = {
            "header": {
                "version": request_header.get("version", 1),
                "message_type": "PHYSICAL_NODE_INFO_RESPONSE",
                "message_id": str(uuid4()),
                "message_sequence": request_header.get("message_sequence"),
                "physical_session_id": request_header.get("physical_session_id"),
                "virtual_session_id": request_header.get("virtual_session_id"),
            },
            "payload": {
                "physical_node_id": physical_node_id,
                "public_key": public_key,
                "key_algorithm": key_algorithm,
                "status": status,
                "endpoints": endpoints,
            },
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "PHYSICAL_NODE_INFO_REQUEST":
            return await self._handle_request(envelope, context, services)

        if envelope.message_type == "PHYSICAL_NODE_INFO_RESPONSE":
            return await self._handle_response(envelope, context, services)

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "database_path": str(services.database.config.db_path),
                "next_step": "implement_physical_node_identity_resolution",
            },
        )

    async def _handle_request(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        local_node = services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=False,
                message_type=envelope.message_type,
                metadata={"reason": "local_physical_node_not_initialized"},
            )

        response_payload = self.build_response_payload(
            request_header=envelope.header,
            physical_node_id=local_node.id,
            public_key=local_node.public_key,
            key_algorithm=local_node.key_algorithm,
            status=local_node.status,
            endpoints=_build_response_endpoints(context),
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "respond_local_physical_node_info",
            },
        )

    async def _handle_response(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        remote_node_id = payload.get("physical_node_id")
        remote_public_key = payload.get("public_key")
        key_algorithm = payload.get("key_algorithm")
        status = payload.get("status", "active")
        endpoints = payload.get("endpoints")

        valid_endpoints = _select_valid_endpoints(endpoints)
        if (
            not isinstance(remote_node_id, str)
            or not isinstance(remote_public_key, str)
            or not valid_endpoints
        ):
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=False,
                message_type=envelope.message_type,
                metadata={"reason": "invalid_physical_node_info_response"},
            )

        services.identity_service.upsert_remote_physical_node(
            node_id=remote_node_id,
            public_key=remote_public_key,
            protocol_version=str(envelope.header.get("version")) if envelope.header.get("version") is not None else None,
            status=status,
            endpoints=valid_endpoints,
            notes_json=json.dumps(
                {
                    "key_algorithm": key_algorithm,
                    "advertised_endpoints": endpoints,
                },
                separators=(",", ":"),
            ),
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "persist_remote_physical_node_info",
                "remote_node_id": remote_node_id,
            },
        )


def _build_response_endpoints(context: PacketContext) -> list[dict[str, object]]:
    if not context.local_host or context.local_port is None:
        return []

    return [
        {
            "transport": context.transport_name,
            "host": context.local_host,
            "port": context.local_port,
            "priority": 0,
        }
    ]


def _select_valid_endpoints(endpoints: object) -> list[dict[str, object]]:
    if not isinstance(endpoints, list):
        return []

    valid_endpoints: list[dict[str, object]] = []
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            continue

        transport = endpoint.get("transport")
        host = endpoint.get("host")
        port = endpoint.get("port")
        if isinstance(transport, str) and transport and isinstance(host, str) and host and isinstance(port, int):
            priority = endpoint.get("priority", 0)
            valid_endpoints.append(
                {
                    "transport": transport,
                    "host": host,
                    "port": port,
                    "priority": priority if isinstance(priority, int) else 0,
                }
            )

    return valid_endpoints
