from __future__ import annotations

import json

from sqlalchemy import func, select

from storage.models import NodeEndpoint, RemotePhysicalNodeIdentity

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class PhysicalNodeInfoExchangeProtocolHandler(ProtocolMessageHandler):
    protocol_family = "physical_node_info_exchange"
    supported_message_types = {
        "PHYSICAL_NODE_INFO_EXCHANGE_REQUEST",
        "PHYSICAL_NODE_INFO_EXCHANGE_RESPONSE",
        "PHYSICAL_NODE_INFO_ANNOUNCE",
    }

    @staticmethod
    def build_request_payload(
        *,
        header: dict[str, object],
        max_records: int,
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": {
                    "max_records": max_records,
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")

    @staticmethod
    def build_response_payload(
        *,
        request_header: dict[str, object],
        records: list[dict[str, object]],
    ) -> bytes:
        return json.dumps(
            {
                "header": {
                    "version": request_header.get("version", 1),
                    "message_type": "PHYSICAL_NODE_INFO_EXCHANGE_RESPONSE",
                    "message_id": request_header.get("message_id"),
                    "message_sequence": request_header.get("message_sequence"),
                    "physical_session_id": request_header.get("physical_session_id"),
                    "virtual_session_id": request_header.get("virtual_session_id"),
                },
                "payload": {
                    "records": records,
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "PHYSICAL_NODE_INFO_EXCHANGE_REQUEST":
            return await self._handle_request(envelope, context, services)

        if envelope.message_type == "PHYSICAL_NODE_INFO_EXCHANGE_RESPONSE":
            return await self._handle_response(envelope, context, services)

        if envelope.message_type == "PHYSICAL_NODE_INFO_ANNOUNCE":
            return await self._handle_announce(envelope, context, services)

        return self._build_not_implemented_result(envelope, context, services)

    async def _handle_request(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        max_records = _read_max_records(payload)
        local_node = services.identity_service.get_local_physical_node_result()
        requester_node_id = _read_requester_node_id(envelope, services)

        if local_node is None:
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=False,
                message_type=envelope.message_type,
                metadata={"reason": "local_physical_node_not_initialized"},
            )

        with services.database.session_scope() as db_session:
            query = select(RemotePhysicalNodeIdentity).where(
                RemotePhysicalNodeIdentity.last_validated_at.is_not(None),
                RemotePhysicalNodeIdentity.status == "active",
            )
            query = query.order_by(func.random())

            remote_nodes = list(db_session.scalars(query).all())
            records: list[dict[str, object]] = []
            for remote_node in remote_nodes:
                if remote_node.id == local_node.id:
                    continue
                if requester_node_id is not None and remote_node.id == requester_node_id:
                    continue

                endpoint_query = (
                    select(NodeEndpoint)
                    .where(NodeEndpoint.physical_node_hash_id == remote_node.id)
                    .order_by(NodeEndpoint.priority.desc(), NodeEndpoint.last_success_at.desc())
                )
                endpoints = [endpoint for endpoint in db_session.scalars(endpoint_query).all() if endpoint.is_active]

                records.append(
                    {
                        "physical_node_id": remote_node.id,
                        "public_key": remote_node.public_key,
                        "reachability_class": remote_node.reachability_class,
                        "relay_capable": remote_node.relay_capable,
                        "hole_punch_capable": remote_node.hole_punch_capable,
                        "protocol_version": remote_node.protocol_version,
                        "status": remote_node.status,
                        "last_validated_at": _format_datetime(remote_node.last_validated_at),
                        "endpoints": [
                            {
                                "transport": endpoint.transport,
                                "host": endpoint.host,
                                "port": endpoint.port,
                                "priority": endpoint.priority,
                            }
                            for endpoint in endpoints
                        ],
                    }
                )
                if len(records) >= max_records:
                    break

        response_payload = self.build_response_payload(
            request_header=envelope.header,
            records=records,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "respond_known_physical_nodes",
                "record_count": len(records),
                "max_records": max_records,
            },
        )

    async def _handle_response(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        records = payload.get("records")
        if not isinstance(records, list):
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=False,
                message_type=envelope.message_type,
                metadata={"reason": "invalid_physical_node_info_exchange_response"},
            )

        responder_node_id = _read_requester_node_id(envelope, services)
        if responder_node_id is not None:
            services.identity_service.mark_physical_node_info_exchange_response_received(
                remote_physical_node_id=responder_node_id,
            )

        persisted_count = 0
        skipped_count = 0
        for record in records:
            if not isinstance(record, dict):
                skipped_count += 1
                continue

            physical_node_id = record.get("physical_node_id")
            public_key = record.get("public_key")
            endpoints = record.get("endpoints")
            if (
                not isinstance(physical_node_id, str)
                or not physical_node_id
                or not isinstance(public_key, str)
                or not public_key
                or not isinstance(endpoints, list)
                or not endpoints
            ):
                skipped_count += 1
                continue

            valid_endpoints = _select_valid_endpoints(endpoints)
            if not valid_endpoints:
                skipped_count += 1
                continue

            services.identity_service.upsert_discovered_remote_physical_node(
                node_id=physical_node_id,
                public_key=public_key,
                protocol_version=_optional_string(record.get("protocol_version")),
                endpoints=valid_endpoints,
                reachability_class=_optional_string(record.get("reachability_class")),
                relay_capable=bool(record.get("relay_capable", False)),
                hole_punch_capable=bool(record.get("hole_punch_capable", False)),
                notes_json=json.dumps(
                    {
                        "source": "physical_node_info_exchange_response",
                        "advertised_status": _optional_string(record.get("status")),
                        "advertised_last_validated_at": _optional_string(record.get("last_validated_at")),
                        "advertised_endpoints": endpoints,
                    },
                    separators=(",", ":"),
                ),
            )
            persisted_count += 1

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "persist_discovered_physical_nodes",
                "persisted_count": persisted_count,
                "skipped_count": skipped_count,
            },
        )

    async def _handle_announce(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        announcer_node_id = _read_requester_node_id(envelope, services)
        if announcer_node_id is not None:
            services.identity_service.mark_physical_node_info_exchange_announce_received(
                remote_physical_node_id=announcer_node_id,
            )

        return self._build_not_implemented_result(envelope, context, services)

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
                "transport_adapters": sorted(services.transport.adapters.keys()),
                "next_step": "implement_known_peer_exchange",
            },
        )


def _read_max_records(payload: dict[str, object]) -> int:
    value = payload.get("max_records")
    if not isinstance(value, int):
        return 50
    return max(1, min(value, 200))

def _read_requester_node_id(
    envelope: ProtocolEnvelope,
    services: EngineServices,
) -> str | None:
    session_id = envelope.header.get("physical_session_id")
    if not isinstance(session_id, str) or not session_id:
        return None

    network_session = services.session_manager.get_session_by_session_id(session_id)
    if network_session is None:
        return None

    return network_session.remote_identity_id


def _format_datetime(value) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _optional_string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _select_valid_endpoints(endpoints: list[object]) -> list[dict[str, object]]:
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
