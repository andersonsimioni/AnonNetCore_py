from __future__ import annotations

import json
from hashlib import sha512
from uuid import uuid4

from crypto import dilithium_sign_hex

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
        requester_endpoints: list[dict[str, object]] | None = None,
        requester_status: str | None = None,
        requester_reachability_class: str | None = None,
        requester_relay_capable: bool = False,
        requester_hole_punch_capable: bool = False,
        requester_feature_flags: list[str] | None = None,
        requester_dpnt_signature: str | None = None,
    ) -> bytes:
        payload = {
            "header": header,
            "payload": {
                "requester_node_id": requester_node_id,
                "requester_public_key": requester_public_key,
                "requester_endpoints": requester_endpoints or [],
                "requester_status": requester_status,
                "requester_reachability_class": requester_reachability_class,
                "requester_relay_capable": requester_relay_capable,
                "requester_hole_punch_capable": requester_hole_punch_capable,
                "requester_feature_flags": requester_feature_flags or [],
                "requester_dpnt_signature": requester_dpnt_signature,
            },
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def sign_dpnt_descriptor(
        *,
        physical_node_public_key: str,
        endpoints: list[dict[str, object]],
        reachability_class: str,
        relay_capable: bool,
        hole_punch_capable: bool,
        protocol_version: str,
        feature_flags: list[str],
        status: str,
        private_key_pem: str,
    ) -> str:
        return _sign_dpnt_descriptor(
            physical_node_public_key=physical_node_public_key,
            endpoints=endpoints,
            reachability_class=reachability_class,
            relay_capable=relay_capable,
            hole_punch_capable=hole_punch_capable,
            protocol_version=protocol_version,
            feature_flags=feature_flags,
            status=status,
            private_key_pem=private_key_pem,
        )

    @staticmethod
    def build_response_payload(
        *,
        request_header: dict[str, object],
        physical_node_id: str,
        public_key: str,
        key_algorithm: str,
        protocol_version: str,
        status: str,
        endpoints: list[dict[str, object]],
        reachability_class: str,
        relay_capable: bool,
        hole_punch_capable: bool,
        feature_flags: list[str],
        dpnt_signature: str,
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
                "protocol_version": protocol_version,
                "status": status,
                "endpoints": endpoints,
                "reachability_class": reachability_class,
                "relay_capable": relay_capable,
                "hole_punch_capable": hole_punch_capable,
                "feature_flags": feature_flags,
                "dpnt_signature": dpnt_signature,
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
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        requester_node_id = payload.get("requester_node_id")
        self._persist_requester_if_present(envelope, context, services)
        if local_node is None:
            services.log_service.warning(
                "physical_node_info",
                "cannot answer request because local physical node is not initialized",
            )
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=False,
                message_type=envelope.message_type,
                metadata={"reason": "local_physical_node_not_initialized"},
            )

        services.log_service.info(
            "physical_node_info",
            "received physical node info request",
            requester_node_id=requester_node_id if isinstance(requester_node_id, str) else None,
            remote_host=context.remote_host,
            remote_port=context.remote_port,
        )
        advertised_endpoints = _build_response_endpoints(context, services)
        services.log_service.debug(
            "physical_node_info",
            "built local advertised endpoints for info response",
            advertised_endpoints=advertised_endpoints,
            observed_remote_host=context.remote_host,
            observed_remote_port=context.remote_port,
            local_host=context.local_host,
            local_port=context.local_port,
        )
        protocol_version = str(envelope.header.get("version", 1))
        reachability_class = "direct"
        relay_capable = False
        hole_punch_capable = False
        feature_flags: list[str] = []
        dpnt_signature = self.sign_dpnt_descriptor(
            physical_node_public_key=local_node.public_key,
            endpoints=advertised_endpoints,
            reachability_class=reachability_class,
            relay_capable=relay_capable,
            hole_punch_capable=hole_punch_capable,
            protocol_version=protocol_version,
            feature_flags=feature_flags,
            status=local_node.status,
            private_key_pem=local_node.private_key_pem,
        )
        response_payload = self.build_response_payload(
            request_header=envelope.header,
            physical_node_id=local_node.id,
            public_key=local_node.public_key,
            key_algorithm=local_node.key_algorithm,
            protocol_version=protocol_version,
            status=local_node.status,
            endpoints=advertised_endpoints,
            reachability_class=reachability_class,
            relay_capable=relay_capable,
            hole_punch_capable=hole_punch_capable,
            feature_flags=feature_flags,
            dpnt_signature=dpnt_signature,
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

    @staticmethod
    def _persist_requester_if_present(
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> None:
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        requester_node_id = payload.get("requester_node_id")
        requester_public_key = payload.get("requester_public_key")
        requester_endpoints = _select_valid_endpoints(payload.get("requester_endpoints"))
        requester_status = payload.get("requester_status")
        requester_reachability_class = payload.get("requester_reachability_class")
        requester_relay_capable = payload.get("requester_relay_capable", False)
        requester_hole_punch_capable = payload.get("requester_hole_punch_capable", False)
        requester_feature_flags = payload.get("requester_feature_flags")
        requester_dpnt_signature = payload.get("requester_dpnt_signature")
        if (
            not isinstance(requester_node_id, str)
            or not requester_node_id
            or not isinstance(requester_public_key, str)
            or not requester_public_key
            or not requester_endpoints
        ):
            services.log_service.debug(
                "physical_node_info",
                "skipped requester persistence from invalid request descriptor",
                requester_node_id=requester_node_id if isinstance(requester_node_id, str) else None,
                has_public_key=isinstance(requester_public_key, str) and bool(requester_public_key),
                requester_endpoint_count=len(requester_endpoints),
                observed_remote_host=context.remote_host,
                observed_remote_port=context.remote_port,
            )
            return

        services.log_service.debug(
            "physical_node_info",
            "persisting requester advertised endpoints from request",
            requester_node_id=requester_node_id,
            requester_endpoints=requester_endpoints,
            observed_remote_host=context.remote_host,
            observed_remote_port=context.remote_port,
        )
        services.identity_service.upsert_discovered_remote_physical_node(
            node_id=requester_node_id,
            public_key=requester_public_key,
            protocol_version=(
                str(envelope.header.get("version"))
                if envelope.header.get("version") is not None
                else None
            ),
            endpoints=requester_endpoints,
            status="discovered",
            reachability_class=(
                requester_reachability_class
                if isinstance(requester_reachability_class, str) and requester_reachability_class
                else None
            ),
            relay_capable=bool(requester_relay_capable),
            hole_punch_capable=bool(requester_hole_punch_capable),
            notes_json=json.dumps(
                {
                    "source": "physical_node_info_request",
                    "advertised_status": (
                        requester_status
                        if isinstance(requester_status, str) and requester_status
                        else None
                    ),
                    "dpnt_signature": (
                        requester_dpnt_signature
                        if isinstance(requester_dpnt_signature, str)
                        else None
                    ),
                    "dpnt_feature_flags": (
                        requester_feature_flags
                        if isinstance(requester_feature_flags, list)
                        else []
                    ),
                },
                separators=(",", ":"),
            ),
        )
        services.log_service.info(
            "physical_node_info",
            "persisted requester physical node from request",
            requester_node_id=requester_node_id,
            endpoint_count=len(requester_endpoints),
            endpoints=requester_endpoints,
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
        protocol_version = payload.get("protocol_version")
        advertised_status = payload.get("status", "active")
        endpoints = payload.get("endpoints")
        reachability_class = payload.get("reachability_class")
        relay_capable = payload.get("relay_capable", False)
        hole_punch_capable = payload.get("hole_punch_capable", False)
        feature_flags = payload.get("feature_flags")
        dpnt_signature = payload.get("dpnt_signature")

        valid_endpoints = _select_valid_endpoints(endpoints)
        services.log_service.debug(
            "physical_node_info",
            "received physical node info response descriptor",
            remote_node_id=remote_node_id if isinstance(remote_node_id, str) else None,
            advertised_endpoint_count=len(endpoints) if isinstance(endpoints, list) else None,
            valid_endpoint_count=len(valid_endpoints),
            valid_endpoints=valid_endpoints,
            observed_remote_host=context.remote_host,
            observed_remote_port=context.remote_port,
        )
        if (
            not isinstance(remote_node_id, str)
            or not isinstance(remote_public_key, str)
            or not valid_endpoints
        ):
            services.log_service.warning(
                "physical_node_info",
                "received invalid physical node info response",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
            )
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=False,
                message_type=envelope.message_type,
                metadata={"reason": "invalid_physical_node_info_response"},
            )

        services.identity_service.upsert_remote_physical_node(
            node_id=remote_node_id,
            public_key=remote_public_key,
            protocol_version=protocol_version if isinstance(protocol_version, str) else (
                str(envelope.header.get("version")) if envelope.header.get("version") is not None else None
            ),
            status="discovered",
            endpoints=valid_endpoints,
            reachability_class=reachability_class if isinstance(reachability_class, str) else None,
            relay_capable=bool(relay_capable),
            hole_punch_capable=bool(hole_punch_capable),
            notes_json=json.dumps(
                {
                    "key_algorithm": key_algorithm,
                    "advertised_status": advertised_status,
                    "advertised_endpoints": endpoints,
                    "dpnt_signature": dpnt_signature,
                    "dpnt_reachability_class": reachability_class,
                    "dpnt_relay_capable": bool(relay_capable),
                    "dpnt_hole_punch_capable": bool(hole_punch_capable),
                    "dpnt_protocol_version": protocol_version,
                    "dpnt_status": advertised_status,
                    "dpnt_feature_flags": feature_flags if isinstance(feature_flags, list) else [],
                },
                separators=(",", ":"),
            ),
        )
        services.log_service.info(
            "physical_node_info",
            "persisted remote physical node info",
            remote_node_id=remote_node_id,
            endpoint_count=len(valid_endpoints),
            endpoints=valid_endpoints,
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


def _build_response_endpoints(
    context: PacketContext,
    services: EngineServices,
) -> list[dict[str, object]]:
    advertised_host = (
        services.engine.get_advertised_tcp_host()
        if services.engine is not None
        else context.local_host
    )
    advertised_port = (
        services.engine.get_advertised_tcp_port()
        if services.engine is not None
        else context.local_port
    )
    if not advertised_host or advertised_port is None:
        return []

    return [
        {
            "transport": context.transport_name,
            "host": advertised_host,
            "port": advertised_port,
            "priority": 0,
        }
    ]


def _sign_dpnt_descriptor(
    *,
    physical_node_public_key: str,
    endpoints: list[dict[str, object]],
    reachability_class: str,
    relay_capable: bool,
    hole_punch_capable: bool,
    protocol_version: str,
    feature_flags: list[str],
    status: str,
    private_key_pem: str,
) -> str:
    physical_node_id = sha512(physical_node_public_key.encode("utf-8")).hexdigest()
    key_hex = sha512(f"dpnt|{physical_node_id}".encode("utf-8")).hexdigest()
    signed_payload = {
        "key": key_hex,
        "pk_physical_node": physical_node_public_key,
        "endpoints": endpoints,
        "transport_methods": sorted({endpoint["transport"] for endpoint in endpoints}),
        "reachability_class": reachability_class,
        "relay_capable": relay_capable,
        "hole_punch_capable": hole_punch_capable,
        "protocol_version": protocol_version,
        "feature_flags": feature_flags,
        "status": status,
    }
    payload_hex = json.dumps(
        signed_payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8").hex()
    return dilithium_sign_hex(payload_hex, private_key_pem)


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
