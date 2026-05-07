from __future__ import annotations

from bootstrap import BootstrapEndpoint
from transport import OutboundMessage, TransportEndpoint

from ...protocols import PhysicalNodeInfoProtocolHandler


class PhysicalNodeInfoClient:
    """Inicia fluxos ativos do protocolo de physical node info."""

    def __init__(self, engine) -> None:
        self.engine = engine

    async def send_request_to_endpoint(
        self,
        endpoint: TransportEndpoint,
    ) -> None:
        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        descriptor = self._build_local_dpnt_descriptor(
            transport_name=endpoint.transport_name,
            local_node=local_node,
        )
        header = self.engine.build_message_header(message_type="PHYSICAL_NODE_INFO_REQUEST")
        payload = PhysicalNodeInfoProtocolHandler.build_request_payload(
            header=header,
            requester_node_id=local_node.id if local_node else None,
            requester_public_key=local_node.public_key if local_node else None,
            requester_endpoints=self._build_requester_endpoints(endpoint.transport_name),
            requester_status=descriptor["status"] if descriptor else None,
            requester_reachability_class=descriptor["reachability_class"] if descriptor else None,
            requester_relay_capable=descriptor["relay_capable"] if descriptor else False,
            requester_hole_punch_capable=descriptor["hole_punch_capable"] if descriptor else False,
            requester_feature_flags=descriptor["feature_flags"] if descriptor else [],
            requester_dpnt_signature=descriptor["dpnt_signature"] if descriptor else None,
        )
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        self.engine.services.log_service.info(
            "physical_node_info_client",
            "sent physical node info request",
            target_transport=endpoint.transport_name,
            target_host=endpoint.host,
            target_port=endpoint.port,
            requester_node_id=local_node.id if local_node else None,
        )

    async def send_request_to_bootstrap_endpoint(
        self,
        endpoint: BootstrapEndpoint,
    ) -> None:
        await self.send_request_to_endpoint(
            TransportEndpoint(
                transport_name=endpoint.transport,
                host=endpoint.host,
                port=endpoint.port,
            )
        )

    def _build_requester_endpoints(
        self,
        transport_name: str,
    ) -> list[dict[str, object]]:
        advertised_host = self.engine.services.config.advertised_tcp_host
        advertised_port = self.engine.services.config.advertised_tcp_port
        if not advertised_host or advertised_port is None:
            return []

        return [
            {
                "transport": transport_name,
                "host": advertised_host,
                "port": advertised_port,
                "priority": 0,
            }
        ]

    def _build_local_dpnt_descriptor(
        self,
        *,
        transport_name: str,
        local_node,
    ) -> dict[str, object] | None:
        if local_node is None:
            return None

        endpoints = self._build_requester_endpoints(transport_name)
        if not endpoints:
            return None

        protocol_version = "1"
        reachability_class = "direct"
        relay_capable = False
        hole_punch_capable = False
        feature_flags: list[str] = []
        dpnt_signature = PhysicalNodeInfoProtocolHandler.sign_dpnt_descriptor(
            physical_node_public_key=local_node.public_key,
            endpoints=endpoints,
            reachability_class=reachability_class,
            relay_capable=relay_capable,
            hole_punch_capable=hole_punch_capable,
            protocol_version=protocol_version,
            feature_flags=feature_flags,
            status=local_node.status,
            private_key_pem=local_node.private_key_pem,
        )
        return {
            "status": local_node.status,
            "reachability_class": reachability_class,
            "relay_capable": relay_capable,
            "hole_punch_capable": hole_punch_capable,
            "feature_flags": feature_flags,
            "dpnt_signature": dpnt_signature,
        }
