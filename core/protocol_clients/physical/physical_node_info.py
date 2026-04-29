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
        header = self.engine.build_message_header(message_type="PHYSICAL_NODE_INFO_REQUEST")
        payload = PhysicalNodeInfoProtocolHandler.build_request_payload(
            header=header,
            requester_node_id=local_node.id if local_node else None,
            requester_public_key=local_node.public_key if local_node else None,
        )
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
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
