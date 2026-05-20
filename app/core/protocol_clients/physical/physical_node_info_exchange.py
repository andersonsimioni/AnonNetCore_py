from __future__ import annotations

import json

from transport import OutboundMessage

from ...protocols import PhysicalNodeInfoExchangeProtocolHandler


class PhysicalNodeInfoExchangeClient:
    """Dispara trocas de informacao sobre physical nodes conhecidos."""

    def __init__(self, engine) -> None:
        self.engine = engine

    async def request_known_physical_nodes(
        self,
        *,
        session_id: str,
        max_records: int | None = None,
    ) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("A physical session informada nao existe em memoria.")
        if session.session_state != "active":
            raise ValueError("A physical session informada ainda nao esta ativa.")
        if _is_observed_only_physical_endpoint(session):
            raise ValueError("A physical session usa endpoint observado e nao deve iniciar exchange.")

        endpoint = self._build_remote_endpoint(session)
        request_limit = max_records or self.engine.services.config.physical_node_info_exchange_max_records

        header = self.engine.build_message_header(
            message_type="PHYSICAL_NODE_INFO_EXCHANGE_REQUEST",
            physical_session_id=session.session_id,
        )
        payload = PhysicalNodeInfoExchangeProtocolHandler.build_request_payload(
            header=header,
            max_records=request_limit,
        )
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        self.engine.services.identity_service.mark_physical_node_info_exchange_request_sent(
            remote_physical_node_id=session.remote_identity_id,
        )
        self.engine.services.log_service.info(
            "physical_node_info_exchange_client",
            "requested known physical nodes",
            session_id=session.session_id,
            remote_physical_node_id=session.remote_identity_id,
            max_records=request_limit,
        )

    @staticmethod
    def _build_remote_endpoint(session):
        from transport import TransportEndpoint

        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("A physical session nao possui endpoint remoto associado.")

        return TransportEndpoint(
            transport_name=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )


def _is_observed_only_physical_endpoint(session) -> bool:
    if not session.metadata_json:
        return False

    try:
        metadata = json.loads(session.metadata_json)
    except json.JSONDecodeError:
        return False

    return isinstance(metadata, dict) and metadata.get("physical_endpoint_source") == "observed"
