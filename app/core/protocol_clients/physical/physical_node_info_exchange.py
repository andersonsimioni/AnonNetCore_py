from __future__ import annotations

from sessions import build_remote_endpoint_from_session, is_observed_only_physical_endpoint
from transport import OutboundMessage

from ...components import EngineBoundComponent
from ...protocols import PhysicalNodeInfoExchangeProtocolHandler


class PhysicalNodeInfoExchangeClient(EngineBoundComponent):
    """Dispara trocas de informacao sobre physical nodes conhecidos."""

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
        if is_observed_only_physical_endpoint(session):
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
        return build_remote_endpoint_from_session(session)
