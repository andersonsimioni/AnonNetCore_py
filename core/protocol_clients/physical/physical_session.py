from __future__ import annotations

import asyncio

from transport import OutboundMessage, TransportEndpoint

from ...protocols import SessionProtocolHandler


class PhysicalSessionClient:
    """Inicia e mantem sessoes fisicas entre peers adjacentes."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._handshake_timeout_seconds = (
            self.engine.services.config.physical_session_handshake_timeout_seconds
        )
        self._handshake_poll_interval_seconds = (
            self.engine.services.config.physical_session_handshake_poll_interval_seconds
        )

    async def start_session(
        self,
        *,
        remote_physical_node_id: str,
    ) -> str:
        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        if local_node is None:
            raise ValueError("A identidade fisica local ainda nao foi inicializada.")

        remote_node = self.engine.services.identity_service.get_remote_physical_node_by_id(remote_physical_node_id)
        if remote_node is None:
            raise ValueError("O physical node remoto ainda nao foi persistido no banco local.")

        endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
            remote_physical_node_id
        )
        if not endpoints:
            raise ValueError("O physical node remoto nao possui endpoints conhecidos.")

        for endpoint_data in endpoints:
            endpoint = TransportEndpoint(
                transport_name=endpoint_data.transport,
                host=endpoint_data.host,
                port=endpoint_data.port,
            )
            self.engine.services.log_service.info(
                "physical_session_client",
                "trying to establish physical session",
                remote_physical_node_id=remote_physical_node_id,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
            )
            session_id = await self._start_session_with_endpoint(
                local_physical_node_id=local_node.id,
                remote_physical_node_id=remote_physical_node_id,
                remote_public_key=remote_node.public_key,
                endpoint=endpoint,
            )
            if session_id is None:
                continue

            if await self._wait_for_activation(session_id):
                self.engine.services.log_service.info(
                    "physical_session_client",
                    "physical session established",
                    session_id=session_id,
                    remote_physical_node_id=remote_physical_node_id,
                )
                return session_id

            self._register_endpoint_failure(
                remote_physical_node_id=remote_physical_node_id,
                endpoint=endpoint,
            )
            self._close_failed_session(session_id)

        raise RuntimeError("Nao foi possivel estabelecer physical session com nenhum endpoint conhecido.")

    async def send_keepalive(
        self,
        *,
        session_id: str,
    ) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("A physical session informada nao existe em memoria.")

        endpoint = self._build_remote_endpoint(session)
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_KEEPALIVE",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_keepalive_payload(header=header)
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        self.engine.services.session_manager.mark_keepalive_sent(session.session_id)
        self.engine.services.log_service.debug(
            "physical_session_client",
            "sent physical session keepalive",
            session_id=session.session_id,
        )

    async def close_session(
        self,
        *,
        session_id: str,
        close_reason: str = "local_closed",
    ) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("A physical session informada nao existe em memoria.")

        endpoint = self._build_remote_endpoint(session)
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_CLOSE",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_close_payload(
            header=header,
            close_reason=close_reason,
        )
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=payload,
                remote_endpoint=endpoint,
            )
        )
        self.engine.services.log_service.info(
            "physical_session_client",
            "sent physical session close",
            session_id=session.session_id,
            close_reason=close_reason,
        )

    async def _start_session_with_endpoint(
        self,
        *,
        local_physical_node_id: str,
        remote_physical_node_id: str,
        remote_public_key: str,
        endpoint: TransportEndpoint,
    ) -> str | None:
        keepalive_interval_seconds = self.engine.services.config.physical_session_keepalive_seconds
        session = self.engine.services.session_manager.create_outbound_physical_session(
            local_physical_node_id=local_physical_node_id,
            remote_physical_node_id=remote_physical_node_id,
            remote_public_key=remote_public_key,
            transport=endpoint.transport_name,
            remote_host=endpoint.host,
            remote_port=endpoint.port,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )
        header = self.engine.build_message_header(
            message_type="PHYSICAL_SESSION_INIT",
            physical_session_id=session.session_id,
        )
        payload = SessionProtocolHandler.build_physical_session_init_payload(
            header=header,
            initiator_physical_node_id=local_physical_node_id,
            keepalive_interval_seconds=keepalive_interval_seconds,
        )

        try:
            await self.engine.send_packet(
                OutboundMessage(
                    transport_name=endpoint.transport_name,
                    payload=payload,
                    remote_endpoint=endpoint,
                )
            )
        except Exception:
            self.engine.services.log_service.warning(
                "physical_session_client",
                "failed to send physical session init",
                remote_physical_node_id=remote_physical_node_id,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
            )
            self._register_endpoint_failure(
                remote_physical_node_id=remote_physical_node_id,
                endpoint=endpoint,
            )
            self._close_failed_session(session.session_id)
            return None

        self.engine.services.log_service.info(
            "physical_session_client",
            "sent physical session init",
            session_id=session.session_id,
            remote_physical_node_id=remote_physical_node_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
        )
        return session.session_id

    async def _wait_for_activation(self, session_id: str) -> bool:
        deadline = asyncio.get_running_loop().time() + self._handshake_timeout_seconds

        while asyncio.get_running_loop().time() < deadline:
            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is None:
                self.engine.services.log_service.warning(
                    "physical_session_client",
                    "session disappeared while waiting for activation",
                    session_id=session_id,
                )
                return False
            if session.session_state == "active":
                return True
            if session.session_state == "closed":
                self.engine.services.log_service.warning(
                    "physical_session_client",
                    "session closed before activation",
                    session_id=session_id,
                )
                return False

            await asyncio.sleep(self._handshake_poll_interval_seconds)

        self.engine.services.log_service.warning(
            "physical_session_client",
            "session activation timed out",
            session_id=session_id,
            timeout_seconds=self._handshake_timeout_seconds,
        )
        return False

    def _close_failed_session(self, session_id: str) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            return

        if session.session_state != "active":
            self.engine.services.session_manager.close_session(
                session_id,
                close_reason="handshake_failed",
            )

    def _register_endpoint_failure(
        self,
        *,
        remote_physical_node_id: str,
        endpoint: TransportEndpoint,
    ) -> None:
        self.engine.services.identity_service.mark_remote_physical_node_validation_failure(
            node_id=remote_physical_node_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
        )
        self.engine.services.log_service.warning(
            "physical_session_client",
            "registered endpoint failure",
            remote_physical_node_id=remote_physical_node_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
        )

    @staticmethod
    def _build_remote_endpoint(session) -> TransportEndpoint:
        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("A physical session nao possui endpoint remoto associado.")

        return TransportEndpoint(
            transport_name=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )
