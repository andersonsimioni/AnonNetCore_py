from __future__ import annotations

import asyncio


class VirtualSessionClient:
    """Estabelece e mantem sessoes virtuais sobre uma rota ja criada."""

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
        local_route_path_id: str,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
        keepalive_interval_seconds: int | None = None,
    ) -> str:
        existing_session = self.engine.services.session_manager.get_active_session_by_remote_virtual_node_id(
            remote_virtual_node_id
        )
        if existing_session is not None and existing_session.bound_route_id == local_route_path_id:
            return existing_session.session_id

        local_virtual_node = self.engine.services.identity_service.get_local_virtual_node_by_id(
            local_virtual_node_id
        )
        if local_virtual_node is None:
            raise ValueError("O virtual node local informado nao existe.")

        remote_virtual_node = self.engine.services.identity_service.get_remote_virtual_node_by_id(
            remote_virtual_node_id
        )
        if remote_virtual_node is None:
            raise ValueError("O virtual node remoto informado nao existe no estado local.")

        keepalive_seconds = keepalive_interval_seconds or (
            self.engine.services.config.physical_session_keepalive_seconds
        )
        session = self.engine.services.session_manager.create_outbound_virtual_session(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
            remote_public_key=remote_virtual_node.public_key,
            bound_route_id=local_route_path_id,
            keepalive_interval_seconds=keepalive_seconds,
        )

        try:
            await self._send_virtual_envelope(
                session=session,
                message_type="VIRTUAL_SESSION_INIT",
                payload={
                    "initiator_virtual_node_id": local_virtual_node_id,
                    "target_virtual_node_id": remote_virtual_node_id,
                    "keepalive_interval_seconds": keepalive_seconds,
                },
                virtual_envelope_ciphered=False,
            )
            if await self._wait_for_activation(session.session_id):
                return session.session_id
        except Exception:
            self._close_failed_session(session.session_id)
            raise

        self._close_failed_session(session.session_id)
        raise RuntimeError("Nao foi possivel estabelecer a virtual session pela rota informada.")

    async def send_keepalive(
        self,
        *,
        session_id: str,
    ) -> None:
        session = self._require_active_session(session_id)
        await self._send_virtual_envelope(
            session=session,
            message_type="VIRTUAL_SESSION_KEEPALIVE",
            payload={},
            virtual_envelope_ciphered=True,
        )
        self.engine.services.session_manager.mark_keepalive_sent(session.session_id)

    async def close_session(
        self,
        *,
        session_id: str,
        close_reason: str = "local_closed",
    ) -> None:
        session = self._require_session(session_id)
        await self._send_virtual_envelope(
            session=session,
            message_type="VIRTUAL_SESSION_CLOSE",
            payload={
                "close_reason": close_reason,
            },
            virtual_envelope_ciphered=True,
        )

    async def _send_virtual_envelope(
        self,
        *,
        session,
        message_type: str,
        payload: dict[str, object],
        virtual_envelope_ciphered: bool,
    ) -> None:
        if not session.bound_route_id:
            raise ValueError("A virtual session nao possui rota local associada.")

        virtual_envelope = {
            "header": self.engine.build_message_header(
                message_type=message_type,
                virtual_session_id=session.session_id,
            ),
            "payload": payload,
        }
        await self.engine.services.protocol_clients.physical.route_execute.send_from_local_route(
            local_route_path_id=session.bound_route_id,
            virtual_session_id=session.session_id,
            virtual_envelope=virtual_envelope,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )

    async def _wait_for_activation(self, session_id: str) -> bool:
        deadline = asyncio.get_running_loop().time() + self._handshake_timeout_seconds

        while asyncio.get_running_loop().time() < deadline:
            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is None:
                return False
            if session.session_state == "active":
                return True
            if session.session_state == "closed":
                return False

            await asyncio.sleep(self._handshake_poll_interval_seconds)

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

    def _require_active_session(self, session_id: str):
        session = self._require_session(session_id)
        if session.session_state != "active":
            raise ValueError("A virtual session informada nao esta ativa.")
        return session

    def _require_session(self, session_id: str):
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_scope != "virtual":
            raise ValueError("A virtual session informada nao existe em memoria.")
        return session
