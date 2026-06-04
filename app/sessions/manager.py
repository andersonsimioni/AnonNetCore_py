from __future__ import annotations

from datetime import datetime, timedelta
from inspect import isawaitable
from uuid import uuid4

from .messages import (
    VirtualSessionMessage,
    VirtualSessionMessageHandler,
    VirtualSessionMessageReply,
)
from .models import NetworkSession, SessionCreateInput, SessionStateUpdateInput, utc_now
from .reliable import (
    ReliableOutboundMessage,
    ReliableReceiveResult,
    ReliableSessionState,
    build_reliable_outbound,
    receive_reliable_payload,
)


class SessionManager:
    """Gerenciador de sessoes de rede mantidas apenas em memoria."""

    def __init__(self) -> None:
        self._sessions_by_id: dict[int, NetworkSession] = {}
        self._sessions_by_session_id: dict[str, NetworkSession] = {}
        self._virtual_message_handlers: dict[str, VirtualSessionMessageHandler] = {}
        self._reliable_states_by_session_id: dict[str, ReliableSessionState] = {}
        self._next_id = 1

    def register_virtual_message_handler(
        self,
        app_message_type: str,
        handler: VirtualSessionMessageHandler,
    ) -> None:
        if not app_message_type:
            raise ValueError("app_message_type cannot be empty.")
        if not callable(handler):
            raise TypeError("handler precisa ser chamavel.")
        self._virtual_message_handlers[app_message_type] = handler

    def unregister_virtual_message_handler(self, app_message_type: str) -> None:
        self._virtual_message_handlers.pop(app_message_type, None)

    def has_virtual_message_handler(self, app_message_type: str) -> bool:
        return app_message_type in self._virtual_message_handlers

    async def handle_virtual_message(
        self,
        message: VirtualSessionMessage,
    ) -> VirtualSessionMessageReply | None:
        handler = self._virtual_message_handlers.get(message.app_message_type)
        if handler is None:
            return None

        result = handler(message)
        if isawaitable(result):
            result = await result

        if result is not None and not isinstance(result, VirtualSessionMessageReply):
            raise TypeError(
                "Virtual session message handler precisa retornar "
                "VirtualSessionMessageReply ou None."
            )
        if result is not None and not isinstance(result.payload, dict):
            raise TypeError("VirtualSessionMessageReply.payload precisa ser um objeto.")

        return result

    def create_session(self, data: SessionCreateInput) -> NetworkSession:
        existing_session = self._sessions_by_session_id.get(data.session_id)
        if existing_session is not None:
            raise ValueError(f"Session '{data.session_id}' ja existe em memoria.")

        session = NetworkSession(
            id=self._next_id,
            session_id=data.session_id,
            session_scope=data.session_scope,
            local_identity_type=data.local_identity_type,
            local_identity_id=data.local_identity_id,
            remote_identity_type=data.remote_identity_type,
            remote_identity_id=data.remote_identity_id,
            remote_host=data.remote_host,
            remote_port=data.remote_port,
            local_endpoint_id=data.local_endpoint_id,
            remote_endpoint_id=data.remote_endpoint_id,
            transport=data.transport,
            direction=data.direction,
            initiator_side=data.initiator_side,
            handshake_state=data.handshake_state,
            session_state=data.session_state,
            key_exchange_algorithm=data.key_exchange_algorithm,
            signature_algorithm=data.signature_algorithm,
            symmetric_algorithm=data.symmetric_algorithm,
            hash_algorithm=data.hash_algorithm,
            remote_public_key=data.remote_public_key,
            local_ephemeral_private_key=data.local_ephemeral_private_key,
            local_ephemeral_public_key=data.local_ephemeral_public_key,
            remote_ephemeral_public_key=data.remote_ephemeral_public_key,
            shared_secret_hex=data.shared_secret_hex,
            session_key_id=data.session_key_id,
            established_at=data.established_at,
            last_activity_at=data.last_activity_at or utc_now(),
            last_keepalive_sent_at=data.last_keepalive_sent_at,
            keepalive_interval_seconds=data.keepalive_interval_seconds,
            keepalive_deadline=data.keepalive_deadline,
            expires_at=data.expires_at,
            closed_at=data.closed_at,
            close_reason=data.close_reason,
            bound_route_id=data.bound_route_id,
            metadata_json=data.metadata_json,
        )
        self._sessions_by_id[session.id] = session
        self._sessions_by_session_id[session.session_id] = session
        self._next_id += 1
        return session

    def get_session_by_id(self, db_id: int) -> NetworkSession | None:
        return self._sessions_by_id.get(db_id)

    def get_session_by_session_id(self, session_id: str) -> NetworkSession | None:
        return self._sessions_by_session_id.get(session_id)

    def create_outbound_physical_session(
        self,
        *,
        local_physical_node_id: str,
        remote_physical_node_id: str,
        remote_public_key: str | None,
        transport: str,
        remote_host: str,
        remote_port: int,
        keepalive_interval_seconds: int = 30,
    ) -> NetworkSession:
        session_id = str(uuid4())
        return self.create_session(
            SessionCreateInput(
                session_id=session_id,
                session_scope="physical",
                local_identity_type="physical_node",
                local_identity_id=local_physical_node_id,
                remote_identity_type="physical_node",
                remote_identity_id=remote_physical_node_id,
                direction="outbound",
                initiator_side="initiator",
                handshake_state="init_sent",
                session_state="pending",
                transport=transport,
                remote_host=remote_host,
                remote_port=remote_port,
                key_exchange_algorithm="ml-kem-768",
                signature_algorithm="ml-dsa-65",
                symmetric_algorithm="aes-256-gcm-siv",
                hash_algorithm="sha512",
                remote_public_key=remote_public_key,
                keepalive_interval_seconds=keepalive_interval_seconds,
                keepalive_deadline=self._build_keepalive_deadline(keepalive_interval_seconds),
            )
        )

    def create_inbound_physical_session(
        self,
        *,
        session_id: str,
        local_physical_node_id: str,
        remote_physical_node_id: str,
        remote_public_key: str | None,
        transport: str,
        remote_host: str | None,
        remote_port: int | None,
        keepalive_interval_seconds: int = 30,
    ) -> NetworkSession:
        existing_session = self.get_session_by_session_id(session_id)
        if existing_session is not None:
            self.bind_remote_endpoint(
                session_id,
                transport=transport,
                host=remote_host,
                port=remote_port,
            )
            return existing_session

        return self.create_session(
            SessionCreateInput(
                session_id=session_id,
                session_scope="physical",
                local_identity_type="physical_node",
                local_identity_id=local_physical_node_id,
                remote_identity_type="physical_node",
                remote_identity_id=remote_physical_node_id,
                direction="inbound",
                initiator_side="responder",
                handshake_state="init_received",
                session_state="pending",
                transport=transport,
                remote_host=remote_host,
                remote_port=remote_port,
                key_exchange_algorithm="ml-kem-768",
                signature_algorithm="ml-dsa-65",
                symmetric_algorithm="aes-256-gcm-siv",
                hash_algorithm="sha512",
                remote_public_key=remote_public_key,
                keepalive_interval_seconds=keepalive_interval_seconds,
                keepalive_deadline=self._build_keepalive_deadline(keepalive_interval_seconds),
            )
        )

    def create_outbound_virtual_session(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
        remote_public_key: str | None,
        bound_route_id: str | None = None,
        keepalive_interval_seconds: int = 30,
    ) -> NetworkSession:
        session_id = str(uuid4())
        return self.create_session(
            SessionCreateInput(
                session_id=session_id,
                session_scope="virtual",
                local_identity_type="virtual_node",
                local_identity_id=local_virtual_node_id,
                remote_identity_type="virtual_node",
                remote_identity_id=remote_virtual_node_id,
                direction="outbound",
                initiator_side="initiator",
                handshake_state="init_sent",
                session_state="pending",
                key_exchange_algorithm="ml-kem-768",
                signature_algorithm="ml-dsa-65",
                symmetric_algorithm="aes-256-gcm-siv",
                hash_algorithm="sha512",
                remote_public_key=remote_public_key,
                bound_route_id=bound_route_id,
                keepalive_interval_seconds=keepalive_interval_seconds,
                keepalive_deadline=self._build_keepalive_deadline(keepalive_interval_seconds),
            )
        )

    def create_inbound_virtual_session(
        self,
        *,
        session_id: str,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
        remote_public_key: str | None,
        bound_route_id: str | None = None,
        keepalive_interval_seconds: int = 30,
    ) -> NetworkSession:
        existing_session = self.get_session_by_session_id(session_id)
        if existing_session is not None:
            # A same-core VN-to-VN flow can receive the inbound side of the
            # handshake in the same process that created the outbound session.
            # Keep the outbound route binding intact; it is the route the
            # initiator must use to send the next handshake packet back.
            return existing_session

        return self.create_session(
            SessionCreateInput(
                session_id=session_id,
                session_scope="virtual",
                local_identity_type="virtual_node",
                local_identity_id=local_virtual_node_id,
                remote_identity_type="virtual_node",
                remote_identity_id=remote_virtual_node_id,
                direction="inbound",
                initiator_side="responder",
                handshake_state="init_received",
                session_state="pending",
                key_exchange_algorithm="ml-kem-768",
                signature_algorithm="ml-dsa-65",
                symmetric_algorithm="aes-256-gcm-siv",
                hash_algorithm="sha512",
                remote_public_key=remote_public_key,
                bound_route_id=bound_route_id,
                keepalive_interval_seconds=keepalive_interval_seconds,
                keepalive_deadline=self._build_keepalive_deadline(keepalive_interval_seconds),
            )
        )

    def list_sessions(self, *, session_scope: str | None = None) -> list[NetworkSession]:
        sessions = list(self._sessions_by_id.values())
        if session_scope is not None:
            sessions = [session for session in sessions if session.session_scope == session_scope]
        return sorted(sessions, key=lambda session: session.id)

    def list_active_sessions(self) -> list[NetworkSession]:
        return [
            session
            for session in self.list_sessions()
            if session.session_state == "active"
        ]

    def list_active_physical_sessions(self) -> list[NetworkSession]:
        return [
            session
            for session in self.list_active_sessions()
            if session.session_scope == "physical"
        ]

    def get_session_by_remote_identity(
        self,
        *,
        remote_identity_type: str,
        remote_identity_id: str,
        session_scope: str | None = None,
    ) -> NetworkSession | None:
        for session in self.list_sessions(session_scope=session_scope):
            if session.remote_identity_type != remote_identity_type:
                continue
            if session.remote_identity_id != remote_identity_id:
                continue
            return session
        return None

    def get_active_session_by_remote_identity(
        self,
        *,
        remote_identity_type: str,
        remote_identity_id: str,
        session_scope: str | None = None,
    ) -> NetworkSession | None:
        for session in self.list_active_sessions():
            if session_scope is not None and session.session_scope != session_scope:
                continue
            if session.remote_identity_type != remote_identity_type:
                continue
            if session.remote_identity_id != remote_identity_id:
                continue
            return session
        return None

    def get_session_by_remote_physical_node_id(
        self,
        remote_physical_node_id: str,
    ) -> NetworkSession | None:
        return self.get_session_by_remote_identity(
            remote_identity_type="physical_node",
            remote_identity_id=remote_physical_node_id,
            session_scope="physical",
        )

    def get_session_by_remote_virtual_node_id(
        self,
        remote_virtual_node_id: str,
    ) -> NetworkSession | None:
        return self.get_session_by_remote_identity(
            remote_identity_type="virtual_node",
            remote_identity_id=remote_virtual_node_id,
        )

    def get_active_session_by_remote_virtual_node_id(
        self,
        remote_virtual_node_id: str,
    ) -> NetworkSession | None:
        return self.get_active_session_by_remote_identity(
            remote_identity_type="virtual_node",
            remote_identity_id=remote_virtual_node_id,
        )

    def get_active_virtual_session_by_local_and_remote_node_id(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
    ) -> NetworkSession | None:
        for session in self.list_active_sessions():
            if session.session_scope != "virtual":
                continue
            if session.local_identity_id != local_virtual_node_id:
                continue
            if session.remote_identity_id != remote_virtual_node_id:
                continue
            return session
        return None

    def has_active_virtual_session_bound_to_route(self, *route_ids: str | None) -> bool:
        active_route_ids = {route_id for route_id in route_ids if route_id}
        if not active_route_ids:
            return False

        for session in self.list_active_sessions():
            if session.session_scope != "virtual":
                continue
            if session.bound_route_id in active_route_ids:
                return True
        return False

    def has_open_physical_session(self, remote_physical_node_id: str) -> bool:
        for session in self.list_sessions(session_scope="physical"):
            if session.remote_identity_id != remote_physical_node_id:
                continue
            if session.session_state in {"pending", "active"}:
                return True
        return False

    def get_active_physical_session_by_remote_node_id(
        self,
        remote_physical_node_id: str,
    ) -> NetworkSession | None:
        return self.get_active_session_by_remote_identity(
            remote_identity_type="physical_node",
            remote_identity_id=remote_physical_node_id,
            session_scope="physical",
        )

    def store_local_ephemeral_keypair(
        self,
        session_id: str,
        *,
        private_key_pem: str,
        public_key_pem: str,
        handshake_state: str,
    ) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        session.local_ephemeral_private_key = private_key_pem
        session.local_ephemeral_public_key = public_key_pem
        session.handshake_state = handshake_state
        self.touch_session(session_id)
        return session

    def store_remote_ephemeral_public_key(
        self,
        session_id: str,
        *,
        public_key_pem: str,
        handshake_state: str,
    ) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        session.remote_ephemeral_public_key = public_key_pem
        session.handshake_state = handshake_state
        self.touch_session(session_id)
        return session

    def store_shared_secret(
        self,
        session_id: str,
        *,
        shared_secret_hex: str,
        handshake_state: str,
        session_state: str,
    ) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        session.shared_secret_hex = shared_secret_hex
        session.session_key_id = session_id
        session.handshake_state = handshake_state
        session.session_state = session_state
        self.touch_session(session_id)
        return session

    def activate_session(self, session_id: str) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        now = utc_now()
        session.handshake_state = "ready"
        session.session_state = "active"
        session.established_at = now
        session.last_activity_at = now
        session.keepalive_deadline = self._build_keepalive_deadline(session.keepalive_interval_seconds)
        return session

    def bind_remote_endpoint(
        self,
        session_id: str,
        *,
        transport: str | None = None,
        host: str | None = None,
        port: int | None = None,
    ) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        if transport is not None:
            session.transport = transport
        if host is not None:
            session.remote_host = host
        if port is not None:
            session.remote_port = port
        return session

    def touch_session(
        self,
        session_id: str,
        *,
        keepalive_interval_seconds: int | None = None,
    ) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        if keepalive_interval_seconds is not None:
            session.keepalive_interval_seconds = keepalive_interval_seconds

        session.last_activity_at = utc_now()
        session.keepalive_deadline = self._build_keepalive_deadline(session.keepalive_interval_seconds)
        return session

    def mark_keepalive_sent(self, session_id: str) -> NetworkSession | None:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            return None

        session.last_keepalive_sent_at = utc_now()
        return session

    def update_session_state(
        self,
        session_id: str,
        data: SessionStateUpdateInput,
    ) -> NetworkSession | None:
        session = self._sessions_by_session_id.get(session_id)
        if session is None:
            return None

        if data.handshake_state is not None:
            session.handshake_state = data.handshake_state
        if data.session_state is not None:
            session.session_state = data.session_state
        if data.last_activity_at is not None:
            session.last_activity_at = data.last_activity_at
        if data.keepalive_deadline is not None:
            session.keepalive_deadline = data.keepalive_deadline
        if data.expires_at is not None:
            session.expires_at = data.expires_at
        if data.closed_at is not None:
            session.closed_at = data.closed_at
        if data.close_reason is not None:
            session.close_reason = data.close_reason
        if data.bound_route_id is not None:
            session.bound_route_id = data.bound_route_id
        if data.session_key_id is not None:
            session.session_key_id = data.session_key_id
        if data.metadata_json is not None:
            session.metadata_json = data.metadata_json

        return session

    def close_session(self, session_id: str, *, close_reason: str) -> NetworkSession | None:
        return self.update_session_state(
            session_id,
            SessionStateUpdateInput(
                handshake_state="closed",
                session_state="closed",
                closed_at=utc_now(),
                close_reason=close_reason,
            ),
        )

    def delete_session(self, session_id: str) -> bool:
        session = self._sessions_by_session_id.pop(session_id, None)
        if session is None:
            return False

        self._sessions_by_id.pop(session.id, None)
        self._reliable_states_by_session_id.pop(session_id, None)
        return True

    def session_exists(self, session_id: str) -> bool:
        return session_id in self._sessions_by_session_id

    def prepare_reliable_outbound(
        self,
        *,
        session_id: str,
        inner_message_type: str,
        inner_payload: dict[str, object],
        retry_after_seconds: float,
        max_attempts: int,
    ) -> ReliableOutboundMessage:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("session_not_found_for_reliable_outbound")
        state = self._get_reliable_state(session)
        return build_reliable_outbound(
            state,
            inner_message_type=inner_message_type,
            inner_payload=inner_payload,
            retry_after_seconds=retry_after_seconds,
            max_attempts=max_attempts,
        )

    def mark_reliable_outbound_sent(
        self,
        *,
        session_id: str,
        sequence_number: int,
    ) -> ReliableOutboundMessage | None:
        message = self._get_reliable_outbound(session_id, sequence_number)
        if message is None or not message.pending:
            return None
        message.attempts += 1
        message.last_sent_at = utc_now()
        return message

    def mark_reliable_outbound_acked(
        self,
        *,
        session_id: str,
        sequence_number: int,
        reliable_message_id: str,
    ) -> ReliableOutboundMessage | None:
        message = self._get_reliable_outbound(session_id, sequence_number)
        if message is None:
            return None
        if message.reliable_message_id != reliable_message_id:
            return None
        message.acked_at = utc_now()
        state = self._reliable_states_by_session_id.get(session_id)
        if state is not None:
            state.outbound_by_sequence.pop(sequence_number, None)
        return message

    def receive_reliable_inbound(
        self,
        *,
        session_id: str,
        payload: dict[str, object],
    ) -> ReliableReceiveResult:
        session = self.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("session_not_found_for_reliable_inbound")
        state = self._get_reliable_state(session)
        return receive_reliable_payload(state, payload)

    def list_due_reliable_outbound_messages(self) -> list[ReliableOutboundMessage]:
        now = utc_now()
        due_messages: list[ReliableOutboundMessage] = []
        for message in self.list_pending_reliable_outbound_messages():
            if message.attempts >= message.max_attempts:
                message.failed_at = now
                message.failure_reason = "max_attempts_exceeded"
                continue
            if message.due_for_send(now):
                due_messages.append(message)
        return sorted(
            due_messages,
            key=lambda message: (message.last_sent_at or message.created_at, message.sequence_number),
        )

    def list_pending_reliable_outbound_messages(self) -> list[ReliableOutboundMessage]:
        pending_messages: list[ReliableOutboundMessage] = []
        for state in self._reliable_states_by_session_id.values():
            pending_messages.extend(
                message
                for message in state.outbound_by_sequence.values()
                if message.pending
            )
        return sorted(
            pending_messages,
            key=lambda message: (message.last_sent_at or message.created_at, message.sequence_number),
        )

    def mark_reliable_outbound_failed(
        self,
        *,
        session_id: str,
        sequence_number: int,
        reason: str,
    ) -> ReliableOutboundMessage | None:
        message = self._get_reliable_outbound(session_id, sequence_number)
        if message is None or not message.pending:
            return None
        message.failed_at = utc_now()
        message.failure_reason = reason
        return message

    def count_pending_reliable_outbound(self, session_id: str) -> int:
        state = self._reliable_states_by_session_id.get(session_id)
        if state is None:
            return 0
        return sum(1 for message in state.outbound_by_sequence.values() if message.pending)

    def _get_reliable_state(self, session: NetworkSession) -> ReliableSessionState:
        state = self._reliable_states_by_session_id.get(session.session_id)
        if state is None:
            state = ReliableSessionState(
                session_id=session.session_id,
                session_scope=session.session_scope,
            )
            self._reliable_states_by_session_id[session.session_id] = state
        return state

    def _get_reliable_outbound(
        self,
        session_id: str,
        sequence_number: int,
    ) -> ReliableOutboundMessage | None:
        state = self._reliable_states_by_session_id.get(session_id)
        if state is None:
            return None
        return state.outbound_by_sequence.get(sequence_number)

    @staticmethod
    def _build_keepalive_deadline(keepalive_interval_seconds: int) -> datetime:
        return utc_now() + timedelta(seconds=keepalive_interval_seconds)
