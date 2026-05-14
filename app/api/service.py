from __future__ import annotations

import base64
import binascii
import json
import inspect
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from crypto import sha512_hex
from crypto import dilithium_sign_hex, dilithium_verify_hex
from core.protocols.virtual.content import VirtualContentProtocolHandler
from identity import VirtualNodeIdentityCreateInput
from sessions import VirtualSessionMessage


VirtualMessageSink = Callable[[dict[str, object]], Awaitable[None] | None]
ApiEventSink = Callable[[dict[str, object]], Awaitable[None] | None]


class CoreApiError(Exception):
    """Erro esperado da API publica do core."""

    def __init__(self, code: str, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class CoreApiService:
    """Fachada publica para apps externas usarem o core sem conhecer internals."""

    def __init__(self, engine, *, inbox_limit: int = 1000) -> None:
        self.engine = engine
        self._virtual_message_inbox: deque[dict[str, object]] = deque(maxlen=inbox_limit)
        self._owned_virtual_handlers: set[str] = set()
        self._inbox_virtual_message_types: set[str] = set()
        self._virtual_message_sinks: set[VirtualMessageSink] = set()
        self._event_sinks: set[ApiEventSink] = set()

    def get_status(self) -> dict[str, object]:
        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        sessions = self.engine.services.session_manager.list_sessions()
        return {
            "node_name": self.engine.get_runtime_node_name(),
            "listen_host": self.engine.services.config.listen_host,
            "listen_port": self.engine.services.config.listen_port,
            "physical_node_id": local_node.id if local_node else None,
            "session_count": len(sessions),
            "active_session_count": len(
                [session for session in sessions if session.session_state == "active"]
            ),
        }

    def list_local_virtual_nodes(self, *, only_active: bool = False) -> list[dict[str, object]]:
        nodes = self.engine.services.identity_service.list_local_virtual_nodes(
            only_active=only_active,
        )
        return [self._serialize_virtual_node(node, local=True) for node in nodes]

    def list_remote_virtual_nodes(
        self,
        *,
        status: str | None = None,
    ) -> list[dict[str, object]]:
        nodes = self.engine.services.identity_service.list_remote_virtual_nodes(status=status)
        return [self._serialize_virtual_node(node, local=False) for node in nodes]

    def create_local_virtual_node(
        self,
        *,
        kind: str = "default",
        expires_at: str | None = None,
        is_active: bool = True,
        metadata_json: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        local_physical_node = self.engine.services.identity_service.ensure_local_physical_node()
        node = self.engine.services.identity_service.create_local_virtual_node(
            VirtualNodeIdentityCreateInput(
                kind=kind,
                owner_physical_node_id=local_physical_node.id,
                expires_at=self._parse_optional_datetime(expires_at),
                is_active=is_active,
                metadata_json=self._build_metadata_json(metadata_json, metadata),
            )
        )
        return self._serialize_virtual_node(node, local=True)

    def upsert_remote_virtual_node(
        self,
        *,
        public_key: str,
        node_id: str | None = None,
        kind: str = "default",
        status: str = "active",
        expires_at: str | None = None,
        metadata_json: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if not public_key:
            raise CoreApiError("public_key_required", "public_key e obrigatorio.")

        resolved_node_id = node_id or sha512_hex(public_key.encode("utf-8"))
        node = self.engine.services.identity_service.upsert_remote_virtual_node(
            node_id=resolved_node_id,
            public_key=public_key,
            kind=kind,
            status=status,
            expires_at=self._parse_optional_datetime(expires_at),
            metadata_json=self._build_metadata_json(metadata_json, metadata),
        )
        return self._serialize_virtual_node(node, local=False)

    async def dht_publish(
        self,
        *,
        namespace: str,
        logical_key: str,
        record_json: str | None = None,
        record: dict[str, object] | None = None,
        expires_at: str | None = None,
    ) -> dict[str, object]:
        if not record_json and record is None:
            raise CoreApiError("record_required", "Informe record_json ou record.")

        payload_json = record_json or json.dumps(
            record,
            separators=(",", ":"),
            sort_keys=True,
        )
        return await self.engine.services.protocol_clients.physical.dht.publish(
            namespace=namespace,
            logical_key=logical_key,
            record_json=payload_json,
            expires_at=expires_at,
        )

    async def dht_query(self, *, namespace: str, logical_key: str) -> dict[str, object]:
        return await self.engine.services.protocol_clients.physical.dht.query(
            namespace=namespace,
            logical_key=logical_key,
        )

    def build_dht_key(self, *, namespace: str, logical_key: str) -> dict[str, object]:
        normalized_namespace = namespace.strip().lower()
        if not normalized_namespace or not logical_key:
            raise CoreApiError("dht_key_input_required", "namespace e logical_key sao obrigatorios.")
        return {
            "namespace": normalized_namespace,
            "logical_key": logical_key,
            "key": sha512_hex(f"{normalized_namespace}|{logical_key}".encode("utf-8")),
        }

    def sign_local_virtual_node_payload(
        self,
        *,
        local_virtual_node_id: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        virtual_node = self.engine.services.identity_service.get_local_virtual_node_by_id(
            local_virtual_node_id,
        )
        if virtual_node is None:
            raise CoreApiError("local_virtual_node_not_found", "Virtual node local nao encontrado.")

        payload_hex = self._canonical_payload_hex(payload)
        return {
            "local_virtual_node_id": local_virtual_node_id,
            "signature_hex": dilithium_sign_hex(payload_hex, virtual_node.private_key_encrypted),
        }

    def verify_virtual_node_payload_signature(
        self,
        *,
        public_key: str,
        payload: dict[str, object],
        signature_hex: str,
    ) -> dict[str, object]:
        if not public_key or not signature_hex:
            raise CoreApiError("signature_input_required", "public_key e signature_hex sao obrigatorios.")

        payload_hex = self._canonical_payload_hex(payload)
        try:
            is_valid = dilithium_verify_hex(payload_hex, signature_hex, public_key)
        except Exception:
            is_valid = False
        return {"valid": is_valid}

    def list_virtual_sessions(self) -> list[dict[str, object]]:
        sessions = self.engine.services.session_manager.list_sessions(session_scope="virtual")
        return [self._serialize_session(session) for session in sessions]

    async def start_virtual_session(
        self,
        *,
        local_virtual_node_id: str,
        remote_virtual_node_id: str,
    ) -> dict[str, object]:
        session_id = await self.engine.services.protocol_clients.virtual.session.start_session_to_virtual_node(
            local_virtual_node_id=local_virtual_node_id,
            remote_virtual_node_id=remote_virtual_node_id,
        )
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise CoreApiError("session_not_found", "A virtual session nao ficou disponivel.")
        return self._serialize_session(session)

    async def send_virtual_message(
        self,
        *,
        session_id: str,
        app_message_type: str,
        payload: dict[str, object] | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        resolved_request_id = await self.engine.services.protocol_clients.virtual.session.send_message(
            session_id=session_id,
            app_message_type=app_message_type,
            payload=payload,
            request_id=request_id,
        )
        return {
            "session_id": session_id,
            "app_message_type": app_message_type,
            "request_id": resolved_request_id,
        }

    async def close_virtual_session(
        self,
        *,
        session_id: str,
        close_reason: str = "api_closed",
    ) -> dict[str, object]:
        await self.engine.services.protocol_clients.virtual.session.close_session(
            session_id=session_id,
            close_reason=close_reason,
        )
        session = self.engine.services.session_manager.close_session(
            session_id,
            close_reason=close_reason,
        )
        if session is None:
            raise CoreApiError("session_not_found", "Virtual session nao encontrada.", status_code=404)
        return self._serialize_session(session)

    def store_content(
        self,
        *,
        data_base64: str,
        title: str | None = None,
        content_type: str = "application/octet-stream",
        tags: list[str] | None = None,
        is_encrypted: bool = False,
        encryption_scheme: str | None = None,
    ) -> dict[str, object]:
        try:
            data = base64.b64decode(data_base64.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error) as error:
            raise CoreApiError("invalid_base64", "data_base64 invalido.") from error

        content = self.engine.services.content_transfer_service.store_local_content(
            data=data,
            title=title,
            content_type=content_type,
            tags=tags or [],
            is_encrypted=is_encrypted,
            encryption_scheme=encryption_scheme,
        )
        return self._serialize_content_info(content)

    def list_content(self, *, limit: int = 100) -> list[dict[str, object]]:
        content_items = self.engine.services.content_transfer_service.list_content(limit=limit)
        return [self._serialize_content_info(content) for content in content_items]

    def get_content_info(self, *, content_id: str) -> dict[str, object]:
        content = self.engine.services.content_transfer_service.get_content_info(content_id)
        if content is None:
            raise CoreApiError("content_not_found", "Conteudo nao encontrado.", status_code=404)
        return self._serialize_content_info(content)

    def read_content_range(
        self,
        *,
        content_id: str,
        start_byte: int,
        end_byte: int,
    ) -> dict[str, object]:
        try:
            content_range = self.engine.services.content_transfer_service.read_content_range(
                content_id=content_id,
                start_byte=start_byte,
                end_byte=end_byte,
            )
        except FileNotFoundError as error:
            raise CoreApiError("content_not_found", "Conteudo nao encontrado.", status_code=404) from error
        except ValueError as error:
            raise CoreApiError("invalid_content_range", str(error)) from error

        return {
            "content_id": content_range.content_id,
            "start_byte": content_range.start_byte,
            "end_byte": content_range.end_byte,
            "length": content_range.end_byte - content_range.start_byte,
            "data_base64": content_range.data_base64,
        }

    async def publish_content_provider(
        self,
        *,
        content_id: str,
        local_virtual_node_id: str,
        ttl_seconds: int | None = None,
    ) -> dict[str, object]:
        ttl = ttl_seconds or self.engine.services.config.content_provider_advertisement_ttl_seconds
        advertisement = self.engine.services.content_transfer_service.build_provider_advertisement(
            content_id=content_id,
            local_virtual_node_id=local_virtual_node_id,
            ttl_seconds=ttl,
        )
        if advertisement is None:
            raise CoreApiError(
                "provider_advertisement_not_available",
                "Nao foi possivel montar o anuncio DDT para esse conteudo/VN.",
            )

        publish_result = await self.engine.services.protocol_clients.physical.dht.publish(
            namespace=advertisement.namespace,
            logical_key=advertisement.logical_key,
            record_json=advertisement.record_json,
            expires_at=advertisement.expires_at.isoformat(),
        )
        if publish_result.get("status") == "stored":
            self.engine.services.content_transfer_service.mark_provider_advertisement_published(
                content_id=content_id,
                local_virtual_node_id=local_virtual_node_id,
                expires_at=advertisement.expires_at,
            )

        data = {
            "namespace": advertisement.namespace,
            "logical_key": advertisement.logical_key,
            "key": advertisement.key,
            "expires_at": advertisement.expires_at.isoformat(),
            "publish_result": publish_result,
        }
        await self.emit_event("content_provider_published", data)
        return data

    async def start_content_download(
        self,
        *,
        session_id: str,
        content_id: str,
        ddt_key: str | None = None,
    ) -> dict[str, object]:
        if not content_id and not ddt_key:
            raise CoreApiError("content_id_or_ddt_key_required", "Informe content_id ou ddt_key.")

        await self.engine.services.protocol_clients.virtual.session.send_protocol_message(
            session_id=session_id,
            message_type="VIRTUAL_CONTENT_INFO_REQUEST",
            payload=VirtualContentProtocolHandler.build_content_info_request_payload(
                content_id=content_id or None,
                ddt_key=ddt_key,
            ),
            virtual_envelope_ciphered=True,
        )
        data = {
            "session_id": session_id,
            "content_id": content_id,
            "ddt_key": ddt_key,
            "status": "requested",
        }
        await self.emit_event("content_download_requested", data)
        return data

    def list_content_downloads(
        self,
        *,
        session_id: str | None = None,
    ) -> list[dict[str, object]]:
        states = self.engine.services.content_transfer_service.list_download_states(
            session_id=session_id,
        )
        return [self._serialize_download_state(state) for state in states]

    def get_content_download(
        self,
        *,
        session_id: str,
        content_id: str,
    ) -> dict[str, object]:
        state = self.engine.services.content_transfer_service.get_download_state(
            session_id=session_id,
            content_id=content_id,
        )
        if state is None:
            raise CoreApiError("download_not_found", "Download nao encontrado.", status_code=404)
        return self._serialize_download_state(state)

    def subscribe_virtual_messages(self, *, app_message_type: str) -> dict[str, object]:
        self.ensure_virtual_message_handler(app_message_type)
        self._inbox_virtual_message_types.add(app_message_type)
        return {
            "app_message_type": app_message_type,
            "subscribed": True,
        }

    def ensure_virtual_message_handler(self, app_message_type: str) -> None:
        if not app_message_type:
            raise CoreApiError("app_message_type_required", "app_message_type e obrigatorio.")

        session_manager = self.engine.services.session_manager
        if (
            session_manager.has_virtual_message_handler(app_message_type)
            and app_message_type not in self._owned_virtual_handlers
        ):
            raise CoreApiError(
                "handler_already_registered",
                "Ja existe um handler registrado para esse app_message_type.",
                status_code=409,
            )

        session_manager.register_virtual_message_handler(
            app_message_type,
            self._handle_virtual_message,
        )
        self._owned_virtual_handlers.add(app_message_type)

    def add_virtual_message_sink(self, sink: VirtualMessageSink) -> None:
        self._virtual_message_sinks.add(sink)

    def remove_virtual_message_sink(self, sink: VirtualMessageSink) -> None:
        self._virtual_message_sinks.discard(sink)

    def add_event_sink(self, sink: ApiEventSink) -> None:
        self._event_sinks.add(sink)

    def remove_event_sink(self, sink: ApiEventSink) -> None:
        self._event_sinks.discard(sink)

    async def emit_event(self, event_type: str, data: dict[str, object]) -> None:
        event = {
            "type": event_type,
            "data": data,
            "emitted_at": datetime.now().astimezone().isoformat(),
        }
        for sink in list(self._event_sinks):
            result = sink(event)
            if inspect.isawaitable(result):
                await result

    def read_virtual_messages(
        self,
        *,
        app_message_type: str | None = None,
        limit: int = 100,
        consume: bool = True,
    ) -> list[dict[str, object]]:
        resolved_limit = max(1, min(limit, 1000))
        selected: list[dict[str, object]] = []
        remaining: deque[dict[str, object]] = deque(maxlen=self._virtual_message_inbox.maxlen)

        while self._virtual_message_inbox:
            message = self._virtual_message_inbox.popleft()
            is_match = (
                app_message_type is None
                or message.get("app_message_type") == app_message_type
            )
            if is_match and len(selected) < resolved_limit:
                selected.append(message)
                if consume:
                    continue
            remaining.append(message)

        self._virtual_message_inbox = remaining
        return selected

    async def _handle_virtual_message(self, message: VirtualSessionMessage) -> None:
        message_data = {
            **asdict(message),
            "received_at": datetime.now().astimezone().isoformat(),
        }
        if message.app_message_type in self._inbox_virtual_message_types:
            self._virtual_message_inbox.append(message_data)

        self.engine.services.log_service.info(
            "core_api",
            "received inbound virtual message",
            session_id=message.session_id,
            app_message_type=message.app_message_type,
            request_id=message.request_id,
            sink_count=len(self._virtual_message_sinks),
        )
        await self._deliver_virtual_message(message_data)

    async def _deliver_virtual_message(self, message_data: dict[str, object]) -> None:
        event = {
            "type": "virtual_message_received",
            "data": message_data,
        }
        await self.emit_event("virtual_message_received", message_data)
        for sink in list(self._virtual_message_sinks):
            result = sink(event)
            if inspect.isawaitable(result):
                await result

    @staticmethod
    def _serialize_virtual_node(node, *, local: bool) -> dict[str, object]:
        data = {
            "id": node.id,
            "public_key": node.public_key,
            "kind": node.kind,
            "expires_at": _datetime_to_iso(getattr(node, "expires_at", None)),
            "metadata_json": getattr(node, "metadata_json", None),
            "created_at": _datetime_to_iso(getattr(node, "created_at", None)),
            "updated_at": _datetime_to_iso(getattr(node, "updated_at", None)),
        }
        if local:
            data.update(
                {
                    "owner_physical_node_id": node.owner_physical_node_id,
                    "is_active": node.is_active,
                }
            )
        else:
            data.update(
                {
                    "status": node.status,
                    "first_seen_at": _datetime_to_iso(node.first_seen_at),
                    "last_seen_at": _datetime_to_iso(node.last_seen_at),
                }
            )
        return data

    @staticmethod
    def _canonical_payload_hex(payload: dict[str, object]) -> str:
        return json.dumps(
            payload,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8").hex()

    @staticmethod
    def _serialize_session(session) -> dict[str, object]:
        return {
            "id": session.id,
            "session_id": session.session_id,
            "session_scope": session.session_scope,
            "local_identity_type": session.local_identity_type,
            "local_identity_id": session.local_identity_id,
            "remote_identity_type": session.remote_identity_type,
            "remote_identity_id": session.remote_identity_id,
            "direction": session.direction,
            "initiator_side": session.initiator_side,
            "handshake_state": session.handshake_state,
            "session_state": session.session_state,
            "bound_route_id": session.bound_route_id,
            "established_at": _datetime_to_iso(session.established_at),
            "last_activity_at": _datetime_to_iso(session.last_activity_at),
            "last_keepalive_sent_at": _datetime_to_iso(session.last_keepalive_sent_at),
            "keepalive_interval_seconds": session.keepalive_interval_seconds,
            "keepalive_deadline": _datetime_to_iso(session.keepalive_deadline),
            "metadata_json": session.metadata_json,
        }

    @staticmethod
    def _serialize_content_info(content) -> dict[str, object]:
        return {
            "content_id": content.content_id,
            "content_hash": content.content_hash,
            "size_bytes": content.size_bytes,
            "content_type": content.content_type,
            "storage_path": content.storage_path,
        }

    @staticmethod
    def _serialize_download_state(state) -> dict[str, object]:
        progress = 1.0 if state.size_bytes == 0 else state.next_start_byte / state.size_bytes
        return {
            "session_id": state.session_id,
            "content_id": state.content_id,
            "content_hash": state.content_hash,
            "size_bytes": state.size_bytes,
            "content_type": state.content_type,
            "downloaded_bytes": state.next_start_byte,
            "progress": min(1.0, progress),
            "status": state.status,
            "error_message": state.error_message,
            "partial_path": str(state.partial_path),
            "final_path": str(state.final_path),
        }

    @staticmethod
    def _build_metadata_json(
        metadata_json: str | None,
        metadata: dict[str, object] | None,
    ) -> str | None:
        if metadata_json:
            return metadata_json
        if metadata is None:
            return None
        if not isinstance(metadata, dict):
            raise CoreApiError("metadata_invalid", "metadata precisa ser um objeto.")
        return json.dumps(metadata, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _parse_optional_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise CoreApiError("datetime_invalid", "Datetime informado e invalido.") from error


def _datetime_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
