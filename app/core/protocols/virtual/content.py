from __future__ import annotations

from uuid import uuid4

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler
from ..helpers import (
    as_payload_dict as _as_payload_dict,
    optional_string as _read_optional_string,
    require_non_negative_int as _read_non_negative_int,
    require_positive_int as _read_positive_int,
    require_string as _read_required_string,
    read_virtual_session_id as _read_virtual_session_id,
)


class VirtualContentProtocolHandler(ProtocolMessageHandler):
    """Esqueleto do protocolo virtual de transferencia de conteudo por byte range."""

    protocol_family = "content_transfer"
    supported_message_types = {
        "VIRTUAL_CONTENT_INFO_REQUEST",
        "VIRTUAL_CONTENT_INFO_RESPONSE",
        "VIRTUAL_CONTENT_RANGE_REQUEST",
        "VIRTUAL_CONTENT_RANGE_RESPONSE",
        "VIRTUAL_CONTENT_NOT_FOUND",
        "VIRTUAL_CONTENT_RANGE_DENIED",
        "VIRTUAL_CONTENT_RANGE_ERROR",
    }

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "VIRTUAL_CONTENT_INFO_REQUEST":
            return await self._handle_content_info_request(envelope, context, services)
        if envelope.message_type == "VIRTUAL_CONTENT_INFO_RESPONSE":
            return await self._handle_content_info_response(envelope, context, services)
        if envelope.message_type == "VIRTUAL_CONTENT_RANGE_REQUEST":
            return await self._handle_content_range_request(envelope, context, services)
        if envelope.message_type == "VIRTUAL_CONTENT_RANGE_RESPONSE":
            return await self._handle_content_range_response(envelope, context, services)
        if envelope.message_type == "VIRTUAL_CONTENT_NOT_FOUND":
            return self._handle_content_not_found(envelope, context, services)
        if envelope.message_type == "VIRTUAL_CONTENT_RANGE_DENIED":
            return self._handle_content_range_denied(envelope, context, services)
        if envelope.message_type == "VIRTUAL_CONTENT_RANGE_ERROR":
            return self._handle_content_range_error(envelope, context, services)

        return self._build_invalid_result(envelope, "unsupported_content_message_type")

    async def _handle_content_info_request(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_result = self._require_active_virtual_session(envelope, services)
        if session_result is not None:
            return session_result

        payload = _as_payload_dict(envelope)
        try:
            content_id = _read_optional_string(payload, "content_id")
            ddt_key = _read_optional_string(payload, "ddt_key")
        except ValueError as error:
            return self._build_invalid_result(envelope, str(error))

        if content_id is None and ddt_key is None:
            return self._build_invalid_result(envelope, "content_id_or_ddt_key_required")

        services.log_service.info(
            "content_transfer",
            "received content info request",
            session_id=_read_virtual_session_id(envelope),
            content_id=content_id,
            ddt_key=ddt_key,
            route_path_id=context.metadata.get("route_path_id"),
        )

        content_info = services.content_transfer_service.get_content_info(content_id)
        if content_info is None:
            services.log_service.info(
                "content_transfer",
                "content info request not found locally",
                session_id=_read_virtual_session_id(envelope),
                content_id=content_id,
                ddt_key=ddt_key,
            )
            return self._build_virtual_response_result(
                envelope,
                action="content_info_not_found",
                response_message_type="VIRTUAL_CONTENT_NOT_FOUND",
                payload=self.build_content_error_payload(
                    content_id=content_id,
                    error_code="content_not_found",
                    error_message="Conteudo nao encontrado no node local.",
                ),
                content_id=content_id,
                ddt_key=ddt_key,
            )

        response_payload = self.build_content_info_response_payload(
            content_id=content_info.content_id,
            size_bytes=content_info.size_bytes,
            content_hash=content_info.content_hash,
            content_type=content_info.content_type,
            ddt_key=ddt_key,
        )
        return self._build_virtual_response_result(
            envelope,
            action="content_info_response_ready",
            response_message_type="VIRTUAL_CONTENT_INFO_RESPONSE",
            payload=response_payload,
            content_id=content_info.content_id,
            content_hash=content_info.content_hash,
            size_bytes=content_info.size_bytes,
            content_type=content_info.content_type,
            ddt_key=ddt_key,
        )

    async def _handle_content_info_response(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_result = self._require_active_virtual_session(envelope, services)
        if session_result is not None:
            return session_result

        try:
            content_info = _parse_content_info_payload(_as_payload_dict(envelope))
        except ValueError as error:
            return self._build_invalid_result(envelope, str(error))

        session_id = _read_virtual_session_id(envelope)
        if session_id is None:
            return self._build_invalid_result(envelope, "virtual_session_id_required")

        services.log_service.info(
            "content_transfer",
            "starting or resuming content download",
            session_id=session_id,
            content_id=content_info.content_id,
            content_hash=content_info.content_hash,
            size_bytes=content_info.size_bytes,
            storage_dir=str(services.content_transfer_service.storage_dir),
        )
        download_state = services.content_transfer_service.start_or_update_download(
            session_id=session_id,
            content_id=content_info.content_id,
            content_hash=content_info.content_hash,
            size_bytes=content_info.size_bytes,
            content_type=content_info.content_type,
        )
        next_range = services.content_transfer_service.get_next_range_request(
            session_id=session_id,
            content_id=content_info.content_id,
        )
        services.log_service.info(
            "content_transfer",
            "received content info response",
            session_id=session_id,
            content_id=content_info.content_id,
            size_bytes=content_info.size_bytes,
            content_type=content_info.content_type,
            download_status=download_state.status,
            has_next_range=next_range is not None,
        )
        if next_range is not None:
            start_byte, end_byte = next_range
            return self._build_virtual_response_result(
                envelope,
                action="content_range_request_ready",
                response_message_type="VIRTUAL_CONTENT_RANGE_REQUEST",
                payload=self.build_content_range_request_payload(
                    content_id=content_info.content_id,
                    start_byte=start_byte,
                    end_byte=end_byte,
                ),
                content_id=content_info.content_id,
                start_byte=start_byte,
                end_byte=end_byte,
                length=end_byte - start_byte,
            )

        return self._build_skeleton_result(
            envelope,
            action="content_info_response_received",
            content_id=content_info.content_id,
            size_bytes=content_info.size_bytes,
            content_hash=content_info.content_hash,
            content_type=content_info.content_type,
            next_step="store_pending_content_metadata_for_range_reads",
        )

    async def _handle_content_range_request(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_result = self._require_active_virtual_session(envelope, services)
        if session_result is not None:
            return session_result

        try:
            range_request = _parse_range_request_payload(_as_payload_dict(envelope))
        except ValueError as error:
            return self._build_invalid_result(envelope, str(error))

        services.log_service.info(
            "content_transfer",
            "received content range request",
            session_id=_read_virtual_session_id(envelope),
            content_id=range_request.content_id,
            start_byte=range_request.start_byte,
            end_byte=range_request.end_byte,
            length=range_request.length,
            route_path_id=context.metadata.get("route_path_id"),
        )

        try:
            content_range = services.content_transfer_service.read_content_range(
                content_id=range_request.content_id,
                start_byte=range_request.start_byte,
                end_byte=range_request.end_byte,
            )
        except FileNotFoundError:
            return self._build_virtual_response_result(
                envelope,
                action="content_range_not_found",
                response_message_type="VIRTUAL_CONTENT_NOT_FOUND",
                payload=self.build_content_error_payload(
                    content_id=range_request.content_id,
                    error_code="content_not_found",
                    error_message="Conteudo nao encontrado no node local.",
                ),
                content_id=range_request.content_id,
            )
        except ValueError as error:
            return self._build_virtual_response_result(
                envelope,
                action="content_range_denied",
                response_message_type="VIRTUAL_CONTENT_RANGE_DENIED",
                payload=self.build_content_error_payload(
                    content_id=range_request.content_id,
                    error_code="invalid_range",
                    error_message=str(error),
                ),
                content_id=range_request.content_id,
                start_byte=range_request.start_byte,
                end_byte=range_request.end_byte,
            )

        return self._build_virtual_response_result(
            envelope,
            action="content_range_response_ready",
            response_message_type="VIRTUAL_CONTENT_RANGE_RESPONSE",
            payload=self.build_content_range_response_payload(
                content_id=content_range.content_id,
                start_byte=content_range.start_byte,
                end_byte=content_range.end_byte,
                data_base64=content_range.data_base64,
            ),
            content_id=content_range.content_id,
            start_byte=content_range.start_byte,
            end_byte=content_range.end_byte,
            length=content_range.end_byte - content_range.start_byte,
        )

    async def _handle_content_range_response(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        session_result = self._require_active_virtual_session(envelope, services)
        if session_result is not None:
            return session_result

        try:
            range_response = _parse_range_response_payload(_as_payload_dict(envelope))
        except ValueError as error:
            return self._build_invalid_result(envelope, str(error))

        services.log_service.info(
            "content_transfer",
            "received content range response",
            session_id=_read_virtual_session_id(envelope),
            content_id=range_response.content_id,
            start_byte=range_response.start_byte,
            end_byte=range_response.end_byte,
            data_base64_size=len(range_response.data_base64),
        )
        session_id = _read_virtual_session_id(envelope)
        if session_id is None:
            return self._build_invalid_result(envelope, "virtual_session_id_required")

        try:
            download_state = services.content_transfer_service.handle_content_range_response(
                session_id=session_id,
                content_id=range_response.content_id,
                start_byte=range_response.start_byte,
                end_byte=range_response.end_byte,
                data_base64=range_response.data_base64,
            )
        except ValueError as error:
            return self._build_invalid_result(envelope, str(error))

        if download_state.status == "failed":
            await self._emit_api_event(
                services,
                "content_download_failed",
                {
                    "session_id": session_id,
                    "content_id": download_state.content_id,
                    "error_message": download_state.error_message,
                },
            )
            return self._build_skeleton_result(
                envelope,
                action="content_download_failed",
                content_id=download_state.content_id,
                error_message=download_state.error_message,
            )
        if download_state.status == "completed":
            await self._publish_download_provider_to_ddt(
                envelope,
                services,
                download_state=download_state,
                session_id=session_id,
            )
            await self._emit_api_event(
                services,
                "content_download_completed",
                {
                    "session_id": session_id,
                    "content_id": download_state.content_id,
                    "content_hash": download_state.content_hash,
                    "size_bytes": download_state.size_bytes,
                    "storage_path": str(download_state.final_path),
                },
            )
            return self._build_skeleton_result(
                envelope,
                action="content_download_completed",
                content_id=download_state.content_id,
                content_hash=download_state.content_hash,
                size_bytes=download_state.size_bytes,
                storage_path=str(download_state.final_path),
            )

        next_range = services.content_transfer_service.get_next_range_request(
            session_id=session_id,
            content_id=range_response.content_id,
        )
        if next_range is not None:
            start_byte, end_byte = next_range
            return self._build_virtual_response_result(
                envelope,
                action="content_range_request_ready",
                response_message_type="VIRTUAL_CONTENT_RANGE_REQUEST",
                payload=self.build_content_range_request_payload(
                    content_id=range_response.content_id,
                    start_byte=start_byte,
                    end_byte=end_byte,
                ),
                content_id=range_response.content_id,
                start_byte=start_byte,
                end_byte=end_byte,
                length=end_byte - start_byte,
            )

        return self._build_skeleton_result(
            envelope,
            action="content_range_response_received",
            content_id=range_response.content_id,
            start_byte=range_response.start_byte,
            end_byte=range_response.end_byte,
            data_base64_size=len(range_response.data_base64),
            download_status=download_state.status,
        )

    async def _emit_api_event(
        self,
        services: EngineServices,
        event_type: str,
        data: dict[str, object],
    ) -> None:
        api_service = getattr(services, "api_service", None)
        if api_service is None:
            return
        await api_service.emit_event(event_type, data)

    def _handle_content_not_found(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._handle_content_error_message(
            envelope,
            context,
            services,
            action="content_not_found_received",
        )

    def _handle_content_range_denied(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._handle_content_error_message(
            envelope,
            context,
            services,
            action="content_range_denied_received",
        )

    def _handle_content_range_error(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        return self._handle_content_error_message(
            envelope,
            context,
            services,
            action="content_range_error_received",
        )

    def _handle_content_error_message(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
        *,
        action: str,
    ) -> PacketProcessingResult:
        session_result = self._require_active_virtual_session(envelope, services)
        if session_result is not None:
            return session_result

        payload = _as_payload_dict(envelope)
        try:
            content_id = _read_optional_string(payload, "content_id")
            error_code = _read_optional_string(payload, "error_code")
            error_message = _read_optional_string(payload, "error_message")
        except ValueError as error:
            return self._build_invalid_result(envelope, str(error))

        services.log_service.info(
            "content_transfer",
            action,
            session_id=_read_virtual_session_id(envelope),
            message_type=envelope.message_type,
            content_id=content_id,
            error_code=error_code,
            error_message=error_message,
            route_path_id=context.metadata.get("route_path_id"),
        )
        return self._build_skeleton_result(
            envelope,
            action=action,
            content_id=content_id,
            error_code=error_code,
            error_message=error_message,
        )

    async def _publish_download_provider_to_ddt(
        self,
        envelope: ProtocolEnvelope,
        services: EngineServices,
        *,
        download_state,
        session_id: str,
    ) -> None:
        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.local_identity_type != "virtual_node":
            services.log_service.warning(
                "content_transfer",
                "download completed but local virtual session was not found for ddt publish",
                session_id=session_id,
                content_id=download_state.content_id,
            )
            return

        advertisement = services.content_transfer_service.build_provider_advertisement(
            content_id=download_state.content_id,
            local_virtual_node_id=session.local_identity_id,
            ttl_seconds=services.config.content_provider_advertisement_ttl_seconds,
        )
        if advertisement is None:
            services.log_service.warning(
                "content_transfer",
                "download completed but ddt provider advertisement could not be built",
                session_id=session_id,
                content_id=download_state.content_id,
                local_virtual_node_id=session.local_identity_id,
            )
            return
        if services.protocol_clients is None:
            services.log_service.warning(
                "content_transfer",
                "download completed but protocol clients are unavailable for ddt publish",
                session_id=session_id,
                content_id=download_state.content_id,
                ddt_key=advertisement.key,
            )
            return

        try:
            publish_result = await services.protocol_clients.physical.dht.publish(
                namespace=advertisement.namespace,
                logical_key=advertisement.logical_key,
                record_json=advertisement.record_json,
                expires_at=advertisement.expires_at.isoformat(),
            )
        except Exception as error:
            services.log_service.warning(
                "content_transfer",
                "failed to publish downloaded content provider to ddt",
                session_id=session_id,
                content_id=download_state.content_id,
                ddt_key=advertisement.key,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return

        if publish_result.get("status") != "stored":
            services.log_service.warning(
                "content_transfer",
                "downloaded content provider ddt publish did not store record",
                session_id=session_id,
                content_id=download_state.content_id,
                ddt_key=advertisement.key,
                status=publish_result.get("status"),
                reason=publish_result.get("reason"),
                stored_count=publish_result.get("stored_count"),
                required_stored_count=publish_result.get("required_stored_count"),
                stored_by=publish_result.get("stored_by"),
            )
            return

        services.content_transfer_service.mark_provider_advertisement_published(
            content_id=download_state.content_id,
            local_virtual_node_id=session.local_identity_id,
            expires_at=advertisement.expires_at,
        )
        services.log_service.info(
            "content_transfer",
            "published downloaded content provider to ddt",
            session_id=session_id,
            content_id=download_state.content_id,
            local_virtual_node_id=session.local_identity_id,
            ddt_key=advertisement.key,
            message_id=envelope.header.get("message_id"),
            stored_count=publish_result.get("stored_count"),
            required_stored_count=publish_result.get("required_stored_count"),
        )
        await self._emit_api_event(
            services,
            "content_provider_published",
            {
                "session_id": session_id,
                "content_id": download_state.content_id,
                "local_virtual_node_id": session.local_identity_id,
                "ddt_key": advertisement.key,
                "expires_at": advertisement.expires_at.isoformat(),
            },
        )

    def _require_active_virtual_session(
        self,
        envelope: ProtocolEnvelope,
        services: EngineServices,
    ) -> PacketProcessingResult | None:
        session_id = _read_virtual_session_id(envelope)
        if session_id is None:
            return self._build_invalid_result(envelope, "virtual_session_id_required")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_scope != "virtual":
            return self._build_invalid_result(envelope, "virtual_session_not_found")
        if session.session_state != "active":
            return self._build_invalid_result(envelope, "virtual_session_not_active")

        services.session_manager.touch_session(session_id)
        return None

    def _build_skeleton_result(
        self,
        envelope: ProtocolEnvelope,
        *,
        action: str,
        **metadata: object,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": action,
                "session_id": _read_virtual_session_id(envelope),
                **metadata,
            },
        )

    def _build_virtual_response_result(
        self,
        envelope: ProtocolEnvelope,
        *,
        action: str,
        response_message_type: str,
        payload: dict[str, object],
        **metadata: object,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "action": action,
                "session_id": _read_virtual_session_id(envelope),
                "virtual_response_message_type": response_message_type,
                "virtual_response_envelope": self.build_virtual_response_envelope(
                    envelope.header,
                    message_type=response_message_type,
                    payload=payload,
                ),
                **metadata,
            },
        )

    def _build_invalid_result(
        self,
        envelope: ProtocolEnvelope,
        reason: str,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "reason": reason,
                "session_id": _read_virtual_session_id(envelope),
            },
        )

    @staticmethod
    def build_content_info_request_payload(
        *,
        content_id: str | None = None,
        ddt_key: str | None = None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {}
        if content_id:
            payload["content_id"] = content_id
        if ddt_key:
            payload["ddt_key"] = ddt_key
        return payload

    @staticmethod
    def build_content_info_response_payload(
        *,
        content_id: str,
        size_bytes: int,
        content_hash: str,
        content_type: str,
        ddt_key: str | None = None,
        max_range_size: int | None = None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "content_id": content_id,
            "size_bytes": size_bytes,
            "content_hash": content_hash,
            "content_type": content_type,
            "range_unit": "bytes",
        }
        if ddt_key:
            payload["ddt_key"] = ddt_key
        if max_range_size is not None:
            payload["max_range_size"] = max_range_size
        return payload

    @staticmethod
    def build_content_range_request_payload(
        *,
        content_id: str,
        start_byte: int,
        end_byte: int,
    ) -> dict[str, object]:
        return {
            "content_id": content_id,
            "start_byte": start_byte,
            "end_byte": end_byte,
            "range_unit": "bytes",
        }

    @staticmethod
    def build_content_range_response_payload(
        *,
        content_id: str,
        start_byte: int,
        end_byte: int,
        data_base64: str,
    ) -> dict[str, object]:
        return {
            "content_id": content_id,
            "start_byte": start_byte,
            "end_byte": end_byte,
            "range_unit": "bytes",
            "data_base64": data_base64,
        }

    @staticmethod
    def build_content_error_payload(
        *,
        content_id: str | None,
        error_code: str,
        error_message: str,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "error_code": error_code,
            "error_message": error_message,
        }
        if content_id:
            payload["content_id"] = content_id
        return payload

    @staticmethod
    def build_virtual_response_envelope(
        request_header: dict[str, object],
        *,
        message_type: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        return {
            "header": {
                "version": request_header.get("version", 1),
                "message_type": message_type,
                "message_id": str(uuid4()),
                "message_sequence": request_header.get("message_sequence"),
                "physical_session_id": request_header.get("physical_session_id"),
                "virtual_session_id": request_header.get("virtual_session_id"),
                "response_to_message_id": request_header.get("message_id"),
            },
            "payload": payload,
        }


class ContentInfoPayload:
    def __init__(
        self,
        *,
        content_id: str,
        size_bytes: int,
        content_hash: str,
        content_type: str,
    ) -> None:
        self.content_id = content_id
        self.size_bytes = size_bytes
        self.content_hash = content_hash
        self.content_type = content_type


class ContentRangePayload:
    def __init__(
        self,
        *,
        content_id: str,
        start_byte: int,
        end_byte: int,
        data_base64: str | None = None,
    ) -> None:
        self.content_id = content_id
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.length = end_byte - start_byte
        self.data_base64 = data_base64 or ""


def _parse_content_info_payload(payload: dict[str, object]) -> ContentInfoPayload:
    content_id = _read_required_string(payload, "content_id")
    size_bytes = _read_non_negative_int(payload, "size_bytes")
    content_hash = _read_required_string(payload, "content_hash")
    content_type = _read_required_string(payload, "content_type")
    return ContentInfoPayload(
        content_id=content_id,
        size_bytes=size_bytes,
        content_hash=content_hash,
        content_type=content_type,
    )


def _parse_range_request_payload(payload: dict[str, object]) -> ContentRangePayload:
    content_id = _read_required_string(payload, "content_id")
    start_byte = _read_non_negative_int(payload, "start_byte")
    end_byte = _read_positive_int(payload, "end_byte")
    if end_byte <= start_byte:
        raise ValueError("end_byte precisa ser maior que start_byte.")
    return ContentRangePayload(
        content_id=content_id,
        start_byte=start_byte,
        end_byte=end_byte,
    )


def _parse_range_response_payload(payload: dict[str, object]) -> ContentRangePayload:
    range_payload = _parse_range_request_payload(payload)
    data_base64 = _read_required_string(payload, "data_base64")
    return ContentRangePayload(
        content_id=range_payload.content_id,
        start_byte=range_payload.start_byte,
        end_byte=range_payload.end_byte,
        data_base64=data_base64,
    )

