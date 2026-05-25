from __future__ import annotations

from sessions import VirtualSessionMessage

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler
from ..helpers import build_response_header, read_virtual_session_id as _read_virtual_session_id


class VirtualMessageProtocolHandler(ProtocolMessageHandler):
    protocol_family = "virtual_message"
    supported_message_types = {
        "VIRTUAL_SESSION_DATA",
    }

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        session_id = _read_virtual_session_id(envelope)
        app_message_type = payload.get("app_message_type")
        app_payload = payload.get("payload")
        request_id = payload.get("request_id")

        if not session_id:
            return self._build_invalid_result(envelope, "virtual_session_id_required")
        if not isinstance(app_message_type, str) or not app_message_type:
            return self._build_invalid_result(envelope, "app_message_type_required")
        if app_payload is None:
            app_payload = {}
        if not isinstance(app_payload, dict):
            return self._build_invalid_result(envelope, "payload_must_be_object")
        if request_id is not None and not isinstance(request_id, str):
            return self._build_invalid_result(envelope, "request_id_must_be_string")

        session = services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_scope != "virtual":
            return self._build_invalid_result(envelope, "virtual_session_not_found")
        if session.session_state != "active":
            return self._build_invalid_result(envelope, "virtual_session_not_active")

        message = VirtualSessionMessage(
            session_id=session_id,
            local_virtual_node_id=session.local_identity_id,
            remote_virtual_node_id=session.remote_identity_id,
            app_message_type=app_message_type,
            payload=app_payload,
            route_path_id=_read_optional_string(context.metadata.get("route_path_id")),
            request_id=request_id,
        )
        reply = await services.session_manager.handle_virtual_message(message)
        services.session_manager.touch_session(session_id)
        services.log_service.info(
            "virtual_message",
            "delivered virtual session data",
            session_id=session_id,
            app_message_type=app_message_type,
            request_id=request_id,
            has_reply=reply is not None,
        )

        metadata: dict[str, object] = {
            "protocol_family": self.protocol_family,
            "action": "virtual_message_delivered",
            "session_id": session_id,
            "app_message_type": app_message_type,
            "request_id": request_id,
            "has_handler": services.session_manager.has_virtual_message_handler(app_message_type),
        }
        if reply is not None:
            metadata.update(
                {
                    "action": "virtual_message_reply",
                    "virtual_response_envelope": {
                        "header": _build_response_header(envelope.header),
                        "payload": {
                            "app_message_type": reply.app_message_type,
                            "request_id": reply.request_id,
                            "payload": reply.payload,
                        },
                    },
                }
            )

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata=metadata,
        )

def _build_response_header(request_header: dict[str, object]) -> dict[str, object]:
    return build_response_header(request_header, "VIRTUAL_SESSION_DATA")


def _read_optional_string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
