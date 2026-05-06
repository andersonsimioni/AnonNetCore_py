from __future__ import annotations

from dataclasses import dataclass
import json

from crypto import aes_decrypt_hex, aes_encrypt_hex

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class RouteExecuteProtocolHandler(ProtocolMessageHandler):
    protocol_family = "route_execute"
    supported_message_types = {
        "ROUTE_DATA",
    }

    async def handle(
        self,
        envelope,
        context,
        services: EngineServices,
    ) -> PacketProcessingResult:
        try:
            route_data = self._parse_route_data(envelope.payload)
        except ValueError as error:
            return self._build_invalid_result(envelope, reason=str(error))
        resolution = self._resolve_route_data_path(
            services=services,
            route_data=route_data,
        )

        if resolution.action == "deliver_local":
            try:
                return await self._deliver_local_route_data(
                    envelope=envelope,
                    context=context,
                    services=services,
                    route_data=route_data,
                )
            except ValueError as error:
                return self._build_invalid_result(envelope, reason=str(error))

        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "action": "forward_message",
                "protocol_family": self.protocol_family,
                "route_data_action": resolution.action,
                "target_remote_physical_node_id": resolution.target_remote_physical_node_id,
                "forward_message_type": "ROUTE_DATA",
                "forward_payload": route_data.to_payload(path_id=resolution.next_path_id),
            },
        )

    def _parse_route_data(
        self,
        payload: object,
    ) -> "RouteExecuteData":
        payload_dict = payload if isinstance(payload, dict) else {}
        return RouteExecuteData(
            path_id=_read_required_string(payload_dict, "path_id"),
            virtual_session_id=_read_optional_string(payload_dict, "virtual_session_id"),
            virtual_envelope_ciphered=_read_required_bool(payload_dict, "virtual_envelope_ciphered"),
            virtual_envelope=payload_dict.get("virtual_envelope"),
        )

    async def _deliver_local_route_data(
        self,
        *,
        envelope,
        context,
        services: EngineServices,
        route_data: "RouteExecuteData",
    ) -> PacketProcessingResult:
        virtual_envelope = self._resolve_local_virtual_envelope(
            services=services,
            route_data=route_data,
        )
        nested_envelope = self._build_nested_virtual_envelope(
            outer_envelope=envelope,
            route_data=route_data,
            virtual_envelope=virtual_envelope,
        )
        nested_context = self._build_nested_virtual_context(
            context=context,
            envelope=envelope,
            route_data=route_data,
        )
        if services.engine is None:
            raise ValueError("core engine nao esta disponivel para redispatch do virtual_envelope.")

        nested_result = await services.engine.process_protocol_envelope(
            nested_envelope,
            nested_context,
        )
        reply_context = self._resolve_local_reply_context(
            services=services,
            route_data=route_data,
        )
        reply_result = self._build_route_reply_result(
            envelope=envelope,
            services=services,
            route_data=route_data,
            nested_result=nested_result,
            reply_context=reply_context,
        )
        if reply_result is not None:
            return reply_result

        merged_metadata = {
            **nested_result.metadata,
            "route_data_action": "deliver_local",
            "route_path_id": route_data.path_id,
            "outer_protocol_family": self.protocol_family,
        }
        return PacketProcessingResult(
            protocol_name=nested_result.protocol_name,
            handled=nested_result.handled,
            message_type=nested_result.message_type,
            response_payload=nested_result.response_payload,
            metadata=merged_metadata,
        )

    def _resolve_local_reply_context(
        self,
        *,
        services: EngineServices,
        route_data: "RouteExecuteData",
    ) -> "LocalRouteReplyContext | None":
        initiator_resolution = services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=route_data.path_id,
        )
        if initiator_resolution is not None and initiator_resolution.first_hop_physical_node_id:
            return LocalRouteReplyContext(
                target_remote_physical_node_id=initiator_resolution.first_hop_physical_node_id,
                reply_path_id=route_data.path_id,
            )

        endpoint_resolution = services.route_service.get_endpoint_resolution_by_path_id(
            route_path_id=route_data.path_id,
        )
        if endpoint_resolution is not None and endpoint_resolution.previous_physical_node_id:
            return LocalRouteReplyContext(
                target_remote_physical_node_id=endpoint_resolution.previous_physical_node_id,
                reply_path_id=route_data.path_id,
            )

        return None

    def _build_route_reply_result(
        self,
        *,
        envelope,
        services: EngineServices,
        route_data: "RouteExecuteData",
        nested_result: PacketProcessingResult,
        reply_context: "LocalRouteReplyContext | None",
    ) -> PacketProcessingResult | None:
        virtual_response_envelope = nested_result.metadata.get("virtual_response_envelope")
        if virtual_response_envelope is None:
            return None
        if not isinstance(virtual_response_envelope, dict):
            raise ValueError("virtual_response_envelope precisa ser um objeto.")
        if reply_context is None:
            raise ValueError("nao foi possivel resolver o caminho reverso da resposta virtual.")

        reply_route_data = RouteExecuteData(
            path_id=reply_context.reply_path_id,
            virtual_session_id=route_data.virtual_session_id,
            virtual_envelope_ciphered=route_data.virtual_envelope_ciphered,
            virtual_envelope=self._build_reply_virtual_payload(
                services=services,
                route_data=route_data,
                virtual_response_envelope=virtual_response_envelope,
            ),
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "action": "forward_message",
                "protocol_family": self.protocol_family,
                "route_data_action": "reply_to_virtual",
                "target_remote_physical_node_id": reply_context.target_remote_physical_node_id,
                "forward_message_type": "ROUTE_DATA",
                "forward_payload": reply_route_data.to_payload(path_id=reply_context.reply_path_id),
                "virtual_response_message_type": self._read_virtual_message_type(
                    virtual_response_envelope
                ),
            },
        )

    def _build_reply_virtual_payload(
        self,
        *,
        services: EngineServices,
        route_data: "RouteExecuteData",
        virtual_response_envelope: dict[str, object],
    ) -> object:
        if not route_data.virtual_envelope_ciphered:
            return virtual_response_envelope

        if not route_data.virtual_session_id:
            raise ValueError("virtual_session_id e obrigatorio para responder com virtual cifrado.")

        session = services.session_manager.get_session_by_session_id(route_data.virtual_session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            raise ValueError("virtual session nao encontrada ou inativa para cifrar a resposta.")

        plaintext_hex = json.dumps(
            virtual_response_envelope,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8").hex()
        encrypted_virtual_envelope = aes_encrypt_hex(plaintext_hex, session.shared_secret_hex)
        services.session_manager.touch_session(route_data.virtual_session_id)
        return encrypted_virtual_envelope.payload_hex

    def _resolve_local_virtual_envelope(
        self,
        *,
        services: EngineServices,
        route_data: "RouteExecuteData",
    ) -> dict[str, object]:
        if not route_data.virtual_envelope_ciphered:
            if isinstance(route_data.virtual_envelope, dict):
                return route_data.virtual_envelope
            raise ValueError("virtual_envelope plaintext invalido.")

        if not route_data.virtual_session_id:
            raise ValueError("virtual_session_id e obrigatorio para virtual_envelope cifrado.")
        if not isinstance(route_data.virtual_envelope, str) or not route_data.virtual_envelope:
            raise ValueError("virtual_envelope cifrado invalido.")

        session = services.session_manager.get_session_by_session_id(route_data.virtual_session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            raise ValueError("virtual session nao encontrada ou inativa para decifrar o envelope.")

        plaintext_json = bytes.fromhex(
            aes_decrypt_hex(route_data.virtual_envelope, session.shared_secret_hex)
        ).decode("utf-8")
        virtual_envelope = json.loads(plaintext_json)
        if not isinstance(virtual_envelope, dict):
            raise ValueError("virtual_envelope decifrado precisa ser um objeto.")

        services.session_manager.touch_session(route_data.virtual_session_id)
        return virtual_envelope

    def _build_nested_virtual_envelope(
        self,
        *,
        outer_envelope,
        route_data: "RouteExecuteData",
        virtual_envelope: dict[str, object],
    ) -> ProtocolEnvelope:
        header = virtual_envelope.get("header")
        payload = virtual_envelope.get("payload")
        if not isinstance(header, dict):
            raise ValueError("virtual_envelope.header precisa ser um objeto.")
        if payload is None:
            payload = {}

        if route_data.virtual_session_id and not header.get("virtual_session_id"):
            header = {
                **header,
                "virtual_session_id": route_data.virtual_session_id,
            }
        if envelope_physical_session_id := outer_envelope.header.get("physical_session_id"):
            if not header.get("physical_session_id"):
                header = {
                    **header,
                    "physical_session_id": envelope_physical_session_id,
                }

        message_type = header.get("message_type")
        if message_type is not None and not isinstance(message_type, str):
            raise ValueError("virtual_envelope.header.message_type precisa ser uma string.")

        nested_packet = {
            "header": header,
            "payload": payload,
        }
        raw_payload = json.dumps(
            nested_packet,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return ProtocolEnvelope(
            protocol_name=outer_envelope.protocol_name,
            message_type=message_type,
            payload=payload,
            raw_payload=raw_payload,
            header=header,
        )

    def _build_nested_virtual_context(
        self,
        *,
        context: PacketContext,
        envelope,
        route_data: "RouteExecuteData",
    ) -> PacketContext:
        return PacketContext(
            transport_name=context.transport_name,
            payload=context.payload,
            remote_host=context.remote_host,
            remote_port=context.remote_port,
            local_host=context.local_host,
            local_port=context.local_port,
            connection_id=context.connection_id,
            received_at=context.received_at,
            metadata={
                **context.metadata,
                "route_path_id": route_data.path_id,
                "route_message_type": envelope.message_type,
                "virtual_session_id": route_data.virtual_session_id,
                "virtual_envelope_ciphered": route_data.virtual_envelope_ciphered,
            },
        )

    def _resolve_route_data_path(
        self,
        *,
        services: EngineServices,
        route_data: "RouteExecuteData",
    ) -> "ResolvedRouteExecutePath":
        forward_mapping = services.route_service.get_resolution_by_received_path_id(
            received_path_id=route_data.path_id,
        )
        if forward_mapping is not None:
            return ResolvedRouteExecutePath(
                action="forward_vn_to_pn",
                next_path_id=forward_mapping.generated_path_id,
                target_remote_physical_node_id=forward_mapping.to_physical_node_id,
            )

        reverse_mapping = services.route_service.get_resolution_by_generated_path_id(
            generated_path_id=route_data.path_id,
        )
        if reverse_mapping is not None:
            return ResolvedRouteExecutePath(
                action="forward_pn_to_vn",
                next_path_id=reverse_mapping.received_path_id,
                target_remote_physical_node_id=reverse_mapping.from_physical_node_id,
            )

        return ResolvedRouteExecutePath(action="deliver_local")

    @staticmethod
    def _build_invalid_result(
        envelope,
        *,
        reason: str,
    ) -> PacketProcessingResult:
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=False,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": "route_execute",
                "reason": reason,
            },
        )

    @staticmethod
    def _read_virtual_message_type(virtual_envelope: dict[str, object]) -> str | None:
        header = virtual_envelope.get("header")
        if not isinstance(header, dict):
            return None

        message_type = header.get("message_type")
        return message_type if isinstance(message_type, str) and message_type else None


@dataclass(slots=True, frozen=True)
class RouteExecuteData:
    path_id: str
    virtual_session_id: str | None
    virtual_envelope_ciphered: bool
    virtual_envelope: object

    def to_payload(
        self,
        *,
        path_id: str,
    ) -> dict[str, object]:
        return {
            "path_id": path_id,
            "virtual_session_id": self.virtual_session_id,
            "virtual_envelope_ciphered": self.virtual_envelope_ciphered,
            "virtual_envelope": self.virtual_envelope,
        }


@dataclass(slots=True, frozen=True)
class ResolvedRouteExecutePath:
    action: str
    next_path_id: str | None = None
    target_remote_physical_node_id: str | None = None


@dataclass(slots=True, frozen=True)
class LocalRouteReplyContext:
    target_remote_physical_node_id: str
    reply_path_id: str


def _read_required_string(payload: dict[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser uma string nao vazia.")


def _read_optional_string(payload: dict[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"O campo '{field_name}' precisa ser uma string nao vazia quando informado.")


def _read_required_bool(payload: dict[str, object], field_name: str) -> bool:
    value = payload.get(field_name)
    if isinstance(value, bool):
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser um booleano.")
