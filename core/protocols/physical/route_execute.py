from __future__ import annotations

from dataclasses import dataclass

from ...models import PacketProcessingResult
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
        del context

        route_data = self._parse_route_data(envelope.payload)
        resolution = self._resolve_route_data_path(
            services=services,
            route_data=route_data,
        )

        if resolution.action == "deliver_local":
            return PacketProcessingResult(
                protocol_name=envelope.protocol_name,
                handled=True,
                message_type=envelope.message_type,
                metadata={
                    "protocol_family": self.protocol_family,
                    "route_data_action": "deliver_local",
                    "path_id": route_data.path_id,
                    "payload": route_data.payload,
                },
            )

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
            payload=payload_dict.get("payload"),
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


@dataclass(slots=True, frozen=True)
class RouteExecuteData:
    path_id: str
    payload: object

    def to_payload(
        self,
        *,
        path_id: str,
    ) -> dict[str, object]:
        return {
            "path_id": path_id,
            "payload": self.payload,
        }


@dataclass(slots=True, frozen=True)
class ResolvedRouteExecutePath:
    action: str
    next_path_id: str | None = None
    target_remote_physical_node_id: str | None = None


def _read_required_string(payload: dict[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser uma string nao vazia.")
