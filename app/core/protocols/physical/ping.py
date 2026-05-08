from __future__ import annotations

import json
from uuid import uuid4

from ...models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class PingProtocolHandler(ProtocolMessageHandler):
    protocol_family = "ping"
    supported_message_types = {
        "PING",
        "PONG",
    }

    @staticmethod
    def build_ping_payload(
        *,
        header: dict[str, object],
        nonce: str,
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": {
                    "nonce": nonce,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def build_pong_payload(
        *,
        request_header: dict[str, object],
        nonce: str,
    ) -> bytes:
        return json.dumps(
            {
                "header": {
                    "version": request_header.get("version", 1),
                    "message_type": "PONG",
                    "message_id": str(uuid4()),
                    "response_to_message_id": request_header.get("message_id"),
                    "message_sequence": request_header.get("message_sequence"),
                    "physical_session_id": request_header.get("physical_session_id"),
                    "virtual_session_id": request_header.get("virtual_session_id"),
                },
                "payload": {
                    "nonce": nonce,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    async def handle(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        if envelope.message_type == "PING":
            return await self._handle_ping(envelope, context, services)

        if envelope.message_type == "PONG":
            return await self._handle_pong(envelope, context, services)

        return self._build_invalid_result(envelope, "unsupported_ping_message_type")

    async def _handle_ping(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        nonce = _read_required_string(payload, "nonce")
        if nonce is None:
            services.log_service.warning(
                "physical_ping",
                "received invalid ping payload",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
            )
            return self._build_invalid_result(envelope, "invalid_ping_payload")

        services.log_service.debug(
            "physical_ping",
            "received ping and sending pong",
            nonce=nonce,
            remote_host=context.remote_host,
            remote_port=context.remote_port,
        )
        response_payload = self.build_pong_payload(
            request_header=envelope.header,
            nonce=nonce,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            response_payload=response_payload,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "respond_pong",
            },
        )

    async def _handle_pong(
        self,
        envelope: ProtocolEnvelope,
        context: PacketContext,
        services: EngineServices,
    ) -> PacketProcessingResult:
        payload = _as_payload_dict(envelope)
        nonce = _read_required_string(payload, "nonce")
        response_to_message_id = envelope.header.get("response_to_message_id")
        if (
            nonce is None
            or not isinstance(response_to_message_id, str)
            or services.protocol_clients is None
        ):
            services.log_service.warning(
                "physical_ping",
                "received invalid pong payload",
                remote_host=context.remote_host,
                remote_port=context.remote_port,
            )
            return self._build_invalid_result(envelope, "invalid_pong_payload")

        services.protocol_clients.physical.ping.complete_pong(
            response_to_message_id=response_to_message_id,
            pong_data={
                "nonce": nonce,
                "remote_host": context.remote_host,
                "remote_port": context.remote_port,
                "transport_name": context.transport_name,
            },
        )
        services.log_service.debug(
            "physical_ping",
            "resolved pending pong",
            nonce=nonce,
            response_to_message_id=response_to_message_id,
            remote_host=context.remote_host,
            remote_port=context.remote_port,
        )
        return PacketProcessingResult(
            protocol_name=envelope.protocol_name,
            handled=True,
            message_type=envelope.message_type,
            metadata={
                "protocol_family": self.protocol_family,
                "transport_name": context.transport_name,
                "action": "resolve_pending_pong",
                "response_to_message_id": response_to_message_id,
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
            },
        )


def _as_payload_dict(envelope: ProtocolEnvelope) -> dict[str, object]:
    return envelope.payload if isinstance(envelope.payload, dict) else {}


def _read_required_string(payload: dict[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    if isinstance(value, str) and value:
        return value
    return None
