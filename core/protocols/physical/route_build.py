from __future__ import annotations

from ...models import PacketProcessingResult
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class RouteBuildProtocolHandler(ProtocolMessageHandler):
    protocol_family = "route_build"
    supported_message_types = {
        "ROUTE_CREATE",
        "ROUTE_CREATE_KEM_INFO",
        "ROUTE_CREATE_VALIDATE_AND_PUBLISH",
        "ROUTE_CREATE_OK",
        "ROUTE_CREATE_PING",
        "ROUTE_CREATE_PONG",
    }

    async def handle(
        self,
        envelope,
        context,
        services: EngineServices,
    ) -> PacketProcessingResult:
        strategy = self._require_route_strategy(envelope, services)
        if isinstance(strategy, PacketProcessingResult):
            return strategy

        route_handler = self._resolve_route_handler(strategy, envelope.message_type)
        if route_handler is None:
            return self._build_invalid_result(
                envelope,
                reason=f"unsupported_route_build_message_type:{envelope.message_type}",
            )

        return await route_handler(
            envelope=envelope,
            context=context,
            services=services,
        )

    def _require_route_strategy(
        self,
        envelope,
        services: EngineServices,
    ):
        if services.route_strategies is None:
            return self._build_invalid_result(
                envelope,
                reason="route_strategies_not_initialized",
            )

        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        strategy_name = payload.get("route_strategy")
        if not isinstance(strategy_name, str) or not strategy_name:
            return self._build_invalid_result(
                envelope,
                reason="missing_route_strategy",
            )

        try:
            return services.route_strategies.require(strategy_name)
        except Exception as error:
            return self._build_invalid_result(
                envelope,
                reason=f"unsupported_route_strategy:{error}",
            )

    def _build_invalid_result(
        self,
        envelope,
        *,
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

    @staticmethod
    def _resolve_route_handler(strategy, message_type: str):
        if message_type == "ROUTE_CREATE":
            return strategy.handle_route_create
        if message_type == "ROUTE_CREATE_KEM_INFO":
            return strategy.handle_route_create_kem_info
        if message_type == "ROUTE_CREATE_VALIDATE_AND_PUBLISH":
            return strategy.handle_route_create_validate_and_publish
        if message_type == "ROUTE_CREATE_OK":
            return strategy.handle_route_create_ok
        if message_type == "ROUTE_CREATE_PING":
            return strategy.handle_route_create_ping
        if message_type == "ROUTE_CREATE_PONG":
            return strategy.handle_route_create_pong
        return None
