from __future__ import annotations

from ...models import PacketProcessingResult
from ...services import EngineServices
from ..base import ProtocolMessageHandler


class RoutingProtocolHandler(ProtocolMessageHandler):
    protocol_family = "routing"
    supported_message_types = {
        "ROUTE_CREATE",
        "ROUTE_CREATE_RETURN",
        "ROUTE_CREATE_OK",
        "ROUTE_CREATE_FAIL",
        "ROUTE_DATA",
        "ROUTE_DATA_ACK",
        "ROUTE_KEEPALIVE",
        "ROUTE_KEEPALIVE_ACK",
        "ROUTE_CLOSE",
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

        handlers = {
            "ROUTE_CREATE": strategy.handle_route_create,
            "ROUTE_CREATE_RETURN": strategy.handle_route_create_return,
            "ROUTE_CREATE_OK": strategy.handle_route_create_ok,
            "ROUTE_CREATE_FAIL": strategy.handle_route_create_fail,
            "ROUTE_DATA": strategy.handle_route_data,
            "ROUTE_DATA_ACK": strategy.handle_route_data_ack,
            "ROUTE_KEEPALIVE": strategy.handle_route_keepalive,
            "ROUTE_KEEPALIVE_ACK": strategy.handle_route_keepalive_ack,
            "ROUTE_CLOSE": strategy.handle_route_close,
        }
        handler = handlers.get(envelope.message_type)
        if handler is None:
            return self._build_invalid_result(
                envelope,
                reason="unsupported_routing_message_type",
            )

        return await handler(
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
