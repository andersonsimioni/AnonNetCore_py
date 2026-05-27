from __future__ import annotations

import asyncio
from time import perf_counter
from uuid import uuid4

from transport import OutboundMessage, build_transport_endpoint_from_result

from ...protocols import PingProtocolHandler


class PhysicalPingClient:
    """Cliente ativo para PING/PONG entre physical nodes."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._pong_timeout_seconds = self.engine.services.config.physical_ping_timeout_seconds
        self._pending_pongs: dict[str, asyncio.Future[dict[str, object]]] = {}

    async def ping_physical_node(
        self,
        *,
        remote_physical_node_id: str,
    ) -> dict[str, object]:
        remote_node = self.engine.services.identity_service.get_remote_physical_node_by_id(
            remote_physical_node_id
        )
        if remote_node is None:
            raise ValueError("O physical node remoto ainda nao foi persistido no banco local.")

        endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
            remote_physical_node_id,
            only_active=True,
        )
        if not endpoints:
            raise ValueError("O physical node remoto nao possui endpoints conhecidos.")

        last_error: Exception | None = None
        for endpoint_data in endpoints:
            if not self.engine.services.transport.has_adapter(endpoint_data.transport):
                self.engine.services.log_service.debug(
                    "physical_ping_client",
                    "skipping ping endpoint without registered transport adapter",
                    remote_physical_node_id=remote_physical_node_id,
                    transport=endpoint_data.transport,
                    host=endpoint_data.host,
                    port=endpoint_data.port,
                )
                continue
            endpoint = build_transport_endpoint_from_result(endpoint_data)
            self.engine.services.log_service.info(
                "physical_ping_client",
                "trying ping on remote endpoint",
                remote_physical_node_id=remote_physical_node_id,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
            )
            try:
                result = await self._ping_endpoint(endpoint=endpoint)
                self.engine.services.log_service.info(
                    "physical_ping_client",
                    "ping succeeded",
                    remote_physical_node_id=remote_physical_node_id,
                    observed_rtt_ms=result.get("observed_rtt_ms"),
                    transport=result.get("transport_name"),
                    host=result.get("remote_host"),
                    port=result.get("remote_port"),
                )
                return {
                    "remote_physical_node_id": remote_physical_node_id,
                    **result,
                }
            except Exception as error:
                self.engine.services.identity_service.mark_remote_physical_node_validation_failure(
                    node_id=remote_physical_node_id,
                    transport=endpoint.transport_name,
                    host=endpoint.host,
                    port=endpoint.port,
                )
                self.engine.services.log_service.warning(
                    "physical_ping_client",
                    "ping failed on remote endpoint",
                    remote_physical_node_id=remote_physical_node_id,
                    transport=endpoint.transport_name,
                    host=endpoint.host,
                    port=endpoint.port,
                    error_type=type(error).__name__,
                    error=repr(error),
                )
                last_error = error
                continue

        if last_error is not None:
            raise last_error

        raise RuntimeError("Nao foi possivel executar ping em nenhum endpoint conhecido.")

    async def _ping_endpoint(
        self,
        *,
        endpoint: TransportEndpoint,
    ) -> dict[str, object]:
        nonce = str(uuid4())
        header = self.engine.build_message_header(message_type="PING")
        payload = PingProtocolHandler.build_ping_payload(
            header=header,
            nonce=nonce,
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, object]] = loop.create_future()
        self._pending_pongs[header["message_id"]] = future
        started_at = perf_counter()

        try:
            await self.engine.send_packet(
                OutboundMessage(
                    transport_name=endpoint.transport_name,
                    payload=payload,
                    remote_endpoint=endpoint,
                )
            )
            self.engine.services.log_service.debug(
                "physical_ping_client",
                "sent ping",
                message_id=header["message_id"],
                nonce=nonce,
                transport=endpoint.transport_name,
                host=endpoint.host,
                port=endpoint.port,
            )
            pong_data = await asyncio.wait_for(future, timeout=self._pong_timeout_seconds)
        finally:
            self._pending_pongs.pop(header["message_id"], None)

        if pong_data.get("nonce") != nonce:
            raise ValueError("O nonce do PONG nao corresponde ao PING enviado.")

        observed_rtt_ms = (perf_counter() - started_at) * 1000.0
        return {
            "status": "pong",
            "nonce": nonce,
            "observed_rtt_ms": observed_rtt_ms,
            "transport_name": pong_data.get("transport_name"),
            "remote_host": pong_data.get("remote_host"),
            "remote_port": pong_data.get("remote_port"),
        }

    def complete_pong(
        self,
        *,
        response_to_message_id: str,
        pong_data: dict[str, object],
    ) -> None:
        future = self._pending_pongs.pop(response_to_message_id, None)
        if future is None or future.done():
            self.engine.services.log_service.debug(
                "physical_ping_client",
                "received pong for unknown or completed ping",
                response_to_message_id=response_to_message_id,
                remote_host=pong_data.get("remote_host"),
                remote_port=pong_data.get("remote_port"),
            )
            return

        future.set_result(pong_data)
        self.engine.services.log_service.debug(
            "physical_ping_client",
            "completed pending pong future",
            response_to_message_id=response_to_message_id,
        )
