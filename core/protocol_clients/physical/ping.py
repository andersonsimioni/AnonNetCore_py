from __future__ import annotations

import asyncio
from time import perf_counter
from uuid import uuid4

from transport import OutboundMessage, TransportEndpoint

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
            remote_physical_node_id
        )
        if not endpoints:
            raise ValueError("O physical node remoto nao possui endpoints conhecidos.")

        last_error: Exception | None = None
        for endpoint_data in endpoints:
            endpoint = TransportEndpoint(
                transport_name=endpoint_data.transport,
                host=endpoint_data.host,
                port=endpoint_data.port,
            )
            try:
                result = await self._ping_endpoint(endpoint=endpoint)
                return {
                    "remote_physical_node_id": remote_physical_node_id,
                    **result,
                }
            except Exception as error:
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
            return

        future.set_result(pong_data)
