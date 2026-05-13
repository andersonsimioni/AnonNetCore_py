from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .service import CoreApiError, CoreApiService


@dataclass(slots=True)
class WebSocketApiClient:
    websocket: Any
    subscriptions: set[str] = field(default_factory=set)


class CoreWebSocketApiServer:
    """WebSocket para entregar eventos da API assim que chegam ao core."""

    def __init__(
        self,
        api_service: CoreApiService,
        *,
        host: str = "127.0.0.1",
        port: int = 18081,
        path: str = "/v1/events",
    ) -> None:
        self.api_service = api_service
        self.host = host
        self.port = port
        self.path = path
        self._server = None
        self._clients: list[WebSocketApiClient] = []

    async def start(self) -> None:
        if self._server is not None:
            return

        from websockets.legacy.server import serve

        self.api_service.add_virtual_message_sink(self._send_virtual_message_event)
        self._server = await serve(self._handle_connection, self.host, self.port)

    async def stop(self) -> None:
        if self._server is None:
            return

        self.api_service.remove_virtual_message_sink(self._send_virtual_message_event)
        for client in list(self._clients):
            await self._close_client(client)

        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_connection(self, websocket, path: str) -> None:
        if path != self.path:
            await websocket.close(code=1008, reason="invalid_path")
            return

        client = WebSocketApiClient(websocket=websocket)
        self._clients.append(client)
        await self._send_json(
            websocket,
            {
                "type": "connected",
                "data": {
                    "path": self.path,
                },
            },
        )

        try:
            async for raw_message in websocket:
                await self._handle_client_message(client, raw_message)
        finally:
            self._remove_client(client)

    async def _handle_client_message(
        self,
        client: WebSocketApiClient,
        raw_message: str,
    ) -> None:
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            await self._send_error(client.websocket, "invalid_json", "Mensagem JSON invalida.")
            return

        if not isinstance(message, dict):
            await self._send_error(client.websocket, "invalid_message", "Mensagem precisa ser objeto.")
            return

        message_type = message.get("type")
        if message_type == "subscribe":
            await self._subscribe_client(client, message)
            return
        if message_type == "unsubscribe":
            await self._unsubscribe_client(client, message)
            return
        if message_type == "ping":
            await self._send_json(client.websocket, {"type": "pong", "data": {}})
            return

        await self._send_error(client.websocket, "unknown_message_type", "Tipo de mensagem desconhecido.")

    async def _subscribe_client(
        self,
        client: WebSocketApiClient,
        message: dict[str, object],
    ) -> None:
        app_message_types = self._extract_app_message_types(message)
        if not app_message_types:
            await self._send_error(
                client.websocket,
                "app_message_type_required",
                "Informe app_message_type ou app_message_types.",
            )
            return

        try:
            for app_message_type in app_message_types:
                self.api_service.ensure_virtual_message_handler(app_message_type)
                client.subscriptions.add(app_message_type)
        except CoreApiError as error:
            await self._send_error(client.websocket, error.code, error.message)
            return

        await self._send_json(
            client.websocket,
            {
                "type": "subscribed",
                "data": {
                    "app_message_types": sorted(client.subscriptions),
                },
            },
        )

    async def _unsubscribe_client(
        self,
        client: WebSocketApiClient,
        message: dict[str, object],
    ) -> None:
        for app_message_type in self._extract_app_message_types(message):
            client.subscriptions.discard(app_message_type)

        await self._send_json(
            client.websocket,
            {
                "type": "unsubscribed",
                "data": {
                    "app_message_types": sorted(client.subscriptions),
                },
            },
        )

    async def _send_virtual_message_event(self, event: dict[str, object]) -> None:
        data = event.get("data")
        if not isinstance(data, dict):
            return

        app_message_type = data.get("app_message_type")
        if not isinstance(app_message_type, str) or not app_message_type:
            return

        dead_clients: list[WebSocketApiClient] = []
        for client in list(self._clients):
            if app_message_type not in client.subscriptions:
                continue
            try:
                await self._send_json(client.websocket, event)
            except Exception:
                dead_clients.append(client)

        for client in dead_clients:
            self._remove_client(client)

    async def _close_client(self, client: WebSocketApiClient) -> None:
        self._remove_client(client)
        try:
            await client.websocket.close(code=1001, reason="server_stopping")
        except Exception:
            pass

    def _remove_client(self, client: WebSocketApiClient) -> None:
        try:
            self._clients.remove(client)
        except ValueError:
            pass

    @staticmethod
    async def _send_json(websocket, payload: dict[str, object]) -> None:
        await websocket.send(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    async def _send_error(self, websocket, code: str, message: str) -> None:
        await self._send_json(
            websocket,
            {
                "type": "error",
                "error": {
                    "code": code,
                    "message": message,
                },
            },
        )

    @staticmethod
    def _extract_app_message_types(message: dict[str, object]) -> list[str]:
        single_type = message.get("app_message_type")
        if isinstance(single_type, str) and single_type:
            return [single_type]

        multiple_types = message.get("app_message_types")
        if not isinstance(multiple_types, list):
            return []

        return [
            app_message_type
            for app_message_type in multiple_types
            if isinstance(app_message_type, str) and app_message_type
        ]
