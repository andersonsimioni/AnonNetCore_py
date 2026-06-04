from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from .service import CoreApiError, CoreApiService


class CoreHttpApiServer:
    """HTTP JSON fino sobre o CoreApiService, sem dependencias externas."""

    def __init__(
        self,
        api_service: CoreApiService,
        *,
        host: str = "127.0.0.1",
        port: int = 18080,
        cors_allow_origin: str = "*",
    ) -> None:
        self.api_service = api_service
        self.host = host
        self.port = port
        self.cors_allow_origin = cors_allow_origin
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(self._handle_connection, self.host, self.port)

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await self._read_request(reader)
            status_code, payload = await self._dispatch(request)
            await self._write_json_response(writer, status_code, payload)
        except CoreApiError as error:
            await self._write_json_response(
                writer,
                error.status_code,
                self._error_payload(error.code, error.message),
            )
        except ValueError as error:
            await self._write_json_response(
                writer,
                400,
                self._error_payload("invalid_request", str(error)),
            )
        except Exception as error:
            await self._write_json_response(
                writer,
                500,
                self._error_payload("internal_error", str(error)),
            )
        finally:
            writer.close()
            await writer.wait_closed()

    async def _read_request(self, reader: asyncio.StreamReader) -> "HttpRequest":
        request_line = (await reader.readline()).decode("utf-8", errors="replace").strip()
        if not request_line:
            raise CoreApiError("empty_request", "Request vazia.", status_code=400)

        parts = request_line.split()
        if len(parts) != 3:
            raise CoreApiError("invalid_request_line", "Request line invalida.", status_code=400)

        method, raw_target, _http_version = parts
        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if line in {b"\r\n", b"\n", b""}:
                break
            header_line = line.decode("utf-8", errors="replace").strip()
            name, _, value = header_line.partition(":")
            if name:
                headers[name.lower()] = value.strip()

        content_length = self._parse_content_length(headers.get("content-length"))
        body = await reader.readexactly(content_length) if content_length > 0 else b""
        parsed_target = urlparse(raw_target)
        return HttpRequest(
            method=method.upper(),
            path=parsed_target.path,
            query=parse_qs(parsed_target.query),
            headers=headers,
            body=body,
        )

    async def _dispatch(self, request: "HttpRequest") -> tuple[int, dict[str, object]]:
        if request.method == "OPTIONS":
            return 200, {"ok": True, "data": {}}

        routes: dict[tuple[str, str], Callable[[HttpRequest], Awaitable[object] | object]] = {
            ("GET", "/health"): self._health,
            ("GET", "/debug/state"): self._debug_state,
            ("GET", "/v1/status"): self._status,
            ("GET", "/v1/virtual-nodes/local"): self._list_local_virtual_nodes,
            ("GET", "/v1/virtual-nodes/remote"): self._list_remote_virtual_nodes,
            ("POST", "/v1/virtual-nodes"): self._create_local_virtual_node,
            ("POST", "/v1/virtual-nodes/remote"): self._upsert_remote_virtual_node,
            ("POST", "/v1/dht/publish"): self._dht_publish,
            ("POST", "/v1/dht/publish-jobs"): self._dht_publish_job,
            ("POST", "/v1/dht/query"): self._dht_query,
            ("POST", "/v1/dht/key"): self._dht_key,
            ("POST", "/v1/virtual-nodes/local/sign"): self._sign_local_virtual_node_payload,
            ("POST", "/v1/virtual-nodes/verify-signature"): self._verify_virtual_node_payload_signature,
            ("GET", "/v1/sessions/virtual"): self._list_virtual_sessions,
            ("POST", "/v1/sessions/virtual"): self._start_virtual_session,
            ("POST", "/v1/messages/virtual/subscribe"): self._subscribe_virtual_messages,
            ("GET", "/v1/messages/virtual"): self._read_virtual_messages,
            ("GET", "/v1/content"): self._list_content,
            ("POST", "/v1/content"): self._store_content,
            ("GET", "/v1/downloads"): self._list_downloads,
            ("POST", "/v1/downloads"): self._start_download,
        }
        handler = routes.get((request.method, request.path))
        if handler is None and request.method == "POST":
            handler = (
                self._match_virtual_message_sender(request.path)
                or self._match_virtual_session_closer(request.path)
                or self._match_content_provider_publisher(request.path)
            )
        if handler is None and request.method == "GET":
            handler = (
                self._match_content_getter(request.path)
                or self._match_content_range_getter(request.path)
                or self._match_download_getter(request.path)
                or self._match_dht_publish_job_getter(request.path)
            )
        if handler is None:
            raise CoreApiError("route_not_found", "API route not found.", status_code=404)

        result = handler(request)
        if isinstance(result, Awaitable):
            result = await result
        return 200, {"ok": True, "data": result}

    def _health(self, _request: "HttpRequest") -> dict[str, object]:
        return {"status": "ok"}

    def _status(self, _request: "HttpRequest") -> dict[str, object]:
        return self.api_service.get_status()

    def _debug_state(self, _request: "HttpRequest") -> dict[str, object]:
        return self.api_service.get_debug_state()

    def _list_local_virtual_nodes(self, request: "HttpRequest") -> list[dict[str, object]]:
        return self.api_service.list_local_virtual_nodes(
            only_active=self._query_bool(request, "only_active", default=False),
        )

    def _list_remote_virtual_nodes(self, request: "HttpRequest") -> list[dict[str, object]]:
        return self.api_service.list_remote_virtual_nodes(
            status=self._first_query_value(request, "status"),
        )

    def _create_local_virtual_node(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.create_local_virtual_node(
            kind=str(body.get("kind") or "default"),
            expires_at=self._optional_str(body.get("expires_at")),
            is_active=bool(body.get("is_active", True)),
            metadata_json=self._optional_str(body.get("metadata_json")),
            metadata=self._optional_dict(body.get("metadata")),
        )

    def _upsert_remote_virtual_node(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.upsert_remote_virtual_node(
            public_key=str(body.get("public_key") or ""),
            node_id=self._optional_str(body.get("node_id")),
            kind=str(body.get("kind") or "default"),
            status=str(body.get("status") or "active"),
            expires_at=self._optional_str(body.get("expires_at")),
            metadata_json=self._optional_str(body.get("metadata_json")),
            metadata=self._optional_dict(body.get("metadata")),
        )

    async def _dht_publish(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return await self.api_service.dht_publish(
            namespace=str(body.get("namespace") or ""),
            logical_key=str(body.get("logical_key") or ""),
            record_json=self._optional_str(body.get("record_json")),
            record=self._optional_dict(body.get("record")),
            expires_at=self._optional_str(body.get("expires_at")),
        )

    def _dht_publish_job(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.start_dht_publish_job(
            namespace=str(body.get("namespace") or ""),
            logical_key=str(body.get("logical_key") or ""),
            record_json=self._optional_str(body.get("record_json")),
            record=self._optional_dict(body.get("record")),
            expires_at=self._optional_str(body.get("expires_at")),
        )

    async def _dht_query(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return await self.api_service.dht_query(
            namespace=str(body.get("namespace") or ""),
            logical_key=str(body.get("logical_key") or ""),
        )

    def _dht_key(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.build_dht_key(
            namespace=str(body.get("namespace") or ""),
            logical_key=str(body.get("logical_key") or ""),
        )

    def _sign_local_virtual_node_payload(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.sign_local_virtual_node_payload(
            local_virtual_node_id=str(body.get("local_virtual_node_id") or ""),
            payload=self._optional_dict(body.get("payload")) or {},
        )

    def _verify_virtual_node_payload_signature(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.verify_virtual_node_payload_signature(
            public_key=str(body.get("public_key") or ""),
            payload=self._optional_dict(body.get("payload")) or {},
            signature_hex=str(body.get("signature_hex") or ""),
        )

    def _list_virtual_sessions(self, _request: "HttpRequest") -> list[dict[str, object]]:
        return self.api_service.list_virtual_sessions()

    async def _start_virtual_session(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return await self.api_service.start_virtual_session(
            local_virtual_node_id=str(body.get("local_virtual_node_id") or ""),
            remote_virtual_node_id=str(body.get("remote_virtual_node_id") or ""),
        )

    def _subscribe_virtual_messages(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.subscribe_virtual_messages(
            app_message_type=str(body.get("app_message_type") or ""),
        )

    def _read_virtual_messages(self, request: "HttpRequest") -> list[dict[str, object]]:
        return self.api_service.read_virtual_messages(
            app_message_type=self._first_query_value(request, "app_message_type"),
            limit=self._query_int(request, "limit", default=100),
            consume=self._query_bool(request, "consume", default=True),
        )

    def _list_content(self, request: "HttpRequest") -> list[dict[str, object]]:
        return self.api_service.list_content(limit=self._query_int(request, "limit", default=100))

    def _store_content(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return self.api_service.store_content(
            data_base64=str(body.get("data_base64") or ""),
            title=self._optional_str(body.get("title")),
            content_type=str(body.get("content_type") or "application/octet-stream"),
            tags=self._optional_list_of_strings(body.get("tags")) or [],
            is_encrypted=bool(body.get("is_encrypted", False)),
            encryption_scheme=self._optional_str(body.get("encryption_scheme")),
        )

    async def _start_download(self, request: "HttpRequest") -> dict[str, object]:
        body = self._json_body(request)
        return await self.api_service.start_content_download(
            session_id=str(body.get("session_id") or ""),
            content_id=str(body.get("content_id") or ""),
            ddt_key=self._optional_str(body.get("ddt_key")),
        )

    def _list_downloads(self, request: "HttpRequest") -> list[dict[str, object]]:
        return self.api_service.list_content_downloads(
            session_id=self._first_query_value(request, "session_id"),
        )

    def _match_virtual_message_sender(
        self,
        path: str,
    ) -> Callable[[HttpRequest], Awaitable[dict[str, object]]] | None:
        prefix = "/v1/sessions/virtual/"
        suffix = "/messages"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None

        session_id = path[len(prefix):-len(suffix)].strip("/")
        if not session_id:
            return None

        async def _send(request: HttpRequest) -> dict[str, object]:
            body = self._json_body(request)
            return await self.api_service.send_virtual_message(
                session_id=session_id,
                app_message_type=str(body.get("app_message_type") or ""),
                payload=self._optional_dict(body.get("payload")) or {},
                request_id=self._optional_str(body.get("request_id")),
            )

        return _send

    def _match_virtual_session_closer(
        self,
        path: str,
    ) -> Callable[[HttpRequest], Awaitable[dict[str, object]]] | None:
        prefix = "/v1/sessions/virtual/"
        suffix = "/close"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None

        session_id = path[len(prefix):-len(suffix)].strip("/")
        if not session_id:
            return None

        async def _close(request: HttpRequest) -> dict[str, object]:
            body = self._json_body(request)
            return await self.api_service.close_virtual_session(
                session_id=session_id,
                close_reason=str(body.get("close_reason") or "api_closed"),
            )

        return _close

    def _match_content_provider_publisher(
        self,
        path: str,
    ) -> Callable[[HttpRequest], Awaitable[dict[str, object]]] | None:
        prefix = "/v1/content/"
        suffix = "/providers/ddt"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None

        content_id = path[len(prefix):-len(suffix)].strip("/")
        if not content_id:
            return None

        async def _publish(request: HttpRequest) -> dict[str, object]:
            body = self._json_body(request)
            return await self.api_service.publish_content_provider(
                content_id=content_id,
                local_virtual_node_id=str(body.get("local_virtual_node_id") or ""),
                ttl_seconds=self._optional_int(body.get("ttl_seconds")),
                async_publish=bool(body.get("async_publish", False)),
            )

        return _publish

    def _match_dht_publish_job_getter(
        self,
        path: str,
    ) -> Callable[[HttpRequest], dict[str, object]] | None:
        prefix = "/v1/dht/publish-jobs/"
        if not path.startswith(prefix):
            return None

        job_id = path[len(prefix):].strip("/")
        if not job_id or "/" in job_id:
            return None

        def _get(_request: HttpRequest) -> dict[str, object]:
            return self.api_service.get_dht_publish_job(job_id=job_id)

        return _get

    def _match_content_getter(
        self,
        path: str,
    ) -> Callable[[HttpRequest], dict[str, object]] | None:
        prefix = "/v1/content/"
        if not path.startswith(prefix) or path.endswith("/range"):
            return None

        content_id = path[len(prefix):].strip("/")
        if not content_id or "/" in content_id:
            return None

        def _get(_request: HttpRequest) -> dict[str, object]:
            return self.api_service.get_content_info(content_id=content_id)

        return _get

    def _match_content_range_getter(
        self,
        path: str,
    ) -> Callable[[HttpRequest], dict[str, object]] | None:
        prefix = "/v1/content/"
        suffix = "/range"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None

        content_id = path[len(prefix):-len(suffix)].strip("/")
        if not content_id:
            return None

        def _get_range(request: HttpRequest) -> dict[str, object]:
            return self.api_service.read_content_range(
                content_id=content_id,
                start_byte=self._query_int(request, "start_byte", default=0),
                end_byte=self._query_int(request, "end_byte", default=0),
            )

        return _get_range

    def _match_download_getter(
        self,
        path: str,
    ) -> Callable[[HttpRequest], dict[str, object]] | None:
        prefix = "/v1/downloads/"
        if not path.startswith(prefix):
            return None

        parts = path[len(prefix):].strip("/").split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None

        def _get_download(_request: HttpRequest) -> dict[str, object]:
            return self.api_service.get_content_download(
                session_id=parts[0],
                content_id=parts[1],
            )

        return _get_download

    async def _write_json_response(
        self,
        writer: asyncio.StreamWriter,
        status_code: int,
        payload: dict[str, object],
    ) -> None:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        reason = self._reason_phrase(status_code)
        headers = [
            f"HTTP/1.1 {status_code} {reason}",
            "Content-Type: application/json; charset=utf-8",
            f"Content-Length: {len(body)}",
            f"Access-Control-Allow-Origin: {self.cors_allow_origin}",
            "Access-Control-Allow-Methods: GET,POST,OPTIONS",
            "Access-Control-Allow-Headers: Content-Type",
            "Connection: close",
            "",
            "",
        ]
        writer.write("\r\n".join(headers).encode("utf-8") + body)
        await writer.drain()

    @staticmethod
    def _json_body(request: "HttpRequest") -> dict[str, object]:
        if not request.body:
            return {}
        try:
            payload = json.loads(request.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CoreApiError("invalid_json", "Body JSON invalido.", status_code=400) from error
        if not isinstance(payload, dict):
            raise CoreApiError("invalid_json_object", "Body precisa ser objeto JSON.", status_code=400)
        return payload

    @staticmethod
    def _parse_content_length(value: str | None) -> int:
        if not value:
            return 0
        try:
            return max(0, int(value))
        except ValueError as error:
            raise CoreApiError("invalid_content_length", "Content-Length invalido.") from error

    @staticmethod
    def _optional_str(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _optional_dict(value: object) -> dict[str, object] | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise CoreApiError("invalid_object", "Valor precisa ser objeto JSON.")
        return value

    @staticmethod
    def _optional_list_of_strings(value: object) -> list[str] | None:
        if value is None:
            return None
        if not isinstance(value, list):
            raise CoreApiError("invalid_list", "Valor precisa ser lista JSON.")
        items: list[str] = []
        for item in value:
            if not isinstance(item, str):
                raise CoreApiError("invalid_list_item", "Lista precisa conter apenas strings.")
            items.append(item)
        return items

    @staticmethod
    def _optional_int(value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        raise CoreApiError("invalid_integer", "Valor precisa ser inteiro.")

    @staticmethod
    def _first_query_value(request: "HttpRequest", name: str) -> str | None:
        values = request.query.get(name)
        if not values:
            return None
        value = values[0]
        return value if value else None

    def _query_bool(self, request: "HttpRequest", name: str, *, default: bool) -> bool:
        value = self._first_query_value(request, name)
        if value is None:
            return default
        return value.lower() in {"1", "true", "yes", "y", "sim"}

    def _query_int(self, request: "HttpRequest", name: str, *, default: int) -> int:
        value = self._first_query_value(request, name)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError as error:
            raise CoreApiError("invalid_query_integer", "Query integer invalida.") from error

    @staticmethod
    def _error_payload(code: str, message: str) -> dict[str, object]:
        return {
            "ok": False,
            "error": {
                "code": code,
                "message": message,
            },
        }

    @staticmethod
    def _reason_phrase(status_code: int) -> str:
        return {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            409: "Conflict",
            500: "Internal Server Error",
            501: "Not Implemented",
        }.get(status_code, "OK")


class HttpRequest:
    def __init__(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, list[str]],
        headers: dict[str, str],
        body: bytes,
    ) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.headers = headers
        self.body = body
