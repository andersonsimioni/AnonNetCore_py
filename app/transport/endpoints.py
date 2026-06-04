from __future__ import annotations

import json

from common import load_json_object

from .models import TransportEndpoint


def normalize_endpoint_dict(endpoint: object) -> dict[str, object] | None:
    if not isinstance(endpoint, dict):
        return None

    transport = endpoint.get("transport")
    host = endpoint.get("host")
    port = endpoint.get("port")
    priority = endpoint.get("priority", 0)
    metadata = endpoint.get("metadata")
    if not isinstance(transport, str) or not transport:
        return None
    if not isinstance(host, str) or not host:
        return None
    if not isinstance(port, int):
        return None

    return {
        "transport": transport,
        "host": host,
        "port": port,
        "priority": priority if isinstance(priority, int) else 0,
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def normalize_endpoint_list(endpoints: object) -> list[dict[str, object]]:
    if not isinstance(endpoints, list):
        return []

    normalized: list[dict[str, object]] = []
    for endpoint in endpoints:
        normalized_endpoint = normalize_endpoint_dict(endpoint)
        if normalized_endpoint is not None:
            normalized.append(normalized_endpoint)
    return normalized


def canonical_endpoint_list(endpoints: object) -> list[dict[str, object]]:
    normalized = normalize_endpoint_list(endpoints)
    return sorted(
        normalized,
        key=lambda endpoint: (
            str(endpoint.get("transport", "")),
            str(endpoint.get("host", "")),
            int(endpoint.get("port", 0)),
            int(endpoint.get("priority", 0)),
            json.dumps(endpoint.get("metadata", {}), separators=(",", ":"), sort_keys=True),
        ),
    )


def build_transport_endpoint_from_result(endpoint_result) -> TransportEndpoint:
    return TransportEndpoint(
        transport_name=endpoint_result.transport,
        host=endpoint_result.host,
        port=endpoint_result.port,
        metadata=load_json_object(endpoint_result.metadata_json),
    )
