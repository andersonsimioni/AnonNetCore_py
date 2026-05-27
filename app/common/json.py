from __future__ import annotations

import json


def compact_json_text(payload: dict[str, object]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def compact_json_bytes(payload: dict[str, object]) -> bytes:
    return compact_json_text(payload).encode("utf-8")


def canonical_payload_hex(payload: dict[str, object]) -> str:
    return compact_json_bytes(payload).hex()


def load_json_object(raw_json: str | None) -> dict[str, object]:
    if not raw_json:
        return {}

    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}

    return payload if isinstance(payload, dict) else {}
