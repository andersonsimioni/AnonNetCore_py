from __future__ import annotations

import json


def compact_json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def canonical_payload_hex(payload: dict[str, object]) -> str:
    return compact_json_bytes(payload).hex()
