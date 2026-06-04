from __future__ import annotations

from uuid import uuid4

from common import canonical_payload_hex, compact_json_bytes
from ..models import ProtocolEnvelope


def as_payload_dict(envelope: ProtocolEnvelope) -> dict[str, object]:
    return envelope.payload if isinstance(envelope.payload, dict) else {}


def build_response_header(
    request_header: dict[str, object],
    message_type: str,
    *,
    include_response_to: bool = True,
) -> dict[str, object]:
    header = {
        "version": request_header.get("version", 1),
        "message_type": message_type,
        "message_id": str(uuid4()),
        "message_sequence": request_header.get("message_sequence"),
        "physical_session_id": request_header.get("physical_session_id"),
        "virtual_session_id": request_header.get("virtual_session_id"),
    }
    if include_response_to:
        header["response_to_message_id"] = request_header.get("message_id")
    return header


def read_header_string(envelope: ProtocolEnvelope, field_name: str) -> str | None:
    value = envelope.header.get(field_name)
    return value if isinstance(value, str) and value else None


def read_physical_session_id(envelope: ProtocolEnvelope) -> str | None:
    return read_header_string(envelope, "physical_session_id")


def read_virtual_session_id(envelope: ProtocolEnvelope) -> str | None:
    return read_header_string(envelope, "virtual_session_id")


def read_string_or_none(payload: dict[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    return value if isinstance(value, str) and value else None


def require_string(payload: dict[str, object], field_name: str) -> str:
    value = read_string_or_none(payload, field_name)
    if value is None:
        raise ValueError(f"Field '{field_name}' is required and must be a non-empty string.")
    return value


def optional_string(payload: dict[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
        raise ValueError(f"Field '{field_name}' must be a non-empty string when provided.")


def require_bool(payload: dict[str, object], field_name: str) -> bool:
    value = payload.get(field_name)
    if isinstance(value, bool):
        return value
    raise ValueError(f"O campo '{field_name}' e obrigatorio e precisa ser um booleano.")


def read_positive_keepalive_interval(
    payload: dict[str, object],
    default_value: int,
) -> int:
    value = payload.get("keepalive_interval_seconds")
    if isinstance(value, int) and value > 0:
        return value
    return default_value


def require_non_negative_int(payload: dict[str, object], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, int) and value >= 0:
        return value
    raise ValueError(f"{field_name} precisa ser um inteiro maior ou igual a zero.")


def require_positive_int(payload: dict[str, object], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, int) and value > 0:
        return value
    raise ValueError(f"{field_name} precisa ser um inteiro maior que zero.")
