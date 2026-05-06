from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import TypeAlias


@dataclass(slots=True, frozen=True)
class DpntRecordPayload:
    pk_physical_node: str
    endpoints: list[dict[str, object]]
    transport_methods: list[str]
    reachability_class: str
    relay_capable: bool
    hole_punch_capable: bool
    protocol_version: str
    feature_flags: list[str]
    last_validated_at: str
    status: str
    signature: str


@dataclass(slots=True, frozen=True)
class DrtRouteEntryRecord:
    pk_physical_node: str
    virtual_node_signature: str
    final_path_id: str
    entry_point_virtual_node_signature: str
    entry_point_physical_node_signature: str
    physical_node_signature: str
    expires_at: str
    rtt: int
    rtt_physical_node_signature: str


@dataclass(slots=True, frozen=True)
class DrtRecordPayload:
    pk_virtual_node: str
    route_entries: list[DrtRouteEntryRecord]
    last_update: str


@dataclass(slots=True, frozen=True)
class DdtHolderRecord:
    pk_virtual_node: str
    expires_at: str
    signature: str


@dataclass(slots=True, frozen=True)
class DdtRecordPayload:
    title: str
    type: str
    tags: list[str]
    holders: list[DdtHolderRecord]


@dataclass(slots=True, frozen=True)
class DttEntryRecord:
    resource_id: str
    pk_virtual_node: str
    created_at: str
    expires_at: str
    signature: str


@dataclass(slots=True, frozen=True)
class DttRecordPayload:
    entries: list[DttEntryRecord]


@dataclass(slots=True, frozen=True)
class DptRecordPayload:
    pk_virtual_node_owner: str
    title: str
    type: str
    last_modified: str
    target_ref: str
    signature: str


DhtPayload: TypeAlias = (
    DpntRecordPayload
    | DrtRecordPayload
    | DdtRecordPayload
    | DttRecordPayload
    | DptRecordPayload
)


def parse_record(namespace: str, record_json: str) -> DhtPayload:
    if not namespace:
        raise ValueError("namespace e obrigatorio.")
    if not record_json:
        raise ValueError("record_json e obrigatorio.")

    try:
        payload = json.loads(record_json)
    except json.JSONDecodeError as error:
        raise ValueError("record_json nao contem um JSON valido.") from error

    if not isinstance(payload, dict):
        raise ValueError("record_json precisa ser um objeto JSON.")

    return parse_record_dict(namespace, payload)


def parse_record_dict(namespace: str, payload: dict[str, object]) -> DhtPayload:
    parser = _PARSERS.get(namespace.lower())
    if parser is None:
        raise ValueError(f"Namespace DHT nao suportado: {namespace}")
    return parser(payload)


def serialize_record(payload: DhtPayload) -> str:
    if not is_dataclass(payload):
        raise ValueError("payload precisa ser um dataclass de DHT.")

    return json.dumps(asdict(payload), separators=(",", ":"), sort_keys=True)


def _parse_dpnt(payload: dict[str, object]) -> DpntRecordPayload:
    return DpntRecordPayload(
        pk_physical_node=_read_required_string(payload, "pk_physical_node"),
        endpoints=_read_list_of_dicts(payload, "endpoints"),
        transport_methods=_read_list_of_strings(payload, "transport_methods"),
        reachability_class=_read_required_string(payload, "reachability_class"),
        relay_capable=_read_required_bool(payload, "relay_capable"),
        hole_punch_capable=_read_required_bool(payload, "hole_punch_capable"),
        protocol_version=_read_required_string(payload, "protocol_version"),
        feature_flags=_read_list_of_strings(payload, "feature_flags"),
        last_validated_at=_read_required_string(payload, "last_validated_at"),
        status=_read_required_string(payload, "status"),
        signature=_read_required_string(payload, "signature"),
    )


def _parse_drt(payload: dict[str, object]) -> DrtRecordPayload:
    return DrtRecordPayload(
        pk_virtual_node=_read_required_string(payload, "pk_virtual_node"),
        route_entries=_read_drt_route_entries(payload),
        last_update=_read_required_string(payload, "last_update"),
    )


def _parse_ddt(payload: dict[str, object]) -> DdtRecordPayload:
    return DdtRecordPayload(
        title=_read_required_string(payload, "title"),
        type=_read_required_string(payload, "type"),
        tags=_read_list_of_strings(payload, "tags"),
        holders=_read_ddt_holders(payload),
    )


def _parse_dtt(payload: dict[str, object]) -> DttRecordPayload:
    return DttRecordPayload(
        entries=_read_dtt_entries(payload),
    )


def _parse_dpt(payload: dict[str, object]) -> DptRecordPayload:
    return DptRecordPayload(
        pk_virtual_node_owner=_read_required_string(payload, "pk_virtual_node_owner"),
        title=_read_required_string(payload, "title"),
        type=_read_required_string(payload, "type"),
        last_modified=_read_required_string(payload, "last_modified"),
        target_ref=_read_required_string(payload, "target_ref"),
        signature=_read_required_string(payload, "signature"),
    )


def _read_list_of_dicts(payload: dict[str, object], field_name: str) -> list[dict[str, object]]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError(f"O campo '{field_name}' precisa ser uma lista.")

    items: list[dict[str, object]] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise ValueError(f"O campo '{field_name}' precisa conter apenas objetos.")
        items.append(item)
    return items


def _read_list_of_strings(payload: dict[str, object], field_name: str) -> list[str]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError(f"O campo '{field_name}' precisa ser uma lista.")

    items: list[str] = []
    for item in raw_value:
        if not isinstance(item, str):
            raise ValueError(f"O campo '{field_name}' precisa conter apenas strings.")
        items.append(item)
    return items


def _read_required_string(payload: dict[str, object], field_name: str) -> str:
    raw_value = payload.get(field_name)
    if not isinstance(raw_value, str) or not raw_value:
        raise ValueError(f"O campo '{field_name}' precisa ser uma string nao vazia.")
    return raw_value


def _read_metadata(payload: dict[str, object]) -> dict[str, object]:
    raw_value = payload.get("metadata")
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ValueError("O campo 'metadata' precisa ser um objeto.")
    return raw_value


def _read_ddt_holders(payload: dict[str, object]) -> list[DdtHolderRecord]:
    raw_value = payload.get("holders")
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError("O campo 'holders' precisa ser uma lista.")

    holders: list[DdtHolderRecord] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise ValueError("O campo 'holders' precisa conter apenas objetos.")

        holders.append(
            DdtHolderRecord(
                pk_virtual_node=_read_required_string(item, "pk_virtual_node"),
                expires_at=_read_required_string(item, "expires_at"),
                signature=_read_required_string(item, "signature"),
            )
        )
    return holders


def _read_drt_route_entries(payload: dict[str, object]) -> list[DrtRouteEntryRecord]:
    raw_value = payload.get("route_entries")
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError("O campo 'route_entries' precisa ser uma lista.")

    route_entries: list[DrtRouteEntryRecord] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise ValueError("O campo 'route_entries' precisa conter apenas objetos.")

        route_entries.append(
            DrtRouteEntryRecord(
                pk_physical_node=_read_required_string(item, "pk_physical_node"),
                virtual_node_signature=_read_required_string(item, "virtual_node_signature"),
                final_path_id=_read_required_string(item, "final_path_id"),
                entry_point_virtual_node_signature=_read_required_string(
                    item,
                    "entry_point_virtual_node_signature",
                ),
                entry_point_physical_node_signature=_read_required_string(
                    item,
                    "entry_point_physical_node_signature",
                ),
                physical_node_signature=_read_required_string(item, "physical_node_signature"),
                expires_at=_read_required_string(item, "expires_at"),
                rtt=_read_required_int(item, "rtt"),
                rtt_physical_node_signature=_read_required_string(
                    item,
                    "rtt_physical_node_signature",
                ),
            )
        )
    return route_entries


def _read_dtt_entries(payload: dict[str, object]) -> list[DttEntryRecord]:
    raw_value = payload.get("entries")
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError("O campo 'entries' precisa ser uma lista.")

    entries: list[DttEntryRecord] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise ValueError("O campo 'entries' precisa conter apenas objetos.")

        entries.append(
            DttEntryRecord(
                resource_id=_read_required_string(item, "resource_id"),
                pk_virtual_node=_read_required_string(item, "pk_virtual_node"),
                created_at=_read_required_string(item, "created_at"),
                expires_at=_read_required_string(item, "expires_at"),
                signature=_read_required_string(item, "signature"),
            )
        )
    return entries


def _read_required_int(payload: dict[str, object], field_name: str) -> int:
    raw_value = payload.get(field_name)
    if not isinstance(raw_value, int):
        raise ValueError(f"O campo '{field_name}' precisa ser um inteiro.")
    return raw_value


def _read_required_bool(payload: dict[str, object], field_name: str) -> bool:
    raw_value = payload.get(field_name)
    if not isinstance(raw_value, bool):
        raise ValueError(f"O campo '{field_name}' precisa ser um booleano.")
    return raw_value


_PARSERS = {
    "dpnt": _parse_dpnt,
    "drt": _parse_drt,
    "ddt": _parse_ddt,
    "dtt": _parse_dtt,
    "dpt": _parse_dpt,
}
