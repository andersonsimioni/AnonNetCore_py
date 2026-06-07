from __future__ import annotations

import json
from dataclasses import asdict, replace
from hashlib import sha512

from transport import canonical_endpoint_list

from .records import (
    DdtHolderRecord,
    DdtRecordPayload,
    DhtPayload,
    DpntRecordPayload,
    DptRecordPayload,
    DrtRouteEntryRecord,
    DrtRecordPayload,
    DttEntryRecord,
    DttRecordPayload,
    parse_record,
    serialize_record,
)

PowPayload = (
    DpntRecordPayload
    | DrtRouteEntryRecord
    | DdtHolderRecord
    | DttEntryRecord
    | DptRecordPayload
)


def attach_record_payload_pow_nonces(
    *,
    namespace: str,
    key_hex: str,
    record_json: str,
    difficulty_bits: int,
) -> str:
    payload = parse_record(namespace, record_json)
    payload_with_pow = attach_payload_pow_nonces(
        namespace=namespace,
        key_hex=key_hex,
        payload=payload,
        difficulty_bits=difficulty_bits,
    )
    return serialize_record(payload_with_pow)


def attach_payload_pow_nonces(
    *,
    namespace: str,
    key_hex: str,
    payload: DhtPayload,
    difficulty_bits: int,
) -> DhtPayload:
    normalized_namespace = namespace.lower()

    if normalized_namespace == "dpnt" and isinstance(payload, DpntRecordPayload):
        return attach_dpnt_pow(payload, key_hex, difficulty_bits)

    if normalized_namespace == "drt" and isinstance(payload, DrtRecordPayload):
        return replace(
            payload,
            route_entries=[
                attach_drt_route_entry_pow(
                    route_entry,
                    key_hex,
                    payload.pk_virtual_node,
                    difficulty_bits,
                )
                for route_entry in payload.route_entries
            ],
        )

    if normalized_namespace == "ddt" and isinstance(payload, DdtRecordPayload):
        return replace(
            payload,
            holders=[
                attach_ddt_holder_pow(holder, key_hex, difficulty_bits)
                for holder in payload.holders
            ],
        )

    if normalized_namespace == "dtt" and isinstance(payload, DttRecordPayload):
        return replace(
            payload,
            entries=[
                attach_dtt_entry_pow(entry, key_hex, difficulty_bits)
                for entry in payload.entries
            ],
        )

    if normalized_namespace == "dpt" and isinstance(payload, DptRecordPayload):
        return attach_dpt_pow(payload, key_hex, difficulty_bits)

    raise ValueError(f"Unsupported payload for semantic DHT PoW: {namespace}")


def validate_record_payload_pow(
    *,
    namespace: str,
    key_hex: str,
    record_json: str,
    difficulty_bits: int,
) -> bool:
    try:
        payload = parse_record(namespace, record_json)
        return validate_payload_pow(
            namespace=namespace,
            key_hex=key_hex,
            payload=payload,
            difficulty_bits=difficulty_bits,
        )
    except Exception:
        return False


def validate_payload_pow(
    *,
    namespace: str,
    key_hex: str,
    payload: DhtPayload,
    difficulty_bits: int,
) -> bool:
    normalized_namespace = namespace.lower()

    if normalized_namespace == "dpnt" and isinstance(payload, DpntRecordPayload):
        return validate_dpnt_pow(payload, key_hex, difficulty_bits)

    if normalized_namespace == "drt" and isinstance(payload, DrtRecordPayload):
        return bool(payload.route_entries) and all(
            validate_drt_route_entry_pow(
                route_entry,
                key_hex,
                payload.pk_virtual_node,
                difficulty_bits,
            )
            for route_entry in payload.route_entries
        )

    if normalized_namespace == "ddt" and isinstance(payload, DdtRecordPayload):
        return bool(payload.holders) and all(
            validate_ddt_holder_pow(holder, key_hex, difficulty_bits)
            for holder in payload.holders
        )

    if normalized_namespace == "dtt" and isinstance(payload, DttRecordPayload):
        return bool(payload.entries) and all(
            validate_dtt_entry_pow(entry, key_hex, difficulty_bits)
            for entry in payload.entries
        )

    if normalized_namespace == "dpt" and isinstance(payload, DptRecordPayload):
        return validate_dpt_pow(payload, key_hex, difficulty_bits)

    return False


def attach_dpnt_pow(
    payload: DpntRecordPayload,
    key_hex: str,
    difficulty_bits: int,
) -> DpntRecordPayload:
    return _with_payload_pow_nonce("dpnt", key_hex, payload, difficulty_bits)


def attach_drt_route_entry_pow(
    route_entry: DrtRouteEntryRecord,
    key_hex: str,
    pk_virtual_node: str,
    difficulty_bits: int,
) -> DrtRouteEntryRecord:
    return _with_payload_pow_nonce(
        "drt",
        key_hex,
        route_entry,
        difficulty_bits,
        parent_pk_virtual_node=pk_virtual_node,
    )


def attach_ddt_holder_pow(
    holder: DdtHolderRecord,
    key_hex: str,
    difficulty_bits: int,
) -> DdtHolderRecord:
    return _with_payload_pow_nonce("ddt", key_hex, holder, difficulty_bits)


def attach_dtt_entry_pow(
    entry: DttEntryRecord,
    key_hex: str,
    difficulty_bits: int,
) -> DttEntryRecord:
    return _with_payload_pow_nonce("dtt", key_hex, entry, difficulty_bits)


def attach_dpt_pow(
    payload: DptRecordPayload,
    key_hex: str,
    difficulty_bits: int,
) -> DptRecordPayload:
    return _with_payload_pow_nonce("dpt", key_hex, payload, difficulty_bits)


def validate_dpnt_pow(
    payload: DpntRecordPayload,
    key_hex: str,
    difficulty_bits: int,
) -> bool:
    return validate_payload_pow_nonce(
        namespace="dpnt",
        key_hex=key_hex,
        payload=payload,
        nonce=payload.pow_nonce,
        difficulty_bits=difficulty_bits,
    )


def validate_drt_route_entry_pow(
    route_entry: DrtRouteEntryRecord,
    key_hex: str,
    pk_virtual_node: str,
    difficulty_bits: int,
) -> bool:
    return validate_payload_pow_nonce(
        namespace="drt",
        key_hex=key_hex,
        payload=route_entry,
        nonce=route_entry.pow_nonce,
        difficulty_bits=difficulty_bits,
        parent_pk_virtual_node=pk_virtual_node,
    )


def validate_ddt_holder_pow(
    holder: DdtHolderRecord,
    key_hex: str,
    difficulty_bits: int,
) -> bool:
    return validate_payload_pow_nonce(
        namespace="ddt",
        key_hex=key_hex,
        payload=holder,
        nonce=holder.pow_nonce,
        difficulty_bits=difficulty_bits,
    )


def validate_dtt_entry_pow(
    entry: DttEntryRecord,
    key_hex: str,
    difficulty_bits: int,
) -> bool:
    return validate_payload_pow_nonce(
        namespace="dtt",
        key_hex=key_hex,
        payload=entry,
        nonce=entry.pow_nonce,
        difficulty_bits=difficulty_bits,
    )


def validate_dpt_pow(
    payload: DptRecordPayload,
    key_hex: str,
    difficulty_bits: int,
) -> bool:
    return validate_payload_pow_nonce(
        namespace="dpt",
        key_hex=key_hex,
        payload=payload,
        nonce=payload.pow_nonce,
        difficulty_bits=difficulty_bits,
    )


def validate_payload_pow_nonce(
    *,
    namespace: str,
    key_hex: str,
    payload: PowPayload,
    nonce: int | None,
    difficulty_bits: int,
    parent_pk_virtual_node: str | None = None,
) -> bool:
    return bool(
        build_payload_pow_details(
            namespace=namespace,
            key_hex=key_hex,
            payload=payload,
            nonce=nonce,
            difficulty_bits=difficulty_bits,
            parent_pk_virtual_node=parent_pk_virtual_node,
        )["is_valid"]
    )


def build_payload_pow_details(
    *,
    namespace: str,
    key_hex: str,
    payload: PowPayload,
    nonce: int | None,
    difficulty_bits: int,
    parent_pk_virtual_node: str | None = None,
) -> dict[str, object]:
    canonical_material = payload_pow_canonical_bytes(
        namespace=namespace,
        key_hex=key_hex,
        payload=payload,
        parent_pk_virtual_node=parent_pk_virtual_node,
    )
    canonical_hash = sha512(canonical_material).hexdigest()
    if nonce is None or nonce < 0:
        return _invalid_pow_details(
            canonical_hash=canonical_hash,
            difficulty_bits=difficulty_bits,
            nonce=nonce,
        )

    proof_material = canonical_material + b"|" + str(nonce).encode("utf-8")
    proof_hash = sha512(proof_material).hexdigest()
    proof_bits = bin(int(proof_hash, 16))[2:].zfill(512)
    return {
        "canonical_hash": canonical_hash,
        "proof_hash": proof_hash,
        "proof_hash_prefix": proof_hash[:16],
        "difficulty_bits": difficulty_bits,
        "nonce": nonce,
        "is_valid": difficulty_bits <= 0 or proof_bits.startswith("0" * difficulty_bits),
    }


def payload_pow_canonical_bytes(
    *,
    namespace: str,
    key_hex: str,
    payload: PowPayload,
    parent_pk_virtual_node: str | None = None,
) -> bytes:
    payload_dict = asdict(payload)
    payload_dict.pop("pow_nonce", None)

    if isinstance(payload, DpntRecordPayload):
        payload_dict["endpoints"] = canonical_endpoint_list(payload.endpoints)

    canonical_data: dict[str, object] = {
        "kind": f"{namespace.lower()}_{type(payload).__name__}_pow_v1",
        "key": key_hex,
        "payload": payload_dict,
    }
    if isinstance(payload, DrtRouteEntryRecord):
        if not parent_pk_virtual_node:
            raise ValueError("parent_pk_virtual_node is required for DRT route entry PoW.")
        canonical_data["pk_virtual_node_id"] = sha512(
            parent_pk_virtual_node.encode("utf-8")
        ).hexdigest()

    return json.dumps(
        canonical_data,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _with_payload_pow_nonce(
    namespace: str,
    key_hex: str,
    payload: PowPayload,
    difficulty_bits: int,
    *,
    parent_pk_virtual_node: str | None = None,
) -> PowPayload:
    if difficulty_bits <= 0:
        return replace(payload, pow_nonce=0)

    nonce = 0
    while True:
        if validate_payload_pow_nonce(
            namespace=namespace,
            key_hex=key_hex,
            payload=payload,
            nonce=nonce,
            difficulty_bits=difficulty_bits,
            parent_pk_virtual_node=parent_pk_virtual_node,
        ):
            return replace(payload, pow_nonce=nonce)
        nonce += 1


def _invalid_pow_details(
    *,
    canonical_hash: str,
    difficulty_bits: int,
    nonce: int | None,
) -> dict[str, object]:
    return {
        "canonical_hash": canonical_hash,
        "proof_hash": None,
        "proof_hash_prefix": None,
        "difficulty_bits": difficulty_bits,
        "nonce": nonce,
        "is_valid": False,
    }
