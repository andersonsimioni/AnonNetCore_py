from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha512

from common import canonical_payload_hex as _canonical_payload_hex
from crypto import dilithium_verify_hex
from transport import canonical_endpoint_list
from .pow import build_payload_pow_details
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
)


def validate_and_merge(
    namespace: str,
    key: str,
    parent: DhtPayload,
    fragment: DhtPayload,
    difficulty_bits: int,
) -> DhtPayload:
    normalized_namespace = namespace.lower()

    if normalized_namespace == "dpnt":
        return validate_and_merge_dpnt_fragment(key, parent, fragment, difficulty_bits)
    if normalized_namespace == "drt":
        return validate_and_merge_drt_fragment(key, parent, fragment, difficulty_bits)
    if normalized_namespace == "ddt":
        return validate_and_merge_ddt_fragment(key, parent, fragment, difficulty_bits)
    if normalized_namespace == "dtt":
        return validate_and_merge_dtt_fragment(key, parent, fragment, difficulty_bits)
    if normalized_namespace == "dpt":
        return validate_and_merge_dpt_fragment(key, parent, fragment, difficulty_bits)

    raise ValueError(f"Unsupported DHT namespace for merge: {namespace}")


def validate_and_merge_dpnt_fragment(
    key: str,
    parent: DhtPayload,
    fragment: DhtPayload,
    difficulty_bits: int,
) -> DpntRecordPayload:
    _ensure_payload_type(parent, DpntRecordPayload, "dpnt")
    _ensure_payload_type(fragment, DpntRecordPayload, "dpnt")

    _ensure_dpnt_key_matches_physical_node(key, fragment.pk_physical_node)

    if not _is_valid_dpnt_fragment(key, fragment, difficulty_bits):
        raise ValueError("O fragmento DPNT possui assinatura invalida.")

    return fragment


def validate_and_merge_drt_fragment(
    key: str,
    parent: DhtPayload,
    fragment: DhtPayload,
    difficulty_bits: int,
) -> DrtRecordPayload:
    _ensure_payload_type(parent, DrtRecordPayload, "drt")
    _ensure_payload_type(fragment, DrtRecordPayload, "drt")

    _ensure_drt_key_matches_virtual_node(key, parent.pk_virtual_node)

    if parent.pk_virtual_node != fragment.pk_virtual_node:
        raise ValueError(
            "Cannot merge DRT fragments from different virtual nodes."
        )

    valid_parent_entries = [
        route_entry
        for route_entry in parent.route_entries
        if _is_valid_drt_route_entry(route_entry, parent.pk_virtual_node, key, difficulty_bits)
    ]
    valid_fragment_entries = [
        route_entry
        for route_entry in fragment.route_entries
        if _is_valid_drt_route_entry(route_entry, fragment.pk_virtual_node, key, difficulty_bits)
    ]
    if fragment.route_entries and not valid_fragment_entries:
        raise ValueError("The DRT fragment has no valid route entries.")

    merged_entries = _deduplicate_drt_route_entries(
        [*valid_parent_entries, *valid_fragment_entries]
    )
    merged_last_update = datetime.now(timezone.utc).isoformat()

    return replace(
        parent,
        route_entries=merged_entries,
        last_update=merged_last_update,
    )


def validate_and_merge_ddt_fragment(
    key: str,
    parent: DhtPayload,
    fragment: DhtPayload,
    difficulty_bits: int,
) -> DdtRecordPayload:
    _ensure_payload_type(parent, DdtRecordPayload, "ddt")
    _ensure_payload_type(fragment, DdtRecordPayload, "ddt")

    _ensure_ddt_metadata_matches(parent, fragment)

    valid_fragment_holders = [
        holder
        for holder in fragment.holders
        if _is_valid_ddt_holder(key, holder, difficulty_bits)
    ]
    if fragment.holders and not valid_fragment_holders:
        raise ValueError("The DDT fragment has no valid holders.")

    merged_holders = _deduplicate_exact_ddt_holders(
        [*parent.holders, *valid_fragment_holders]
    )

    return replace(parent, holders=merged_holders)


def validate_and_merge_dtt_fragment(
    key: str,
    parent: DhtPayload,
    fragment: DhtPayload,
    difficulty_bits: int,
) -> DttRecordPayload:
    _ensure_payload_type(parent, DttRecordPayload, "dtt")
    _ensure_payload_type(fragment, DttRecordPayload, "dtt")

    valid_fragment_entries = [
        entry
        for entry in fragment.entries
        if _is_valid_dtt_entry(key, entry, difficulty_bits)
    ]
    if fragment.entries and not valid_fragment_entries:
        raise ValueError("The DTT fragment has no valid entries.")

    merged_entries = _deduplicate_exact_dtt_entries(
        [*parent.entries, *valid_fragment_entries]
    )

    return replace(parent, entries=merged_entries)


def validate_and_merge_dpt_fragment(
    key: str,
    parent: DhtPayload,
    fragment: DhtPayload,
    difficulty_bits: int,
) -> DptRecordPayload:
    _ensure_payload_type(parent, DptRecordPayload, "dpt")
    _ensure_payload_type(fragment, DptRecordPayload, "dpt")

    _ensure_dpt_key_matches_owner_and_title(
        key,
        fragment.pk_virtual_node_owner,
        fragment.title,
    )

    if not _is_valid_dpt_fragment(key, fragment, difficulty_bits):
        raise ValueError("O fragmento DPT possui assinatura invalida.")

    if not _is_fragment_newer(parent.last_modified, fragment.last_modified):
        return parent

    return fragment


def _ensure_payload_type(
    payload: DhtPayload,
    expected_type: type,
    namespace: str,
) -> None:
    if isinstance(payload, expected_type):
        return

    raise ValueError(
        f"Payload invalido para namespace '{namespace}'. "
        f"Esperado: {expected_type.__name__}. "
        f"Recebido: {type(payload).__name__}."
    )


def _ensure_drt_key_matches_virtual_node(key: str, pk_virtual_node: str) -> None:
    virtual_node_id = sha512(pk_virtual_node.encode("utf-8")).hexdigest()
    expected_key = sha512(f"drt|{virtual_node_id}".encode("utf-8")).hexdigest()
    if key == expected_key:
        return

    raise ValueError("The DRT key does not match the provided virtual_node_id.")


def _ensure_dpnt_key_matches_physical_node(key: str, pk_physical_node: str) -> None:
    physical_node_id = sha512(pk_physical_node.encode("utf-8")).hexdigest()
    expected_key = sha512(f"dpnt|{physical_node_id}".encode("utf-8")).hexdigest()
    if key == expected_key:
        return

    raise ValueError("The DPNT key does not match the node_id derived from pk_physical_node.")


def _ensure_dpt_key_matches_owner_and_title(
    key: str,
    pk_virtual_node_owner: str,
    title: str,
) -> None:
    pk_id_virtual_node_owner = sha512(pk_virtual_node_owner.encode("utf-8")).hexdigest()
    expected_key = sha512(
        f"dpt|{pk_id_virtual_node_owner}|{title}".encode("utf-8")
    ).hexdigest()
    if key == expected_key:
        return

    raise ValueError(
            "The DPT key does not match the provided virtual node owner id and title."
    )


def _ensure_ddt_metadata_matches(
    parent: DdtRecordPayload,
    fragment: DdtRecordPayload,
) -> None:
    if parent.title != fragment.title:
        raise ValueError("Cannot merge DDT fragments with different titles.")
    if parent.type != fragment.type:
        raise ValueError("Cannot merge DDT fragments with different types.")
    if parent.tags != fragment.tags:
        raise ValueError("Cannot merge DDT fragments with different tags.")


def _is_valid_drt_route_entry(
    route_entry: DrtRouteEntryRecord,
    pk_virtual_node: str,
    key: str,
    difficulty_bits: int,
) -> bool:
    if _is_expired(route_entry.expires_at):
        return False

    if not route_entry.virtual_node_signature:
        return False
    if not route_entry.entry_point_virtual_node_signature:
        return False
    if not route_entry.entry_point_physical_node_signature:
        return False
    if not route_entry.physical_node_signature:
        return False
    if not route_entry.rtt_physical_node_signature:
        return False
    if not build_payload_pow_details(
        namespace="drt",
        key_hex=key,
        payload=route_entry,
        nonce=route_entry.pow_nonce,
        difficulty_bits=difficulty_bits,
        parent_pk_virtual_node=pk_virtual_node,
    )["is_valid"]:
        return False

    try:
        final_physical_node_id = sha512(route_entry.pk_physical_node.encode("utf-8")).hexdigest()
        virtual_node_id = sha512(pk_virtual_node.encode("utf-8")).hexdigest()
        entry_point_virtual_payload = {
            "final_path_id": route_entry.final_path_id,
            "final_physical_node_id": final_physical_node_id,
        }
        entry_point_physical_payload = {
            "virtual_node_id": virtual_node_id,
            "final_path_id": route_entry.final_path_id,
            "virtual_node_signature": route_entry.virtual_node_signature,
        }
        rtt_payload = {
            "pk_physical_node": route_entry.pk_physical_node,
            "expires_at": route_entry.expires_at,
            "rtt": route_entry.rtt,
        }

        virtual_signature_valid = dilithium_verify_hex(
            _canonical_payload_hex(entry_point_virtual_payload),
            route_entry.virtual_node_signature,
            pk_virtual_node,
        )
        entry_point_virtual_signature_valid = dilithium_verify_hex(
            _canonical_payload_hex(entry_point_virtual_payload),
            route_entry.entry_point_virtual_node_signature,
            pk_virtual_node,
        )
        duplicated_virtual_signature_matches = (
            route_entry.entry_point_virtual_node_signature
            == route_entry.virtual_node_signature
        )
        physical_signature_valid = dilithium_verify_hex(
            _canonical_payload_hex(entry_point_physical_payload),
            route_entry.physical_node_signature,
            route_entry.pk_physical_node,
        )
        entry_point_physical_signature_valid = dilithium_verify_hex(
            _canonical_payload_hex(entry_point_physical_payload),
            route_entry.entry_point_physical_node_signature,
            route_entry.pk_physical_node,
        )
        rtt_signature_valid = dilithium_verify_hex(
            _canonical_payload_hex(rtt_payload),
            route_entry.rtt_physical_node_signature,
            route_entry.pk_physical_node,
        )
        return (
            virtual_signature_valid
            and entry_point_virtual_signature_valid
            and duplicated_virtual_signature_matches
            and physical_signature_valid
            and entry_point_physical_signature_valid
            and rtt_signature_valid
        )
    except Exception:
        return False


def _is_valid_dpnt_fragment(key: str, fragment: DpntRecordPayload, difficulty_bits: int) -> bool:
    if not build_payload_pow_details(
        namespace="dpnt",
        key_hex=key,
        payload=fragment,
        nonce=fragment.pow_nonce,
        difficulty_bits=difficulty_bits,
    )["is_valid"]:
        return False

    endpoints = canonical_endpoint_list(fragment.endpoints)
    signed_payload = {
        "key": key,
        "pk_physical_node": fragment.pk_physical_node,
        "endpoints": endpoints,
        "transport_methods": sorted(
            {
                endpoint["transport"]
                for endpoint in endpoints
                if isinstance(endpoint.get("transport"), str)
            }
        ),
        "reachability_class": fragment.reachability_class,
        "relay_capable": fragment.relay_capable,
        "hole_punch_capable": fragment.hole_punch_capable,
        "protocol_version": fragment.protocol_version,
        "feature_flags": fragment.feature_flags,
        "status": fragment.status,
    }
    message_hex = _canonical_payload_hex(signed_payload)

    try:
        return dilithium_verify_hex(
            message_hex,
            fragment.signature,
            fragment.pk_physical_node,
        )
    except Exception:
        return False


def _is_valid_dpt_fragment(key: str, fragment: DptRecordPayload, difficulty_bits: int) -> bool:
    if not build_payload_pow_details(
        namespace="dpt",
        key_hex=key,
        payload=fragment,
        nonce=fragment.pow_nonce,
        difficulty_bits=difficulty_bits,
    )["is_valid"]:
        return False

    signed_payload = {
        "key": key,
        "pk_virtual_node_owner": fragment.pk_virtual_node_owner,
        "title": fragment.title,
        "type": fragment.type,
        "last_modified": fragment.last_modified,
        "target_ref": fragment.target_ref,
    }
    message_hex = _canonical_payload_hex(signed_payload)

    try:
        return dilithium_verify_hex(
            message_hex,
            fragment.signature,
            fragment.pk_virtual_node_owner,
        )
    except Exception:
        return False


def _is_valid_dtt_entry(key: str, entry: DttEntryRecord, difficulty_bits: int) -> bool:
    if _is_expired(entry.expires_at):
        return False
    if not build_payload_pow_details(
        namespace="dtt",
        key_hex=key,
        payload=entry,
        nonce=entry.pow_nonce,
        difficulty_bits=difficulty_bits,
    )["is_valid"]:
        return False

    signed_payload = {
        "key": key,
        "resource_id": entry.resource_id,
        "pk_virtual_node": entry.pk_virtual_node,
        "created_at": entry.created_at,
        "expires_at": entry.expires_at,
    }
    message_hex = _canonical_payload_hex(signed_payload)

    try:
        return dilithium_verify_hex(
            message_hex,
            entry.signature,
            entry.pk_virtual_node,
        )
    except Exception:
        return False


def _is_valid_ddt_holder(key: str, holder: DdtHolderRecord, difficulty_bits: int) -> bool:
    if _is_expired(holder.expires_at):
        return False
    if not build_payload_pow_details(
        namespace="ddt",
        key_hex=key,
        payload=holder,
        nonce=holder.pow_nonce,
        difficulty_bits=difficulty_bits,
    )["is_valid"]:
        return False

    signed_payload = {
        "key": key,
        "pk_virtual_node": holder.pk_virtual_node,
        "expires_at": holder.expires_at,
    }
    message_hex = _canonical_payload_hex(signed_payload)

    try:
        return dilithium_verify_hex(
            message_hex,
            holder.signature,
            holder.pk_virtual_node,
        )
    except Exception:
        return False


def _deduplicate_drt_route_entries(
    route_entries: list[DrtRouteEntryRecord],
) -> list[DrtRouteEntryRecord]:
    unique_entries: dict[str, DrtRouteEntryRecord] = {}

    for route_entry in route_entries:
        current_entry = unique_entries.get(route_entry.final_path_id)
        if current_entry is None or _is_better_drt_route_entry(route_entry, current_entry):
            unique_entries[route_entry.final_path_id] = route_entry

    return sorted(
        unique_entries.values(),
        key=lambda item: (
            item.final_path_id,
            item.pk_physical_node,
            item.expires_at,
            item.rtt,
            item.physical_node_signature,
            item.virtual_node_signature,
            item.entry_point_virtual_node_signature,
            item.entry_point_physical_node_signature,
            item.rtt_physical_node_signature,
        ),
    )


def _is_better_drt_route_entry(
    candidate: DrtRouteEntryRecord,
    current: DrtRouteEntryRecord,
) -> bool:
    candidate_expires_at = _parse_datetime(candidate.expires_at)
    current_expires_at = _parse_datetime(current.expires_at)
    if candidate_expires_at is not None and current_expires_at is not None:
        if candidate_expires_at != current_expires_at:
            return candidate_expires_at > current_expires_at
    if candidate.rtt != current.rtt:
        return candidate.rtt < current.rtt
    return candidate.pk_physical_node < current.pk_physical_node


def _deduplicate_exact_ddt_holders(
    holders: list[DdtHolderRecord],
) -> list[DdtHolderRecord]:
    unique_holders: dict[DdtHolderRecord, None] = {}

    for holder in holders:
        unique_holders[holder] = None

    return sorted(
        unique_holders.keys(),
        key=lambda item: (
            item.pk_virtual_node,
            item.expires_at,
            item.signature,
        ),
    )


def _deduplicate_exact_dtt_entries(
    entries: list[DttEntryRecord],
) -> list[DttEntryRecord]:
    unique_entries: dict[DttEntryRecord, None] = {}

    for entry in entries:
        unique_entries[entry] = None

    return sorted(
        unique_entries.keys(),
        key=lambda item: (
            item.resource_id,
            item.pk_virtual_node,
            item.created_at,
            item.expires_at,
            item.signature,
        ),
    )


def _is_expired(value: str) -> bool:
    expires_at = _parse_datetime(value)
    if expires_at is None:
        return True
    return expires_at <= datetime.now(timezone.utc)


def _parse_datetime(value: str) -> datetime | None:
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _is_fragment_newer(parent_timestamp: str, fragment_timestamp: str) -> bool:
    parent_datetime = _parse_datetime(parent_timestamp)
    fragment_datetime = _parse_datetime(fragment_timestamp)

    if fragment_datetime is None:
        return False
    if parent_datetime is None:
        return True

    return fragment_datetime > parent_datetime
