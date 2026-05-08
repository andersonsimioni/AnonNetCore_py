from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class NetworkSession:
    id: int
    session_id: str
    session_scope: str
    local_identity_type: str
    local_identity_id: str
    remote_identity_type: str
    remote_identity_id: str
    direction: str
    initiator_side: str
    handshake_state: str
    session_state: str
    transport: str | None = None
    remote_host: str | None = None
    remote_port: int | None = None
    local_endpoint_id: int | None = None
    remote_endpoint_id: int | None = None
    key_exchange_algorithm: str | None = None
    signature_algorithm: str | None = None
    symmetric_algorithm: str | None = None
    hash_algorithm: str | None = None
    remote_public_key: str | None = None
    local_ephemeral_private_key: str | None = None
    local_ephemeral_public_key: str | None = None
    remote_ephemeral_public_key: str | None = None
    shared_secret_hex: str | None = None
    session_key_id: str | None = None
    established_at: datetime | None = None
    last_activity_at: datetime = field(default_factory=utc_now)
    last_keepalive_sent_at: datetime | None = None
    keepalive_interval_seconds: int = 30
    keepalive_deadline: datetime | None = None
    expires_at: datetime | None = None
    closed_at: datetime | None = None
    close_reason: str | None = None
    bound_route_id: str | None = None
    metadata_json: str | None = None


@dataclass(slots=True, frozen=True)
class SessionCreateInput:
    session_id: str
    session_scope: str
    local_identity_type: str
    local_identity_id: str
    remote_identity_type: str
    remote_identity_id: str
    direction: str
    initiator_side: str
    handshake_state: str
    session_state: str
    transport: str | None = None
    remote_host: str | None = None
    remote_port: int | None = None
    local_endpoint_id: int | None = None
    remote_endpoint_id: int | None = None
    key_exchange_algorithm: str | None = None
    signature_algorithm: str | None = None
    symmetric_algorithm: str | None = None
    hash_algorithm: str | None = None
    remote_public_key: str | None = None
    local_ephemeral_private_key: str | None = None
    local_ephemeral_public_key: str | None = None
    remote_ephemeral_public_key: str | None = None
    shared_secret_hex: str | None = None
    session_key_id: str | None = None
    established_at: datetime | None = None
    last_activity_at: datetime | None = None
    last_keepalive_sent_at: datetime | None = None
    keepalive_interval_seconds: int = 30
    keepalive_deadline: datetime | None = None
    expires_at: datetime | None = None
    closed_at: datetime | None = None
    close_reason: str | None = None
    bound_route_id: str | None = None
    metadata_json: str | None = None


@dataclass(slots=True, frozen=True)
class SessionStateUpdateInput:
    handshake_state: str | None = None
    session_state: str | None = None
    last_activity_at: datetime | None = None
    keepalive_deadline: datetime | None = None
    expires_at: datetime | None = None
    closed_at: datetime | None = None
    close_reason: str | None = None
    bound_route_id: str | None = None
    session_key_id: str | None = None
    metadata_json: str | None = None
