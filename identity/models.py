from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class PhysicalNodeIdentityResult:
    id: str
    public_key: str
    private_key_encrypted: str
    key_algorithm: str
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True, frozen=True)
class VirtualNodeIdentityCreateInput:
    kind: str
    owner_physical_node_id: str
    expires_at: datetime | None = None
    is_active: bool = True
    metadata_json: str | None = None


@dataclass(slots=True, frozen=True)
class RemotePhysicalNodeValidationCandidate:
    node_id: str
    public_key: str


@dataclass(slots=True, frozen=True)
class RemotePhysicalNodeEndpointResult:
    transport: str
    host: str
    port: int
    priority: int


@dataclass(slots=True, frozen=True)
class RemotePhysicalNodeExchangeCandidate:
    node_id: str


@dataclass(slots=True, frozen=True)
class RemotePhysicalNodePingCandidate:
    node_id: str


@dataclass(slots=True, frozen=True)
class RemotePhysicalNodeRouteCandidate:
    node_id: str
    public_key: str
    average_rtt_ms: float
