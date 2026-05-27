from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class RelayRegistration:
    target_physical_node_id: str
    target_public_key: str
    target_session_id: str
    challenge_nonce: str
    signature_hex: str
    registered_at: datetime
    expires_at: datetime


@dataclass(slots=True, frozen=True)
class RelayChallenge:
    target_physical_node_id: str
    target_session_id: str
    relay_physical_node_id: str
    nonce: str
    created_at: datetime
    expires_at: datetime


@dataclass(slots=True, frozen=True)
class RelayChannel:
    relay_channel_id: str
    target_physical_node_id: str
    requester_session_id: str
    target_session_id: str
    created_at: datetime
    expires_at: datetime
