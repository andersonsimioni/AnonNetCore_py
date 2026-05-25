from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from common import utc_now


class TransportState(StrEnum):
    STOPPED = "stopped"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"


@dataclass(slots=True, frozen=True)
class TransportEndpoint:
    transport_name: str
    host: str
    port: int


@dataclass(slots=True, frozen=True)
class TransportPacket:
    transport_name: str
    payload: bytes
    remote_endpoint: TransportEndpoint
    local_endpoint: TransportEndpoint | None = None
    received_at: datetime = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class OutboundMessage:
    transport_name: str
    payload: bytes
    remote_endpoint: TransportEndpoint
    local_endpoint: TransportEndpoint | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
