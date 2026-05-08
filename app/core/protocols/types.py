from __future__ import annotations

from enum import StrEnum


class PacketProtocol(StrEnum):
    UNKNOWN = "unknown"
    JSON = "json"
    MSGPACK = "msgpack"
    RAW = "raw"
