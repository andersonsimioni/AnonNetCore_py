from __future__ import annotations

import hashlib

from .utils import BytesLike, ensure_bytes, hex_to_bytes


def sha512_hex(data: BytesLike) -> str:
    payload = ensure_bytes(data)
    return hashlib.sha512(payload).hexdigest()


def sha512_from_hex(data_hex: str) -> str:
    return hashlib.sha512(hex_to_bytes(data_hex)).hexdigest()
