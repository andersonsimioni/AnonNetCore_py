from __future__ import annotations

import binascii
from typing import Union

from .exceptions import InvalidHexError

BytesLike = Union[str, bytes]


def ensure_bytes(value: BytesLike, *, encoding: str = "utf-8") -> bytes:
    if isinstance(value, bytes):
        return value
    return value.encode(encoding)


def bytes_to_hex(data: bytes) -> str:
    return data.hex()


def hex_to_bytes(value: str) -> bytes:
    try:
        return bytes.fromhex(value)
    except ValueError as error:
        raise InvalidHexError("Valor HEX invalido.") from error


def validate_hex_length(value: str, expected_bytes: int, *, field_name: str) -> None:
    normalized = value.strip()
    if len(normalized) != expected_bytes * 2:
        raise InvalidHexError(
            f"{field_name} deve possuir {expected_bytes * 2} caracteres HEX."
        )
    try:
        binascii.unhexlify(normalized)
    except binascii.Error as error:
        raise InvalidHexError(f"{field_name} contem caracteres HEX invalidos.") from error
