from __future__ import annotations

import os
from threading import Lock
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV

from .utils import BytesLike, bytes_to_hex, ensure_bytes, hex_to_bytes, validate_hex_length


AES_KEY_SIZE = 32
AES_GCM_SIV_NONCE_SIZE = 12
AES_GCM_SIV_TAG_SIZE = 16

_used_nonce_keys: set[tuple[str, str]] = set()
_used_nonce_keys_lock = Lock()


@dataclass(frozen=True)
class AESCipherHexResult:
    iv_hex: str
    ciphertext_hex: str
    payload_hex: str

    @property
    def nonce_hex(self) -> str:
        return self.iv_hex


def generate_key_hex() -> str:
    return os.urandom(AES_KEY_SIZE).hex()


def generate_iv_hex() -> str:
    return generate_nonce_hex()


def generate_nonce_hex() -> str:
    return os.urandom(AES_GCM_SIV_NONCE_SIZE).hex()


def encrypt_bytes(
    data: bytes,
    key_hex: str,
    iv_hex: str | None = None,
    aad: BytesLike | None = None,
) -> AESCipherHexResult:
    validate_hex_length(key_hex, AES_KEY_SIZE, field_name="key_hex")
    nonce_hex = iv_hex or _generate_unused_nonce_hex(key_hex)
    validate_hex_length(nonce_hex, AES_GCM_SIV_NONCE_SIZE, field_name="nonce_hex")
    _remember_nonce_use(key_hex, nonce_hex)

    key = hex_to_bytes(key_hex)
    nonce = hex_to_bytes(nonce_hex)

    associated_data = ensure_bytes(aad) if aad is not None else None
    ciphertext = AESGCMSIV(key).encrypt(nonce, data, associated_data)
    ciphertext_hex = bytes_to_hex(ciphertext)

    return AESCipherHexResult(
        iv_hex=nonce_hex,
        ciphertext_hex=ciphertext_hex,
        payload_hex=nonce_hex + ciphertext_hex,
    )


def encrypt_text(
    text: BytesLike,
    key_hex: str,
    iv_hex: str | None = None,
    aad: BytesLike | None = None,
) -> AESCipherHexResult:
    return encrypt_bytes(ensure_bytes(text), key_hex=key_hex, iv_hex=iv_hex, aad=aad)


def encrypt_hex(
    plaintext_hex: str,
    key_hex: str,
    iv_hex: str | None = None,
    aad: BytesLike | None = None,
) -> AESCipherHexResult:
    return encrypt_bytes(hex_to_bytes(plaintext_hex), key_hex=key_hex, iv_hex=iv_hex, aad=aad)


def decrypt_bytes(payload_hex: str, key_hex: str, aad: BytesLike | None = None) -> bytes:
    validate_hex_length(key_hex, AES_KEY_SIZE, field_name="key_hex")
    nonce_hex_length = AES_GCM_SIV_NONCE_SIZE * 2
    minimum_payload_hex_length = (AES_GCM_SIV_NONCE_SIZE + AES_GCM_SIV_TAG_SIZE) * 2
    if len(payload_hex) < minimum_payload_hex_length:
        raise ValueError("payload_hex deve conter nonce + ciphertext autenticado em HEX.")

    nonce_hex = payload_hex[:nonce_hex_length]
    ciphertext_hex = payload_hex[nonce_hex_length:]

    validate_hex_length(nonce_hex, AES_GCM_SIV_NONCE_SIZE, field_name="nonce_hex")
    if not ciphertext_hex:
        raise ValueError("payload_hex nao contem ciphertext.")

    key = hex_to_bytes(key_hex)
    nonce = hex_to_bytes(nonce_hex)
    ciphertext = hex_to_bytes(ciphertext_hex)

    associated_data = ensure_bytes(aad) if aad is not None else None
    return AESGCMSIV(key).decrypt(nonce, ciphertext, associated_data)


def decrypt_text(
    payload_hex: str,
    key_hex: str,
    *,
    encoding: str = "utf-8",
    aad: BytesLike | None = None,
) -> str:
    return decrypt_bytes(payload_hex, key_hex, aad=aad).decode(encoding)


def decrypt_hex(payload_hex: str, key_hex: str, aad: BytesLike | None = None) -> str:
    return bytes_to_hex(decrypt_bytes(payload_hex, key_hex, aad=aad))


def _generate_unused_nonce_hex(key_hex: str) -> str:
    for _ in range(256):
        nonce_hex = generate_nonce_hex()
        if not _was_nonce_used(key_hex, nonce_hex):
            return nonce_hex
    raise RuntimeError("nao foi possivel gerar nonce AES-GCM-SIV unico para esta chave.")


def _remember_nonce_use(key_hex: str, nonce_hex: str) -> None:
    nonce_key = (key_hex, nonce_hex)
    with _used_nonce_keys_lock:
        if nonce_key in _used_nonce_keys:
            raise ValueError("nonce AES-GCM-SIV reutilizado com a mesma chave.")
        _used_nonce_keys.add(nonce_key)


def _was_nonce_used(key_hex: str, nonce_hex: str) -> bool:
    with _used_nonce_keys_lock:
        return (key_hex, nonce_hex) in _used_nonce_keys
