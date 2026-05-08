from __future__ import annotations

import os
from dataclasses import dataclass

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .utils import BytesLike, bytes_to_hex, ensure_bytes, hex_to_bytes, validate_hex_length


AES_KEY_SIZE = 32
AES_BLOCK_SIZE = 16


@dataclass(frozen=True)
class AESCipherHexResult:
    iv_hex: str
    ciphertext_hex: str
    payload_hex: str


def generate_key_hex() -> str:
    return os.urandom(AES_KEY_SIZE).hex()


def generate_iv_hex() -> str:
    return os.urandom(AES_BLOCK_SIZE).hex()


def encrypt_bytes(data: bytes, key_hex: str, iv_hex: str | None = None) -> AESCipherHexResult:
    validate_hex_length(key_hex, AES_KEY_SIZE, field_name="key_hex")
    iv_hex = iv_hex or generate_iv_hex()
    validate_hex_length(iv_hex, AES_BLOCK_SIZE, field_name="iv_hex")

    key = hex_to_bytes(key_hex)
    iv = hex_to_bytes(iv_hex)

    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data) + padder.finalize()

    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()
    ciphertext_hex = bytes_to_hex(ciphertext)

    return AESCipherHexResult(
        iv_hex=iv_hex,
        ciphertext_hex=ciphertext_hex,
        payload_hex=iv_hex + ciphertext_hex,
    )


def encrypt_text(text: BytesLike, key_hex: str, iv_hex: str | None = None) -> AESCipherHexResult:
    return encrypt_bytes(ensure_bytes(text), key_hex=key_hex, iv_hex=iv_hex)


def encrypt_hex(plaintext_hex: str, key_hex: str, iv_hex: str | None = None) -> AESCipherHexResult:
    return encrypt_bytes(hex_to_bytes(plaintext_hex), key_hex=key_hex, iv_hex=iv_hex)


def decrypt_bytes(payload_hex: str, key_hex: str) -> bytes:
    validate_hex_length(key_hex, AES_KEY_SIZE, field_name="key_hex")
    if len(payload_hex) < AES_BLOCK_SIZE * 2:
        raise ValueError("payload_hex deve conter IV + ciphertext em HEX.")

    iv_hex = payload_hex[: AES_BLOCK_SIZE * 2]
    ciphertext_hex = payload_hex[AES_BLOCK_SIZE * 2 :]

    validate_hex_length(iv_hex, AES_BLOCK_SIZE, field_name="iv_hex")
    if not ciphertext_hex:
        raise ValueError("payload_hex nao contem ciphertext.")

    key = hex_to_bytes(key_hex)
    iv = hex_to_bytes(iv_hex)
    ciphertext = hex_to_bytes(ciphertext_hex)

    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    return unpadder.update(padded_plaintext) + unpadder.finalize()


def decrypt_text(payload_hex: str, key_hex: str, *, encoding: str = "utf-8") -> str:
    return decrypt_bytes(payload_hex, key_hex).decode(encoding)


def decrypt_hex(payload_hex: str, key_hex: str) -> str:
    return bytes_to_hex(decrypt_bytes(payload_hex, key_hex))
