from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


from crypto import aes_decrypt_hex, aes_encrypt_hex, generate_key_hex, generate_nonce_hex  # noqa: E402


def main() -> None:
    key_hex = generate_key_hex()
    plaintext_hex = b"authenticated session payload".hex()

    encrypted = aes_encrypt_hex(plaintext_hex, key_hex)
    decrypted_hex = aes_decrypt_hex(encrypted.payload_hex, key_hex)
    if decrypted_hex != plaintext_hex:
        raise RuntimeError("AES-GCM-SIV roundtrip failed.")

    tampered_payload = encrypted.payload_hex[:-2] + ("00" if encrypted.payload_hex[-2:] != "00" else "01")
    try:
        aes_decrypt_hex(tampered_payload, key_hex)
    except Exception:
        pass
    else:
        raise RuntimeError("AES-GCM-SIV accepted a tampered payload.")

    nonce_hex = generate_nonce_hex()
    aes_encrypt_hex(plaintext_hex, key_hex, iv_hex=nonce_hex)
    try:
        aes_encrypt_hex(plaintext_hex, key_hex, iv_hex=nonce_hex)
    except ValueError:
        pass
    else:
        raise RuntimeError("AES-GCM-SIV allowed nonce reuse with the same key.")

    print("crypto AES-GCM-SIV smoke OK")


if __name__ == "__main__":
    main()
