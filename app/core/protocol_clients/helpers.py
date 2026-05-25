from __future__ import annotations

from common import canonical_payload_hex, is_expired_iso_datetime
from crypto import dilithium_verify_hex


def verify_dilithium_payload_signature(
    payload: dict[str, object],
    signature_hex: str,
    public_key_pem: str,
) -> bool:
    try:
        return dilithium_verify_hex(
            canonical_payload_hex(payload),
            signature_hex,
            public_key_pem,
        )
    except Exception:
        return False


def close_failed_handshake_session(engine, session_id: str) -> None:
    session = engine.services.session_manager.get_session_by_session_id(session_id)
    if session is None:
        return

    if session.session_state != "active":
        engine.services.session_manager.close_session(
            session_id,
            close_reason="handshake_failed",
        )
