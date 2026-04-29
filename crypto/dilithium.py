from __future__ import annotations

from dataclasses import dataclass

from ._openssl import OpenSSLWorkspace, run_openssl
from .utils import bytes_to_hex, hex_to_bytes


DILITHIUM_ALGORITHM = "ML-DSA-65"


@dataclass(frozen=True)
class DilithiumKeyPair:
    private_key_pem: str
    public_key_pem: str


def generate_key_pair() -> DilithiumKeyPair:
    with OpenSSLWorkspace() as workspace:
        private_key_path = workspace.path / "dilithium_private.pem"
        public_key_path = workspace.path / "dilithium_public.pem"

        run_openssl(
            ["genpkey", "-algorithm", DILITHIUM_ALGORITHM, "-out", private_key_path.name],
            cwd=workspace.path,
        )
        run_openssl(
            ["pkey", "-in", private_key_path.name, "-pubout", "-out", public_key_path.name],
            cwd=workspace.path,
        )

        return DilithiumKeyPair(
            private_key_pem=private_key_path.read_text(encoding="utf-8"),
            public_key_pem=public_key_path.read_text(encoding="utf-8"),
        )


def sign_hex(message_hex: str, private_key_pem: str) -> str:
    with OpenSSLWorkspace() as workspace:
        message_path = workspace.write_bytes("message.bin", hex_to_bytes(message_hex))
        private_key_path = workspace.write_text("private.pem", private_key_pem)
        signature_path = workspace.path / "signature.bin"

        run_openssl(
            [
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                private_key_path.name,
                "-in",
                message_path.name,
                "-out",
                signature_path.name,
            ],
            cwd=workspace.path,
        )

        return bytes_to_hex(signature_path.read_bytes())


def verify_hex(message_hex: str, signature_hex: str, public_key_pem: str) -> bool:
    with OpenSSLWorkspace() as workspace:
        message_path = workspace.write_bytes("message.bin", hex_to_bytes(message_hex))
        signature_path = workspace.write_bytes("signature.bin", hex_to_bytes(signature_hex))
        public_key_path = workspace.write_text("public.pem", public_key_pem)

        run_openssl(
            [
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                public_key_path.name,
                "-in",
                message_path.name,
                "-sigfile",
                signature_path.name,
            ],
            cwd=workspace.path,
        )

    return True
