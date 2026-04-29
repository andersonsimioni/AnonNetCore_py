from __future__ import annotations

from dataclasses import dataclass

from ._openssl import OpenSSLWorkspace, run_openssl
from .utils import bytes_to_hex, hex_to_bytes


KYBER_ALGORITHM = "ML-KEM-768"


@dataclass(frozen=True)
class KyberKeyPair:
    private_key_pem: str
    public_key_pem: str


@dataclass(frozen=True)
class KyberEncapsulationResult:
    ciphertext_hex: str
    shared_secret_hex: str


def generate_key_pair() -> KyberKeyPair:
    with OpenSSLWorkspace() as workspace:
        private_key_path = workspace.path / "kyber_private.pem"
        public_key_path = workspace.path / "kyber_public.pem"

        run_openssl(
            ["genpkey", "-algorithm", KYBER_ALGORITHM, "-out", private_key_path.name],
            cwd=workspace.path,
        )
        run_openssl(
            ["pkey", "-in", private_key_path.name, "-pubout", "-out", public_key_path.name],
            cwd=workspace.path,
        )

        return KyberKeyPair(
            private_key_pem=private_key_path.read_text(encoding="utf-8"),
            public_key_pem=public_key_path.read_text(encoding="utf-8"),
        )


def encapsulate_hex(public_key_pem: str) -> KyberEncapsulationResult:
    with OpenSSLWorkspace() as workspace:
        public_key_path = workspace.write_text("public.pem", public_key_pem)
        ciphertext_path = workspace.path / "ciphertext.bin"
        shared_secret_path = workspace.path / "shared_secret.bin"

        run_openssl(
            [
                "pkeyutl",
                "-encap",
                "-pubin",
                "-inkey",
                public_key_path.name,
                "-out",
                ciphertext_path.name,
                "-secret",
                shared_secret_path.name,
            ],
            cwd=workspace.path,
        )

        return KyberEncapsulationResult(
            ciphertext_hex=bytes_to_hex(ciphertext_path.read_bytes()),
            shared_secret_hex=bytes_to_hex(shared_secret_path.read_bytes()),
        )


def decapsulate_hex(ciphertext_hex: str, private_key_pem: str) -> str:
    with OpenSSLWorkspace() as workspace:
        private_key_path = workspace.write_text("private.pem", private_key_pem)
        ciphertext_path = workspace.write_bytes("ciphertext.bin", hex_to_bytes(ciphertext_hex))
        shared_secret_path = workspace.path / "shared_secret.bin"

        run_openssl(
            [
                "pkeyutl",
                "-decap",
                "-inkey",
                private_key_path.name,
                "-in",
                ciphertext_path.name,
                "-secret",
                shared_secret_path.name,
            ],
            cwd=workspace.path,
        )

        return bytes_to_hex(shared_secret_path.read_bytes())
