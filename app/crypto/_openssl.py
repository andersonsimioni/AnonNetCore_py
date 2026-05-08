from __future__ import annotations

import shutil
import subprocess
from uuid import uuid4
from pathlib import Path
from typing import Iterable

from .exceptions import OpenSSLExecutionError


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
TEMP_ROOT = PACKAGE_ROOT / ".crypto_tmp"


def run_openssl(
    arguments: Iterable[str],
    *,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    command = ["openssl", *arguments]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        cwd=cwd,
    )

    if result.returncode != 0:
        raise OpenSSLExecutionError(result.stderr.strip() or "Falha ao executar OpenSSL.")

    return result


class OpenSSLWorkspace:
    def __init__(self) -> None:
        TEMP_ROOT.mkdir(exist_ok=True)
        self.path = TEMP_ROOT / f"job_{uuid4().hex}"
        self.path.mkdir()

    def write_bytes(self, filename: str, data: bytes) -> Path:
        file_path = self.path / filename
        file_path.write_bytes(data)
        return file_path

    def write_text(self, filename: str, content: str) -> Path:
        file_path = self.path / filename
        file_path.write_text(content, encoding="utf-8")
        return file_path

    def read_bytes(self, filename: str) -> bytes:
        return (self.path / filename).read_bytes()

    def __enter__(self) -> "OpenSSLWorkspace":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
