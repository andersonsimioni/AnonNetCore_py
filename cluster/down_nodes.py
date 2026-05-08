from __future__ import annotations

import platform
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent


def main() -> int:
    command = _build_platform_command(sys.argv[1:])
    completed = subprocess.run(command, cwd=ROOT_DIR)
    return completed.returncode


def _build_platform_command(arguments: list[str]) -> list[str]:
    system_name = platform.system().lower()
    if system_name == "windows":
        return [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT_DIR / "down_nodes.ps1"),
            *arguments,
        ]

    return [
        "bash",
        str(ROOT_DIR / "down_nodes.sh"),
        *arguments,
    ]


if __name__ == "__main__":
    raise SystemExit(main())
