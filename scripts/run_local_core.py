from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CORE_ENTRYPOINT = PROJECT_ROOT / "app" / "main.py"


def main() -> int:
    args = parse_args()
    command = [
        sys.executable,
        str(CORE_ENTRYPOINT),
        "--listen-port",
        str(args.listen_port),
    ]
    print(f"Iniciando core local na porta TCP {args.listen_port}...")
    return subprocess.call(
        command,
        cwd=PROJECT_ROOT,
        env=build_child_environment(),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sobe apenas um core local AnonNetCore.")
    parser.add_argument(
        "--listen-port",
        type=int,
        default=19101,
        help="Porta TCP do core local.",
    )
    return parser.parse_args()


def build_child_environment() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    return env


if __name__ == "__main__":
    raise SystemExit(main())
