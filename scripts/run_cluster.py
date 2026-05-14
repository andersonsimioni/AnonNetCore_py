from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLUSTER_UP_SCRIPT = PROJECT_ROOT / "cluster" / "up_nodes.py"


def main() -> int:
    args = parse_args()
    command = [
        sys.executable,
        str(CLUSTER_UP_SCRIPT),
        str(args.node_count),
    ]
    if args.detach:
        command.append("--detach")

    print(f"Subindo cluster com {args.node_count} nodes...")
    return subprocess.call(command, cwd=PROJECT_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sobe apenas o cluster Docker de nodes AnonNetCore.",
    )
    parser.add_argument(
        "node_count",
        type=int,
        help="Quantidade de nodes no cluster.",
    )
    parser.add_argument(
        "--detach",
        action="store_true",
        default=True,
        help="Sobe containers em background.",
    )
    parser.add_argument(
        "--foreground",
        action="store_false",
        dest="detach",
        help="Sobe containers no foreground.",
    )
    args = parser.parse_args()
    if args.node_count < 2:
        raise SystemExit("Use pelo menos 2 nodes no cluster.")
    return args


if __name__ == "__main__":
    raise SystemExit(main())
