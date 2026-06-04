from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys


CLUSTER_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = CLUSTER_ROOT.parent
COMPOSE_FILE = CLUSTER_ROOT / "docker-compose.generated.yml"
GENERATOR_SCRIPT = CLUSTER_ROOT / "generate_docker_cluster.py"
CLUSTER_STATE_ROOT = CLUSTER_ROOT / "state"


def main() -> int:
    args = parse_args()
    verify_docker_is_available()
    down_existing_cluster()
    generate_cluster(args.node_count, seed=args.seed)
    reset_cluster_node_state()
    start_compose(detach=args.detach)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Starts the AnonNetCore Docker node cluster.")
    parser.add_argument("node_count", type=int)
    parser.add_argument("-Detach", "--detach", action="store_true")
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional seed to reproduce randomized non-bootstrap node profiles.",
    )
    args = parser.parse_args()
    if args.node_count < 2:
        raise SystemExit("Use at least 2 nodes to keep fixed bootstrap nodes.")
    return args


def verify_docker_is_available() -> None:
    run_command(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def generate_cluster(node_count: int, *, seed: int | None) -> None:
    print(f"Generating cluster with {node_count} nodes...")
    command = [
        sys.executable,
        str(GENERATOR_SCRIPT),
        "--nodes",
        str(node_count),
        "--output-dir",
        str(CLUSTER_ROOT),
    ]
    if seed is not None:
        command.extend(["--seed", str(seed)])

    run_command(command, cwd=PROJECT_ROOT)


def down_existing_cluster() -> None:
    if not COMPOSE_FILE.exists():
        return

    print("Stopping previous Docker cluster...")
    run_command(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "down",
            "--remove-orphans",
        ],
        cwd=PROJECT_ROOT,
    )


def reset_cluster_node_state() -> None:
    print("Cleaning local cluster databases and logs...")
    if not CLUSTER_STATE_ROOT.exists():
        return

    for node_dir in CLUSTER_STATE_ROOT.glob("node-*"):
        if not node_dir.is_dir():
            continue

        database_file = node_dir / "anonnetcore.db"
        if database_file.exists():
            database_file.unlink()

        logs_dir = node_dir / "logs"
        if not logs_dir.exists():
            continue

        for log_file in logs_dir.iterdir():
            if log_file.is_file():
                log_file.unlink()


def start_compose(*, detach: bool) -> None:
    command = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "up",
        "--build",
    ]
    if detach:
        command.append("-d")

    print("Starting containers...")
    run_command(command, cwd=PROJECT_ROOT)


def run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    stdout=None,
    stderr=None,
) -> None:
    subprocess.run(
        command,
        cwd=cwd or PROJECT_ROOT,
        check=True,
        stdout=stdout,
        stderr=stderr,
    )


if __name__ == "__main__":
    raise SystemExit(main())
