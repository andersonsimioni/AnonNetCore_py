from __future__ import annotations

from pathlib import Path
import subprocess


CLUSTER_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = CLUSTER_ROOT.parent
COMPOSE_FILE = CLUSTER_ROOT / "docker-compose.generated.yml"


def main() -> int:
    if not COMPOSE_FILE.exists():
        raise SystemExit(f"Compose gerado nao encontrado em: {COMPOSE_FILE}")

    verify_docker_is_available()
    print("Derrubando containers do cluster...")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "down",
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )
    return 0


def verify_docker_is_available() -> None:
    subprocess.run(
        ["docker", "info"],
        cwd=PROJECT_ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


if __name__ == "__main__":
    raise SystemExit(main())
