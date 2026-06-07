from __future__ import annotations

from pathlib import Path
import subprocess
import time


CLUSTER_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = CLUSTER_ROOT.parent
COMPOSE_FILE = CLUSTER_ROOT / "docker-compose.generated.yml"


def main() -> int:
    if not COMPOSE_FILE.exists():
        raise SystemExit(f"Generated compose file not found at: {COMPOSE_FILE}")

    verify_docker_is_available()
    print("Stopping cluster containers...")
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
    last_result: subprocess.CompletedProcess[str] | None = None
    for attempt in range(1, 6):
        last_result = subprocess.run(
            ["docker", "info"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        if last_result.returncode == 0:
            return

        print(
            "waiting for Docker daemon: "
            f"attempt={attempt}/5 "
            f"error={(last_result.stderr or last_result.stdout or '').strip()}"
        )
        time.sleep(2)

    raise RuntimeError(
        "Docker is not available. "
        f"Last error: {(last_result.stderr or last_result.stdout or '').strip() if last_result else ''}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
