from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
CLUSTER_UP_SCRIPT = PROJECT_ROOT / "cluster" / "up_nodes.py"
CORE_ENTRYPOINT = PROJECT_ROOT / "app" / "main.py"
POC_ROOT = PROJECT_ROOT / "poc"


def main() -> int:
    args = parse_args()
    processes: list[subprocess.Popen] = []

    try:
        if not args.skip_cluster:
            run_cluster(args.cluster_nodes)

        processes.append(start_local_core(args.core_listen_port))
        processes.append(start_poc_server(args.poc_port))

        print("")
        print("PoC pronta.")
        print(f"- Core TCP local: 0.0.0.0:{args.core_listen_port}")
        print("- Core HTTP API: http://127.0.0.1:18080")
        print("- Core WebSocket: ws://127.0.0.1:18081/v1/events")
        print(f"- Front PoC: http://127.0.0.1:{args.poc_port}/web/")
        print("")
        print("Pressione Ctrl+C para parar o core local e o servidor da PoC.")

        wait_until_interrupted(processes)
        return 0
    finally:
        stop_processes(processes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sobe cluster Docker, core local e servidor web da PoC social.",
    )
    parser.add_argument(
        "cluster_nodes",
        type=int,
        nargs="?",
        default=10,
        help="Quantidade de nodes no cluster Docker.",
    )
    parser.add_argument(
        "--core-listen-port",
        type=int,
        default=19101,
        help="Porta TCP do core local fora do cluster.",
    )
    parser.add_argument(
        "--poc-port",
        type=int,
        default=18100,
        help="Porta HTTP estatica para servir a PoC.",
    )
    parser.add_argument(
        "--skip-cluster",
        action="store_true",
        help="Nao sobe o cluster Docker; inicia apenas core local e PoC.",
    )
    args = parser.parse_args()
    if args.cluster_nodes < 2 and not args.skip_cluster:
        raise SystemExit("Use pelo menos 2 nodes no cluster.")
    return args


def run_cluster(node_count: int) -> None:
    print(f"Subindo cluster Docker com {node_count} nodes...")
    run_command(
        [
            sys.executable,
            str(CLUSTER_UP_SCRIPT),
            str(node_count),
            "--detach",
        ],
    )


def start_local_core(listen_port: int) -> subprocess.Popen:
    print(f"Iniciando core local na porta TCP {listen_port}...")
    env = build_child_environment()
    return subprocess.Popen(
        [
            sys.executable,
            str(CORE_ENTRYPOINT),
            "--listen-port",
            str(listen_port),
        ],
        cwd=PROJECT_ROOT,
        env=env,
    )


def start_poc_server(port: int) -> subprocess.Popen:
    print(f"Servindo PoC em http://127.0.0.1:{port}/web/ ...")
    env = build_child_environment()
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "http.server",
            str(port),
            "-d",
            str(POC_ROOT),
        ],
        cwd=PROJECT_ROOT,
        env=env,
    )


def wait_until_interrupted(processes: list[subprocess.Popen]) -> None:
    while True:
        for process in processes:
            exit_code = process.poll()
            if exit_code is not None:
                raise SystemExit(f"Processo encerrou antes da hora com exit code {exit_code}.")
        time.sleep(0.5)


def stop_processes(processes: list[subprocess.Popen]) -> None:
    for process in reversed(processes):
        if process.poll() is not None:
            continue
        stop_process(process)


def stop_process(process: subprocess.Popen) -> None:
    try:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=8)
    except Exception:
        process.kill()
        process.wait(timeout=8)


def run_command(command: list[str]) -> None:
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def build_child_environment() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    return env


if __name__ == "__main__":
    raise SystemExit(main())
