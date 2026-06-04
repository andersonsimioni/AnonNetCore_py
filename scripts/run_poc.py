from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
import webbrowser
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
CLUSTER_UP_SCRIPT = PROJECT_ROOT / "cluster" / "up_nodes.py"
RUN_CORE_SCRIPT = PROJECT_ROOT / "scripts" / "run_core.py"
DEBUG_CONSOLE_SCRIPT = APP_ROOT / "debug" / "console_server.py"
POC_INDEX = PROJECT_ROOT / "poc" / "index.html"


def main() -> int:
    args = parse_args()
    processes: list[subprocess.Popen] = []

    try:
        if not args.skip_cluster:
            start_cluster(args.cluster_nodes)
            wait_before_local_core(args.local_core_delay_seconds)

        processes.append(start_local_core(args.core_listen_port))
        wait_before_debug_console(args.debug_delay_seconds)
        processes.append(start_debug_console(args))

        if not args.no_open:
            open_poc_html()
            open_debug_console(args.debug_host, args.debug_port)

        print("")
        print("PoC pronta.")
        print(f"- Core TCP local: 0.0.0.0:{args.core_listen_port}")
        print("- Core HTTP API: http://127.0.0.1:18080")
        print("- Core WebSocket: ws://127.0.0.1:18081/v1/events")
        print(f"- Front PoC local: {POC_INDEX}")
        print(f"- Debug Console: http://{args.debug_host}:{args.debug_port}")
        print("")
        print("Pressione Ctrl+C para parar o core local e o debug console.")

        wait_until_interrupted(processes)
        return 0
    finally:
        stop_processes(processes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sobe cluster Docker, core local e abre a PoC como HTML local.",
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
        "--local-core-delay-seconds",
        type=float,
        default=8.0,
        help="Tempo de espera apos subir o cluster antes de iniciar o core local.",
    )
    parser.add_argument(
        "--debug-delay-seconds",
        type=float,
        default=2.0,
        help="Tempo de espera apos iniciar o core local antes de iniciar o debug console.",
    )
    parser.add_argument(
        "--debug-host",
        default="127.0.0.1",
        help="Host HTTP do debug console.",
    )
    parser.add_argument(
        "--debug-port",
        type=int,
        default=19888,
        help="Porta HTTP do debug console.",
    )
    parser.add_argument(
        "--core-debug-url",
        default="http://127.0.0.1:18080/debug/state",
        help="URL /debug/state do core local da PoC.",
    )
    parser.add_argument(
        "--skip-cluster",
        action="store_true",
        help="Nao sobe o cluster Docker; inicia apenas o core local.",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Nao abre o HTML/debug automaticamente.",
    )
    parser.add_argument(
        "--no-docker-debug",
        action="store_true",
        help="Debug console nao descobre containers anonnet-node-*.",
    )
    args = parser.parse_args()
    if args.cluster_nodes < 2 and not args.skip_cluster:
        raise SystemExit("Use pelo menos 2 nodes no cluster.")
    return args


def start_cluster(node_count: int) -> None:
    print(f"Subindo cluster Docker com {node_count} nodes...")
    run_command(
        [
            sys.executable,
            str(CLUSTER_UP_SCRIPT),
            str(node_count),
            "--detach",
        ],
    )


def wait_before_local_core(delay_seconds: float) -> None:
    if delay_seconds <= 0:
        return

    print(f"Aguardando {delay_seconds:.1f}s antes de iniciar o core local...")
    time.sleep(delay_seconds)


def wait_before_debug_console(delay_seconds: float) -> None:
    if delay_seconds <= 0:
        return

    print(f"Aguardando {delay_seconds:.1f}s antes de iniciar o debug console...")
    time.sleep(delay_seconds)


def start_local_core(listen_port: int) -> subprocess.Popen:
    print(f"Iniciando core local na porta TCP {listen_port}...")
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen(
        [
            sys.executable,
            str(RUN_CORE_SCRIPT),
            "--listen-port",
            str(listen_port),
        ],
        cwd=PROJECT_ROOT,
        creationflags=creationflags,
    )


def start_debug_console(args: argparse.Namespace) -> subprocess.Popen:
    command = [
        sys.executable,
        str(DEBUG_CONSOLE_SCRIPT),
        "--host",
        args.debug_host,
        "--port",
        str(args.debug_port),
        "--api",
        args.core_debug_url,
    ]
    if args.no_docker_debug:
        command.append("--no-docker")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(APP_ROOT)
    print(f"Iniciando debug console em http://{args.debug_host}:{args.debug_port}...")
    return subprocess.Popen(command, cwd=PROJECT_ROOT, env=env)


def open_poc_html() -> None:
    if not POC_INDEX.exists():
        raise FileNotFoundError(f"PoC HTML nao encontrado: {POC_INDEX}")

    print(f"Abrindo PoC local: {POC_INDEX}")
    webbrowser.open(POC_INDEX.as_uri())


def open_debug_console(host: str, port: int) -> None:
    debug_url = f"http://{host}:{port}"
    print(f"Abrindo Debug Console: {debug_url}")
    webbrowser.open(debug_url)


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
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGTERM)
        process.wait(timeout=8)
    except Exception:
        process.kill()
        process.wait(timeout=8)


def run_command(command: list[str]) -> None:
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
