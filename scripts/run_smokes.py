from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_SUPPORT_ROOT = PROJECT_ROOT / "tests" / "support"
DEFAULT_CLUSTER_NODES = 8
if str(TEST_SUPPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(TEST_SUPPORT_ROOT))

from smokes_config import SMOKES_CONFIG
from smoke_helpers import stop_cluster


@dataclass(frozen=True)
class SmokeSpec:
    name: str
    level: str
    command: list[str]
    description: str
    summary: str


@dataclass(frozen=True)
class SmokeResult:
    spec: SmokeSpec
    exit_code: int
    duration_seconds: float
    log_path: Path

    @property
    def passed(self) -> bool:
        return self.exit_code == 0


def main() -> int:
    args = parse_args()
    run_dir = build_run_dir(args.run_dir)
    smokes = build_smoke_plan(args)

    if args.list:
        print_smoke_plan(smokes)
        return 0

    run_dir.mkdir(parents=True, exist_ok=True)
    print_header("AnonNet smoke suite")
    print(f"Run directory: {run_dir}")
    print(f"Cluster nodes requested for scalable smokes: {args.cluster_nodes}")
    print(f"Stop on first failure: {not args.keep_going}")
    print_smoke_plan(smokes)

    results: list[SmokeResult] = []
    try:
        for index, spec in enumerate(smokes, start=1):
            result = run_smoke(
                spec,
                index=index,
                total=len(smokes),
                run_dir=run_dir,
            )
            results.append(result)
            if not result.passed and not args.keep_going:
                break
    finally:
        stop_smoke_cluster()

    print_summary(results, run_dir=run_dir)
    return 0 if all(result.passed for result in results) and len(results) == len(smokes) else 1


def stop_smoke_cluster() -> None:
    try:
        stop_cluster()
    except Exception as error:
        print(f"Cluster cleanup failed: {error}", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the official AnonNet smoke flows and summarize the results.",
    )
    parser.add_argument(
        "cluster_nodes",
        type=int,
        nargs="?",
        default=DEFAULT_CLUSTER_NODES,
        help="Cluster size passed to scalable integration smokes.",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Directory where the smoke logs will be written.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue running the remaining smokes after a failure.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the smoke plan without running it.",
    )
    return parser.parse_args()


def build_run_dir(custom_run_dir: Path | None) -> Path:
    if custom_run_dir is not None:
        return custom_run_dir

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return PROJECT_ROOT / "data" / "local" / "smoke-runs" / timestamp


def build_smoke_plan(args: argparse.Namespace) -> list[SmokeSpec]:
    python = sys.executable
    cluster_nodes = str(args.cluster_nodes)
    return [
        SmokeSpec(
            name="core-full-flow",
            level="01 core",
            command=[
                python,
                "tests/integration/core_full_flow_smoke.py",
                "--cluster-nodes",
                cluster_nodes,
            ],
            description="Full core validation: physical discovery, DHT/DRT, routes, virtual session, messages, and content.",
            summary="core protocols, DHT/DRT, route, session, message, content",
        ),
        SmokeSpec(
            name="poc-full-flow",
            level="02 poc",
            command=[
                python,
                "tests/integration/poc_full_flow_smoke.py",
                cluster_nodes,
            ],
            description="Full PoC social validation through the core HTTP API and WebSocket-facing flow.",
            summary="social profiles, feed, DHT, DM, API flow",
        ),
    ]


def print_smoke_plan(smokes: list[SmokeSpec]) -> None:
    print("")
    print("Smoke plan:")
    for index, spec in enumerate(smokes, start=1):
        print(f"{index:02d}. [{spec.level}] {spec.name}")
        print(f"    {spec.description}")
        print(f"    command: {format_command(spec.command)}")
    print("")


def run_smoke(
    spec: SmokeSpec,
    *,
    index: int,
    total: int,
    run_dir: Path,
) -> SmokeResult:
    log_path = run_dir / f"{index:02d}-{spec.name}.log"
    print_header(f"{index:02d}/{total} {spec.name}")
    print(f"Level: {spec.level}")
    print(f"Description: {spec.description}")
    print(f"Command: {format_command(spec.command)}")
    print(f"Log: {log_path}")

    started_at = time.monotonic()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    with log_path.open("w", encoding="utf-8", errors="replace") as log_file:
        log_file.write(f"$ {format_command(spec.command)}\n\n")
        log_file.flush()
        process = subprocess.Popen(
            spec.command,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        assert process.stdout is not None
        for line in process.stdout:
            if should_echo_smoke_line(line):
                print(line, end="")
            log_file.write(line)
        exit_code = process.wait()

    duration_seconds = time.monotonic() - started_at
    status = "PASS" if exit_code == 0 else "FAIL"
    print(f"\n{status} {spec.name} in {duration_seconds:.1f}s")
    return SmokeResult(
        spec=spec,
        exit_code=exit_code,
        duration_seconds=duration_seconds,
        log_path=log_path,
    )


def print_summary(results: list[SmokeResult], *, run_dir: Path) -> None:
    print_header("Final smoke evidence")
    passed = len([result for result in results if result.passed])
    total_seconds = sum(result.duration_seconds for result in results)
    print(f"Result: {passed}/{len(results)} passed | total={total_seconds:.1f}s")
    print("")
    print("Passed smokes:")
    for result in results:
        if not result.passed:
            continue
        print(
            f"OK {result.spec.name:<30} "
            f"{result.duration_seconds:>7.1f}s | {result.spec.summary}"
        )

    failed_results = [result for result in results if not result.passed]
    if failed_results:
        print("")
        print("Failed smokes:")
        for result in failed_results:
            print(
                f"FAIL {result.spec.name:<28} "
                f"exit={result.exit_code} | log={result.log_path.name}"
            )

    print("")
    print(f"Logs: {run_dir}")


def print_header(title: str) -> None:
    line = "=" * max(72, len(title) + 8)
    print("")
    print(line)
    print(title)
    print(line)


def should_echo_smoke_line(line: str) -> bool:
    important_fragments = (
        "checkpoint ",
        " OK",
        " PASS",
        " FAIL",
        "failed:",
        "Traceback",
        "Timed out",
        "Run directory:",
        "reset test data:",
        "docker cluster running:",
        "starting docker cluster:",
        "Seed de transporte:",
        "profile=",
    )
    return any(fragment in line for fragment in important_fragments)


def format_command(command: list[str]) -> str:
    return " ".join(quote_arg(part) for part in command)


def quote_arg(value: str) -> str:
    if not value or any(char.isspace() for char in value):
        return f'"{value}"'
    return value


if __name__ == "__main__":
    raise SystemExit(main())
