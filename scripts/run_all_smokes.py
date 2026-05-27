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
DEFAULT_CLUSTER_NODES = 8


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

    print_summary(results, run_dir=run_dir)
    return 0 if all(result.passed for result in results) and len(results) == len(smokes) else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all AnonNet smoke tests from the simplest to the most advanced.",
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
        "--skip-dom",
        action="store_true",
        help="Skip the frontend-only DOM smoke.",
    )
    parser.add_argument(
        "--skip-stress",
        action="store_true",
        help="Skip the heavier API stress smokes.",
    )
    parser.add_argument(
        "--skip-social",
        action="store_true",
        help="Skip the integrated PoC social smoke.",
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
    smokes: list[SmokeSpec] = []

    smokes.append(
        SmokeSpec(
            name="crypto-aes-gcm-siv",
            level="00 crypto",
            command=[python, "tests/integration/crypto_aes_smoke.py"],
            description="Authenticated AES-GCM-SIV encryption, tamper rejection, and nonce reuse guard.",
            summary="AES-GCM-SIV roundtrip, integrity, nonce guard",
        )
    )
    smokes.append(
        SmokeSpec(
            name="reliable-session",
            level="00 session",
            command=[python, "tests/integration/reliable_session_smoke.py"],
            description="Reliable session sequencing, ACK cleanup, buffering, and deduplication.",
            summary="sequence order, ACK, buffering, dedup",
        )
    )
    smokes.append(
        SmokeSpec(
            name="physical-udp",
            level="00 transport",
            command=[python, "tests/integration/physical_udp_smoke.py"],
            description="Physical session handshake and keepalive over UDP with JSON chunk reassembly.",
            summary="UDP transport, chunking, session keepalive",
        )
    )

    if not args.skip_dom:
        smokes.append(
            SmokeSpec(
                name="poc-social-dom",
                level="01 frontend",
                command=["node", "poc/smokes/social_dom.js"],
                description="Frontend DOM behavior without core/network dependencies.",
                summary="DOM profile, friends, posts, and DM UI",
            )
        )

    smokes.extend(
        [
            SmokeSpec(
                name="debug-state",
                level="02 cluster",
                command=[python, "tests/integration/debug_state_smoke.py"],
                description="Docker cluster health and debug snapshot consistency.",
                summary="cluster health, sessions, DHT duplicates",
            ),
            SmokeSpec(
                name="virtual-session",
                level="03 virtual",
                command=[
                    python,
                    "tests/integration/virtual_session_smoke.py",
                    "--cluster-nodes",
                    cluster_nodes,
                ],
                description="Virtual route discovery and virtual session establishment.",
                summary="DRT route discovery and session keepalive",
            ),
            SmokeSpec(
                name="virtual-message",
                level="04 virtual",
                command=[
                    python,
                    "tests/integration/virtual_message_smoke.py",
                    "--cluster-nodes",
                    cluster_nodes,
                ],
                description="Application messages over an active virtual session.",
                summary="virtual app message exchange",
            ),
            SmokeSpec(
                name="virtual-content",
                level="05 virtual",
                command=[
                    python,
                    "tests/integration/virtual_content_smoke.py",
                    "--cluster-nodes",
                    cluster_nodes,
                ],
                description="Virtual content info/range transfer flow.",
                summary="content info and byte range download",
            ),
            SmokeSpec(
                name="core-full-flow",
                level="06 full core",
                command=[
                    python,
                    "tests/integration/core_full_flow_smoke.py",
                    "--cluster-nodes",
                    cluster_nodes,
                ],
                description="Route, DRT, virtual session, message, and content flow together.",
                summary="route, DRT, session, message, content",
            ),
        ]
    )

    if not args.skip_stress:
        smokes.extend(
            [
                SmokeSpec(
                    name="virtual-api-stress",
                    level="07 api stress",
                    command=[
                        python,
                        "tests/integration/virtual_api_stress_smoke.py",
                        "--cluster-nodes",
                        cluster_nodes,
                    ],
                    description="HTTP API stress with two cores and randomized virtual traffic.",
                    summary="two-core API traffic, messages, downloads",
                ),
                SmokeSpec(
                    name="virtual-api-local-vn-stress",
                    level="08 api stress",
                    command=[
                        python,
                        "tests/integration/virtual_api_local_vn_stress_smoke.py",
                        "--cluster-nodes",
                        cluster_nodes,
                    ],
                    description="HTTP API stress with two local VNs on the same core.",
                    summary="same-core multi-VN API stress",
                ),
            ]
        )

    if not args.skip_social:
        smokes.append(
            SmokeSpec(
                name="poc-social-integrated",
                level="09 poc",
                command=[python, "scripts/run_social_smoke.py", cluster_nodes],
                description="Integrated PoC social flow through the core HTTP API.",
                summary="PoC social profiles, feed, DHT, DM",
            )
        )

    return smokes


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


def format_command(command: list[str]) -> str:
    return " ".join(quote_arg(part) for part in command)


def quote_arg(value: str) -> str:
    if not value or any(char.isspace() for char in value):
        return f'"{value}"'
    return value


if __name__ == "__main__":
    raise SystemExit(main())
