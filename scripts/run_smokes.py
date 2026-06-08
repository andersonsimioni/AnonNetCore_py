from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from threading import Lock, Thread
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_SUPPORT_ROOT = PROJECT_ROOT / "tests" / "support"
DEFAULT_CLUSTER_NODES = 8
SMOKE_ERROR_COLLECTOR_HOST = "0.0.0.0"
SMOKE_ERROR_COLLECTOR_PORT = 18999
SMOKE_ERROR_COLLECTOR_PATH = "/v1/smoke-log-events"
TOPOLOGY_LINE_PATTERN = re.compile(
    r"^(?P<node>node-\d+): profile=(?P<profile>\S+) reachability=(?P<reachability>\S+) "
    r"tcp=(?P<tcp>true|false) udp=(?P<udp>true|false) relay=(?P<relay>true|false)"
)
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
class SmokeTopology:
    nodes: tuple[dict[str, object], ...]

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    def profile_counts(self) -> Counter[str]:
        return Counter(str(node["profile"]) for node in self.nodes)

    def reachability_counts(self) -> Counter[str]:
        return Counter(str(node["reachability"]) for node in self.nodes)

    def transport_counts(self) -> Counter[str]:
        counts: Counter[str] = Counter()
        for node in self.nodes:
            tcp = bool(node["tcp"])
            udp = bool(node["udp"])
            if tcp and udp:
                counts["tcp_udp"] += 1
            elif tcp:
                counts["tcp_only"] += 1
            elif udp:
                counts["udp_only"] += 1
            else:
                counts["no_direct_transport"] += 1
        return counts

    def relay_counts(self) -> Counter[str]:
        counts: Counter[str] = Counter()
        for node in self.nodes:
            counts["relay_capable" if bool(node["relay"]) else "not_relay_capable"] += 1
        return counts


@dataclass(frozen=True)
class SmokeResult:
    spec: SmokeSpec
    exit_code: int
    duration_seconds: float
    log_path: Path
    topology: "SmokeTopology"

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
    error_collector = SmokeErrorCollector(
        host=SMOKE_ERROR_COLLECTOR_HOST,
        port=SMOKE_ERROR_COLLECTOR_PORT,
        path=SMOKE_ERROR_COLLECTOR_PATH,
        output_path=run_dir / "reported-errors.jsonl",
    )
    print_header("AnonNet smoke suite")
    print(f"Run directory: {run_dir}")
    print(f"Cluster nodes requested for scalable smokes: {args.cluster_nodes}")
    print(f"Stop on first failure: {not args.keep_going}")
    error_collector.start()
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
        error_collector.stop()

    print_summary(results, run_dir=run_dir, error_collector=error_collector)
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
    topology_nodes: list[dict[str, object]] = []
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
            topology_node = parse_topology_line(line)
            if topology_node is not None:
                topology_nodes.append(topology_node)
            if should_echo_smoke_line(line):
                print(line, end="")
            log_file.write(line)
        exit_code = process.wait()

    duration_seconds = time.monotonic() - started_at
    status = "PASS" if exit_code == 0 else "FAIL"
    print(f"\n{status} {spec.name} in {duration_seconds:.1f}s")
    topology = SmokeTopology(nodes=tuple(topology_nodes))
    print_topology_summary(spec.name, topology)
    return SmokeResult(
        spec=spec,
        exit_code=exit_code,
        duration_seconds=duration_seconds,
        log_path=log_path,
        topology=topology,
    )


def print_summary(
    results: list[SmokeResult],
    *,
    run_dir: Path,
    error_collector: "SmokeErrorCollector",
) -> None:
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
    print_global_topology_summary(results)
    error_collector.print_summary()


def print_header(title: str) -> None:
    line = "=" * max(72, len(title) + 8)
    print("")
    print(line)
    print(title)
    print(line)


def should_echo_smoke_line(line: str) -> bool:
    important_fragments = (
        "STEP ",
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


def parse_topology_line(line: str) -> dict[str, object] | None:
    match = TOPOLOGY_LINE_PATTERN.match(line.strip())
    if match is None:
        return None

    return {
        "node": match.group("node"),
        "profile": match.group("profile"),
        "reachability": match.group("reachability"),
        "tcp": match.group("tcp") == "true",
        "udp": match.group("udp") == "true",
        "relay": match.group("relay") == "true",
    }


def print_topology_summary(smoke_name: str, topology: SmokeTopology) -> None:
    print("")
    print(f"Topology for {smoke_name}: random draw from cluster profile generator")
    if topology.node_count == 0:
        print("TOPOLOGY no cluster topology was captured for this smoke")
        return

    print(f"TOPOLOGY nodes={topology.node_count}")
    print(f"TOPOLOGY profiles={dict(topology.profile_counts())}")
    print(f"TOPOLOGY reachability={dict(topology.reachability_counts())}")
    print(f"TOPOLOGY transports={dict(topology.transport_counts())}")
    print(f"TOPOLOGY relay={dict(topology.relay_counts())}")


def print_global_topology_summary(results: list[SmokeResult]) -> None:
    all_nodes: list[dict[str, object]] = []
    for result in results:
        all_nodes.extend(result.topology.nodes)

    print("")
    print("Network topology summary:")
    print("TOPOLOGY source=random draw per smoke cluster")
    if not all_nodes:
        print("TOPOLOGY no cluster topology was captured")
        return

    topology = SmokeTopology(nodes=tuple(all_nodes))
    print(f"TOPOLOGY total_cluster_instances={len(results)} total_nodes={topology.node_count}")
    print(f"TOPOLOGY profiles={dict(topology.profile_counts())}")
    print(f"TOPOLOGY reachability={dict(topology.reachability_counts())}")
    print(f"TOPOLOGY transports={dict(topology.transport_counts())}")
    print(f"TOPOLOGY relay={dict(topology.relay_counts())}")


def format_command(command: list[str]) -> str:
    return " ".join(quote_arg(part) for part in command)


def quote_arg(value: str) -> str:
    if not value or any(char.isspace() for char in value):
        return f'"{value}"'
    return value


class SmokeErrorCollector:
    """Small HTTP collector used only by smoke runs to centralize node warnings/errors."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        path: str,
        output_path: Path,
    ) -> None:
        self.host = host
        self.port = port
        self.path = path
        self.output_path = output_path
        self._lock = Lock()
        self._events: list[dict[str, object]] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    @property
    def public_endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}{self.path}"

    def start(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text("", encoding="utf-8")
        handler = self._build_handler()
        try:
            self._server = ThreadingHTTPServer((self.host, self.port), handler)
        except OSError as error:
            print(f"Smoke error collector disabled: {error}", file=sys.stderr)
            return

        self._thread = Thread(
            target=self._server.serve_forever,
            name="anonnet-smoke-error-collector",
            daemon=True,
        )
        self._thread.start()
        print(f"Smoke error collector: {self.public_endpoint}")

    def stop(self) -> None:
        if self._server is None:
            return

        self._server.shutdown()
        self._server.server_close()
        self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def print_summary(self) -> None:
        with self._lock:
            events = list(self._events)
        print("")
        print("Reported node warnings/errors:")
        print(f"REPORT total={len(events)} | file={self.output_path.name}")
        if not events:
            return

        by_level = Counter(str(event.get("level") or "unknown") for event in events)
        by_component = Counter(str(event.get("component") or "unknown") for event in events)
        by_node = Counter(str(event.get("node") or "unknown") for event in events)
        print(f"REPORT levels={dict(by_level)}")
        print(f"REPORT top_nodes={dict(by_node.most_common(5))}")
        print(f"REPORT top_components={dict(by_component.most_common(5))}")

    def _build_handler(self):
        collector = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != collector.path:
                    self.send_response(404)
                    self.end_headers()
                    return

                content_length = int(self.headers.get("Content-Length") or "0")
                raw_body = self.rfile.read(content_length)
                try:
                    payload = json.loads(raw_body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self.send_response(400)
                    self.end_headers()
                    return

                collector._store_payload(payload)
                self.send_response(204)
                self.end_headers()

            def log_message(self, _format: str, *_args: object) -> None:
                return

        return Handler

    def _store_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return

        raw_events = payload.get("events")
        if not isinstance(raw_events, list):
            raw_events = [payload]

        events = [
            event
            for event in raw_events
            if isinstance(event, dict)
        ]
        if not events:
            return

        with self._lock:
            with self.output_path.open("a", encoding="utf-8") as output_file:
                for event in events:
                    self._events.append(event)
                    output_file.write(
                        json.dumps(event, separators=(",", ":"), ensure_ascii=True) + "\n"
                    )


if __name__ == "__main__":
    raise SystemExit(main())
