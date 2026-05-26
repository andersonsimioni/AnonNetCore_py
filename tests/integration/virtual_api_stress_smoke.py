from __future__ import annotations

import argparse
import asyncio
import base64
from dataclasses import dataclass
import json
from pathlib import Path
import random
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from core_helpers import reset_core_data_dir, stop_cores
from smoke_helpers import (
    MIN_CLUSTER_NODES,
    create_test_core,
    reset_cluster,
    resolve_cluster_node_count,
    resolve_required_ready_nodes,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_stable_drt_online_route_count,
    wait_for_network_ready,
    wait_until_value,
)


TEST_DATA_BASE_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "virtual-api-stress-smoke"
APP_MESSAGE_TYPE = "integration.virtual-api-stress.message"
DEFAULT_CORE_A_PORT = 19501
DEFAULT_CORE_B_PORT = 19502
DEFAULT_LOCAL_CORE_PORT_POOL = range(19501, 19541)


async def main() -> None:
    args = parse_args()
    random_source = random.Random(args.seed)
    test_data_root = TEST_DATA_BASE_ROOT / args.run_id
    test_log_root = test_data_root / "logs"
    ports = resolve_smoke_ports(args)
    cluster_nodes = resolve_cluster_node_count(args.cluster_nodes)
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )

    reset_core_data_dir(test_data_root)
    print(f"reset test data: {test_data_root}", flush=True)
    print(
        "resolved smoke ports: "
        f"core_a={ports.core_a} core_b={ports.core_b} "
        f"api_a={ports.api_a} api_b={ports.api_b}",
        flush=True,
    )
    await run_sync_step("reset docker cluster", reset_cluster, timeout_seconds=60.0)
    await run_sync_step(
        f"start docker cluster: nodes={cluster_nodes}",
        lambda: start_cluster(node_count=cluster_nodes),
        timeout_seconds=180.0,
    )
    await run_sync_step(
        "wait docker cluster containers",
        lambda: wait_for_cluster_containers(expected_count=cluster_nodes),
        timeout_seconds=120.0,
    )

    core_a = create_test_core(
        data_dir=test_data_root / "core-a",
        listen_port=ports.core_a,
        log_dir=test_log_root / "core-a",
        api_port=ports.api_a,
        virtual_route_expected_round_trip_ttl_ms=args.route_rtt_ms,
        virtual_route_pending_timeout_seconds=args.route_pending_timeout_seconds,
        virtual_route_min_online_routes=args.min_online_routes,
    )
    core_b = create_test_core(
        data_dir=test_data_root / "core-b",
        listen_port=ports.core_b,
        log_dir=test_log_root / "core-b",
        api_port=ports.api_b,
        virtual_route_expected_round_trip_ttl_ms=args.route_rtt_ms,
        virtual_route_pending_timeout_seconds=args.route_pending_timeout_seconds,
        virtual_route_min_online_routes=args.min_online_routes,
    )
    api_a = JsonApiClient(f"http://127.0.0.1:{ports.api_a}", timeout_seconds=150.0)
    api_b = JsonApiClient(f"http://127.0.0.1:{ports.api_b}", timeout_seconds=150.0)

    try:
        await run_async_step(
            "checkpoint 1: start API cores A/B",
            asyncio.gather(core_a.start(), core_b.start()),
            timeout_seconds=45.0,
        )
        await run_sync_step(
            "checkpoint 1b: docker can reach local core TCP ports",
            lambda: wait_for_cluster_to_reach_local_core_ports(ports.core_a, ports.core_b),
            timeout_seconds=45.0,
        )

        vn_a, vn_b = await run_async_step(
            "checkpoint 2: create API virtual nodes",
            asyncio.gather(
                api_a.post(
                    "/v1/virtual-nodes",
                    {
                        "kind": "virtual-api-stress-a",
                        "metadata": {"source": "virtual_api_stress_smoke"},
                    },
                ),
                api_b.post(
                    "/v1/virtual-nodes",
                    {
                        "kind": "virtual-api-stress-b",
                        "metadata": {"source": "virtual_api_stress_smoke"},
                    },
                ),
            ),
            timeout_seconds=30.0,
        )
        print(
            f"checkpoint 2 details: vn_a={vn_a['id']} vn_b={vn_b['id']}",
            flush=True,
        )

        await run_async_step(
            f"checkpoint 3a: wait network ready: required_ready_nodes={required_ready_nodes}",
            asyncio.gather(
                wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes),
                wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes),
            ),
            timeout_seconds=240.0,
        )
        await run_async_step(
            "checkpoint 3b: wait cluster network maturity",
            wait_for_cluster_network_maturity(
                core_a,
                core_b,
                required_ready_nodes=required_ready_nodes,
            ),
            timeout_seconds=120.0,
        )

        route_inventory = await run_async_step(
            f"checkpoint 4: wait DRT route inventories: min_online_routes={args.min_online_routes}",
            asyncio.gather(
                wait_for_stable_drt_online_route_count(
                    core_b,
                    virtual_node_public_key=str(vn_a["public_key"]),
                    minimum_routes=args.min_online_routes,
                    timeout_seconds=args.route_inventory_timeout_seconds,
                ),
                wait_for_stable_drt_online_route_count(
                    core_a,
                    virtual_node_public_key=str(vn_b["public_key"]),
                    minimum_routes=args.min_online_routes,
                    timeout_seconds=args.route_inventory_timeout_seconds,
                ),
            ),
            timeout_seconds=args.route_inventory_timeout_seconds + 15.0,
        )
        print(
            "checkpoint 4 details: "
            f"vn_a_routes={route_inventory[0]['online_route_count']} "
            f"vn_b_routes={route_inventory[1]['online_route_count']}",
            flush=True,
        )

        await run_async_step(
            "checkpoint 5: upsert API remote VN identities",
            asyncio.gather(
                api_a.post(
                    "/v1/virtual-nodes/remote",
                    {
                        "node_id": vn_b["id"],
                        "public_key": vn_b["public_key"],
                        "kind": vn_b["kind"],
                        "metadata": {"source": "virtual_api_stress_smoke"},
                    },
                ),
                api_b.post(
                    "/v1/virtual-nodes/remote",
                    {
                        "node_id": vn_a["id"],
                        "public_key": vn_a["public_key"],
                        "kind": vn_a["kind"],
                        "metadata": {"source": "virtual_api_stress_smoke"},
                    },
                ),
            ),
            timeout_seconds=30.0,
        )

        session_b_to_a = await run_async_step(
            "checkpoint 6a: start API virtual session B->A",
            api_b.post(
                "/v1/sessions/virtual",
                {
                    "local_virtual_node_id": vn_b["id"],
                    "remote_virtual_node_id": vn_a["id"],
                },
            ),
            timeout_seconds=90.0,
        )
        session_a_to_b = await run_async_step(
            "checkpoint 6b: start API virtual session A->B",
            api_a.post(
                "/v1/sessions/virtual",
                {
                    "local_virtual_node_id": vn_a["id"],
                    "remote_virtual_node_id": vn_b["id"],
                },
            ),
            timeout_seconds=90.0,
        )
        print(
            "checkpoint 6 details: "
            f"b_to_a={session_b_to_a['session_id']} a_to_b={session_a_to_b['session_id']}",
            flush=True,
        )

        await run_async_step(
            "checkpoint 7a: subscribe API virtual inboxes",
            asyncio.gather(
                api_a.post("/v1/messages/virtual/subscribe", {"app_message_type": APP_MESSAGE_TYPE}),
                api_b.post("/v1/messages/virtual/subscribe", {"app_message_type": APP_MESSAGE_TYPE}),
            ),
            timeout_seconds=30.0,
        )
        await run_async_step(
            f"checkpoint 7b: API virtual message stress: rounds={args.message_rounds}",
            exercise_virtual_messages(
                api_a=api_a,
                api_b=api_b,
                session_a_to_b=str(session_a_to_b["session_id"]),
                session_b_to_a=str(session_b_to_a["session_id"]),
                rounds=args.message_rounds,
                random_source=random_source,
            ),
            timeout_seconds=max(60.0, args.message_rounds * 35.0),
        )

        await run_async_step(
            f"checkpoint 8: API virtual content stress: downloads={args.content_downloads}",
            exercise_content_downloads(
                api_provider=api_a,
                api_downloader=api_b,
                provider_virtual_node_id=str(vn_a["id"]),
                session_id=str(session_b_to_a["session_id"]),
                downloads=args.content_downloads,
                random_source=random_source,
            ),
            timeout_seconds=max(90.0, args.content_downloads * 90.0),
        )

        print("OK virtual API stress smoke passed", flush=True)
    finally:
        try:
            await asyncio.wait_for(stop_cores(core_b, core_a), timeout=30.0)
        except TimeoutError:
            print("warning: timed out while stopping API smoke cores", flush=True)


async def exercise_virtual_messages(
    *,
    api_a: "JsonApiClient",
    api_b: "JsonApiClient",
    session_a_to_b: str,
    session_b_to_a: str,
    rounds: int,
    random_source: random.Random,
) -> None:
    for index in range(1, rounds + 1):
        if random_source.choice([True, False]):
            sender = api_a
            receiver = api_b
            session_id = session_a_to_b
            direction = "a_to_b"
        else:
            sender = api_b
            receiver = api_a
            session_id = session_b_to_a
            direction = "b_to_a"

        payload = {
            "direction": direction,
            "sequence": index,
            "body": random_text(random_source, min_size=16, max_size=512),
            "nonce": random_source.getrandbits(64),
        }
        print(f"message round {index}/{rounds}: direction={direction}", flush=True)
        result = await sender.post(
            f"/v1/sessions/virtual/{quote(session_id, safe='')}/messages",
            {
                "app_message_type": APP_MESSAGE_TYPE,
                "payload": payload,
            },
        )
        request_id = str(result["request_id"])
        await wait_for_api_message(
            receiver,
            request_id=request_id,
            expected_payload=payload,
            label=f"virtual api message {index} {direction}",
        )


async def exercise_content_downloads(
    *,
    api_provider: "JsonApiClient",
    api_downloader: "JsonApiClient",
    provider_virtual_node_id: str,
    session_id: str,
    downloads: int,
    random_source: random.Random,
) -> None:
    for index in range(1, downloads + 1):
        print(f"content download {index}/{downloads}: preparing random content", flush=True)
        content_bytes = random_source.randbytes(random_source.randint(8192, 128 * 1024))
        stored = await api_provider.post(
            "/v1/content",
            {
                "data_base64": base64.b64encode(content_bytes).decode("ascii"),
                "title": f"virtual-api-stress-{index}",
                "content_type": "application/octet-stream",
                "tags": ["integration", "virtual-api-stress"],
            },
        )
        content_id = str(stored["content_id"])
        publish_result = await api_provider.post(
            f"/v1/content/{quote(content_id, safe='')}/providers/ddt",
            {
                "local_virtual_node_id": provider_virtual_node_id,
                "async_publish": True,
            },
        )
        await wait_for_publish_job_stored(api_provider, str(publish_result["publish_result"]["job_id"]))

        await api_downloader.post(
            "/v1/downloads",
            {
                "session_id": session_id,
                "content_id": content_id,
            },
        )
        await wait_for_api_download(
            api_downloader,
            session_id=session_id,
            content_id=content_id,
            expected_size=len(content_bytes),
        )
        print(f"content download {index}/{downloads}: completed content_id={content_id}", flush=True)


async def run_async_step(label: str, awaitable, *, timeout_seconds: float):
    print(f"{label} started", flush=True)
    try:
        result = await asyncio.wait_for(awaitable, timeout=timeout_seconds)
    except Exception as error:
        print(f"{label} failed: {type(error).__name__}: {error}", flush=True)
        raise
    print(f"{label} OK", flush=True)
    return result


async def run_sync_step(label: str, func, *, timeout_seconds: float):
    return await run_async_step(
        label,
        asyncio.to_thread(func),
        timeout_seconds=timeout_seconds,
    )


def resolve_smoke_ports(args: argparse.Namespace) -> "SmokePorts":
    used_ports: set[int] = set()
    core_a = resolve_local_core_port(
        requested_port=args.core_a_port,
        default_port=DEFAULT_CORE_A_PORT,
        used_ports=used_ports,
        label="core-a",
    )
    core_b = resolve_local_core_port(
        requested_port=args.core_b_port,
        default_port=DEFAULT_CORE_B_PORT,
        used_ports=used_ports,
        label="core-b",
    )
    return SmokePorts(
        core_a=core_a,
        core_b=core_b,
        api_a=args.api_a_port or find_free_tcp_port(used_ports),
        api_b=args.api_b_port or find_free_tcp_port(used_ports),
    )


def resolve_local_core_port(
    *,
    requested_port: int | None,
    default_port: int,
    used_ports: set[int],
    label: str,
) -> int:
    if requested_port is not None:
        reserve_tcp_port(requested_port, used_ports=used_ports, label=label)
        return requested_port

    candidates = [default_port]
    candidates.extend(port for port in DEFAULT_LOCAL_CORE_PORT_POOL if port != default_port)
    for port in candidates:
        if port in used_ports or not is_tcp_port_available(port):
            continue
        used_ports.add(port)
        return port

    raise RuntimeError(
        f"No free local core TCP ports found in {DEFAULT_LOCAL_CORE_PORT_POOL.start}-"
        f"{DEFAULT_LOCAL_CORE_PORT_POOL.stop - 1}."
    )


def reserve_tcp_port(port: int, *, used_ports: set[int], label: str) -> None:
    if port in used_ports:
        raise RuntimeError(f"Duplicated {label} port in smoke config: {port}")
    if not is_tcp_port_available(port):
        raise RuntimeError(f"{label} TCP port is already in use: {port}")
    used_ports.add(port)


def is_tcp_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", port))
        except OSError:
            return False
    return True


def find_free_tcp_port(used_ports: set[int]) -> int:
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        if port not in used_ports:
            used_ports.add(port)
            return port


def wait_for_cluster_to_reach_local_core_ports(*ports: int) -> None:
    deadline = time.monotonic() + 40.0
    pending_ports = set(ports)
    last_errors: dict[int, str] = {}

    while time.monotonic() < deadline and pending_ports:
        for port in list(pending_ports):
            result = probe_tcp_from_cluster_container(port)
            if result.returncode == 0:
                print(f"docker reachability OK: host.docker.internal:{port}", flush=True)
                pending_ports.remove(port)
                continue

            last_errors[port] = (result.stderr or result.stdout).strip()
        if pending_ports:
            print(
                "waiting docker reachability: "
                f"pending_ports={sorted(pending_ports)}",
                flush=True,
            )
            time.sleep(1.0)

    if pending_ports:
        details = "; ".join(
            f"{port}: {last_errors.get(port, 'no error captured')}"
            for port in sorted(pending_ports)
        )
        raise TimeoutError(f"Docker cluster cannot reach local core ports: {details}")


def probe_tcp_from_cluster_container(port: int) -> subprocess.CompletedProcess[str]:
    script = (
        "import socket; "
        "sock=socket.create_connection(('host.docker.internal', "
        f"{port}), 2); "
        "sock.close()"
    )
    return subprocess.run(
        ["docker", "exec", "anonnet-node-001", "python", "-c", script],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )


async def wait_for_api_message(
    api: "JsonApiClient",
    *,
    request_id: str,
    expected_payload: dict[str, object],
    label: str,
) -> dict[str, object]:
    async def load_message():
        messages = await api.get(
            f"/v1/messages/virtual?app_message_type={quote(APP_MESSAGE_TYPE, safe='')}&limit=100&consume=true"
        )
        for message in messages:
            if message.get("request_id") == request_id:
                if message.get("payload") != expected_payload:
                    raise RuntimeError(f"{label} payload mismatch.")
                return message
        return None

    return await wait_until_value(load_message, timeout_seconds=30.0, label=label)


async def wait_for_publish_job_stored(api: "JsonApiClient", job_id: str) -> dict[str, object]:
    async def load_job():
        job = await api.get(f"/v1/dht/publish-jobs/{quote(job_id, safe='')}")
        if job.get("status") == "stored":
            return job
        if job.get("status") == "failed":
            raise RuntimeError(f"DHT publish job failed: {job}")
        return None

    return await wait_until_value(load_job, timeout_seconds=60.0, label="DHT publish job stored")


async def wait_for_api_download(
    api: "JsonApiClient",
    *,
    session_id: str,
    content_id: str,
    expected_size: int,
) -> dict[str, object]:
    async def load_download():
        try:
            state = await api.get(
                f"/v1/downloads/{quote(session_id, safe='')}/{quote(content_id, safe='')}"
            )
        except RuntimeError as error:
            if "download_not_found" in str(error):
                print(
                    "waiting API content download state: "
                    f"session_id={session_id} content_id={content_id}",
                    flush=True,
                )
                return None
            raise
        if state.get("status") == "completed":
            if int(state.get("size_bytes") or -1) != expected_size:
                raise RuntimeError("Downloaded content size mismatch.")
            return state
        if state.get("status") == "failed":
            raise RuntimeError(f"Content download failed: {state}")
        return None

    return await wait_until_value(load_download, timeout_seconds=60.0, label="API content download")


def random_text(random_source: random.Random, *, min_size: int, max_size: int) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_"
    size = random_source.randint(min_size, max_size)
    return "".join(random_source.choice(alphabet) for _ in range(size))


class JsonApiClient:
    def __init__(self, base_url: str, *, timeout_seconds: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def get(self, path: str) -> object:
        return await asyncio.to_thread(self._request, "GET", path, None)

    async def post(self, path: str, payload: dict[str, object]) -> object:
        return await asyncio.to_thread(self._request, "POST", path, payload)

    def _request(self, method: str, path: str, payload: dict[str, object] | None) -> object:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except HTTPError as error:
            raw_error = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"API {method} {path} failed: status={error.code} body={raw_error}") from error

        decoded = json.loads(raw.decode("utf-8"))
        if not decoded.get("ok"):
            raise RuntimeError(f"API {method} {path} failed: {decoded}")
        return decoded.get("data")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stress smoke: valida sessoes, mensagens e downloads virtuais via HTTP API.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=MIN_CLUSTER_NODES)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    parser.add_argument("--min-online-routes", type=int, default=1)
    parser.add_argument("--route-inventory-timeout-seconds", type=float, default=360.0)
    parser.add_argument("--route-rtt-ms", type=int, default=30000)
    parser.add_argument("--route-pending-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--message-rounds", type=int, default=12)
    parser.add_argument("--content-downloads", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260521)
    parser.add_argument("--run-id", default=f"run-{int(time.time())}")
    parser.add_argument("--core-a-port", type=int, default=None)
    parser.add_argument("--core-b-port", type=int, default=None)
    parser.add_argument("--api-a-port", type=int, default=None)
    parser.add_argument("--api-b-port", type=int, default=None)
    return parser.parse_args()


@dataclass(slots=True, frozen=True)
class SmokePorts:
    core_a: int
    core_b: int
    api_a: int
    api_b: int


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"virtual API stress smoke failed: {error}", file=sys.stderr)
        raise
