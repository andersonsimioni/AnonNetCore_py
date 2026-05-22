from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from urllib.error import URLError
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORE_DEBUG_URL = "http://127.0.0.1:18080/debug/state"
DOCKER_DEBUG_URL = "http://127.0.0.1:18080/debug/state"


def main() -> int:
    args = parse_args()
    registry = DebugNodeRegistry(
        api_urls=args.api,
        include_docker=not args.no_docker,
        docker_filter=args.docker_filter,
        timeout_seconds=args.node_timeout_seconds,
        cache_ttl_seconds=args.node_cache_ttl_seconds,
        max_workers=args.max_workers,
    )
    server = DebugConsoleServer(
        (args.host, args.port),
        build_handler(registry, refresh_interval_ms=int(args.refresh_interval_seconds * 1000)),
    )
    print(f"Debug console em http://{args.host}:{args.port}")
    print("Use Ctrl+C para parar.")
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        print("\nParando debug console...")
    finally:
        server.server_close()
    return 0


class DebugNodeRegistry:
    def __init__(
        self,
        *,
        api_urls: list[str],
        include_docker: bool,
        docker_filter: str,
        timeout_seconds: float,
        cache_ttl_seconds: float,
        max_workers: int,
    ) -> None:
        self.api_urls = list(api_urls)
        self.include_docker = include_docker
        self.docker_filter = docker_filter
        self.timeout_seconds = timeout_seconds
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_workers = max(1, max_workers)
        self._cache: dict[str, CachedDebugState] = {}
        self._cache_lock = threading.Lock()

    def collect(self) -> dict[str, object]:
        started_at = time.monotonic()
        targets = self._build_targets()
        nodes: list[dict[str, object]] = []
        with ThreadPoolExecutor(max_workers=min(self.max_workers, max(1, len(targets)))) as executor:
            future_by_target = {
                executor.submit(self._load_target_state, target): target
                for target in targets
            }
            for future in as_completed(future_by_target):
                target = future_by_target[future]
                try:
                    nodes.append(future.result())
                except Exception as error:
                    nodes.append(
                        {
                            "source": target.label,
                            "kind": target.kind,
                            "ok": False,
                            "error": compact_error(error),
                        }
                    )

        nodes.sort(key=lambda node: str(node.get("source") or ""))
        return {
            "ok": True,
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
            "node_count": len(nodes),
            "healthy_node_count": len([node for node in nodes if node.get("ok") is True]),
            "nodes": nodes,
        }

    def _build_targets(self) -> list["DebugTarget"]:
        targets: list[DebugTarget] = [
            HttpDebugTarget(label=f"api:{url}", url=url)
            for url in self.api_urls
        ]
        if self.include_docker:
            targets.extend(
                DockerDebugTarget(container_name=name, url=DOCKER_DEBUG_URL)
                for name in self._list_docker_node_names()
            )
        return targets

    def _list_docker_node_names(self) -> list[str]:
        completed = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"name={self.docker_filter}",
                "--format",
                "{{.Names}}",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            return []
        return sorted(line.strip() for line in completed.stdout.splitlines() if line.strip())

    def _load_target_state(self, target: "DebugTarget") -> dict[str, object]:
        cached = self._get_cached_state(target)
        if cached is not None:
            return cached

        try:
            state = target.load_state(self.timeout_seconds)
        except Exception as error:
            cached_error = self._get_cached_state(target, allow_stale=True)
            if cached_error is not None:
                cached_error["stale"] = True
                cached_error["warning"] = compact_error(error)
                return cached_error
            raise

        with self._cache_lock:
            self._cache[target.cache_key] = CachedDebugState(
                captured_monotonic=time.monotonic(),
                state=state,
            )
        return state

    def _get_cached_state(self, target: "DebugTarget", *, allow_stale: bool = False) -> dict[str, object] | None:
        with self._cache_lock:
            cached = self._cache.get(target.cache_key)
        if cached is None:
            return None
        age_seconds = time.monotonic() - cached.captured_monotonic
        if not allow_stale and age_seconds > self.cache_ttl_seconds:
            return None

        state = dict(cached.state)
        state["cached"] = True
        state["cache_age_seconds"] = round(age_seconds, 2)
        return state


@dataclass(slots=True)
class CachedDebugState:
    captured_monotonic: float
    state: dict[str, object]


class DebugTarget:
    label: str
    kind: str

    @property
    def cache_key(self) -> str:
        return f"{self.kind}:{self.label}"

    def load_state(self, timeout_seconds: float) -> dict[str, object]:
        raise NotImplementedError


class HttpDebugTarget(DebugTarget):
    kind = "api"

    def __init__(self, *, label: str, url: str) -> None:
        self.label = label
        self.url = url

    def load_state(self, timeout_seconds: float) -> dict[str, object]:
        state = load_debug_state_url(self.url, timeout_seconds=timeout_seconds)
        return {
            "source": self.label,
            "kind": self.kind,
            "ok": True,
            "state": state,
        }


class DockerDebugTarget(DebugTarget):
    kind = "docker"

    def __init__(self, *, container_name: str, url: str) -> None:
        self.container_name = container_name
        self.label = container_name
        self.url = url

    def load_state(self, timeout_seconds: float) -> dict[str, object]:
        script = (
            "import json,sys,urllib.request;"
            f"r=urllib.request.urlopen({self.url!r},timeout={timeout_seconds!r});"
            "sys.stdout.write(r.read().decode('utf-8'))"
        )
        completed = subprocess.run(
            ["docker", "exec", self.container_name, "python", "-c", script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout_seconds + 2.0,
        )
        if completed.returncode != 0:
            error = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(error or f"docker exec failed with {completed.returncode}")
        payload = json.loads(completed.stdout)
        return {
            "source": self.label,
            "kind": self.kind,
            "ok": True,
            "state": unwrap_api_payload(payload),
        }


def load_debug_state_url(url: str, *, timeout_seconds: float) -> dict[str, object]:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (TimeoutError, URLError, OSError) as error:
        raise RuntimeError(str(error)) from error
    return unwrap_api_payload(payload)


def compact_error(error: BaseException) -> str:
    text = str(error).strip()
    if not text:
        return type(error).__name__
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return type(error).__name__
    for line in reversed(lines):
        if "TimeoutError" in line or "RuntimeError" in line or "ConnectionError" in line:
            return line[-240:]
    return lines[-1][-240:]


def unwrap_api_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise RuntimeError("debug response is not a JSON object")
    if payload.get("ok") is True and isinstance(payload.get("data"), dict):
        return payload["data"]
    if payload.get("ok") is False:
        raise RuntimeError(json.dumps(payload.get("error"), ensure_ascii=False))
    return payload


class DebugConsoleServer(ThreadingHTTPServer):
    daemon_threads = True


def build_handler(registry: DebugNodeRegistry, *, refresh_interval_ms: int):
    class DebugConsoleHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path in {"/", "/index.html"}:
                self._write_html(render_page(refresh_interval_ms=refresh_interval_ms))
                return
            if self.path == "/api/nodes":
                self._write_json(registry.collect())
                return
            self.send_error(404, "Not Found")

        def log_message(self, format: str, *args) -> None:
            return

        def _write_json(self, payload: dict[str, object]) -> None:
            body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_html(self, body: str) -> None:
            raw = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    return DebugConsoleHandler


def render_page(*, refresh_interval_ms: int) -> str:
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AnonNet Debug Console</title>
  <style>{html.escape(CSS, quote=False)}</style>
</head>
<body>
  <main class="shell">
    <header class="hero">
      <div>
        <p class="eyebrow">AnonNetCore</p>
        <h1>Debug Console</h1>
        <p class="subtitle">Visao central em tempo real dos nodes locais e containers.</p>
      </div>
      <div class="status-panel">
        <strong id="health">Carregando...</strong>
        <span id="updatedAt">-</span>
      </div>
    </header>
    <section id="summary" class="summary-grid"></section>
    <section id="problems" class="problem-list"></section>
    <section id="nodes" class="node-grid"></section>
  </main>
  <script>const DEBUG_REFRESH_INTERVAL_MS = {refresh_interval_ms};</script>
  <script>{JS}</script>
</body>
</html>"""


CSS = r"""
:root {
  --bg: #eef2e6;
  --ink: #162017;
  --muted: #66705f;
  --panel: #fffdf3;
  --line: #d9dccb;
  --good: #137a4d;
  --warn: #b4690e;
  --bad: #ba2f2f;
  --blue: #1d5f9e;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background:
    radial-gradient(circle at 10% 10%, rgba(29,95,158,.18), transparent 28rem),
    radial-gradient(circle at 90% 0%, rgba(180,105,14,.16), transparent 24rem),
    var(--bg);
  color: var(--ink);
  font-family: Georgia, "Times New Roman", serif;
}
.shell { width: min(1500px, calc(100vw - 32px)); margin: 0 auto; padding: 28px 0; }
.hero {
  display: flex; align-items: stretch; justify-content: space-between; gap: 18px;
  border: 1px solid var(--line); background: rgba(255,253,243,.86);
  border-radius: 28px; padding: 24px; box-shadow: 0 18px 45px rgba(22,32,23,.09);
}
.eyebrow { margin: 0 0 8px; color: var(--blue); font-weight: 700; letter-spacing: .14em; text-transform: uppercase; }
h1 { margin: 0; font-size: clamp(34px, 6vw, 76px); line-height: .9; }
.subtitle { margin: 12px 0 0; color: var(--muted); font-size: 18px; }
.status-panel {
  min-width: 260px; display: grid; place-content: center; gap: 8px;
  border-radius: 20px; padding: 18px; background: #172018; color: #f7f1d6;
}
.status-panel strong { font-size: 28px; }
.status-panel span { color: #cdd4be; }
.summary-grid, .node-grid { display: grid; gap: 16px; margin-top: 18px; }
.summary-grid { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
.node-grid { grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); align-items: start; }
.tile, .node-card, .problem-list article {
  border: 1px solid var(--line); background: rgba(255,253,243,.92);
  border-radius: 22px; padding: 18px; box-shadow: 0 12px 30px rgba(22,32,23,.06);
}
.tile span { color: var(--muted); display: block; font-size: 13px; text-transform: uppercase; letter-spacing: .08em; }
.tile strong { display: block; margin-top: 8px; font-size: 32px; }
.node-card { overflow: visible; }
.node-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }
.node-head h2 { margin: 0; font-size: 24px; word-break: break-word; }
.node-kind { padding: 6px 10px; border-radius: 999px; background: #e8efe4; color: var(--blue); font-weight: 700; }
.bad .node-kind { background: #fee6e6; color: var(--bad); }
.node-meta { color: var(--muted); margin: 6px 0 0; font-size: 14px; word-break: break-all; }
.metrics { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-top: 14px; }
.metric { border: 1px solid var(--line); border-radius: 14px; padding: 10px; background: #fbf7e6; }
.metric span { color: var(--muted); font-size: 12px; display: block; }
.metric strong { font-size: 22px; }
.section { margin-top: 14px; }
.section h3 { margin: 0 0 8px; font-size: 16px; color: var(--blue); }
.list { display: grid; gap: 6px; }
.row {
  display: grid; grid-template-columns: minmax(110px, 180px) minmax(0, 1fr); gap: 8px;
  border: 1px solid #e9e3ca; border-radius: 12px; padding: 8px; background: rgba(255,255,255,.42);
}
.row b { font-size: 12px; color: var(--muted); overflow-wrap: anywhere; }
.row span { word-break: break-all; font-size: 13px; }
.problem-list { display: grid; gap: 10px; margin-top: 18px; }
.problem-list article { border-left: 5px solid var(--warn); }
.problem-list article.bad { border-left-color: var(--bad); }
.empty { color: var(--muted); font-style: italic; }
@media (max-width: 900px) {
  .hero { flex-direction: column; }
  .summary-grid { grid-template-columns: repeat(2, 1fr); }
  .node-grid { grid-template-columns: 1fr; }
  .metrics { grid-template-columns: repeat(2, 1fr); }
  .row { grid-template-columns: 1fr; }
}
"""


JS = r"""
const summaryEl = document.querySelector("#summary");
const nodesEl = document.querySelector("#nodes");
const problemsEl = document.querySelector("#problems");
const healthEl = document.querySelector("#health");
const updatedAtEl = document.querySelector("#updatedAt");

async function refresh() {
  try {
    const response = await fetch("/api/nodes", { cache: "no-store" });
    const payload = await response.json();
    render(payload);
  } catch (error) {
    healthEl.textContent = "Console com erro";
    updatedAtEl.textContent = String(error);
  }
}

function render(payload) {
  const nodes = payload.nodes || [];
  const healthy = nodes.filter((node) => node.ok).length;
  const totals = buildTotals(nodes);
  healthEl.textContent = `${healthy}/${nodes.length} nodes online`;
  updatedAtEl.textContent = `${payload.captured_at} | ${payload.duration_ms}ms`;
  summaryEl.innerHTML = [
    tile("Nodes", nodes.length),
    tile("Online", healthy),
    tile("Peers unicos", totals.uniquePeers),
    tile("Sessoes ativas", `${totals.activeSessions}/${totals.totalSessions}`),
    tile("Rotas ativas", totals.activeRoutes),
    tile("DHT keys unicas", totals.uniqueDhtRecords),
  ].join("");
  problemsEl.innerHTML = renderProblems(nodes);
  nodesEl.innerHTML = nodes.map(renderNode).join("");
}

function buildTotals(nodes) {
  const uniquePeers = new Set();
  const uniqueDhtKeys = new Set();
  const totals = nodes.reduce((acc, node) => {
    const state = node.state || {};
    for (const peer of state.peers?.items || []) {
      if (peer.node_id) {
        uniquePeers.add(peer.node_id);
      }
    }
    for (const key of state.dht?.keys || []) {
      if (key) {
        uniqueDhtKeys.add(key);
      }
    }
    acc.activeSessions += Number(state.sessions?.active || 0);
    acc.totalSessions += Number(state.sessions?.total || 0);
    acc.activeRoutes += Number(state.routes?.active || 0);
    return acc;
  }, { activeSessions: 0, totalSessions: 0, activeRoutes: 0 });
  return {
    ...totals,
    uniquePeers: uniquePeers.size,
    uniqueDhtRecords: uniqueDhtKeys.size,
  };
}

function renderNode(node) {
  if (!node.ok) {
    return `<article class="node-card bad">
      <div class="node-head"><h2>${escapeHtml(node.source)}</h2><span class="node-kind">${escapeHtml(node.kind)}</span></div>
      <p class="node-meta">${escapeHtml(node.error || "offline")}</p>
    </article>`;
  }
  const state = node.state || {};
  const info = state.node || {};
  const sessions = state.sessions || {};
  const routes = state.routes || {};
  const dht = state.dht || {};
  const peers = state.peers?.diagnostics || {};
  const dhtRows = Object.values(dht.counts_by_namespace || {}).reduce((a, b) => a + Number(b || 0), 0);
  const dhtUnique = Object.values(dht.unique_counts_by_namespace || {}).reduce((a, b) => a + Number(b || 0), 0);
  return `<article class="node-card">
    <div class="node-head">
      <div>
        <h2>${escapeHtml(info.name || node.source)}</h2>
        <p class="node-meta">${escapeHtml(info.physical_node_id || "-")}</p>
        ${node.cached ? `<p class="node-meta">cache: ${escapeHtml(node.cache_age_seconds)}s${node.stale ? " | stale" : ""}${node.warning ? ` | ${escapeHtml(node.warning)}` : ""}</p>` : ""}
      </div>
      <span class="node-kind">${escapeHtml(node.kind)}</span>
    </div>
    <div class="metrics">
      ${metric("Porta", info.listen_port ?? "-")}
      ${metric("Peers", peers.route_ready_nodes ?? 0)}
      ${metric("Sessoes", `${sessions.active ?? 0}/${sessions.total ?? 0}`)}
      ${metric("Rotas", routes.active ?? 0)}
      ${metric("DHT keys", `${dhtUnique}/${dhtRows}`)}
      ${metric("VNs", (state.virtual_nodes?.local || []).length)}
      ${metric("Downloads", (state.content?.downloads || []).length)}
      ${metric("Problemas", (state.problems || []).length)}
    </div>
    ${renderRuntimeSection(state.runtimes || {})}
    ${renderList("DHT por namespace", Object.entries(dht.unique_counts_by_namespace || {}).map(([k,v]) => [k, `${v} keys | ${dht.counts_by_namespace?.[k] ?? 0} rows`]))}
    ${renderRouteSection(routes.items || [])}
    ${renderSessionSection(sessions.items || [])}
  </article>`;
}

function renderProblems(nodes) {
  const items = [];
  for (const node of nodes) {
    if (!node.ok) {
      items.push(`<article class="bad"><strong>${escapeHtml(node.source)}</strong><p>${escapeHtml(node.error || "offline")}</p></article>`);
      continue;
    }
    for (const problem of node.state?.problems || []) {
      items.push(`<article><strong>${escapeHtml(node.source)} | ${escapeHtml(problem.area)}</strong><p>${escapeHtml(problem.message)}</p></article>`);
    }
  }
  return items.length ? items.join("") : `<article><strong>Sem problemas destacados</strong><p class="empty">Nenhum alerta no snapshot atual.</p></article>`;
}

function renderRuntimeSection(runtimes) {
  const rows = Object.entries(runtimes).map(([name, runtime]) => [
    name,
    runtime.running ? `running | ${runtime.interval_seconds}s` : "stopped"
  ]);
  return renderList("Runtimes", rows);
}

function renderRouteSection(routes) {
  return renderList("Rotas", routes.map((route) => [
    route.local_role,
    `${route.status} | ${route.final_path_id || route.initial_path_id || route.route_path_id || "-"}`
  ]));
}

function renderSessionSection(sessions) {
  return renderList("Sessoes", sessions.map((session) => [
    session.scope,
    `${session.state} | ${session.remote_identity_id || "-"}`
  ]));
}

function renderList(title, rows) {
  if (!rows.length) {
    return `<div class="section"><h3>${escapeHtml(title)}</h3><p class="empty">Vazio</p></div>`;
  }
  return `<div class="section"><h3>${escapeHtml(title)}</h3><div class="list">${
    rows.map(([left, right]) => `<div class="row"><b>${escapeHtml(left)}</b><span>${escapeHtml(right)}</span></div>`).join("")
  }</div></div>`;
}

function tile(label, value) {
  return `<article class="tile"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`;
}

function metric(label, value) {
  return `<div class="metric"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

refresh();
setInterval(refresh, DEBUG_REFRESH_INTERVAL_MS);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dashboard local para depurar nodes AnonNetCore.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19888)
    parser.add_argument(
        "--api",
        action="append",
        default=[],
        help="URL /debug/state de um core local. Pode repetir.",
    )
    parser.add_argument("--no-docker", action="store_true", help="Nao descobrir containers anonnet-node-*.")
    parser.add_argument("--docker-filter", default="anonnet-node-")
    parser.add_argument("--node-timeout-seconds", type=float, default=8.0)
    parser.add_argument("--node-cache-ttl-seconds", type=float, default=8.0)
    parser.add_argument("--refresh-interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-workers", type=int, default=4)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
