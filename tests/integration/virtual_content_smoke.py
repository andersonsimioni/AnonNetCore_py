from __future__ import annotations

import argparse
import asyncio
from hashlib import sha512
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from storage.models import ContentAdvertisement, ContentObject

from core_helpers import reset_core_data_dir, stop_cores
from smoke_helpers import (
    MIN_CLUSTER_NODES,
    create_local_virtual_node,
    create_route_for_virtual_node,
    create_test_core,
    reset_cluster,
    resolve_cluster_node_count,
    resolve_required_ready_nodes,
    start_cluster,
    wait_for_cluster_containers,
    wait_for_cluster_network_maturity,
    wait_for_network_ready,
    wait_for_route_active,
    wait_for_virtual_session_active,
    wait_until_value,
)


TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "virtual-content-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
CORE_A_PORT = 19401
CORE_B_PORT = 19402


async def main() -> None:
    args = parse_args()
    cluster_nodes = resolve_cluster_node_count(args.cluster_nodes)
    required_ready_nodes = resolve_required_ready_nodes(
        cluster_nodes=cluster_nodes,
        minimum_remote_nodes=args.minimum_remote_nodes,
    )

    reset_core_data_dir(TEST_DATA_ROOT)
    print(f"reset test data: {TEST_DATA_ROOT}")
    reset_cluster()
    start_cluster(node_count=cluster_nodes)
    wait_for_cluster_containers(expected_count=cluster_nodes)

    core_a = create_test_core(
        data_dir=TEST_DATA_ROOT / "core-a",
        listen_port=CORE_A_PORT,
        log_dir=TEST_LOG_ROOT / "core-a",
    )
    core_b = create_test_core(
        data_dir=TEST_DATA_ROOT / "core-b",
        listen_port=CORE_B_PORT,
        log_dir=TEST_LOG_ROOT / "core-b",
    )

    try:
        await asyncio.gather(core_a.start(), core_b.start())
        print("checkpoint 1 OK: cores A/B started")

        vn_a = create_local_virtual_node(
            core_a,
            kind="virtual-content-vn-a",
            metadata_source="virtual_content_smoke",
        )
        vn_b = create_local_virtual_node(
            core_b,
            kind="virtual-content-vn-b",
            metadata_source="virtual_content_smoke",
        )
        print(f"checkpoint 2 OK: virtual nodes created: vn_a={vn_a.id} vn_b={vn_b.id}")

        content_bytes = build_test_content()
        content_hash = register_local_content(core_a, content_bytes)
        print(f"checkpoint 3 OK: provider content registered: content_hash={content_hash}")

        await wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes)
        await wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes)
        print(f"checkpoint 4 OK: network ready: required_ready_nodes={required_ready_nodes}")

        await wait_for_cluster_network_maturity(
            core_a,
            core_b,
            required_ready_nodes=required_ready_nodes,
        )
        print("checkpoint 5 OK: cluster network maturity reached")

        route_result = await create_route_for_virtual_node(core_a)
        initial_path_id = str(route_result["initial_path_id"])
        active_route = await wait_for_route_active(core_a, initial_path_id)
        print(
            "checkpoint 6 OK: route active: "
            f"initial_path_id={initial_path_id} final_path_id={active_route.final_path_id}"
        )

        core_b.services.identity_service.upsert_remote_virtual_node(
            node_id=vn_a.id,
            public_key=vn_a.public_key,
            kind=vn_a.kind,
            status="active",
            metadata_json='{"source":"virtual_content_smoke_identity_exchange"}',
        )
        print("checkpoint 7 OK: core B learned VN A identity")

        session_id = await core_b.services.protocol_clients.virtual.session.start_session_to_virtual_node(
            local_virtual_node_id=vn_b.id,
            remote_virtual_node_id=vn_a.id,
        )
        await wait_for_virtual_session_active(core_b, session_id)
        print(f"checkpoint 8 OK: virtual session active: session_id={session_id}")

        downloaded_bytes = await run_virtual_content_protocol_smoke(
            provider_engine=core_a,
            downloader_engine=core_b,
            session_id=session_id,
            content_bytes=content_bytes,
            content_hash=content_hash,
        )
        print(f"checkpoint 10 OK: virtual content downloaded: size_bytes={len(downloaded_bytes)}")
        print("OK virtual content smoke passed")
    finally:
        await stop_cores(core_b, core_a)


async def run_virtual_content_protocol_smoke(
    *,
    provider_engine,
    downloader_engine,
    session_id: str,
    content_bytes: bytes | None = None,
    content_hash: str | None = None,
) -> bytes:
    resolved_content_bytes = content_bytes or build_test_content()
    resolved_content_hash = content_hash or register_local_content(
        provider_engine,
        resolved_content_bytes,
    )

    await downloader_engine.services.protocol_clients.virtual.session.send_protocol_message(
        session_id=session_id,
        message_type="VIRTUAL_CONTENT_INFO_REQUEST",
        payload={
            "content_id": resolved_content_hash,
        },
    )

    downloaded_content = await wait_for_downloaded_content(
        downloader_engine,
        content_hash=resolved_content_hash,
    )
    await wait_for_ddt_provider_advertisement(
        downloader_engine,
        content_hash=resolved_content_hash,
    )
    downloaded_bytes = Path(downloaded_content.storage_path).read_bytes()
    if downloaded_bytes != resolved_content_bytes:
        raise RuntimeError("Downloaded content bytes mismatch.")
    return downloaded_bytes


def build_test_content() -> bytes:
    line = b"AnonNetCore virtual content smoke payload.\n"
    return line * 4096


def register_local_content(engine, content_bytes: bytes) -> str:
    content_hash = sha512(content_bytes).hexdigest()
    storage_dir = Path(engine.services.config.content_storage_dir)
    storage_dir.mkdir(parents=True, exist_ok=True)
    storage_path = storage_dir / content_hash
    storage_path.write_bytes(content_bytes)

    with engine.services.database.session_scope() as session:
        session.add(
            ContentObject(
                content_hash=content_hash,
                title="virtual-content-smoke",
                content_type="application/octet-stream",
                mime_type="application/octet-stream",
                size_bytes=len(content_bytes),
                storage_path=str(storage_path),
                is_encrypted=False,
                encryption_scheme=None,
                is_deleted=False,
            )
        )

    return content_hash


async def wait_for_downloaded_content(engine, *, content_hash: str):
    async def load_content():
        return engine.services.content_transfer_service.get_content_info(content_hash)

    content_info = await wait_until_value(
        load_content,
        timeout_seconds=30.0,
        label="virtual content download completed",
    )
    if content_info.storage_path is None:
        raise RuntimeError("Downloaded content has no storage path.")
    return content_info


async def wait_for_ddt_provider_advertisement(engine, *, content_hash: str):
    async def load_advertisement():
        with engine.services.database.session_scope() as session:
            content_object = (
                session.query(ContentObject)
                .filter(ContentObject.content_hash == content_hash)
                .first()
            )
            if content_object is None:
                return None

            return (
                session.query(ContentAdvertisement)
                .filter(ContentAdvertisement.content_object_id == content_object.id)
                .filter(ContentAdvertisement.published_in_ddt.is_(True))
                .filter(ContentAdvertisement.is_active.is_(True))
                .first()
            )

    return await wait_until_value(
        load_advertisement,
        timeout_seconds=20.0,
        label="downloaded content ddt provider advertisement",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test: VirtualContentProtocolHandler baixa conteudo por byte ranges.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=MIN_CLUSTER_NODES)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"virtual content smoke failed: {error}", file=sys.stderr)
        raise
