from __future__ import annotations

import argparse
import asyncio
import base64
import json
from pathlib import Path
import sys
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = PROJECT_ROOT / "app"
INTEGRATION_ROOT = PROJECT_ROOT / "tests" / "integration"
for path in (APP_ROOT, INTEGRATION_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

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
    wait_for_drt_entry,
    wait_for_network_ready,
    wait_for_runtime_route_active,
    wait_for_virtual_session_active,
    wait_until_value,
)


TEST_DATA_ROOT = PROJECT_ROOT / "data" / "local" / "integration" / "poc-social-smoke"
TEST_LOG_ROOT = TEST_DATA_ROOT / "logs"
CORE_A_PORT = 19401
CORE_B_PORT = 19402
DEFAULT_CLUSTER_NODES = 5
SOCIAL_APP_ID = "anonnet.social"
SOCIAL_DIRECT_MESSAGE_TYPE = "social.direct_message"
SOCIAL_PROFILE_CONTENT_TYPE = "application/anonnet-social-user-state+json"
SOCIAL_PROFILE_DPT_TITLE = "profile"


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

        vn_a = core_a.services.api_service.create_local_virtual_node(
            kind="social",
            metadata={"app": "anonnet-poc", "role": "profile", "owner": "A"},
        )
        vn_b = core_b.services.api_service.create_local_virtual_node(
            kind="social",
            metadata={"app": "anonnet-poc", "role": "profile", "owner": "B"},
        )
        print(f"checkpoint 2 OK: social VNs created: vn_a={vn_a['id']} vn_b={vn_b['id']}")

        user_state_a = build_user_state(
            virtual_node=vn_a,
            display_name="Anderson A",
            bio="Perfil A do smoke integrado da PoC",
            friend_virtual_node_ids=[str(vn_b["id"])],
            feed_posts=[
                build_feed_post(
                    author_virtual_node_id=str(vn_a["id"]),
                    author_name="Anderson A",
                    text="primeira publicacao integrada",
                )
            ],
        )
        content_a_v1 = core_a.services.api_service.store_content(
            data_base64=encode_json_base64(user_state_a),
            title=SOCIAL_PROFILE_DPT_TITLE,
            content_type=SOCIAL_PROFILE_CONTENT_TYPE,
            tags=["social", "profile", "user-state"],
        )
        loaded_user_state_a_v1 = read_json_content(core_a, str(content_a_v1["content_id"]))
        assert_equal(loaded_user_state_a_v1["profile"]["display_name"], "Anderson A")
        assert_equal(len(loaded_user_state_a_v1["feed_posts"]), 1)
        print(f"checkpoint 3 OK: user state A saved: content_id={content_a_v1['content_id']}")

        user_state_a_v2 = {
            **loaded_user_state_a_v1,
            "feed_posts": [
                build_feed_post(
                    author_virtual_node_id=str(vn_a["id"]),
                    author_name="Anderson A",
                    text="segunda publicacao integrada",
                ),
                *loaded_user_state_a_v1["feed_posts"],
            ],
        }
        content_a_v2 = core_a.services.api_service.store_content(
            data_base64=encode_json_base64(user_state_a_v2),
            title=SOCIAL_PROFILE_DPT_TITLE,
            content_type=SOCIAL_PROFILE_CONTENT_TYPE,
            tags=["social", "profile", "user-state"],
        )
        loaded_user_state_a_v2 = read_json_content(core_a, str(content_a_v2["content_id"]))
        assert_equal(len(loaded_user_state_a_v2["feed_posts"]), 2)
        print(f"checkpoint 4 OK: user state A updated with feed post: content_id={content_a_v2['content_id']}")

        user_state_b = build_user_state(
            virtual_node=vn_b,
            display_name="Anderson B",
            bio="Perfil B do smoke integrado da PoC",
            friend_virtual_node_ids=[str(vn_a["id"])],
            feed_posts=[],
        )
        content_b = core_b.services.api_service.store_content(
            data_base64=encode_json_base64(user_state_b),
            title=SOCIAL_PROFILE_DPT_TITLE,
            content_type=SOCIAL_PROFILE_CONTENT_TYPE,
            tags=["social", "profile", "user-state"],
        )
        assert_equal(read_json_content(core_b, str(content_b["content_id"]))["profile"]["display_name"], "Anderson B")
        print(f"checkpoint 5 OK: user state B saved: content_id={content_b['content_id']}")

        await wait_for_network_ready(core_a, minimum_remote_nodes=required_ready_nodes)
        await wait_for_network_ready(core_b, minimum_remote_nodes=required_ready_nodes)
        print(f"checkpoint 6 OK: network ready: required_ready_nodes={required_ready_nodes}")

        await wait_for_cluster_network_maturity(
            core_a,
            core_b,
            required_ready_nodes=required_ready_nodes,
        )
        print("checkpoint 7 OK: cluster network maturity reached")

        active_route = await wait_for_runtime_route_active(
            core_a,
            local_virtual_node_id=str(vn_a["id"]),
        )
        print(
            "checkpoint 8 OK: VN A route active from runtime: "
            f"initial_path_id={active_route.initial_path_id} final_path_id={active_route.final_path_id}"
        )

        await wait_for_drt_entry(core_b, virtual_node_public_key=str(vn_a["public_key"]))
        print("checkpoint 9 OK: VN A route discovered through DRT from core B")

        core_b.services.api_service.upsert_remote_virtual_node(
            node_id=str(vn_a["id"]),
            public_key=str(vn_a["public_key"]),
            kind=str(vn_a["kind"]),
            status="active",
            metadata={"source": "poc_social_smoke_friend_identity"},
        )
        print("checkpoint 10 OK: core B registered VN A as remote friend identity")

        core_a.services.api_service.subscribe_virtual_messages(
            app_message_type=SOCIAL_DIRECT_MESSAGE_TYPE,
        )
        session_id = await get_or_create_social_session(
            core_b,
            local_virtual_node_id=str(vn_b["id"]),
            remote_virtual_node_id=str(vn_a["id"]),
            sessions_by_remote_vn={},
        )
        await wait_for_virtual_session_active(core_b, session_id)
        print(f"checkpoint 11 OK: social virtual session active: session_id={session_id}")

        message_payload = build_direct_message(
            from_virtual_node_id=str(vn_b["id"]),
            to_virtual_node_id=str(vn_a["id"]),
            text="mensagem direta integrada da PoC",
        )
        await core_b.services.api_service.send_virtual_message(
            session_id=session_id,
            app_message_type=SOCIAL_DIRECT_MESSAGE_TYPE,
            payload=message_payload,
        )
        received_message = await wait_for_social_message(
            core_a,
            expected_text="mensagem direta integrada da PoC",
        )
        assert_equal(received_message["payload"]["from_virtual_node_id"], str(vn_b["id"]))
        assert_equal(received_message["payload"]["to_virtual_node_id"], str(vn_a["id"]))
        print("checkpoint 12 OK: direct message delivered through real virtual session")

        reused_session_id = await get_or_create_social_session(
            core_b,
            local_virtual_node_id=str(vn_b["id"]),
            remote_virtual_node_id=str(vn_a["id"]),
            sessions_by_remote_vn={str(vn_a["id"]): session_id},
        )
        assert_equal(reused_session_id, session_id)
        print("checkpoint 13 OK: social session map reuses VN -> session_id")
        print("OK poc social integration smoke passed")
    finally:
        await stop_cores(core_b, core_a)


def build_user_state(
    *,
    virtual_node: dict[str, object],
    display_name: str,
    bio: str,
    friend_virtual_node_ids: list[str],
    feed_posts: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "schema": "anonnet.social.user_state.v1",
        "app_id": SOCIAL_APP_ID,
        "profile": {
            "schema": "anonnet.social.profile.v1",
            "app_id": SOCIAL_APP_ID,
            "virtual_node_id": virtual_node["id"],
            "public_key": virtual_node["public_key"],
            "display_name": display_name,
            "bio": bio,
            "photo_content_id": None,
            "friend_virtual_node_ids": friend_virtual_node_ids,
            "friend_public_keys": [],
            "updated_at": "2026-05-13T00:00:00+00:00",
        },
        "feed_posts": feed_posts,
        "updated_at": "2026-05-13T00:00:00+00:00",
    }


def build_feed_post(
    *,
    author_virtual_node_id: str,
    author_name: str,
    text: str,
) -> dict[str, object]:
    return {
        "schema": "anonnet.social.feed_post.v1",
        "app_id": SOCIAL_APP_ID,
        "post_id": str(uuid4()),
        "author_virtual_node_id": author_virtual_node_id,
        "author_name": author_name,
        "text": text,
        "created_at": "2026-05-13T00:00:00+00:00",
    }


def build_direct_message(
    *,
    from_virtual_node_id: str,
    to_virtual_node_id: str,
    text: str,
) -> dict[str, object]:
    return {
        "schema": "anonnet.social.direct_message.v1",
        "from_virtual_node_id": from_virtual_node_id,
        "to_virtual_node_id": to_virtual_node_id,
        "text": text,
        "sent_at": "2026-05-13T00:00:00+00:00",
    }


async def get_or_create_social_session(
    engine,
    *,
    local_virtual_node_id: str,
    remote_virtual_node_id: str,
    sessions_by_remote_vn: dict[str, str],
) -> str:
    existing_session_id = sessions_by_remote_vn.get(remote_virtual_node_id)
    if existing_session_id:
        return existing_session_id

    session = await engine.services.api_service.start_virtual_session(
        local_virtual_node_id=local_virtual_node_id,
        remote_virtual_node_id=remote_virtual_node_id,
    )
    session_id = str(session["session_id"])
    sessions_by_remote_vn[remote_virtual_node_id] = session_id
    return session_id


async def wait_for_social_message(
    engine,
    *,
    expected_text: str,
    timeout_seconds: float = 25.0,
) -> dict[str, object]:
    async def load_message():
        messages = engine.services.api_service.read_virtual_messages(
            app_message_type=SOCIAL_DIRECT_MESSAGE_TYPE,
            limit=20,
            consume=False,
        )
        for message in messages:
            payload = message.get("payload")
            if isinstance(payload, dict) and payload.get("text") == expected_text:
                return message
        return None

    return await wait_until_value(
        load_message,
        timeout_seconds=timeout_seconds,
        label="PoC social direct message delivery",
    )


def read_json_content(engine, content_id: str) -> dict[str, object]:
    info = engine.services.api_service.get_content_info(content_id=content_id)
    content_range = engine.services.api_service.read_content_range(
        content_id=content_id,
        start_byte=0,
        end_byte=int(info["size_bytes"]),
    )
    raw_json = base64.b64decode(str(content_range["data_base64"]).encode("ascii")).decode("utf-8")
    return json.loads(raw_json)


def encode_json_base64(value: dict[str, object]) -> str:
    return base64.b64encode(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")


def assert_equal(actual, expected) -> None:
    if actual != expected:
        raise AssertionError(f"Expected {expected!r}, got {actual!r}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PoC social integration smoke: valida perfil, feed e DM sobre core real.",
    )
    parser.add_argument("--cluster-nodes", type=int, default=DEFAULT_CLUSTER_NODES)
    parser.add_argument("--minimum-remote-nodes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"poc social integration smoke failed: {error}", file=sys.stderr)
        raise
