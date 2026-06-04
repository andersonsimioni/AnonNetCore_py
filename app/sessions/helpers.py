from __future__ import annotations

from common import load_json_object


def load_session_metadata(session) -> dict[str, object]:
    return load_json_object(session.metadata_json)


def is_observed_only_physical_endpoint(session) -> bool:
    metadata = load_session_metadata(session)
    return metadata.get("physical_endpoint_source") == "observed"


def is_observed_only_physical_session(session) -> bool:
    if session.session_scope != "physical":
        return False
    return is_observed_only_physical_endpoint(session)


def build_remote_endpoint_from_session(session):
    from transport import TransportEndpoint

    if not session.transport or not session.remote_host or session.remote_port is None:
        raise ValueError("The physical session has no associated remote endpoint.")

    return TransportEndpoint(
        transport_name=session.transport,
        host=session.remote_host,
        port=session.remote_port,
        metadata=load_session_metadata(session),
    )
