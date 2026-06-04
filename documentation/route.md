# Routes and Route Execute

Routes let a virtual node be reached through a sequence of physical nodes. The
physical layer forwards packets by `path_id`; the virtual layer lives inside the
physical `ROUTE_DATA` payload.

## Goals

- separate physical identity from virtual identity;
- allow different route-building strategies;
- publish reachable virtual-node entry points in DRT;
- keep route execution simple and hop-by-hop;
- keep application semantics out of the physical route executor.

## Route Build Messages

```text
ROUTE_CREATE
ROUTE_CREATE_KEM_INFO
ROUTE_CREATE_VALIDATE_AND_PUBLISH
ROUTE_CREATE_PING
ROUTE_CREATE_PONG
ROUTE_CREATE_OK
```

The current functional strategy is `random_walk_ttl`. The route builder forwards
`ROUTE_CREATE` hop-by-hop until the strategy decides to stop at a final physical
node. The final physical node returns KEM information, validates the VN request,
measures RTT with ping/pong, publishes the DRT entry, and sends `ROUTE_CREATE_OK`.

## DRT Publication

The final physical node publishes a DRT route entry that includes:

- virtual-node public key;
- final physical-node public key;
- final path id;
- measured RTT;
- expiration time;
- virtual-node signature accepting the final path id and final physical node;
- final physical-node signatures accepting the route and RTT.

Those signatures prove that both the VN and the final PN agreed on that route.

## Route Execute

Route execute is intentionally small. It receives `ROUTE_DATA`, maps the path id,
checks direction, and either delivers locally or forwards to the next hop.

Physical payload shape:

```json
{
  "path_id": "...",
  "direction": "vn_to_pn",
  "virtual_session_id": "...",
  "virtual_envelope_ciphered": true,
  "payload": {
    "header": {
      "message_type": "VIRTUAL_SESSION_DATA"
    },
    "payload": {}
  }
}
```

If the route ends locally and a virtual envelope exists, the handler asks the
virtual session layer to decrypt it when needed and then dispatches the restored
virtual envelope through the normal core protocol flow.

## Strategy Extension Points

The routing layer is strategy-based. Besides `random_walk_ttl`, possible
strategies include maximum hop count, minimum bandwidth, latency-aware routing,
onion-like route composition, relay-preferred routes, or policy-driven routing.

The route executor does not need to know which strategy created the route.
