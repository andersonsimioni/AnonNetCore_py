# Transport

`app/transport` implements physical transport adapters used by the MVP.

Each adapter exposes the same transport interface so protocol clients can send
physical envelopes without knowing whether the path is direct TCP, UDP, or
relay.

## TCP

TCP is the primary direct transport. It uses stable listener endpoints and
length-prefixed frames.

## UDP

UDP is available as an MVP direct transport. It keeps lightweight channel state
with keepalive and uses simple fragmentation/reassembly controlled by
`CoreConfig.udp_fragment_payload_size` and
`CoreConfig.udp_fragment_reassembly_timeout_seconds`.

## Relay

`relay_tcp` lets a private node advertise a public relay-capable physical node
as its reachable endpoint. The relay forwards `PHYSICAL_RELAY_DATA` to the
target private node when both sides have an active physical session with the
relay.

The advertised endpoint shared with peers must always be stable: a listener
endpoint for public nodes or a relay endpoint for private nodes. It must never be
an ephemeral port observed from an inbound TCP connection.
