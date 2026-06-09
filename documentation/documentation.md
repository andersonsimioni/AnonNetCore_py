# AnonNetCore Technical Overview

AnonNetCore is a Python MVP for a decentralized peer-to-peer network split into
two layers:

- a physical layer made of real processes connected by transport adapters;
- a virtual layer made of virtual nodes, virtual sessions, and application
  payloads.

The MVP validates bootstrap, physical-node discovery, DHT publication/query,
route construction, route execution, virtual sessions, reliable message
delivery, virtual content transfer, a local API, a debug console, and a social
PoC.

## Architecture

Main packages:

- `app/core`: engine, configuration, protocol routing, runtimes, and strategy
  registry.
- `app/transport`: TCP, UDP, relay, and shared transport models.
- `app/identity`: physical and virtual identity management.
- `app/dht`: distributed records, key generation, proof-of-work, merge rules,
  and storage helpers.
- `app/route`: route-resolution management and DRT publication helpers.
- `app/sessions`: physical and virtual session state, keepalive, and reliable
  delivery metadata.
- `app/content`: local content storage and virtual content downloads.
- `app/api`: HTTP and WebSocket API for external applications.
- `app/debug`: local Debug Console server.
- `poc`: local HTML/JS social PoC.
- `tests`: integration smokes and shared test helpers.
- `cluster`: Docker cluster generation and lifecycle scripts.

## Runtime Flow

1. Load `CoreConfig`.
2. Initialize logging, storage, identity, DHT, sessions, routes, content, and
   transport services.
3. Start physical transport listeners according to node reachability and enabled
   adapters.
4. Start API and WebSocket listeners when enabled.
5. Start bootstrap and peer-discovery runtimes.
6. Start DHT, relay, physical-session, virtual-session, and virtual-route
   maintenance.
7. Accept protocol envelopes and dispatch handlers by message type.

## Physical Layer

Physical nodes represent real processes. They own physical identities, advertise
stable listener or relay endpoints, exchange physical-node information, validate
peers, start physical sessions, and publish DPNT records.

The physical layer can use:

- direct TCP;
- UDP with simple fragmentation/reassembly and keepalive;
- `relay_tcp`, where a private node advertises a public relay endpoint and
  receives packets through `PHYSICAL_RELAY_DATA`.

## Virtual Layer

Virtual nodes are logical identities. They request routes, publish DRT entries
through final physical nodes, establish virtual sessions, exchange application
messages, and transfer content. A virtual packet is carried inside physical
`ROUTE_DATA`.

## DHT

The DHT uses namespaces for physical-node discovery, route discovery, content
discovery, mutable pointers, and future structured data. Publication targets the
K closest nodes by XOR distance and includes a semantic proof-of-work nonce on
the payload or payload fragment being published.

## Routes

Route build is strategy-based. The main strategy is `random_walk_ttl`, but the
architecture allows other strategies such as max-hop, latency-aware,
bandwidth-aware, onion-like, relay-preferred, or policy-driven routing.

Route execute is strategy-agnostic. It maps a path id, checks direction, and
either forwards the physical envelope or delivers the embedded virtual envelope
locally.

## API and PoC

The local API exposes enough functionality for external apps to create virtual
nodes, publish/query DHT records, start virtual sessions, send messages, and
download content. The social PoC uses those APIs to demonstrate profiles, friend
feeds, and direct messages.

## Validation

The smoke suite validates core flow and social PoC flow using Docker cluster
nodes and local cores:

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py 10
```

The runner prints the validation steps, captures logs, reports node
warnings/errors through a local collector, summarizes the randomized topology,
and stops the cluster at the end.

## Limits

The MVP is not production-ready. Abuse protection, global adversary resistance,
large-scale DHT behavior, mature relay/NAT traversal, formal cryptographic
audits, and production-grade UI/application hardening remain future work.
