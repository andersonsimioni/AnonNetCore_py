# AnonNetCore Technical Overview

AnonNetCore is a Python MVP for a decentralized peer-to-peer network split into
two layers:

- a physical layer made of real processes connected by transport adapters;
- a virtual layer made of virtual nodes, virtual sessions, and application
  payloads.

The MVP validates bootstrap, physical-node discovery, DHT publication/query,
route construction, route execution, virtual sessions, virtual messages, virtual
content transfer, a local API, and a social PoC.

## Architecture

Main packages:

- `app/core`: engine, configuration, protocol routing, runtimes, and strategy
  registry.
- `app/transport`: transport adapters and shared transport models.
- `app/identity`: physical and virtual identity management.
- `app/dht`: distributed records, key generation, merge rules, and storage
  helpers.
- `app/route`: route-resolution management.
- `app/sessions`: physical and virtual session state, keepalive, and reliable
  delivery metadata.
- `app/content`: local content storage and virtual content downloads.
- `app/api`: HTTP and WebSocket API for external applications.
- `poc`: local HTML/JS social PoC.
- `tests`: integration smokes and shared test helpers.
- `cluster`: Docker cluster generation and lifecycle scripts.

## Runtime Flow

1. Load `CoreConfig`.
2. Initialize logging, storage, identity, DHT, sessions, routes, content, and
   transport services.
3. Start physical transport listeners when the node is public.
4. Start API and WebSocket listeners when enabled.
5. Start bootstrap and peer-discovery runtimes.
6. Start DHT, physical-session, virtual-session, and virtual-route maintenance.
7. Accept protocol envelopes and dispatch handlers by message type.

## Physical Layer

Physical nodes represent real processes. They own physical identities, listen on
advertised endpoints, exchange physical-node information, validate peers, start
physical sessions, and publish DPNT records.

## Virtual Layer

Virtual nodes are logical identities. They request routes, publish DRT entries
through final physical nodes, establish virtual sessions, exchange application
messages, and transfer content.

## DHT

The DHT uses namespaces for physical-node discovery, route discovery, content
discovery, and mutable pointers. Publication targets the K closest nodes by XOR
distance and includes a simple proof-of-work cost.

## Routes

Route build is strategy-based. The main strategy is `random_walk_ttl`, but the
architecture allows other strategies such as max-hop, latency-aware,
bandwidth-aware, onion-like, or relay-preferred routing.

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

## Limits

The MVP is not production-ready. NAT traversal, abuse protection, global
adversary resistance, large-scale DHT behavior, formal cryptographic audits, and
production-grade UI/application hardening remain future work.
