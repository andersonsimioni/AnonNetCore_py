# Entities and Persistence

The database stores durable state. Live sockets, active session secrets, and
in-flight handshakes remain in memory.

## Identity Rule

Node identifiers are SHA-512 hashes of their public keys. Distributed references
prefer ids instead of full public keys to keep records smaller.

## Physical Node Identity

Represents the local physical process:

- `id`: SHA-512 of the physical public key;
- `public_key`: physical public key;
- `private_key`: local private key material for the MVP;
- `key_algorithm`: identity algorithm;
- `created_at` and `updated_at`.

## Virtual Node Identity

Represents an application-level identity. In the social PoC:

```text
1 virtual node = 1 user profile
```

The virtual-node id is the SHA-512 hash of the virtual public key.

## Remote Physical Nodes

Remote physical-node records store public key, protocol version, advertised
endpoints, validation state, reachability, relay capability, and timestamps. The
advertised endpoint must be the listener endpoint, not an ephemeral inbound TCP
port.

## Distributed Records

Important DHT records:

- DPNT: physical-node information and advertised endpoints.
- DRT: route entries for a virtual node.
- DDT: content holders.
- DPT: signed mutable pointer owned by a virtual node.
- DTT: structured table model reserved for future work.

## Route Resolution

`RouteResolution` stores everything the local node needs to resolve a route in
any role:

- local and remote path ids;
- previous and next physical nodes;
- direction mapping;
- strategy name and route status;
- virtual-node id;
- signatures and KEM material when relevant.

`RouteService` centralizes creation, lookup, and updates for those resolutions.

## Sessions

Physical and virtual sessions share high-level session state: identities,
handshake state, keepalive data, reliable delivery metadata, and associated
route information when present.

The database must not store live sockets, active crypto objects, or transient
session secrets beyond what the MVP explicitly needs.

## Local Content

Local content metadata stores content id, SHA-512 hash, size, MIME type, path,
tags, and ownership information. A completed download can also publish in DDT
that the downloader now holds the content.
