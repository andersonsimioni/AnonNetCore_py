# DHT

The DHT stores distributed records used for physical-node discovery, virtual-node
routes, content location, and mutable application pointers.

## Key Model

Every namespace uses a deterministic physical key:

```text
sha512("<namespace>|<logical_key>")
```

The logical key is usually already a SHA-512 identifier to keep distributed keys
compact and stable.

## Namespaces

- `dpnt`: physical-node information, endpoints, reachability, and capabilities.
- `drt`: route entries for a virtual node.
- `ddt`: content holders for a content identifier.
- `dpt`: signed mutable pointer owned by a virtual node.
- `dtt`: table model/parser support for future structured data.

## Publication Flow

Publishing uses the physical DHT client. The client finds the K closest known
physical nodes for the key and only returns `stored` when the required replicas
accepted the record. Each request tracks `stored_by` to avoid redundant writes.

The MVP also applies a simple proof-of-work cost before DHT publication. The
same `CoreConfig.proof_of_work_difficulty_bits` value is used across route and
DHT publication paths.

## Query Flow

Queries are forwarded hop-by-hop through DHT handlers instead of requiring the
originating node to chase every closer peer itself. The request keeps the origin
anonymous at the protocol level: no origin node, visited list, or hop counter is
included in the payload.

## Merge Rules

Merge behavior depends on namespace:

- DRT merges route entries for the same virtual node.
- DDT merges holders for the same content.
- DPT keeps the latest valid signed pointer.
- DPNT keeps the valid physical-node record for the derived node id.

## Maintenance

DHT maintenance republishes validated local records and transfers responsibility
when the K closest nodes around a key change. This keeps records available as the
local network view evolves.

## Limits

- The MVP does not include a mature reputation system.
- Coordinated malicious peers are not fully mitigated.
- Large-list sharding is modeled but not production-hardened.
