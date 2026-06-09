# DHT

The DHT stores distributed records used for physical-node discovery, virtual-node
routes, content location, mutable application pointers, and future structured
tables.

## Key Model

Every namespace uses a deterministic physical key:

```text
sha512("<namespace>|<logical_key>")
```

The logical key should be compact and stable. In most flows it is either a
SHA-512 id or a short title derived from an id.

## Namespaces

- `dpnt`: physical-node information, endpoints, reachability, and capabilities.
- `drt`: route entries for a virtual node.
- `ddt`: content holders for a content identifier.
- `dpt`: signed mutable pointer owned by a virtual node.
- `dtt`: table model/parser support for future structured data.

## Publication Flow

Publishing uses the physical DHT client. The client resolves the K closest known
physical nodes for the key and only returns `stored` when the required replicas
accepted the record. Each request tracks `stored_by` to avoid redundant writes
while forwarding.

The DHT protocol also supports forwarded publication/query requests. This lets
the requested physical node continue the search and return the result through a
temporary return path instead of forcing the original requester to chase every
closer peer.

## Proof Of Work

The network uses one configured difficulty:

```text
CoreConfig.network_pow_difficulty_bits
```

The nonce is semantic: it is attached to the payload or payload fragment that is
actually being published. The canonical material excludes `pow_nonce` so every
peer can validate the same hash.

Payloads currently carrying their own nonce:

- `DpntRecordPayload`
- `DrtRouteEntryRecord`
- `DdtHolderRecord`
- `DttEntryRecord`
- `DptRecordPayload`

This matters for aggregate records. For example, `DrtRecordPayload` can contain
multiple route entries, but each `DrtRouteEntryRecord` is validated separately
because peers may publish or replicate a single route entry.

## Query Flow

Queries are forwarded hop-by-hop through DHT handlers instead of requiring the
originating node to chase every closer peer itself. The request avoids explicit
origin metadata at the protocol level: no origin node id, visited list, or
public hop counter is included in the query payload.

## Merge Rules

Merge behavior depends on namespace:

- DRT merges route entries for the same virtual node.
- DDT merges holders for the same content.
- DPT keeps the latest valid signed pointer.
- DPNT keeps the valid physical-node record for the derived node id.
- DTT merges structured entries according to its table model.

## Maintenance

DHT maintenance republishes validated local records and transfers
responsibility when the K closest nodes around a key change. This keeps records
available as the local network view evolves.

## Limits

- The MVP does not include a mature reputation system.
- Coordinated malicious peers are not fully mitigated.
- Large-list sharding is modeled but not production-hardened.
