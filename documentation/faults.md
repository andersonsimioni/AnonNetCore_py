# Faults and Limits

AnonNetCore is a functional MVP. It validates architecture and protocols, but it
is not a production product.

## Security Limits

- There is no complete defense against Sybil attacks, flooding, spam, or
  coordinated abuse.
- There is no mature reputation system.
- There is no robust per-peer, per-namespace, or per-application rate limit.
- There has been no formal cryptographic security audit.
- Profile-level privacy, moderation, and blocking are intentionally minimal in
  the social PoC.

## Network Limits

- Full NAT traversal is not implemented.
- Relay support exists as an MVP transport, not as a production relay network.
- QUIC is not implemented.
- The test environment mostly uses LAN and Docker, not a complex public
  Internet deployment.

## DHT Limits

- The DHT replicates records to the responsible K nodes and maintains validated
  records, but conflict handling is still simple.
- Malicious conflict resolution is not complete.
- Proof-of-work adds publication cost but is not a complete anti-abuse system.

## Route Limits

- The main strategy used by the MVP is `random_walk_ttl`.
- Other strategies are represented as extension points or prototypes.
- Global adversary correlation protection is not complete.

## PoC Limits

- Profile pictures are stored as data URLs inside social profile state for demo
  simplicity.
- There is no pagination, distributed ranking, moderation, or full distributed
  search.
