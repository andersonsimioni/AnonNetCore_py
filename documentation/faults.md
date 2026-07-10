# Faults and Limits

AnonNetCore is a functional MVP. It validates architecture and protocols, but it
is not a production product.

## Security Limits

- There is no complete defense against Sybil attacks, flooding, spam, or
  coordinated abuse.
- There is no mature reputation system.
- There is no robust per-peer, per-namespace, or per-application rate limit.
- There has been no formal cryptographic security audit.
- Local private key storage and cryptographic lifecycle management are not
  production-hardened.
- The local HTTP/WebSocket API does not yet provide strong application
  authentication or a permission model between local apps and the local core.
- HTTPS is not enabled by default because the current target is local
  development and local application integration.
- Profile-level privacy, moderation, and blocking are intentionally minimal in
  the social PoC.

## Network Limits

- Full NAT traversal is not implemented.
- UDP transport exists as an experimental adapter, but UDP hole punching,
  STUN, and TURN are not implemented.
- Relay support exists as an MVP transport, not as a production relay network.
- QUIC is not implemented.
- The test environment mostly uses LAN and Docker, not a complex public
  Internet deployment.
- Long-running tests with Internet-scale churn, latency, packet loss, and
  heterogeneous public nodes remain future work.

## DHT Limits

- The DHT replicates records to the responsible K nodes and maintains validated
  records, but conflict handling is still simple.
- The DHT does not yet implement a complete Kademlia-style routing table,
  bucket management, iterative lookup, refresh policy, or mature churn
  handling.
- Malicious conflict resolution is not complete.
- Proof-of-work adds publication cost but is not a complete anti-abuse system.
- Large DDT and DTT records are not yet sharded, paginated, expired, or garbage
  collected in a production-ready way.

## Route Limits

- The main strategy used by the MVP is `random_walk_ttl`.
- Other strategies are represented as extension points or prototypes.
- Global adversary correlation protection is not complete.
- Route failover, route migration, adaptive retry policies, and advanced
  congestion handling are not complete.

## Reliability Limits

- Reliable session payload delivery is implemented with sequence numbers,
  acknowledgments, duplicate detection, ordered delivery, and retransmission.
- The reliability layer is still lightweight and does not replace mature
  congestion control, adaptive backoff, session migration, or full reliability
  coverage for every control protocol.

## PoC Limits

- Profile pictures are stored as data URLs inside social profile state for demo
  simplicity.
- Friend feeds currently download complete friend profile states instead of a
  media-optimized or paginated distributed feed format.
- There is no pagination, distributed ranking, moderation, or full distributed
  search.
