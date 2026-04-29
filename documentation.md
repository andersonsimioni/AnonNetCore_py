# documentation.md

## 1. Document purpose

- This file is the normative implementation guide for the **Python MVP** of the TCC project.
- It replaces the previous implementation baseline for the restart phase.
- It must be used by Codex and human developers as the **main technical source of truth** for the codebase.
- It is based on the TCC architecture: modular decentralized overlay network, secure communication, distributed persistence, decentralized applications, peer discovery, routing, storage, anonymity, and post-quantum-ready cryptography. The semester ratification also explicitly requires an MVP, anonymity, persistence, distributed communication techniques, and the use of Kyber-1024, SHA3-512, and AES-CBC-256 in the project scope.

---

## 2. Project objective

- Build a **Python MVP** of a decentralized P2P overlay engine.
- The MVP must prove the architecture works in practice before a future high-performance rewrite.
- The MVP must prioritize:
  - implementation speed
  - clarity of architecture
  - testability with many peers
  - local automation
  - deterministic behavior
  - modular code
- The MVP must support, at minimum:
  - peer discovery
  - connection establishment
  - virtual-node identity routing
  - encrypted message exchange
  - immutable object publication
  - DDT lookup by content hash
  - DTT lookup by tags
  - multi-hop routing
  - local persistence
  - automated multi-peer tests
- The social-network proof of concept is **not** the first milestone. The first milestone is the network engine.

---

## 3. MVP strategy

### 3.1 Core decision

- The Python MVP is a **functional architecture prototype**, not the final optimized implementation.
- It must prove the behavior of the network, not maximum performance.
- Local same-machine testing with many processes is the default environment.

### 3.2 Transport decision for MVP

- **MVP transport = TCP only**.
- No UDP hole punching in the first MVP.
- No TURN/STUN in the first MVP.
- No QUIC in the first MVP.
- All NAT traversal logic must remain abstracted behind interfaces for later integration.

### 3.3 Crypto decision for MVP

- The architecture target remains:
  - **Dilithium** for signatures
  - **Kyber-1024** for KEM/session establishment
  - **AES-256-CBC** for payload encryption
  - **SHA3-512** for hashing
- To avoid blocking implementation speed, the Python MVP must use a **provider abstraction**:
  - `HashProvider`
  - `SignatureProvider`
  - `KEMProvider`
  - `SymmetricCipherProvider`
- Two operating modes are allowed:
  - `strict_architecture_mode`: real algorithms matching the thesis target as closely as available
  - `dev_mvp_mode`: compatible development substitutes behind the same interfaces for faster local integration
- The code must never hardcode cryptographic algorithms directly into routing, discovery, storage, or protocol logic.

### 3.4 Storage decision for MVP

- **MVP local metadata store = SQLite**.
- **MVP object store = filesystem**.
- SQLite is chosen only for Python MVP speed and simplicity.
- Future rewrites may move metadata to LMDB or another engine.

### 3.5 Serialization decision for MVP

- **Canonical wire format = MessagePack**.
- **Canonical local debug export = JSON**.
- MessagePack should be used for protocol messages.
- JSON may be used for logs, fixtures, snapshots, diagnostics, and exported metadata.

---

## 4. Python MVP baseline

### 4.1 Runtime

- Python 3.x
- `asyncio` as the concurrency model
- one operating system process per peer
- one event loop per peer process

### 4.2 Mandatory implementation style

- asynchronous networking
- explicit dataclasses or typed models for all protocol structures
- no shared global mutable state across modules
- clean separation of interfaces and implementations
- test-first for protocol-critical modules
- all network messages versioned
- all records timestamped or expiration-bounded when required

### 4.3 Suggested repository shape

- `anonnetcore/`
  - `app_api/`
  - `config/`
  - `core/`
  - `crypto/`
  - `discovery/`
  - `routing/`
  - `storage/`
  - `tables/`
  - `transport/`
  - `protocol/`
  - `identity/`
  - `simulation/`
  - `tests/`
  - `tools/`

---

## 5. Layer model

### 5.1 Physical and Classical Network Layer

Responsibilities:
- TCP listeners
- outbound TCP connections
- framing
- socket lifecycle
- keepalive
- peer endpoint management
- connection retry policy
- relay placeholders
- future NAT traversal abstraction

Input:
- physical peer endpoints

Output:
- secure or unsecured transport channels between physical nodes

### 5.2 Overlay Network Layer

Responsibilities:
- logical identity routing
- DRT resolution
- path creation
- path forwarding
- path state
- route expiration
- next-hop logic
- DDT/DDT/DTT/DPT/DPNT interaction

Input:
- virtual node IDs
- content hashes
- distributed table lookups

Output:
- route contexts
- forwarded protocol messages

### 5.3 Service Layer

Responsibilities:
- messaging API
- object publish/retrieve API
- tag lookup API
- pointer API
- local engine services

### 5.4 Application Layer

Responsibilities:
- CLI client
- later social-network PoC
- test clients
- automation clients

---

## 6. System entities

### 6.1 Physical Node

Definition:
- the actual running engine process participating in transport communication

Fields:
- `physical_public_key`
- `physical_private_key`
- `physical_node_id = H(physical_public_key)`
- `endpoints[]`
- `transport_methods[]`
- `reachability_class`
- `relay_capable`
- `hole_punch_capable`
- `protocol_version`
- `feature_flags[]`
- `last_validated_at`
- `status`

Responsibilities:
- own transport sockets
- connect to other physical nodes
- host virtual nodes
- forward overlay traffic
- publish physical descriptors to DPNT
- participate in bootstrap and discovery

### 6.2 Persistent Virtual Node

Definition:
- long-lived overlay identity for user/application presence

Fields:
- `virtual_public_key`
- `virtual_private_key`
- `virtual_node_id = H(virtual_public_key)`
- `owner_profile_ref`
- `created_at`
- `status`

Responsibilities:
- act as the main overlay identity
- send and receive routed messages
- own logical resources
- publish or authorize DRT/DPT state

### 6.3 Ephemeral Virtual Node

Definition:
- temporary overlay identity for short-lived tasks

Fields:
- `ephemeral_public_key`
- `ephemeral_private_key`
- `ephemeral_virtual_node_id = H(ephemeral_public_key)`
- `purpose`
- `created_at`
- `expires_at`
- `bound_object_hash?`
- `bound_pointer_key?`

Responsibilities:
- serve content anonymously
- represent temporary storage holders
- optionally publish temporary routing presence
- expire naturally unless renewed

### 6.4 Data Object

Definition:
- immutable byte payload stored and retrieved by content hash

Fields:
- `content_hash`
- `type`
- `title`
- `tags[]`
- `content_length`
- `created_at`
- `content_codec`
- `encryption_mode`
- `metadata`

Responsibilities:
- immutable storage unit
- retrieval target
- replication unit

### 6.5 Protocol Message

Definition:
- typed wire message used by the network

Fields:
- `message_id`
- `message_type`
- `protocol_version`
- `source_physical_node_id?`
- `source_virtual_node_id?`
- `target_virtual_node_id?`
- `path_id?`
- `created_at`
- `ttl_ms?`
- `payload`
- `signature?`
- `mac?`

Responsibilities:
- discovery
- handshake
- route creation
- routing
- lookup
- object exchange
- acknowledgments

---

## 7. Identifiers and hashing

### 7.1 Canonical hash target

- Architecture target: **SHA3-512**.
- Hash output is used for:
  - physical node ID
  - virtual node ID
  - content hash
  - tag key
  - DPT key
  - message dedup key

### 7.2 Hash rules

- `physical_node_id = H(physical_public_key_bytes)`
- `virtual_node_id = H(virtual_public_key_bytes)`
- `content_hash = H(canonical_object_representation)`
- `tag_key = H(normalize_tag(tag))`
- `pointer_key = H(owner_virtual_node_public_key || stable_title)`
- `message_hash = H(canonical_protocol_message_without_transport_wrapper)`

### 7.3 Tag normalization algorithm

- input: raw tag string
- steps:
  - trim whitespace
  - lowercase
  - unicode normalize
  - collapse internal repeated spaces
  - replace spaces with single hyphen or preserve spaces consistently, choose one and keep it global
  - remove forbidden control characters
- output: normalized tag string

Normative rule:
- tag normalization must be deterministic and shared across all peers.

---

## 8. Cryptographic model

### 8.1 Target algorithms

- hash: **SHA3-512**
- signature: **Dilithium**
- KEM: **Kyber-1024**
- symmetric encryption: **AES-256-CBC**
- message authentication with CBC mode: **HMAC-SHA3-512**

### 8.2 MVP provider interfaces

#### `HashProvider`

Methods:
- `hash_bytes(data: bytes) -> bytes`
- `hash_hex(data: bytes) -> str`

#### `SignatureProvider`

Methods:
- `generate_keypair() -> (public_key, private_key)`
- `sign(private_key, data: bytes) -> signature`
- `verify(public_key, data: bytes, signature) -> bool`

#### `KEMProvider`

Methods:
- `generate_keypair() -> (public_key, private_key)`
- `encapsulate(public_key) -> (ciphertext, shared_secret)`
- `decapsulate(private_key, ciphertext) -> shared_secret`

#### `SymmetricCipherProvider`

Methods:
- `generate_key() -> bytes`
- `encrypt(key, iv, plaintext: bytes) -> ciphertext`
- `decrypt(key, iv, ciphertext: bytes) -> plaintext`
- `mac(mac_key, data: bytes) -> tag`
- `verify_mac(mac_key, data: bytes, tag) -> bool`

### 8.3 Session establishment algorithm

1. initiator resolves route or direct physical connection target
2. initiator sends `SESSION_INIT`
3. responder creates ephemeral KEM keypair
4. responder signs ephemeral KEM public key with its signature key
5. responder returns `SESSION_INIT_OK`
6. initiator verifies signature
7. initiator encapsulates shared secret using responder ephemeral KEM public key
8. initiator derives:
   - `session_enc_key`
   - `session_mac_key`
   - `session_id`
9. initiator sends `SESSION_KEY_CONFIRM` containing ciphertext + signature
10. responder decapsulates shared secret
11. responder derives same session keys
12. responder sends encrypted `SESSION_READY`
13. both peers mark session as active

### 8.4 Session key derivation

- derive keys from `shared_secret || session_nonce || initiator_id || responder_id`
- produce:
  - encryption key
  - MAC key
  - session ID

### 8.5 Encrypted payload format

Fields:
- `session_id`
- `iv`
- `ciphertext`
- `mac_tag`
- `seq_no`
- `created_at`

Rules:
- MAC must cover header + IV + ciphertext
- reject on MAC failure
- reject on expired freshness window
- reject on duplicate `seq_no` when replay cache is enabled

### 8.6 Replay protection

- each active session maintains recent sequence cache
- reject duplicate sequence numbers within active window
- reject messages older than configured freshness bound

---

## 9. Distributed structures

### 9.1 DRT — Distributed Route Table

Purpose:
- map target virtual node ID to candidate physical entry points

Logical form:
- `H(pk_vn) -> { route_entries: {(H(pk_pn), exp, rtt_estimate), ...}, last_updated }`

Local schema:
- `target_virtual_node_id`
- `entry_physical_node_id`
- `entry_endpoint`
- `expires_at`
- `rtt_estimate_ms`
- `published_by`
- `signature`
- `last_updated`

Rules:
- soft state only
- entries expire naturally
- entries are hints, not complete routes
- do not expose stable physical ownership of the virtual node

### 9.2 DDT — Distributed Data Table

Purpose:
- map content hash to lightweight object descriptor and holders

Logical form:
- `H(content) -> {title, type, tags[], holders: {(H(pk_vn), exp), ...}}`

Local schema:
- `content_hash`
- `title`
- `type`
- `tags[]`
- `content_length`
- `holder_virtual_node_id`
- `holder_expires_at`
- `published_at`
- `signature`

Rules:
- DDT does not store object bytes
- DDT stores holder advertisements only
- stale holders may exist temporarily
- requester must try alternatives on failure

### 9.3 DTT — Distributed Tag Table

Purpose:
- semantic lookup from tag to candidate content hashes

Logical form:
- `H(tag) -> {H(content1), H(content2), ...}`

Local schema:
- `tag_key`
- `normalized_tag`
- `content_hash`
- `published_at`
- `expires_at?`
- `signature?`

Rules:
- DTT is the first semantic resolution stage
- multiple tags are combined by intersection first
- union and ranking are optional second-stage strategies

### 9.4 DPT — Distributed Pointer Table

Purpose:
- stable pointer from owner+title to latest mutable state object

Logical form:
- `H(pk_vn_owner || title) -> {title, pk_vn_owner, type, last_modified, target_ref, signature}`

Local schema:
- `pointer_key`
- `stable_title`
- `owner_virtual_node_id`
- `type`
- `target_ref`
- `last_modified`
- `signature`

Rules:
- title is stable and immutable in pointer key generation
- mutable history lives outside DPT in referenced objects

### 9.5 DPNT — Distributed Physical Nodes Table

Purpose:
- store recoverable operational descriptors for physical peers

Logical form:
- `H(pk_physical_node) -> { endpoints[], transport_methods[], reachability_class, relay_capable, hole_punch_capable, protocol_version, feature_flags[], last_validated_at, status }`

Local schema:
- `physical_node_id`
- `endpoints[]`
- `transport_methods[]`
- `reachability_class`
- `relay_capable`
- `hole_punch_capable`
- `protocol_version`
- `feature_flags[]`
- `last_validated_at`
- `status`
- `signature`

Rules:
- DPNT complements local discovery
- DPNT does not replace bootstrap
- only validated descriptors should be published

---

## 10. Core modules

### 10.1 `config`

Responsibilities:
- load environment
- load peer runtime config
- load port assignments
- load crypto provider mode
- load bootstrap peers
- load timeouts

Key files:
- `settings.py`
- `models.py`
- `defaults.py`

### 10.2 `core`

Responsibilities:
- engine startup
- dependency wiring
- lifecycle management
- task supervision
- shutdown handling

Key files:
- `engine.py`
- `runtime.py`
- `service_container.py`

### 10.3 `identity`

Responsibilities:
- create/load physical identities
- create/load persistent virtual identities
- create ephemeral identities
- identity serialization
- key persistence

Key files:
- `identity_manager.py`
- `models.py`
- `key_store.py`

### 10.4 `transport`

Responsibilities:
- TCP server
- TCP client
- framing
- connection pool
- message send/receive
- keepalive
- reconnect logic

Key files:
- `tcp_server.py`
- `tcp_client.py`
- `frame_codec.py`
- `connection_registry.py`

### 10.5 `crypto`

Responsibilities:
- crypto provider interfaces
- provider implementations
- session key derivation
- payload encrypt/decrypt
- signature verify
- replay cache

Key files:
- `providers.py`
- `session_crypto.py`
- `hashing.py`
- `signatures.py`
- `kem.py`
- `symmetric.py`

### 10.6 `protocol`

Responsibilities:
- protocol message schemas
- message encoding/decoding
- validation
- handler dispatch

Key files:
- `message_types.py`
- `schemas.py`
- `codec.py`
- `dispatcher.py`

### 10.7 `discovery`

Responsibilities:
- bootstrap from seed peers
- physical peer exchange
- local peer scoring
- peer validation
- DPNT publication/lookup

Key files:
- `bootstrap.py`
- `peer_exchange.py`
- `peer_validator.py`
- `peer_cache.py`

### 10.8 `tables`

Responsibilities:
- local management of DRT/DDT/DTT/DPT/DPNT records
- record replication hooks
- expiration/pruning
- query APIs

Key files:
- `drt_store.py`
- `ddt_store.py`
- `dtt_store.py`
- `dpt_store.py`
- `dpnt_store.py`

### 10.9 `routing`

Responsibilities:
- resolve target virtual node to entry points
- create route contexts
- assign path IDs
- forward data hop by hop
- route cache
- failure retry

Key files:
- `route_builder.py`
- `route_forwarder.py`
- `route_cache.py`
- `path_registry.py`

### 10.10 `storage`

Responsibilities:
- object hashing
- object publish
- object retrieval
- byte-range retrieval
- local object filesystem
- DDT/DTT updates

Key files:
- `object_store.py`
- `publisher.py`
- `retriever.py`
- `tag_indexer.py`

### 10.11 `app_api`

Responsibilities:
- local engine API for CLI/app/test harness
- send message
- publish object
- retrieve object
- query tags
- create pointers

Key files:
- `service.py`
- `models.py`
- `errors.py`

### 10.12 `simulation`

Responsibilities:
- spawn multiple peers
- create deterministic scenarios
- generate fixtures
- collect metrics
- kill/restart peers

Key files:
- `peer_launcher.py`
- `scenario_runner.py`
- `metrics.py`
- `topologies.py`

---

## 11. Wire protocol

### 11.1 General envelope

Every network message must contain:
- `protocol_version`
- `message_type`
- `message_id`
- `created_at`
- `ttl_ms?`
- `source_physical_node_id?`
- `source_virtual_node_id?`
- `target_virtual_node_id?`
- `payload`
- `signature?`
- `session_id?`
- `path_id?`

### 11.2 Message families

#### Discovery
- `PING`
- `PONG`
- `DISCOVERY_REQUEST`
- `DISCOVERY_RESPONSE`
- `DPNT_PUBLISH`
- `DPNT_QUERY`
- `DPNT_RESULT`

#### Session / crypto
- `SESSION_INIT`
- `SESSION_INIT_OK`
- `SESSION_KEY_CONFIRM`
- `SESSION_READY`
- `SESSION_CLOSE`

#### Routing
- `ROUTE_CREATE`
- `ROUTE_CREATE_FORWARD`
- `ROUTE_CREATE_OK`
- `ROUTE_CREATE_FAIL`
- `ROUTE_DATA`
- `ROUTE_ACK`

#### Table operations
- `DRT_PUBLISH`
- `DRT_QUERY`
- `DRT_RESULT`
- `DDT_PUBLISH`
- `DDT_QUERY`
- `DDT_RESULT`
- `DTT_PUBLISH`
- `DTT_QUERY`
- `DTT_RESULT`
- `DPT_PUBLISH`
- `DPT_QUERY`
- `DPT_RESULT`

#### Object exchange
- `OBJECT_GET`
- `OBJECT_CHUNK`
- `OBJECT_END`
- `OBJECT_PUT_ANNOUNCE`
- `OBJECT_PUT_CONFIRM`

#### Application
- `APP_MESSAGE_SEND`
- `APP_MESSAGE_DELIVERED`

### 11.3 Framing format

- TCP messages must use length-prefixed framing.
- Recommended frame layout:
  - `4 bytes length`
  - `N bytes msgpack payload`

---

## 12. Routing model

### 12.1 MVP routing objective

- route messages toward a target virtual node using:
  - DRT for entry-point hints
  - local hop-by-hop forwarding state
  - multi-hop path construction

### 12.2 Route creation algorithm

1. requester resolves `target_virtual_node_id` in DRT
2. requester selects one candidate entry point
3. requester creates:
   - `route_id_global` for local request tracking
   - `path_id_local` for first hop
   - `route_expiration`
   - `ttl_ms`
4. requester sends `ROUTE_CREATE`
5. each hop:
   - validates TTL
   - creates new local `path_id`
   - stores mapping `(incoming_path_id -> outgoing_path_id, next_hop)`
   - forwards request
6. final entry physical node resolves target hosted virtual node
7. final entry responds `ROUTE_CREATE_OK`
8. path becomes active

### 12.3 Forwarding algorithm

1. receive `ROUTE_DATA`
2. lookup local `path_id`
3. if local node is final delivery host:
   - deliver to hosted virtual node
4. else:
   - rewrite local forwarding wrapper
   - send to next hop

### 12.4 Failure algorithm

1. active route fails
2. sender marks route broken
3. sender retries with next cached candidate
4. if no cached candidate works:
   - new DRT lookup
   - new route creation
5. if all fail:
   - operation returns error

### 12.5 Route cache

Store:
- `target_virtual_node_id`
- `entry_physical_node_id`
- `path_metadata`
- `expires_at`
- `last_success_at`
- `failure_count`

Rules:
- prefer working cached routes first
- invalidate on repeated failure

---

## 13. Discovery model

### 13.1 Bootstrap

- each peer starts with one or more configured bootstrap peers
- bootstrap peers are physical nodes only
- initial bootstrap returns reachable physical peers only

### 13.2 Discovery algorithm

1. peer connects to bootstrap peer
2. peer sends `DISCOVERY_REQUEST`
3. peer receives bounded sample of validated physical peers
4. peer attempts direct validation of those peers
5. validated peers enter local peer cache
6. peer may publish validated descriptors into DPNT

### 13.3 Local peer cache fields

- `physical_node_id`
- `endpoints[]`
- `transport_methods[]`
- `last_seen`
- `last_successful_connect_at`
- `reachability_class`
- `score`
- `status`

### 13.4 Peer scoring

Score inputs:
- successful connections
- successful pings
- successful forwarding
- stale failures
- repeated timeout
- invalid protocol behavior

### 13.5 Discovery pruning

- remove or downgrade peers after repeated failure thresholds
- never trust indirect peers without validation

---

## 14. Storage model

### 14.1 Object representation

Canonical object package:
- `type`
- `title`
- `tags[]`
- `created_at`
- `content_bytes`
- `metadata`

Canonical content hash must be derived from the **exact serialized immutable representation**.

### 14.2 Publish algorithm

1. application asks engine to publish object
2. engine serializes canonical immutable representation
3. engine computes `content_hash`
4. engine writes object bytes to local object store
5. engine creates or selects ephemeral storage virtual node
6. engine publishes DDT holder advertisement
7. engine publishes DTT entries for each normalized tag
8. engine returns `content_hash`

### 14.3 Retrieve algorithm

1. requester knows `content_hash` or obtains it via DTT
2. requester queries DDT
3. requester receives holder virtual nodes
4. requester chooses one holder
5. requester resolves holder route through DRT
6. requester creates route/session
7. requester sends `OBJECT_GET`
8. holder returns chunks
9. requester reconstructs object
10. requester validates hash
11. requester may keep local replica and republish holder advertisement

### 14.4 Byte-range retrieval

- support `offset`
- support `length`
- holder may send partial `OBJECT_CHUNK` sequence

### 14.5 Replication rule

- ordinary content is replicated by consumer interest
- no mandatory storage by uninvolved peers in MVP

---

## 15. Pointer model

### 15.1 Purpose

- support mutable logical resources while keeping stored objects immutable

### 15.2 Pointer update algorithm

1. owner creates new immutable state object
2. owner computes `target_ref = new_content_hash`
3. owner computes `pointer_key = H(owner_public_key || stable_title)`
4. owner signs pointer record
5. owner publishes DPT record

### 15.3 Use cases

- latest profile state
- latest post state
- mutable timeline root
- latest permissions object

---

## 16. Local persistence layout

### 16.1 SQLite tables

Recommended local tables:
- `physical_identities`
- `virtual_identities`
- `ephemeral_virtual_identities`
- `known_physical_peers`
- `sessions`
- `route_cache`
- `path_registry`
- `drt_records`
- `ddt_records`
- `dtt_records`
- `dpt_records`
- `dpnt_records`
- `message_dedup_cache`
- `local_objects`
- `object_tags`
- `outbox`
- `inbox`

### 16.2 Filesystem layout

- `data/peer_<id>/keys/`
- `data/peer_<id>/db/`
- `data/peer_<id>/objects/aa/bb/<content_hash>.bin`
- `data/peer_<id>/logs/`
- `data/peer_<id>/snapshots/`

### 16.3 Object file naming

- content addressed by full hash
- shard into directories by first bytes of hash

---

## 17. Implementation phases

### Phase 0 — Skeleton

Deliver:
- project structure
- config loader
- engine bootstrap
- logger
- MessagePack codec
- length-prefixed TCP framing
- SQLite initialization
- identity generation/loading

Acceptance:
- one peer starts cleanly
- peer persists identity on restart
- peer listens on configured TCP port

### Phase 1 — Physical transport

Deliver:
- TCP server/client
- peer connection registry
- ping/pong
- basic discovery request/response
- local peer cache

Acceptance:
- peer A connects to peer B
- peer B returns known peers
- peers survive reconnects
- automated test with 10 peers passes

### Phase 2 — Sessions and crypto abstraction

Deliver:
- crypto provider interfaces
- development provider implementation
- session init flow
- encrypted payload wrapper
- replay protection cache

Acceptance:
- two peers establish session
- encrypted ping works
- invalid MAC is rejected
- replay attempt is rejected

### Phase 3 — Virtual identities and DRT

Deliver:
- persistent virtual node support
- DRT local schema
- DRT publish/query
- simple entry-point resolution
- route cache

Acceptance:
- virtual node is published
- another peer resolves it via DRT
- route candidate list is returned correctly

### Phase 4 — Multi-hop routing

Deliver:
- path registry
- route creation messages
- hop-by-hop forwarding
- route data forwarding
- route failure retry

Acceptance:
- peer A reaches peer C through peer B
- path state is local per hop
- broken route retries another path

### Phase 5 — DDT and object storage

Deliver:
- immutable object serialization
- local object store
- DDT publish/query
- object retrieval by hash
- hash verification

Acceptance:
- peer A publishes object
- peer B resolves holder via DDT
- peer B retrieves object and validates hash

### Phase 6 — DTT and semantic search

Deliver:
- tag normalization
- DTT publish/query
- multi-tag intersection

Acceptance:
- peer A publishes object with tags
- peer B searches by one tag
- peer B searches by two tags with intersection

### Phase 7 — DPT and mutable logical resources

Deliver:
- pointer key generation
- DPT publish/query
- pointer update flow

Acceptance:
- resource pointer resolves latest state object
- updating pointer changes latest reference only

### Phase 8 — App API and CLI client

Deliver:
- local service API
- send message command
- publish object command
- retrieve object command
- tag lookup command
- pointer resolve command

Acceptance:
- local CLI can exercise all core flows

### Phase 9 — Simulation harness

Deliver:
- process launcher for N peers
- deterministic topology generation
- scenario runner
- metrics collector
- churn simulator

Acceptance:
- 25-peer automated scenario passes
- 50-peer automated scenario passes
- 100-peer smoke test passes

### Phase 10 — Social-network PoC

Deliver:
- minimal profile object
- minimal post object
- timeline lookup via DPT/DDT/DTT
- private message flow

Acceptance:
- user profile published and resolved
- post published and found by tags
- private message delivered through routed session

---

## 18. Recommended implementation order inside each phase

For every new feature, follow this exact order:

1. define datamodel
2. define storage schema
3. define wire messages
4. define service interface
5. implement local-only logic
6. implement network handler
7. write unit tests
8. write multi-peer integration test
9. add logs/metrics
10. only then expose it to CLI/app API

---

## 19. Test plan

### 19.1 Unit tests

Mandatory targets:
- tag normalization
- canonical object serialization
- content hashing
- pointer key generation
- route/path registry logic
- crypto provider contract
- replay cache
- expiration handling

### 19.2 Integration tests

Mandatory scenarios:
- 2-peer direct connection
- 3-peer multi-hop route
- 5-peer DRT lookup
- 5-peer DDT object retrieval
- 5-peer DTT tag intersection
- route failure and retry
- stale holder fallback
- peer restart with persisted identity

### 19.3 Multi-peer simulation tests

Mandatory scenarios:
- 25 peers join network
- 50 peers peer-discovery expansion
- 100 peers smoke test
- 100 peers message flood control test
- 100 peers object publication and random retrieval test
- churn test with random peer shutdown/restart

### 19.4 Metrics

Collect:
- route creation latency
- message delivery latency
- object retrieval latency
- DRT lookup latency
- DDT lookup latency
- DTT lookup latency
- failed route count
- session establishment failures
- object hash mismatch count
- peer churn recovery time

---

## 20. MVP simplifications explicitly allowed

- TCP only
- local same-machine test topology first
- SQLite instead of LMDB
- development crypto providers behind architecture interfaces
- no full DHT partitioning in first version
- distributed tables may initially operate as replicated soft-state stores over known peers instead of a fully optimized Kademlia-like implementation
- limited relay placeholders only
- no fancy reputation system
- no GUI first
- no media-heavy social features first

---

## 21. Features explicitly out of scope for first MVP

- full UDP hole punching
- STUN/TURN production integration
- QUIC transport
- bandwidth optimization
- onion-layer payload wrapping beyond current hop/session requirements
- advanced proof-of-retrievability
- full byzantine resistance
- anti-censorship hardening
- production-grade abuse prevention
- mobile clients
- browser clients

---

## 22. Non-negotiable architectural rules

- physical identity and virtual identity must remain separate
- all stored content must be immutable
- all distributed records with soft state must expire naturally
- routing must be hop by hop
- DRT must return entry hints, not full routes
- DDT must return holders, not object bytes
- DTT must remain semantic lookup only
- DPT must remain pointer indirection only
- protocol messages must be versioned
- every network-facing structure must be serializable deterministically
- every feature must be testable with multiple peer processes

---

## 23. First deliverable after this document

The first real coding target after adopting this file must be:

- start 3 peers locally
- bootstrap peer discovery
- establish physical TCP connections
- create/load physical and virtual identities
- exchange encrypted ping over a session
- publish one DRT record
- resolve one virtual node through DRT
- create one multi-hop route
- send one encrypted routed application message

If this works reliably, the project foundation is correct.

---

## 24. Second deliverable after the first one

- publish immutable object
- advertise it in DDT
- advertise tags in DTT
- retrieve object from another peer
- verify hash
- store local replica
- republish as new holder

---

## 25. Final MVP definition

The Python MVP is considered complete when all of the following are true:

- 25+ peers run automatically and pass integration scenarios
- 100 peers can be started for smoke and stress validation
- direct and multi-hop message delivery work
- DRT lookup works
- DDT lookup works
- DTT lookup works
- immutable object retrieval works
- pointer indirection works
- local persistence survives restart
- route retry works after peer failure
- a basic social proof of concept can:
  - create a profile object
  - publish a post object
  - find content by tags
  - send a private message

---

## 26. Practical note for Codex

When in doubt:
- prefer simpler implementation
- prefer deterministic serialization
- prefer explicit datamodels
- prefer tests before optimization
- prefer architecture correctness over speed
- never couple crypto details into routing/storage modules
- never skip the simulation harness

