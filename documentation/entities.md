IMPORTANT
---------
Active sessions, sockets, in-progress handshakes, and temporary in-use routes should remain in memory,
not in the local database.

1. CORE NETWORK ENTITIES
------------------------

1. local_physical_node_identity
Represents the local engine physical identity.

Properties:
- id (sha512 of public key)
- public_key
- private_key_encrypted
- key_algorithm
- created_at
- updated_at
- status

2. local_virtual_node_identity
Represents a virtual node hosted locally.

Properties:
- id (sha512 of public key)
- public_key
- private_key_encrypted
- kind (persistent, ephemeral, storage, relay, etc.)
- owner_physical_node_id
- created_at
- expires_at
- is_active
- metadata_json

3. remote_physical_node_identity
Represents a physical node known by the local engine.

Properties:
- id (sha512 of public key)
- public_key
- display_name (optional)
- reachability_class
- relay_capable
- hole_punch_capable
- protocol_version
- status
- last_seen_at
- last_validated_at
- score
- notes_json

4. node_endpoint
Represents known endpoints of a physical node.

Properties:
- id
- physical_node_hash_id
- transport (tcp, udp, ipv4, ipv6, relay)
- host
- port
- priority
- is_active
- last_success_at
- last_failure_at
- failure_count
- metadata_json

5. bootstrap_seed
Represents nodes or seeds used during initial bootstrap.

Properties:
- id
- host
- port
- transport
- is_enabled
- last_used_at
- last_success_at
- last_failure_at

6. remote_virtual_node_identity
Represents cached remote virtual nodes already known locally.

Properties:
- id (sha512 of public key)
- public_key
- kind
- first_seen_at
- last_seen_at
- expires_at
- status
- metadata_json

2. DISTRIBUTED STATE PERSISTED LOCALLY
--------------------------------------

7. dpnt_record
Represents a local Distributed Physical Nodes Table record.

Properties:
- id (sha512 of public key)
- physical_node_public_key
- endpoints_json
- transport_methods_json
- reachability_class
- relay_capable
- hole_punch_capable
- protocol_version
- feature_flags_json
- last_validated_at
- expires_at
- signature
- source
- created_at
- updated_at

8. drt_record
Represents a local Distributed Route Table record.

Properties:
- id (sha512 of target_virtual_node public key)
- target_virtual_node_public_key
- route_entries_json
- last_updated_at
- expires_at
- signature
- source
- created_at
- updated_at

9. ddt_record
Represents a local Distributed Data Table record.

Properties:
- id (sha512 of file content)
- content_type
- title
- tags_json
- holders_json -> list of DRT keys/virtual nodes hashs, with expire and sign
- source
- created_at
- updated_at

10. dtt_record
Represents a local Distributed Tag Table record.

Properties:
- id (sha512 of tag name)
- normalized_tag
- content_refs_json -> list of DDT keys (files content hashs)
- expires_at
- signature
- source
- created_at
- updated_at

11. dpt_record
Represents a local Distributed Pointer Table record.

Properties:
- id (sha512 of virtual_node_owner_public_key ∥ title)
- owner_virtual_node_public_key
- title
- resource_type
- target_ref
- last_modified_at
- signature of pk_virtual_node owner)
- expires_at
- source
- created_at
- updated_at

3. LOCAL CONTENT STORAGE
------------------------

12. content_object
Represents local metadata of stored content.

Properties:
- id
- content_hash
- title
- content_type
- mime_type
- size_bytes
- storage_path
- is_encrypted
- encryption_scheme
- created_at
- updated_at
- last_access_at
- is_deleted

13. content_tag
Represents local tags associated with content.

Properties:
- id
- content_object_id
- tag
- normalized_tag
- created_at

14. content_advertisement
Represents the fact that the local engine is advertising possession of a content object.

Properties:
- id
- content_object_id
- advertiser_virtual_node_id
- published_in_ddt
- published_at
- expires_at
- is_active

15. content_replica
Represents local replica or retention control of content.

Properties:
- id
- content_object_id
- retention_policy
- is_pinned
- created_at
- expires_at
- last_verified_at


SESSION ENTITIES FOR THE OVERLAY NETWORK MVP
============================================

IMPORTANT
---------
Persist only session metadata in the local database.
Live socket handles, active cipher contexts, ephemeral runtime secrets,
packet retransmission buffers, and transport-specific runtime internals
should remain in memory.

1. session
Represents a secure session established by the overlay.
The same entity is used for both session scopes:

- hop: secure session between adjacent physical nodes
- e2e: secure session between local and remote virtual nodes

Purpose:
- represent the generic secure session lifecycle
- support both hop-by-hop and end-to-end protection
- avoid duplication of identical establishment logic
- allow the same handshake and key management model to be reused

Properties:
- id
- session_id
- session_scope (hop, e2e)
- local_identity_type (physical, virtual)
- local_identity_id
- remote_identity_type (physical, virtual)
- remote_identity_id
- local_endpoint_id (optional)
- remote_endpoint_id (optional)
- transport (optional)
- direction (inbound, outbound, bidirectional)
- initiator_side (local, remote)
- handshake_state (pending, negotiating, established, failed, closed)
- session_state (active, idle, expired, failed, closed)
- key_exchange_algorithm
- signature_algorithm
- symmetric_algorithm
- hash_algorithm
- local_ephemeral_public_key
- remote_ephemeral_public_key
- session_key_id
- established_at
- last_activity_at
- keepalive_deadline
- expires_at
- closed_at
- close_reason
- bound_route_id (optional)
- metadata_json

Notes:
- for hop sessions, local_identity_type and remote_identity_type must be physical
- for e2e sessions, local_identity_type and remote_identity_type must be virtual
- transport and endpoint fields are mainly relevant for hop sessions
- bound_route_id is mainly relevant for e2e sessions, but may also be used for hop context if useful

2. session_key_record
Represents metadata about a derived session key.
Do not store raw active plaintext keys here unless explicitly protected.

Purpose:
- identify which derived key belongs to which session
- support expiration, rotation, and auditing

Properties:
- id
- session_key_id
- session_id
- key_purpose (encrypt, decrypt, both)
- key_algorithm
- key_size_bits
- key_reference
- derivation_method
- created_at
- activates_at
- expires_at
- revoked_at
- status (pending, active, expired, revoked)
- metadata_json

3. session_handshake_record
Represents the persisted metadata of a handshake attempt.

Purpose:
- track handshake lifecycle
- debug failed establishments
- correlate retries and negotiation attempts

Properties:
- id
- handshake_id
- session_id
- session_scope (hop, e2e)
- initiator_side (local, remote)
- local_identity_id
- remote_identity_id
- local_identity_type
- remote_identity_type
- route_id (optional)
- transport (optional)
- key_exchange_algorithm
- signature_algorithm
- handshake_state
- attempt_count
- started_at
- last_update_at
- completed_at
- failed_at
- failure_reason
- metadata_json

4. session_route_binding
Represents the association between a session and a route.

Purpose:
- bind a session to the currently used route
- allow route replacement without losing the logical session
- preserve route history relevant to a session

Properties:
- id
- binding_id
- session_id
- session_scope (hop, e2e)
- route_id
- first_hop_session_id (optional)
- bound_at
- unbound_at
- status (active, replaced, expired, failed)
- metadata_json

Notes:
- this entity is especially useful for e2e sessions
- for hop sessions, route binding may be optional depending on implementation

5. session_keepalive_record
Represents keepalive and liveness tracking for a session.

Purpose:
- control timeout and expiration
- allow safe session reuse while still valid
- support failure detection

Properties:
- id
- session_id
- session_scope (hop, e2e)
- last_sent_keepalive_at
- last_received_keepalive_at
- last_valid_activity_at
- keepalive_interval_ms
- timeout_interval_ms
- retry_count
- max_retries
- deadline_at
- status (healthy, delayed, stale, expired)
- metadata_json

6. session_failure_record
Represents a failure event related to a session.

Purpose:
- record establishment or runtime failures
- support retry policy
- help diagnose route, transport, or crypto problems

Properties:
- id
- failure_id
- session_id
- session_scope (hop, e2e)
- handshake_id (optional)
- route_id (optional)
- endpoint_id (optional)
- failure_type (timeout, auth_failed, decrypt_failed, route_broken, transport_error, peer_unreachable, expired)
- failure_message
- occurred_at
- retryable
- metadata_json

7. session_rekey_record
Represents rekey or renewal events of a session.

Purpose:
- track key rotation
- preserve key evolution history
- support controlled replacement of key material

Properties:
- id
- rekey_id
- session_id
- session_scope (hop, e2e)
- old_session_key_id
- new_session_key_id
- trigger_reason (rotation, expiry, compromise, manual, reconnect)
- started_at
- completed_at
- status (pending, completed, failed)
- metadata_json

MINIMUM SESSION SET FOR THE MVP
-------------------------------
For the MVP, the minimum useful set is:

- session
- session_key_record
- session_handshake_record
- session_route_binding
- session_keepalive_record
- session_failure_record

SECOND PHASE
------------
Leave this for a second phase if needed:

- session_rekey_record

PRACTICAL RULES
---------------
- use one generic session entity for both hop and e2e scopes
- distinguish the scope through session_scope
- distinguish the identity level through local_identity_type and remote_identity_type
- one e2e session may survive route changes
- hop sessions are usually tied to direct adjacency and transport state
- active cipher objects, sockets, retransmission state, and handshake runtime buffers stay in memory
- the database stores only durable session metadata


5. OPERATIONAL SUPPORT
----------------------

22. seen_hash
Prevents reprocessing of messages, packets, or objects.

Properties:
- id
- hash_value
- hash_type (message, packet, content, control)
- first_seen_at
- expires_at

23. local_setting
Represents persisted engine configuration.

Properties:
- id
- key
- value
- value_type
- updated_at

24. local_event_log
Represents persisted important event logs.

Properties:
- id
- event_type
- severity
- entity_type
- entity_id
- message
- details_json
- created_at

8. PRACTICAL MODELING RULE
--------------------------

To keep the project clean:

- identities, known nodes, content, messages, advertisements, and settings should go into the local database
- active sessions, sockets, temporary in-use routes, and in-progress handshakes should remain in memory
- large payloads should be stored as files or binary objects, and the database should store only metadata and file paths
