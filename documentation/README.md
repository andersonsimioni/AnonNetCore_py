# AnonNetCore Python MVP - Documentation

This folder documents the functional Python MVP. The documentation is written to
support maintenance, validation, and thesis writing while staying close to the
current implementation.

## Main Documents

- [Technical overview](documentation.md): goals, architecture, layers, packages,
  runtime flow, and MVP boundaries.
- [Entities and persistence](entities.md): local identities, remote nodes,
  route resolutions, sessions, content, and distributed records.
- [DHT and distributed tables](dht_doc.md): key model, namespaces, XOR
  responsibility, replication, proof-of-work, and maintenance.
- [Routes and route execute](route.md): route construction, DRT publication,
  route failure, physical/virtual encapsulation, and hop-by-hop forwarding.
- [Local API](api.md): HTTP endpoints, WebSocket events, and external app usage.
- [Social PoC](poc.md): profile state, DDT/DPT publication, feed, and direct
  messages.
- [Faults and limits](faults.md): known risks and expected behavior under
  failures.
- [Tests](tests/README.md): official smoke flows, commands, logs, and topology
  evidence.

## Current MVP Scope

The MVP demonstrates:

- bootstrap through hardcoded public endpoints in `CoreConfig`;
- physical-node information exchange, validation, DPNT publication, and peer
  discovery;
- TCP, UDP, and relay transport adapters behind a common transport interface;
- physical sessions with KEM, signatures, AES-256-GCM-SIV encryption,
  keepalive, reliable ordered payload delivery, retry, and ACK handling;
- generic DHT namespaces `dpnt`, `drt`, `ddt`, `dpt`, and `dtt`;
- semantic proof-of-work nonces for DHT payloads and route entries;
- replicated DHT publication to the K closest physical nodes;
- hop-by-hop DHT query forwarding with return-path handling;
- automatic route publication for local virtual nodes;
- route failure propagation through `ROUTE_CREATE_FAIL`;
- virtual sessions using DRT-published routes;
- virtual messages and virtual content transfer by byte ranges;
- HTTP and WebSocket API for external applications;
- local HTML/JS social PoC without a web server requirement;
- Debug Console for local cores and Docker containers.

## Run the Demo

From the project root:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

Without opening the browser automatically:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10 --no-open
```

## Run the Smokes

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py 10
```

The runner executes the official core and PoC integration flows, records logs
under `data/local/smoke-runs`, prints the randomized cluster topology, and stops
the cluster after completion.

## Thesis Note

This is an architectural MVP. It proves the network design in a controlled
environment, but abuse protection, real Internet scale, cryptographic hardening,
resource governance, mature NAT traversal, and formal audits remain future work.
