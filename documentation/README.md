# AnonNetCore Python MVP - Documentation

This folder documents the functional Python MVP. The documents are intended to
support maintenance, validation, and thesis writing.

## Main Documents

- [Technical overview](documentation.md): goals, architecture, layers, modules,
  runtime flow, and MVP boundaries.
- [Entities and persistence](entities.md): local entities, SQLite tables,
  in-memory state, and distributed records.
- [DHT and distributed tables](dht_doc.md): key model, namespaces, XOR
  responsibility, replication, maintenance, and proof-of-work.
- [Routes and route execute](route.md): route construction, DRT publication,
  physical/virtual encapsulation, and hop-by-hop forwarding.
- [Faults and limits](faults.md): known risks and expected behavior under
  failures.
- [Local API](api.md): HTTP endpoints, WebSocket events, and external app usage.
- [Social PoC](poc.md): demo social network, DPT/DDT profile state, feed, and
  direct messages.
- [Tests](tests/README.md): smoke tests, integration flows, and commands.

## Current MVP Scope

The MVP demonstrates:

- physical nodes connected through TCP and relay transport;
- bootstrap through hardcoded public endpoints in `CoreConfig`;
- physical-node information exchange and validation;
- physical sessions with KEM, signatures, encryption, keepalive, and reliable
  delivery metadata;
- generic DHT namespaces `dpnt`, `drt`, `ddt`, `dpt`, plus DTT parsing support;
- proof-of-work cost for DHT publication;
- replicated DHT publication to the K closest physical nodes;
- periodic DHT maintenance for validated records;
- automatic route publication for local virtual nodes;
- virtual sessions using routes published in DRT;
- virtual messages through `VIRTUAL_SESSION_DATA`;
- virtual content transfer by byte ranges;
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

## Thesis Note

This is an architectural MVP. It proves the network design in a controlled
environment, but abuse protection, real Internet scale, cryptographic hardening,
resource governance, NAT traversal, and formal audits remain future work.
