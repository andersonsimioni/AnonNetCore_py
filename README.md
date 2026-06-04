# AnonNetCore Python MVP

AnonNetCore Python MVP is a functional peer-to-peer network prototype with a
physical layer, virtual layer, DHT, route construction, virtual sessions,
content transfer, and a local HTML/JS social PoC.

The project exists to validate the architecture and main flows before a more
performant implementation.

## Quick Start

Install dependencies in the local virtual environment and run:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

The PoC command also starts the Debug Console.

Run only one local core:

```powershell
.\.venv\Scripts\python.exe scripts\run_core.py
```

Run the official smoke suite:

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py 10
```

## Documentation

Technical documentation lives in [documentation/README.md](documentation/README.md).

Main references:

- [Technical overview](documentation/documentation.md)
- [Entities and persistence](documentation/entities.md)
- [DHT](documentation/dht_doc.md)
- [Routes and route execute](documentation/route.md)
- [Local API](documentation/api.md)
- [Social PoC](documentation/poc.md)
- [Tests](documentation/tests/README.md)

## Status

This repository is a functional MVP and experimental prototype. It demonstrates
the main architecture and flows, but it should not be treated as production-ready
software.
