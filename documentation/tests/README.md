# Tests

The integration tests are organized as full smoke flows. They validate the MVP
through the same public scripts and APIs used during demos.

Run all official smokes:

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py 10
```

The smoke runner starts a Docker cluster, starts local cores as needed, captures
logs under `data/local/smoke-runs`, collects node warnings/errors through a
local HTTP collector, prints a compact summary, prints the randomized topology,
and stops the cluster at the end.

## Official Smokes

### Core full flow

`tests/integration/core_full_flow_smoke.py`

Validates:

- local physical identity;
- randomized cluster topology;
- bootstrap and peer discovery;
- physical sessions;
- DHT publication/query;
- local virtual-node creation;
- automatic virtual route publication;
- route execution through DRT/DPNT resolution;
- virtual session establishment;
- reliable virtual message delivery;
- virtual content download.

### PoC full flow

`tests/integration/poc_full_flow_smoke.py`

Validates:

- two social profiles backed by local virtual nodes;
- profile publication through DDT and signed DPT;
- friend resolution through DPT/DDT;
- remote profile reading;
- direct message exchange through virtual sessions;
- browser-facing social JavaScript flow reused by the PoC.

## Topology Evidence

Cluster topology is randomized for non-bootstrap nodes. Smokes print the drawn
profiles, reachability classes, direct transports, and relay capability so a
test run can be used as implementation evidence.
