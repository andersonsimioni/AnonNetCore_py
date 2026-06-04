# Tests

The integration tests are organized as smoke flows that validate the MVP from
the simplest core functionality to the external social PoC.

Run all official smokes:

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py 10
```

The smoke runner starts a Docker cluster, starts local cores as needed, captures
logs under `data/local/smoke-runs`, and prints a compact summary.

Core full flow validates:

- local physical identity;
- bootstrap and peer discovery;
- physical sessions;
- DHT publication/query;
- local virtual-node creation;
- automatic virtual route publication;
- virtual session establishment;
- virtual message delivery;
- virtual content download.

Social full flow validates:

- two social profiles backed by local virtual nodes;
- profile publication through DDT and signed DPT;
- friend resolution through DPT/DDT;
- remote profile reading;
- direct message exchange through virtual sessions.
