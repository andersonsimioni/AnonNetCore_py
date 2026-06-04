# Core

`app/core` contains the engine, configuration, envelope models, protocol
handlers, protocol clients, routing strategies, and runtimes that drive the
network.

Main areas:

- `config.py`: central runtime configuration.
- `engine.py`: packet decoding, routing, dispatch, encryption restoration, and
  runtime startup.
- `protocols/physical`: physical-layer handlers.
- `protocols/virtual`: virtual-layer handlers.
- `protocol_clients`: high-level clients used by runtimes, API, and tests.
- `routing_strategies`: route-build strategy implementations.
- `runtime`: recurring background jobs for bootstrap, DHT, sessions, routes,
  and validation.

Startup flow:

1. Load `CoreConfig`.
2. Prepare logging, storage, identity, DHT, sessions, routes, and transports.
3. Start public listeners when enabled.
4. Start API and WebSocket servers when enabled.
5. Start runtimes for bootstrap, DHT, physical sessions, virtual sessions, and
   virtual route maintenance.
