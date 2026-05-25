# Core

O diretorio `app/core` concentra a engine, configuracao, modelos de envelope,
registro de protocolos, clients de protocolo e runtimes.

## Responsabilidades

- iniciar o ambiente de execucao;
- configurar listener TCP, logs, API e WebSocket;
- executar bootstrap;
- solicitar informacoes iniciais de physical nodes;
- iniciar runtimes;
- receber envelopes de protocolo;
- encaminhar cada envelope ao handler correto;
- expor services para protocolos e API.

## Arquivos Principais

- `config.py`: configuracoes globais do core.
- `engine.py`: ciclo de vida principal.
- `models.py`: `PacketContext`, `ProtocolEnvelope` e resultados.
- `message_registry.py`: roteamento de message types para handlers.
- `services.py`: agregacao dos services usados pela engine.
- `network.py`: utilitarios de rede local.
- `protocols/physical/`: handlers da camada fisica.
- `protocols/virtual/`: handlers da camada virtual.
- `protocol_clients/physical/`: clients que iniciam protocolos fisicos.
- `protocol_clients/virtual/`: clients que iniciam protocolos virtuais.
- `runtime/`: tarefas periodicas.
- `routing_strategies/`: estrategias de criacao de rotas.

## Ciclo de Vida

1. `main.py` instancia o core.
2. `CoreEngine.start()` configura o ambiente.
3. O listener TCP e iniciado.
4. A API HTTP/WebSocket e iniciada quando habilitada.
5. Bootstrap carrega endpoints hardcoded.
6. O core solicita `PHYSICAL_NODE_INFO` aos bootstraps.
7. Runtimes fisicos, DHT, sessao e rotas virtuais sao iniciados.
8. Envelopes recebidos sao processados pelo registry.

## Runtimes

- `PhysicalPingRuntime`: ping e RTT de physical nodes.
- `PhysicalNodeValidationRuntime`: valida peers e endpoints.
- `PhysicalNodeInfoExchangeRuntime`: troca peers conhecidos.
- `SessionRuntime`: keepalive de sessoes fisicas e virtuais.
- `DhtMaintenanceRuntime`: manutencao e republicacao de DHT.
- `VirtualRouteMaintenanceRuntime`: garante rotas minimas para VNs locais.

## Protocolos

Fisicos:

- ping;
- physical node info;
- physical node info exchange;
- physical session;
- DHT;
- route build;
- route execute.

Virtuais:

- virtual session;
- virtual message;
- virtual content.
