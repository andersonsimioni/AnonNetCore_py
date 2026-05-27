# AnonNetCore Python MVP

## 1. Objetivo

O AnonNetCore Python MVP e um prototipo funcional de uma rede P2P com duas
camadas:

- camada fisica, formada por processos reais conectados por TCP;
- camada virtual, formada por identidades logicas hospedadas nesses processos.

O objetivo do MVP e validar a arquitetura antes de uma implementacao mais
performativa. O projeto demonstra descoberta de peers, sessoes seguras, DHT,
rotas para virtual nodes, mensagens virtuais, transferencia de conteudo e uma
PoC social.

## 2. Escopo do MVP

O MVP demonstra:

- bootstrap por endpoints conhecidos;
- descoberta e validacao de physical nodes;
- sessoes fisicas hop-by-hop;
- DHT replicada por proximidade XOR;
- publicacao DPNT, DRT, DDT e DPT;
- criacao automatica de rotas para VNs locais;
- sessao virtual end-to-end usando DRT/DPNT/ROUTE_DATA;
- mensagens virtuais;
- download de conteudo por byte ranges;
- API HTTP e WebSocket local;
- PoC social em HTML/JS local.

O MVP nao deve ser descrito como producao. Ele ainda nao possui hardening
completo contra abuso, adversarios coordenados, NAT complexo ou uso massivo em
Internet publica.

## 3. Premissas Tecnicas

- Transporte: TCP.
- Framing: frame TCP com prefixo de tamanho.
- Envelope de protocolo: JSON.
- Persistencia local: SQLite via SQLAlchemy.
- Conteudo local: filesystem.
- IDs: SHA-512 da chave publica.
- Assinatura: ML-DSA-65 via OpenSSL quando disponivel.
- KEM: ML-KEM-768 via OpenSSL quando disponivel.
- Cifra simetrica: AES-256-GCM-SIV.
- Testes de rede: processos locais e containers Docker.

## 4. Arquitetura

### 4.1 Camada Fisica

A camada fisica representa os processos reais da rede.

Responsabilidades:

- abrir listener TCP;
- enviar e receber frames;
- processar envelopes fisicos;
- validar peers;
- manter sessoes fisicas;
- trocar informacoes de physical nodes;
- publicar DPNT;
- responder DHT;
- construir e executar rotas.

Modulos principais:

- `app/transport/`
- `app/core/protocols/physical/`
- `app/core/protocol_clients/physical/`
- `app/core/runtime/`

### 4.2 Camada Virtual

A camada virtual representa identidades logicas que rodam sobre physical nodes.
Um physical node pode hospedar varios virtual nodes.

Responsabilidades:

- criar e manter VNs locais;
- publicar rotas para VNs locais na DRT;
- resolver VNs remotos pela DRT;
- estabelecer sessoes virtuais;
- entregar mensagens de aplicacao;
- transferir conteudo.

Modulos principais:

- `app/core/protocols/virtual/`
- `app/core/protocol_clients/virtual/`
- `app/sessions/`
- `app/content/`

### 4.3 Services

Os services concentram regras reutilizaveis.

Modulos principais:

- `app/identity/`
- `app/route/`
- `app/dht/`
- `app/content/`
- `app/api/`
- `app/debug/`

### 4.4 Aplicacao Externa

A PoC social fica fora do core e usa a API local.

Modulos principais:

- `poc/index.html`
- `poc/assets/js/`
- `poc/assets/css/`
- `poc/smokes/`

## 5. Identidades

### Physical Node

Physical node e a identidade do processo real.

```text
physical_node_id = SHA512(physical_public_key)
```

Ele participa da rede TCP, valida peers, hospeda VNs, encaminha pacotes e
publica DPNT quando validado.

### Virtual Node

Virtual node e uma identidade logica.

```text
virtual_node_id = SHA512(virtual_public_key)
```

No PoC social:

```text
1 VN = 1 perfil
```

O VN assina DPT/DDT, possui rotas publicadas na DRT, estabelece sessoes
virtuais e envia mensagens.

## 6. Bootstrap

O bootstrap e definido na configuracao do core:

```text
CoreConfig.bootstrap_public_endpoints
```

Por padrao, o core usa dois endpoints:

```text
host_detectado:19001
host_detectado:19002
```

O host vem de:

1. `ANONNET_BOOTSTRAP_HOST`;
2. `ANONNET_ADVERTISED_TCP_HOST`;
3. deteccao automatica da rede local.

Fluxo:

1. core inicia identidade fisica local;
2. abre listener em `0.0.0.0`;
3. carrega endpoints bootstrap;
4. ignora endpoints que apontam para ele mesmo;
5. envia `PHYSICAL_NODE_INFO_REQUEST`;
6. salva peers recebidos;
7. valida peers;
8. troca listas de peers;
9. publica peers validados na DPNT.

## 7. Protocolos Fisicos

### Ping

- `PING`
- `PONG`

Usado para validar conectividade e medir RTT.

### Physical Node Info

- `PHYSICAL_NODE_INFO_REQUEST`
- `PHYSICAL_NODE_INFO_RESPONSE`

Usado no bootstrap e em consultas diretas para obter chave publica, ID e
endpoints anunciados.

### Physical Node Info Exchange

- `PHYSICAL_NODE_INFO_EXCHANGE_REQUEST`
- `PHYSICAL_NODE_INFO_EXCHANGE_RESPONSE`
- `PHYSICAL_NODE_INFO_ANNOUNCE`

Usado para espalhar peers conhecidos sem depender somente do bootstrap.

### Physical Session

- `PHYSICAL_SESSION_INIT`
- `PHYSICAL_SESSION_INIT_OK`
- `PHYSICAL_SESSION_KEY_CONFIRM`
- `PHYSICAL_SESSION_READY`
- `PHYSICAL_SESSION_KEEPALIVE`
- `PHYSICAL_SESSION_KEEPALIVE_ACK`
- `PHYSICAL_SESSION_CLOSE`

Cria sessoes seguras entre physical nodes adjacentes.

### DHT

- `DHT_PUBLISH`
- `DHT_QUERY`
- `DHT_RESULT`

Fornece publicacao e consulta hop-by-hop para DPNT, DRT, DDT, DPT e DTT.

### Route Build

- `ROUTE_CREATE`
- `ROUTE_CREATE_KEM_INFO`
- `ROUTE_CREATE_VALIDATE_AND_PUBLISH`
- `ROUTE_CREATE_PING`
- `ROUTE_CREATE_PONG`
- `ROUTE_CREATE_OK`

Cria rotas fisicas para VNs.

### Route Execute

- `ROUTE_DATA`

Encaminha dados por uma rota ja criada.

## 8. DHT

A DHT usa a tabela local generica `dht_record`.

Regra de chave:

```text
key = SHA512(namespace + "|" + logical_key)
```

Namespaces:

- `dpnt`: physical nodes validados;
- `drt`: rotas publicadas para virtual nodes;
- `ddt`: holders de conteudo;
- `dpt`: ponteiros mutaveis assinados;
- `dtt`: modelo para tags, ainda fora do fluxo principal da PoC.

O publish e considerado bem-sucedido quando os `K` responsaveis armazenam o
registro. O valor de `K` vem de:

```text
CoreConfig.dht_replication_factor
```

## 9. Rotas

O runtime de manutencao de rotas cuida de todos os VNs locais:

```text
VirtualRouteMaintenanceRuntime
```

Ele tenta manter pelo menos:

```text
CoreConfig.virtual_route_maintenance_route_min_online_routes
```

rotas online na DRT para cada VN local ativo.

O fluxo principal usa a estrategia `random_walk_ttl`:

1. VN inicia criacao de rota.
2. Hops fisicos encaminham `ROUTE_CREATE`.
3. PN final envia informacao KEM.
4. VN envia validacao cifrada.
5. PN final valida assinatura do VN.
6. PN final executa ping/pong da rota.
7. PN final mede RTT.
8. PN final publica a rota na DRT.
9. PN final envia `ROUTE_CREATE_OK` ao VN.

## 10. Route Execute e Encapsulamento

Depois da rota criada, pacotes virtuais trafegam dentro de `ROUTE_DATA`.

Modelo:

```text
physical_envelope {
  message_type: ROUTE_DATA,
  payload: {
    path_id,
    direction,
    virtual_session_id,
    virtual_envelope_ciphered,
    virtual_envelope
  }
}
```

O handler:

1. resolve `path_id`;
2. decide se encaminha ou entrega localmente;
3. quando entrega localmente, reconstrui o envelope virtual;
4. dispara o envelope virtual no core;
5. se houver resposta virtual, encapsula novamente em `ROUTE_DATA`.

## 11. Sessoes Virtuais

Sessoes virtuais sao end-to-end entre VNs.

Message types:

- `VIRTUAL_SESSION_INIT`
- `VIRTUAL_SESSION_INIT_OK`
- `VIRTUAL_SESSION_KEY_CONFIRM`
- `VIRTUAL_SESSION_READY`
- `VIRTUAL_SESSION_KEEPALIVE`
- `VIRTUAL_SESSION_KEEPALIVE_ACK`
- `VIRTUAL_SESSION_CLOSE`

Fluxo:

1. iniciador consulta DRT do VN remoto;
2. escolhe entry point fisico;
3. resolve o PN na DPNT;
4. envia `VIRTUAL_SESSION_INIT` por `ROUTE_DATA`;
5. o recebedor valida a public key enviada no init;
6. ambos derivam segredo via KEM;
7. a sessao fica ativa;
8. `SessionRuntime` envia keepalives.

Mensagens de aplicacao usam:

```text
VIRTUAL_SESSION_DATA
```

## 12. Conteudo Virtual

O protocolo de conteudo roda na camada virtual.

Message types:

- `VIRTUAL_CONTENT_INFO_REQUEST`
- `VIRTUAL_CONTENT_INFO_RESPONSE`
- `VIRTUAL_CONTENT_RANGE_REQUEST`
- `VIRTUAL_CONTENT_RANGE_RESPONSE`
- `VIRTUAL_CONTENT_NOT_FOUND`
- `VIRTUAL_CONTENT_RANGE_DENIED`
- `VIRTUAL_CONTENT_RANGE_ERROR`

Fluxo de download:

1. app resolve DDT;
2. core abre sessao virtual com holder;
3. solicita metadados do conteudo;
4. baixa ranges de bytes;
5. salva arquivo completo;
6. publica na DDT que tambem virou holder.

## 13. API Local

HTTP:

```text
http://127.0.0.1:18080
```

WebSocket:

```text
ws://127.0.0.1:18081/v1/events
```

A API permite:

- criar/listar VNs;
- cadastrar VNs remotos;
- publicar/consultar DHT;
- iniciar sessoes virtuais;
- enviar mensagens;
- receber eventos;
- armazenar conteudo;
- iniciar downloads;
- consultar debug state.

## 14. PoC Social

A PoC social usa:

- VN como perfil;
- DPT como ponteiro para o estado atual;
- DDT para localizar holders do estado;
- DRT para encontrar entry points de VNs;
- sessao virtual para mensagens diretas.

Chave DPT:

```text
namespace = dpt
logical_key = anonnet.social|virtual_node_id
```

O `target_ref` aponta para o `content_id` do estado social mais recente.

## 15. Scripts

Rodar cluster:

```powershell
.\.venv\Scripts\python.exe scripts\run_cluster.py 10
```

Rodar core local:

```powershell
.\.venv\Scripts\python.exe scripts\run_local_core.py
```

Rodar PoC:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

Rodar PoC com debug:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc_debug.py 10
```

## 16. Observabilidade

O projeto possui:

- logs por node;
- endpoint `/debug/state`;
- Debug Console;
- smokes de integracao;
- eventos WebSocket.

O Debug Console mostra peers, sessoes, DHT, rotas, runtimes e problemas
detectados.

## 17. Definicao de MVP Funcional

O MVP e considerado funcional porque demonstra:

- rede fisica multi-node;
- bootstrap e peer exchange;
- validacao de peers;
- sessoes fisicas;
- DHT replicada;
- rotas virtuais mantidas automaticamente;
- sessoes virtuais;
- mensagens virtuais;
- conteudo por byte ranges;
- API local;
- PoC social usando o core real.

## 18. Limitacoes

- Nao ha NAT traversal real.
- Nao ha QUIC.
- Nao ha reputacao, rate limit robusto ou protecao anti-abuso completa.
- Nao ha auditoria formal de seguranca.
- DTT esta modelada, mas nao e parte da PoC social.
- A PoC guarda foto como data URL para simplificar a demo.
- A rede ainda precisa de testes longos com churn, falhas e muitos nodes.
