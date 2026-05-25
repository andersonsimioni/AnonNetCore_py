# Testes e Smokes

Os testes do projeto sao orientados a fluxo real. Eles sobem cluster Docker,
cores locais, API HTTP/WebSocket e exercitam os protocolos principais.

## Estrutura

- `tests/integration/`: smokes de integracao do core.
- `poc/smokes/`: smokes JavaScript da PoC social.
- `scripts/`: scripts para subir cluster, core local, PoC e debug console.

## Testes Principais

### `virtual_session_smoke.py`

Valida:

- cluster fisico minimo;
- criacao de VNs locais;
- rotas criadas pelo `VirtualRouteMaintenanceRuntime`;
- publicacao DRT;
- estabelecimento de sessao virtual;
- keepalive virtual.

### `virtual_message_smoke.py`

Valida:

- sessao virtual ativa;
- envio de mensagem por `VIRTUAL_SESSION_DATA`;
- entrega no handler registrado no `SessionManager`;
- resposta da mensagem.

### `virtual_content_smoke.py`

Valida:

- armazenamento local de conteudo;
- anuncio DDT;
- abertura de sessao virtual com holder;
- `VIRTUAL_CONTENT_INFO_REQUEST`;
- download por byte ranges;
- publicacao de holder DDT ao concluir download.

### `virtual_api_stress_smoke.py`

Valida o core pela API local com carga maior:

- criacao de VNs pela API;
- resolucao DRT;
- sessoes virtuais;
- mensagens;
- repeticao e aleatoriedade para encontrar bugs de estado.

### `virtual_api_local_vn_stress_smoke.py`

Valida o caso de dois VNs no mesmo core se comunicando pelo caminho padrao. O
teste nao deve usar atalho por serem VNs locais; ele passa por DRT/DPNT/rota
como uma comunicacao normal.

### `debug_state_smoke.py`

Valida sanidade operacional:

- nodes ativos;
- peers unicos;
- limite de sessoes;
- ausencia de duplicidade excessiva em DHT;
- snapshots do debug state.

### `core_full_flow_smoke.py`

Executa os smokes principais em ordem, reaproveitando cluster e cores quando
possivel.

### `poc/smokes/social_dom.js`

Smoke JS para funcoes do front-end social. Usa DOM fake para testar os handlers
que tambem sao usados pela pagina.

### `scripts/run_social_smoke.py`

Smoke integrado da PoC social com core real. Valida fluxo social basico por API:

- criacao de perfil/VN;
- publicacao de estado via DPT/DDT;
- amizade entre VNs;
- leitura de perfil remoto;
- feed;
- mensagem direta.

## Comandos Uteis

Rodar PoC com cluster:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

Rodar PoC sem abrir navegador:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10 --no-open
```

Rodar PoC com debug console:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc_debug.py 10
```

Rodar cluster:

```powershell
.\.venv\Scripts\python.exe scripts\run_cluster.py 10
```

Rodar core local:

```powershell
.\.venv\Scripts\python.exe scripts\run_local_core.py
```

Rodar smoke full flow:

```powershell
.\.venv\Scripts\python.exe tests\integration\core_full_flow_smoke.py
```

Rodar stress de API local com dois VNs:

```powershell
.\.venv\Scripts\python.exe tests\integration\virtual_api_local_vn_stress_smoke.py
```

## Observabilidade

Durante testes, usar:

- logs em `data/local/logs` e `cluster/logs`;
- endpoint `/debug/state`;
- `scripts/debug_console.py`;
- eventos WebSocket em `ws://127.0.0.1:18081/v1/events`.

O Debug Console foi criado para facilitar a leitura de peers, sessoes, DHT,
rotas e problemas detectados em tempo real.
