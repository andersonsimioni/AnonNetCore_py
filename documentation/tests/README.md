# Testes e Smokes

Os smokes oficiais foram reduzidos para dois fluxos completos. A ideia e evitar
validacoes fragmentadas e redundantes: um smoke valida o core como produto, e o
outro valida a PoC social usando o core real.

## Estrutura

- `tests/integration/core_full_flow_smoke.py`: fluxo completo do core.
- `tests/integration/poc_full_flow_smoke.py`: fluxo completo da PoC social.
- `tests/support/core_flow_checks.py`: validacoes reutilizadas pelo core full flow.
- `tests/support/core_helpers.py`: cria cores isolados para integracao.
- `tests/support/smoke_helpers.py`: helpers de cluster, DRT, rotas e sessoes.
- `tests/support/smokes_config.py`: parametros centralizados dos smokes.
- `tests/support/poc_flow_runner.py`: fluxo Python que executa a PoC social.
- `poc/smokes/social_flow.js`: fluxo social executado pelo PoC full flow.
- `poc/smokes/social_dom.js`: smoke JS manual para o DOM do front-end.

## Smokes Oficiais

### `core_full_flow_smoke.py`

Valida o core em fluxo real:

- cluster Docker;
- descoberta e maturidade de peers fisicos;
- criacao de VNs locais;
- rotas criadas pelo `VirtualRouteMaintenanceRuntime`;
- publicacao e descoberta DRT;
- estabelecimento de sessao virtual;
- keepalive virtual;
- troca de mensagem virtual;
- download de conteudo por protocolo virtual;
- publicacao DDT do holder ao concluir download.

### `poc_full_flow_smoke.py`

Valida a PoC social com core real e API local:

- cluster Docker;
- core HTTP/WebSocket;
- criacao de dois perfis/VNs sociais;
- publicacao de estado social por DPT/DDT;
- amizade entre VNs;
- leitura de perfil remoto;
- feed de posts;
- mensagem direta entre amigos.

## Comandos Uteis

Rodar todos os smokes oficiais:

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py 10
```

Listar plano de smokes:

```powershell
.\.venv\Scripts\python.exe scripts\run_smokes.py --list
```

Rodar apenas o core full flow:

```powershell
.\.venv\Scripts\python.exe tests\integration\core_full_flow_smoke.py --cluster-nodes 10
```

Rodar apenas o PoC full flow:

```powershell
.\.venv\Scripts\python.exe tests\integration\poc_full_flow_smoke.py 10
```

Rodar PoC com cluster:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

## Observabilidade

Durante testes, usar:

- logs em `data/local/logs` e `cluster/logs`;
- endpoint `/debug/state`;
- Debug Console iniciado por `scripts/run_poc.py`;
- eventos WebSocket em `ws://127.0.0.1:18081/v1/events`.
