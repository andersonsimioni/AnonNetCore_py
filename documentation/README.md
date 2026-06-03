# AnonNetCore Python MVP - Documentacao

Esta pasta documenta o MVP funcional do AnonNetCore em Python. O objetivo dos
documentos e servir como base tecnica para o TCC e como guia de manutencao do
codigo.

## Documentos principais

- [Visao tecnica geral](documentation.md): objetivo, arquitetura, camadas,
  modulos, fluxo de execucao e definicao do MVP.
- [Entidades e persistencia](entities.md): entidades locais, tabelas SQLite,
  estado em memoria e registros distribuidos.
- [DHT e tabelas distribuidas](dht_doc.md): modelo de chave, namespaces,
  responsabilidade por proximidade XOR, replicacao e manutencao.
- [Rotas e route execute](route.md): construcao de rotas, publicacao na DRT,
  encapsulamento fisico/virtual e encaminhamento hop-by-hop.
- [Falhas e limites](faults.md): riscos conhecidos e comportamento esperado em
  falhas.
- [API local](api.md): endpoints HTTP, WebSocket e uso por apps externas.
- [PoC social](poc.md): rede social de demonstracao, DPT/DDT, perfis, feed e
  mensagens diretas.
- [Testes](tests/README.md): smokes, testes de integracao e comandos.

## Estado atual do MVP

O MVP demonstra:

- rede de physical nodes via TCP;
- bootstrap por endpoints hardcoded na configuracao do core;
- troca e validacao de informacoes de physical nodes;
- sessoes fisicas com KEM, assinatura e keepalive;
- DHT generica com namespaces `dpnt`, `drt`, `ddt`, `dpt` e parser para `dtt`;
- publicacao replicada nos K physical nodes mais proximos da chave;
- manutencao periodica de registros DHT validados;
- criacao automatica de rotas para virtual nodes locais;
- sessoes virtuais end-to-end usando rotas publicadas na DRT;
- mensagens virtuais entregues por `VIRTUAL_SESSION_DATA`;
- transferencia de conteudo por byte ranges no layer virtual;
- API HTTP e WebSocket para aplicacoes externas;
- PoC social em HTML/JS local sem servidor web obrigatorio;
- Debug Console para observar nodes locais e containers Docker.

## Como executar a demo

Na raiz do projeto:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

Sem abrir o navegador automaticamente:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10 --no-open
```

O `run_poc.py` tambem sobe o Debug Console.

## Observacao para o TCC

Este projeto e um MVP arquitetural. Ele prova o funcionamento do desenho de
rede, mas ainda nao e uma implementacao de producao. Aspectos como protecao
contra abuso, escalabilidade real em Internet publica, hardening criptografico,
controle de recursos e auditoria formal ainda sao trabalhos futuros.
