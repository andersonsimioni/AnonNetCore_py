# AnonNetCore Python MVP

AnonNetCore Python MVP e um prototipo funcional de rede P2P com camada fisica,
camada virtual, DHT, rotas, sessoes virtuais, transferencia de conteudo e uma
PoC social em HTML/JS.

O projeto foi construido para validar arquitetura e fluxos principais antes de
uma implementacao mais performatica.

## Execucao Rapida

Instale dependencias no ambiente virtual e execute:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

Com Debug Console:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc_debug.py 10
```

Rodar apenas o core local:

```powershell
.\.venv\Scripts\python.exe scripts\run_local_core.py
```

Rodar cluster Docker:

```powershell
.\.venv\Scripts\python.exe scripts\run_cluster.py 10
```

## Documentacao

A documentacao tecnica fica em [documentation/README.md](documentation/README.md).

Pontos principais:

- [Visao tecnica geral](documentation/documentation.md)
- [Entidades e persistencia](documentation/entities.md)
- [DHT](documentation/dht_doc.md)
- [Rotas e route execute](documentation/route.md)
- [API local](documentation/api.md)
- [PoC social](documentation/poc.md)
- [Testes](documentation/tests/README.md)

## Estado

Este repositorio representa um MVP funcional/prototipo experimental. Ele ja
demonstra os fluxos principais, mas ainda nao deve ser tratado como
implementacao pronta para producao.
