# Bootstrap

O bootstrap fornece os primeiros endpoints conhecidos da rede. Ele nao cria uma
rede separada: os endpoints apontam para physical nodes normais que tambem
escutam TCP.

## Configuracao

Os endpoints padrao nascem em:

```text
CoreConfig.bootstrap_public_endpoints
```

O host e escolhido nesta ordem:

1. `ANONNET_BOOTSTRAP_HOST`;
2. `ANONNET_ADVERTISED_TCP_HOST`;
3. `detect_local_network_host()`.

Portas padrao:

```text
19001
19002
```

## Fluxo

1. Engine carrega endpoints.
2. Remove endpoints que apontam para o proprio node local.
3. Aguarda `bootstrap_warmup_seconds`.
4. Envia `PHYSICAL_NODE_INFO_REQUEST`.
5. Salva peers retornados.
6. Os runtimes passam a validar e trocar peers.

## Modulos

- `models.py`: `BootstrapEndpoint`, `DnsSeed` e resultado de resolucao.
- `dns_seed_resolver.py`: base para DNS seeds.
- `service.py`: carrega e normaliza endpoints.

No MVP atual, DNS seeds existem como modelo, mas a demo usa principalmente
endpoints hardcoded.
