# Bootstrap

Camada inicial para entrada na rede antes do peer discovery normal.

## Objetivo

- carregar DNS seeds hardcoded
- carregar endpoints publicos hardcoded
- entregar alvos iniciais para a engine tentar conexao

## Estrutura

- `config.py`
  - seeds e endpoints hardcoded
- `models.py`
  - modelos de bootstrap
- `dns_seed_resolver.py`
  - resolver inicial dos DNS seeds
- `service.py`
  - servico principal de bootstrap

## Uso basico

```python
from bootstrap import BootstrapService

service = BootstrapService()
targets = await service.list_bootstrap_endpoints()
```
