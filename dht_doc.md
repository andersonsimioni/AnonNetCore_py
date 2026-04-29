# DHT

## Objetivo

A DHT do projeto usa uma tabela unica de registros e distribui a responsabilidade de cada chave entre os `K` peers ativos mais proximos dela.

Isso evita centralizacao e permite redundancia controlada.

## Chave DHT

Cada registro usa:

```text
key = SHA512(namespace || logical_key)
```

Onde:

- `namespace`: define o tipo do registro, por exemplo `drt`, `ddt`, `dtt`
- `logical_key`: identifica o recurso logico, por exemplo `file_hash`, `tag`, `node_id`

## Responsabilidade da chave

Para uma `key`, os `K` peers ativos mais proximos dela sao os responsaveis por armazenar e responder por esse registro.

A proximidade deve ser calculada por distancia `XOR` entre:

- `node_id`
- `dht_key`

Resumo:

```text
distance = XOR(node_id, dht_key)
```

Os `K` menores resultados sao os responsaveis.

## Problema dos registros colossais

Algumas chaves podem apontar para listas gigantes.

Exemplos:

- `DDT`: uma key pode apontar para milhares ou milhoes de holders de um arquivo
- `DTT`: uma key pode apontar para milhares ou milhoes de `file_hashes` de uma tag

Se tudo ficar em um unico registro, um node pode acabar armazenando volume demais para apenas uma key.

## Solucao: root + shards

A solucao adotada e dividir registros colossais em:

- `root record`
- `shard records`

### Root record

O root guarda apenas metadata leve do conjunto.

Chave:

```text
root_key = SHA512(namespace || logical_key)
```

Campos esperados no valor:

- `namespace`
- `logical_key`
- `shard_count`
- `updated_at`

### Shard record

Cada shard guarda apenas uma parte da lista total.

Chave:

```text
shard_key = SHA512(namespace || logical_key || shard_index)
```

Campos esperados no valor:

- `namespace`
- `logical_key`
- `shard_index`
- `items`
- `item_count`
- `updated_at`

## Regra de particionamento

O limite principal sera por quantidade de itens, nao por bytes.

Isso funciona bem porque os campos de texto relevantes terao limite de tamanho definido pela rede.

Exemplo:

- `max_items_per_shard = 10000`

Esse valor nao precisa ser armazenado no `root`.

Ele deve existir como configuracao global da rede, igual para todos os nodes.

Se um registro exceder esse limite:

- o shard atual e considerado cheio
- um novo shard e criado com `shard_index + 1`
- o root atualiza `shard_count`

## Exemplo

Para `DDT`:

```text
namespace = ddt
logical_key = file_hash
```

Estrutura:

```text
SHA512(ddt || file_hash) -> root
SHA512(ddt || file_hash || 0) -> shard 0
SHA512(ddt || file_hash || 1) -> shard 1
SHA512(ddt || file_hash || 2) -> shard 2
```

Para `DTT`:

```text
namespace = dtt
logical_key = tag
```

Estrutura:

```text
SHA512(dtt || tag) -> root
SHA512(dtt || tag || 0) -> shard 0
SHA512(dtt || tag || 1) -> shard 1
```

## Escrita

Fluxo esperado:

1. Ler o `root`
2. Verificar o ultimo shard
3. Se ainda houver espaco, adicionar o item nele
4. Se estiver cheio, criar um novo shard
5. Atualizar o `root` com o novo `shard_count`

## Leitura

Fluxo esperado:

1. Ler o `root`
2. Descobrir `shard_count`
3. Ler os shards de `0` ate `shard_count - 1`
4. Unir os itens em memoria

## Vantagens

- evita hotspot gigante em uma unica key
- limita o tamanho de cada registro
- distribui melhor a responsabilidade entre peers
- funciona bem com `DDT` e `DTT`
- mantem a DHT simples de entender

## Resumo

A DHT do projeto deve usar:

- uma `key` base para identificar o recurso logico
- `K` peers mais proximos como responsaveis
- um `root record` pequeno
- varios `shards` para listas grandes
- limite por quantidade de itens por shard

Essa abordagem permite escalar registros muito grandes sem transformar uma unica key em um ponto de sobrecarga.
