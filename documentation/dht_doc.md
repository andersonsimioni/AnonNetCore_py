# DHT e Tabelas Distribuidas

Este documento descreve a DHT implementada no MVP. A DHT e usada como base para
descoberta de physical nodes, rotas de virtual nodes e localizacao de conteudo.

## Objetivo

A DHT fornece um armazenamento distribuido simples, replicado e enderecado por
hash. Cada registro possui:

- `namespace`: tipo logico do registro.
- `logical_key`: chave legivel dentro daquele namespace.
- `key`: hash usado para roteamento e responsabilidade.
- `record_json`: payload canonico.

Regra:

```text
key = SHA512(namespace + "|" + logical_key)
```

Exemplo:

```text
namespace = dpnt
logical_key = physical_node_id
key = SHA512("dpnt|physical_node_id")
```

## Responsabilidade por Proximidade

Cada chave e responsabilidade dos `K` physical nodes mais proximos dela.

O MVP usa distancia XOR:

```text
distance = XOR(physical_node_id, dht_key)
```

Os `K` menores valores sao os responsaveis. O valor padrao de `K` vem de:

```text
CoreConfig.dht_replication_factor
```

Atualmente o padrao e `3`.

## Namespaces

### `dpnt`

Distributed Physical Nodes Table.

Serve para localizar physical nodes por ID. O valor contem chave publica,
endpoints anunciados, capacidades, status e assinatura.

Logical key:

```text
physical_node_id
```

### `drt`

Distributed Route Table.

Serve para localizar entry points fisicos que aceitam entregar trafego para um
virtual node.

Logical key:

```text
virtual_node_id
```

### `ddt`

Distributed Data Table.

Serve para localizar VNs que possuem um conteudo.

Logical key:

```text
content_id
```

### `dpt`

Distributed Pointer Table.

Serve para publicar um ponteiro mutavel assinado por um VN. Na PoC social, a
DPT aponta para o arquivo de estado mais recente do perfil.

Logical key da PoC:

```text
anonnet.social|virtual_node_id
```

### `dtt`

Distributed Tag Table.

Modelo previsto para associar tags a recursos. O parser e o merge existem no
codigo, mas a PoC social atual nao depende desse namespace.

## Publicacao

Fluxo de publicacao:

1. O cliente calcula `key = SHA512(namespace + "|" + logical_key)`.
2. O cliente seleciona candidatos conhecidos proximos da chave.
3. A primeira requisicao pode ser enviada para um peer aleatorio entre
   candidatos para reduzir correlacao direta.
4. O handler `DHT_PUBLISH` encaminha hop-by-hop para peers mais proximos.
5. Cada responsavel valida e salva o registro localmente.
6. A resposta informa `stored_by`.
7. O publish so e considerado armazenado quando os `K` responsaveis necessarios
   confirmam armazenamento.

O payload de publish nao carrega origin node, visited nodes ou hop count. Isso
evita expor quem iniciou a operacao para todos os hops.

## Consulta

Fluxo de consulta:

1. O cliente calcula a chave DHT.
2. Envia `DHT_QUERY` para um peer inicial.
3. O peer consulta registro local e encaminha para candidatos mais proximos.
4. Resultados encontrados retornam como `DHT_RESULT`.
5. O cliente recebe os registros e valida o payload conforme o namespace.

Assim como no publish, a query e encaminhada sem carregar o origin node. O
primeiro peer solicitado fica responsavel por coordenar a busca encadeada.

## Merge e Deduplicacao

A tabela local `dht_record` e unica. Quando um registro de mesma `key` chega, o
core tenta mesclar o fragmento com o registro existente.

Regras gerais:

- DPNT: aceita descritor valido do physical node correspondente.
- DRT: agrega entradas de rota do mesmo VN.
- DDT: agrega holders do mesmo conteudo.
- DPT: aceita ponteiro valido assinado pelo VN dono, preferindo o estado mais
  recente.
- DTT: agrega entradas de tag.

O merge tambem evita duplicidade de informacoes repetidas, como holders DDT e
route entries DRT.

## Manutencao

O `DhtMaintenanceRuntime` roda periodicamente para:

- validar registros locais;
- identificar os `K` responsaveis atuais de cada chave;
- republicar registros quando o node local ainda deve contribuir;
- mover responsabilidade quando novos peers passam a ser mais proximos;
- evitar republicacao excessiva usando backoff.

Configuracoes principais:

```text
CoreConfig.dht_maintenance_runtime_interval_seconds
CoreConfig.dht_maintenance_publish_backoff_seconds
CoreConfig.dht_replication_factor
```

## Publicacao de Physical Nodes na DPNT

Physical nodes nao sao publicados como confiaveis imediatamente. O fluxo
esperado e:

1. peer e descoberto por bootstrap ou exchange;
2. endpoint anunciado e salvo;
3. `PhysicalNodeValidationRuntime` tenta validar conexao direta;
4. quando validado, o core monta um registro DPNT;
5. o registro DPNT e publicado na DHT.

Isso separa descoberta local de publicacao distribuida.

## Publicacao de Rotas na DRT

Rotas sao publicadas pelo processo de route build. A DRT so deve receber uma
rota quando:

- o VN aceitou o `final_path_id`;
- o PN final aceitou ser entry point;
- assinaturas foram validadas;
- RTT da rota foi medido e assinado;
- o registro DRT foi montado pelo `RouteService`.

## Limites Atuais

- O modelo e adequado ao MVP e a testes em LAN/Docker.
- Nao ha protecao completa contra peers maliciosos coordenados.
- Nao ha sistema de reputacao.
- Nao ha shard fisico implementado para listas gigantes. A ideia de root/shards
  continua sendo um caminho futuro para DDT/DTT em escala maior.
