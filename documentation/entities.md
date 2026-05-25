# Entidades e Persistencia

Este documento descreve o modelo de dados atual do AnonNetCore Python MVP. A
persistencia local usa SQLite via SQLAlchemy. Estado temporario de rede, como
sockets abertos, segredos de sessao ativos e handshakes em andamento, permanece
em memoria.

## Regra Geral de Identidade

Tanto physical nodes quanto virtual nodes sao identificados pelo hash da chave
publica:

```text
node_id = SHA512(public_key)
```

Essa regra reduz o tamanho das referencias usadas no banco, nas DHTs e nos
protocolos. A chave publica completa continua existindo quando e necessario
validar assinatura ou reconstruir a identidade.

## Identidades Locais

### `local_physical_node_identity`

Representa a identidade fisica do processo local.

Campos principais:

- `id`: SHA-512 da chave publica fisica.
- `public_key`: chave publica ML-DSA.
- `private_key_encrypted`: chave privada local. No MVP, o campo ainda pode
  conter o material bruto, apesar do nome historico.
- `key_algorithm`: algoritmo usado pela identidade.
- `status`: estado operacional.
- `created_at` e `updated_at`.

### `local_virtual_node_identity`

Representa um virtual node hospedado pelo physical node local.

Campos principais:

- `id`: SHA-512 da chave publica virtual.
- `public_key`: chave publica do VN.
- `private_key_encrypted`: chave privada local do VN.
- `kind`: tipo logico do VN.
- `owner_physical_node_id`: PN local que hospeda esse VN.
- `expires_at`: expiracao opcional.
- `is_active`: indica se o VN participa dos runtimes.
- `metadata_json`: dados auxiliares de aplicacao.

No PoC social, a regra pratica e:

```text
1 virtual node = 1 perfil de usuario
```

## Identidades Remotas

### `remote_physical_node_identity`

Representa um physical node conhecido pelo core local.

Campos principais:

- `id`: SHA-512 da chave publica fisica remota.
- `public_key`: chave publica remota.
- `reachability_class`: classificacao de alcance.
- `relay_capable` e `hole_punch_capable`: capacidades anunciadas.
- `protocol_version`: versao do protocolo.
- `status`: estado conhecido.
- `last_seen_at` e `last_validated_at`.
- `score`: pontuacao operacional local.
- `notes_json`: metadados auxiliares.

### `node_endpoint`

Endpoint conhecido de um physical node remoto.

Campos principais:

- `physical_node_hash_id`: PN dono do endpoint.
- `transport`: `tcp` no MVP atual.
- `host` e `port`: endereco de listener anunciado pelo peer.
- `priority`: preferencia local.
- `is_active`: se o endpoint ainda deve ser usado.
- `last_success_at`, `last_failure_at` e `failure_count`.
- `metadata_json`.

Importante: endpoints salvos devem representar o host/porta de listener do peer,
nao a porta efemera criada por uma conexao TCP recebida.

### `remote_virtual_node_identity`

Cache local de virtual nodes remotos conhecidos.

Campos principais:

- `id`: SHA-512 da chave publica virtual remota.
- `public_key`: chave publica do VN remoto.
- `kind`: tipo logico.
- `first_seen_at`, `last_seen_at` e `expires_at`.
- `status`.
- `metadata_json`.

O VN remoto pode ser conhecido por cadastro externo ou pelo proprio handshake de
sessao virtual. No `VIRTUAL_SESSION_INIT`, o iniciador envia sua public key, e o
recebedor valida:

```text
SHA512(public_key_recebida) == virtual_node_id_informado
```

## Estado Distribuido Local

### `dht_record`

Tabela generica para registros DHT locais.

Campos principais:

- `key`: chave fisica DHT calculada.
- `namespace`: namespace logico (`dpnt`, `drt`, `ddt`, `dpt`, `dtt`).
- `logical_key`: chave logica antes do hash.
- `record_json`: payload canonico em JSON.
- `source`: origem local do registro.
- `last_validated_at`: ultima validacao local.
- `expires_at`: expiracao opcional.
- `created_at` e `updated_at`.

Regra de chave:

```text
key = SHA512(namespace + "|" + logical_key)
```

Os payloads suportados ficam em `app/dht/records.py`.

## Payloads DHT

### DPNT - Distributed Physical Nodes Table

Usada para localizar physical nodes por `physical_node_id`.

Logical key:

```text
physical_node_id
```

Payload:

- `pk_physical_node`
- `endpoints`
- `transport_methods`
- `reachability_class`
- `relay_capable`
- `hole_punch_capable`
- `protocol_version`
- `feature_flags`
- `last_validated_at`
- `status`
- `signature`

### DRT - Distributed Route Table

Usada para localizar entry points fisicos capazes de entregar trafego a um
virtual node.

Logical key:

```text
virtual_node_id
```

Payload:

- `pk_virtual_node`
- `route_entries`
- `last_update`

Cada `route_entry` contem:

- `pk_physical_node`
- `virtual_node_signature`
- `final_path_id`
- `entry_point_virtual_node_signature`
- `entry_point_physical_node_signature`
- `physical_node_signature`
- `expires_at`
- `rtt`
- `rtt_physical_node_signature`

Essas assinaturas provam que o VN e o PN final aceitaram publicar aquele
`final_path_id`, incluindo a informacao de RTT medida na rota.

### DDT - Distributed Data Table

Usada para anunciar quais VNs possuem determinado conteudo.

Logical key:

```text
content_id
```

Payload:

- `title`
- `type`
- `tags`
- `holders`

Cada holder contem:

- `pk_virtual_node`
- `expires_at`
- `signature`

Quando um download e finalizado, o VN que baixou o arquivo tambem pode publicar
um novo holder na DDT.

### DPT - Distributed Pointer Table

Usada para apontar uma chave mutavel para um conteudo atual.

Payload:

- `pk_virtual_node_owner`
- `title`
- `type`
- `last_modified`
- `target_ref`
- `signature`

Na PoC social, a DPT aponta para o estado mais recente do perfil:

```text
namespace = dpt
logical_key = anonnet.social|virtual_node_id
target_ref = content_id do estado social mais recente
```

### DTT - Distributed Tag Table

Modelo previsto para tags distribuidas.

Payload:

- `entries`

Cada entrada contem:

- `resource_id`
- `pk_virtual_node`
- `created_at`
- `expires_at`
- `signature`

O parser e o merge existem no codigo, mas a PoC social atual nao depende da DTT.

## Rotas

### `route_resolution`

Tabela local que substitui estados separados de path mapping, endpoint e
iniciador. Cada hop salva apenas o mapeamento que ele precisa conhecer para uma
rota.

Campos principais:

- `local_role`: papel local (`initiator`, `hop`, `final_endpoint`).
- `route_strategy`: estrategia usada, como `random_walk_ttl`.
- `status`: estado da rota.
- `route_nonce`: nonce usado no proof of work.
- `initial_path_id`: path id inicial do VN.
- `route_path_id`: path id operacional no PN final.
- `received_path_id`: path id recebido pelo hop.
- `generated_path_id`: path id gerado pelo hop para o proximo trecho.
- `final_path_id`: path id final publicado na DRT.
- `from_physical_node_id` e `to_physical_node_id`.
- `previous_physical_node_id` e `first_hop_physical_node_id`.
- `local_virtual_node_id`.
- `final_physical_node_public_key`.
- assinaturas e material KEM da rota.
- `metadata_json`.

O `RouteService` centraliza criacao, busca e atualizacao dessas resolucoes.

## Sessoes

As sessoes ativas ficam em memoria no `SessionManager`. A mesma estrutura
representa sessoes fisicas hop-by-hop e sessoes virtuais end-to-end.

Campos principais em memoria:

- `session_id`
- `session_scope`: `physical` ou `virtual`.
- identidade local e remota.
- estado de handshake e sessao.
- algoritmos usados.
- material publico efemero.
- segredo compartilhado derivado.
- timestamps de atividade e keepalive.
- rota associada, quando existir.

O banco nao deve guardar sockets, objetos criptograficos ativos ou segredos
temporarios sem protecao.

## Conteudo Local

### `content_object`

Metadados de um conteudo salvo localmente.

Campos principais:

- `content_hash`: SHA-512 dos bytes do conteudo.
- `title`
- `content_type`
- `mime_type`
- `size_bytes`
- `storage_path`
- `is_encrypted`
- `encryption_scheme`
- `last_access_at`
- `is_deleted`

### `content_tag`

Tags locais associadas a conteudo.

### `content_advertisement`

Registra que um VN local anunciou possuir determinado conteudo na DDT.

Campos principais:

- `content_object_id`
- `advertiser_virtual_node_id`
- `published_in_ddt`
- `published_at`
- `expires_at`
- `is_active`

### `content_replica`

Controle local de replica e retencao.

## Entidades Operacionais

### `seen_hash`

Evita reprocessamento de mensagens, pacotes ou objetos ja vistos.

### `local_setting`

Configuracoes persistidas localmente.

### `local_event_log`

Eventos relevantes persistidos para auditoria/debug.

### `physical_node_info_exchange_state`

Controle local de cadencia da troca de informacoes entre peers fisicos.

### `rtt_info`

Estatisticas de RTT observadas para physical nodes remotos.

## Regras Praticas

- Persistir identidades, endpoints, conteudos, registros DHT e metadados de
  suporte.
- Manter em memoria sockets, handshakes ativos, buffers temporarios e segredos
  de sessao.
- Guardar payloads grandes como arquivos, deixando no banco apenas metadados.
- Usar IDs SHA-512 nas referencias distribuidas, evitando publicar chaves
  publicas completas como keys de DHT.
- Validar assinaturas no momento em que um registro DHT for aceito ou mesclado.
