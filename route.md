# Route Creation

Este documento descreve o modelo de criacao de rota da DEMO.

## Objetivo

A rota existe para permitir que um `virtual node` envie dados por uma sequencia
de `physical nodes` sem expor claramente:

- quem iniciou a rota
- qual e o `virtual node` de destino
- quais hops participaram da rota para observadores externos

Cada hop da rota atua apenas como um encaminhador fisico. O significado logico
da comunicacao continua protegido.

## Principios

- a criacao da rota acontece na camada `physical`
- cada hop conhece apenas o hop anterior e o proximo hop que ele mesmo escolher
- o `virtual node` de destino fica cifrado para o `final physical node`
- nao existe campo `next_physical_node` no payload trafegado
- nao existe diferenciacao visivel entre hop inicial e hop intermediario
- o controle de profundidade da rota e feito por `max_hops`
- a prova dos hops e acumulada por entradas cifradas independentes

## Payload Base de ROUTE_CREATE

O `ROUTE_CREATE` deve conter, no minimo:

- `pk_final_physical_node`
- `remaining_ttl_ms`
- `kem_ciphertext_for_final_physical_node`
- `path_id`
- `encrypted_payload_for_final_physical_node`
- `nonce`

## Significado dos Campos

### `pk_final_physical_node`

Identifica o `physical node` final da rota. Somente ele deve conseguir
recuperar o `pk_virtual_node` de destino.

### `remaining_ttl_ms`

Budget temporal restante da criacao da rota.

Cada hop reduz esse valor com base no custo estimado do salto antes de
encaminhar a mensagem.

### `kem_ciphertext_for_final_physical_node`

Ciphertext do `ML-KEM` gerado para o `pk_final_physical_node`.

Esse campo permite que o `final physical node` decapsule o segredo compartilhado
usado para abrir `encrypted_payload_for_final_physical_node`.

### `path_id`

Identificador local do trecho da rota. Esse valor muda a cada hop.

Exemplo:

- A conhece `path_id_1`
- B recebe `path_id_1`, traduz localmente e encaminha como `path_id_2`
- C recebe `path_id_2`

Assim, cada hop conhece apenas a traducao local do trecho anterior para o
seguinte.

### `encrypted_payload_for_final_physical_node`

Bloco cifrado para o `pk_final_physical_node`.

Os hops intermediarios nao conseguem abrir esse payload. Ele contem as
informacoes secretas que o `final physical node` precisa para aceitar ser entry
point da rota.

Esse campo deve ser decifrado usando a chave simetrica derivada do segredo
obtido por `kem_ciphertext_for_final_physical_node`.

Payload sugerido dentro desse bloco cifrado:

```json
{
  "final_path_id": "....",
  "virtual_node_signature": "...."
}
```

O `virtual_node_signature` e a assinatura do `VN` sobre o `final_path_id`.
Depois disso, o `final PN` assina o par:

- `virtual_node_signature`
- `final_path_id`

Isso amarra a aceitacao do `final PN` a um `entry point` especifico criado por
aquele `VN`.

### `nonce`

Inteiro usado como `proof of work` da criacao da rota.

O `nonce` nao entra dentro da serializacao canonica do payload base. O fluxo
correto e:

- serializar canonicamente os campos imutaveis da rota
- concatenar o `nonce` inteiro a esse bloco serializado
- calcular `SHA-512`
- verificar se a hash atende a dificuldade global da rede

Assim, todos os hops conseguem verificar a prova usando o mesmo material
imutavel, sem depender de campos remapeados no caminho.

## Fluxo de Criacao da Rota

### 1. Criacao inicial

O `virtual node` iniciador:

- escolhe o `pk_final_physical_node`
- gera um `final_path_id`
- assina o `final_path_id`
- cifra para o `final physical node`:
  - `final_path_id`
  - `virtual_node_signature`
- gera `nonce` do proof of work
- cria o primeiro `path_id`
- envia `ROUTE_CREATE` para o primeiro `physical node`

### 2. Recepcao por um hop

Ao receber `ROUTE_CREATE`, o hop:

- valida o formato do payload
- verifica o proof of work do `nonce`
- decide se ele proprio e o `final physical node`

### 3. Se o hop nao for o final

O hop intermediario:

- escolhe aleatoriamente o proximo `physical node`
- gera um novo `path_id` local
- salva o mapeamento:
  - `received_path_id`
  - `generated_path_id`
  - `from_physical_node_id`
  - `to_physical_node_id`
- reduz o `remaining_ttl_ms`
- encaminha um novo `ROUTE_CREATE`

Para o proximo hop, o pacote continua parecendo apenas um `ROUTE_CREATE`
normal. Nao existe um `ROUTE_CREATE_FORWARD`.

### 4. Se o hop for o final

O `final physical node`:

- decifra `encrypted_payload_for_final_physical_node`
- recupera:
  - `final_path_id`
  - `virtual_node_signature`
- faz controle anti-replay do `final_path_id`
- associa:
  - `received_path_id`
  - `final_path_id`
- assina o payload canonico:
  - `virtual_node_signature`
  - `final_path_id`
- decide se a rota pode ser aceita
- responde com `ROUTE_CREATE_RETURN`

## Validacao Final e Publicacao na DRT

A criacao da rota nao termina com a simples validacao do `final physical node`.

O fluxo correto da DEMO e:

- `VN -> hops -> final PN -> hops -> VN -> DHT`

### Etapa 1. Criacao hop-by-hop

O `virtual node` inicia o `ROUTE_CREATE` e o pedido percorre os hops fisicos
ate alcancar o `final physical node`.

### Etapa 2. Validacao pelo final physical node

O `final physical node` valida:

- `pk_final_physical_node`
- `encrypted_payload_for_final_physical_node`
- proof of work do `nonce`

Se tudo estiver correto, ele nao publica a rota diretamente na DHT.

Em vez disso, ele devolve um `ROUTE_CREATE_RETURN` pela propria rota de retorno.

### Etapa 3. Retorno ate o virtual node

O `ROUTE_CREATE_RETURN` percorre os hops no sentido inverso ate retornar ao
`virtual node` que iniciou o pedido.

### Etapa 4. Validacao pelo virtual node

Ao receber o `ROUTE_CREATE_RETURN`, o `virtual node` valida o resultado final da
rota.

Essa etapa existe porque:

- o `final physical node` valida a parte fisica da rota
- o `virtual node` valida a parte logica e a publicacao em seu proprio nome

### Etapa 5. Commit ou abort da rota

Depois da validacao do `virtual node`, o iniciador decide:

- enviar `ROUTE_CREATE_OK` se a rota foi aceita
- enviar `ROUTE_CREATE_FAIL` se a rota foi rejeitada

Essas mensagens tambem percorrem os hops da rota.

Cada hop usa essa etapa para:

- marcar a rota como valida e ativa
- ou invalidar e limpar o estado local da rota

### Etapa 6. Publicacao na DRT

Somente depois da validacao do `virtual node`, a nova rota pode ser publicada na
`DRT`.

Assim, a publicacao da rota segue este principio:

- o `physical node` final confirma a viabilidade fisica do caminho
- o `virtual node` confirma a publicacao logica da rota

Isso evita que um `physical node` publique sozinho uma rota em nome de um
`virtual node`.

## Validacao da DRT

No entry point publicado na `DRT`, a ideia e guardar:

- `final_path_id`
- `virtual_node_signature`
- `final_physical_node_signature`

As validacoes ficam assim:

- o `VN` prova que criou aquele `final_path_id`
- o `final PN` prova que aceitou aquele `final_path_id` especifico daquele `VN`
- o `final PN` tambem impede replay localmente usando cache de `final_path_id`

## Message Types da Rota

Na DEMO, a familia de `routing` fica assim:

- `ROUTE_CREATE`
- `ROUTE_CREATE_RETURN`
- `ROUTE_CREATE_OK`
- `ROUTE_CREATE_FAIL`
- `ROUTE_DATA`
- `ROUTE_DATA_ACK`
- `ROUTE_KEEPALIVE`
- `ROUTE_KEEPALIVE_ACK`
- `ROUTE_CLOSE`

## Route Strategies

A DEMO suporta a ideia de multiplas estrategias de composicao de rota.

O protocolo de `routing` continua unico. O que muda e a estrategia usada para
montar, encaminhar e validar a rota.

Exemplos de estrategias:

- `random_walk_ttl_based`
- `random_walk_max_hop_based`
- `onion_like_based`

Cada cliente pode escolher a estrategia que considerar mais adequada para o
caso de uso.

## Random Walk by TTL

Essa estrategia usa um budget temporal aproximado para a criacao da rota.

### Principio

- o `virtual node` iniciador escolhe um `original_ttl_ms`
- esse valor fica apenas no estado local do `virtual node`
- o payload da rota carrega apenas `remaining_ttl_ms`

### Fluxo

1. o `virtual node` inicia a rota e guarda localmente:
   - `original_ttl_ms`
   - instante de inicio da criacao
2. o `ROUTE_CREATE` trafega apenas com `remaining_ttl_ms`
3. cada hop mede seu custo local
4. cada hop reduz `remaining_ttl_ms`
5. se o budget acabar, a criacao da rota falha
6. no final, o `virtual node` valida a rota comparando:
   - o tempo total observado
   - o `original_ttl_ms` que ele guardou localmente

### Observacao

O `original_ttl_ms` nao aparece no payload. Ele existe apenas no estado local do
`virtual node` iniciador.

## Random Walk by Max Hop

Essa estrategia usa um limite de hops e um historico cifrado de provas do
caminho percorrido.

### Principio

- o `virtual node` define um `max_hops`
- cada hop acrescenta sua propria prova cifrada ao payload
- no final, o `virtual node` valida o caminho completo

### Fluxo

1. o `virtual node` inicia `ROUTE_CREATE` com:
   - `max_hops`
   - `route_public_key`
   - `chl_entries`
2. cada hop:
   - escolhe aleatoriamente o proximo `physical node`
   - reduz o contador de hops restantes
   - remapeia `path_id`
3. o `final physical node` valida a parte fisica do caminho
4. a rota retorna ao `virtual node`
5. o `virtual node` valida o caminho completo

### Objetivo

Essa estrategia favorece auditabilidade do caminho e controle da profundidade
maxima da rota.

## Onion Like

Essa estrategia segue um modelo inspirado em TOR.

### Principio

- o `virtual node` iniciador escolhe exatamente por quais `physical nodes` a
  rota vai passar
- a rota e montada em camadas cifradas
- cada hop conhece apenas:
  - o hop anterior
  - o proximo hop

### Fluxo

1. o `virtual node` escolhe toda a sequencia de `physical nodes`
2. ele cifra a rota em camadas
3. cada hop remove apenas a camada que lhe pertence
4. cada hop descobre apenas:
   - a informacao necessaria para encaminhar ao proximo
   - o `path_id` local do trecho
5. o ultimo hop recupera apenas a parte final da rota

### Objetivo

Essa estrategia favorece previsibilidade do caminho e isolamento de
conhecimento entre os hops.

## Resumo

A criacao da rota e um processo fisico hop-by-hop.

O pedido original carrega apenas os dados publicos imutaveis e um bloco cifrado
para o `final physical node`.
Os hops intermediaros nao descobrem o `final_path_id` real da rota.
O `final physical node` decifra esse valor, valida anti-replay e assina a prova
de aceitacao do entry point.

Depois disso, o `final physical node` envia `ROUTE_CREATE_RETURN` de volta ao
`virtual node` iniciador. O `virtual node` valida o resultado final, envia
`ROUTE_CREATE_OK` ou `ROUTE_CREATE_FAIL` para comitar ou abortar a rota nos
hops, e so entao publica a nova rota na `DRT`.

Dependendo da estrategia escolhida, essa validacao final pode priorizar:

- budget temporal
- numero maximo de hops
- caminho onion predefinido
