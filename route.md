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
- a identidade logica do `VN` nao aparece no `ROUTE_CREATE`
- o `final physical node` so recebe o material sensivel depois, em
  `ROUTE_CREATE_VALIDATE_AND_PUBLISH`
- nao existe campo `next_physical_node` no payload trafegado
- nao existe diferenciacao visivel entre hop inicial e hop intermediario
- o controle de profundidade da rota e feito por `max_hops`
- a prova dos hops e acumulada por entradas cifradas independentes

## Payload Base de ROUTE_CREATE

O `ROUTE_CREATE` deve conter, no minimo:

- `pk_final_physical_node`
- `remaining_ttl_ms`
- `path_id`
- `nonce`

## Significado dos Campos

### `pk_final_physical_node`

Identifica o `physical node` final da rota. Somente ele deve receber o
payload sensivel de validacao da rota.

### `remaining_ttl_ms`

Budget temporal restante da criacao da rota.

Cada hop reduz esse valor com base no custo estimado do salto antes de
encaminhar a mensagem.

### `path_id`

Identificador local do trecho da rota. Esse valor muda a cada hop.

Exemplo:

- A conhece `path_id_1`
- B recebe `path_id_1`, traduz localmente e encaminha como `path_id_2`
- C recebe `path_id_2`

Assim, cada hop conhece apenas a traducao local do trecho anterior para o
seguinte.

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

- decide se a rota pode ser aceita
- gera uma chave publica efemera de `ML-KEM`
- assina essa chave publica junto com o `path_id`
- devolve isso ao `VN` usando `ROUTE_CREATE_KEM_INFO`

## Validacao Final e Publicacao na DRT

A criacao da rota nao termina com a simples validacao do `final physical node`.

O fluxo correto da DEMO e:

- `VN -> hops -> final PN -> hops -> VN -> DHT`

### Etapa 1. Criacao hop-by-hop

O `virtual node` inicia o `ROUTE_CREATE` e o pedido percorre os hops fisicos
ate alcancar o `final physical node`.

### Etapa 2. Oferta de KEM pelo final physical node

O `final physical node` valida:

- `pk_final_physical_node`
- proof of work do `nonce`

Se tudo estiver correto, ele nao publica a rota diretamente na DHT.
Depois disso, ele devolve ao `VN` um `ROUTE_CREATE_KEM_INFO` contendo:

- `kyber_public_key_pem`
- `physical_node_signature`

O `path_id` continua existindo apenas como identificador operacional do retorno
hop-by-hop. Ele nao faz parte do conteudo semantico assinado do `KEM_INFO`.

### Etapa 3. Retorno ate o virtual node

O `ROUTE_CREATE_KEM_INFO` percorre os hops no sentido inverso ate retornar ao
`virtual node` que iniciou o pedido.

### Etapa 4. Pedido de validacao e publicacao

Ao receber o `ROUTE_CREATE_KEM_INFO`, o `virtual node`:

- valida a assinatura do `final physical node`
- encapsula um segredo usando a chave publica de `ML-KEM`
- cifra o payload de validacao
- envia `ROUTE_CREATE_VALIDATE_AND_PUBLISH` ao `final physical node`

Esse payload cifrado carrega:

- `virtual_node_id`
- `virtual_node_public_key`
- `final_path_id`
- `final_physical_node_id`
- `virtual_node_signature`

Essa etapa existe porque:

- o `final physical node` valida a parte fisica da rota
- o `virtual node` valida a parte logica e a publicacao em seu proprio nome

### Etapa 5. Confirmacao final

O `final physical node`:

- decapsula o segredo
- decifra o payload
- valida a assinatura do `VN`
- assina a aceitacao do entry point
- responde usando `ROUTE_CREATE_OK`

O `ROUTE_CREATE_OK` volta ate o `VN`, que:

- valida a assinatura privada do `final PN`
- valida a assinatura publica da parte aberta da rota
- decide publicar a rota na `DRT`

### Etapa 6. Publicacao na DRT

Somente depois da validacao final do `VN`, a nova rota pode ser publicada na
`DRT`.

### Etapa 7. Validacao por latencia da rota

Depois da validacao criptografica, o `VN` pode validar a estimativa temporal da
rota com:

- `ROUTE_CREATE_PING`
- `ROUTE_CREATE_PONG`

Fluxo:

1. o `VN` envia `ROUTE_CREATE_PING`
2. o ping percorre a rota ate o `final physical node`
3. o `final physical node` responde com `ROUTE_CREATE_PONG`
4. o `VN` mede o tempo total observado
5. no `random_walk_ttl_based`, o `VN` compara esse tempo com o budget que
   estimou localmente

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

Na DEMO, a familia de `route_build` fica assim:

- `ROUTE_CREATE`
- `ROUTE_CREATE_KEM_INFO`
- `ROUTE_CREATE_VALIDATE_AND_PUBLISH`
- `ROUTE_CREATE_OK`
- `ROUTE_CREATE_PING`
- `ROUTE_CREATE_PONG`

E a familia de `route_execute` fica assim:

- `ROUTE_DATA`

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
4. o `final physical node` responde usando `ROUTE_DATA`
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

O pedido original carrega apenas os dados publicos imutaveis da criacao e o
proof of work.
Os hops intermediaros nao descobrem o `final_path_id` real da rota.
Esse identificador, junto com a assinatura do `VN`, segue depois em um
`ROUTE_CREATE_VALIDATE_AND_PUBLISH` cifrado para o `final physical node`.

O `final physical node` decifra esse payload, valida anti-replay e assina a
prova de aceitacao do entry point.

Depois disso, o `final physical node` usa `ROUTE_CREATE_OK` para responder ao
`virtual node` iniciador. O `virtual node` valida o resultado final e, estando
tudo certo, pode publicar a nova rota na `DRT`.

Dependendo da estrategia escolhida, essa validacao final pode priorizar:

- budget temporal
- numero maximo de hops
- caminho onion predefinido
