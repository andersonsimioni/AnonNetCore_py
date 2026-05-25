# API Local

A API local permite que aplicacoes externas usem o core sem importar modulos
internos. No MVP, ela e usada pela PoC social e pelos smokes de integracao.

## Enderecos

HTTP:

```text
http://127.0.0.1:18080
```

WebSocket:

```text
ws://127.0.0.1:18081/v1/events
```

Configuracoes principais:

```text
CoreConfig.api_enabled
CoreConfig.api_host
CoreConfig.api_port
CoreConfig.api_websocket_enabled
CoreConfig.api_websocket_host
CoreConfig.api_websocket_port
CoreConfig.api_websocket_path
```

## Principios

- A API e local por padrao.
- O front-end pode rodar como arquivo `.html` local.
- CORS e liberado no MVP para facilitar integracao com apps externas.
- Operacoes longas podem usar jobs ou eventos WebSocket.
- A API nao substitui os protocolos internos; ela chama os clients e services do
  core.

## Endpoints de Status

### `GET /health`

Retorna status basico do servidor HTTP.

### `GET /v1/status`

Retorna estado resumido do core, incluindo node local, contadores e flags.

### `GET /debug/state`

Retorna snapshot detalhado para debug console e smokes.

## Virtual Nodes

### `GET /v1/virtual-nodes/local`

Lista VNs locais.

### `POST /v1/virtual-nodes`

Cria um VN local.

Corpo tipico:

```json
{
  "kind": "social_profile",
  "metadata": {}
}
```

### `GET /v1/virtual-nodes/remote`

Lista VNs remotos conhecidos localmente.

### `POST /v1/virtual-nodes/remote`

Cadastra ou atualiza um VN remoto conhecido.

Corpo tipico:

```json
{
  "virtual_node_id": "sha512-da-public-key",
  "public_key": "-----BEGIN PUBLIC KEY-----..."
}
```

## Assinaturas de VNs

### `POST /v1/virtual-nodes/local/sign`

Assina payload com a chave privada de um VN local.

### `POST /v1/virtual-nodes/verify-signature`

Verifica assinatura de um VN a partir da chave publica informada.

Esses endpoints sao usados por apps que precisam montar registros DHT assinados,
como DPT/DDT da PoC social.

## DHT

### `POST /v1/dht/key`

Calcula a chave DHT para um namespace e logical key.

Corpo:

```json
{
  "namespace": "dpt",
  "logical_key": "anonnet.social|virtual_node_id"
}
```

### `POST /v1/dht/query`

Consulta a DHT.

Corpo:

```json
{
  "namespace": "dpt",
  "logical_key": "anonnet.social|virtual_node_id"
}
```

### `POST /v1/dht/publish`

Publica um registro DHT de forma sincrona.

Corpo:

```json
{
  "namespace": "dpt",
  "logical_key": "anonnet.social|virtual_node_id",
  "record": {}
}
```

### `POST /v1/dht/publish-jobs`

Cria uma publicacao assincrona. E o caminho recomendado para a UI quando a
publicacao pode demorar.

### `GET /v1/dht/publish-jobs/{job_id}`

Consulta o estado de um publish job.

## Sessoes Virtuais

### `GET /v1/sessions/virtual`

Lista sessoes virtuais conhecidas.

### `POST /v1/sessions/virtual`

Inicia sessao virtual com um VN remoto.

Corpo tipico:

```json
{
  "local_virtual_node_id": "vn-local",
  "remote_virtual_node_id": "vn-remoto"
}
```

O core resolve a rota pela DRT, resolve o entry point fisico pela DPNT e inicia
o handshake virtual sobre `ROUTE_DATA`.

### `POST /v1/sessions/virtual/{session_id}/messages`

Envia mensagem de aplicacao por uma sessao virtual.

Corpo tipico:

```json
{
  "app_message_type": "social.direct_message",
  "payload": {
    "text": "ola"
  }
}
```

### `POST /v1/sessions/virtual/{session_id}/close`

Fecha uma sessao virtual.

## Mensagens Virtuais

### `POST /v1/messages/virtual/subscribe`

Registra interesse em um `app_message_type` para que mensagens recebidas fiquem
disponiveis na inbox da API.

### `GET /v1/messages/virtual`

Le mensagens recebidas.

Filtros comuns:

- `app_message_type`
- `limit`

## Conteudo

### `GET /v1/content`

Lista conteudos locais.

### `POST /v1/content`

Armazena conteudo localmente.

Corpo tipico:

```json
{
  "title": "profile-state.json",
  "content_type": "application/json",
  "data_base64": "..."
}
```

### `GET /v1/content/{content_id}`

Retorna metadados de conteudo local.

### `GET /v1/content/{content_id}/range?start_byte=0&end_byte=65536`

Retorna uma faixa de bytes em base64.

### `POST /v1/content/{content_id}/providers/ddt`

Publica na DDT que um VN local possui aquele conteudo.

## Downloads

### `GET /v1/downloads`

Lista downloads em andamento ou concluidos.

### `POST /v1/downloads`

Inicia download virtual de conteudo. O core usa o protocolo
`VIRTUAL_CONTENT_*` sobre uma sessao virtual.

### `GET /v1/downloads/{session_id}/{content_id}`

Consulta o estado de um download especifico.

## WebSocket

O WebSocket entrega eventos assim que chegam ao core.

Conectar em:

```text
ws://127.0.0.1:18081/v1/events
```

Mensagem de inscricao:

```json
{
  "type": "subscribe",
  "event_types": ["virtual_message_received"],
  "app_message_types": ["social.direct_message"]
}
```

Eventos comuns:

- `virtual_message_received`
- `content_provider_published`
- `content_download_requested`

Mensagem de ping:

```json
{
  "type": "ping"
}
```

Resposta:

```json
{
  "type": "pong",
  "data": {}
}
```

## Uso na PoC Social

A PoC usa a API para:

- criar/listar perfis locais como VNs;
- assinar DPT/DDT;
- publicar estado social;
- consultar estado de amigos;
- baixar conteudo de perfil;
- abrir sessoes virtuais;
- enviar DM;
- receber DM por WebSocket.

## Limites

- A API atual e local e voltada para demo/desenvolvimento.
- Nao ha autenticacao forte entre app local e core.
- Nao ha HTTPS por padrao porque o alvo e `localhost`.
- Apps externas devem tratar erros e timeouts como parte normal da rede.
