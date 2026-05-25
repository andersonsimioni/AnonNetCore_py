# Rotas e Route Execute

Este documento descreve a criacao e a execucao de rotas no MVP atual.

## Objetivo

Uma rota permite que um virtual node seja alcancado por uma sequencia de
physical nodes. A camada fisica encaminha pacotes por `path_id`; a camada
virtual fica encapsulada dentro do payload fisico.

Objetivos praticos:

- separar identidade fisica de identidade virtual;
- permitir que um VN publique entry points na DRT;
- evitar que hops intermediarios conhecam todo o caminho;
- permitir sessoes virtuais end-to-end sobre caminhos fisicos;
- manter a logica hop-by-hop simples e auditavel no codigo.

## Familias de Protocolo

Route build:

- `ROUTE_CREATE`
- `ROUTE_CREATE_KEM_INFO`
- `ROUTE_CREATE_VALIDATE_AND_PUBLISH`
- `ROUTE_CREATE_PING`
- `ROUTE_CREATE_PONG`
- `ROUTE_CREATE_OK`

Route execute:

- `ROUTE_DATA`

## Criacao da Rota

A criacao acontece na camada fisica. O VN local solicita uma rota, mas os hops
que encaminham `ROUTE_CREATE` operam como physical nodes.

Fluxo resumido:

1. O VN escolhe um PN final candidato e um primeiro hop.
2. O VN gera `path_id`, nonce e budget de RTT esperado.
3. O primeiro hop recebe `ROUTE_CREATE`.
4. Cada hop salva apenas seu proprio mapeamento local de path.
5. Cada hop escolhe o proximo PN e encaminha o pedido.
6. Quando o PN final e alcancado, ele responde com `ROUTE_CREATE_KEM_INFO`.
7. O VN usa a chave KEM recebida para cifrar validacao final.
8. O VN envia `ROUTE_CREATE_VALIDATE_AND_PUBLISH`.
9. O PN final valida assinatura, nonce e autorizacao do VN.
10. O PN final envia `ROUTE_CREATE_PING`.
11. Ao receber `ROUTE_CREATE_PONG`, o PN final mede RTT e publica DRT.
12. O PN final envia `ROUTE_CREATE_OK` de volta ao VN.

## `ROUTE_CREATE`

O pedido inicial carrega apenas dados necessarios para a construcao hop-by-hop.

Campos conceituais:

- `route_strategy`
- `path_id`
- `pk_final_physical_node`
- `remaining_ttl_ms`
- `nonce`

Cada hop:

- valida o formato;
- valida proof of work;
- verifica se e o PN final;
- se nao for final, escolhe outro PN e cria novo `path_id`;
- salva uma `RouteResolution` local;
- encaminha o pedido.

O hop nao precisa conhecer a identidade virtual de alto nivel para encaminhar o
pedido inicial.

## `RouteResolution`

Cada node salva apenas a resolucao local necessaria para cumprir seu papel na
rota.

Exemplos:

- Hop intermediario salva `received_path_id -> generated_path_id`.
- PN final salva `route_path_id`, material KEM, assinaturas e estado da
  validacao.
- VN iniciador salva `initial_path_id`, `final_path_id` e status da rota.

Isso evita uma lista global de hops no banco. Cada hop conhece somente o trecho
que precisa operar.

## Validacao Final

Depois de `ROUTE_CREATE_KEM_INFO`, o VN envia
`ROUTE_CREATE_VALIDATE_AND_PUBLISH` com payload cifrado usando a chave
encapsulada pelo KEM.

Esse payload cifrado contem:

- `virtual_node_id`
- `virtual_node_public_key`
- `final_path_id`
- `final_physical_node_id`
- `expected_round_trip_ttl_ms`
- `virtual_node_signature`

A assinatura do VN autoriza aquele PN final a atuar como entry point para
aquele `final_path_id`.

## Ping/Pong de Rota

Antes de publicar a DRT, o PN final valida o RTT da rota:

1. PN final envia `ROUTE_CREATE_PING`.
2. O ping percorre a rota ate o VN.
3. O VN responde com `ROUTE_CREATE_PONG`.
4. O PN final mede `observed_round_trip_ms`.
5. O PN final compara com `expected_round_trip_ttl_ms`.
6. Se estiver dentro da janela permitida, o PN final assina o RTT.

Configuracao relacionada:

```text
CoreConfig.random_walk_ttl_route_error_ms
```

## Publicacao na DRT

No MVP atual, a publicacao da rota na DRT e feita pelo PN final apos validar o
`ROUTE_CREATE_PONG`.

O registro DRT e montado a partir da `RouteResolution` do PN final e inclui:

- chave publica do VN;
- chave publica do PN entry point;
- `final_path_id`;
- assinatura do VN;
- assinatura do PN final aceitando ser entry point;
- assinatura publica de aceitacao da rota;
- RTT observado;
- assinatura do PN final sobre o RTT;
- expiracao.

Logical key:

```text
namespace = drt
logical_key = virtual_node_id
```

Essa publicacao permite que outros VNs encontrem entry points para iniciar uma
sessao virtual com o VN publicado.

## `ROUTE_CREATE_OK`

Depois da publicacao DRT ser agendada/concluida, o PN final devolve
`ROUTE_CREATE_OK` ao VN. O payload sensivel tambem e cifrado usando o segredo
derivado no KEM.

O OK confirma para o VN que:

- o PN final aceitou a rota;
- o `final_path_id` foi validado;
- as assinaturas foram geradas;
- o processo chegou ao final.

## Manutencao Automatica de Rotas

O `VirtualRouteMaintenanceRuntime` cuida de todos os VNs locais ativos.

Ele tenta manter pelo menos:

```text
CoreConfig.virtual_route_maintenance_route_min_online_routes
```

rotas online publicadas na DRT.

O runtime:

- lista VNs locais ativos;
- consulta a DRT de cada VN;
- conta rotas online validas;
- considera rotas pendentes;
- cria novas rotas quando o total esta abaixo do minimo;
- aceita repetir entry point em redes pequenas se necessario.

## Route Execute

Depois da rota criada, os dados trafegam por:

```text
ROUTE_DATA
```

O payload fisico contem metadados da rota e um envelope virtual dentro dele:

```text
{
  "path_id": "...",
  "direction": "vn_to_pn ou pn_to_vn",
  "virtual_session_id": "...",
  "virtual_envelope_ciphered": true,
  "virtual_envelope": {}
}
```

Responsabilidades do handler:

1. Receber `ROUTE_DATA`.
2. Resolver `path_id` no `RouteService`.
3. Decidir se entrega localmente ou encaminha.
4. Se encaminhar, trocar o path conforme a `RouteResolution`.
5. Se entregar localmente, reconstruir o envelope virtual.
6. Se o envelope estiver cifrado, pedir a sessao virtual para decifrar.
7. Redisparar o envelope virtual no core.
8. Se o handler virtual produzir resposta, encapsular novamente em `ROUTE_DATA`.

O route execute nao conhece semantica de aplicacao. Ele apenas move envelopes
entre hops e entrega localmente quando a rota termina.

## Encapsulamento Fisico/Virtual

Modelo:

```text
physical_envelope {
  message_type: ROUTE_DATA,
  payload: {
    metadata fisica,
    virtual_envelope_ciphered,
    virtual_envelope
  }
}
```

O envelope virtual pode conter:

- `VIRTUAL_SESSION_*`
- `VIRTUAL_SESSION_DATA`
- `VIRTUAL_CONTENT_*`

Antes da sessao virtual estar estabelecida, o envelope virtual pode trafegar
sem cifra end-to-end porque ainda nao existe segredo compartilhado. Depois do
handshake, os payloads virtuais usam a chave da sessao virtual.

## Estrategias de Rota

O protocolo suporta multiplas estrategias. O MVP usa principalmente:

```text
random_walk_ttl
```

Tambem existem bases para:

- `random_walk_max_hop`
- `onion_like`

No MVP funcional, a estrategia relevante para testes e PoC e a random walk por
TTL/RTT.

## Limites

- A estrategia atual e experimental.
- Nao ha protecao completa contra correlacao por adversarios globais.
- A rede de teste usa LAN/Docker, nao Internet publica com NAT complexo.
- O route execute e hop-by-hop; confiabilidade fim-a-fim avancada ainda e
  trabalho futuro.
