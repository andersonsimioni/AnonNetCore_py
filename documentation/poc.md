# PoC Social

A PoC social demonstra uma aplicacao externa usando o core por API local. Ela e
intencionalmente simples, mas exercita os fluxos principais do MVP:

- perfis baseados em virtual nodes;
- publicacao de estado em DPT/DDT;
- leitura de estado de amigos;
- feed de posts;
- mensagens diretas por sessao virtual;
- eventos WebSocket.

## Execucao

Na raiz do projeto:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

Sem abrir navegador automaticamente:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10 --no-open
```

Com Debug Console:

```powershell
.\.venv\Scripts\python.exe scripts\run_poc_debug.py 10
```

O HTML da PoC fica em:

```text
poc/index.html
```

Ele pode ser aberto diretamente como arquivo local, sem servidor web.

## Modelo Social

Regra principal:

```text
1 virtual node = 1 perfil
```

O estado de um perfil contem:

- nome;
- bio;
- foto em `photo_data_url`;
- lista de amigos por VN ID;
- posts;
- metadados locais.

Para demo, a foto e salva como data URL dentro do estado do perfil. Isso
simplifica a PoC, mas nao e o modelo ideal para producao.

## Publicacao de Perfil

A PoC publica o estado do usuario em duas etapas:

1. Salva o estado completo como conteudo local.
2. Publica na DDT que o VN local e holder desse conteudo.
3. Atualiza a DPT do perfil para apontar para o `content_id` mais recente.

Chave DPT:

```text
namespace = dpt
logical_key = anonnet.social|virtual_node_id
```

O registro DPT contem:

```text
target_ref = content_id do estado social mais recente
```

Como o DPT e assinado pelo VN dono, outros peers conseguem validar se o ponteiro
realmente pertence ao perfil consultado.

## Sincronizacao

Ao carregar a pagina, a PoC:

1. carrega perfis locais do cache do navegador;
2. consulta a DPT do perfil local;
3. compara estado local com o estado publicado;
4. se o local estiver mais novo ou divergente, publica DDT e atualiza DPT;
5. consulta DPTs dos amigos;
6. baixa os estados apontados por DDT;
7. monta o feed.

Um servico background JS repete essa logica periodicamente para manter o perfil
e o feed sincronizados.

## Amigos

A lista de amigos e uma lista simples de VN IDs.

Para ler o perfil de um amigo:

1. calcular `logical_key = anonnet.social|friend_virtual_node_id`;
2. consultar DPT;
3. validar assinatura do ponteiro;
4. ler `target_ref`;
5. resolver DDT;
6. baixar o arquivo de estado;
7. renderizar posts e dados do perfil.

## Feed

O feed e construido a partir dos posts presentes no estado baixado de cada
amigo. O MVP nao possui algoritmo de ranking; a ordenacao e temporal.

Posts do proprio usuario ficam no mesmo arquivo de estado do perfil.

## Mensagens Diretas

Mensagens diretas usam sessoes virtuais.

Fluxo:

1. usuario escolhe amigo por VN ID;
2. app solicita ao core uma sessao virtual;
3. core resolve DRT do amigo;
4. core resolve DPNT do entry point;
5. core estabelece `VIRTUAL_SESSION_*` sobre `ROUTE_DATA`;
6. app envia `VIRTUAL_SESSION_DATA` com `app_message_type`;
7. amigo recebe evento via WebSocket.

Tipo usado pela PoC:

```text
social.direct_message
```

## Cache Local

A PoC usa armazenamento local do navegador para:

- perfis locais;
- perfil selecionado;
- estado social em edicao;
- cache de amigos;
- mensagens recebidas.

Existe botao de limpar dados/cache do site para facilitar a demo.

## Arquivos Principais

- `poc/index.html`: estrutura da pagina.
- `poc/assets/css/`: estilos.
- `poc/assets/js/app.js`: coordenacao da UI.
- `poc/assets/js/anonnet-api.js`: cliente da API local.
- `poc/assets/js/social-flow.js`: fluxos sociais compartilhados com smoke.
- `poc/smokes/social_dom.js`: smoke de DOM.
- `poc/smokes/social_flow.js`: smoke integrado da PoC.

## Limites da PoC

- Nao ha moderacao, bloqueio, privacidade avancada ou criptografia de dados de
  perfil alem do que ja existe no transporte/sessao.
- Foto como data URL e aceitavel para demo, mas deve virar conteudo separado em
  uma versao real.
- O feed baixa estado completo de amigos; isso e simples e didatico, mas nao e
  eficiente para uma rede social grande.
- O objetivo e demonstrar integracao com core, DHT, rotas e sessoes virtuais.
