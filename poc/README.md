# AnonNet PoC

PoC de uma micro rede social usando o core como dependencia externa via HTTP e WebSocket.

## Estrutura

```text
poc/
  shared/
    anonnet-client.js
    social-models.js
    social-service.js
    social-session-store.js
  sdk/
    anonnet-client.js
  web/
    index.html
    src/
      app.js
      state.js
      styles.css
  tests/
    social-smoke.mjs
  scripts/
    up_poc.py
```

## Ideia

O core continua responsavel pela rede, sessoes virtuais, DHTs, DDT, DPT e downloads.
A PoC apenas consome a API publica do core para validar um produto real por cima da rede.
O front e os smokes importam os mesmos modulos de `poc/shared`, evitando duplicar regra de produto.

Estruturas iniciais:

- Perfil: `anonnet.social.profile.v1`, com foto por `photo_content_id`, nome, bio e amigos.
- Amigos: listas simples no perfil, usando `friend_virtual_node_ids` e `friend_public_keys`.
- Ponteiro de perfil: pensado para DPT usando chave logica `anonnet.social|<virtual_node_id>|profile`.
- Mensagem direta: `anonnet.social.direct_message.v1`, enviada via virtual session.

Fluxo inicial esperado:

1. Criar ou selecionar um virtual node local.
2. Publicar perfil/conteudo usando DHT generica.
3. Registrar contatos externos por virtual node/public key.
4. Abrir sessao virtual com outro VN.
5. Enviar e receber mensagens via sessao virtual.
6. Baixar conteudos por ranges usando o protocolo virtual de conteudo.

## Rodar

Sirva a pasta `poc` como raiz estatica para o browser conseguir importar `poc/shared`:

```powershell
python -m http.server 18100 -d poc
```

Abra `http://127.0.0.1:18100/web/`.

## Smoke JS

```powershell
node poc\tests\social-smoke.mjs
```
