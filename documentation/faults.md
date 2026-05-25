# Falhas, Limites e Trabalhos Futuros

Este projeto e um MVP tecnico e experimental. Ele valida arquitetura e fluxo de
protocolos, mas ainda nao deve ser tratado como produto de producao ou como rede
publica robusta.

## Limites de Seguranca

- Nao ha defesa completa contra Sybil, flood, spam ou abuso coordenado.
- Nao ha sistema maduro de reputacao.
- Nao ha rate limit robusto por peer, namespace ou aplicacao.
- Nao ha auditoria formal de seguranca criptografica.
- Chaves privadas locais ainda precisam de protecao mais forte em uma versao
  real.
- A PoC social nao implementa privacidade avancada de perfil, moderacao ou
  controle de acesso.

## Limites de Rede

- O MVP usa TCP e testes em LAN/Docker.
- Nao ha NAT traversal completo.
- Nao ha relay server de producao.
- Nao ha QUIC.
- DNS seeds existem como modelo, mas a demo usa endpoints hardcoded.
- Comportamento sob churn intenso ainda precisa de testes longos.

## Limites da DHT

- A DHT ja replica registros nos K responsaveis, mas ainda nao possui uma
  tabela de roteamento estilo Kademlia completa.
- Listas muito grandes em DDT/DTT ainda precisam de particionamento por shards.
- Regras de expiracao, garbage collection e rebalanceamento ainda podem evoluir.
- Nao ha mecanismo completo de resolucao de conflitos maliciosos.

## Limites de Rotas e Sessoes

- A estrategia principal atual e `random_walk_ttl`.
- O route execute entrega hop-by-hop, mas ainda nao possui confiabilidade
  avancada fim-a-fim.
- A protecao contra correlacao por adversario global nao e completa.
- Rotas e sessoes precisam de testes mais longos com perda, latencia e peers
  instaveis.

## Limites da PoC Social

- Foto de perfil e salva como data URL dentro do estado social para simplificar
  a demo.
- Feed baixa o estado completo dos amigos.
- Nao ha paginacao, ranking, moderacao ou busca distribuida completa.
- O objetivo da PoC e provar integracao com core, DHT, DRT, DDT, DPT e sessoes
  virtuais.

## Resumo

O MVP prova os fluxos principais da arquitetura. Os proximos passos naturais sao
hardening, testes de escala, protecao anti-abuso, NAT traversal, melhorias de
DHT e otimizacao do transporte.
