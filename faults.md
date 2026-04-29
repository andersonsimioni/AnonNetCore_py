# Faults

Este projeto e apenas uma `DEMO` tecnica e experimental.

Ele nao deve ser tratado como produto comercial, ambiente de producao ou base pronta para operacao real em rede aberta.

## Principais limitacoes

- seguranca de rede ainda insuficiente, sem protecao real contra `sybil`, `flood`, `spam`, `replay`, publicacoes abusivas e peers maliciosos
- ausencia de controles operacionais basicos, como `rate limiting`, quotas, reputacao, auditoria, metricas e observabilidade
- persistencia local inadequada para escala, usando `SQLite` e sem otimizacoes reais de desempenho, concorrencia e recuperacao
- implementacao em linguagem inadequada para o objetivo final, pois esta DEMO foi feita em Python e o MVP real sera em `C++`
- selecao dos `K` peers mais proximos da chave ainda nao otimizada, sem estrutura de roteamento estilo `Kademlia` e sem lookup iterativo real
- DHT ainda sem politicas maduras de expiracao, garbage collection, revalidacao, replicacao, rebalanceamento e resolucao de conflitos
- conectividade de rede incompleta, sem `relay server`, `hole punching`, `DNS seed` real e `NAT traversal` completo
- bootstrap ainda simplificado, dependente de seeds e endpoints hardcoded
- gerenciamento de chaves e segredos ainda fraco, sem protecao local seria para a chave privada
- cobertura de testes ainda insuficiente para rede distribuida real, sem validacao pesada de churn, particao, latencia e comportamento hostil

## Resumo

Esta base serve para validar arquitetura, fluxo de protocolos e organizacao do sistema.

Ela nao foi feita para seguranca forte, alta performance, grande escala ou uso comercial.
