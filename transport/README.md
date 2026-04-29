# Transport

Camada de transporte de rede classica.

## Objetivo

- centralizar envio e recebimento de mensagens
- abstrair os protocolos de transporte disponiveis
- manter o sistema pronto para adicionar novos transportes
- entregar pacotes recebidos para uma camada superior

## Estrutura

- `models.py`
  - modelos comuns para endpoints, pacotes e mensagens de saida
- `interfaces.py`
  - contrato dos adapters de transporte
- `service.py`
  - servico principal que registra e coordena todos os transportes
- `frame_codec.py`
  - framing de 4 bytes para TCP
- `tcp_transport.py`
  - adapter TCP inicial

## Uso basico

```python
from transport import (
    OutboundMessage,
    TcpTransportAdapter,
    TcpTransportConfig,
    TransportEndpoint,
    TransportService,
)

transport_service = TransportService()
transport_service.register_adapter(
    TcpTransportAdapter(
        TcpTransportConfig(
            listen_host="127.0.0.1",
            listen_port=9000,
        )
    )
)
```
