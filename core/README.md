# Core Engine

Esqueleto inicial para o processamento central de pacotes recebidos.

## Objetivo

- receber um pacote ja entregue por qualquer camada de transporte
- identificar o protocolo do payload
- normalizar o pacote para um envelope comum
- encaminhar para o processador correto

## Componentes

- `models.py`
  - `PacketContext`
  - `ProtocolEnvelope`
  - `PacketProcessingResult`
- `identifiers.py`
  - identifica o wire format recebido
- `interfaces.py`
  - `PacketProtocolIdentifier`
  - `PacketProcessor`
- `protocols/`
  - `session.py`
  - `dht.py`
  - `routing.py`
  - `content.py`
  - `json_processor.py`
- `engine.py`
  - `CoreEngine`

## Uso basico

```python
from core import CoreEngine, PacketContext

engine = CoreEngine()

result = await engine.process_received_packet(
    PacketContext(
        transport_name="tcp",
        payload=b'{"header":{"version":1,"message_type":"PING","message_id":"1","message_sequence":1,"physical_session_id":null,"virtual_session_id":null},"payload":{}}',
        remote_host="127.0.0.1",
        remote_port=9001,
    )
)
```

## Familias de protocolo

- `session`
  - criacao, confirmacao e fechamento de sessao
- `dht`
  - publicacao e consulta de `DPNT`, `DRT`, `DDT`, `DTT`, `DPT`
- `routing`
  - criacao de rota, encaminhamento e confirmacao
- `content`
  - download de dados, chunks, anuncios e entrega de aplicacao
