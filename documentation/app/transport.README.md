# Transport

O diretorio `app/transport` implementa o transporte fisico do MVP.

## Responsabilidades

- abrir listener TCP;
- aceitar conexoes;
- enviar bytes para peers remotos;
- aplicar framing;
- entregar pacotes recebidos para a engine.

## TCP

O MVP usa TCP como transporte principal.

Cada mensagem e enviada como frame com prefixo de tamanho. Depois que o frame e
lido, o payload e entregue ao core como bytes JSON.

## Arquivos

- `frame_codec.py`: codificacao e decodificacao de frames TCP.
- `tcp_transport.py`: listener e envio TCP.

## Observacao

O endpoint anunciado para outros peers deve ser o host/porta de listener, nao a
porta efemera criada por uma conexao recebida. Essa regra e essencial para que
nodes em Docker e nodes locais consigam se conectar entre si.
