# Resumo de Route Build

```text
ROUTE_CREATE                         VN -> hops -> PN final
ROUTE_CREATE_KEM_INFO                PN final -> hops -> VN
ROUTE_CREATE_VALIDATE_AND_PUBLISH    VN -> hops -> PN final
ROUTE_CREATE_PING                    PN final -> hops -> VN
ROUTE_CREATE_PONG                    VN -> hops -> PN final
ROUTE_CREATE_OK                      PN final -> hops -> VN
```

Depois do `ROUTE_CREATE_PONG`, o PN final valida o RTT, publica a entrada DRT e
envia `ROUTE_CREATE_OK` ao VN.
