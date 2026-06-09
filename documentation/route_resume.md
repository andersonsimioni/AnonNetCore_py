# Route Build Summary

```text
ROUTE_CREATE                         VN -> hops -> final PN
ROUTE_CREATE_KEM_INFO                final PN -> hops -> VN
ROUTE_CREATE_VALIDATE_AND_PUBLISH    VN -> hops -> final PN
ROUTE_CREATE_PING                    final PN -> hops -> VN
ROUTE_CREATE_PONG                    VN -> hops -> final PN
ROUTE_CREATE_OK                      final PN -> hops -> VN
ROUTE_CREATE_FAIL                    final PN -> hops -> VN
```

After `ROUTE_CREATE_PONG`, the final physical node validates the measured RTT,
publishes the DRT entry, and sends `ROUTE_CREATE_OK` back to the virtual node.

If validation fails, the final physical node sends `ROUTE_CREATE_FAIL` so every
hop can reject the pending route resolution instead of waiting for timeout.
