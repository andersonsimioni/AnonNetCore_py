# Transport

`app/transport` implements physical transport adapters used by the MVP.

TCP is the primary direct transport. Relay transport allows a public relay-capable
node to forward traffic to a private node that registered itself at that relay.

Each transport exposes a common adapter interface so protocol clients do not need
to special-case transport details. The advertised endpoint shared with peers must
be the stable listener endpoint, not an ephemeral port observed from an inbound
TCP connection.
