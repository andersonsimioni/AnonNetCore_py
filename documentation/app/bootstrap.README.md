# Bootstrap

Bootstrap provides the first known public endpoints for the network. It does not
create a separate service or listener. Bootstrap nodes are regular physical
nodes whose host and port are hardcoded in `CoreConfig`, similar to DNS seeds.

The core uses those endpoints to start physical-node information exchange,
validate peers, and populate local peer state. After that, normal peer exchange
and DHT discovery expand the known network.
