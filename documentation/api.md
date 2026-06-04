# Local API

The local API lets external applications use the core without importing internal
Python modules. In the MVP it is used by the social PoC and by integration
smokes.

## Addresses

HTTP:

```text
http://127.0.0.1:18080
```

WebSocket:

```text
ws://127.0.0.1:18081/v1/events
```

Main configuration fields:

```text
CoreConfig.api_enabled
CoreConfig.api_host
CoreConfig.api_port
CoreConfig.api_websocket_enabled
CoreConfig.api_websocket_host
CoreConfig.api_websocket_port
CoreConfig.api_websocket_path
```

## Principles

- The API is local by default.
- The front-end can run directly as a local `.html` file.
- CORS is open in the MVP to simplify external app integration.
- Long operations can use jobs or WebSocket events.
- The API does not replace internal protocols; it delegates to core clients and
  services.

## Status Endpoints

### `GET /health`

Returns basic HTTP server health.

### `GET /v1/status`

Returns a compact core status snapshot.

### `GET /debug/state`

Returns a detailed snapshot used by the Debug Console and smoke tests.

## Virtual Nodes

### `GET /v1/virtual-nodes/local`

Lists local virtual nodes.

### `POST /v1/virtual-nodes`

Creates a local virtual node.

Typical body:

```json
{
  "kind": "social_profile",
  "metadata": {}
}
```

### `GET /v1/virtual-nodes/remote`

Lists remote virtual nodes known locally.

### `POST /v1/virtual-nodes/remote`

Upserts a known remote virtual node.

## DHT

### `POST /v1/dht/publish`

Publishes a DHT record through the physical DHT client.

### `POST /v1/dht/query`

Queries a DHT namespace/logical key.

### `GET /v1/dht/jobs/{job_id}`

Reads the status of an asynchronous DHT publication job.

## Virtual Sessions

### `POST /v1/virtual-sessions`

Starts a virtual session with a remote virtual node. The core resolves the route
through DRT, resolves the physical entry point through DPNT, and sends
`VIRTUAL_SESSION_INIT` over `ROUTE_DATA`.

### `POST /v1/virtual-sessions/{session_id}/messages`

Sends an application message over an active virtual session.

### `DELETE /v1/virtual-sessions/{session_id}`

Closes a virtual session.

## Content

### `POST /v1/content`

Stores content locally.

### `GET /v1/content/{content_id}`

Returns local content metadata.

### `POST /v1/content/{content_id}/publish`

Publishes in DDT that a local virtual node holds that content.

### `POST /v1/content/downloads`

Starts a virtual content download. The core uses `VIRTUAL_CONTENT_*` messages
over an active virtual session and requests byte ranges until the file is
complete.

## WebSocket Events

The WebSocket endpoint streams incoming virtual messages and operational events
to external applications.

Subscription message:

```json
{
  "type": "subscribe",
  "scope": "virtual_messages"
}
```

Ping message:

```json
{
  "type": "ping"
}
```

## Social PoC Usage

The social PoC uses the API to:

- create local virtual nodes;
- publish social profile state;
- resolve friend profiles through DPT and DDT;
- download profile content;
- establish virtual sessions;
- exchange direct messages;
- receive WebSocket events.

## Current Limits

- There is no strong authentication between a local app and the local core.
- HTTPS is not enabled by default because the target is localhost.
- Browser-facing production usage would require a local app bridge or a trusted
  local permission model.
