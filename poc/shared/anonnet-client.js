export class AnonNetClient {
  constructor({
    httpBaseUrl = "http://127.0.0.1:18080",
    wsUrl = "ws://127.0.0.1:18081/v1/events",
    webSocketFactory = (url) => new WebSocket(url),
  } = {}) {
    this.httpBaseUrl = httpBaseUrl.replace(/\/$/, "");
    this.wsUrl = wsUrl;
    this.webSocketFactory = webSocketFactory;
    this.websocket = null;
  }

  async getStatus() {
    return this.#get("/v1/status");
  }

  async createLocalVirtualNode({ kind = "social", metadata = {} } = {}) {
    return this.#post("/v1/virtual-nodes", { kind, metadata });
  }

  async upsertRemoteVirtualNode({ publicKey, nodeId = null, kind = "social", metadata = {} }) {
    return this.#post("/v1/virtual-nodes/remote", {
      public_key: publicKey,
      node_id: nodeId,
      kind,
      metadata,
    });
  }

  async dhtPublish({ namespace, logicalKey, record, recordJson = null, expiresAt = null }) {
    return this.#post("/v1/dht/publish", {
      namespace,
      logical_key: logicalKey,
      record,
      record_json: recordJson,
      expires_at: expiresAt,
    });
  }

  async startVirtualSession({ localVirtualNodeId, remoteVirtualNodeId }) {
    return this.#post("/v1/sessions/virtual", {
      local_virtual_node_id: localVirtualNodeId,
      remote_virtual_node_id: remoteVirtualNodeId,
    });
  }

  async sendVirtualMessage({ sessionId, appMessageType, payload = {}, requestId = null }) {
    return this.#post(`/v1/sessions/virtual/${encodeURIComponent(sessionId)}/messages`, {
      app_message_type: appMessageType,
      payload,
      request_id: requestId,
    });
  }

  async storeContent({
    dataBase64,
    title = null,
    contentType = "application/octet-stream",
    tags = [],
  }) {
    return this.#post("/v1/content", {
      data_base64: dataBase64,
      title,
      content_type: contentType,
      tags,
    });
  }

  connectEvents({ eventTypes = [], appMessageTypes = [], onEvent }) {
    this.websocket = this.webSocketFactory(this.wsUrl);
    this.websocket.addEventListener("open", () => {
      this.websocket.send(JSON.stringify({
        type: "subscribe",
        event_types: eventTypes,
        app_message_types: appMessageTypes,
      }));
    });
    this.websocket.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (typeof onEvent === "function") {
        onEvent(message);
      }
    });
    return this.websocket;
  }

  async #get(path) {
    return this.#request(path, { method: "GET" });
  }

  async #post(path, body) {
    return this.#request(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async #request(path, options) {
    const response = await fetch(`${this.httpBaseUrl}${path}`, options);
    const payload = await response.json();
    if (!payload.ok) {
      const error = new Error(payload.error?.message || "AnonNet API error");
      error.code = payload.error?.code;
      throw error;
    }
    return payload.data;
  }
}
