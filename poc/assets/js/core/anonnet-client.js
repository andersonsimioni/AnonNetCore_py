class AnonNetClient {
  constructor({
    httpBaseUrl = "http://127.0.0.1:18080",
    wsUrl = "ws://127.0.0.1:18081/v1/events",
    webSocketFactory = (url) => new WebSocket(url),
    requestTimeoutMs = 180000,
  } = {}) {
    this.httpBaseUrl = httpBaseUrl.replace(/\/$/, "");
    this.wsUrl = wsUrl;
    this.webSocketFactory = webSocketFactory;
    this.requestTimeoutMs = requestTimeoutMs;
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

  async listRemoteVirtualNodes({ status = null } = {}) {
    const query = new URLSearchParams();
    if (status) {
      query.set("status", status);
    }
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return this.#get(`/v1/virtual-nodes/remote${suffix}`);
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

  async dhtPublishJob({ namespace, logicalKey, record, recordJson = null, expiresAt = null }) {
    return this.#post("/v1/dht/publish-jobs", {
      namespace,
      logical_key: logicalKey,
      record,
      record_json: recordJson,
      expires_at: expiresAt,
    });
  }

  async getDhtPublishJob({ jobId }) {
    return this.#get(`/v1/dht/publish-jobs/${encodeURIComponent(jobId)}`);
  }

  async dhtQuery({ namespace, logicalKey }) {
    return this.#post("/v1/dht/query", {
      namespace,
      logical_key: logicalKey,
    });
  }

  async buildDhtKey({ namespace, logicalKey }) {
    return this.#post("/v1/dht/key", {
      namespace,
      logical_key: logicalKey,
    });
  }

  async signLocalVirtualNodePayload({ localVirtualNodeId, payload }) {
    return this.#post("/v1/virtual-nodes/local/sign", {
      local_virtual_node_id: localVirtualNodeId,
      payload,
    });
  }

  async verifyVirtualNodePayloadSignature({ publicKey, payload, signatureHex }) {
    return this.#post("/v1/virtual-nodes/verify-signature", {
      public_key: publicKey,
      payload,
      signature_hex: signatureHex,
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

  async getContentInfo({ contentId }) {
    return this.#get(`/v1/content/${encodeURIComponent(contentId)}`);
  }

  async readContentRange({ contentId, startByte, endByte }) {
    return this.#get(
      `/v1/content/${encodeURIComponent(contentId)}/range?start_byte=${startByte}&end_byte=${endByte}`,
    );
  }

  async publishContentProvider({
    contentId,
    localVirtualNodeId,
    ttlSeconds = null,
    asyncPublish = false,
  }) {
    return this.#post(`/v1/content/${encodeURIComponent(contentId)}/providers/ddt`, {
      local_virtual_node_id: localVirtualNodeId,
      ttl_seconds: ttlSeconds,
      async_publish: asyncPublish,
    });
  }

  async startContentDownload({ sessionId, contentId, ddtKey = null }) {
    return this.#post("/v1/downloads", {
      session_id: sessionId,
      content_id: contentId,
      ddt_key: ddtKey,
    });
  }

  async getContentDownload({ sessionId, contentId }) {
    return this.#get(
      `/v1/downloads/${encodeURIComponent(sessionId)}/${encodeURIComponent(contentId)}`,
    );
  }

  async subscribeVirtualMessages({ appMessageType }) {
    return this.#post("/v1/messages/virtual/subscribe", {
      app_message_type: appMessageType,
    });
  }

  async readVirtualMessages({ appMessageType = null, limit = 100, consume = true } = {}) {
    const query = new URLSearchParams({
      limit: String(limit),
      consume: consume ? "true" : "false",
    });
    if (appMessageType) {
      query.set("app_message_type", appMessageType);
    }
    return this.#get(`/v1/messages/virtual?${query.toString()}`);
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
    const method = options?.method || "GET";
    console.debug("[AnonNet API] request", {
      method,
      path,
      body: compactAnonNetApiPayload(options?.body),
    });
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    let response;
    let payload;
    try {
      response = await fetch(`${this.httpBaseUrl}${path}`, {
        ...options,
        signal: controller.signal,
      });
      payload = await response.json();
    } catch (error) {
      const requestError = new Error(
        error.name === "AbortError"
          ? `Request timed out while calling ${method} ${path}.`
          : error.message,
      );
      requestError.code = error.name === "AbortError" ? "api_request_timeout" : "api_request_failed";
      requestError.status = null;
      console.error("[AnonNet API] request failed", {
        method,
        path,
        code: requestError.code,
        message: requestError.message,
      });
      throw requestError;
    } finally {
      clearTimeout(timeout);
    }

    if (!payload.ok) {
      const fallbackMessage = `AnonNet API error: ${JSON.stringify(payload)}`;
      const error = new Error(payload.error?.message || fallbackMessage);
      error.code = payload.error?.code;
      error.status = response.status;
      error.payload = payload;
      console.error("[AnonNet API] request failed", {
        method,
        path,
        status: response.status,
        code: error.code,
        message: error.message,
        payload,
      });
      throw error;
    }
    console.debug("[AnonNet API] response", {
      method,
      path,
      status: response.status,
      data: compactAnonNetApiPayload(payload.data),
    });
    return payload.data;
  }
}

window.AnonNetClient = AnonNetClient;

function compactAnonNetApiPayload(value) {
  if (!value) {
    return value;
  }
  if (typeof value === "string") {
    try {
      return compactAnonNetApiPayload(JSON.parse(value));
    } catch {
      return compactTextForLog(value);
    }
  }
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (typeof value !== "object") {
    return value;
  }

  const compact = {};
  for (const [key, item] of Object.entries(value)) {
    compact[key] = compactLogValue(key, item);
  }
  return compact;
}

function compactLogValue(key, value) {
  if (value === null || value === undefined) {
    return value;
  }
  if ([
    "public_key",
    "pk_virtual_node",
    "pk_virtual_node_owner",
    "pk_physical_node",
    "private_key",
    "signature",
    "signature_hex",
    "record_json",
    "data_base64",
    "payload",
    "record",
  ].includes(key)) {
    return summarizeHeavyLogValue(value);
  }
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (typeof value === "object") {
    return compactAnonNetApiPayload(value);
  }
  if (typeof value === "string") {
    return compactTextForLog(value);
  }
  return value;
}

function summarizeHeavyLogValue(value) {
  if (typeof value === "string") {
    return compactTextForLog(value);
  }
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (value && typeof value === "object") {
    return "{...}";
  }
  return value;
}

function compactTextForLog(value) {
  if (value.length <= 80) {
    return value;
  }
  return `${value.slice(0, 80)}...(${value.length} chars)`;
}
