class SocialSessionStore {
  constructor(initialSessions = {}) {
    this.sessionsByVirtualNodeId = { ...initialSessions };
  }

  get(localVirtualNodeId, remoteVirtualNodeId = null) {
    const key = this.#buildKey(localVirtualNodeId, remoteVirtualNodeId);
    return this.sessionsByVirtualNodeId[key] || null;
  }

  set(localVirtualNodeId, remoteVirtualNodeId, sessionId = null) {
    const key = this.#buildKey(localVirtualNodeId, remoteVirtualNodeId);
    this.sessionsByVirtualNodeId[key] = sessionId;
    return sessionId;
  }

  toJSON() {
    return { ...this.sessionsByVirtualNodeId };
  }

  #buildKey(localVirtualNodeId, remoteVirtualNodeId) {
    if (!remoteVirtualNodeId) {
      return localVirtualNodeId;
    }
    return `${localVirtualNodeId}::${remoteVirtualNodeId}`;
  }
}

window.SocialSessionStore = SocialSessionStore;
