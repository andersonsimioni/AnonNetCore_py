class SocialSessionStore {
  constructor(initialSessions = {}) {
    this.sessionsByVirtualNodeId = { ...initialSessions };
  }

  get(remoteVirtualNodeId) {
    return this.sessionsByVirtualNodeId[remoteVirtualNodeId] || null;
  }

  set(remoteVirtualNodeId, sessionId) {
    this.sessionsByVirtualNodeId[remoteVirtualNodeId] = sessionId;
    return sessionId;
  }

  toJSON() {
    return { ...this.sessionsByVirtualNodeId };
  }
}

window.SocialSessionStore = SocialSessionStore;
