{
const {
  SOCIAL_DIRECT_MESSAGE_TYPE,
  SOCIAL_PROFILE_CONTENT_TYPE,
  SOCIAL_PROFILE_DPT_TITLE,
  buildProfileDptLogicalKey,
  createDirectMessage,
  createProfile,
  createUserState,
  decodeJsonFromBase64,
  encodeJsonToBase64,
  normalizeUniqueStrings,
} = window.AnonNetSocialModels;

class SocialService {
  constructor(client, { sessionStore = new SocialSessionStore() } = {}) {
    this.client = client;
    this.sessionStore = sessionStore;
  }

  async createLocalProfileNode() {
    return this.client.createLocalVirtualNode({
      kind: "social",
      metadata: {
        app: "anonnet-poc",
        role: "profile",
      },
    });
  }

  async saveLocalProfile({
    localVirtualNode,
    displayName,
    bio = "",
    photoContentId = null,
    friendVirtualNodeIds = [],
    friendPublicKeys = [],
    feedPosts = [],
  }) {
    const profile = createProfile({
      virtualNodeId: localVirtualNode.id,
      publicKey: localVirtualNode.public_key,
      displayName,
      bio,
      photoContentId,
      friendVirtualNodeIds,
      friendPublicKeys,
    });

    return this.saveUserState({
      localVirtualNode,
      profile,
      feedPosts,
    });
  }

  async saveUserState({
    localVirtualNode,
    profile,
    feedPosts = [],
  }) {
    const userState = createUserState({
      profile: {
        ...profile,
        virtual_node_id: localVirtualNode.id,
        public_key: localVirtualNode.public_key,
        updated_at: new Date().toISOString(),
      },
      feedPosts,
    });
    const content = await this.client.storeContent({
      dataBase64: encodeJsonToBase64(userState),
      title: SOCIAL_PROFILE_DPT_TITLE,
      contentType: SOCIAL_PROFILE_CONTENT_TYPE,
      tags: ["profile", "social", "user-state"],
    });
    return {
      profile: userState.profile,
      userState,
      content,
    };
  }

  async publishLocalUserState({
    localVirtualNode,
    profile,
    feedPosts = [],
  }) {
    const savedState = await this.saveUserState({
      localVirtualNode,
      profile,
      feedPosts,
    });
    const ddt = await this.client.publishContentProvider({
      contentId: savedState.content.content_id,
      localVirtualNodeId: localVirtualNode.id,
    });
    assertDhtPublishCompleted(ddt.publish_result, "DDT");
    const dpt = await this.publishLocalProfilePointer({
      localVirtualNode,
      targetRef: ddt.logical_key,
    });

    return {
      ...savedState,
      ddt,
      dpt,
    };
  }

  async publishLocalProfilePointer({ localVirtualNode, targetRef }) {
    const logicalKey = buildProfileDptLogicalKey(localVirtualNode.id);
    const dhtKey = await this.client.buildDhtKey({
      namespace: "dpt",
      logicalKey,
    });
    const lastModified = new Date().toISOString();
    const signedPayload = {
      key: dhtKey.key,
      pk_virtual_node_owner: localVirtualNode.public_key,
      title: SOCIAL_PROFILE_DPT_TITLE,
      type: SOCIAL_PROFILE_CONTENT_TYPE,
      last_modified: lastModified,
      target_ref: targetRef,
    };
    const signature = await this.client.signLocalVirtualNodePayload({
      localVirtualNodeId: localVirtualNode.id,
      payload: signedPayload,
    });
    const record = {
      pk_virtual_node_owner: localVirtualNode.public_key,
      title: SOCIAL_PROFILE_DPT_TITLE,
      type: SOCIAL_PROFILE_CONTENT_TYPE,
      last_modified: lastModified,
      target_ref: targetRef,
      signature: signature.signature_hex,
    };
    const publishResult = await this.client.dhtPublish({
      namespace: "dpt",
      logicalKey,
      record,
    });
    assertDhtPublishCompleted(publishResult, "DPT");

    return {
      logicalKey,
      dhtKey: dhtKey.key,
      record,
      publishResult,
    };
  }

  async resolveProfilePointer({ virtualNodeId, publicKey }) {
    const logicalKey = buildProfileDptLogicalKey(virtualNodeId);
    const dhtKey = await this.client.buildDhtKey({
      namespace: "dpt",
      logicalKey,
    });
    const queryResult = await this.client.dhtQuery({
      namespace: "dpt",
      logicalKey,
    });
    if (queryResult.status !== "found" || !queryResult.record_json) {
      throw new Error(`DPT nao encontrada para VN ${virtualNodeId}.`);
    }

    const record = JSON.parse(queryResult.record_json);
    const signedPayload = {
      key: dhtKey.key,
      pk_virtual_node_owner: record.pk_virtual_node_owner,
      title: record.title,
      type: record.type,
      last_modified: record.last_modified,
      target_ref: record.target_ref,
    };
    const verification = await this.client.verifyVirtualNodePayloadSignature({
      publicKey: publicKey || record.pk_virtual_node_owner,
      payload: signedPayload,
      signatureHex: record.signature,
    });
    if (!verification.valid) {
      throw new Error(`Assinatura DPT invalida para VN ${virtualNodeId}.`);
    }

    return {
      logicalKey,
      dhtKey: dhtKey.key,
      record,
      queryResult,
    };
  }

  async downloadUserStateFromPointer({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey,
  }) {
    const pointer = await this.resolveProfilePointer({
      virtualNodeId: remoteVirtualNodeId,
      publicKey: remotePublicKey,
    });
    const session = await this.getOrCreateDirectSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
      remotePublicKey,
    });
    await this.client.startContentDownload({
      sessionId: session.sessionId,
      contentId: pointer.record.target_ref,
      ddtKey: pointer.record.target_ref,
    });
    const download = await waitForValue(async () => {
      const state = await this.client.getContentDownload({
        sessionId: session.sessionId,
        contentId: pointer.record.target_ref,
      }).catch((error) => {
        if (error.code === "download_not_found") {
          return null;
        }
        throw error;
      });
      return state?.status === "completed" ? state : null;
    }, {
      timeoutMs: 60000,
      label: "content download completed",
    });
    const userState = await this.readLocalUserState(pointer.record.target_ref);

    return {
      pointer,
      session,
      download,
      userState,
    };
  }

  async readLocalUserState(contentId) {
    const info = await this.client.getContentInfo({ contentId });
    const contentRange = await this.client.readContentRange({
      contentId,
      startByte: 0,
      endByte: info.size_bytes,
    });
    return decodeJsonFromBase64(contentRange.data_base64);
  }

  addFriendToProfile({
    profile,
    friendVirtualNodeId = null,
    friendPublicKey = null,
  }) {
    return {
      ...profile,
      friend_virtual_node_ids: normalizeUniqueStrings([
        ...(profile.friend_virtual_node_ids || []),
        friendVirtualNodeId,
      ]),
      friend_public_keys: normalizeUniqueStrings([
        ...(profile.friend_public_keys || []),
        friendPublicKey,
      ]),
      updated_at: new Date().toISOString(),
    };
  }

  async sendDirectMessageToVirtualNode({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey = null,
    text,
  }) {
    const session = await this.getOrCreateDirectSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
      remotePublicKey,
    });
    await this.sendDirectMessage({
      sessionId: session.sessionId,
      fromVirtualNodeId: localVirtualNodeId,
      toVirtualNodeId: remoteVirtualNodeId,
      text,
    });
    return session;
  }

  async resolveDirectSessionId({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey = null,
  }) {
    const session = await this.getOrCreateDirectSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
      remotePublicKey,
    });
    return session.sessionId;
  }

  async getOrCreateDirectSession({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey = null,
  }) {
    const existingSessionId = this.sessionStore.get(remoteVirtualNodeId);
    if (existingSessionId) {
      return {
        sessionId: existingSessionId,
        reused: true,
      };
    }

    const session = await this.startDirectSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
      remotePublicKey,
    });
    return {
      sessionId: this.sessionStore.set(remoteVirtualNodeId, session.session_id),
      reused: false,
    };
  }

  async sendDirectMessage({
    sessionId,
    fromVirtualNodeId,
    toVirtualNodeId,
    text,
  }) {
    const message = createDirectMessage({
      fromVirtualNodeId,
      toVirtualNodeId,
      text,
    });

    return this.client.sendVirtualMessage({
      sessionId,
      appMessageType: SOCIAL_DIRECT_MESSAGE_TYPE,
      payload: message,
    });
  }

  async startDirectSession({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey = null,
  }) {
    if (remotePublicKey) {
      await this.client.upsertRemoteVirtualNode({
        nodeId: remoteVirtualNodeId,
        publicKey: remotePublicKey,
        kind: "social",
        metadata: {
          app: "anonnet-poc",
          source: "friend_list",
        },
      });
    }

    return this.client.startVirtualSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
    });
  }
}

window.SocialService = SocialService;

function assertDhtPublishCompleted(result, label) {
  const status = result?.status;
  if (status === "stored" || status === "stored_locally") {
    return;
  }

  const error = new Error(
    `Nao foi possivel publicar o registro ${label} na DHT. Status: ${status || "desconhecido"}.`,
  );
  error.code = `${label.toLowerCase()}_publish_failed`;
  error.publishResult = result;
  throw error;
}

async function waitForValue(loader, { timeoutMs, label }) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await loader();
    if (value) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out waiting for ${label}.`);
}
}
