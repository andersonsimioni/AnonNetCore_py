import {
  SOCIAL_APP_ID,
  SOCIAL_DIRECT_MESSAGE_TYPE,
  SOCIAL_PROFILE_CONTENT_TYPE,
  SOCIAL_PROFILE_DPT_TITLE,
  buildProfileDptLogicalKey,
  createDirectMessage,
  createProfile,
  createUserState,
  encodeJsonToBase64,
  normalizeUniqueStrings,
} from "./social-models.js";
import { SocialSessionStore } from "./social-session-store.js";

export class SocialService {
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

    const userState = createUserState({
      profile,
      feedPosts,
    });
    const content = await this.client.storeContent({
      dataBase64: encodeJsonToBase64(userState),
      title: SOCIAL_PROFILE_DPT_TITLE,
      contentType: SOCIAL_PROFILE_CONTENT_TYPE,
      tags: ["profile", "social", "user-state"],
    });

    return { profile, userState, content };
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

  async publishLocalProfilePointer({ localVirtualNode, profileContentId }) {
    return this.client.dhtPublish({
      namespace: "dpt",
      logicalKey: buildProfileDptLogicalKey(localVirtualNode.id),
      record: {
        app: "anonnet-poc",
        schema: "anonnet.social.profile_pointer.v1",
        app_id: SOCIAL_APP_ID,
        virtual_node_id: localVirtualNode.id,
        pk_virtual_node_owner: localVirtualNode.public_key,
        title: SOCIAL_PROFILE_DPT_TITLE,
        type: SOCIAL_PROFILE_CONTENT_TYPE,
        target_ref: profileContentId,
        updated_at: new Date().toISOString(),
      },
    });
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
