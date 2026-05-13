import assert from "node:assert/strict";
import { SocialService } from "../shared/social-service.js";
import { SocialSessionStore } from "../shared/social-session-store.js";
import { decodeJsonFromBase64 } from "../shared/social-models.js";

class FakeAnonNetClient {
  constructor() {
    this.createdVirtualNodes = 0;
    this.storedContent = [];
    this.remoteVirtualNodes = [];
    this.startedSessions = [];
    this.sentMessages = [];
  }

  async createLocalVirtualNode({ kind, metadata }) {
    this.createdVirtualNodes += 1;
    return {
      id: `local-vn-${this.createdVirtualNodes}`,
      public_key: `local-public-key-${this.createdVirtualNodes}`,
      kind,
      metadata,
    };
  }

  async storeContent({ dataBase64, title, contentType, tags }) {
    this.storedContent.push({ dataBase64, title, contentType, tags });
    return {
      content_id: `content-${this.storedContent.length}`,
      content_type: contentType,
      size_bytes: dataBase64.length,
    };
  }

  async dhtPublish({ namespace, logicalKey, record }) {
    return {
      status: "stored",
      namespace,
      logical_key: logicalKey,
      record,
    };
  }

  async upsertRemoteVirtualNode(payload) {
    this.remoteVirtualNodes.push(payload);
    return {
      id: payload.nodeId,
      public_key: payload.publicKey,
    };
  }

  async startVirtualSession({ localVirtualNodeId, remoteVirtualNodeId }) {
    const session = {
      session_id: `session-${this.startedSessions.length + 1}`,
      local_virtual_node_id: localVirtualNodeId,
      remote_virtual_node_id: remoteVirtualNodeId,
    };
    this.startedSessions.push(session);
    return session;
  }

  async sendVirtualMessage(payload) {
    this.sentMessages.push(payload);
    return {
      request_id: `request-${this.sentMessages.length}`,
    };
  }
}

const client = new FakeAnonNetClient();
const sessionStore = new SocialSessionStore();
const social = new SocialService(client, { sessionStore });

const localVirtualNode = await social.createLocalProfileNode();
assert.equal(localVirtualNode.kind, "social");

const profileResult = await social.saveLocalProfile({
  localVirtualNode,
  displayName: "Anderson",
  bio: "PoC social",
  friendVirtualNodeIds: ["remote-vn-1"],
});
assert.equal(profileResult.profile.display_name, "Anderson");
assert.deepEqual(profileResult.profile.friend_virtual_node_ids, ["remote-vn-1"]);

const storedProfile = decodeJsonFromBase64(client.storedContent[0].dataBase64);
assert.equal(storedProfile.schema, "anonnet.social.profile.v1");
assert.equal(storedProfile.virtual_node_id, localVirtualNode.id);

const updatedProfile = social.addFriendToProfile({
  profile: profileResult.profile,
  friendVirtualNodeId: "remote-vn-1",
  friendPublicKey: "remote-public-key-1",
});
assert.deepEqual(updatedProfile.friend_virtual_node_ids, ["remote-vn-1"]);
assert.deepEqual(updatedProfile.friend_public_keys, ["remote-public-key-1"]);

const firstMessage = await social.sendDirectMessageToVirtualNode({
  localVirtualNodeId: localVirtualNode.id,
  remoteVirtualNodeId: "remote-vn-1",
  remotePublicKey: "remote-public-key-1",
  text: "fala mano",
});
const secondMessage = await social.sendDirectMessageToVirtualNode({
  localVirtualNodeId: localVirtualNode.id,
  remoteVirtualNodeId: "remote-vn-1",
  remotePublicKey: "remote-public-key-1",
  text: "segunda mensagem",
});

assert.equal(firstMessage.sessionId, "session-1");
assert.equal(secondMessage.sessionId, "session-1");
assert.equal(client.startedSessions.length, 1);
assert.equal(client.remoteVirtualNodes.length, 1);
assert.equal(client.sentMessages.length, 2);
assert.equal(client.sentMessages[0].appMessageType, "social.direct_message");
assert.equal(client.sentMessages[0].payload.to_virtual_node_id, "remote-vn-1");
assert.equal(sessionStore.get("remote-vn-1"), "session-1");

console.log("OK poc social smoke passed");
