const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert/strict");

const PROJECT_ROOT = path.resolve(__dirname, "..", "..");
const POC_JS_ROOT = path.join(PROJECT_ROOT, "poc", "assets", "js");
const SOCIAL_MESSAGE_TEXT = "mensagem direta do smoke social";

loadBrowserScript("core/anonnet-client.js");
loadBrowserScript("social/models.js");
loadBrowserScript("social/session-store.js");
loadBrowserScript("social/service.js");

main().catch((error) => {
  console.error(`social flow smoke failed: ${error.stack || error.message}`);
  process.exitCode = 1;
});

async function main() {
  const coreA = createClient("CORE_A_HTTP", "http://127.0.0.1:18180");
  const coreB = createClient("CORE_B_HTTP", "http://127.0.0.1:18280");
  const socialA = new SocialService(coreA);
  const socialB = new SocialService(coreB);

  await waitForCore(coreA, "core A");
  await waitForCore(coreB, "core B");
  console.log("checkpoint 1 OK: cores A/B HTTP APIs are ready");

  const vnA = getPreparedVirtualNode("CORE_A_VN") || await socialA.createLocalProfileNode();
  const vnB = getPreparedVirtualNode("CORE_B_VN") || await socialB.createLocalProfileNode();
  assert.equal(vnA.kind, "social");
  assert.equal(vnB.kind, "social");
  console.log(`checkpoint 2 OK: social VNs created: A=${vnA.id} B=${vnB.id}`);

  await coreA.subscribeVirtualMessages({
    appMessageType: window.AnonNetSocialModels.SOCIAL_DIRECT_MESSAGE_TYPE,
  });
  console.log("checkpoint 3 OK: core A subscribed to social direct messages");

  const profileA = window.AnonNetSocialModels.createProfile({
    virtualNodeId: vnA.id,
    publicKey: vnA.public_key,
    displayName: "Smoke Alice",
    bio: "Perfil A publicado via DDT e DPT assinada.",
    friendVirtualNodeIds: [vnB.id],
    friendPublicKeys: [vnB.public_key],
  });
  const postA = window.AnonNetSocialModels.createFeedPost({
    authorVirtualNodeId: vnA.id,
    authorName: profileA.display_name,
    text: "primeiro post publico do smoke",
  });
  const publicationA = await socialA.publishLocalUserState({
    localVirtualNode: vnA,
    profile: profileA,
    feedPosts: [postA],
  });
  assert.equal(publicationA.dpt.record.target_ref, publicationA.content.content_id);
  console.log(`checkpoint 4 OK: profile A published: content=${publicationA.content.content_id}`);

  const profileB = window.AnonNetSocialModels.createProfile({
    virtualNodeId: vnB.id,
    publicKey: vnB.public_key,
    displayName: "Smoke Bob",
    bio: "Perfil B local do smoke.",
    friendVirtualNodeIds: [vnA.id],
    friendPublicKeys: [vnA.public_key],
  });
  await socialB.publishLocalUserState({
    localVirtualNode: vnB,
    profile: profileB,
    feedPosts: [],
  });
  console.log("checkpoint 5 OK: profile B published");

  const pointerA = await waitForValue(
    () => socialB.resolveProfilePointer({
      virtualNodeId: vnA.id,
      publicKey: vnA.public_key,
    }).catch(() => null),
    { timeoutMs: 60000, label: "profile A DPT visible from core B" },
  );
  assert.equal(pointerA.record.target_ref, publicationA.content.content_id);
  console.log("checkpoint 6 OK: core B resolved and verified profile A DPT");

  const downloadedA = await socialB.downloadUserStateFromPointer({
    localVirtualNodeId: vnB.id,
    remoteVirtualNodeId: vnA.id,
    remotePublicKey: vnA.public_key,
  });
  assert.equal(downloadedA.userState.profile.virtual_node_id, vnA.id);
  assert.equal(downloadedA.userState.feed_posts[0].text, postA.text);
  console.log("checkpoint 7 OK: core B downloaded profile A user state through virtual content");

  const dmSession = await socialB.sendDirectMessageToVirtualNode({
    localVirtualNodeId: vnB.id,
    remoteVirtualNodeId: vnA.id,
    remotePublicKey: vnA.public_key,
    text: SOCIAL_MESSAGE_TEXT,
  });
  assert.ok(dmSession.sessionId);
  console.log(`checkpoint 8 OK: core B sent DM through virtual session: session=${dmSession.sessionId}`);

  const receivedMessage = await waitForValue(
    async () => {
      const messages = await coreA.readVirtualMessages({
        appMessageType: window.AnonNetSocialModels.SOCIAL_DIRECT_MESSAGE_TYPE,
        limit: 20,
        consume: false,
      });
      return messages.find((message) => message.payload?.text === SOCIAL_MESSAGE_TEXT) || null;
    },
    { timeoutMs: 60000, label: "direct message received by core A" },
  );
  assert.equal(receivedMessage.payload.from_virtual_node_id, vnB.id);
  assert.equal(receivedMessage.payload.to_virtual_node_id, vnA.id);
  console.log("checkpoint 9 OK: core A received social direct message");

  console.log("OK poc social JS smoke passed");
}

function createClient(envName, fallbackUrl) {
  return new AnonNetClient({
    httpBaseUrl: process.env[envName] || fallbackUrl,
    webSocketFactory: () => {
      throw new Error("WebSocket is not used by this smoke.");
    },
  });
}

function getPreparedVirtualNode(envPrefix) {
  const id = process.env[`${envPrefix}_ID`];
  const publicKey = process.env[`${envPrefix}_PUBLIC_KEY`];
  if (!id || !publicKey) {
    return null;
  }
  return {
    id,
    public_key: publicKey,
    kind: "social",
  };
}

async function waitForCore(client, label) {
  await waitForValue(
    () => client.getStatus().catch(() => null),
    { timeoutMs: 60000, label },
  );
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

function loadBrowserScript(relativePath) {
  global.window = global;
  const scriptPath = path.join(POC_JS_ROOT, relativePath);
  const source = fs.readFileSync(scriptPath, "utf-8");
  vm.runInThisContext(source, { filename: scriptPath });
}
