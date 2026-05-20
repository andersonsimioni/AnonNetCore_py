const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert/strict");

const PROJECT_ROOT = path.resolve(__dirname, "..", "..");
const POC_JS_ROOT = path.join(PROJECT_ROOT, "poc", "assets", "js");
const SOCIAL_MESSAGE_TEXT = `mensagem direta do smoke social ${Date.now()}`;
const SOCIAL_REPLY_TEXT = `resposta direta do smoke social ${Date.now()}`;

loadBrowserScript("core/anonnet-client.js");
loadBrowserScript("social/models.js");
loadBrowserScript("social/session-store.js");
loadBrowserScript("social/runtime.js");
loadBrowserScript("social/service.js");
loadBrowserScript("social/background-sync.js");

main().catch((error) => {
  console.error(`social flow smoke failed: ${error.stack || error.message}`);
  process.exitCode = 1;
});

async function main() {
  const coreA = createClient("CORE_A_HTTP", "http://127.0.0.1:18180");
  if (process.env.SOCIAL_SMOKE_MODE === "same-core") {
    await waitForCore(coreA, "core A");
    await coreA.subscribeVirtualMessages({
      appMessageType: window.AnonNetSocialModels.SOCIAL_DIRECT_MESSAGE_TYPE,
    });
    console.log("checkpoint 1 OK: same-core HTTP API is ready and subscribed");

    const localPair = await runSameCoreLocalPairScenario({
      client: coreA,
      label: "core A local pair",
    });
    assertContactHasPosts(localPair.stateA, localPair.vnB.id, ["post local do VN B no mesmo core"]);
    assertContactHasPosts(localPair.stateB, localPair.vnA.id, ["post local do VN A no mesmo core"]);
    console.log("checkpoint 2 OK: two local VNs on the same core exchanged posts and DM through the standard route flow");
    console.log("OK poc social JS smoke passed");
    return;
  }

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
  console.log(`checkpoint 2 OK: social VNs ready: A=${vnA.id} B=${vnB.id}`);

  await coreA.subscribeVirtualMessages({
    appMessageType: window.AnonNetSocialModels.SOCIAL_DIRECT_MESSAGE_TYPE,
  });
  await coreB.subscribeVirtualMessages({
    appMessageType: window.AnonNetSocialModels.SOCIAL_DIRECT_MESSAGE_TYPE,
  });
  console.log("checkpoint 3 OK: cores A/B subscribed to social direct messages");

  const stateA = createProfileRuntimeState({
    localVirtualNode: vnA,
    displayName: "Smoke Alice",
    bio: "Perfil A publicado via DDT e DPT assinada.",
    friendVirtualNodeIds: [vnB.id],
    firstPostText: "primeiro post publico da Alice",
  });
  const stateB = createProfileRuntimeState({
    localVirtualNode: vnB,
    displayName: "Smoke Bob",
    bio: "Perfil B publicado via DDT e DPT assinada.",
    friendVirtualNodeIds: [vnA.id],
    firstPostText: "primeiro post publico do Bob",
  });

  const duplicateFriendProfile = socialA.addFriendToProfile({
    profile: socialA.addFriendToProfile({
      profile: stateA.profile,
      friendVirtualNodeId: vnB.id,
    }),
    friendVirtualNodeId: vnB.id,
  });
  assert.equal(duplicateFriendProfile.friend_virtual_node_ids.length, 1);
  console.log("checkpoint 4 OK: friend list dedupe keeps one VN ID");

  const publicationA = await publishUserStateAndAssert({
    socialService: socialA,
    label: "profile A",
    profileState: stateA,
  });
  const publicationB = await publishUserStateAndAssert({
    socialService: socialB,
    label: "profile B",
    profileState: stateB,
  });
  console.log("checkpoint 5 OK: profiles A/B published through DDT and DPT");

  await waitForProfilePointer({
    socialService: socialB,
    virtualNodeId: vnA.id,
    publicKey: vnA.public_key,
    expectedTargetRef: publicationA.content.content_id,
    label: "profile A DPT visible from core B",
  });
  await waitForProfilePointer({
    socialService: socialA,
    virtualNodeId: vnB.id,
    publicKey: vnB.public_key,
    expectedTargetRef: publicationB.content.content_id,
    label: "profile B DPT visible from core A",
  });
  console.log("checkpoint 6 OK: both cores resolve and verify friend DPT records");

  await downloadAndAssertUserState({
    socialService: socialB,
    localVirtualNodeId: vnB.id,
    remoteVirtualNodeId: vnA.id,
    expectedProfileId: vnA.id,
    expectedPostTexts: ["primeiro post publico da Alice"],
    label: "core B downloads Alice",
  });
  await downloadAndAssertUserState({
    socialService: socialA,
    localVirtualNodeId: vnA.id,
    remoteVirtualNodeId: vnB.id,
    expectedProfileId: vnB.id,
    expectedPostTexts: ["primeiro post publico do Bob"],
    label: "core A downloads Bob",
  });
  console.log("checkpoint 7 OK: both friends download each other through virtual content");

  await assertDdtHasHolder({
    client: coreB,
    contentId: publicationA.content.content_id,
    label: "Alice content DDT after Bob download",
  });
  console.log("checkpoint 8 OK: downloaded content is visible through DDT holders");

  const syncA = createBackgroundSync("A", socialA, stateA);
  const syncB = createBackgroundSync("B", socialB, stateB);
  await syncA.runOnce({ reason: "smoke_initial_sync" });
  await syncB.runOnce({ reason: "smoke_initial_sync" });
  assertContactHasPosts(stateA, vnB.id, ["primeiro post publico do Bob"]);
  assertContactHasPosts(stateB, vnA.id, ["primeiro post publico da Alice"]);
  console.log("checkpoint 9 OK: background sync builds feeds from friend DPT/DDT records");

  const secondPostA = window.AnonNetSocialModels.createFeedPost({
    authorVirtualNodeId: vnA.id,
    authorName: stateA.profile.display_name,
    text: "segundo post publico da Alice",
  });
  stateA.feedPosts.unshift(secondPostA);
  await syncA.runOnce({ reason: "smoke_alice_new_post" });
  const updatedPointerA = await waitForProfilePointer({
    socialService: socialB,
    virtualNodeId: vnA.id,
    publicKey: vnA.public_key,
    expectedDifferentFrom: publicationA.content.content_id,
    label: "profile A DPT points to updated content",
  });
  assert.notEqual(updatedPointerA.record.target_ref, publicationA.content.content_id);
  await waitForValue(async () => {
    await syncB.runOnce({ reason: "smoke_wait_alice_new_post" });
    return contactHasPosts(stateB, vnA.id, [
      "primeiro post publico da Alice",
      "segundo post publico da Alice",
    ]);
  }, { timeoutMs: 60000, label: "Bob feed sees Alice updated DPT/DDT state" });
  console.log("checkpoint 10 OK: DPT target moves after post update and friend feed refreshes");

  const dmSession = await socialB.sendDirectMessageToVirtualNode({
    localVirtualNodeId: vnB.id,
    remoteVirtualNodeId: vnA.id,
    text: SOCIAL_MESSAGE_TEXT,
  });
  assert.ok(dmSession.sessionId);
  const reusedDmSession = await socialB.sendDirectMessageToVirtualNode({
    localVirtualNodeId: vnB.id,
    remoteVirtualNodeId: vnA.id,
    text: `${SOCIAL_MESSAGE_TEXT} reuse`,
  });
  assert.equal(reusedDmSession.reused, true);
  assert.equal(reusedDmSession.sessionId, dmSession.sessionId);
  console.log(`checkpoint 11 OK: core B sends and reuses DM session: session=${dmSession.sessionId}`);

  const receivedMessage = await waitForDirectMessage({
    client: coreA,
    text: SOCIAL_MESSAGE_TEXT,
    fromVirtualNodeId: vnB.id,
    toVirtualNodeId: vnA.id,
    label: "direct message received by core A",
  });
  assert.equal(receivedMessage.payload.from_virtual_node_id, vnB.id);
  assert.equal(receivedMessage.payload.to_virtual_node_id, vnA.id);
  console.log("checkpoint 12 OK: core A received social direct message");

  const replySession = await socialA.sendDirectMessageToVirtualNode({
    localVirtualNodeId: vnA.id,
    remoteVirtualNodeId: vnB.id,
    text: SOCIAL_REPLY_TEXT,
  });
  assert.ok(replySession.sessionId);
  await waitForDirectMessage({
    client: coreB,
    text: SOCIAL_REPLY_TEXT,
    fromVirtualNodeId: vnA.id,
    toVirtualNodeId: vnB.id,
    label: "direct reply received by core B",
  });
  console.log("checkpoint 13 OK: direct messages work in both directions");

  const localPair = await runSameCoreLocalPairScenario({
    client: coreA,
    label: "core A local pair",
  });
  assertContactHasPosts(localPair.stateA, localPair.vnB.id, ["post local do VN B no mesmo core"]);
  assertContactHasPosts(localPair.stateB, localPair.vnA.id, ["post local do VN A no mesmo core"]);
  console.log("checkpoint 14 OK: two local VNs on the same core read posts through DHT/content flow");

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

function createProfileRuntimeState({
  localVirtualNode,
  displayName,
  bio,
  friendVirtualNodeIds,
  firstPostText,
}) {
  const profile = window.AnonNetSocialModels.createProfile({
    virtualNodeId: localVirtualNode.id,
    publicKey: localVirtualNode.public_key,
    displayName,
    bio,
    friendVirtualNodeIds,
  });
  const firstPost = window.AnonNetSocialModels.createFeedPost({
    authorVirtualNodeId: localVirtualNode.id,
    authorName: profile.display_name,
    text: firstPostText,
  });
  return window.AnonNetSocialRuntime.createSocialProfileState(localVirtualNode, {
    profile,
    contacts: friendVirtualNodeIds.map((friendVirtualNodeId, index) => ({
      virtual_node_id: friendVirtualNodeId,
      display_name: `Friend ${index + 1}`,
      public_key: null,
      status: "pendente",
      feed_posts: [],
      user_state_content_id: null,
      last_synced_at: null,
    })),
    feedPosts: [firstPost],
  });
}

async function runSameCoreLocalPairScenario({ client, label }) {
  const socialService = new SocialService(client);
  const vnA = getPreparedVirtualNode("CORE_A_LOCAL_VN_A") || await socialService.createLocalProfileNode();
  const vnB = getPreparedVirtualNode("CORE_A_LOCAL_VN_B") || await socialService.createLocalProfileNode();
  const stateA = createProfileRuntimeState({
    localVirtualNode: vnA,
    displayName: "Local Alice",
    bio: `${label} A`,
    friendVirtualNodeIds: [vnB.id],
    firstPostText: "post local do VN A no mesmo core",
  });
  const stateB = createProfileRuntimeState({
    localVirtualNode: vnB,
    displayName: "Local Bob",
    bio: `${label} B`,
    friendVirtualNodeIds: [vnA.id],
    firstPostText: "post local do VN B no mesmo core",
  });
  const syncA = createBackgroundSync("local A", socialService, stateA);
  const syncB = createBackgroundSync("local B", socialService, stateB);

  await syncA.runOnce({ reason: "same_core_publish_a" });
  await syncB.runOnce({ reason: "same_core_publish_b" });
  await waitForProfilePointer({
    socialService,
    virtualNodeId: vnA.id,
    publicKey: vnA.public_key,
    label: "same-core VN A DPT visible",
  });
  await waitForProfilePointer({
    socialService,
    virtualNodeId: vnB.id,
    publicKey: vnB.public_key,
    label: "same-core VN B DPT visible",
  });
  await syncA.runOnce({ reason: "same_core_sync_a_reads_b" });
  await syncB.runOnce({ reason: "same_core_sync_b_reads_a" });

  const dmText = `same-core DM ${Date.now()}`;
  await socialService.sendDirectMessageToVirtualNode({
    localVirtualNodeId: vnA.id,
    remoteVirtualNodeId: vnB.id,
    text: dmText,
  });
  await waitForDirectMessage({
    client,
    text: dmText,
    fromVirtualNodeId: vnA.id,
    toVirtualNodeId: vnB.id,
    label: "same-core direct message delivered",
  });
  return { vnA, vnB, stateA, stateB };
}

async function publishUserStateAndAssert({ socialService, label, profileState }) {
  const publication = await socialService.publishLocalUserState({
    localVirtualNode: profileState.localVirtualNode,
    profile: profileState.profile,
    feedPosts: profileState.feedPosts,
  });
  assert.equal(publication.dpt.record.target_ref, publication.content.content_id);
  profileState.profile = publication.profile;
  profileState.userStateContent = publication.content;
  profileState.profilePointer = publication.dpt;
  console.log(`${label} published: content=${publication.content.content_id}`);
  return publication;
}

async function waitForProfilePointer({
  socialService,
  virtualNodeId,
  publicKey,
  expectedTargetRef = null,
  expectedDifferentFrom = null,
  label,
}) {
  return waitForValue(
    async () => {
      const pointer = await socialService.resolveProfilePointer({
        virtualNodeId,
        publicKey,
      }).catch(() => null);
      if (!pointer) {
        return null;
      }
      if (expectedTargetRef && pointer.record.target_ref !== expectedTargetRef) {
        return null;
      }
      if (expectedDifferentFrom && pointer.record.target_ref === expectedDifferentFrom) {
        return null;
      }
      return pointer;
    },
    { timeoutMs: 60000, label },
  );
}

async function downloadAndAssertUserState({
  socialService,
  localVirtualNodeId,
  remoteVirtualNodeId,
  expectedProfileId,
  expectedPostTexts,
  label,
}) {
  const downloaded = await socialService.downloadUserStateFromPointer({
    localVirtualNodeId,
    remoteVirtualNodeId,
  });
  assert.equal(downloaded.userState.profile.virtual_node_id, expectedProfileId);
  assertPostTexts(downloaded.userState.feed_posts, expectedPostTexts, label);
  return downloaded;
}

async function assertDdtHasHolder({ client, contentId, label }) {
  const queryResult = await waitForValue(async () => {
    const result = await client.dhtQuery({
      namespace: "ddt",
      logicalKey: contentId,
    }).catch(() => null);
    if (result?.status !== "found" || !result.record_json) {
      return null;
    }
    return result;
  }, { timeoutMs: 60000, label });
  const record = JSON.parse(queryResult.record_json);
  assert.ok(Array.isArray(record.holders));
  assert.ok(record.holders.length >= 1);
}

function createBackgroundSync(label, socialService, profileState) {
  return new SocialBackgroundSyncService({
    socialService,
    getActiveProfile: () => profileState,
    saveLocalState: () => console.debug(`background ${label}: saveLocalState`),
    render: () => console.debug(`background ${label}: render`),
    appendEvent: (event) => console.log(`background ${label}: ${event.type}`),
    appendError: (error) => console.error(`background ${label}: ${error.message}`),
    intervalMs: 60000,
  });
}

function assertContactHasPosts(profileState, friendVirtualNodeId, expectedPostTexts) {
  assert.equal(contactHasPosts(profileState, friendVirtualNodeId, expectedPostTexts), true);
}

function contactHasPosts(profileState, friendVirtualNodeId, expectedPostTexts) {
  const contact = profileState.contacts.find((item) => item.virtual_node_id === friendVirtualNodeId);
  if (!contact) {
    return false;
  }
  return expectedPostTexts.every((text) => (
    contact.feed_posts.some((post) => post.text === text)
  ));
}

function assertPostTexts(posts, expectedPostTexts, label) {
  for (const expectedText of expectedPostTexts) {
    assert.ok(
      posts.some((post) => post.text === expectedText),
      `${label} missing expected post: ${expectedText}`,
    );
  }
}

async function waitForDirectMessage({
  client,
  text,
  fromVirtualNodeId,
  toVirtualNodeId,
  label,
}) {
  return waitForValue(
    async () => {
      const messages = await client.readVirtualMessages({
        appMessageType: window.AnonNetSocialModels.SOCIAL_DIRECT_MESSAGE_TYPE,
        limit: 50,
        consume: false,
      });
      return messages.find((message) => (
        message.payload?.text === text
        && message.payload?.from_virtual_node_id === fromVirtualNodeId
        && message.payload?.to_virtual_node_id === toVirtualNodeId
      )) || null;
    },
    { timeoutMs: 60000, label },
  );
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
