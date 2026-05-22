{
const {
  SOCIAL_DIRECT_MESSAGE_TYPE,
  createFeedPost,
  createProfile,
} = window.AnonNetSocialModels;
const {
  collectSocialFeedPosts,
  createSocialProfileState,
  ensureSocialContact,
  normalizeSocialProfileState,
} = window.AnonNetSocialRuntime;

const LOCAL_STATE_KEY = "anonnet.poc.social.local_state.v2";
const LEGACY_LOCAL_STATE_KEY = "anonnet.poc.social.local_state.v1";
const EVENT_TEXT_LIMIT = 220;
const EVENT_OBJECT_KEY_LIMIT = 12;
const state = createState();
const client = new AnonNetClient();
const sessionStore = new SocialSessionStore();
const socialService = new SocialService(client, { sessionStore });

const elements = {
  appFeedback: document.querySelector("#app-feedback"),
  appLoading: document.querySelector("#app-loading"),
  appLoadingTitle: document.querySelector("#app-loading-title"),
  appLoadingMessage: document.querySelector("#app-loading-message"),
  connectionPill: document.querySelector("#connection-pill"),
  statusOutput: document.querySelector("#core-status"),
  profileSelect: document.querySelector("#active-profile-select"),
  createProfileButton: document.querySelector("#create-profile-button"),
  localVnOutput: document.querySelector("#local-vn-output"),
  profileAvatar: document.querySelector("#profile-avatar"),
  composerAvatar: document.querySelector("#composer-avatar"),
  profileName: document.querySelector("#profile-name"),
  profileBio: document.querySelector("#profile-bio"),
  friendCount: document.querySelector("#friend-count"),
  friendSummary: document.querySelector("#friend-summary"),
  postCount: document.querySelector("#post-count"),
  directMessageSummary: document.querySelector("#direct-message-summary"),
  directMessageList: document.querySelector("#direct-message-list"),
  friendList: document.querySelector("#friend-list"),
  feedList: document.querySelector("#feed-list"),
  eventLog: document.querySelector("#event-log"),
  profileForm: document.querySelector("#profile-form"),
  profilePhotoInput: document.querySelector("#profile-photo"),
  postTemplate: document.querySelector("#post-template"),
};
let activeOperationCount = 0;

document.addEventListener("submit", (event) => event.preventDefault(), { capture: true });

document.querySelector("#refresh-status").addEventListener("click", refreshStatus);
document.querySelector("#reset-site-cache").addEventListener("click", resetSiteCache);
elements.createProfileButton.addEventListener("click", createLocalProfile);
elements.profileSelect.addEventListener("change", selectActiveProfile);
elements.profileForm.addEventListener("submit", saveProfile);
document.querySelector("#friend-form").addEventListener("submit", addFriend);
document.querySelector("#message-form").addEventListener("submit", sendMessage);
document.querySelector("#post-form").addEventListener("submit", publishLocalPost);
elements.profilePhotoInput.addEventListener("change", previewProfilePhoto);

restoreLocalState();
const backgroundSync = new SocialBackgroundSyncService({
  socialService,
  getActiveProfile,
  saveLocalState,
  render,
  appendEvent,
  appendError,
});
connectEvents();
render();
backgroundSync.start();

async function refreshStatus() {
  const operation = startUiOperation({
    name: "refresh_status",
    title: "Consultando core",
    message: "Lendo o estado atual do core local.",
  });
  try {
    const status = await client.getStatus();
    elements.statusOutput.textContent = JSON.stringify(status, null, 2);
    markCoreStatus("online");
    notifyUser("success", "Core online", "Status do core atualizado.");
  } catch (error) {
    elements.statusOutput.textContent = error.message;
    markCoreStatus("offline");
    appendError(error);
  } finally {
    endUiOperation(operation);
  }
}

function resetSiteCache() {
  if (!window.confirm("Limpar todos os dados locais deste site? Perfis, amigos, posts e DMs salvos no navegador serao removidos.")) {
    return;
  }

  localStorage.removeItem(LOCAL_STATE_KEY);
  localStorage.removeItem(LEGACY_LOCAL_STATE_KEY);
  state.activeProfileId = null;
  state.profiles = {};
  state.events = [];
  elements.eventLog.replaceChildren();
  elements.statusOutput.textContent = 'Cache local do site limpo. Clique em "+ Novo perfil" para recomecar.';
  render();
  notifyUser("success", "Cache limpo", "Os dados locais da PoC foram removidos deste navegador.");
  logWeb("info", "site_cache_reset");
}

async function createLocalProfile(event) {
  event.preventDefault();
  const operation = startUiOperation({
    name: "create_local_profile",
    title: "Criando perfil",
    message: "Criando um virtual node social local.",
  });
  try {
    const virtualNode = await socialService.createLocalProfileNode();
    const profileState = createProfileState(virtualNode);
    state.profiles[virtualNode.id] = profileState;
    state.activeProfileId = virtualNode.id;
    saveLocalState();
    appendEvent({ type: "local_profile_created", data: { id: virtualNode.id } });
    render();
    notifyUser("success", "Perfil criado", "O VN social local foi criado e selecionado.");
  } catch (error) {
    appendError(error);
  } finally {
    endUiOperation(operation);
  }
}

function selectActiveProfile(event) {
  const profileId = event.currentTarget.value;
  state.activeProfileId = profileId || null;
  saveLocalState();
  render();
  runBackgroundSync("profile_selected");
}

async function saveProfile(event) {
  event.preventDefault();
  const active = getActiveProfile();
  if (!active) {
    appendEvent({ type: "poc_error", data: { message: "Crie ou selecione um perfil primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  const operation = startUiOperation({
    name: "save_profile",
    title: "Salvando perfil",
    message: "Salvando estado local. A rede sera sincronizada em background.",
  });
  try {
    const profile = createProfile({
      virtualNodeId: active.localVirtualNode.id,
      publicKey: active.localVirtualNode.public_key,
      displayName: form.get("displayName")?.toString().trim(),
      bio: form.get("bio")?.toString().trim(),
      photoContentId: active.profile?.photo_content_id || null,
      photoDataUrl: active.profilePhotoPreview || active.profile?.photo_data_url || null,
      friendVirtualNodeIds: active.profile?.friend_virtual_node_ids || [],
    });
    active.profilePhotoPreview = profile.photo_data_url || active.profilePhotoPreview || null;
    active.profile = profile;
    saveLocalState();
    render();
    appendEvent({
      type: "profile_save_started",
      data: {
        profileId: active.localVirtualNode.id,
        displayName: profile.display_name,
      },
    });

    runBackgroundSync("profile_saved");
    notifyUser("success", "Perfil salvo", "O background sync vai publicar o estado na rede.");
  } catch (error) {
    saveLocalState();
    renderProfile();
    appendError(error);
  } finally {
    endUiOperation(operation);
  }
}

function addFriend(event) {
  event.preventDefault();
  const active = getActiveProfile();
  if (!active?.profile) {
    appendEvent({ type: "poc_error", data: { message: "Salve o perfil ativo primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  const friendVirtualNodeId = form.get("friendVirtualNodeId")?.toString().trim();
  if (!friendVirtualNodeId) {
    return;
  }

  const operation = startUiOperation({
    name: "add_friend",
    title: "Adicionando amigo",
    message: "Atualizando seu perfil e republicando o estado social.",
  });
  active.profile = socialService.addFriendToProfile({
    profile: active.profile,
    friendVirtualNodeId,
  });
  const contactResult = ensureSocialContact(active, friendVirtualNodeId);
  saveLocalState();
  appendEvent({
    type: contactResult.created ? "friend_added" : "friend_already_exists",
    data: { friendVirtualNodeId },
  });
  event.currentTarget.reset();
  render();
  notifyUser("success", "Amigo adicionado", "O background sync vai atualizar a DPT/DDT e o feed.");
  runBackgroundSync("friend_added")
    .finally(() => endUiOperation(operation));
}

async function sendMessage(event) {
  event.preventDefault();
  const messageForm = event.currentTarget;
  const active = getActiveProfile();
  if (!active) {
    appendEvent({ type: "poc_error", data: { message: "Crie ou selecione um perfil primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  const toVirtualNodeId = form.get("toVirtualNodeId")?.toString().trim();
  const text = form.get("text")?.toString().trim();
  if (!toVirtualNodeId || !text) {
    return;
  }

  const operation = startUiOperation({
    name: "send_direct_message",
    title: "Enviando mensagem",
    message: "Abrindo ou reutilizando sessao virtual para enviar a DM.",
  });
  try {
    appendEvent({
      type: "direct_message_send_started",
      data: {
        fromProfileId: active.localVirtualNode.id,
        toVirtualNodeId,
      },
    });
    const result = await socialService.sendDirectMessageToVirtualNode({
      localVirtualNodeId: active.localVirtualNode.id,
      remoteVirtualNodeId: toVirtualNodeId,
      text,
    });
    active.directMessages.unshift({
      to_virtual_node_id: toVirtualNodeId,
      text,
      sent_at: new Date().toISOString(),
    });
    saveLocalState();
    render();
    appendEvent({
      type: "direct_message_sent",
      data: {
        fromProfileId: active.localVirtualNode.id,
        toVirtualNodeId,
        session_reused: result.reused,
      },
    });
    messageForm.reset();
    notifyUser("success", "Mensagem enviada", "A mensagem direta foi entregue ao core.");
  } catch (error) {
    appendError(error);
  } finally {
    endUiOperation(operation);
  }
}

async function publishLocalPost(event) {
  event.preventDefault();
  const active = getActiveProfile();
  if (!active?.profile) {
    appendEvent({ type: "poc_error", data: { message: "Crie um perfil e salve os dados primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  const text = form.get("postText")?.toString().trim();
  if (!text) {
    return;
  }

  const operation = startUiOperation({
    name: "publish_local_post",
    title: "Publicando post",
    message: "Salvando o post local. A rede sera sincronizada em background.",
  });
  active.feedPosts.unshift(createFeedPost({
    authorVirtualNodeId: active.localVirtualNode.id,
    authorName: active.profile.display_name || "Voce",
    authorPhotoDataUrl: active.profile.photo_data_url || active.profilePhotoPreview || null,
    text,
  }));
  saveLocalState();
  render();
  event.currentTarget.reset();
  try {
    appendEvent({
      type: "post_created",
      data: {
        profileId: active.localVirtualNode.id,
        postCount: active.feedPosts.length,
      },
    });
    runBackgroundSync("post_created");
    render();
    notifyUser("success", "Post salvo", "O background sync vai atualizar seu estado publicado.");
  } catch (error) {
    saveLocalState();
    renderProfile();
    appendError(error);
  } finally {
    endUiOperation(operation);
  }
}

function previewProfilePhoto(event) {
  const active = getActiveProfile();
  const file = event.target.files?.[0];
  if (!active || !file) {
    return;
  }

  const reader = new FileReader();
  reader.addEventListener("load", () => {
    active.profilePhotoPreview = reader.result;
    saveLocalState();
    renderProfile();
  });
  reader.readAsDataURL(file);
}

function connectEvents() {
  try {
    const websocket = client.connectEvents({
      eventTypes: [
        "content_download_requested",
        "content_download_completed",
        "content_provider_published",
      ],
      appMessageTypes: [SOCIAL_DIRECT_MESSAGE_TYPE],
      onEvent: handleRealtimeEvent,
    });
    websocket.addEventListener("open", () => markCoreStatus("events online"));
    websocket.addEventListener("close", () => markCoreStatus("offline"));
    websocket.addEventListener("error", () => markCoreStatus("offline"));
  } catch (error) {
    appendError(error);
  }
}

async function runBackgroundSync(reason) {
  try {
    await backgroundSync.runOnce({ reason });
  } catch (error) {
    appendError(error);
  }
}

function handleRealtimeEvent(event) {
  appendEvent(event);
  if (event.type !== "virtual_message_received") {
    return;
  }

  const message = event.data?.payload || event.data;
  if (!message || message.schema !== "anonnet.social.direct_message.v1") {
    return;
  }
  const targetProfile = findProfileForMessage(message) || getActiveProfile();
  if (!targetProfile) {
    return;
  }

  targetProfile.directMessages.unshift({
    ...message,
    received_at: event.data?.received_at || new Date().toISOString(),
  });
  saveLocalState();
  render();
  notifyUser(
    "success",
    "Nova mensagem direta",
    `${shortKey(message.from_virtual_node_id)}: ${message.text || ""}`,
  );
}

function render() {
  renderProfileSelector();
  renderProfile();
  renderFriends();
  renderFeed();
  renderDirectMessages();
}

function renderProfileSelector() {
  elements.profileSelect.replaceChildren();
  elements.profileSelect.append(new Option("Nenhum perfil", ""));

  for (const profileState of Object.values(state.profiles)) {
    const label = buildProfileLabel(profileState);
    elements.profileSelect.append(new Option(label, profileState.localVirtualNode.id));
  }

  elements.profileSelect.value = state.activeProfileId || "";
}

function renderProfile() {
  const active = getActiveProfile();
  const profile = active?.profile;
  const name = profile?.display_name || "Seu perfil";
  const bio = profile?.bio || "Crie ou selecione um perfil social para iniciar.";
  const initials = getInitials(name);
  const friends = profile?.friend_virtual_node_ids?.length || active?.contacts.length || 0;
  const profilePhoto = active?.profilePhotoPreview || profile?.photo_data_url || null;

  elements.profileName.textContent = name;
  elements.profileBio.textContent = bio;
  setAvatarImage(elements.profileAvatar, profilePhoto, initials);
  setAvatarImage(elements.composerAvatar, profilePhoto, initials);
  elements.friendCount.textContent = String(friends);
  elements.friendSummary.textContent = String(friends);
  elements.postCount.textContent = String(active?.feedPosts.length || 0);
  elements.localVnOutput.textContent = active?.localVirtualNode.id || "Nenhum perfil criado";

  elements.profileForm.elements.displayName.value = profile?.display_name || "";
  elements.profileForm.elements.bio.value = profile?.bio || "";

}

function saveLocalState() {
  localStorage.setItem(
    LOCAL_STATE_KEY,
    JSON.stringify({
      activeProfileId: state.activeProfileId,
      profiles: state.profiles,
      events: state.events,
    }),
  );
}

function restoreLocalState() {
  const savedState = loadSavedState();
  if (!savedState) {
    return;
  }

  state.activeProfileId = savedState.activeProfileId || null;
  state.profiles = normalizeProfiles(savedState.profiles || {});
  state.events = Array.isArray(savedState.events) ? savedState.events : [];

  if (!state.activeProfileId || !state.profiles[state.activeProfileId]) {
    state.activeProfileId = Object.keys(state.profiles)[0] || null;
  }
}

function loadSavedState() {
  const currentState = localStorage.getItem(LOCAL_STATE_KEY);
  if (currentState) {
    try {
      return JSON.parse(currentState);
    } catch {
      localStorage.removeItem(LOCAL_STATE_KEY);
    }
  }

  const legacyState = localStorage.getItem(LEGACY_LOCAL_STATE_KEY);
  if (!legacyState) {
    return null;
  }

  try {
    return migrateLegacyState(JSON.parse(legacyState));
  } catch {
    localStorage.removeItem(LEGACY_LOCAL_STATE_KEY);
    return null;
  }
}

function migrateLegacyState(legacyState) {
  if (!legacyState.localVirtualNode?.id) {
    return null;
  }

  const profileState = createProfileState(legacyState.localVirtualNode);
  profileState.profile = legacyState.localProfile || null;
  profileState.userStateContent = legacyState.localUserStateContent || null;
  profileState.profilePointer = null;
  profileState.profilePhotoPreview = legacyState.profilePhotoPreview || null;
  profileState.contacts = Array.isArray(legacyState.contacts) ? legacyState.contacts : [];
  profileState.directMessages = Array.isArray(legacyState.directMessages)
    ? legacyState.directMessages
    : [];
  profileState.feedPosts = Array.isArray(legacyState.feedPosts) ? legacyState.feedPosts : [];

  return {
    activeProfileId: legacyState.localVirtualNode.id,
    profiles: {
      [legacyState.localVirtualNode.id]: profileState,
    },
    events: [],
  };
}

function renderFriends() {
  const active = getActiveProfile();
  elements.friendList.replaceChildren();
  for (const contact of active?.contacts || []) {
    const item = document.createElement("div");
    item.className = "friend-item";
    const avatarInitials = getInitials(contact.display_name);
    item.innerHTML = `
      <div class="friend-avatar">${avatarInitials}</div>
      <div>
        <strong>${escapeHtml(contact.display_name)}</strong>
        <span>${escapeHtml(contact.virtual_node_id)}</span>
        <small>${escapeHtml(buildFriendMeta(contact))}</small>
      </div>
      <i class="friend-status ${contact.status === "sincronizado" ? "online" : ""}"></i>
    `;
    setAvatarImage(item.querySelector(".friend-avatar"), contact.photo_data_url, avatarInitials);
    item.addEventListener("click", () => selectFriendForMessage(contact));
    elements.friendList.append(item);
  }
}

function renderFeed() {
  const active = getActiveProfile();
  elements.feedList.replaceChildren();
  for (const post of collectFeedPosts(active)) {
    const node = elements.postTemplate.content.firstElementChild.cloneNode(true);
    setAvatarImage(
      node.querySelector(".post-avatar"),
      post.author_photo_data_url,
      getInitials(post.author_name),
    );
    node.querySelector(".post-author").textContent = post.author_name;
    node.querySelector(".post-meta").textContent = formatTime(post.created_at);
    node.querySelector(".post-text").textContent = post.text;
    elements.feedList.append(node);
  }
}

function renderDirectMessages() {
  const active = getActiveProfile();
  const messages = active?.directMessages || [];
  elements.directMessageSummary.textContent = String(messages.length);
  elements.directMessageList.replaceChildren();

  if (!messages.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "Nenhuma DM recebida ou enviada neste perfil.";
    elements.directMessageList.append(empty);
    return;
  }

  for (const message of messages.slice(0, 20)) {
    const incoming = message.to_virtual_node_id === active.localVirtualNode.id;
    const item = document.createElement("article");
    item.className = `direct-message-item ${incoming ? "incoming" : "outgoing"}`;
    item.innerHTML = `
      <strong>${incoming ? "Recebida" : "Enviada"}</strong>
      <span>${escapeHtml(resolveDirectMessagePeer(message, active))}</span>
      <p>${escapeHtml(message.text || "")}</p>
      <small>${escapeHtml(formatTime(message.received_at || message.sent_at))}</small>
    `;
    elements.directMessageList.append(item);
  }
}

function createProfileState(localVirtualNode) {
  return createSocialProfileState(localVirtualNode);
}

function normalizeProfiles(profiles) {
  const normalized = {};
  for (const [profileId, profileState] of Object.entries(profiles)) {
    if (!profileState?.localVirtualNode?.id) {
      continue;
    }

    normalized[profileId] = normalizeSocialProfileState(profileState);
  }
  return normalized;
}

function getActiveProfile() {
  if (!state.activeProfileId) {
    return null;
  }
  return state.profiles[state.activeProfileId] || null;
}

function selectFriendForMessage(contact) {
  const form = document.querySelector("#message-form");
  form.elements.toVirtualNodeId.value = contact.virtual_node_id || "";
  document.querySelector("#message-text").focus();
}

function collectFeedPosts(active) {
  if (!active) {
    return [];
  }

  return collectSocialFeedPosts(active);
}

function buildFriendMeta(contact) {
  const postCount = contact.feed_posts?.length || 0;
  if (contact.last_synced_at) {
    return `${postCount} posts sincronizados - ${formatTime(contact.last_synced_at)}`;
  }
  return "aguardando sincronizacao pela DHT";
}

function resolveDirectMessagePeer(message, active) {
  const peerId = message.to_virtual_node_id === active.localVirtualNode.id
    ? message.from_virtual_node_id
    : message.to_virtual_node_id;
  const contact = active.contacts.find((item) => item.virtual_node_id === peerId);
  return contact?.display_name || shortKey(peerId);
}

function findProfileForMessage(message) {
  const profileId = message?.to_virtual_node_id || message?.local_virtual_node_id;
  if (profileId && state.profiles[profileId]) {
    return state.profiles[profileId];
  }
  return null;
}

function buildProfileLabel(profileState) {
  const name = profileState.profile?.display_name?.trim();
  if (name) {
    return name;
  }
  return `Perfil ${shortKey(profileState.localVirtualNode.id)}`;
}

function appendEvent(event) {
  const compactEvent = compactEventForUi(event);
  logWeb("info", compactEvent.type, compactEvent.data);
  state.events.unshift(compactEvent);
  state.events = state.events.slice(0, 30);

  const item = document.createElement("li");
  item.textContent = `${compactEvent.type}: ${JSON.stringify(compactEvent.data || {})}`;
  elements.eventLog.prepend(item);

  while (elements.eventLog.children.length > 30) {
    elements.eventLog.lastElementChild.remove();
  }
}

function appendError(error) {
  const message = error.message || "Erro desconhecido.";
  logWeb("error", "poc_error", {
    code: error.code,
    message,
    status: error.status,
    publishResult: error.publishResult,
    payload: error.payload,
  });
  notifyUser("error", "Acao nao concluida", message);
  appendEvent({
    type: "poc_error",
    data: {
      code: error.code,
      message,
    },
  });
}

function startUiOperation({ name, title, message }) {
  activeOperationCount += 1;
  elements.appLoadingTitle.textContent = title;
  elements.appLoadingMessage.textContent = message;
  elements.appLoading.hidden = false;
  document.body.classList.add("ui-busy");
  logWeb("info", `${name}_started`, { title, message });
  return {
    name,
    startedAt: performance.now(),
  };
}

function endUiOperation(operation) {
  activeOperationCount = Math.max(0, activeOperationCount - 1);
  const elapsedMs = operation ? Math.round(performance.now() - operation.startedAt) : null;
  if (operation) {
    logWeb("info", `${operation.name}_finished`, { elapsedMs });
  }

  if (activeOperationCount > 0) {
    return;
  }

  elements.appLoading.hidden = true;
  document.body.classList.remove("ui-busy");
}

function notifyUser(type, title, message) {
  const item = document.createElement("div");
  item.className = `feedback-message ${type}`;
  item.innerHTML = `
    <strong>${escapeHtml(title)}</strong>
    <span>${escapeHtml(message)}</span>
  `;
  elements.appFeedback.prepend(item);

  while (elements.appFeedback.children.length > 4) {
    elements.appFeedback.lastElementChild.remove();
  }
  setTimeout(() => item.remove(), type === "error" ? 12000 : 6500);
}

function logWeb(level, eventType, data = {}) {
  const logger = console[level] || console.log;
  logger.call(console, `[AnonNet PoC] ${eventType}`, data);
}

function compactEventForUi(event) {
  return {
    type: event?.type || "event",
    data: compactEventValue(event?.data || {}),
  };
}

function compactEventValue(value, depth = 0) {
  if (value === null || value === undefined) {
    return value;
  }
  if (typeof value === "string") {
    return compactText(value);
  }
  if (typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (depth >= 2) {
    return "{...}";
  }

  const compact = {};
  const entries = Object.entries(value).slice(0, EVENT_OBJECT_KEY_LIMIT);
  for (const [key, item] of entries) {
    if (isHeavyEventField(key)) {
      compact[key] = summarizeHeavyEventField(item);
      continue;
    }
    compact[key] = compactEventValue(item, depth + 1);
  }
  const hiddenKeyCount = Object.keys(value).length - entries.length;
  if (hiddenKeyCount > 0) {
    compact.more = `${hiddenKeyCount} campos ocultos`;
  }
  return compact;
}

function isHeavyEventField(key) {
  return [
    "public_key",
    "pk_physical_node",
    "pk_virtual_node",
    "pk_virtual_node_owner",
    "record_json",
    "responsible_nodes",
    "signature",
  ].includes(key);
}

function summarizeHeavyEventField(value) {
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (typeof value === "string") {
    return compactText(value);
  }
  if (value && typeof value === "object") {
    return "{...}";
  }
  return value;
}

function compactText(value) {
  if (value.length <= EVENT_TEXT_LIMIT) {
    return value;
  }
  return `${value.slice(0, EVENT_TEXT_LIMIT)}...(${value.length} chars)`;
}

function markCoreStatus(status) {
  elements.connectionPill.textContent = status;
  elements.connectionPill.classList.toggle("online", status !== "offline");
}

function getInitials(value) {
  return value
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join("") || "?";
}

function setAvatarImage(element, imageDataUrl, fallbackText) {
  element.textContent = imageDataUrl ? "" : fallbackText;
  element.style.backgroundImage = imageDataUrl ? `url("${imageDataUrl}")` : "";
  element.style.backgroundSize = imageDataUrl ? "cover" : "";
  element.style.backgroundPosition = imageDataUrl ? "center" : "";
}

function formatTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "agora";
  }
  return new Intl.DateTimeFormat("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "short",
  }).format(date);
}

function shortKey(value) {
  if (!value) {
    return "friend";
  }
  return value.slice(0, 16);
}

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
}
