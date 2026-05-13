import { AnonNetClient } from "../../shared/anonnet-client.js";
import { SocialService } from "../../shared/social-service.js";
import { createState } from "./state.js";
import {
  SOCIAL_DIRECT_MESSAGE_TYPE,
  createFeedPost,
} from "../../shared/social-models.js";

const client = new AnonNetClient();
const socialService = new SocialService(client);
const state = createState();

const elements = {
  connectionPill: document.querySelector("#connection-pill"),
  statusOutput: document.querySelector("#core-status"),
  localVnOutput: document.querySelector("#local-vn-output"),
  profileAvatar: document.querySelector("#profile-avatar"),
  composerAvatar: document.querySelector("#composer-avatar"),
  profileName: document.querySelector("#profile-name"),
  profileBio: document.querySelector("#profile-bio"),
  friendCount: document.querySelector("#friend-count"),
  friendSummary: document.querySelector("#friend-summary"),
  postCount: document.querySelector("#post-count"),
  friendList: document.querySelector("#friend-list"),
  feedList: document.querySelector("#feed-list"),
  eventLog: document.querySelector("#event-log"),
  postTemplate: document.querySelector("#post-template"),
};

document.addEventListener("submit", (event) => event.preventDefault(), { capture: true });

document.querySelector("#refresh-status").addEventListener("click", refreshStatus);
document.querySelector("#local-vn-form").addEventListener("submit", createLocalVirtualNode);
document.querySelector("#profile-form").addEventListener("submit", saveProfile);
document.querySelector("#friend-form").addEventListener("submit", addFriend);
document.querySelector("#message-form").addEventListener("submit", sendMessage);
document.querySelector("#post-form").addEventListener("submit", publishLocalPost);
document.querySelector("#profile-photo").addEventListener("change", previewProfilePhoto);

connectEvents();
render();

async function refreshStatus() {
  try {
    const status = await client.getStatus();
    elements.statusOutput.textContent = JSON.stringify(status, null, 2);
    markCoreStatus("online");
  } catch (error) {
    elements.statusOutput.textContent = error.message;
    markCoreStatus("offline");
  }
}

async function createLocalVirtualNode(event) {
  event.preventDefault();
  try {
    const virtualNode = await socialService.createLocalProfileNode();
    state.localVirtualNode = virtualNode;
    appendEvent({ type: "local_virtual_node_created", data: { id: virtualNode.id } });
    render();
  } catch (error) {
    appendError(error);
  }
}

async function saveProfile(event) {
  event.preventDefault();
  if (!state.localVirtualNode) {
    appendEvent({ type: "poc_error", data: { message: "Crie um VN local primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  try {
    const result = await socialService.saveLocalProfile({
      localVirtualNode: state.localVirtualNode,
      displayName: form.get("displayName")?.toString().trim(),
      bio: form.get("bio")?.toString().trim(),
      photoContentId: state.localProfile?.photo_content_id || null,
      friendVirtualNodeIds: state.localProfile?.friend_virtual_node_ids || [],
      friendPublicKeys: state.localProfile?.friend_public_keys || [],
    });

    state.localProfile = result.profile;
    appendEvent({ type: "profile_saved", data: { content_id: result.content.content_id } });
    render();
  } catch (error) {
    appendError(error);
  }
}

function addFriend(event) {
  event.preventDefault();
  if (!state.localProfile) {
    appendEvent({ type: "poc_error", data: { message: "Salve um perfil local primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  const friendVirtualNodeId = form.get("friendVirtualNodeId")?.toString().trim();
  const friendPublicKey = form.get("friendPublicKey")?.toString().trim();
  if (!friendVirtualNodeId && !friendPublicKey) {
    return;
  }

  state.localProfile = socialService.addFriendToProfile({
    profile: state.localProfile,
    friendVirtualNodeId,
    friendPublicKey,
  });
  state.contacts.unshift({
    virtual_node_id: friendVirtualNodeId || shortKey(friendPublicKey),
    display_name: `Friend ${state.contacts.length + 1}`,
    public_key: friendPublicKey || null,
    status: "online",
  });
  appendEvent({ type: "friend_added_locally", data: { friendVirtualNodeId } });
  event.currentTarget.reset();
  render();
}

async function sendMessage(event) {
  event.preventDefault();
  if (!state.localVirtualNode) {
    appendEvent({ type: "poc_error", data: { message: "Crie um VN local primeiro." } });
    return;
  }

  const form = new FormData(event.currentTarget);
  const toVirtualNodeId = form.get("toVirtualNodeId")?.toString().trim();
  const text = form.get("text")?.toString().trim();
  if (!toVirtualNodeId || !text) {
    return;
  }

  try {
    const contact = state.contacts.find((item) => item.virtual_node_id === toVirtualNodeId);
    const result = await socialService.sendDirectMessageToVirtualNode({
      localVirtualNodeId: state.localVirtualNode.id,
      toVirtualNodeId,
      remoteVirtualNodeId: toVirtualNodeId,
      remotePublicKey: contact?.public_key || null,
      text,
    });
    state.directMessages.unshift({
      to_virtual_node_id: toVirtualNodeId,
      text,
      sent_at: new Date().toISOString(),
    });
    appendEvent({
      type: "direct_message_sent",
      data: {
        toVirtualNodeId,
        session_id: result.sessionId,
      },
    });
    event.currentTarget.reset();
  } catch (error) {
    appendError(error);
  }
}

function publishLocalPost(event) {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const text = form.get("postText")?.toString().trim();
  if (!text) {
    return;
  }

  const post = createFeedPost({
    authorVirtualNodeId: state.localVirtualNode?.id || "local-demo",
    authorName: state.localProfile?.display_name || "Voce",
    text,
  });
  state.feedPosts.unshift(post);
  appendEvent({ type: "post_created_locally", data: { text } });
  event.currentTarget.reset();
  renderFeed();
  renderProfile();
}

function previewProfilePhoto(event) {
  const file = event.target.files?.[0];
  if (!file) {
    return;
  }

  const reader = new FileReader();
  reader.addEventListener("load", () => {
    state.profilePhotoPreview = reader.result;
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

function handleRealtimeEvent(event) {
  appendEvent(event);
  if (event.type !== "virtual_message_received") {
    return;
  }

  const message = event.data?.payload || event.data;
  state.directMessages.unshift(message);
}

function render() {
  renderProfile();
  renderFriends();
  renderFeed();
}

function renderProfile() {
  const profile = state.localProfile;
  const name = profile?.display_name || "Seu perfil";
  const bio = profile?.bio || "Crie um VN social e salve seu perfil para iniciar.";
  const initials = getInitials(name);
  const friends = profile?.friend_virtual_node_ids?.length || state.contacts.length;

  elements.profileName.textContent = name;
  elements.profileBio.textContent = bio;
  elements.profileAvatar.textContent = state.profilePhotoPreview ? "" : initials;
  elements.composerAvatar.textContent = initials;
  elements.friendCount.textContent = String(friends);
  elements.friendSummary.textContent = String(friends);
  elements.postCount.textContent = String(state.feedPosts.length);
  elements.localVnOutput.textContent = state.localVirtualNode?.id || "Nenhum VN criado";

  if (state.profilePhotoPreview) {
    elements.profileAvatar.style.backgroundImage = `url("${state.profilePhotoPreview}")`;
    elements.profileAvatar.style.backgroundSize = "cover";
  }
}

function renderFriends() {
  elements.friendList.replaceChildren();
  for (const contact of state.contacts) {
    const item = document.createElement("div");
    item.className = "friend-item";
    item.innerHTML = `
      <div class="friend-avatar">${getInitials(contact.display_name)}</div>
      <div>
        <strong>${escapeHtml(contact.display_name)}</strong>
        <span>${escapeHtml(contact.virtual_node_id)}</span>
      </div>
      <i class="friend-status ${contact.status === "online" ? "online" : ""}"></i>
    `;
    elements.friendList.append(item);
  }
}

function renderFeed() {
  elements.feedList.replaceChildren();
  for (const post of state.feedPosts) {
    const node = elements.postTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector(".post-avatar").textContent = getInitials(post.author_name);
    node.querySelector(".post-author").textContent = post.author_name;
    node.querySelector(".post-meta").textContent = formatTime(post.created_at);
    node.querySelector(".post-text").textContent = post.text;
    elements.feedList.append(node);
  }
}

function appendEvent(event) {
  state.events.unshift(event);
  state.events = state.events.slice(0, 30);

  const item = document.createElement("li");
  item.textContent = `${event.type}: ${JSON.stringify(event.data || {})}`;
  elements.eventLog.prepend(item);

  while (elements.eventLog.children.length > 30) {
    elements.eventLog.lastElementChild.remove();
  }
}

function appendError(error) {
  appendEvent({
    type: "poc_error",
    data: {
      code: error.code,
      message: error.message,
    },
  });
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
