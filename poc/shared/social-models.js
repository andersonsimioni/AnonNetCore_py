export const SOCIAL_APP_ID = "anonnet.social";
export const SOCIAL_PROFILE_DPT_TITLE = "profile";
export const SOCIAL_PROFILE_CONTENT_TYPE = "application/anonnet-social-profile+json";
export const SOCIAL_DIRECT_MESSAGE_TYPE = "social.direct_message";
export const SOCIAL_FEED_POST_TYPE = "social.feed_post";

export function buildProfileDptLogicalKey(virtualNodeId) {
  return `${SOCIAL_APP_ID}|${virtualNodeId}|${SOCIAL_PROFILE_DPT_TITLE}`;
}

export function createProfile({
  virtualNodeId,
  publicKey,
  displayName,
  bio = "",
  photoContentId = null,
  friendVirtualNodeIds = [],
  friendPublicKeys = [],
}) {
  return {
    schema: "anonnet.social.profile.v1",
    app_id: SOCIAL_APP_ID,
    virtual_node_id: virtualNodeId,
    public_key: publicKey,
    display_name: displayName,
    bio,
    photo_content_id: photoContentId,
    friend_virtual_node_ids: normalizeUniqueStrings(friendVirtualNodeIds),
    friend_public_keys: normalizeUniqueStrings(friendPublicKeys),
    updated_at: new Date().toISOString(),
  };
}

export function createDirectMessage({
  fromVirtualNodeId,
  toVirtualNodeId,
  text,
}) {
  return {
    schema: "anonnet.social.direct_message.v1",
    from_virtual_node_id: fromVirtualNodeId,
    to_virtual_node_id: toVirtualNodeId,
    text,
    sent_at: new Date().toISOString(),
  };
}

export function createFeedPost({
  authorVirtualNodeId,
  authorName,
  text,
}) {
  return {
    schema: "anonnet.social.feed_post.v1",
    app_id: SOCIAL_APP_ID,
    author_virtual_node_id: authorVirtualNodeId,
    author_name: authorName,
    text,
    created_at: new Date().toISOString(),
  };
}

export function encodeJsonToBase64(value) {
  return encodeBase64Utf8(JSON.stringify(value));
}

export function decodeJsonFromBase64(value) {
  return JSON.parse(decodeBase64Utf8(value));
}

export function normalizeUniqueStrings(values) {
  return [...new Set(
    values
      .map((value) => value?.toString().trim())
      .filter(Boolean),
  )];
}

function encodeBase64Utf8(value) {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(value, "utf-8").toString("base64");
  }
  return btoa(unescape(encodeURIComponent(value)));
}

function decodeBase64Utf8(value) {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(value, "base64").toString("utf-8");
  }
  return decodeURIComponent(escape(atob(value)));
}
