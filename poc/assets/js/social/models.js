const SOCIAL_APP_ID = "anonnet.social";
const SOCIAL_PROFILE_DPT_TITLE = "profile";
const SOCIAL_PROFILE_CONTENT_TYPE = "application/anonnet-social-user-state+json";
const SOCIAL_DIRECT_MESSAGE_TYPE = "social.direct_message";
const SOCIAL_FEED_POST_TYPE = "social.feed_post";

function buildProfileDptLogicalKey(virtualNodeId) {
  return `${SOCIAL_APP_ID}|${virtualNodeId}`;
}

function createProfile({
  virtualNodeId,
  publicKey,
  displayName,
  bio = "",
  photoContentId = null,
  friendVirtualNodeIds = [],
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
    updated_at: new Date().toISOString(),
  };
}

function createDirectMessage({
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

function createFeedPost({
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

function createUserState({
  profile,
  feedPosts = [],
}) {
  return {
    schema: "anonnet.social.user_state.v1",
    app_id: SOCIAL_APP_ID,
    profile,
    feed_posts: feedPosts,
    updated_at: new Date().toISOString(),
  };
}

function encodeJsonToBase64(value) {
  return encodeBase64Utf8(JSON.stringify(value));
}

function decodeJsonFromBase64(value) {
  return JSON.parse(decodeBase64Utf8(value));
}

function normalizeUniqueStrings(values) {
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

window.AnonNetSocialModels = {
  SOCIAL_APP_ID,
  SOCIAL_PROFILE_DPT_TITLE,
  SOCIAL_PROFILE_CONTENT_TYPE,
  SOCIAL_DIRECT_MESSAGE_TYPE,
  SOCIAL_FEED_POST_TYPE,
  buildProfileDptLogicalKey,
  createProfile,
  createDirectMessage,
  createFeedPost,
  createUserState,
  encodeJsonToBase64,
  decodeJsonFromBase64,
  normalizeUniqueStrings,
};
