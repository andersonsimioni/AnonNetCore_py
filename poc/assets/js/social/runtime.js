function createSocialProfileState(localVirtualNode, overrides = {}) {
  return {
    localVirtualNode,
    profile: overrides.profile || null,
    userStateContent: overrides.userStateContent || null,
    profilePointer: overrides.profilePointer || null,
    profilePhotoPreview: overrides.profilePhotoPreview || null,
    contacts: Array.isArray(overrides.contacts) ? overrides.contacts : [],
    directMessages: Array.isArray(overrides.directMessages) ? overrides.directMessages : [],
    feedPosts: Array.isArray(overrides.feedPosts) ? overrides.feedPosts : [],
  };
}

function normalizeSocialProfileState(profileState) {
  return createSocialProfileState(profileState.localVirtualNode, profileState);
}

function ensureSocialContact(profileState, friendVirtualNodeId) {
  const existing = profileState.contacts.find((contact) => (
    contact.virtual_node_id === friendVirtualNodeId
  ));
  if (existing) {
    return { contact: existing, created: false };
  }

  const contact = {
    virtual_node_id: friendVirtualNodeId,
    display_name: `Amigo ${profileState.contacts.length + 1}`,
    public_key: null,
    status: "pendente",
    feed_posts: [],
    user_state_content_id: null,
    last_synced_at: null,
  };
  profileState.contacts.unshift(contact);
  return { contact, created: true };
}

function collectSocialFeedPosts(profileState) {
  if (!profileState) {
    return [];
  }

  const localPosts = (profileState.feedPosts || []).map((post) => ({ ...post, source: "local" }));
  const friendPosts = (profileState.contacts || []).flatMap((contact) => (
    contact.feed_posts || []
  ).map((post) => ({
    ...post,
    source: "friend",
    author_name: post.author_name || contact.display_name,
  })));

  return [...localPosts, ...friendPosts].sort((left, right) => (
    new Date(right.created_at).getTime() - new Date(left.created_at).getTime()
  ));
}

window.AnonNetSocialRuntime = {
  createSocialProfileState,
  normalizeSocialProfileState,
  ensureSocialContact,
  collectSocialFeedPosts,
};
