class SocialBackgroundSyncService {
  constructor({
    socialService,
    getActiveProfile,
    saveLocalState,
    render,
    appendEvent,
    appendError,
    intervalMs = 60000,
  }) {
    this.socialService = socialService;
    this.getActiveProfile = getActiveProfile;
    this.saveLocalState = saveLocalState;
    this.render = render;
    this.appendEvent = appendEvent;
    this.appendError = appendError;
    this.intervalMs = intervalMs;
    this.timer = null;
    this.running = false;
    this.pendingRuns = [];
    this.currentRun = null;
  }

  start() {
    if (this.timer !== null) {
      return;
    }

    logBackgroundSyncInfo("started", {
      intervalMs: this.intervalMs,
    });
    this.runOnce({ reason: "page_load" });
    this.timer = setInterval(() => {
      this.runOnce({ reason: "background_interval" });
    }, this.intervalMs);
  }

  stop() {
    if (this.timer === null) {
      return;
    }
    clearInterval(this.timer);
    this.timer = null;
    logBackgroundSyncInfo("stopped");
  }

  async runOnce({ reason = "manual" } = {}) {
    this.pendingRuns.push(reason);
    if (this.running) {
      logBackgroundSyncDebug("queued_while_running", {
        reason,
        pendingCount: this.pendingRuns.length,
      });
      return this.currentRun;
    }

    this.currentRun = this.drainQueue();
    return this.currentRun;
  }

  async drainQueue() {
    this.running = true;
    try {
      while (this.pendingRuns.length > 0) {
        const reason = this.pendingRuns.shift();
        await this.runSingle(reason);
      }
    } finally {
      this.running = false;
      this.currentRun = null;
    }
  }

  async runSingle(reason) {
    const profileState = this.getActiveProfile();
    if (!profileState?.profile || !profileState.localVirtualNode) {
      logBackgroundSyncDebug("skipped_profile_not_ready", { reason });
      return;
    }

    logBackgroundSyncInfo("run_started", {
      reason,
      profileId: profileState.localVirtualNode.id,
    });
    try {
      await this.withRetry(
        "local_profile_sync",
        () => this.syncLocalProfile(profileState, reason),
        { profileId: profileState.localVirtualNode.id, reason },
      );
      await this.syncFriends(profileState, reason);
      this.saveLocalState();
      this.render();
      logBackgroundSyncInfo("run_finished", {
        reason,
        profileId: profileState.localVirtualNode.id,
      });
    } catch (error) {
      logBackgroundSyncInfo("run_failed", {
        reason,
        profileId: profileState.localVirtualNode.id,
        code: error.code,
        message: error.message,
      });
      this.appendError(error);
    }
  }

  async withRetry(label, operation, context) {
    let lastError = null;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        const retry = attempt < 3 && isRetryableBackgroundSyncError(error);
        logBackgroundSyncInfo("retryable_step_failed", {
          label,
          attempt,
          retry,
          code: error.code,
          message: error.message,
          ...context,
        });
        if (!retry) {
          break;
        }
        await sleepBackgroundSync(Math.min(1500 * attempt, 5000));
      }
    }
    throw lastError;
  }

  async syncLocalProfile(profileState, reason) {
    logBackgroundSyncDebug("local_profile_sync_started", {
      reason,
      profileId: profileState.localVirtualNode.id,
      currentContentId: profileState.userStateContent?.content_id || null,
    });
    const result = await this.socialService.ensureLocalUserStatePublished({
      localVirtualNode: profileState.localVirtualNode,
      profile: profileState.profile,
      feedPosts: profileState.feedPosts,
      userStateContent: profileState.userStateContent,
    });

    if (result.status !== "published") {
      logBackgroundSyncInfo("local_profile_sync_checked", {
        reason,
        profileId: profileState.localVirtualNode.id,
        status: result.status,
      });
      this.appendEvent({
        type: "profile_background_sync_checked",
        data: {
          reason,
          profileId: profileState.localVirtualNode.id,
          status: result.status,
        },
      });
      return;
    }

    profileState.profile = result.profile || profileState.profile;
    profileState.userStateContent = result.content;
    profileState.profilePointer = result.dpt;
    logBackgroundSyncInfo("local_profile_published", {
      reason,
      profileId: profileState.localVirtualNode.id,
      contentId: result.content.content_id,
      dptKey: result.dpt.dhtKey,
    });
    this.appendEvent({
      type: "profile_background_published",
      data: {
        reason,
        profileId: profileState.localVirtualNode.id,
        contentId: result.content.content_id,
        dptKey: result.dpt.dhtKey,
      },
    });
  }

  async syncFriends(profileState, reason) {
    this.ensureContactsFromProfile(profileState);
    const contacts = profileState.contacts.filter((contact) => contact.virtual_node_id);
    logBackgroundSyncDebug("friends_sync_started", {
      reason,
      profileId: profileState.localVirtualNode.id,
      contactCount: contacts.length,
    });
    if (!contacts.length) {
      return;
    }

    for (const contact of contacts) {
      try {
        await this.syncFriend(profileState, contact, reason);
      } catch (error) {
        logBackgroundSyncInfo("friend_sync_failed", {
          reason,
          friend: contact.virtual_node_id,
          code: error.code,
          message: error.message,
        });
        this.appendEvent({
          type: "friend_background_sync_failed",
          data: {
            reason,
            friend: contact.virtual_node_id,
            code: error.code,
            message: error.message,
          },
        });
      }
    }
  }

  ensureContactsFromProfile(profileState) {
    const profile = profileState.profile;
    const friendIds = Array.isArray(profile?.friend_virtual_node_ids)
      ? profile.friend_virtual_node_ids
      : [];
    const contactsById = new Map(
      profileState.contacts
        .filter((contact) => contact.virtual_node_id)
        .map((contact) => [contact.virtual_node_id, contact]),
    );

    for (let index = 0; index < friendIds.length; index += 1) {
      const friendId = friendIds[index];
      if (contactsById.has(friendId)) {
        continue;
      }

      profileState.contacts.push({
        virtual_node_id: friendId,
        display_name: `Amigo ${profileState.contacts.length + 1}`,
        public_key: null,
        status: "pendente",
        feed_posts: [],
        user_state_content_id: null,
        last_synced_at: null,
      });
      logBackgroundSyncDebug("friend_contact_created_from_profile", {
        profileId: profileState.localVirtualNode.id,
        friendId,
      });
    }
  }

  async syncFriend(profileState, contact, reason) {
    logBackgroundSyncDebug("friend_sync_started", {
      reason,
      profileId: profileState.localVirtualNode.id,
      friend: contact.virtual_node_id,
    });
    const result = await this.socialService.downloadUserStateFromPointer({
      localVirtualNodeId: profileState.localVirtualNode.id,
      remoteVirtualNodeId: contact.virtual_node_id,
    });
    const remoteProfile = result.userState.profile || {};

    contact.display_name = remoteProfile.display_name || contact.display_name;
    contact.public_key = remoteProfile.public_key || contact.public_key;
    contact.status = "sincronizado";
    contact.bio = remoteProfile.bio || "";
    contact.feed_posts = Array.isArray(result.userState.feed_posts)
      ? result.userState.feed_posts
      : [];
    contact.user_state_content_id = result.pointer.record.target_ref;
    contact.last_synced_at = new Date().toISOString();

    logBackgroundSyncInfo("friend_synced", {
      reason,
      friend: contact.virtual_node_id,
      posts: contact.feed_posts.length,
      contentId: contact.user_state_content_id,
    });
    this.appendEvent({
      type: "friend_background_synced",
      data: {
        reason,
        friend: contact.virtual_node_id,
        posts: contact.feed_posts.length,
        contentId: contact.user_state_content_id,
      },
    });
  }
}

window.SocialBackgroundSyncService = SocialBackgroundSyncService;

function logBackgroundSyncInfo(eventType, data = {}) {
  console.info("[AnonNet Social Sync]", eventType, data);
}

function logBackgroundSyncDebug(eventType, data = {}) {
  console.debug("[AnonNet Social Sync]", eventType, data);
}

function isRetryableBackgroundSyncError(error) {
  return [
    "api_request_timeout",
    "api_request_failed",
    "virtual_route_not_found",
    "ddt_publish_failed",
    "dpt_publish_failed",
  ].includes(error.code);
}

function sleepBackgroundSync(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
