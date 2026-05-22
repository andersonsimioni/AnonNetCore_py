{
const {
  SOCIAL_DIRECT_MESSAGE_TYPE,
  SOCIAL_PROFILE_CONTENT_TYPE,
  SOCIAL_PROFILE_DPT_TITLE,
  buildProfileDptLogicalKey,
  createDirectMessage,
  createProfile,
  createUserState,
  decodeJsonFromBase64,
  encodeJsonToBase64,
  normalizeUniqueStrings,
} = window.AnonNetSocialModels;

class SocialService {
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
    photoDataUrl = null,
    friendVirtualNodeIds = [],
    feedPosts = [],
  }) {
    const profile = createProfile({
      virtualNodeId: localVirtualNode.id,
      publicKey: localVirtualNode.public_key,
      displayName,
      bio,
      photoContentId,
      photoDataUrl,
      friendVirtualNodeIds,
    });

    return this.saveUserState({
      localVirtualNode,
      profile,
      feedPosts,
    });
  }

  async saveUserState({
    localVirtualNode,
    profile,
    feedPosts = [],
  }) {
    logSocialDebug("save_user_state_started", {
      localVirtualNodeId: localVirtualNode.id,
      postCount: feedPosts.length,
      friendCount: profile?.friend_virtual_node_ids?.length || 0,
    });
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
    logSocialInfo("save_user_state_finished", {
      localVirtualNodeId: localVirtualNode.id,
      contentId: content.content_id,
      sizeBytes: content.size_bytes,
    });
    return {
      profile: userState.profile,
      userState,
      content,
    };
  }

  async publishLocalUserState({
    localVirtualNode,
    profile,
    feedPosts = [],
  }) {
    logSocialInfo("publish_local_user_state_started", {
      localVirtualNodeId: localVirtualNode.id,
      postCount: feedPosts.length,
      friendCount: profile?.friend_virtual_node_ids?.length || 0,
    });
    const savedState = await this.saveUserState({
      localVirtualNode,
      profile,
      feedPosts,
    });
    const ddt = await retrySocialOperation({
      label: "ddt_publish",
      operation: async () => {
        const publish = await this.client.publishContentProvider({
          contentId: savedState.content.content_id,
          localVirtualNodeId: localVirtualNode.id,
          asyncPublish: true,
        });
        publish.publish_result = await this.waitForDhtPublishResult({
          publishResult: publish.publish_result,
          label: "DDT",
        });
        assertDhtPublishCompleted(publish.publish_result, "DDT");
        return publish;
      },
      shouldRetry: isTransientDhtPublishFailure,
    });
    logSocialInfo("ddt_publish_finished", {
      localVirtualNodeId: localVirtualNode.id,
      contentId: savedState.content.content_id,
      logicalKey: ddt.logical_key,
      status: ddt.publish_result?.status,
      storedByCount: ddt.publish_result?.stored_by?.length || 0,
    });
    const dpt = await this.publishLocalProfilePointer({
      localVirtualNode,
      targetRef: ddt.logical_key,
    });
    logSocialInfo("publish_local_user_state_finished", {
      localVirtualNodeId: localVirtualNode.id,
      contentId: savedState.content.content_id,
      dptLogicalKey: dpt.logicalKey,
      dptStatus: dpt.publishResult?.status,
    });

    return {
      ...savedState,
      ddt,
      dpt,
    };
  }

  async ensureLocalUserStatePublished({
    localVirtualNode,
    profile,
    feedPosts = [],
    userStateContent = null,
  }) {
    logSocialDebug("ensure_local_user_state_published_started", {
      localVirtualNodeId: localVirtualNode.id,
      currentContentId: userStateContent?.content_id || null,
    });
    const syncState = await this.verifyLocalProfileSync({
      localVirtualNode,
      profile,
      feedPosts,
      userStateContent,
    }).catch((error) => {
      if (!["ddt_not_found", "content_not_found"].includes(error.code)) {
        throw error;
      }
      return {
        status: "out_of_sync",
        message: error.message,
      };
    });
    if (syncState.status === "synced") {
      logSocialInfo("local_user_state_already_synced", {
        localVirtualNodeId: localVirtualNode.id,
        contentId: userStateContent?.content_id || null,
      });
      return {
        status: "synced",
        pointer: syncState.pointer,
        content: userStateContent,
      };
    }

    const publication = await this.publishLocalUserState({
      localVirtualNode,
      profile,
      feedPosts,
    });
    return {
      status: "published",
      reason: syncState.status,
      ...publication,
    };
  }

  async publishLocalProfilePointer({ localVirtualNode, targetRef }) {
    const logicalKey = buildProfileDptLogicalKey(localVirtualNode.id);
    logSocialDebug("dpt_publish_started", {
      localVirtualNodeId: localVirtualNode.id,
      logicalKey,
      targetRef,
    });
    const dhtKey = await this.client.buildDhtKey({
      namespace: "dpt",
      logicalKey,
    });
    const lastModified = new Date().toISOString();
    const signedPayload = {
      key: dhtKey.key,
      pk_virtual_node_owner: localVirtualNode.public_key,
      title: SOCIAL_PROFILE_DPT_TITLE,
      type: SOCIAL_PROFILE_CONTENT_TYPE,
      last_modified: lastModified,
      target_ref: targetRef,
    };
    const signature = await this.client.signLocalVirtualNodePayload({
      localVirtualNodeId: localVirtualNode.id,
      payload: signedPayload,
    });
    const record = {
      pk_virtual_node_owner: localVirtualNode.public_key,
      title: SOCIAL_PROFILE_DPT_TITLE,
      type: SOCIAL_PROFILE_CONTENT_TYPE,
      last_modified: lastModified,
      target_ref: targetRef,
      signature: signature.signature_hex,
    };
    const publishResult = await retrySocialOperation({
      label: "dpt_publish",
      operation: async () => {
        const queued = await this.client.dhtPublishJob({
          namespace: "dpt",
          logicalKey,
          record,
        });
        const result = await this.waitForDhtPublishResult({
          publishResult: queued,
          label: "DPT",
        });
        assertDhtPublishCompleted(result, "DPT");
        return result;
      },
      shouldRetry: isTransientDhtPublishFailure,
    });
    logSocialInfo("dpt_publish_finished", {
      localVirtualNodeId: localVirtualNode.id,
      logicalKey,
      dhtKey: dhtKey.key,
      targetRef,
      status: publishResult?.status,
      storedByCount: publishResult?.stored_by?.length || 0,
    });
    return {
      logicalKey,
      dhtKey: dhtKey.key,
      record,
      publishResult,
    };
  }

  async waitForDhtPublishResult({
    publishResult,
    label,
    timeoutMs = 300000,
  }) {
    if (!publishResult?.job_id) {
      return publishResult;
    }

    logSocialInfo("dht_publish_job_wait_started", {
      label,
      jobId: publishResult.job_id,
      status: publishResult.status,
      namespace: publishResult.namespace,
      logicalKey: publishResult.logical_key,
    });
    let job;
    try {
      job = await waitForValue(async () => {
        const current = await this.client.getDhtPublishJob({
          jobId: publishResult.job_id,
        });
        logSocialDebug("dht_publish_job_polled", {
          label,
          jobId: current.job_id,
          status: current.status,
          attempt: current.attempt,
          maxAttempts: current.max_attempts,
          resultStatus: current.result?.status,
          storedCount: current.result?.stored_count,
          requiredStoredCount: current.result?.required_stored_count,
          reason: current.result?.reason,
        });
        if (current.status === "stored" || current.status === "failed") {
          return current;
        }
        return null;
      }, {
        timeoutMs,
        label: `${label} DHT publish job`,
      });
    } catch (error) {
      error.code = `${label.toLowerCase()}_publish_failed`;
      error.publishResult = {
        status: "timeout",
        reason: "dht_publish_job_wait_timeout",
      };
      throw error;
    }

    logSocialInfo("dht_publish_job_wait_finished", {
      label,
      jobId: job.job_id,
      status: job.status,
      resultStatus: job.result?.status,
      storedCount: job.result?.stored_count,
      requiredStoredCount: job.result?.required_stored_count,
      reason: job.result?.reason,
      error: job.error,
    });
    if (job.status !== "stored" || !job.result) {
      const error = new Error(
        `Nao foi possivel publicar o registro ${label} na DHT. Job: ${job.status}.`,
      );
      error.code = `${label.toLowerCase()}_publish_failed`;
      error.publishResult = job.result || {
        status: job.status,
        reason: job.error || "dht_publish_job_failed",
      };
      throw error;
    }
    return job.result;
  }

  async resolveProfilePointer({ virtualNodeId, publicKey }) {
    const logicalKey = buildProfileDptLogicalKey(virtualNodeId);
    logSocialDebug("dpt_query_started", {
      virtualNodeId,
      logicalKey,
      hasExpectedPublicKey: Boolean(publicKey),
    });
    const dhtKey = await this.client.buildDhtKey({
      namespace: "dpt",
      logicalKey,
    });
    const queryResult = await this.client.dhtQuery({
      namespace: "dpt",
      logicalKey,
    });
    logSocialInfo("dpt_query_finished", {
      virtualNodeId,
      logicalKey,
      dhtKey: dhtKey.key,
      status: queryResult.status,
      hasRecord: Boolean(queryResult.record_json),
    });
    if (queryResult.status !== "found" || !queryResult.record_json) {
      const error = new Error("DPT nao encontrada para este perfil. O background sync vai tentar publicar o estado local.");
      error.code = "dpt_not_found";
      error.queryResult = queryResult;
      throw error;
    }

    const record = JSON.parse(queryResult.record_json);
    await assertVirtualNodeIdMatchesPublicKey({
      virtualNodeId,
      publicKey: record.pk_virtual_node_owner,
    });
    logSocialDebug("dpt_owner_id_validated", {
      virtualNodeId,
      dhtKey: dhtKey.key,
    });
    const signedPayload = {
      key: dhtKey.key,
      pk_virtual_node_owner: record.pk_virtual_node_owner,
      title: record.title,
      type: record.type,
      last_modified: record.last_modified,
      target_ref: record.target_ref,
    };
    const verification = await this.client.verifyVirtualNodePayloadSignature({
      publicKey: publicKey || record.pk_virtual_node_owner,
      payload: signedPayload,
      signatureHex: record.signature,
    });
    if (!verification.valid) {
      throw new Error(`Assinatura DPT invalida para VN ${virtualNodeId}.`);
    }
    logSocialInfo("dpt_signature_validated", {
      virtualNodeId,
      logicalKey,
      targetRef: record.target_ref,
    });

    return {
      logicalKey,
      dhtKey: dhtKey.key,
      record,
      queryResult,
    };
  }

  async verifyLocalProfileSync({
    localVirtualNode,
    profile,
    feedPosts = [],
    userStateContent = null,
  }) {
    if (!localVirtualNode?.id || !localVirtualNode.public_key) {
      throw new Error("VN local invalido para verificar sincronizacao do perfil.");
    }
    if (!profile) {
      return {
        status: "not_ready",
        message: "Salve o perfil antes de verificar sincronizacao.",
      };
    }

    const pointer = await this.resolveProfilePointer({
      virtualNodeId: localVirtualNode.id,
      publicKey: localVirtualNode.public_key,
    }).catch((error) => {
      if (error.code !== "dpt_not_found") {
        throw error;
      }
      return null;
    });
    if (pointer === null) {
      logSocialInfo("profile_sync_state", {
        localVirtualNodeId: localVirtualNode.id,
        status: "out_of_sync",
        reason: "dpt_not_found",
      });
      return {
        status: "out_of_sync",
        message: "A DPT deste perfil ainda nao existe na DHT.",
      };
    }
    const localContentId = userStateContent?.content_id || null;
    if (!localContentId) {
      logSocialInfo("profile_sync_state", {
        localVirtualNodeId: localVirtualNode.id,
        status: "out_of_sync",
        reason: "missing_local_content_id",
        dptTargetRef: pointer.record.target_ref,
      });
      return {
        status: "out_of_sync",
        message: "Nao ha conteudo local publicado para comparar.",
        pointer,
      };
    }
    if (pointer.record.target_ref !== localContentId) {
      logSocialInfo("profile_sync_state", {
        localVirtualNodeId: localVirtualNode.id,
        status: "out_of_sync",
        reason: "target_ref_mismatch",
        localContentId,
        remoteContentId: pointer.record.target_ref,
      });
      return {
        status: "out_of_sync",
        message: "A DHT aponta para outro estado do perfil.",
        pointer,
        localContentId,
        remoteContentId: pointer.record.target_ref,
      };
    }

    const published = await this.readPublishedUserStateFromDht({
      localVirtualNode,
      contentId: pointer.record.target_ref,
    });
    const publishedUserState = published.userState;
    const localUserState = createUserStateSnapshot({ profile, feedPosts });
    const publishedSnapshot = createUserStateSnapshot({
      profile: publishedUserState.profile,
      feedPosts: publishedUserState.feed_posts,
    });
    if (stableJson(localUserState) !== stableJson(publishedSnapshot)) {
      logSocialInfo("profile_sync_state", {
        localVirtualNodeId: localVirtualNode.id,
        status: "out_of_sync",
        reason: "content_payload_mismatch",
        localContentId,
      });
      return {
        status: "out_of_sync",
        message: "O conteudo publicado nao bate com o estado local atual.",
        pointer,
        localContentId,
      };
    }

    logSocialInfo("profile_sync_state", {
      localVirtualNodeId: localVirtualNode.id,
      status: "synced",
      localContentId,
    });
    return {
      status: "synced",
      message: "Perfil local bate com o ponteiro DPT e o conteudo publicado.",
      pointer,
      localContentId,
    };
  }

  async loadPublishedLocalUserState({ localVirtualNode }) {
    if (!localVirtualNode?.id || !localVirtualNode.public_key) {
      throw new Error("VN local invalido para sincronizar perfil pela DHT.");
    }

    const pointer = await this.resolveProfilePointer({
      virtualNodeId: localVirtualNode.id,
      publicKey: localVirtualNode.public_key,
    });
    const published = await this.readPublishedUserStateFromDht({
      localVirtualNode,
      contentId: pointer.record.target_ref,
    });

    return {
      pointer,
      userState: published.userState,
      contentId: pointer.record.target_ref,
      source: published.source,
    };
  }

  async downloadUserStateFromPointer({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey,
  }) {
    logSocialInfo("download_user_state_from_dpt_started", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      hasRemotePublicKey: Boolean(remotePublicKey),
    });
    const pointer = await this.resolveProfilePointer({
      virtualNodeId: remoteVirtualNodeId,
      publicKey: remotePublicKey,
    });
    const resolvedRemotePublicKey = remotePublicKey || pointer.record.pk_virtual_node_owner;
    await this.client.upsertRemoteVirtualNode({
      nodeId: remoteVirtualNodeId,
      publicKey: resolvedRemotePublicKey,
      kind: "social",
      metadata: {
        app: "anonnet-poc",
        source: "social_profile_dpt",
      },
    });
    const session = await this.getOrCreateDirectSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
      remotePublicKey: resolvedRemotePublicKey,
    });
    const published = await this.downloadPublishedUserStateContent({
      sessionId: session.sessionId,
      contentId: pointer.record.target_ref,
    });
    logSocialInfo("download_user_state_from_dpt_finished", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      targetRef: pointer.record.target_ref,
      sessionId: session.sessionId,
      sessionReused: session.reused,
    });

    return {
      pointer,
      session,
      download: published.download,
      userState: published.userState,
    };
  }

  async readPublishedUserStateFromDht({ localVirtualNode, contentId }) {
    const localUserState = await this.readLocalUserState(contentId).catch((error) => {
      if (!isLocalContentMissing(error)) {
        throw error;
      }
      return null;
    });
    if (localUserState !== null) {
      logSocialDebug("read_published_user_state_source", {
        contentId,
        source: "local_content_cache",
      });
      return {
        source: "local_content_cache",
        userState: localUserState,
      };
    }

    const provider = await this.resolveContentProviderFromDdt({
      contentId,
      localVirtualNodeId: localVirtualNode.id,
    });
    const remoteNode = await this.client.upsertRemoteVirtualNode({
      publicKey: provider.pk_virtual_node,
      kind: "social",
      metadata: {
        app: "anonnet-poc",
        source: "ddt_holder",
      },
    });
    const session = await this.getOrCreateDirectSession({
      localVirtualNodeId: localVirtualNode.id,
      remoteVirtualNodeId: remoteNode.id,
      remotePublicKey: provider.pk_virtual_node,
    });
    const downloaded = await this.downloadPublishedUserStateContent({
      sessionId: session.sessionId,
      contentId,
    });
    logSocialInfo("read_published_user_state_source", {
      contentId,
      source: "ddt_holder_download",
      holderVirtualNodeId: remoteNode.id,
      sessionId: session.sessionId,
      sessionReused: session.reused,
    });
    return {
      source: "ddt_holder_download",
      holderVirtualNodeId: remoteNode.id,
      session,
      ...downloaded,
    };
  }

  async resolveContentProviderFromDdt({ contentId, localVirtualNodeId }) {
    logSocialDebug("ddt_query_started", {
      contentId,
      localVirtualNodeId,
    });
    const ddtKey = await this.client.buildDhtKey({
      namespace: "ddt",
      logicalKey: contentId,
    });
    const queryResult = await this.client.dhtQuery({
      namespace: "ddt",
      logicalKey: contentId,
    });
    logSocialInfo("ddt_query_finished", {
      contentId,
      dhtKey: ddtKey.key,
      status: queryResult.status,
      hasRecord: Boolean(queryResult.record_json),
    });
    if (queryResult.status !== "found" || !queryResult.record_json) {
      const error = new Error("DDT nao encontrada para o conteudo publicado pelo perfil.");
      error.code = "ddt_not_found";
      error.queryResult = queryResult;
      throw error;
    }

    const record = JSON.parse(queryResult.record_json);
    const holders = Array.isArray(record.holders) ? record.holders : [];
    logSocialDebug("ddt_holders_loaded", {
      contentId,
      holderCount: holders.length,
    });
    for (const holder of holders) {
      if (!holder?.pk_virtual_node || !holder.signature || !holder.expires_at) {
        continue;
      }
      if (isExpiredIso(holder.expires_at)) {
        continue;
      }

      const verification = await this.client.verifyVirtualNodePayloadSignature({
        publicKey: holder.pk_virtual_node,
        payload: {
          key: ddtKey.key,
          pk_virtual_node: holder.pk_virtual_node,
          expires_at: holder.expires_at,
        },
        signatureHex: holder.signature,
      });
      if (!verification.valid) {
        logSocialDebug("ddt_holder_signature_invalid", {
          contentId,
          expiresAt: holder.expires_at,
        });
        continue;
      }

      const remoteNode = await this.client.upsertRemoteVirtualNode({
        publicKey: holder.pk_virtual_node,
        kind: "social",
        metadata: {
          app: "anonnet-poc",
          source: "ddt_holder_probe",
        },
      });
      if (remoteNode.id === localVirtualNodeId) {
        logSocialDebug("ddt_holder_skipped_local_node", {
          contentId,
          localVirtualNodeId,
        });
        continue;
      }

      logSocialInfo("ddt_holder_selected", {
        contentId,
        holderVirtualNodeId: remoteNode.id,
        expiresAt: holder.expires_at,
      });
      return holder;
    }

    throw new Error("Nenhum holder remoto valido foi encontrado na DDT para baixar este conteudo.");
  }

  async downloadPublishedUserStateContent({ sessionId, contentId }) {
    logSocialInfo("content_download_started", {
      sessionId,
      contentId,
    });
    await this.client.startContentDownload({
      sessionId,
      contentId,
      ddtKey: contentId,
    });
    const download = await waitForValue(async () => {
      const state = await this.client.getContentDownload({
        sessionId,
        contentId,
      }).catch((error) => {
        if (error.code === "download_not_found") {
          return null;
        }
        throw error;
      });
      return state?.status === "completed" ? state : null;
    }, {
      timeoutMs: 60000,
      label: "content download completed",
    });
    logSocialInfo("content_download_finished", {
      sessionId,
      contentId,
      status: download.status,
      sizeBytes: download.size_bytes,
    });
    return {
      download,
      userState: await this.readLocalUserState(contentId),
    };
  }

  async readLocalUserState(contentId) {
    const info = await this.client.getContentInfo({ contentId });
    const contentRange = await this.client.readContentRange({
      contentId,
      startByte: 0,
      endByte: info.size_bytes,
    });
    return decodeJsonFromBase64(contentRange.data_base64);
  }

  addFriendToProfile({
    profile,
    friendVirtualNodeId,
  }) {
    return {
      ...profile,
      friend_virtual_node_ids: normalizeUniqueStrings([
        ...(profile.friend_virtual_node_ids || []),
        friendVirtualNodeId,
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
    logSocialInfo("direct_message_send_started", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      hasRemotePublicKey: Boolean(remotePublicKey),
      textLength: text?.length || 0,
    });
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
    logSocialInfo("direct_message_send_finished", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      sessionId: session.sessionId,
      sessionReused: session.reused,
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
    const existingSessionId = this.sessionStore.get(localVirtualNodeId, remoteVirtualNodeId);
    if (existingSessionId) {
      logSocialDebug("direct_session_reused", {
        localVirtualNodeId,
        remoteVirtualNodeId,
        sessionId: existingSessionId,
      });
      return {
        sessionId: existingSessionId,
        reused: true,
      };
    }

    logSocialInfo("direct_session_missing", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      hasRemotePublicKey: Boolean(remotePublicKey),
    });
    const session = await this.startDirectSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
      remotePublicKey,
    });
    const sessionId = this.sessionStore.set(
      localVirtualNodeId,
      remoteVirtualNodeId,
      session.session_id,
    );
    logSocialInfo("direct_session_created", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      sessionId,
    });
    return {
      sessionId,
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
    logSocialDebug("virtual_message_send_started", {
      sessionId,
      fromVirtualNodeId,
      toVirtualNodeId,
      messageType: SOCIAL_DIRECT_MESSAGE_TYPE,
    });
    const result = await this.client.sendVirtualMessage({
      sessionId,
      appMessageType: SOCIAL_DIRECT_MESSAGE_TYPE,
      payload: message,
    });
    logSocialDebug("virtual_message_send_finished", {
      sessionId,
      fromVirtualNodeId,
      toVirtualNodeId,
    });
    return result;
  }

  async startDirectSession({
    localVirtualNodeId,
    remoteVirtualNodeId,
    remotePublicKey = null,
  }) {
    logSocialInfo("direct_session_start_started", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      hasRemotePublicKey: Boolean(remotePublicKey),
    });
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

    const session = await this.client.startVirtualSession({
      localVirtualNodeId,
      remoteVirtualNodeId,
    });
    logSocialInfo("direct_session_start_finished", {
      localVirtualNodeId,
      remoteVirtualNodeId,
      sessionId: session.session_id,
    });
    return session;
  }
}

window.SocialService = SocialService;

function assertDhtPublishCompleted(result, label) {
  const status = result?.status;
  if (status === "stored" || status === "stored_locally") {
    return;
  }

  const error = new Error(
    `Nao foi possivel publicar o registro ${label} na DHT. Status: ${status || "desconhecido"}.`,
  );
  error.code = `${label.toLowerCase()}_publish_failed`;
  error.publishResult = result;
  throw error;
}

async function retrySocialOperation({
  label,
  operation,
  shouldRetry,
  maxAttempts = 5,
}) {
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      logSocialDebug("retryable_operation_attempt_started", {
        label,
        attempt,
        maxAttempts,
      });
      const result = await operation();
      logSocialDebug("retryable_operation_attempt_finished", {
        label,
        attempt,
        maxAttempts,
      });
      return result;
    } catch (error) {
      lastError = error;
      const retry = attempt < maxAttempts && shouldRetry(error);
      logSocialInfo("retryable_operation_attempt_failed", {
        label,
        attempt,
        maxAttempts,
        retry,
        code: error.code,
        message: error.message,
        status: error.publishResult?.status,
        reason: error.publishResult?.reason,
      });
      if (!retry) {
        break;
      }
      await sleep(retryDelayMs(attempt));
    }
  }
  throw lastError;
}

function isTransientDhtPublishFailure(error) {
  if (error.code === "api_request_timeout" || error.code === "api_request_failed") {
    return true;
  }
  if (error.code !== "ddt_publish_failed" && error.code !== "dpt_publish_failed") {
    return false;
  }

  const status = error.publishResult?.status;
  return (
    status === "timeout"
    || status === "send_failed"
    || status === "not_routable"
    || status === "failed"
  );
}

function retryDelayMs(attempt) {
  return Math.min(1000 * (2 ** (attempt - 1)), 8000);
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function logSocialInfo(eventType, data = {}) {
  console.info("[AnonNet Social]", eventType, compactSocialLogData(data));
}

function logSocialDebug(eventType, data = {}) {
  console.debug("[AnonNet Social]", eventType, compactSocialLogData(data));
}

function compactSocialLogData(data) {
  return Object.fromEntries(
    Object.entries(data).map(([key, value]) => [key, compactSocialLogValue(key, value)]),
  );
}

function compactSocialLogValue(key, value) {
  if (value === null || value === undefined) {
    return value;
  }
  if ([
    "publicKey",
    "remotePublicKey",
    "pk_virtual_node",
    "pk_virtual_node_owner",
    "signature",
    "record_json",
  ].includes(key)) {
    return summarizeSocialLogValue(value);
  }
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (typeof value === "object") {
    return "{...}";
  }
  if (typeof value === "string" && value.length > 96) {
    return `${value.slice(0, 96)}...(${value.length} chars)`;
  }
  return value;
}

function summarizeSocialLogValue(value) {
  if (typeof value === "string") {
    return value.length > 32 ? `${value.slice(0, 32)}...(${value.length} chars)` : value;
  }
  if (Array.isArray(value)) {
    return `[${value.length} items]`;
  }
  if (value && typeof value === "object") {
    return "{...}";
  }
  return value;
}

async function assertVirtualNodeIdMatchesPublicKey({ virtualNodeId, publicKey }) {
  const publicKeyId = await sha512Hex(publicKey);
  if (publicKeyId === virtualNodeId) {
    return;
  }

  throw new Error("DPT invalida: a public key do dono nao corresponde ao VN ID solicitado.");
}

async function sha512Hex(value) {
  if (typeof crypto !== "undefined" && crypto.subtle) {
    const digest = await crypto.subtle.digest("SHA-512", new TextEncoder().encode(value));
    return [...new Uint8Array(digest)]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  if (typeof require === "function") {
    return require("crypto").createHash("sha512").update(value, "utf8").digest("hex");
  }

  throw new Error("SHA512 indisponivel neste ambiente.");
}

function createUserStateSnapshot({ profile, feedPosts = [] }) {
  return {
    profile: normalizeProfileSnapshot(profile),
    feed_posts: Array.isArray(feedPosts) ? feedPosts : [],
  };
}

function normalizeProfileSnapshot(profile) {
  if (!profile || typeof profile !== "object") {
    return null;
  }
  return Object.fromEntries(
    Object.entries(profile).filter(([key]) => key !== "updated_at"),
  );
}

function stableJson(value) {
  return JSON.stringify(sortJsonValue(value));
}

function sortJsonValue(value) {
  if (Array.isArray(value)) {
    return value.map(sortJsonValue);
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  return Object.keys(value)
    .sort()
    .reduce((result, key) => {
      result[key] = sortJsonValue(value[key]);
      return result;
    }, {});
}

function isLocalContentMissing(error) {
  return error?.code === "content_not_found" || error?.status === 404;
}

function isExpiredIso(value) {
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return true;
  }
  return timestamp <= Date.now();
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
}
