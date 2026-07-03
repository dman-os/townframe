use crate::interlude::*;

#[derive(Clone)]
pub struct BigRepoSyncBackend {
    repo: std::sync::Weak<crate::BigRepo>,
        Arc<surelock::mutex::Mutex<std::collections::HashMap<PeerId, Arc<RepoRpcClient>>>>,
}

impl BigRepoSyncBackend {
    pub async fn boot(
        repo: std::sync::Weak<crate::BigRepo>,
        endpoint: iroh::Endpoint,
    ) -> Res<Self> {
        Ok(Self {
            repo,
        })
    }
}

#[async_trait::async_trait]
impl big_sync::SyncBackend for BigRepoSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        obj_id: big_sync_core::ObjId,
        remote_payload: Option<big_sync::ObjPayload>,
    ) -> Res<big_sync::SyncTaskRunOutcome> {
        let repo: Arc<crate::BigRepo> = self
            .repo
            .upgrade()
            .ok_or_else(|| eyre::eyre!("big repo dropped while sync backend was active"))?;
        let doc_id: crate::DocumentId = obj_id;
        let local_heads = super::partition_doc_heads_payload(&repo.big_sync_store, doc_id).await?;
        let Some(local_heads) = local_heads else {
            // FIXME: this should be folded into partition_doc_heads_payload query
            let local_doc_exists = repo.big_sync_store.obj_exists(doc_id).await?;
            // Doc not yet synced locally. Pull from peer via Subduction sync.
            // Subduction syncs from an empty tree — the peer sends everything it has.
            repo.runtime
                .sync_doc_with_peer(doc_id, peer_id, Some(repo.sync_policy().doc_sync_timeout))
                .await
                .map_err(|sync_error| match sync_error {
                    crate::SyncDocError::NotFound => ferr!("remote doc was not found"),
                    crate::SyncDocError::Unauthorized => ferr!("remote doc access denied"),
                    crate::SyncDocError::PolicyRejected => {
                        ferr!("remote doc sync hit policy rejection")
                    }
                    _ => ferr!("{sync_error}"),
                })?;
            let deets = if local_doc_exists {
                big_sync_core::SyncCompletionDeets::ChangedObject
            } else {
                big_sync_core::SyncCompletionDeets::AddedMember
            };
            return Ok(big_sync::SyncTaskRunOutcome::Completion(
                big_sync_core::SyncTaskCompletion { obj_id, deets },
            ));
        };
        // short circuit if the payloads are equal
        if let Some(remote_payload) = &remote_payload {
            let remote_heads = super::doc_heads_from_payload(&remote_payload);
            if local_heads.as_ref() == remote_heads.as_ref() {
                return Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::Noop,
                    },
                ));
            }
        }
        match repo
            .runtime
            .sync_doc_with_peer(doc_id, peer_id, Some(repo.sync_policy().doc_sync_timeout))
            .await
        {
            Ok(()) => {
                let heads = repo.doc_payload_heads(doc_id).await?
                    .ok_or_eyre("local doc payload missing after successful sync")?;
                let deets = if remote_payload.is_none() && heads.as_ref() == local_heads.as_ref() {
                    big_sync_core::SyncCompletionDeets::Noop
                } else {
                    big_sync_core::SyncCompletionDeets::ChangedObject
                };
                Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion { obj_id, deets },
                ))
            }
            Err(crate::SyncDocError::Other(inner)) => Err(inner),
            Err(crate::SyncDocError::IoError(inner)) => {
                Err(inner).wrap_err("i/o error syncing doc")
            }
            Err(crate::SyncDocError::TransportError) => {
                eyre::bail!("transport error syncing doc")
            }
            Err(crate::SyncDocError::NotFound) => {
                eyre::bail!("remote doc was not found")
            }
            Err(crate::SyncDocError::Unauthorized) => {
                eyre::bail!("remote doc access denied")
            }
            Err(crate::SyncDocError::PolicyRejected) => {
                eyre::bail!("remote doc sync hit policy rejection")
            }
        }
    }
}
