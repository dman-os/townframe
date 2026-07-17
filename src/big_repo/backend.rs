use crate::interlude::*;

#[derive(Clone)]
pub struct BigRepoSyncBackend {
    repo: std::sync::Weak<crate::BigRepo>,
}

impl BigRepoSyncBackend {
    pub async fn boot(repo: std::sync::Weak<crate::BigRepo>) -> Res<Self> {
        Ok(Self { repo })
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

        let has_local_doc_state = match futures::future::select(
            core::pin::pin!(repo.runtime.has_doc_worker(doc_id)),
            core::pin::pin!(repo.runtime.contains_sedimentree_id(doc_id)),
        )
        .await
        {
            futures::future::Either::Left((val, other)) => val? || other.await?,
            futures::future::Either::Right((val, other)) => val? || other.await?,
        };
        if !has_local_doc_state && remote_payload.is_none() {
            return Ok(big_sync::SyncTaskRunOutcome::Completion(
                big_sync_core::SyncTaskCompletion {
                    obj_id,
                    deets: big_sync_core::SyncCompletionDeets::Noop,
                },
            ));
        }

        // short circuit if the payloads are equal
        let local_heads = repo.doc_payload_heads(doc_id).await?;
        if let Some(remote_payload) = &remote_payload {
            if let Some(local_heads) = &local_heads {
                let remote_heads = super::doc_heads_from_payload(remote_payload);
                if local_heads.as_ref() == remote_heads.as_ref() {
                    return Ok(big_sync::SyncTaskRunOutcome::Completion(
                        big_sync_core::SyncTaskCompletion {
                            obj_id,
                            deets: big_sync_core::SyncCompletionDeets::Noop,
                        },
                    ));
                }
            };
        }
        match repo
            .runtime
            .sync_doc_with_peer(doc_id, peer_id, Some(repo.sync_policy().doc_sync_timeout))
            .await
        {
            Ok(()) => {
                let heads = repo
                    .doc_payload_heads(doc_id)
                    .await?
                    .ok_or_eyre("local doc payload missing after successful sync")?;
                let deets = if remote_payload.is_none()
                    && local_heads
                        .as_ref()
                        .map(|prev| prev.as_ref() == heads.as_ref())
                        .unwrap_or_default()
                {
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
                eyre::bail!("remote doc sync was unauthorized")
            }
            Err(crate::SyncDocError::Policy(error)) => {
                eyre::bail!("remote doc sync was rejected by policy: {error}")
            }
        }
    }
}
