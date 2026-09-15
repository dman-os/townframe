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
    #[tracing::instrument(
        skip_all,
        fields(%peer_id, %obj_id, remote_payload_present = remote_payload.is_some()),
    )]
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

        let has_local_doc_state = repo.runtime.has_local_doc_state(doc_id).await?;
        tracing::debug!(
            remote_peer_id = %peer_id,
            %doc_id,
            has_local_doc_state,
            remote_payload = remote_payload.is_some(),
            "big repo sync_obj",
        );
        // short circuit if the payloads are equal
        let local_heads = repo.doc_payload_heads(doc_id).await?;
        if let Some(remote_payload) = &remote_payload
            && let Some(local_heads) = &local_heads
        {
            let remote_heads = super::doc_heads_from_payload(remote_payload);
            if local_heads.as_ref() == remote_heads.as_ref() {
                return Ok(big_sync::SyncTaskRunOutcome::Completion(
                    big_sync_core::SyncTaskCompletion {
                        obj_id,
                        deets: big_sync_core::SyncCompletionDeets::Noop,
                    },
                ));
            }
        }
        let timeout = repo.sync_policy().doc_sync_timeout;
        let receipt = match tokio::time::timeout(
            timeout,
            repo.runtime.sync_doc_with_peer_receipt(doc_id, peer_id),
        )
        .await
        {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(crate::SyncDocError::Other(inner))) => return Err(inner),
            Ok(Err(crate::SyncDocError::IoError(inner))) => {
                return Err(inner).wrap_err("i/o error syncing doc");
            }
            Ok(Err(crate::SyncDocError::TransportError)) => {
                eyre::bail!("transport error syncing doc");
            }
            Ok(Err(crate::SyncDocError::NotFound)) => {
                eyre::bail!("remote doc was not found");
            }
            Ok(Err(crate::SyncDocError::Unauthorized)) => {
                eyre::bail!("remote doc sync was unauthorized");
            }
            Ok(Err(crate::SyncDocError::Policy(error))) => {
                eyre::bail!("remote doc sync was rejected by policy: {error}");
            }
            Err(_) => {
                eyre::bail!("timed out syncing doc");
            }
        };
        debug!(peer_id = %peer_id, obj_id = %obj_id, ?receipt.outcome, "big sync document receipt");
        let heads = repo
            .doc_payload_heads(doc_id)
            .await?
            .ok_or_eyre("local doc payload missing after successful sync")?;
        debug!(
            head_count = heads.len(),
            "loaded persisted heads after document sync"
        );
        let deets = if local_heads
            .as_ref()
            .map(|prev| prev.as_ref() == heads.as_ref())
            .unwrap_or_default()
        {
            big_sync_core::SyncCompletionDeets::Noop
        } else {
            big_sync_core::SyncCompletionDeets::ChangedObject
        };
        debug!(?deets, "document sync backend completed");
        Ok(big_sync::SyncTaskRunOutcome::Completion(
            big_sync_core::SyncTaskCompletion { obj_id, deets },
        ))
    }
}
