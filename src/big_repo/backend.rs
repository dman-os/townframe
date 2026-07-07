use crate::interlude::*;

use surelock::key::lock_scope;

#[derive(Clone)]
pub struct BigRepoSyncBackend {
    repo: std::sync::Weak<crate::BigRepo>,
    remote_peer_endpoints: Arc<surelock::mutex::Mutex<HashMap<PeerId, iroh::EndpointAddr>>>,
}

impl BigRepoSyncBackend {
    pub async fn boot(
        repo: std::sync::Weak<crate::BigRepo>,
        _endpoint: iroh::Endpoint,
    ) -> Res<Self> {
        Ok(Self {
            repo,
            remote_peer_endpoints: Arc::new(surelock::mutex::Mutex::new(default())),
        })
    }

    pub fn register_remote_peer(&self, peer_id: PeerId, endpoint_addr: iroh::EndpointAddr) {
        lock_scope(|key| {
            let (mut m, _) = key.lock(&self.remote_peer_endpoints);
            m.insert(peer_id, endpoint_addr);
        })
    }

    pub fn unregister_remote_peer(&self, peer_id: PeerId) {
        lock_scope(|key| {
            let (mut m, _) = key.lock(&self.remote_peer_endpoints);
            m.remove(&peer_id);
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

        let has_local_doc_state = repo.runtime.has_doc_worker(doc_id).await?
            || repo.runtime.contains_sedimentree_id(doc_id).await?;
        if !has_local_doc_state && remote_payload.is_none() {
            return Ok(big_sync::SyncTaskRunOutcome::Completion(
                big_sync_core::SyncTaskCompletion {
                    obj_id,
                    deets: big_sync_core::SyncCompletionDeets::Noop,
                },
            ));
        }

        let local_heads = super::partition_doc_heads_payload(&repo.big_sync_store, doc_id).await?;
        let Some(local_heads) = local_heads else {
            // Doc has a subduction sedimentree but no big_sync payload yet.
            // Pull from peer via Subduction sync.
            repo.runtime
                .sync_doc_with_peer(doc_id, peer_id, Some(repo.sync_policy().doc_sync_timeout))
                .await
                .map_err(|sync_error| match sync_error {
                    crate::SyncDocError::NotFound => ferr!("remote doc was not found"),
                    _ => ferr!("{sync_error}"),
                })?;
            return Ok(big_sync::SyncTaskRunOutcome::Completion(
                big_sync_core::SyncTaskCompletion {
                    obj_id,
                    deets: big_sync_core::SyncCompletionDeets::ChangedObject,
                },
            ));
        };
        // short circuit if the payloads are equal
        if let Some(remote_payload) = &remote_payload {
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
        }
    }
}
