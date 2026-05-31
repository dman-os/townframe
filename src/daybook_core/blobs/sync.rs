use crate::interlude::*;

use crate::blobs::{blob_hash_from_id, blob_id_to_iroh_hash, BlobId, BlobUseHints, BlobsRepo};
use big_repo::SharedPartStore;

use big_sync::{SyncBackend, SyncTaskRunOutcome};
use big_sync_core::{ObjId, PeerId, SyncCompletionDeets, SyncTaskCompletion};
use futures::StreamExt;
use surelock::key::lock_scope;

#[derive(Clone)]
pub struct BlobSyncBackend {
    blobs_repo: Arc<BlobsRepo>,
    part_store: SharedPartStore,
    endpoint: iroh::Endpoint,
    remote_peer_endpoints: Arc<surelock::mutex::Mutex<HashMap<PeerId, iroh::EndpointAddr>>>,
}

impl BlobSyncBackend {
    pub fn new(
        blobs_repo: Arc<BlobsRepo>,
        part_store: SharedPartStore,
        endpoint: iroh::Endpoint,
    ) -> Self {
        Self {
            blobs_repo,
            part_store,
            endpoint,
            remote_peer_endpoints: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }

    pub fn register_remote_peer(&self, peer_id: PeerId, endpoint_addr: iroh::EndpointAddr) {
        lock_scope(|key| {
            let (mut remote_peer_endpoints, _key) = key.lock(&self.remote_peer_endpoints);
            remote_peer_endpoints.insert(peer_id, endpoint_addr);
        })
    }

    pub fn unregister_remote_peer(&self, peer_id: PeerId) {
        lock_scope(|key| {
            let (mut remote_peer_endpoints, _key) = key.lock(&self.remote_peer_endpoints);
            remote_peer_endpoints.remove(&peer_id);
        })
    }

    fn blob_id_from_obj_id(obj_id: ObjId) -> BlobId {
        BlobId::new(*obj_id.as_bytes())
    }

    async fn ensure_local_blob(&self, peer_id: PeerId, blob_id: BlobId) -> Res<()> {
        if self.blobs_repo.has_hash(blob_id).await? {
            return Ok(());
        }

        let iroh_hash = blob_id_to_iroh_hash(blob_id);
        if self.blobs_repo.iroh_store().blobs().has(iroh_hash).await? {
            self.blobs_repo
                .put_from_store(blob_id, BlobUseHints::Unknown)
                .await?;
            return Ok(());
        }

        let has_peer = lock_scope(|key| {
            let (remote_peer_endpoints, _key) = key.lock(&self.remote_peer_endpoints);
            remote_peer_endpoints.contains_key(&peer_id)
        });
        if !has_peer {
            eyre::bail!("missing registered peer {peer_id} for blob sync");
        }

        let downloader = self.blobs_repo.iroh_store().downloader(&self.endpoint);
        let provider = iroh::PublicKey::from_bytes(peer_id.as_bytes())
            .expect("peer id must be a valid iroh public key");
        let progress = downloader.download(iroh_hash, vec![provider]);
        let mut stream = progress.stream().await?;

        let mut saw_download_error = false;
        while let Some(item) = stream.next().await {
            match item {
                iroh_blobs::api::downloader::DownloadProgressItem::TryProvider { .. } => {}
                iroh_blobs::api::downloader::DownloadProgressItem::Progress(_) => {}
                iroh_blobs::api::downloader::DownloadProgressItem::PartComplete { .. } => {}
                iroh_blobs::api::downloader::DownloadProgressItem::ProviderFailed { .. } => {}
                iroh_blobs::api::downloader::DownloadProgressItem::DownloadError
                | iroh_blobs::api::downloader::DownloadProgressItem::Error(_) => {
                    saw_download_error = true;
                }
            }
        }
        if saw_download_error {
            eyre::bail!(
                "error seen during blob download for {}",
                blob_hash_from_id(blob_id)
            );
        }

        if !self.blobs_repo.iroh_store().blobs().has(iroh_hash).await? {
            eyre::bail!("download completed but blob missing from store");
        }

        self.blobs_repo
            .put_from_store(blob_id, BlobUseHints::Unknown)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SyncBackend for BlobSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
        remote_payload: Option<big_sync_core::part_store::ObjPayload>,
    ) -> Res<SyncTaskRunOutcome> {
        let blob_id = Self::blob_id_from_obj_id(obj_id);
        let local_has_blob = self.blobs_repo.has_hash(blob_id).await?;
        let local_payload = self.part_store.obj_payload(obj_id).await?;
        if local_has_blob {
            match &remote_payload {
                Some(remote_payload) if local_payload.as_ref() == Some(remote_payload) => {
                    return Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
                        obj_id,
                        deets: SyncCompletionDeets::Noop,
                    }));
                }
                None if local_payload.is_some() => {
                    return Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
                        obj_id,
                        deets: SyncCompletionDeets::Noop,
                    }));
                }
                _ => {}
            }
        }

        self.ensure_local_blob(peer_id, blob_id).await?;
        let payload = remote_payload
            .clone()
            .or_else(|| local_payload.clone())
            .unwrap_or_else(|| serde_json::json!({}));
        self.part_store.set_obj_payload(obj_id, payload).await?;
        let deets = if remote_payload.is_none() {
            SyncCompletionDeets::Noop
        } else if local_payload.is_none() {
            SyncCompletionDeets::AddedMember
        } else if local_payload.as_ref() != remote_payload.as_ref() {
            SyncCompletionDeets::ChangedObject
        } else {
            SyncCompletionDeets::Noop
        };
        Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
            obj_id,
            deets,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blobs::NoopPartitionMembershipWriter;
    use big_sync::backend::contract::{
        self, SyncBackendHarness, SyncBackendOutcome, SyncBackendScenario,
    };
    use big_sync::HostPartStore;
    use big_sync::MemoryPartStore;
    use tempfile::tempdir;

    fn test_part() -> PartId {
        PartId::new([9; 32])
    }

    fn test_parts() -> Vec<PartId> {
        vec![test_part()]
    }

    fn extra_part() -> PartId {
        PartId::new([8; 32])
    }

    async fn build_blob_backend() -> Res<(
        Arc<dyn SyncBackend>,
        big_repo::SharedPartStore,
        Arc<BlobsRepo>,
    )> {
        let temp_root = tempdir()?;
        let blobs_repo = BlobsRepo::new(
            temp_root.path().to_path_buf(),
            daybook_types::doc::UserPathBuf::from("/test-user/test-device"),
            Arc::new(NoopPartitionMembershipWriter),
        )
        .await?;
        let part_store: big_repo::SharedPartStore = Arc::new(MemoryPartStore::new());
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await
            .wrap_err("failed to bind blob sync backend test endpoint")?;
        let backend: Arc<dyn SyncBackend> = Arc::new(BlobSyncBackend::new(
            Arc::clone(&blobs_repo),
            Arc::clone(&part_store),
            endpoint,
        ));
        Ok((backend, part_store, blobs_repo))
    }

    struct BlobSyncBackendContractHarness {
        backend: Arc<dyn SyncBackend>,
        store: Arc<dyn HostPartStore>,
    }

    #[async_trait]
    impl SyncBackendHarness for BlobSyncBackendContractHarness {
        fn backend(&self) -> &dyn SyncBackend {
            self.backend.as_ref()
        }

        fn store(&self) -> &dyn HostPartStore {
            self.store.as_ref()
        }
    }

    fn blob_sync_backend_cases(
        noop_blob_id: BlobId,
        noop_missing_remote_blob_id: BlobId,
        changed_blob_id: BlobId,
        changed_empty_hints_blob_id: BlobId,
        changed_multi_hints_blob_id: BlobId,
        added_blob_id: BlobId,
    ) -> Vec<SyncBackendScenario> {
        let parts = test_parts();
        let extra_part = extra_part();
        let noop_payload = serde_json::json!({"kind": "noop"});
        let old_payload = serde_json::json!({"kind": "old"});
        let new_payload = serde_json::json!({"kind": "new"});
        vec![
            SyncBackendScenario::noop(
                "noop_when_membership_and_payload_match",
                PeerId::new([2; 32]),
                noop_blob_id,
                noop_payload.clone(),
                parts.clone(),
            ),
            SyncBackendScenario {
                name: "noop_when_remote_payload_is_missing_and_blob_exists",
                peer_id: PeerId::new([2; 32]),
                obj_id: noop_missing_remote_blob_id,
                initial_payload: Some(noop_payload.clone()),
                initial_parts: parts.clone(),
                remote_payload: None,
                expected_outcome: SyncBackendOutcome::Completion(
                    big_sync_core::SyncCompletionDeets::Noop,
                ),
                expected_payload: Some(noop_payload.clone()),
                expected_parts: parts.clone(),
            },
            SyncBackendScenario::changed_object(
                "changed_object_applies_remote_payload",
                PeerId::new([2; 32]),
                changed_blob_id,
                old_payload.clone(),
                new_payload.clone(),
                parts.clone(),
            ),
            SyncBackendScenario::changed_object(
                "changed_object_with_empty_part_hints",
                PeerId::new([2; 32]),
                changed_empty_hints_blob_id,
                old_payload.clone(),
                new_payload.clone(),
                vec![],
            ),
            SyncBackendScenario::changed_object(
                "changed_object_with_multiple_part_hints",
                PeerId::new([2; 32]),
                changed_multi_hints_blob_id,
                old_payload.clone(),
                new_payload.clone(),
                vec![parts[0], extra_part],
            ),
            SyncBackendScenario::added_member(
                "added_member_materializes_missing_blob",
                PeerId::new([2; 32]),
                added_blob_id,
                new_payload.clone(),
                parts.clone(),
            ),
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blob_sync_backend_contract() -> Res<()> {
        let (backend, part_store, blobs_repo) = build_blob_backend().await?;
        let noop_blob_id = blobs_repo
            .put(b"blob-sync-contract-noop", BlobUseHints::Unknown)
            .await?;
        let noop_missing_remote_blob_id = blobs_repo
            .put(
                b"blob-sync-contract-noop-missing-remote",
                BlobUseHints::Unknown,
            )
            .await?;
        let changed_blob_id = blobs_repo
            .put(b"blob-sync-contract-changed", BlobUseHints::Unknown)
            .await?;
        let changed_empty_hints_blob_id = blobs_repo
            .put(
                b"blob-sync-contract-changed-empty-hints",
                BlobUseHints::Unknown,
            )
            .await?;
        let changed_multi_hints_blob_id = blobs_repo
            .put(
                b"blob-sync-contract-changed-multi-hints",
                BlobUseHints::Unknown,
            )
            .await?;
        let added_blob_id = blobs_repo
            .put(b"blob-sync-contract-added", BlobUseHints::Unknown)
            .await?;
        let harness = BlobSyncBackendContractHarness {
            backend,
            store: part_store,
        };
        contract::assert_sync_backend_scenarios(
            &harness,
            &blob_sync_backend_cases(
                noop_blob_id,
                noop_missing_remote_blob_id,
                changed_blob_id,
                changed_empty_hints_blob_id,
                changed_multi_hints_blob_id,
                added_blob_id,
            ),
        )
        .await
    }
}
