use crate::interlude::*;

use crate::blobs::{BlobId, BlobUseHints, BlobsRepo, blob_id_to_iroh_hash};
use big_repo::SharedPartStore;

use big_sync::{SyncBackend, SyncTaskRunOutcome};
use big_sync_core::{ObjId, PeerId, SyncCompletionDeets, SyncTaskCompletion};

#[derive(Clone)]
pub struct BlobSyncBackend {
    blobs_repo: Arc<BlobsRepo>,
    part_store: SharedPartStore,
    endpoint: iroh::Endpoint,
    address_lookup: iroh::address_lookup::MemoryLookup,
    peer_addrs: Arc<surelock::mutex::Mutex<HashMap<PeerId, iroh::EndpointAddr>>>,
}

impl BlobSyncBackend {
    pub fn new(
        blobs_repo: Arc<BlobsRepo>,
        part_store: SharedPartStore,
        endpoint: iroh::Endpoint,
        address_lookup: iroh::address_lookup::MemoryLookup,
    ) -> Self {
        Self {
            blobs_repo,
            part_store,
            endpoint,
            address_lookup,
            peer_addrs: Arc::new(surelock::mutex::Mutex::new(HashMap::new())),
        }
    }

    pub fn register_peer_addr(&self, peer_id: PeerId, addr: iroh::EndpointAddr) {
        self.address_lookup.add_endpoint_info(addr.clone());
        surelock::key::lock_scope(|key| {
            let (mut map, _key) = key.lock(&self.peer_addrs);
            map.insert(peer_id, addr);
        });
    }

    pub fn active_peer_ids(&self) -> Vec<PeerId> {
        surelock::key::lock_scope(|key| {
            let (map, _key) = key.lock(&self.peer_addrs);
            map.keys().copied().collect()
        })
    }

    pub fn unregister_peer_addr(&self, peer_id: PeerId) {
        surelock::key::lock_scope(|key| {
            let (mut map, _key) = key.lock(&self.peer_addrs);
            map.remove(&peer_id);
        });
    }

    pub async fn ensure_local_blob(&self, peer_id: PeerId, blob_id: BlobId) -> Res<()> {
        if self.blobs_repo.has_blob_on_disk(blob_id).await? {
            return Ok(());
        }

        let iroh_hash = blob_id_to_iroh_hash(blob_id);
        if self.blobs_repo.iroh_store().blobs().has(iroh_hash).await? {
            self.blobs_repo
                .put_from_store(blob_id, BlobUseHints::Unknown)
                .await?;
            return Ok(());
        }

        let provider_addr = surelock::key::lock_scope(|key| {
            let (map, _key) = key.lock(&self.peer_addrs);
            map.get(&peer_id).cloned()
        })
        .ok_or_else(|| eyre::eyre!("peer {peer_id} is no longer registered for blob downloads"))?;

        tracing::info!(%peer_id, %blob_id, ?provider_addr, %iroh_hash, "downloading blob via iroh downloader from peer");

        let downloader = self.blobs_repo.iroh_store().downloader(&self.endpoint);

        self.address_lookup.add_endpoint_info(provider_addr.clone());

        let res = downloader.download(iroh_hash, vec![provider_addr.id]).await;
        res.map_err(|err| {
            eyre::eyre!("failed downloading blob {blob_id} from peer {peer_id}: {err:?}")
        })?;

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
        let blob_id = BlobId::new(*obj_id.as_bytes());
        let local_has_blob = self.blobs_repo.has_blob_on_disk(blob_id).await?;
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

    use big_sync::HostPartStore;
    use big_sync::MemoryPartStore;
    use big_sync::backend::contract::{
        self, SyncBackendHarness, SyncBackendOutcome, SyncBackendScenario,
    };
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
        tempfile::TempDir,
    )> {
        let temp_root = tempdir()?;
        tracing::info!(path = %temp_root.path().display(), "booted test blob sync node");
        let blobs_repo = BlobsRepo::new(
            temp_root.path().to_path_buf(),
            daybook_types::doc::UserPathBuf::from("/test-user/test-device"),
        )
        .await?;
        let part_store: big_repo::SharedPartStore = Arc::new(MemoryPartStore::new());
        let address_lookup = iroh::address_lookup::MemoryLookup::default();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(address_lookup.clone())
            .bind()
            .await
            .wrap_err("failed to bind blob sync backend test endpoint")?;
        let backend: Arc<dyn SyncBackend> = Arc::new(BlobSyncBackend::new(
            Arc::clone(&blobs_repo),
            Arc::clone(&part_store),
            endpoint,
            address_lookup,
        ));
        Ok((backend, part_store, blobs_repo, temp_root))
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
        let (backend, part_store, blobs_repo, _temp_root) = build_blob_backend().await?;
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

    #[tokio::test]
    async fn test_direct_blob_downloader_transfer() -> Res<()> {
        let temp_dir = tempfile::tempdir()?;
        let dir_a = temp_dir.path().join("a");
        let dir_b = temp_dir.path().join("b");

        let address_lookup_a = iroh::address_lookup::MemoryLookup::default();
        let endpoint_a = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(address_lookup_a.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;

        let address_lookup_b = iroh::address_lookup::MemoryLookup::default();
        let endpoint_b = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(address_lookup_b.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;

        let blobs_repo_a = BlobsRepo::new(
            dir_a.join("blobs"),
            daybook_types::doc::UserPathBuf::from("/test-user/test-device"),
        )
        .await?;

        let blobs_repo_b = BlobsRepo::new(
            dir_b.join("blobs"),
            daybook_types::doc::UserPathBuf::from("/test-user/test-device"),
        )
        .await?;

        let router_a = iroh::protocol::Router::builder(endpoint_a.clone())
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs_repo_a.iroh_store(), None),
            )
            .spawn();

        let part_store_b: big_repo::SharedPartStore = Arc::new(MemoryPartStore::new());
        let backend_b = BlobSyncBackend::new(
            Arc::clone(&blobs_repo_b),
            part_store_b,
            endpoint_b.clone(),
            address_lookup_b.clone(),
        );

        let payload = b"hello direct blob transfer".to_vec();
        let hash = blobs_repo_a.put(&payload, BlobUseHints::Docs).await?;

        let addr_a = iroh::EndpointAddr::from_parts(
            endpoint_a.id(),
            endpoint_a
                .bound_sockets()
                .into_iter()
                .map(iroh::TransportAddr::Ip),
        );
        let peer_id_a = PeerId::new(*endpoint_a.id().as_bytes());

        backend_b.register_peer_addr(peer_id_a, addr_a);
        backend_b.ensure_local_blob(peer_id_a, hash).await?;

        let got = blobs_repo_b.get_path(hash).await?;
        let bytes = tokio::fs::read(got).await?;
        assert_eq!(bytes, payload);

        router_a.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_node_blob_sync_backend_contract() -> Res<()> {
        let temp_dir = tempfile::tempdir()?;
        let dir_a = temp_dir.path().join("node_a");
        let dir_b = temp_dir.path().join("node_b");

        let address_lookup_a = iroh::address_lookup::MemoryLookup::default();
        let endpoint_a = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(address_lookup_a.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;

        let address_lookup_b = iroh::address_lookup::MemoryLookup::default();
        let endpoint_b = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(address_lookup_b.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;

        let blobs_repo_a = BlobsRepo::new(
            dir_a.join("blobs"),
            daybook_types::doc::UserPathBuf::from("/user-a/device-a"),
        )
        .await?;

        let blobs_repo_b = BlobsRepo::new(
            dir_b.join("blobs"),
            daybook_types::doc::UserPathBuf::from("/user-b/device-b"),
        )
        .await?;

        let router_a = iroh::protocol::Router::builder(endpoint_a.clone())
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs_repo_a.iroh_store(), None),
            )
            .spawn();

        let part_store_b: big_repo::SharedPartStore = Arc::new(MemoryPartStore::new());
        let backend_b = BlobSyncBackend::new(
            Arc::clone(&blobs_repo_b),
            Arc::clone(&part_store_b),
            endpoint_b.clone(),
            address_lookup_b.clone(),
        );

        let addr_a = iroh::EndpointAddr::from_parts(
            endpoint_a.id(),
            endpoint_a
                .bound_sockets()
                .into_iter()
                .map(iroh::TransportAddr::Ip),
        );
        let peer_id_a = PeerId::new(*endpoint_a.id().as_bytes());
        backend_b.register_peer_addr(peer_id_a, addr_a);

        // Case 1: Remote Blob Added — Node B is missing blob bytes and sync_obj materializes it from Node A over iroh downloader
        let payload_added = b"contract-multi-node-added-blob".to_vec();
        let hash_added = blobs_repo_a.put(&payload_added, BlobUseHints::Docs).await?;
        let obj_id_added = ObjId::new(*hash_added.as_bytes());

        assert!(!blobs_repo_b.has_hash(hash_added).await?);

        let remote_payload = serde_json::json!({ "mime": "text/plain" });
        let outcome = backend_b
            .sync_obj(peer_id_a, obj_id_added, Some(remote_payload.clone()))
            .await?;

        match outcome {
            big_sync::SyncTaskRunOutcome::Completion(comp) => {
                assert_eq!(comp.deets, big_sync_core::SyncCompletionDeets::AddedMember);
            }
            other => panic!("expected completion with AddedMember, got {other:?}"),
        }

        assert!(blobs_repo_b.has_hash(hash_added).await?);
        let path_b = blobs_repo_b.get_path(hash_added).await?;
        assert_eq!(tokio::fs::read(path_b).await?, payload_added);
        assert_eq!(
            part_store_b.obj_payload(obj_id_added).await?,
            Some(remote_payload.clone())
        );

        // Case 2: Subsequent sync_obj when blob is already materialized locally returns Noop
        let noop_outcome = backend_b
            .sync_obj(peer_id_a, obj_id_added, Some(remote_payload))
            .await?;
        match noop_outcome {
            big_sync::SyncTaskRunOutcome::Completion(comp) => {
                assert_eq!(comp.deets, big_sync_core::SyncCompletionDeets::Noop);
            }
            other => panic!("expected completion with Noop, got {other:?}"),
        }

        router_a.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blob_sync_backend_stress_contract() -> Res<()> {
        struct TestNode {
            _dir: PathBuf,
            endpoint: iroh::Endpoint,
            blobs_repo: Arc<BlobsRepo>,
            _part_store: big_repo::SharedPartStore,
            backend: Arc<BlobSyncBackend>,
            router: iroh::protocol::Router,
        }

        let temp_dir = tempfile::tempdir()?;
        let mut nodes = Vec::new();

        for ii in 0..3 {
            let dir = temp_dir.path().join(format!("node_{ii}"));
            let address_lookup = iroh::address_lookup::MemoryLookup::default();
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .address_lookup(address_lookup.clone())
                .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
                .relay_mode(iroh::RelayMode::Disabled)
                .bind()
                .await?;

            let blobs_repo = BlobsRepo::new(
                dir.join("blobs"),
                daybook_types::doc::UserPathBuf::from(format!("/user-{ii}/device-{ii}")),
            )
            .await?;

            let router = iroh::protocol::Router::builder(endpoint.clone())
                .accept(
                    iroh_blobs::ALPN,
                    iroh_blobs::BlobsProtocol::new(&blobs_repo.iroh_store(), None),
                )
                .spawn();

            let part_store: big_repo::SharedPartStore = Arc::new(MemoryPartStore::new());
            let backend = Arc::new(BlobSyncBackend::new(
                Arc::clone(&blobs_repo),
                Arc::clone(&part_store),
                endpoint.clone(),
                address_lookup,
            ));
            blobs_repo.set_sync_backend((*backend).clone());

            nodes.push(TestNode {
                _dir: dir,
                endpoint,
                blobs_repo,
                _part_store: part_store,
                backend,
                router,
            });
        }

        // Register peer addresses in mesh topology
        for i in 0..nodes.len() {
            for j in 0..nodes.len() {
                if i != j {
                    let addr_j = iroh::EndpointAddr::from_parts(
                        nodes[j].endpoint.id(),
                        nodes[j]
                            .endpoint
                            .bound_sockets()
                            .into_iter()
                            .map(iroh::TransportAddr::Ip),
                    );
                    let peer_id_j = PeerId::new(*nodes[j].endpoint.id().as_bytes());
                    nodes[i].backend.register_peer_addr(peer_id_j, addr_j);
                }
            }
        }

        // Phase 1: Node 0 creates 5 blobs
        let mut created_blobs = Vec::new();
        for idx in 0..5 {
            let payload = format!(
                "stress-blob-payload-node0-{idx}-{}",
                "x".repeat(1024 * (idx + 1))
            )
            .into_bytes();
            let hash = nodes[0]
                .blobs_repo
                .put(&payload, BlobUseHints::Docs)
                .await?;
            created_blobs.push((hash, payload));
        }

        // Phase 2: Node 1 syncs all blobs from Node 0
        let peer_0 = PeerId::new(*nodes[0].endpoint.id().as_bytes());
        for (hash, payload) in &created_blobs {
            let obj_id = ObjId::new(*hash.as_bytes());
            let remote_meta = serde_json::json!({ "mime": "text/plain" });
            let outcome = nodes[1]
                .backend
                .sync_obj(peer_0, obj_id, Some(remote_meta))
                .await?;
            match outcome {
                big_sync::SyncTaskRunOutcome::Completion(comp) => {
                    assert_eq!(comp.deets, big_sync_core::SyncCompletionDeets::AddedMember);
                }
                other => panic!("expected AddedMember for node 1 sync_obj, got {other:?}"),
            }
            let read_bytes = tokio::fs::read(nodes[1].blobs_repo.get_path(*hash).await?).await?;
            assert_eq!(&read_bytes, payload);
        }

        // Phase 3: Node 1 creates 3 new blobs
        for idx in 0..3 {
            let payload = format!(
                "stress-blob-payload-node1-{idx}-{}",
                "y".repeat(2048 * (idx + 1))
            )
            .into_bytes();
            let hash = nodes[1]
                .blobs_repo
                .put(&payload, BlobUseHints::Docs)
                .await?;
            created_blobs.push((hash, payload));
        }

        // Phase 4: Node 2 syncs all 8 blobs from Node 1
        let peer_1 = PeerId::new(*nodes[1].endpoint.id().as_bytes());
        for (hash, payload) in &created_blobs {
            let obj_id = ObjId::new(*hash.as_bytes());
            let remote_meta = serde_json::json!({ "mime": "text/plain" });
            let outcome = nodes[2]
                .backend
                .sync_obj(peer_1, obj_id, Some(remote_meta))
                .await?;
            match outcome {
                big_sync::SyncTaskRunOutcome::Completion(_) => {}
                other => panic!("expected Completion for node 2 sync_obj, got {other:?}"),
            }
            let read_bytes = tokio::fs::read(nodes[2].blobs_repo.get_path(*hash).await?).await?;
            assert_eq!(&read_bytes, payload);
        }

        // Phase 5: Parity check across all 3 nodes for all 8 blobs
        for node in &nodes {
            for (hash, payload) in &created_blobs {
                let path = node.blobs_repo.get_path(*hash).await?;
                let bytes = tokio::fs::read(path).await?;
                assert_eq!(&bytes, payload);
            }
        }

        for node in nodes {
            node.router.shutdown().await?;
        }
        Ok(())
    }
}
