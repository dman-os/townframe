use crate::interlude::*;

use crate::blobs::{BlobId, BlobsRepo, blob_id_to_iroh_hash};
use big_repo::SharedPartStore;

use big_sync::{SyncBackend, SyncTaskRunOutcome};
use big_sync_core::{ObjKey, PeerKey, SyncCompletionDeets, SyncTaskCompletion};

#[derive(Clone)]
pub struct BlobSyncBackend {
    blobs_repo: Arc<BlobsRepo>,
    part_store: SharedPartStore,
    endpoint: iroh::Endpoint,
    address_lookup: iroh::address_lookup::MemoryLookup,
    peer_addrs: Arc<surelock::mutex::Mutex<HashMap<PeerKey, iroh::EndpointAddr>>>,
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

    pub fn register_peer_addr(&self, peer_id: PeerKey, addr: iroh::EndpointAddr) {
        self.address_lookup.add_endpoint_info(addr.clone());
        surelock::key::lock_scope(|key| {
            let (mut map, _key) = key.lock(&self.peer_addrs);
            map.insert(peer_id, addr);
        });
    }

    pub fn active_peer_ids(&self) -> Vec<PeerKey> {
        surelock::key::lock_scope(|key| {
            let (map, _key) = key.lock(&self.peer_addrs);
            map.keys().cloned().collect()
        })
    }

    pub fn unregister_peer_addr(&self, peer_id: PeerKey) {
        surelock::key::lock_scope(|key| {
            let (mut map, _key) = key.lock(&self.peer_addrs);
            map.remove(&peer_id);
        });
    }

    pub async fn ensure_local_blob(&self, peer_id: PeerKey, blob_id: BlobId) -> Res<()> {
        if self.blobs_repo.has_blob_on_disk(blob_id.clone()).await? {
            return Ok(());
        }

        let iroh_hash = blob_id_to_iroh_hash(blob_id.clone());
        // The store's `has()` answers "can this store serve the hash without a
        // download", and it reports virtual entries (outboard here, ciphertext
        // produced on demand by the registered provider) as complete. Export
        // can only materialize *stored* bytes and is refused for virtual ones
        // ("cannot export a virtual entry to a path; it has no stored data"),
        // so gate the export on a sync reader: only a stored-complete entry
        // hands one out (`fs.rs` `sync_reader`). A virtual entry reaching this
        // function still asked for materialization, so it falls through to the
        // download branch - the serving peer produces the real bytes - rather
        // than failing the same way on every retry.
        if self.blobs_repo.iroh_store().blobs().has(iroh_hash).await?
            && self
                .blobs_repo
                .iroh_store()
                .blobs()
                .sync_reader(iroh_hash)
                .await?
                .is_some()
        {
            self.blobs_repo.put_from_store(blob_id).await?;
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

        self.blobs_repo.put_from_store(blob_id).await?;

        Ok(())
    }
}

#[async_trait]
impl SyncBackend for BlobSyncBackend {
    async fn sync_obj(
        &self,
        peer_id: PeerKey,
        obj_id: ObjKey,
        parts: Vec<PartKey>,
        remote_payload: Option<big_sync_core::part_store::ObjPayload>,
    ) -> Res<SyncTaskRunOutcome> {
        // The key was written by whichever peer holds this part, so a key that
        // names no blob digest names no blob here: an error, not a path and not
        // a panic.
        let blob_id = BlobId::try_from(&obj_id)
            .wrap_err_with(|| format!("blob object key is not a blob digest: {obj_id}"))?;
        let local_has_blob = self.blobs_repo.has_blob_on_disk(blob_id.clone()).await?;
        // ADR 003 §14: possession of a cipher representation is "ready to be
        // served, physically or virtually". A node that holds the plaintext
        // serves the ciphertext on demand through its registered provider, so
        // a virtual entry whose `ct:`/`pt:` pair is still rooted is terminal
        // possession: there is no transfer this node could want. Without this
        // branch the possession leg below demanded an export the store refuses
        // for virtual entries and this task failed-and-rescheduled forever -
        // the randomized stress's "big sync object task failed; rescheduling"
        // storm, hundreds of identical retries per (peer, object), which the
        // settlement fence can never ride out.
        let local_possessed_virtual = !local_has_blob
            && self
                .blobs_repo
                .blob_is_possessed_without_bytes(&blob_id)
                .await?;
        let local_payload = self.part_store.obj_payload(obj_id.clone()).await?;
        if local_has_blob || local_possessed_virtual {
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

        // Possessed virtually: skip the possession leg (its export and its
        // `blob_now_held` row are meaningless without stored bytes), but still
        // reconcile the part payload and membership below.
        if !local_possessed_virtual {
            self.ensure_local_blob(peer_id, blob_id.clone()).await?;
        }
        let payload = remote_payload
            .clone()
            .or_else(|| local_payload.clone())
            .unwrap_or_else(|| serde_json::json!({}));
        self.part_store
            .set_obj_payload(obj_id.clone(), payload)
            .await?;
        let deets = if remote_payload.is_none() {
            SyncCompletionDeets::Noop
        } else if local_payload.is_none() {
            SyncCompletionDeets::AddedMember
        } else if local_payload.as_ref() != remote_payload.as_ref() {
            SyncCompletionDeets::ChangedObject
        } else {
            SyncCompletionDeets::Noop
        };
        for part_id in parts {
            self.part_store
                .add_obj_to_parts(obj_id.clone(), vec![part_id])
                .await?;
        }
        Ok(SyncTaskRunOutcome::Completion(SyncTaskCompletion {
            obj_id,
            deets,
        }))
    }

    async fn remove_obj_from_parts(
        &self,
        obj_id: big_sync_core::ObjKey,
        parts: Vec<big_sync_core::PartKey>,
    ) -> Res<()> {
        for part_id in parts {
            self.part_store
                .remove_obj_from_part(obj_id.clone(), part_id)
                .await?;
        }
        Ok(())
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

    fn test_part() -> PartKey {
        PartKey::new([9; 32])
    }

    fn test_parts() -> Vec<PartKey> {
        vec![test_part()]
    }

    fn extra_part() -> PartKey {
        PartKey::new([8; 32])
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
                PeerKey::new([2; 32]),
                ObjKey::from(noop_blob_id),
                noop_payload.clone(),
                parts.clone(),
            ),
            SyncBackendScenario {
                name: "noop_when_remote_payload_is_missing_and_blob_exists",
                peer_id: PeerKey::new([2; 32]),
                obj_id: ObjKey::from(noop_missing_remote_blob_id),
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
                PeerKey::new([2; 32]),
                ObjKey::from(changed_blob_id),
                old_payload.clone(),
                new_payload.clone(),
                parts.clone(),
            ),
            SyncBackendScenario::changed_object(
                "changed_object_with_empty_part_hints",
                PeerKey::new([2; 32]),
                ObjKey::from(changed_empty_hints_blob_id),
                old_payload.clone(),
                new_payload.clone(),
                vec![],
            ),
            SyncBackendScenario::changed_object(
                "changed_object_with_multiple_part_hints",
                PeerKey::new([2; 32]),
                ObjKey::from(changed_multi_hints_blob_id),
                old_payload.clone(),
                new_payload.clone(),
                vec![parts[0].clone(), extra_part],
            ),
            SyncBackendScenario::added_member(
                "added_member_materializes_missing_blob",
                PeerKey::new([2; 32]),
                ObjKey::from(added_blob_id),
                new_payload.clone(),
                parts.clone(),
            ),
        ]
    }

    /// The blob part store is synced from peers, so an object key in it is
    /// whatever the peer wrote. A key that names no digest names no blob: it
    /// must be an error, never a path built outside the blob root.
    #[tokio::test(flavor = "multi_thread")]
    async fn blob_sync_obj_rejects_a_key_that_is_not_a_digest() -> Res<()> {
        let (backend, _part_store, _blobs_repo, temp_root) = build_blob_backend().await?;
        // An absolute spelling: `Path::join` reads it as a replacement for the
        // blob root, so a key like this must never reach path construction.
        let escape_target = temp_root.path().join("daybook-escape");
        // The on-disk name a digest-shaped path gets: `<digest>.blob`.
        let escape_blob = PathBuf::from(format!("{}.blob", escape_target.display()));
        let obj_id = ObjKey::new(escape_target.to_string_lossy().as_bytes());

        let err = backend
            .sync_obj(
                PeerKey::new([2; 32]),
                obj_id.clone(),
                test_parts(),
                Some(serde_json::json!({ "lengthOctets": 1 })),
            )
            .await
            .expect_err("an object key that is not a blob digest must not be accepted");

        assert!(
            err.to_string().contains(&obj_id.to_string()),
            "error must name the offending key: {err}"
        );
        assert!(
            !tokio::fs::try_exists(&escape_blob).await?,
            "the reserved key produced {}",
            escape_blob.display()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blob_sync_backend_contract() -> Res<()> {
        let (backend, part_store, blobs_repo, _temp_root) = build_blob_backend().await?;
        let noop_blob_id = blobs_repo.put(b"blob-sync-contract-noop").await?;
        let noop_missing_remote_blob_id = blobs_repo
            .put(b"blob-sync-contract-noop-missing-remote")
            .await?;
        let changed_blob_id = blobs_repo.put(b"blob-sync-contract-changed").await?;
        let changed_empty_hints_blob_id = blobs_repo
            .put(b"blob-sync-contract-changed-empty-hints")
            .await?;
        let changed_multi_hints_blob_id = blobs_repo
            .put(b"blob-sync-contract-changed-multi-hints")
            .await?;
        let added_blob_id = blobs_repo.put(b"blob-sync-contract-added").await?;
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
        let hash = blobs_repo_a.put(&payload).await?;

        let addr_a = iroh::EndpointAddr::from_parts(
            endpoint_a.id(),
            endpoint_a
                .bound_sockets()
                .into_iter()
                .map(iroh::TransportAddr::Ip),
        );
        let peer_id_a = PeerKey::new(*endpoint_a.id().as_bytes());

        backend_b.register_peer_addr(peer_id_a.clone(), addr_a);
        backend_b.ensure_local_blob(peer_id_a, hash.clone()).await?;

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
        let peer_id_a = PeerKey::new(*endpoint_a.id().as_bytes());
        backend_b.register_peer_addr(peer_id_a.clone(), addr_a);

        // Case 1: Remote Blob Added — Node B is missing blob bytes and sync_obj materializes it from Node A over iroh downloader
        let payload_added = b"contract-multi-node-added-blob".to_vec();
        let hash_added = blobs_repo_a.put(&payload_added).await?;
        let obj_id_added = ObjKey::from(hash_added.clone());

        assert!(!blobs_repo_b.has_hash(hash_added.clone()).await?);

        let remote_payload = serde_json::json!({ "mime": "text/plain" });
        let outcome = backend_b
            .sync_obj(
                peer_id_a.clone(),
                obj_id_added.clone(),
                Vec::new(),
                Some(remote_payload.clone()),
            )
            .await?;

        match outcome {
            big_sync::SyncTaskRunOutcome::Completion(comp) => {
                assert_eq!(comp.deets, big_sync_core::SyncCompletionDeets::AddedMember);
            }
            other => panic!("expected completion with AddedMember, got {other:?}"),
        }

        assert!(blobs_repo_b.has_hash(hash_added.clone()).await?);
        let path_b = blobs_repo_b.get_path(hash_added).await?;
        assert_eq!(tokio::fs::read(path_b).await?, payload_added);
        assert_eq!(
            part_store_b.obj_payload(obj_id_added.clone()).await?,
            Some(remote_payload.clone())
        );

        // Case 2: Subsequent sync_obj when blob is already materialized locally returns Noop
        let noop_outcome = backend_b
            .sync_obj(peer_id_a, obj_id_added, Vec::new(), Some(remote_payload))
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
    async fn multi_node_chain_blob_sync_stress() -> Res<()> {
        struct TestNode {
            blobs_repo: Arc<BlobsRepo>,
            endpoint: iroh::Endpoint,
            _address_lookup: iroh::address_lookup::MemoryLookup,
            backend: BlobSyncBackend,
            _part_store: big_repo::SharedPartStore,
            router: iroh::protocol::Router,
        }

        let temp_dir = tempfile::tempdir()?;
        let num_nodes = 3;
        let mut nodes = Vec::new();

        for i in 0..num_nodes {
            let dir = temp_dir.path().join(format!("node_{i}"));
            let address_lookup = iroh::address_lookup::MemoryLookup::default();
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .address_lookup(address_lookup.clone())
                .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
                .relay_mode(iroh::RelayMode::Disabled)
                .bind()
                .await?;

            let blobs_repo = BlobsRepo::new(
                dir.join("blobs"),
                daybook_types::doc::UserPathBuf::from(format!("/user-{i}/device-{i}")),
            )
            .await?;

            let router = iroh::protocol::Router::builder(endpoint.clone())
                .accept(
                    iroh_blobs::ALPN,
                    iroh_blobs::BlobsProtocol::new(&blobs_repo.iroh_store(), None),
                )
                .spawn();

            let part_store: big_repo::SharedPartStore = Arc::new(MemoryPartStore::new());
            let backend = BlobSyncBackend::new(
                Arc::clone(&blobs_repo),
                Arc::clone(&part_store),
                endpoint.clone(),
                address_lookup.clone(),
            );
            blobs_repo.set_sync_backend(backend.clone());

            nodes.push(TestNode {
                blobs_repo,
                endpoint,
                _address_lookup: address_lookup,
                backend,
                _part_store: part_store,
                router,
            });
        }

        // Register peer addresses
        for i in 0..num_nodes {
            for j in 0..num_nodes {
                if i != j {
                    let addr = iroh::EndpointAddr::from_parts(
                        nodes[j].endpoint.id(),
                        nodes[j]
                            .endpoint
                            .bound_sockets()
                            .into_iter()
                            .map(iroh::TransportAddr::Ip),
                    );
                    let peer_id = PeerKey::new(*nodes[j].endpoint.id().as_bytes());
                    nodes[i].backend.register_peer_addr(peer_id, addr);
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
            let hash = nodes[0].blobs_repo.put(&payload).await?;
            created_blobs.push((hash, payload));
        }

        // Phase 2: Node 1 syncs all blobs from Node 0
        let peer_0 = PeerKey::new(*nodes[0].endpoint.id().as_bytes());
        for (hash, payload) in &created_blobs {
            let obj_id = ObjKey::from(hash.clone());
            let remote_meta = serde_json::json!({ "mime": "text/plain" });
            let outcome = nodes[1]
                .backend
                .sync_obj(peer_0.clone(), obj_id, Vec::new(), Some(remote_meta))
                .await?;
            match outcome {
                big_sync::SyncTaskRunOutcome::Completion(comp) => {
                    assert_eq!(comp.deets, big_sync_core::SyncCompletionDeets::AddedMember);
                }
                other => panic!("expected AddedMember for node 1 sync_obj, got {other:?}"),
            }
            let read_bytes =
                tokio::fs::read(nodes[1].blobs_repo.get_path(hash.clone()).await?).await?;
            assert_eq!(&read_bytes, payload);
        }

        // Phase 3: Node 1 creates 3 new blobs
        for idx in 0..3 {
            let payload = format!(
                "stress-blob-payload-node1-{idx}-{}",
                "y".repeat(2048 * (idx + 1))
            )
            .into_bytes();
            let hash = nodes[1].blobs_repo.put(&payload).await?;
            created_blobs.push((hash, payload));
        }

        // Phase 4: Node 2 syncs all 8 blobs from Node 1
        let peer_1 = PeerKey::new(*nodes[1].endpoint.id().as_bytes());
        for (hash, payload) in &created_blobs {
            let obj_id = ObjKey::from(hash.clone());
            let remote_meta = serde_json::json!({ "mime": "text/plain" });
            let outcome = nodes[2]
                .backend
                .sync_obj(peer_1.clone(), obj_id, Vec::new(), Some(remote_meta))
                .await?;
            match outcome {
                big_sync::SyncTaskRunOutcome::Completion(_) => {}
                other => panic!("expected Completion for node 2 sync_obj, got {other:?}"),
            }
            let read_bytes =
                tokio::fs::read(nodes[2].blobs_repo.get_path(hash.clone()).await?).await?;
            assert_eq!(&read_bytes, payload);
        }

        // Phase 5: Parity check across all 3 nodes for all 8 blobs
        for node in &nodes {
            for (hash, payload) in &created_blobs {
                let path = node.blobs_repo.get_path(hash.clone()).await?;
                let bytes = tokio::fs::read(path).await?;
                assert_eq!(&bytes, payload);
            }
        }

        for node in nodes {
            node.router.shutdown().await?;
        }
        Ok(())
    }
    /// ADR 003 §14 possession regression. A key-holder holds the ciphertext
    /// only virtually (outboard + rooted provider pair; the bytes it serves are
    /// re-encrypted from the stored plaintext on demand). When a peer
    /// advertises the ciphertext's object, the task must settle without any
    /// transfer: before the fix the possession leg took the `has()` shortcut
    /// into `export`, which the store refuses for virtual entries, and the
    /// task failed-and-rescheduled forever - the four-node randomized stress's
    /// ~800-identical-retries-per-(peer, object) storm, whose settlement fence
    /// starved with every document already converged. The peer here is
    /// deliberately never registered for downloads: touching the download
    /// branch would error loudly instead of passing.
    #[tokio::test(flavor = "multi_thread")]
    async fn blob_sync_obj_settles_for_a_virtually_possessed_cipher_representation() -> Res<()> {
        let (backend, _part_store, blobs_repo, _temp_root) = build_blob_backend().await?;
        let pair_roots =
            crate::blobs::pair_roots::PairRoots::boot(crate::app::SqlCtx::memory().await?).await?;
        let key = crate::blobs::encrypt::MasterKey::random();
        // The full add pass installs and registers as one act: P gets stored
        // bytes, C gets the virtual entry, and the pair roots both under the
        // `ct:`/`pt:` tags.
        let (c, p_hash) = crate::blobs::encrypt::add_encrypted(
            &blobs_repo.iroh_store(),
            &blobs_repo.cipher_provider(),
            &pair_roots,
            &key,
            crate::blobs::encrypt::EncodingParams::DEFAULT,
            b"cipherblob virtual possession",
        )
        .await?;

        let blob_id = BlobId::new(*c.as_bytes());
        let store = blobs_repo.iroh_store();
        let blobs = store.blobs();
        assert!(blobs.has(c).await?, "the virtual entry is servable");
        assert!(
            blobs.sync_reader(c).await?.is_none(),
            "installed C must be virtual: no stored bytes behind it"
        );
        assert!(
            blobs.sync_reader(p_hash).await?.is_some(),
            "the plaintext it serves from is the stored side"
        );
        assert!(
            blobs_repo.blob_is_possessed_without_bytes(&blob_id).await?,
            "virtual entry with its pair rooted is possessed without bytes"
        );

        let obj_id = ObjKey::from(blob_id);
        let outcome = backend
            .sync_obj(
                PeerKey::new([2; 32]),
                obj_id,
                test_parts(),
                Some(serde_json::json!({"mime": "application/octet-stream"})),
            )
            .await?;
        match outcome {
            SyncTaskRunOutcome::Completion(comp) => {
                assert_eq!(
                    comp.deets,
                    SyncCompletionDeets::AddedMember,
                    "the part payload was reconciled without any transfer"
                );
            }
            other => panic!("expected completion for the virtually held object, got {other:?}"),
        }
        assert!(
            !blobs_repo
                .has_blob_on_disk(BlobId::new(*c.as_bytes()))
                .await?,
            "settling a virtually possessed object must never export the ciphertext to disk"
        );
        Ok(())
    }

    /// The relay-shape negative control: a node with no pair (no tags, no keys,
    /// nothing virtual of its own) still takes the download branch and lands
    /// real stored bytes, while the serving side holds C only virtually. This
    /// is the world where the possession leg's export is correct.
    #[tokio::test(flavor = "multi_thread")]
    async fn blob_sync_materializes_ciphertext_for_a_node_without_a_pair() -> Res<()> {
        let temp_dir = tempfile::tempdir()?;
        let dir_a = temp_dir.path().join("origin");
        let dir_b = temp_dir.path().join("relay");

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

        let pair_roots =
            crate::blobs::pair_roots::PairRoots::boot(crate::app::SqlCtx::memory().await?).await?;
        let key = crate::blobs::encrypt::MasterKey::random();
        let (c, _p_hash) = crate::blobs::encrypt::add_encrypted(
            &blobs_repo_a.iroh_store(),
            &blobs_repo_a.cipher_provider(),
            &pair_roots,
            &key,
            crate::blobs::encrypt::EncodingParams::DEFAULT,
            b"ciphertext served on demand to a pairless relay",
        )
        .await?;
        let blob_id = BlobId::new(*c.as_bytes());
        assert!(
            blob_id_to_iroh_hash(blob_id.clone()) == c,
            "the object identity is the ciphertext digest itself"
        );
        assert!(
            !crate::blobs::encrypt::has_pair_tags(&blobs_repo_b.iroh_store(), c).await?,
            "the relay holds no pair: nothing of its own is virtual"
        );

        let addr_a = iroh::EndpointAddr::from_parts(
            endpoint_a.id(),
            endpoint_a
                .bound_sockets()
                .into_iter()
                .map(iroh::TransportAddr::Ip),
        );
        let peer_id_a = PeerKey::new(*endpoint_a.id().as_bytes());
        backend_b.register_peer_addr(peer_id_a.clone(), addr_a);
        backend_b
            .ensure_local_blob(peer_id_a, blob_id.clone())
            .await?;

        assert!(
            blobs_repo_b.has_blob_on_disk(blob_id.clone()).await?,
            "the relay materializes the served ciphertext it had no bytes for"
        );
        let on_disk = blobs_repo_b.get_path(blob_id.clone()).await?;
        let bytes = tokio::fs::read(on_disk).await?;
        assert_eq!(
            BlobId::new(*blake3::hash(&bytes).as_bytes()),
            blob_id,
            "the materialized object is the ciphertext itself"
        );
        assert!(
            blobs_repo_b
                .iroh_store()
                .blobs()
                .sync_reader(c)
                .await?
                .is_some(),
            "the relay's store now holds the stored bytes"
        );

        router_a.shutdown().await?;
        Ok(())
    }
}
