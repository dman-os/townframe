use super::*;
use automerge::transaction::Transactable;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use big_sync_core::revisioned_store::RevisionReadLimits;
use futures::future::BoxFuture;
use service_utils_rs::Client;
use service_utils_rs::InlineRoute;
use service_utils_rs::StopToken;
use sqlite_rs::SqliteRuntime;

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("internal blob admission does not use client authority") })
    }
}

pub(super) struct Harness {
    pub backend: BlobSyncBackend,
    pub endpoint: iroh::Endpoint,
    pub mirrors: InventoryMirrors,
    pub stop: StopToken,
    pub repo_stop: big_repo::BigRepoStopToken,
}

impl Harness {
    pub async fn new() -> Res<Self> {
        Self::boot(None, false, 101).await
    }

    pub async fn new_network_with_seed(seed: u8) -> Res<Self> {
        Self::boot(None, true, seed).await
    }

    pub async fn reopen(path: &std::path::Path) -> Res<Self> {
        Self::boot(Some(path), false, 101).await
    }

    async fn boot(path: Option<&std::path::Path>, network: bool, seed: u8) -> Res<Self> {
        let runtime = Arc::new(SqliteRuntime::new(Default::default()));
        let (repo, repo_stop) = big_repo::BigRepo::boot(
            Arc::clone(&runtime),
            big_repo::Config {
                node_identity_seed: [seed; 32],
                storage: path.map_or(big_repo::StorageConfig::Memory, |path| {
                    big_repo::StorageConfig::Disk {
                        path: path.join("repo"),
                    }
                }),
                scope_key: "inventory-transfers".into(),
                hidden_parts: Default::default(),
                automerge_frontier_group_scope: Default::default(),
                causal_checkpoint_group_scope: Default::default(),
                group_part_group_scope: Default::default(),
            },
        )
        .await?;
        let sql = match path {
            Some(path) => {
                runtime.database(path.join("blob-state.sqlite"), &[], "fixture_migrations")
            }
            None => runtime.ephemeral(&[])?,
        };
        let parts: big_repo::SharedPartStore = match path {
            Some(_) => Arc::new(
                big_sync::SqlitePartStore::new(sql.clone(), "inventory-transfers", 4).await?,
            ),
            None => Arc::new(big_sync::MemoryPartStore::new()),
        };
        let (blobs, blob_stop) = crate::boot(crate::Config {
            storage: path.map_or(crate::Storage::Memory, |path| {
                crate::Storage::Filesystem(path.join("blobs"))
            }),
            sql: sql.clone(),
            authority: Client::direct(Arc::new(UnusedAuthority)),
            namespace: Namespace {
                namespace_id: Uuid7::new(),
                capability: CapId::from_bytes([102; 16]),
            },
        })
        .await?;
        let queue = crate::work_queue::WorkQueue::new(sql.clone()).await?;
        let (mirrors, mirror_stop) =
            InventoryMirrors::boot(sql, blobs.service, parts, repo, queue).await?;
        let lookup = iroh::address_lookup::MemoryLookup::default();
        let mut endpoint_builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .address_lookup(lookup.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled);
        if !network {
            endpoint_builder = endpoint_builder.clear_ip_transports();
        }
        let endpoint = endpoint_builder.bind().await?;
        let backend = BlobSyncBackend::new(mirrors.clone(), endpoint.clone(), lookup);
        Ok(Self {
            backend,
            endpoint,
            mirrors,
            stop: mirror_stop.then(blob_stop),
            repo_stop,
        })
    }

    pub async fn inventory(&self) -> Res<big_cabinet_core::DocId> {
        let mut document = automerge::Automerge::new();
        let mut tx = document.transaction();
        tx.put(automerge::ROOT, "fixture", true)?;
        tx.commit();
        let inventory = self
            .mirrors
            .projection
            .repo
            .create_doc(document)
            .await?
            .document_id();
        self.mirrors.admission.register(&inventory).await?;
        Ok(inventory)
    }

    pub async fn pin(
        &self,
        inventory: &big_cabinet_core::DocId,
        digest: BlobId,
        length: u64,
    ) -> Res<()> {
        self.mirrors.refresh_authorization(inventory).await?;
        self.mirrors
            .admission
            .pins(inventory, 1, &BTreeMap::from([(digest, length)]), true)
            .await
    }

    pub async fn observe(
        &self,
        inventory: &big_cabinet_core::DocId,
        digest: BlobId,
        _revision: u64,
        present: bool,
    ) -> Res<()> {
        let parts = vec![crate::workers::inventory_part(inventory)];
        if present {
            self.backend
                .sync_obj(PeerKey::new([103; 32]), digest.into(), parts, None)
                .await?;
        } else {
            self.backend
                .remove_obj_from_parts(PeerKey::new([103; 32]), digest.into(), parts)
                .await?;
        }
        Ok(())
    }

    pub async fn wait_membership(
        &self,
        inventory: &big_cabinet_core::DocId,
        digest: BlobId,
        present: bool,
    ) -> Res<()> {
        let parts = &self.mirrors.projection.parts;
        let part = crate::workers::inventory_part(inventory);
        let mut reader = parts.open_revision_reader_all(0).await?.unwrap();
        loop {
            if parts.obj_parts(digest.into()).await?.contains(&part) == present {
                return Ok(());
            }
            reader.next(RevisionReadLimits::default()).await?;
        }
    }

    pub async fn shutdown(self) -> Res<()> {
        self.stop.stop().await;
        self.endpoint.close().await;
        self.repo_stop.stop().await?;
        Ok(())
    }
}

#[tokio::test]
async fn observations_are_durable_candidates_not_hash_only_admission() -> Res<()> {
    let harness = Harness::new().await?;
    let first = harness.inventory().await?;
    let second = harness.inventory().await?;
    let bytes = b"a source observation cannot unlock a different inventory";
    let digest = BlobId::new(*iroh_blobs::Hash::new(bytes).as_bytes());
    let guard = harness
        .mirrors
        .projection
        .service
        .store()
        .blobs()
        .add_bytes(bytes.as_slice())
        .temp_tag()
        .await?;
    harness
        .mirrors
        .select_inventory(first.clone(), "root".into())
        .await?;
    harness
        .mirrors
        .select_inventory(second.clone(), "root".into())
        .await?;
    harness.pin(&first, digest, bytes.len() as u64).await?;
    harness.pin(&second, digest, bytes.len() as u64).await?;
    harness.observe(&first, digest, 10, true).await?;
    harness.wait_membership(&first, digest, true).await?;
    assert_eq!(
        harness
            .mirrors
            .projection
            .parts
            .obj_parts(digest.into())
            .await?,
        vec![crate::workers::inventory_part(&first)]
    );
    assert!(
        !harness
            .mirrors
            .admission
            .state(&second, digest)
            .await?
            .unwrap()
            .advertisable()
    );
    harness.observe(&first, digest, 20, false).await?;
    harness.wait_membership(&first, digest, false).await?;
    harness.observe(&first, digest, 10, true).await?;
    assert!(
        harness
            .mirrors
            .admission
            .state(&first, digest)
            .await?
            .unwrap()
            .observed
    );
    drop(guard);
    harness.shutdown().await
}

#[tokio::test]
async fn unknown_parts_and_non_digest_objects_are_rejected_without_side_effects() -> Res<()> {
    let harness = Harness::new().await?;
    let digest = BlobId::new([104; 32]);
    let part = big_sync_core::PartKey::new([105; 32]);
    for object in [ObjKey::from(digest), ObjKey::new(b"/not/a/blob/digest")] {
        assert!(
            harness
                .backend
                .sync_obj(
                    PeerKey::new([103; 32]),
                    object.clone(),
                    vec![part.clone()],
                    None
                )
                .await
                .is_err()
        );
        assert!(
            harness
                .backend
                .remove_obj_from_parts(
                    PeerKey::new([103; 32]),
                    object.clone(),
                    vec![part.clone()]
                )
                .await
                .is_err()
        );
        assert!(
            harness
                .mirrors
                .projection
                .parts
                .obj_parts(object)
                .await?
                .is_empty()
        );
    }
    assert!(harness.mirrors.admission.inventories().await?.is_empty());
    harness.shutdown().await
}

#[tokio::test]
async fn peer_registration_is_shared_and_revocable() -> Res<()> {
    let harness = Harness::new().await?;
    let peer = PeerKey::new([103; 32]);
    let backend = harness.backend.clone();
    backend.register_peer_addr(peer.clone(), iroh::EndpointAddr::new(harness.endpoint.id()));
    assert_eq!(harness.backend.active_peer_ids(), vec![peer.clone()]);
    harness.backend.unregister_peer_addr(peer.clone());
    harness.backend.unregister_peer_addr(peer);
    assert!(backend.active_peer_ids().is_empty());
    harness.shutdown().await
}
