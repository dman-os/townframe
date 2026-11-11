use super::project::*;
use super::*;
use automerge::transaction::Transactable;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use futures::future::BoxFuture;
use iroh_blobs::Hash;
use service_client_native_rs::Client;
use service_client_native_rs::InlineRoute;
use service_client_native_rs::Transport;
use service_utils_rs::StopToken;

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("admission projection does not mint client capabilities") })
    }
}

struct Harness {
    projection: Projection,
    admission: Admission,
    stop: StopToken,
    repo_stop: big_repo::BigRepoStopToken,
}

impl Harness {
    async fn boot() -> Res<Self> {
        let runtime = Arc::new(sqlite_rs::SqliteRuntime::new(Default::default()));
        let (repo, repo_stop) = big_repo::BigRepo::boot(big_repo::Config {
            node_identity_seed: [92; 32],
            storage: big_repo::Storage::memory(&runtime).unwrap(),
            scope_key: "admission-projection".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await?;
        let (blobs, blobs_stop) = crate::boot(crate::Config {
            storage: crate::Storage::memory().unwrap(),
            sql: runtime.ephemeral(&[])?,
            authority: Client::new(Transport::direct(Arc::new(UnusedAuthority))),
            namespace: Namespace {
                namespace_id: Uuid7::new(),
                capability: CapId::from_bytes([93; 16]),
            },
        })
        .await?;
        let (admission, admission_stop) = Admission::boot(runtime.ephemeral(&[])?).await?;
        Ok(Self {
            projection: Projection {
                service: blobs.service,
                parts: Arc::new(big_sync::MemoryPartStore::new()),
                repo,
            },
            admission,
            stop: admission_stop.then(blobs_stop),
            repo_stop,
        })
    }

    async fn inventory(&self) -> Res<DocId> {
        let mut document = automerge::Automerge::new();
        let mut transaction = document.transaction();
        transaction.put(automerge::ROOT, "fixture", true)?;
        transaction.commit();
        Ok(self
            .projection
            .repo
            .create_doc(document)
            .await?
            .document_id())
    }

    async fn pin(&self, inventory: &DocId, digest: BlobId, length: u64) -> Res<()> {
        self.admission.authorize(inventory, true).await?;
        self.admission
            .pins(inventory, 1, &BTreeMap::from([(digest, length)]), true)
            .await
    }

    /// Publish the pair as it stands now, as the runtime does for a decision it just made.
    async fn apply(&self, inventory: &DocId, digest: BlobId) -> Res<()> {
        let generation = self
            .admission
            .state(inventory, digest)
            .await?
            .expect("an applied pair exists")
            .generation;
        self.projection
            .apply(&self.admission, inventory, digest, generation)
            .await
    }
}

#[tokio::test]
async fn cached_bytes_do_not_publish_until_physical_bytes_exist() -> Res<()> {
    let harness = Harness::boot().await?;
    let inventory = harness.inventory().await?;
    let bytes = b"physical completion, not a cached boolean";
    let digest = BlobId::new(*Hash::new(bytes).as_bytes());
    harness
        .admission
        .produce(&inventory, digest, "writer", true)
        .await?;
    harness.pin(&inventory, digest, bytes.len() as u64).await?;
    harness.apply(&inventory, digest).await?;
    let part = crate::workers::inventory_part(&inventory);
    assert_eq!(
        harness.projection.parts.member_count(part.clone()).await?,
        0
    );
    assert!(
        harness
            .projection
            .parts
            .obj_payload(digest.into())
            .await?
            .is_none()
    );
    harness
        .projection
        .service
        .import_stream(futures::stream::iter([Ok(bytes::Bytes::copy_from_slice(
            bytes,
        ))]))
        .await?;
    harness.apply(&inventory, digest).await?;
    assert_eq!(harness.projection.parts.member_count(part).await?, 1);
    harness.stop.stop().await;
    harness.repo_stop.stop().await?;
    Ok(())
}

#[tokio::test]
async fn same_digest_keeps_inventory_admission_and_quarantine_independent() -> Res<()> {
    let harness = Harness::boot().await?;
    let first = harness.inventory().await?;
    let second = harness.inventory().await?;
    let upload = harness
        .projection
        .service
        .import_stream(futures::stream::iter([Ok(bytes::Bytes::from_static(
            b"independent inventory admissions",
        ))]))
        .await?;
    let info = upload.info.clone();
    let digest = info.blob_id;
    let peer = PeerKey::new([94; 32]);
    for inventory in [&first, &second] {
        harness.admission.select(inventory, "root", true).await?;
        harness.pin(inventory, digest, info.length_octets).await?;
        harness.admission.available(inventory, digest, true).await?;
    }
    harness
        .admission
        .observe(&first, digest, &peer, 10, true, None)
        .await?;
    harness.apply(&first, digest).await?;
    harness.apply(&second, digest).await?;
    harness.projection.service.release_import(&upload).await?;
    assert_eq!(
        harness.projection.parts.obj_parts(digest.into()).await?,
        vec![crate::workers::inventory_part(&first)]
    );
    harness.admission.select(&first, "root", false).await?;
    harness.apply(&first, digest).await?;
    assert!(
        harness
            .projection
            .parts
            .obj_parts(digest.into())
            .await?
            .is_empty()
    );
    assert!(
        harness
            .projection
            .service
            .ownership
            .contains(&digest)
            .await?
    );
    harness
        .admission
        .observe(&first, digest, &peer, 20, false, None)
        .await?;
    harness.apply(&first, digest).await?;
    assert!(
        !harness
            .projection
            .service
            .ownership
            .contains(&digest)
            .await?
    );
    assert!(harness.projection.service.stat(&digest).await?.is_some());
    harness.stop.stop().await;
    harness.repo_stop.stop().await?;
    Ok(())
}

#[tokio::test]
async fn publication_rechecks_live_inventory_access_instead_of_cached_authority() -> Res<()> {
    let harness = Harness::boot().await?;
    let inventory = DocId::new(
        ed25519_dalek::SigningKey::from_bytes(&[95; 32])
            .verifying_key()
            .to_bytes(),
    );
    let info = harness
        .projection
        .service
        .import_stream(futures::stream::iter([Ok(bytes::Bytes::from_static(
            b"cached authorization is not standing",
        ))]))
        .await?
        .info;
    harness
        .admission
        .produce(&inventory, info.blob_id, "writer", true)
        .await?;
    harness
        .pin(&inventory, info.blob_id, info.length_octets)
        .await?;
    assert!(
        harness
            .admission
            .state(&inventory, info.blob_id)
            .await?
            .unwrap()
            .advertisable()
    );
    assert!(!harness.projection.readable(&inventory).await?);
    harness.apply(&inventory, info.blob_id).await?;
    assert!(
        harness
            .projection
            .parts
            .obj_parts(info.blob_id.into())
            .await?
            .is_empty()
    );
    harness.stop.stop().await;
    harness.repo_stop.stop().await?;
    Ok(())
}

#[tokio::test]
async fn virtual_publication_requires_provider_and_physical_plaintext_dependency() -> Res<()> {
    let harness = Harness::boot().await?;
    let inventory = harness.inventory().await?;
    let service = &harness.projection.service;
    let (ciphertext, plaintext) = crate::encrypt::add_encrypted(
        service.store(),
        &service.provider,
        &service.roots,
        &crate::encrypt::MasterKey::random(),
        crate::encrypt::EncodingParams::default(),
        b"virtual ciphertext publication",
    )
    .await?;
    let digest = BlobId::new(*ciphertext.as_bytes());
    let info = service.stat(&digest).await?.unwrap();
    harness
        .admission
        .produce(&inventory, digest, "writer", true)
        .await?;
    harness.pin(&inventory, digest, info.length_octets).await?;
    assert!(
        service
            .store()
            .blobs()
            .sync_reader(ciphertext)
            .await?
            .is_none()
    );
    harness.apply(&inventory, digest).await?;
    assert!(service.ownership.contains(&digest).await?);
    assert!(
        service
            .ownership
            .contains(&BlobId::new(*plaintext.as_bytes()))
            .await?
    );
    assert_eq!(
        harness
            .projection
            .parts
            .member_count(crate::workers::inventory_part(&inventory))
            .await?,
        1
    );
    service.provider.remove(&ciphertext);
    assert!(local_bytes(service, digest).await?.is_none());
    harness.apply(&inventory, digest).await?;
    assert!(
        harness
            .projection
            .parts
            .obj_parts(digest.into())
            .await?
            .is_empty()
    );
    harness.stop.stop().await;
    harness.repo_stop.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a_change_landing_during_a_decision_stays_dirty_for_the_next_round() -> Res<()> {
    let harness = Harness::boot().await?;
    let inventory = harness.inventory().await?;
    let digest = BlobId::new(*Hash::new(b"observed mid-decision").as_bytes());
    harness.pin(&inventory, digest, 21).await?;
    // The runtime reads the pair, finds nothing to fetch (no source has it yet), and goes on to
    // publish what it decided.
    let decided = harness
        .admission
        .state(&inventory, digest)
        .await?
        .unwrap()
        .generation;
    // A source reports the digest before that publish lands.
    harness
        .admission
        .observe(&inventory, digest, &PeerKey::new([95; 32]), 1, true, None)
        .await?;
    harness
        .projection
        .apply(&harness.admission, &inventory, digest, decided)
        .await?;
    // The observation is a newer generation no decision saw: it must still be dirty, or nothing
    // would ever consider fetching it.
    let dirty = harness.admission.dirty().await?;
    let pending = dirty
        .iter()
        .find(|(dirty_inventory, dirty_digest, _)| {
            *dirty_inventory == inventory && *dirty_digest == digest
        })
        .expect("the observed pair is still dirty");
    assert!(pending.2 > decided);
    harness.stop.stop().await;
    harness.repo_stop.stop().await?;
    Ok(())
}
