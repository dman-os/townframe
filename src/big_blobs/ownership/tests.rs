use super::*;
use crate::encrypt::EncodingParams;
use crate::encrypt::MasterKey;
use crate::encrypt::add_encrypted;
use crate::encrypt::has_pair_tags;
use crate::pair_roots::PairRoots;
use big_blobs_core::BlobInfo;
use iroh_blobs::store::virtual_blob::Provider as _;
use std::collections::BTreeMap;

struct Harness {
    sql: Database,
    store: Store,
    roots: PairRoots,
    ownership: ByteOwnership,
    provider: Arc<CipherBlobProvider>,
    stop: StopToken,
}

impl Harness {
    async fn new() -> Res<Self> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[])?;
        let (memory, virtuals) =
            iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
        let store = Store::from(memory);
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;
        let roots = PairRoots::boot(sql.clone()).await?;
        let (ownership, stop) =
            ByteOwnership::boot(sql.clone(), store.clone(), Arc::clone(&provider)).await?;
        Ok(Self {
            sql,
            store,
            roots,
            ownership,
            provider,
            stop,
        })
    }

    async fn blob(&self, bytes: &'static [u8]) -> Res<(TempTag, BlobId)> {
        let tag = self.store.blobs().add_bytes(bytes).temp_tag().await?;
        let id = BlobId::new(*tag.hash().as_bytes());
        Ok((tag, id))
    }

    async fn rooted(&self, prefix: &str, id: &BlobId) -> Res<bool> {
        let hash = Hash::from(id.to_bytes32());
        Ok(self
            .store
            .tags()
            .get(format!("{prefix}:{hash}"))
            .await?
            .is_some())
    }
}

#[tokio::test]
async fn cancelled_publication_commits_claims_and_drains_external_io_before_release() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, id) = h.blob(b"serialized publication").await?;
    h.sql
        .execute_batch("CREATE TABLE publication (digest TEXT PRIMARY KEY NOT NULL) STRICT;")
        .await?;
    let sql = h.sql.clone();
    let (started, entered) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let mut publishing = Box::pin(h.ownership.publish(move |publication| {
        Box::pin(async move {
            publication.claim("inventory:publication", &[id]).await?;
            sql.execute(
                "INSERT INTO publication (digest) VALUES (?)",
                (id.to_string(),),
            )
            .await?;
            started.send(()).unwrap();
            waiting.await?;
            sql.execute(
                "DELETE FROM publication WHERE digest = ?",
                (id.to_string(),),
            )
            .await?;
            publication.release("inventory:publication").await?;
            drop(tag);
            Ok(())
        })
    }));
    assert!(futures::poll!(publishing.as_mut()).is_pending());
    entered.await?;
    assert!(h.rooted("owned", &id).await?);
    assert_eq!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners", ())
            .await?
            .all::<String>()?,
        vec!["inventory:publication".to_owned()]
    );
    assert_eq!(
        h.sql
            .query("SELECT digest FROM publication", ())
            .await?
            .all::<String>()?,
        vec![id.to_string()]
    );
    let mut release = Box::pin(h.ownership.release("inventory:publication"));
    assert!(futures::poll!(release.as_mut()).is_pending());
    drop((publishing, release));
    let mut stopping = Box::pin(h.stop.stop());
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    resume.send(()).unwrap();
    stopping.await;
    assert!(h.ownership.sender.is_closed());
    assert!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    assert!(
        h.sql
            .query("SELECT digest FROM publication", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    assert!(
        h.sql
            .query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    let hash = Hash::from(id.to_bytes32());
    assert!(h.store.tags().get(format!("owned:{hash}")).await?.is_none());
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_virtual_publication_serializes_inspection_claim_membership_and_release()
-> Res<()> {
    use big_sync::HostPartStore as _;

    let h = Harness::new().await?;
    let (cipher, plaintext) = add_encrypted(
        &h.store,
        &h.provider,
        &h.roots,
        &MasterKey::random(),
        EncodingParams::DEFAULT,
        b"serialized virtual publication",
    )
    .await?;
    let id = BlobId::new(*cipher.as_bytes());
    let plain_id = BlobId::new(*plaintext.as_bytes());
    let upload = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: h.store.blobs().get_bytes(cipher).await?.len() as u64,
    });
    let tag = h.store.tags().temp_tag(HashAndFormat::raw(cipher)).await?;
    h.ownership.stage(upload.clone(), tag).await?;
    h.ownership.resolve_pair(&id).await?;
    let parts = Arc::new(big_sync::MemoryPartStore::new());
    let part = big_sync_core::PartKey::new([107; 32]);
    let target = part.clone();
    let membership = Arc::clone(&parts);
    let (inspected, inspection) = oneshot::channel();
    let (claim, claiming) = oneshot::channel();
    let (committed, commitment) = oneshot::channel();
    let (publish, publishing) = oneshot::channel();
    let mut publication = Box::pin(h.ownership.publish(move |context| {
        Box::pin(async move {
            let guards = vec![
                context
                    .store()
                    .tags()
                    .temp_tag(HashAndFormat::raw(cipher))
                    .await?,
                context
                    .store()
                    .tags()
                    .temp_tag(HashAndFormat::raw(plaintext))
                    .await?,
            ];
            assert!(context.store().blobs().sync_reader(cipher).await?.is_none());
            assert!(
                context
                    .store()
                    .blobs()
                    .sync_reader(plaintext)
                    .await?
                    .is_some()
            );
            let iroh_blobs::api::proto::BlobStatus::Complete { size } =
                context.store().blobs().status(cipher).await?
            else {
                panic!("virtual ciphertext must be complete");
            };
            assert!(context.provider().reader_for(&cipher).is_some());
            inspected.send(()).unwrap();
            claiming.await?;
            assert!(
                context.provider().reader_for(&cipher).is_some(),
                "queued last-owner release must not tear down the inspected provider"
            );
            context.claim("inventory:publication", &[id]).await?;
            committed.send(()).unwrap();
            publishing.await?;
            membership.ensure_part(target.clone()).await?;
            membership
                .set_obj_payload(id.into(), serde_json::json!({"lengthOctets": size}))
                .await?;
            membership.add_obj_to_parts(id.into(), vec![target]).await?;
            drop(guards);
            Ok(())
        })
    }));
    assert!(futures::poll!(publication.as_mut()).is_pending());
    inspection.await?;
    let mut release = Box::pin(h.ownership.release_import(&upload));
    assert!(futures::poll!(release.as_mut()).is_pending());
    drop(publication);
    claim.send(()).unwrap();
    commitment.await?;
    assert!(futures::poll!(release.as_mut()).is_pending());
    assert_eq!(
        h.sql
            .query(
                "SELECT owner FROM big_blobs_byte_owners WHERE owner = ?",
                ("inventory:publication",),
            )
            .await?
            .all::<String>()?,
        vec!["inventory:publication".to_owned()]
    );
    assert!(h.rooted("owned", &id).await?);
    assert!(h.rooted("owned", &plain_id).await?);
    assert_eq!(parts.member_count(part.clone()).await?, 0);
    assert!(h.provider.reader_for(&cipher).is_some());
    publish.send(()).unwrap();
    release.await?;
    assert_eq!(parts.member_count(part.clone()).await?, 1);
    assert!(h.provider.reader_for(&cipher).is_some());
    assert!(has_pair_tags(&h.store, cipher).await?);
    let membership = Arc::clone(&parts);
    let target = part.clone();
    let (removed, removal) = oneshot::channel();
    let (release, releasing) = oneshot::channel();
    let mut unpublishing = Box::pin(h.ownership.publish(move |context| {
        Box::pin(async move {
            assert!(context.provider().reader_for(&cipher).is_some());
            membership
                .remove_obj_from_part(id.into(), target.clone())
                .await?;
            assert_eq!(membership.member_count(target).await?, 0);
            assert!(context.provider().reader_for(&cipher).is_some());
            removed.send(()).unwrap();
            releasing.await?;
            context.release("inventory:publication").await?;
            assert!(context.provider().reader_for(&cipher).is_none());
            Ok(())
        })
    }));
    assert!(futures::poll!(unpublishing.as_mut()).is_pending());
    removal.await?;
    assert_eq!(parts.member_count(part).await?, 0);
    assert!(h.rooted("owned", &id).await?);
    assert!(h.rooted("owned", &plain_id).await?);
    assert!(h.provider.reader_for(&cipher).is_some());
    assert_eq!(
        h.sql
            .query(
                "SELECT owner FROM big_blobs_byte_owners WHERE owner = ?",
                ("inventory:publication",),
            )
            .await?
            .all::<String>()?,
        vec!["inventory:publication".to_owned()]
    );
    drop(unpublishing);
    let mut retained = Box::pin(h.ownership.contains(&id));
    assert!(futures::poll!(retained.as_mut()).is_pending());
    release.send(()).unwrap();
    assert!(!retained.await?);
    assert!(!h.ownership.contains(&plain_id).await?);
    assert!(!h.rooted("owned", &id).await?);
    assert!(!h.rooted("owned", &plain_id).await?);
    assert!(!has_pair_tags(&h.store, cipher).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_publication_keeps_committed_ownership_until_explicit_release() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, id) = h.blob(b"failed publication").await?;
    let error = h
        .ownership
        .publish::<()>(move |publication| {
            Box::pin(async move {
                publication.claim("inventory:publication", &[id]).await?;
                drop(tag);
                eyre::bail!("injected publication failure")
            })
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected publication failure"));
    assert!(h.ownership.contains(&id).await?);
    assert!(h.rooted("owned", &id).await?);
    h.ownership
        .publish(move |publication| {
            Box::pin(async move { publication.release("inventory:publication").await })
        })
        .await?;
    assert!(!h.ownership.contains(&id).await?);
    assert!(!h.rooted("owned", &id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staging_holds_are_independent_idempotent_and_never_reused() -> Res<()> {
    let h = Harness::new().await?;
    let (first_tag, id) = h.blob(b"same upload bytes").await?;
    let (second_tag, same_id) = h.blob(b"same upload bytes").await?;
    assert_eq!(id, same_id);
    let first = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: 17,
    });
    let second = ImportedBlob::new(first.info.clone());
    h.ownership.stage(first.clone(), first_tag).await?;
    h.ownership.stage(second.clone(), second_tag).await?;
    h.ownership.release_import(&first).await?;
    h.ownership.release_import(&first).await?;
    assert!(h.ownership.contains(&id).await?);
    assert!(h.rooted("owned", &id).await?);
    let (third_tag, _) = h.blob(b"same upload bytes").await?;
    let third = ImportedBlob::new(first.info.clone());
    h.ownership.stage(third.clone(), third_tag).await?;
    h.ownership.release_import(&second).await?;
    h.ownership.release_import(&first).await?;
    assert!(h.ownership.contains(&id).await?);
    let (collision_tag, _) = h.blob(b"same upload bytes").await?;
    assert!(
        h.ownership
            .stage(first.clone(), collision_tag)
            .await
            .is_err()
    );
    let unknown = ImportedBlob::new(first.info.clone());
    h.ownership.release_import(&unknown).await?;
    assert!(h.ownership.contains(&id).await?);
    assert_eq!(
        h.sql
            .query(
                "SELECT COUNT(*) FROM big_blobs_upload_receipts WHERE released = 0",
                ()
            )
            .await?
            .all::<i64>()?,
        vec![1]
    );
    h.ownership.release_import(&third).await?;
    assert!(!h.ownership.contains(&id).await?);
    assert!(!h.rooted("owned", &id).await?);
    assert_eq!(
        h.sql
            .query(
                "SELECT released FROM big_blobs_upload_receipts ORDER BY owner",
                ()
            )
            .await?
            .all::<i64>()?,
        vec![1, 1, 1]
    );
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn upload_release_uses_durable_identity_not_supplied_blob_metadata() -> Res<()> {
    let h = Harness::new().await?;
    let (first_tag, first_id) = h.blob(b"first receipt bytes").await?;
    let (second_tag, second_id) = h.blob(b"second receipt bytes").await?;
    let first = ImportedBlob::new(BlobInfo {
        blob_id: first_id,
        length_octets: 19,
    });
    let second = ImportedBlob::new(BlobInfo {
        blob_id: second_id,
        length_octets: 20,
    });
    h.ownership.stage(first.clone(), first_tag).await?;
    h.ownership.stage(second.clone(), second_tag).await?;
    let mut changed_info = first.clone();
    changed_info.info = second.info.clone();
    h.ownership.release_import(&changed_info).await?;
    h.ownership.release_import(&first).await?;
    assert!(!h.ownership.contains(&first_id).await?);
    assert!(!h.rooted("owned", &first_id).await?);
    assert!(h.ownership.contains(&second_id).await?);
    assert!(h.rooted("owned", &second_id).await?);
    assert_eq!(
        h.sql
            .query("SELECT digest, released FROM big_blobs_upload_receipts", (),)
            .await?
            .all::<(String, i64)>()?
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(first_id.to_string(), 1), (second_id.to_string(), 0)])
    );
    h.ownership.release_import(&second).await?;
    assert!(!h.ownership.contains(&second_id).await?);
    assert!(!h.rooted("owned", &second_id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_upload_release_rolls_back_owner_and_receipt_together() -> Res<()> {
    let h = Harness::new().await?;
    let (first_tag, id) = h.blob(b"atomic receipt release").await?;
    let (second_tag, _) = h.blob(b"atomic receipt release").await?;
    let first = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: 22,
    });
    let second = ImportedBlob::new(first.info.clone());
    h.ownership.stage(first.clone(), first_tag).await?;
    h.ownership.stage(second.clone(), second_tag).await?;
    h.sql
        .execute_batch(
            "CREATE TRIGGER fail_upload_owner_release BEFORE DELETE ON big_blobs_byte_owners
             BEGIN SELECT RAISE(FAIL, 'injected upload release failure'); END;",
        )
        .await?;
    let error = h.ownership.release_import(&first).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected upload release failure")
    );
    assert_eq!(
        h.sql
            .query(
                "SELECT released FROM big_blobs_upload_receipts ORDER BY owner",
                ()
            )
            .await?
            .all::<i64>()?,
        vec![0, 0]
    );
    assert_eq!(
        h.sql
            .query("SELECT COUNT(*) FROM big_blobs_byte_owners", ())
            .await?
            .all::<i64>()?,
        vec![2]
    );
    assert!(h.ownership.contains(&id).await?);
    assert!(h.rooted("owned", &id).await?);
    h.sql
        .execute("DROP TRIGGER fail_upload_owner_release", ())
        .await?;
    h.ownership.release_import(&first).await?;
    h.ownership.release_import(&first).await?;
    assert_eq!(
        h.sql
            .query("SELECT owner, released FROM big_blobs_upload_receipts", ())
            .await?
            .all::<(String, i64)>()?
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(first.owner(), 1), (second.owner(), 0)])
    );
    assert!(h.ownership.contains(&id).await?);
    assert!(h.rooted("owned", &id).await?);
    h.ownership.release_import(&second).await?;
    assert!(!h.ownership.contains(&id).await?);
    assert!(!h.rooted("owned", &id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_upload_and_release_drain_before_shutdown() -> Res<()> {
    let h = Harness::new().await?;
    let (first_tag, id) = h.blob(b"cancelled receipt operations").await?;
    let (second_tag, _) = h.blob(b"cancelled receipt operations").await?;
    let first = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: 28,
    });
    let second = ImportedBlob::new(first.info.clone());
    h.ownership.stage(first.clone(), first_tag).await?;
    let (started, entered) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let mut barrier = Box::pin(h.ownership.run(move |_| {
        Box::pin(async move {
            started.send(()).unwrap();
            waiting.await?;
            Ok(())
        })
    }));
    assert!(futures::poll!(barrier.as_mut()).is_pending());
    entered.await?;
    let mut upload = Box::pin(h.ownership.stage(second.clone(), second_tag));
    assert!(futures::poll!(upload.as_mut()).is_pending());
    let mut release = Box::pin(h.ownership.release_import(&first));
    assert!(futures::poll!(release.as_mut()).is_pending());
    drop((barrier, upload, release));
    let mut stopping = Box::pin(h.stop.stop());
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    resume.send(()).unwrap();
    stopping.await;
    assert!(h.ownership.sender.is_closed());
    assert_eq!(
        h.sql
            .query("SELECT owner, released FROM big_blobs_upload_receipts", ())
            .await?
            .all::<(String, i64)>()?
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(first.owner(), 1), (second.owner(), 0)])
    );
    assert_eq!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners", ())
            .await?
            .all::<String>()?,
        vec![second.owner()]
    );
    let hash = Hash::from(id.to_bytes32());
    assert!(h.store.tags().get(format!("owned:{hash}")).await?.is_some());
    assert!(
        h.sql
            .query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn adopted_document_ownership_and_staging_tombstone_survive_filesystem_restart() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let bytes = b"durable document adoption";
    let hash = Hash::new(bytes);
    let imported = ImportedBlob::new(BlobInfo {
        blob_id: BlobId::new(*hash.as_bytes()),
        length_octets: bytes.len() as u64,
    });
    let document = "document:retention:restart";
    for phase in 0..3 {
        let sql = runtime.database(
            directory.path().join("owners.sqlite"),
            &[],
            "test_migrations",
        );
        let roots = PairRoots::boot(sql.clone()).await?;
        let (filesystem, virtuals) = iroh_blobs::store::fs::FsStore::load_with_virtuals(
            directory.path().join("blobs.db"),
            iroh_blobs::store::fs::options::Options::new(directory.path()),
        )
        .await?;
        let store = Store::from(filesystem);
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;
        let (ownership, stop) =
            ByteOwnership::boot(sql.clone(), store.clone(), Arc::clone(&provider)).await?;
        if phase == 0 {
            let tag = store.blobs().add_bytes(bytes.as_slice()).temp_tag().await?;
            ownership.stage(imported.clone(), tag).await?;
            ownership.claim(document, &[imported.info.blob_id]).await?;
            ownership.adopt(&imported, document).await?;
        } else {
            ownership.release_import(&imported).await?;
            assert_eq!(
                sql.query("SELECT released FROM big_blobs_upload_receipts", ())
                    .await?
                    .all::<i64>()?,
                vec![1]
            );
            if phase == 1 {
                assert_eq!(store.blobs().get_bytes(hash).await?.as_ref(), bytes);
                assert_eq!(
                    sql.query("SELECT owner FROM big_blobs_byte_owners", ())
                        .await?
                        .all::<String>()?,
                    vec![document.to_owned()]
                );
                for prefix in ["owned", "pin"] {
                    assert!(
                        store
                            .tags()
                            .get(format!("{prefix}:{hash}"))
                            .await?
                            .is_some()
                    );
                }
                ownership.adopt(&imported, document).await?;
                ownership.release(document).await?;
            }
            assert!(!ownership.contains(&imported.info.blob_id).await?);
            for prefix in ["owned", "pin"] {
                assert!(
                    store
                        .tags()
                        .get(format!("{prefix}:{hash}"))
                        .await?
                        .is_none()
                );
            }
        }
        stop.stop().await;
        drop((ownership, roots, provider, sql));
        store.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn virtual_staging_adoption_keeps_dependencies_until_the_last_independent_hold() -> Res<()> {
    let h = Harness::new().await?;
    let (cipher, plain) = add_encrypted(
        &h.store,
        &h.provider,
        &h.roots,
        &MasterKey::random(),
        EncodingParams::DEFAULT,
        b"virtual staging adoption",
    )
    .await?;
    let id = BlobId::new(*cipher.as_bytes());
    let plaintext = BlobId::new(*plain.as_bytes());
    let first = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: h.store.blobs().get_bytes(cipher).await?.len() as u64,
    });
    let second = ImportedBlob::new(first.info.clone());
    for imported in [&first, &second] {
        let tag = h.store.tags().temp_tag(HashAndFormat::raw(cipher)).await?;
        h.ownership.stage(imported.clone(), tag).await?;
    }
    h.ownership.resolve_pair(&id).await?;
    h.ownership
        .claim("document:retention:virtual", &[id])
        .await?;
    h.ownership
        .adopt(&first, "document:retention:virtual")
        .await?;
    h.ownership.release("document:retention:virtual").await?;
    h.ownership.release_import(&first).await?;
    assert!(h.ownership.contains(&id).await?);
    assert!(h.ownership.contains(&plaintext).await?);
    assert!(h.rooted("owned", &id).await?);
    assert!(h.rooted("owned", &plaintext).await?);
    assert!(!h.rooted("pin", &id).await?);
    assert!(has_pair_tags(&h.store, cipher).await?);
    assert!(h.provider.reader_for(&cipher).is_some());
    assert!(!h.store.blobs().get_bytes(cipher).await?.is_empty());
    h.ownership.release_import(&second).await?;
    assert!(!h.ownership.contains(&id).await?);
    assert!(!h.ownership.contains(&plaintext).await?);
    assert!(!h.rooted("owned", &id).await?);
    assert!(!h.rooted("owned", &plaintext).await?);
    assert!(!has_pair_tags(&h.store, cipher).await?);
    assert!(h.provider.reader_for(&cipher).is_none());
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staging_holds_and_release_tombstones_survive_filesystem_restart() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let bytes = b"durable upload receipts";
    let hash = Hash::new(bytes);
    let first = ImportedBlob::new(BlobInfo {
        blob_id: BlobId::new(*hash.as_bytes()),
        length_octets: bytes.len() as u64,
    });
    let second = ImportedBlob::new(first.info.clone());
    let inventory = format!("inventory:restart:{}", first.info.blob_id);
    for phase in 0..3 {
        let sql = runtime.database(
            directory.path().join("owners.sqlite"),
            &[],
            "test_migrations",
        );
        let roots = PairRoots::boot(sql.clone()).await?;
        let (filesystem, virtuals) = iroh_blobs::store::fs::FsStore::load_with_virtuals(
            directory.path().join("blobs.db"),
            iroh_blobs::store::fs::options::Options::new(directory.path()),
        )
        .await?;
        let store = Store::from(filesystem);
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;
        let (ownership, stop) =
            ByteOwnership::boot(sql.clone(), store.clone(), Arc::clone(&provider)).await?;
        match phase {
            0 => {
                for receipt in [&first, &second] {
                    let tag = store.blobs().add_bytes(bytes.as_slice()).temp_tag().await?;
                    ownership.stage(receipt.clone(), tag).await?;
                }
                ownership.release_import(&first).await?;
            }
            1 => {
                assert!(ownership.contains(&first.info.blob_id).await?);
                assert!(store.tags().get(format!("owned:{hash}")).await?.is_some());
                assert_eq!(store.blobs().get_bytes(hash).await?.as_ref(), bytes);
                ownership.release_import(&first).await?;
                let collision = store.blobs().add_bytes(bytes.as_slice()).temp_tag().await?;
                assert!(ownership.stage(first.clone(), collision).await.is_err());
                assert!(ownership.contains(&second.info.blob_id).await?);
                assert_eq!(
                    sql.query("SELECT owner, released FROM big_blobs_upload_receipts", ())
                        .await?
                        .all::<(String, i64)>()?
                        .into_iter()
                        .collect::<BTreeMap<_, _>>(),
                    BTreeMap::from([(first.owner(), 1), (second.owner(), 0)])
                );
                ownership
                    .set_owner(&inventory, &first.info.blob_id, true, vec![])
                    .await?;
                ownership.release_import(&second).await?;
            }
            2 => {
                ownership.release_import(&first).await?;
                ownership.release_import(&second).await?;
                assert!(ownership.contains(&first.info.blob_id).await?);
                assert!(store.tags().get(format!("owned:{hash}")).await?.is_some());
                assert_eq!(
                    sql.query("SELECT owner FROM big_blobs_byte_owners", ())
                        .await?
                        .all::<String>()?,
                    vec![inventory.clone()]
                );
                ownership
                    .set_owner(&inventory, &first.info.blob_id, false, vec![])
                    .await?;
                assert!(!ownership.contains(&first.info.blob_id).await?);
                assert!(store.tags().get(format!("owned:{hash}")).await?.is_none());
                assert_eq!(
                    sql.query(
                        "SELECT released FROM big_blobs_upload_receipts ORDER BY owner",
                        ()
                    )
                    .await?
                    .all::<i64>()?,
                    vec![1, 1]
                );
            }
            _ => unreachable!(),
        }
        stop.stop().await;
        drop((ownership, provider, roots, sql));
        store.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn owners_release_independently_and_reconcile_only_one_digest() -> Res<()> {
    let h = Harness::new().await?;
    let (tag_a, a) = h.blob(b"owned A").await?;
    let (tag_b, b) = h.blob(b"owned B").await?;
    h.ownership.claim("upload:first", &[a, b]).await?;
    h.ownership.claim("upload:first", &[a]).await?;
    h.ownership.claim("upload:second", &[a]).await?;
    h.ownership
        .claim("document:retention:first", &[a, b])
        .await?;
    drop((tag_a, tag_b));
    h.ownership.release("upload:first").await?;
    h.ownership.release("upload:first").await?;
    h.ownership
        .transaction(move |tx| {
            Box::pin(async move { tx.reconcile("document:retention:", &a, &[]).await })
        })
        .await?;
    assert!(!h.rooted("pin", &a).await?);
    assert!(h.rooted("pin", &b).await?);
    assert!(h.ownership.contains(&a).await?);
    h.ownership.release("upload:second").await?;
    assert!(!h.ownership.contains(&a).await?);
    assert!(!h.rooted("owned", &a).await?);
    assert!(h.ownership.contains(&b).await?);
    h.ownership.replace("document:retention:first", &[]).await?;
    assert!(!h.rooted("owned", &b).await?);
    assert!(!h.rooted("pin", &b).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn inventory_owners_set_and_remove_only_the_exact_pair() -> Res<()> {
    let h = Harness::new().await?;
    let (tag_a, a) = h.blob(b"accepted inventory bytes").await?;
    let (tag_b, b) = h.blob(b"quarantined inventory bytes").await?;
    let first = format!("inventory:first:{a}");
    let second = format!("inventory:second:{a}");
    let other = format!("inventory:first:{b}");
    h.ownership.set_owner(&first, &a, true, vec![tag_a]).await?;
    h.ownership.set_owner(&first, &a, true, vec![]).await?;
    h.ownership.set_owner(&second, &a, true, vec![]).await?;
    h.ownership.set_owner(&other, &b, true, vec![tag_b]).await?;
    h.ownership.set_owner(&other, &a, false, vec![]).await?;
    assert_eq!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners ORDER BY owner", ())
            .await?
            .all::<String>()?,
        {
            let mut owners = vec![first.clone(), second.clone(), other.clone()];
            owners.sort();
            owners
        }
    );
    h.ownership.set_owner(&first, &a, false, vec![]).await?;
    h.ownership.set_owner(&first, &a, false, vec![]).await?;
    assert!(h.ownership.contains(&a).await?);
    assert!(h.rooted("owned", &a).await?);
    assert!(!h.rooted("held", &a).await?);
    h.ownership.set_owner(&second, &a, false, vec![]).await?;
    assert!(!h.ownership.contains(&a).await?);
    assert!(!h.rooted("owned", &a).await?);
    assert!(h.ownership.contains(&b).await?);
    assert!(h.rooted("owned", &b).await?);
    assert!(!h.rooted("held", &b).await?);
    h.ownership.set_owner(&other, &b, false, vec![]).await?;
    assert!(!h.ownership.contains(&b).await?);
    assert!(!h.rooted("owned", &b).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_inventory_publication_precedes_upload_release_and_provider_removal() -> Res<()> {
    let h = Harness::new().await?;
    let (c, p) = add_encrypted(
        &h.store,
        &h.provider,
        &h.roots,
        &MasterKey::random(),
        EncodingParams::DEFAULT,
        b"downloaded virtual inventory bytes",
    )
    .await?;
    let cipher = BlobId::new(*c.as_bytes());
    let plain = BlobId::new(*p.as_bytes());
    let receipt = ImportedBlob::new(BlobInfo {
        blob_id: cipher,
        length_octets: h.store.blobs().get_bytes(c).await?.len() as u64,
    });
    let tag = h.store.tags().temp_tag(HashAndFormat::raw(c)).await?;
    h.ownership.stage(receipt.clone(), tag).await?;
    h.ownership.resolve_pair(&cipher).await?;
    let guards = vec![
        h.store.tags().temp_tag(HashAndFormat::raw(c)).await?,
        h.store.tags().temp_tag(HashAndFormat::raw(p)).await?,
    ];
    let (started, entered) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let mut barrier = Box::pin(h.ownership.run(move |_| {
        Box::pin(async move {
            started.send(()).unwrap();
            waiting.await?;
            Ok(())
        })
    }));
    assert!(futures::poll!(barrier.as_mut()).is_pending());
    entered.await?;
    let owner = format!("inventory:download:{cipher}");
    let client = h.ownership.clone();
    let mut publish = Box::pin(client.set_owner(&owner, &cipher, true, guards));
    assert!(futures::poll!(publish.as_mut()).is_pending());
    drop((publish, barrier));
    let mut release = Box::pin(h.ownership.release_import(&receipt));
    assert!(futures::poll!(release.as_mut()).is_pending());
    resume.send(()).unwrap();
    release.await?;
    assert_eq!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners", ())
            .await?
            .all::<String>()?,
        vec![owner.clone()]
    );
    assert!(h.ownership.contains(&cipher).await?);
    assert!(h.ownership.contains(&plain).await?);
    assert!(h.rooted("owned", &cipher).await?);
    assert!(h.rooted("owned", &plain).await?);
    assert!(!h.rooted("held", &cipher).await?);
    assert!(!h.rooted("held", &plain).await?);
    assert!(h.provider.reader_for(&c).is_some());
    assert!(has_pair_tags(&h.store, c).await?);
    client.set_owner(&owner, &cipher, false, vec![]).await?;
    assert!(!h.ownership.contains(&cipher).await?);
    assert!(!h.ownership.contains(&plain).await?);
    assert!(!h.rooted("owned", &cipher).await?);
    assert!(!h.rooted("owned", &plain).await?);
    assert!(h.provider.reader_for(&c).is_none());
    assert!(!has_pair_tags(&h.store, c).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn virtual_owners_protect_plaintext_and_provider_until_last_release() -> Res<()> {
    let h = Harness::new().await?;
    let (c, p) = add_encrypted(
        &h.store,
        &h.provider,
        &h.roots,
        &MasterKey::random(),
        EncodingParams::DEFAULT,
        b"virtual owner dependency",
    )
    .await?;
    let cipher = BlobId::new(*c.as_bytes());
    let plain = BlobId::new(*p.as_bytes());
    h.ownership
        .claim("document:retention:source", &[cipher])
        .await?;
    h.ownership.claim("mirror:inventory", &[cipher]).await?;
    h.ownership.claim("in-flight:reader", &[cipher]).await?;
    h.ownership.claim("quarantine:plaintext", &[plain]).await?;
    h.ownership.resolve_pair(&cipher).await?;
    h.ownership.release("document:retention:source").await?;
    h.ownership.release("mirror:inventory").await?;
    assert!(has_pair_tags(&h.store, c).await?);
    assert!(h.rooted("owned", &plain).await?);
    assert!(h.provider.reader_for(&c).is_some());
    assert!(!h.store.blobs().get_bytes(c).await?.is_empty());
    h.ownership.release("in-flight:reader").await?;
    assert!(!has_pair_tags(&h.store, c).await?);
    assert!(h.provider.reader_for(&c).is_none());
    assert!(h.ownership.contains(&plain).await?);
    assert!(h.rooted("owned", &plain).await?);
    h.ownership.release("quarantine:plaintext").await?;
    assert!(!h.ownership.contains(&plain).await?);
    assert!(!h.rooted("owned", &plain).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unresolved_pair_roots_and_retention_pins_survive_owner_release() -> Res<()> {
    let h = Harness::new().await?;
    let (c, _) = add_encrypted(
        &h.store,
        &h.provider,
        &h.roots,
        &MasterKey::random(),
        EncodingParams::DEFAULT,
        b"preclaim and retention pin roots",
    )
    .await?;
    let id = BlobId::new(*c.as_bytes());
    h.ownership.claim("writer:effect", &[id]).await?;
    h.ownership.release("writer:effect").await?;
    assert!(has_pair_tags(&h.store, c).await?);
    assert!(h.provider.reader_for(&c).is_some());
    h.store
        .tags()
        .set(format!("pin:{c}"), HashAndFormat::raw(c))
        .await?;
    h.ownership.resolve_pair(&id).await?;
    assert!(has_pair_tags(&h.store, c).await?);
    assert!(h.provider.reader_for(&c).is_some());
    h.store.tags().delete(format!("pin:{c}")).await?;
    h.ownership.recover().await?;
    assert!(!has_pair_tags(&h.store, c).await?);
    assert!(h.provider.reader_for(&c).is_none());
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn absent_bytes_and_plaintext_tags_do_not_establish_ownership() -> Res<()> {
    let h = Harness::new().await?;
    let absent = BlobId::new(*Hash::new(b"absent").as_bytes());
    let owner = format!("inventory:missing:{absent}");
    assert!(
        h.ownership
            .set_owner(&owner, &absent, true, vec![])
            .await
            .is_err()
    );
    assert!(!h.ownership.contains(&absent).await?);
    let (_tag, plain) = h.blob(b"unrelated plaintext").await?;
    let c = Hash::from(absent.to_bytes32());
    h.store
        .tags()
        .set(
            format!("pt:{c}"),
            HashAndFormat::raw(Hash::from(plain.to_bytes32())),
        )
        .await?;
    assert!(
        h.ownership
            .set_owner(&owner, &absent, true, vec![])
            .await
            .is_err()
    );
    assert!(!h.ownership.contains(&absent).await?);
    assert!(!h.ownership.contains(&plain).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rollback_preserves_domain_rows_and_previous_owner_set() -> Res<()> {
    let h = Harness::new().await?;
    h.sql
        .execute(
            "CREATE TABLE ownership_domain (value TEXT PRIMARY KEY) STRICT",
            (),
        )
        .await?;
    let (_tag, id) = h.blob(b"rollback bytes").await?;
    h.ownership.claim("upload:rollback", &[id]).await?;
    let error = h
        .ownership
        .transaction(|tx| {
            Box::pin(async move {
                tx.sql()
                    .execute("INSERT INTO ownership_domain VALUES ('uncommitted')", ())
                    .await?;
                tx.replace("upload:rollback", &[BlobId::new([8; 32])]).await
            })
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "byte claim requires complete stored bytes"
    );
    assert!(h.ownership.contains(&id).await?);
    assert!(h.rooted("owned", &id).await?);
    assert!(
        h.sql
            .query("SELECT value FROM ownership_domain", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_operation_keeps_tags_until_sql_and_effects_finish() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, id) = h.blob(b"cancelled ownership commit").await?;
    let (started, entered) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let mut completion = Box::pin(h.ownership.transaction(move |tx| {
        Box::pin(async move {
            tx.tags.push(tag);
            started.send(()).unwrap();
            waiting.await?;
            tx.claim("upload:cancelled", &[id]).await
        })
    }));
    assert!(futures::poll!(completion.as_mut()).is_pending());
    entered.await?;
    drop(completion);
    let mut next = Box::pin(h.ownership.contains(&id));
    assert!(futures::poll!(next.as_mut()).is_pending());
    resume.send(()).unwrap();
    assert!(next.await?);
    assert!(h.rooted("owned", &id).await?);
    assert!(
        h.sql
            .query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    h.ownership.release("upload:cancelled").await?;
    assert!(!h.rooted("owned", &id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_accepted_work_with_live_client_clones() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, id) = h.blob(b"drained ownership").await?;
    let (started, entered) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let client = h.ownership.clone();
    let mut first = Box::pin(h.ownership.transaction(move |tx| {
        Box::pin(async move {
            tx.tags.push(tag);
            started.send(()).unwrap();
            waiting.await?;
            tx.claim("upload:drain-first", &[id]).await
        })
    }));
    assert!(futures::poll!(first.as_mut()).is_pending());
    entered.await?;
    let ids = [id];
    let mut second = Box::pin(client.claim("upload:drain-second", &ids));
    assert!(futures::poll!(second.as_mut()).is_pending());
    drop((first, second));
    let mut stopping = Box::pin(h.stop.stop());
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    resume.send(()).unwrap();
    stopping.await;
    assert!(client.sender.is_closed());
    assert_eq!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners ORDER BY owner", ())
            .await?
            .all::<String>()?,
        vec!["upload:drain-first", "upload:drain-second"]
    );
    let hash = Hash::from(id.to_bytes32());
    assert!(h.store.tags().get(format!("owned:{hash}")).await?.is_some());
    assert!(
        h.sql
            .query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_adoption_waits_for_document_commit_and_releases_only_its_staging_hold() -> Res<()>
{
    let h = Harness::new().await?;
    let (first_tag, id) = h.blob(b"atomic adoption").await?;
    let (second_tag, _) = h.blob(b"atomic adoption").await?;
    let first = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: 15,
    });
    let second = ImportedBlob::new(first.info.clone());
    h.ownership.stage(first.clone(), first_tag).await?;
    h.ownership.stage(second.clone(), second_tag).await?;
    let (started, entered) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let mut document = Box::pin(h.ownership.transaction(move |tx| {
        Box::pin(async move {
            tx.claim("document:retention:adopted", &[id]).await?;
            started.send(()).unwrap();
            waiting.await?;
            Ok(())
        })
    }));
    assert!(futures::poll!(document.as_mut()).is_pending());
    entered.await?;
    let mut adoption = Box::pin(h.ownership.adopt(&first, "document:retention:adopted"));
    assert!(futures::poll!(adoption.as_mut()).is_pending());
    drop((document, adoption));
    let mut release = Box::pin(h.ownership.release_import(&second));
    assert!(futures::poll!(release.as_mut()).is_pending());
    resume.send(()).unwrap();
    release.await?;
    assert!(h.ownership.contains(&id).await?);
    assert!(h.rooted("owned", &id).await?);
    assert!(h.rooted("pin", &id).await?);
    assert_eq!(
        h.sql
            .query(
                "SELECT owner FROM big_blobs_byte_owners WHERE digest = ?",
                (id.to_string(),)
            )
            .await?
            .all::<String>()?,
        vec!["document:retention:adopted".to_string()]
    );
    h.ownership.release("document:retention:adopted").await?;
    assert!(!h.rooted("owned", &id).await?);
    assert!(!h.rooted("pin", &id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn adoption_requires_committed_ownership_of_the_durable_staging_digest() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, a) = h.blob(b"staged attachment").await?;
    let (other_tag, b) = h.blob(b"other attachment").await?;
    let mut imported = ImportedBlob::new(BlobInfo {
        blob_id: a,
        length_octets: 17,
    });
    h.ownership.stage(imported.clone(), tag).await?;
    assert!(
        h.ownership
            .transaction::<()>(move |tx| Box::pin(async move {
                tx.claim("document:retention:target", &[a]).await?;
                Err(eyre::eyre!("injected document rollback"))
            }))
            .await
            .is_err()
    );
    assert!(
        h.ownership
            .adopt(&imported, "document:retention:target")
            .await
            .is_err()
    );
    h.ownership.claim("document:retention:target", &[b]).await?;
    imported.info.blob_id = b;
    assert!(
        h.ownership
            .adopt(&imported, "document:retention:target")
            .await
            .is_err()
    );
    assert!(
        h.ownership
            .adopt(&imported, "writer:retention:target")
            .await
            .is_err()
    );
    assert!(h.ownership.contains(&a).await?);
    assert!(h.rooted("owned", &a).await?);
    h.ownership.claim("document:retention:target", &[a]).await?;
    h.ownership
        .adopt(&imported, "document:retention:target")
        .await?;
    h.ownership
        .adopt(&imported, "document:retention:target")
        .await?;
    assert_eq!(
        h.sql
            .query("SELECT released FROM big_blobs_upload_receipts", ())
            .await?
            .all::<i64>()?,
        vec![1]
    );
    drop(other_tag);
    h.ownership.release("document:retention:target").await?;
    assert!(!h.ownership.contains(&a).await?);
    assert!(!h.ownership.contains(&b).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_staging_handoff_preserves_committed_document_and_staging_owners() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, id) = h.blob(b"failed handoff").await?;
    let imported = ImportedBlob::new(BlobInfo {
        blob_id: id,
        length_octets: 14,
    });
    h.ownership.stage(imported.clone(), tag).await?;
    h.ownership
        .claim("document:retention:target", &[id])
        .await?;
    h.sql
        .execute_batch(
            "CREATE TRIGGER fail_staging_handoff BEFORE DELETE ON big_blobs_byte_owners
         BEGIN SELECT RAISE(FAIL, 'injected staging handoff failure'); END;",
        )
        .await?;
    let error = h
        .ownership
        .adopt(&imported, "document:retention:target")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected staging handoff failure")
    );
    assert_eq!(
        h.sql
            .query("SELECT released FROM big_blobs_upload_receipts", ())
            .await?
            .all::<i64>()?,
        vec![0]
    );
    assert_eq!(
        h.sql
            .query("SELECT COUNT(*) FROM big_blobs_byte_owners", ())
            .await?
            .all::<i64>()?,
        vec![2]
    );
    assert!(h.rooted("owned", &id).await?);
    assert!(h.rooted("pin", &id).await?);
    h.sql
        .execute("DROP TRIGGER fail_staging_handoff", ())
        .await?;
    h.ownership
        .adopt(&imported, "document:retention:target")
        .await?;
    h.ownership.release_import(&imported).await?;
    assert!(h.ownership.contains(&id).await?);
    assert_eq!(
        h.sql
            .query("SELECT owner FROM big_blobs_byte_owners", ())
            .await?
            .all::<String>()?,
        vec!["document:retention:target".to_owned()]
    );
    h.ownership.release("document:retention:target").await?;
    assert!(!h.rooted("owned", &id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_effect_ack_retains_guards_and_replays_idempotently() -> Res<()> {
    let h = Harness::new().await?;
    let (tag, id) = h.blob(b"effect recovery").await?;
    h.sql
        .execute_batch(
            "CREATE TRIGGER fail_byte_effect_ack BEFORE DELETE ON big_blobs_byte_effects
            BEGIN SELECT RAISE(FAIL, 'injected effect ack failure'); END;",
        )
        .await?;
    let owner = format!("inventory:recovery:{id}");
    let claiming = owner.clone();
    let (published, publication) = oneshot::channel();
    let error = h
        .ownership
        .publish(move |context| {
            Box::pin(async move {
                context.claim(&claiming, &[id]).await?;
                published.send(()).unwrap();
                drop(tag);
                Ok(true)
            })
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected effect ack failure"));
    assert!(publication.await.is_err());
    assert!(h.ownership.contains(&id).await?);
    assert!(
        h.ownership
            .run(|state| Box::pin(async move { Ok(state.tags.len() == 1) }))
            .await?
    );
    assert_eq!(
        h.sql
            .query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?,
        vec![id.to_string()]
    );
    h.sql
        .execute("DROP TRIGGER fail_byte_effect_ack", ())
        .await?;
    let claiming = owner.clone();
    assert!(
        h.ownership
            .publish(move |context| {
                Box::pin(async move {
                    context.claim(&claiming, &[id]).await?;
                    Ok(true)
                })
            })
            .await?
    );
    h.ownership.recover().await?;
    assert!(h.rooted("owned", &id).await?);
    assert!(
        h.ownership
            .run(|state| Box::pin(async move { Ok(state.tags.is_empty()) }))
            .await?
    );
    h.ownership
        .publish(move |context| Box::pin(async move { context.release(&owner).await }))
        .await?;
    assert!(!h.rooted("owned", &id).await?);
    h.stop.stop().await;
    h.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn filesystem_restart_replays_committed_effects_without_persisting_keys() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let database_path = directory.path().join("owners.sqlite");
    let store_path = directory.path().join("blobs.db");
    let sql = runtime.database(&database_path, &[], "test_migrations");
    let roots = PairRoots::boot(sql.clone()).await?;
    let (filesystem, virtuals) = iroh_blobs::store::fs::FsStore::load_with_virtuals(
        store_path.clone(),
        iroh_blobs::store::fs::options::Options::new(directory.path()),
    )
    .await?;
    let store = Store::from(filesystem);
    let provider = Arc::new(CipherBlobProvider::new());
    provider.register(&virtuals)?;
    let ownership = State::new(sql.clone(), store.clone(), Arc::clone(&provider)).await?;
    let key = MasterKey::random();
    let (c, p) = add_encrypted(
        &store,
        &provider,
        &roots,
        &key,
        EncodingParams::DEFAULT,
        b"durable virtual bytes, ephemeral key",
    )
    .await?;
    let id = BlobId::new(*c.as_bytes());
    let mut pending = ownership.begin().await?;
    pending.claim("mirror:restart", &[id]).await?;
    let OwnershipTransaction { tx, tags, .. } = pending;
    tx.commit().await?;
    drop(tags);
    assert!(store.tags().get(format!("owned:{c}")).await?.is_none());
    drop((ownership, roots, provider, sql));
    store.shutdown().await?;
    drop(store);

    let sql = runtime.database(&database_path, &[], "test_migrations");
    let roots = PairRoots::boot(sql.clone()).await?;
    let (filesystem, virtuals) = iroh_blobs::store::fs::FsStore::load_with_virtuals(
        store_path.clone(),
        iroh_blobs::store::fs::options::Options::new(directory.path()),
    )
    .await?;
    let store = Store::from(filesystem);
    let provider = Arc::new(CipherBlobProvider::new());
    provider.register(&virtuals)?;
    let mut ownership = State::new(sql.clone(), store.clone(), Arc::clone(&provider)).await?;
    assert!(ownership.contains(&id).await?);
    assert!(store.tags().get(format!("owned:{c}")).await?.is_some());
    assert!(store.tags().get(format!("owned:{p}")).await?.is_some());
    assert!(provider.reader_for(&c).is_none());
    assert!(store.blobs().get_bytes(c).await.is_err());
    ownership
        .transaction(move |tx| Box::pin(async move { tx.replace("mirror:restart", &[id]).await }))
        .await?;
    assert!(ownership.contains(&id).await?);
    assert!(provider.reader_for(&c).is_none());
    assert!(store.tags().get(format!("owned:{p}")).await?.is_some());
    assert!(
        sql.query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    provider
        .register_pair(&store, &roots, c, &key, p, EncodingParams::DEFAULT)
        .await?;
    assert!(!store.blobs().get_bytes(c).await?.is_empty());
    ownership
        .transaction(move |tx| {
            Box::pin(async move {
                tx.dependency(&id).await?;
                tx.dirty(&id).await?;
                tx.sql()
                    .execute(
                        "DELETE FROM blob_pair_root WHERE cipher_hash = ?",
                        (c.to_hex().to_string(),),
                    )
                    .await?;
                Ok(())
            })
        })
        .await?;
    let pending = ownership.begin().await?;
    pending.release("mirror:restart").await?;
    let OwnershipTransaction { tx, tags, .. } = pending;
    tx.commit().await?;
    drop(tags);
    drop(ownership);
    let (ownership, stop) = ByteOwnership::boot(sql, store.clone(), Arc::clone(&provider)).await?;
    assert!(!ownership.contains(&id).await?);
    assert!(!has_pair_tags(&store, c).await?);
    assert!(provider.reader_for(&c).is_none());
    assert!(store.tags().get(format!("owned:{p}")).await?.is_none());
    stop.stop().await;
    store.shutdown().await?;
    Ok(())
}
