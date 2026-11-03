use super::*;
use crate::encrypt::EncodingParams;
use crate::encrypt::MasterKey;
use crate::encrypt::Padding;
use crate::encrypt::encrypt_with_rs;
use crate::encrypt::has_pair_tags;
use crate::pair_roots::VirtualPair;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::dmeta::Uuid7;
use futures::StreamExt;
use futures::future::BoxFuture;
use iroh_blobs::HashAndFormat;
use iroh_blobs::store::virtual_blob::Provider as _;
use service_utils_rs::InlineRoute;

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("local provenance recovery must not read remote metadata") })
    }
}

async fn reopen(
    runtime: &sqlite_rs::SqliteRuntime,
    directory: &std::path::Path,
) -> Res<(Blobs, StopToken, Database)> {
    let sql = runtime.database(directory.join("local.sqlite"), &[], "test_migrations");
    let (blobs, stop) = boot(Config {
        storage: Storage::Filesystem(directory.join("store")),
        sql: sql.clone(),
        authority: Client::direct(Arc::new(UnusedAuthority)),
        namespace: Namespace {
            namespace_id: Uuid7::new(),
            capability: CapId::from_bytes([7; 16]),
        },
    })
    .await?;
    Ok((blobs, stop, sql))
}

async fn ciphertext(store: &Store, hash: Hash) -> Res<Vec<u8>> {
    let mut export = store
        .blobs()
        .export_bao(hash, bao_tree::ChunkRanges::all())
        .stream();
    let mut bytes = Vec::new();
    while let Some(item) = export.next().await {
        match item {
            bao_tree::io::mixed::EncodedItem::Leaf(leaf) => bytes.extend_from_slice(&leaf.data),
            bao_tree::io::mixed::EncodedItem::Error(error) => {
                utils_rs::prelude::eyre::bail!("virtual ciphertext export failed: {error:?}");
            }
            _ => {}
        }
    }
    Ok(bytes)
}

#[tokio::test]
async fn shutdown_recovers_pending_effects_after_service_handles_drop() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    for storage in [
        Storage::Memory,
        Storage::Filesystem(directory.path().join("store")),
    ] {
        let sql = runtime.ephemeral(&[])?;
        let (blobs, stop) = boot(Config {
            storage,
            sql: sql.clone(),
            authority: Client::direct(Arc::new(UnusedAuthority)),
            namespace: Namespace {
                namespace_id: Uuid7::new(),
                capability: CapId::from_bytes([7; 16]),
            },
        })
        .await?;
        let imported = blobs
            .service
            .import_stream(futures::stream::iter([Ok(Bytes::from_static(
                b"shutdown recovery",
            ))]))
            .await?;
        let id = imported.info.blob_id;
        let hash = Hash::from(id.to_bytes32());
        let ownership = blobs.service.ownership.clone();
        let store = blobs.service.store.clone();
        let service = Arc::downgrade(&blobs.service);
        sql.execute_batch(
            "CREATE TRIGGER fail_shutdown_effect_ack BEFORE DELETE ON big_blobs_byte_effects
             BEGIN SELECT RAISE(FAIL, 'injected shutdown effect ack failure'); END;",
        )
        .await?;
        let error = ownership.claim("mirror:shutdown", &[id]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected shutdown effect ack failure")
        );
        sql.execute("DROP TRIGGER fail_shutdown_effect_ack", ())
            .await?;
        store.tags().delete(format!("owned:{hash}")).await?;
        assert!(store.tags().get(format!("owned:{hash}")).await?.is_none());
        assert_eq!(
            sql.query("SELECT digest FROM big_blobs_byte_effects", ())
                .await?
                .all::<String>()?,
            vec![id.to_string()]
        );
        drop(blobs);
        assert!(service.upgrade().is_some());
        assert!(store.blobs().has(hash).await?);
        stop.stop().await;
        assert!(service.upgrade().is_none());
        assert!(
            sql.query("SELECT digest FROM big_blobs_byte_effects", ())
                .await?
                .all::<String>()?
                .is_empty()
        );
        assert!(store.blobs().has(hash).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_cancelled_publication_before_closing_store() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[])?;
    let (blobs, stop) = boot(Config {
        storage: Storage::Memory,
        sql: sql.clone(),
        authority: Client::direct(Arc::new(UnusedAuthority)),
        namespace: Namespace {
            namespace_id: Uuid7::new(),
            capability: CapId::from_bytes([7; 16]),
        },
    })
    .await?;
    let imported = blobs
        .service
        .import_stream(futures::stream::iter([Ok(Bytes::from_static(
            b"cancelled shutdown",
        ))]))
        .await?;
    let id = imported.info.blob_id;
    let hash = Hash::from(id.to_bytes32());
    let owner = imported.owner();
    let ownership = blobs.service.ownership.clone();
    let store = blobs.service.store.clone();
    let service = Arc::downgrade(&blobs.service);
    let (started, entered) = tokio::sync::oneshot::channel();
    let (resume, waiting) = tokio::sync::oneshot::channel();
    let mut publishing = Box::pin(ownership.publish(move |publication| {
        Box::pin(async move {
            started.send(()).unwrap();
            waiting.await?;
            publication.release(&owner).await
        })
    }));
    assert!(futures::poll!(publishing.as_mut()).is_pending());
    entered.await?;
    drop((publishing, blobs));
    let mut stopping = Box::pin(stop.stop());
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    assert!(service.upgrade().is_some());
    assert!(store.tags().get(format!("owned:{hash}")).await?.is_some());
    resume.send(()).unwrap();
    stopping.await;
    assert!(service.upgrade().is_none());
    assert!(
        sql.query("SELECT digest FROM big_blobs_byte_owners", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    assert!(
        sql.query("SELECT digest FROM big_blobs_byte_effects", ())
            .await?
            .all::<String>()?
            .is_empty()
    );
    assert!(store.blobs().has(hash).await.is_err());
    Ok(())
}

#[tokio::test]
async fn complete_virtual_outboard_is_unavailable_after_final_owner_release() -> Res<()> {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[])?;
    let (blobs, stop) = boot(Config {
        storage: Storage::Memory,
        sql: sql.clone(),
        authority: Client::direct(Arc::new(UnusedAuthority)),
        namespace: Namespace {
            namespace_id: Uuid7::new(),
            capability: CapId::from_bytes([7; 16]),
        },
    })
    .await?;
    let service = &blobs.service;
    let (cipher, plain) = crate::encrypt::add_encrypted(
        &service.store,
        &service.provider,
        &service.roots,
        &MasterKey::random(),
        EncodingParams::DEFAULT,
        b"virtual availability is not outboard completeness",
    )
    .await?;
    let id = BlobId::new(*cipher.as_bytes());
    let plain_id = BlobId::new(*plain.as_bytes());
    service.ownership.claim("document:virtual", &[id]).await?;
    service.ownership.resolve_pair(&id).await?;
    service.ownership.claim("inventory:virtual", &[id]).await?;
    let info = service.stat(&id).await?.unwrap();
    service.ownership.release("document:virtual").await?;
    assert!(service.ownership.contains(&id).await?);
    assert!(service.ownership.contains(&plain_id).await?);
    assert_eq!(service.stat(&id).await?, Some(info.clone()));
    service.ownership.release("inventory:virtual").await?;
    assert!(!service.ownership.contains(&id).await?);
    assert!(!service.ownership.contains(&plain_id).await?);
    assert!(matches!(
        service.store.blobs().status(cipher).await?,
        BlobStatus::Complete { size } if size == info.length_octets
    ));
    assert!(service.store.blobs().sync_reader(cipher).await?.is_none());
    assert!(service.provider.reader_for(&cipher).is_none());
    assert!(!has_pair_tags(&service.store, cipher).await?);
    assert!(service.roots.virtual_pairs().await?.is_empty());
    assert!(service.roots.unresolved().await?.is_empty());
    assert!(service.stat(&id).await?.is_none());
    assert!(service.announce(id).await.is_err());
    assert!(service.stat(&plain_id).await?.is_some());
    service.ownership.recover().await?;
    assert!(service.stat(&id).await?.is_none());
    assert!(service.provider.reader_for(&cipher).is_none());
    stop.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filesystem_provenance_restores_identical_ciphertext_until_last_owner_releases() -> Res<()>
{
    for padding in [Padding::Minimal, Padding::Record] {
        filesystem_provenance_restart_case(padding, false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filesystem_restart_installs_committed_provenance_before_final_release() -> Res<()> {
    for padding in [Padding::Minimal, Padding::Record] {
        filesystem_provenance_restart_case(padding, true).await?;
    }
    Ok(())
}

async fn filesystem_provenance_restart_case(
    padding: Padding,
    interrupted_install: bool,
) -> Res<()> {
    let directory = tempfile::tempdir()?;
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (blobs, stop, sql) = reopen(&runtime, directory.path()).await?;
    let plain = vec![0x5A; 3 * 4096 + 123];
    let plaintext = Hash::new(&plain);
    let tag = blobs
        .service
        .store
        .blobs()
        .add_bytes(plain.clone())
        .temp_tag()
        .await?;
    let key = MasterKey::random();
    let encoding = EncodingParams::new(4096, padding)?;
    let expected = encrypt_with_rs(&key, &plain, encoding.record_size, padding);
    let cipher = Hash::new(&expected);
    let id = BlobId::new(*cipher.as_bytes());
    let plain_id = BlobId::new(*plaintext.as_bytes());
    let source = serde_json::json!({
        "kind": "accepted-cipher-blob",
        "document": "fixture-document",
        "datum": "fixture-cipher",
        "heads": ["accepted-head"],
        "inventory": "selected-inventory",
    })
    .to_string();
    let pair = VirtualPair {
        cipher,
        plaintext,
        key,
        encoding,
        source: source.clone(),
        claimed_by: Some(("fixture-document".into(), "main".into())),
    };
    blobs
        .service
        .ownership
        .claim("document:plaintext", &[plain_id])
        .await?;
    let (blobs, stop, sql) = if interrupted_install {
        blobs.service.roots.record_virtual(&pair).await?;
        assert!(!blobs.service.store.blobs().has(cipher).await?);
        assert!(blobs.service.provider.reader_for(&cipher).is_none());
        assert!(!has_pair_tags(&blobs.service.store, cipher).await?);
        assert_eq!(blobs.service.roots.unresolved().await?.len(), 1);
        drop(tag);
        stop.stop().await;
        drop((blobs, sql));
        let (blobs, stop, sql) = reopen(&runtime, directory.path()).await?;
        assert!(blobs.service.store.blobs().has(cipher).await?);
        assert!(blobs.service.provider.reader_for(&cipher).is_some());
        assert_eq!(ciphertext(&blobs.service.store, cipher).await?, expected);
        assert_eq!(blobs.service.roots.unresolved().await?.len(), 1);
        (blobs, stop, sql)
    } else {
        blobs
            .service
            .provider
            .register_local_pair(&blobs.service.store, &blobs.service.roots, pair)
            .await?;
        drop(tag);
        (blobs, stop, sql)
    };
    blobs.service.ownership.claim("mirror:first", &[id]).await?;
    blobs
        .service
        .ownership
        .claim("mirror:second", &[id])
        .await?;
    blobs.service.ownership.resolve_pair(&id).await?;
    assert!(blobs.service.roots.unresolved().await?.is_empty());
    assert!(blobs.service.store.sync_reader(cipher).await?.is_none());
    assert_eq!(ciphertext(&blobs.service.store, cipher).await?, expected);
    assert_eq!(
        sql.query("SELECT source FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<String>()?,
        source
    );
    stop.stop().await;
    drop((blobs, sql));

    let (blobs, stop, sql) = reopen(&runtime, directory.path()).await?;
    assert!(blobs.service.provider.reader_for(&cipher).is_some());
    assert!(blobs.service.roots.unresolved().await?.is_empty());
    assert!(blobs.service.store.sync_reader(cipher).await?.is_none());
    assert_eq!(ciphertext(&blobs.service.store, cipher).await?, expected);
    assert_eq!(
        sql.query("SELECT source FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<String>()?,
        source
    );
    blobs.service.ownership.release("mirror:first").await?;
    assert_eq!(ciphertext(&blobs.service.store, cipher).await?, expected);
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<i64>()?,
        1
    );
    blobs.service.ownership.release("mirror:second").await?;
    assert!(blobs.service.provider.reader_for(&cipher).is_none());
    assert!(blobs.service.stat(&id).await?.is_none());
    assert!(!has_pair_tags(&blobs.service.store, cipher).await?);
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<i64>()?,
        0
    );
    assert!(blobs.service.ownership.contains(&plain_id).await?);
    assert_eq!(
        blobs
            .service
            .store
            .blobs()
            .get_bytes(plaintext)
            .await?
            .as_ref(),
        plain
    );
    blobs
        .service
        .store
        .tags()
        .set(format!("ct:{cipher}"), HashAndFormat::raw(cipher))
        .await?;
    blobs
        .service
        .store
        .tags()
        .set(format!("pt:{cipher}"), HashAndFormat::raw(plaintext))
        .await?;
    stop.stop().await;
    drop((blobs, sql));

    let (blobs, stop, sql) = reopen(&runtime, directory.path()).await?;
    assert!(blobs.service.provider.reader_for(&cipher).is_none());
    assert!(ciphertext(&blobs.service.store, cipher).await.is_err());
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<i64>()?,
        0
    );
    assert_eq!(
        blobs
            .service
            .store
            .blobs()
            .get_bytes(plaintext)
            .await?
            .as_ref(),
        plain
    );
    blobs
        .service
        .ownership
        .release("document:plaintext")
        .await?;
    stop.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filesystem_boot_discards_provenance_after_committed_last_release_before_effects() -> Res<()>
{
    let directory = tempfile::tempdir()?;
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let (blobs, stop, sql) = reopen(&runtime, directory.path()).await?;
    let tag = blobs
        .service
        .store
        .blobs()
        .add_bytes(b"release pending on restart".as_slice())
        .temp_tag()
        .await?;
    let plain = tag.hash();
    let cipher = blobs
        .service
        .provider
        .install(
            &blobs.service.store,
            &blobs.service.roots,
            &MasterKey::random(),
            plain,
            EncodingParams::DEFAULT,
        )
        .await?;
    let id = BlobId::new(*cipher.as_bytes());
    blobs
        .service
        .ownership
        .claim("document:retention:restart", &[id])
        .await?;
    blobs.service.ownership.resolve_pair(&id).await?;
    drop(tag);
    stop.stop().await;
    drop(blobs);
    let tx = sql.begin_write().await?;
    tx.execute(
        "INSERT INTO big_blobs_byte_effects (digest) VALUES (?)",
        (id.to_string(),),
    )
    .await?;
    tx.execute(
        "DELETE FROM big_blobs_byte_owners WHERE digest = ?",
        (id.to_string(),),
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<i64>()?,
        1
    );
    sql.execute_batch(
        "CREATE TRIGGER require_provenance_drain_before_ownership_recovery
         BEFORE DELETE ON big_blobs_byte_dependencies
         WHEN EXISTS (SELECT 1 FROM big_blobs_virtual_pairs WHERE digest = OLD.digest)
         BEGIN
             SELECT RAISE(ABORT, 'ownership recovery preceded provenance drain');
         END;",
    )
    .await?;
    drop(sql);

    let (blobs, stop, sql) = reopen(&runtime, directory.path()).await?;
    assert!(blobs.service.provider.reader_for(&cipher).is_none());
    assert!(!has_pair_tags(&blobs.service.store, cipher).await?);
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_virtual_pairs", ())
            .await?
            .one::<i64>()?,
        0
    );
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_byte_dependencies", ())
            .await?
            .one::<i64>()?,
        0
    );
    assert_eq!(
        sql.query("SELECT count(*) FROM big_blobs_byte_effects", ())
            .await?
            .one::<i64>()?,
        0
    );
    stop.stop().await;
    Ok(())
}
