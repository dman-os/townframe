use super::*;
use automerge::transaction::Transactable;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use service_utils_rs::Client;

#[tokio::test]
async fn runtime_drains_257_dirty_pairs_from_one_coalesced_notification() -> Res<()> {
    let runtime = Arc::new(sqlite_rs::SqliteRuntime::new(Default::default()));
    let (repo, repo_stop) = big_repo::BigRepo::boot(
        Arc::clone(&runtime),
        big_repo::Config {
            node_identity_seed: [124; 32],
            storage: big_repo::StorageConfig::Memory,
            scope_key: "admission-runtime-backlog".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        },
    )
    .await?;
    let sql = runtime.ephemeral(&[])?;
    let (blobs, blobs_stop) = crate::boot(crate::Config {
        storage: crate::Storage::Memory,
        sql: sql.clone(),
        authority: Client::direct(Arc::new(UnusedAuthority)),
        namespace: Namespace {
            namespace_id: Uuid7::new(),
            capability: CapId::from_bytes([125; 16]),
        },
    })
    .await?;
    let mut document = automerge::Automerge::new();
    let mut transaction = document.transaction();
    transaction.put(automerge::ROOT, "fixture", true)?;
    transaction.commit();
    let inventory = repo.create_doc(document).await?.document_id();
    State::new(sql.clone()).await?.register(&inventory).await?;
    let parts: big_repo::SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
    let queue = crate::work_queue::WorkQueue::new(sql.clone()).await?;
    let (mirrors, mirrors_stop) =
        InventoryMirrors::boot(sql.clone(), blobs.service, Arc::clone(&parts), repo, queue).await?;
    let mut progress = mirrors.progress.subscribe();
    progress.wait_for(|revision| *revision >= 2).await?;
    assert_eq!(*progress.borrow(), 2);
    assert!(mirrors.admission.dirty().await?.is_empty());

    let pins: BTreeMap<_, _> = (0_u64..257)
        .map(|index| {
            (
                BlobId::new(*blake3::hash(&index.to_le_bytes()).as_bytes()),
                42,
            )
        })
        .collect();
    let part = crate::workers::inventory_part(&inventory);
    parts.ensure_part(part.clone()).await?;
    for digest in pins.keys() {
        parts
            .set_obj_payload((*digest).into(), serde_json::json!({"lengthOctets": 42}))
            .await?;
        parts
            .add_obj_to_parts((*digest).into(), vec![part.clone()])
            .await?;
    }
    assert_eq!(parts.member_count(part.clone()).await?, 257);
    mirrors.admission.pins(&inventory, 1, &pins, true).await?;
    progress.wait_for(|revision| *revision >= 5).await?;
    assert_eq!(*progress.borrow(), 5);
    assert!(mirrors.admission.dirty().await?.is_empty());
    assert_eq!(parts.member_count(part).await?, 0);
    assert_eq!(
        sql.query(
            "SELECT COUNT(*)
                  , SUM(dirty)
                  , MIN(generation)
                  , MAX(generation)
                  , SUM(bytes)
                  , COUNT(attempt)
               FROM big_blobs_admission_pairs
              WHERE inventory_id = ?",
            (inventory.to_string(),),
        )
        .await?
        .all::<(u64, u64, u64, u64, u64, u64)>()?,
        vec![(257, 0, 1, 1, 0, 0)]
    );
    for digest in pins.keys() {
        assert!(parts.obj_parts((*digest).into()).await?.is_empty());
    }
    mirrors_stop.stop().await;
    blobs_stop.stop().await;
    repo_stop.stop().await?;
    Ok(())
}
