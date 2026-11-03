use super::*;

#[tokio::test]
async fn sqlite_leaf_source_revisions() -> Res<()> {
    let store = test_store("source-revisions").await?;
    host_contract::assert_leaf_source_revision_contract(&store).await
}

#[tokio::test]
async fn source_revisions_remain_ordered_after_sqlite_reopen() -> Res<()> {
    use big_sync_core::FingerprintSeed;
    use big_sync_core::rpc::LeafBucketRequest;
    use big_sync_core::rpc::LeafBucketsRequest;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source-revisions.sqlite");
    let part = test_part_id(11);
    let obj = test_obj_id(12);
    let scope = "source-revision-reopen";
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.database(&path, &[], "migrations");
    let store = SqlitePartStore::new(sql, scope, BuckId::MAX_LEVEL).await?;
    let subscriber = crate::part_store::contract::grant_bucket_read(&store, [part.clone()]).await?;
    store
        .set_obj_payload(obj.clone(), serde_json::json!(1))
        .await?;
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;
    let before = store.latest_revision().await?;
    let request = LeafBucketsRequest {
        part_id: part.clone(),
        since: 0,
        buckets: vec![LeafBucketRequest {
            buck_id: BuckId::ROOT,
            after: None,
        }],
        seed: FingerprintSeed::new(5, 6),
        limit_hint: 16,
    };
    assert_eq!(
        store
            .leaf_buckets(request.clone(), subscriber.clone())
            .await??
            .bucks[&BuckId::ROOT]
            .entries[0]
            .source_revision,
        before
    );
    drop(store);
    drop(runtime);
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    let sql = runtime.database(path, &[], "migrations");
    let store = SqlitePartStore::new(sql, scope, BuckId::MAX_LEVEL).await?;
    assert_eq!(store.latest_revision().await?, before);
    assert_eq!(
        store
            .leaf_buckets(request.clone(), subscriber.clone())
            .await??
            .bucks[&BuckId::ROOT]
            .entries[0]
            .source_revision,
        before
    );
    store
        .remove_obj_from_part(obj.clone(), part.clone())
        .await?;
    let removed = store.latest_revision().await?;
    assert!(removed > before);
    let leaf = store
        .leaf_buckets(request.clone(), subscriber.clone())
        .await??;
    let entry = &leaf.bucks[&BuckId::ROOT].entries[0];
    assert!(entry.dead);
    assert_eq!(entry.source_revision, removed);
    store
        .add_obj_to_parts(obj.clone(), vec![part.clone()])
        .await?;
    let readded = store.latest_revision().await?;
    assert!(readded > removed);
    let leaf = store.leaf_buckets(request, subscriber).await??;
    let entry = &leaf.bucks[&BuckId::ROOT].entries[0];
    assert!(!entry.dead);
    assert_eq!(entry.source_revision, readded);
    let events = store
        .list_events([part.clone()].into(), before, 16)
        .await??;
    assert!(events[&part].events.iter().any(|event| matches!(event, PartEvent::Changed(event) if event.obj_id == obj && event.cursor == readded)));
    Ok(())
}

/// The leaf page's byte budget, which the store applies beside the entry hint.
#[tokio::test(flavor = "multi_thread")]
async fn sqlite_leaf_page_byte_budget_contract() -> Res<()> {
    let store = test_store("big-sync-sqlite-test://leaf-byte-budget").await?;
    host_contract::assert_leaf_page_byte_budget_contract(&store).await
}
