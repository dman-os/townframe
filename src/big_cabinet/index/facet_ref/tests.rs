use super::*;
use automerge::transaction::Transactable;
use big_cabinet_core::DocId;
use big_cabinet_core::FacetKey;

fn route(doc: u8, name: &str) -> FacetRoute {
    FacetRoute {
        doc_id: DocId::new([doc; 32]),
        facet_key: FacetKey::from_text("org.example.note", name).unwrap(),
    }
}

fn edge(origin: &FacetRoute, path: &str, target: &FacetRoute, heads: &[u8]) -> FacetRefEdge {
    FacetRefEdge {
        origin: origin.clone(),
        path: path.to_owned(),
        target: target.clone(),
        ref_heads: heads.iter().map(|byte| ChangeHash([*byte; 32])).collect(),
    }
}

#[cfg(test)]
async fn progress(store: &FacetRefStore, tag: &str) -> Option<u64> {
    store.cursors().await.unwrap().get(tag).copied()
}

#[tokio::test]
async fn settlement_replaces_edges_serves_both_directions_and_never_regresses() {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = FacetRefStore::new(sql.clone()).await.unwrap();
    let mut parked = BTreeMap::new();
    let origin = route(1, "main");
    let first = route(2, "first");
    let second = route(3, "second");
    let third = route(4, "third");
    store
        .settle(
            &origin,
            &[
                edge(&origin, "/link", &first, &[7; 2]),
                edge(&origin, "/nested/inner", &second, &[]),
            ],
            1,
            &mut parked,
        )
        .await
        .unwrap();
    assert_eq!(progress(&store, "org.example.note").await, Some(1));
    assert_eq!(
        store
            .query(FacetRefQuery::Origin(origin.clone()))
            .await
            .unwrap(),
        vec![
            edge(&origin, "/link", &first, &[7; 2]),
            edge(&origin, "/nested/inner", &second, &[]),
        ]
    );
    assert_eq!(
        store
            .query(FacetRefQuery::Target(first.clone()))
            .await
            .unwrap(),
        vec![edge(&origin, "/link", &first, &[7; 2])]
    );
    assert_eq!(
        store
            .query(FacetRefQuery::OriginDoc(origin.doc_id.clone()))
            .await
            .unwrap(),
        vec![
            edge(&origin, "/link", &first, &[7; 2]),
            edge(&origin, "/nested/inner", &second, &[]),
        ]
    );
    assert_eq!(
        store
            .query(FacetRefQuery::TargetDoc(first.doc_id.clone()))
            .await
            .unwrap(),
        vec![edge(&origin, "/link", &first, &[7; 2])]
    );
    store
        .settle(
            &origin,
            &[edge(&origin, "/link", &third, &[])],
            2,
            &mut parked,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .query(FacetRefQuery::Origin(origin.clone()))
            .await
            .unwrap(),
        vec![edge(&origin, "/link", &third, &[])]
    );
    assert!(
        store
            .query(FacetRefQuery::Target(first.clone()))
            .await
            .unwrap()
            .is_empty()
    );
    // A replayed or late settlement re-replaces idempotently and the cursor never moves back.
    store.settle(&origin, &[], 1, &mut parked).await.unwrap();
    assert!(
        store
            .query(FacetRefQuery::Origin(origin.clone()))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(progress(&store, "org.example.note").await, Some(2));
    let reopened = FacetRefStore::new(sql).await.unwrap();
    assert_eq!(progress(&reopened, "org.example.note").await, Some(2));
}

#[tokio::test]
async fn failed_settlement_preserves_edges_cursor_and_parked_replay_without_notification() {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = FacetRefStore::new(sql.clone()).await.unwrap();
    let origin = route(1, "main");
    let original = vec![edge(&origin, "/link", &route(2, "first"), &[7])];
    let replacement = vec![edge(&origin, "/link", &route(3, "second"), &[])];
    let mut parked = BTreeMap::new();
    store
        .settle(&origin, &original, 3, &mut parked)
        .await
        .unwrap();
    store
        .park(
            origin.clone(),
            present(vec![ChangeHash([9; 32])]),
            5,
            &mut parked,
        )
        .await
        .unwrap();
    let expected_parked = parked.clone();
    let mut settled = store.subscribe_settled();
    sql.execute_batch("CREATE TRIGGER reject_facet_ref BEFORE INSERT ON big_cabinet_facet_ref_progress BEGIN SELECT RAISE(ABORT, 'injected'); END")
        .await.unwrap();
    assert!(
        store
            .settle(&origin, &replacement, 6, &mut parked)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .query(FacetRefQuery::Origin(origin.clone()))
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        store.cursors().await.unwrap(),
        BTreeMap::from([("org.example.note".into(), 3)])
    );
    assert_eq!(store.parked().await.unwrap(), expected_parked);
    assert_eq!(parked, expected_parked);
    assert!(!settled.has_changed().unwrap());
    sql.execute_batch("DROP TRIGGER reject_facet_ref")
        .await
        .unwrap();
    store
        .settle(&origin, &replacement, 6, &mut parked)
        .await
        .unwrap();
    settled.changed().await.unwrap();
    assert_eq!(
        store.query(FacetRefQuery::Origin(origin)).await.unwrap(),
        replacement
    );
    assert_eq!(
        store.cursors().await.unwrap(),
        BTreeMap::from([("org.example.note".into(), 6)])
    );
    assert!(store.parked().await.unwrap().is_empty());
    assert!(parked.is_empty());
}

/// A document carrying one unsigned branch facet: a reference-declaring tag whose value the
/// gate refuses, so it settles with no edges and exercises the route plumbing alone.
fn branch_document() -> (Automerge, FacetKey) {
    let facet_key = FacetKey::from_text(big_cabinet_core::dmeta::BRANCH_FACET_TAG, "main").unwrap();
    let mut document = Automerge::new();
    let mut tx = document.transaction();
    let facets = tx
        .put_object(automerge::ROOT, "facets", ObjType::Map)
        .unwrap();
    let facet = tx
        .put_object(&facets, facet_key.to_string(), ObjType::Map)
        .unwrap();
    tx.put(&facet, "from", "db+facet:/self/org.example.note/main")
        .unwrap();
    tx.commit();
    (document, facet_key)
}

fn present(heads: Vec<ChangeHash>) -> FacetMembership {
    FacetMembership::Present {
        doc_heads: heads,
        facet_heads: Vec::new(),
        clock: 0,
    }
}

async fn close(repo: SharedBigRepo, stop: big_repo::BigRepoStopToken, sql: Database) {
    drop(repo);
    stop.stop().await.unwrap();
    drop(sql);
}

#[tokio::test]
async fn a_newer_revision_releases_or_outranks_a_parked_route_and_parking_survives_reopen() {
    let schemas = SchemaRepo::open(Vec::new()).unwrap();
    let (document, facet_key) = branch_document();
    let (_directory, repo, stop, doc_id) =
        crate::index::test_support::reopen_document(document).await;
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = FacetRefStore::new(sql.clone()).await.unwrap();
    let target = route(2, "other");
    let route = FacetRoute { doc_id, facet_key };
    let unknown = present(vec![ChangeHash([9; 32])]);
    let tag = big_cabinet_core::dmeta::BRANCH_FACET_TAG;
    let mut parked = BTreeMap::new();

    // Revision 1 parks (the heads are not local); revision 2 settles the route and releases
    // the parked entry, so no later materialization can retry revision 1.
    store
        .process(
            &repo,
            &schemas,
            route.clone(),
            Some(unknown.clone()),
            1,
            &mut parked,
        )
        .await
        .unwrap();
    assert_eq!(parked[&route].revision, 1);
    assert_eq!(store.parked().await.unwrap(), parked);
    store
        .settle(
            &route,
            &[edge(&route, "/link", &target, &[])],
            2,
            &mut parked,
        )
        .await
        .unwrap();
    assert!(parked.is_empty());
    assert!(store.parked().await.unwrap().is_empty());
    assert_eq!(progress(&store, tag).await, Some(2));

    // Revision 3 parks; a replay of revision 2 (a removal) is outranked by it: the edges
    // revision 2 settled stay and the parked entry is untouched.
    store
        .process(
            &repo,
            &schemas,
            route.clone(),
            Some(unknown.clone()),
            3,
            &mut parked,
        )
        .await
        .unwrap();
    store
        .process(
            &repo,
            &schemas,
            route.clone(),
            Some(FacetMembership::Removed {
                doc_heads: None,
                facet_heads: Vec::new(),
                clock: 0,
            }),
            2,
            &mut parked,
        )
        .await
        .unwrap();
    assert_eq!(
        parked,
        BTreeMap::from([(
            route.clone(),
            Parked {
                membership: unknown.clone(),
                revision: 3,
            }
        )])
    );
    assert_eq!(
        store
            .query(FacetRefQuery::Origin(route.clone()))
            .await
            .unwrap(),
        vec![edge(&route, "/link", &target, &[])]
    );
    assert_eq!(progress(&store, tag).await, Some(2));

    // A tombstone for the route at revision 4 clears both the edges and the parked entry.
    store
        .process(&repo, &schemas, route.clone(), None, 4, &mut parked)
        .await
        .unwrap();
    assert!(parked.is_empty());
    assert!(
        store
            .query(FacetRefQuery::Origin(route.clone()))
            .await
            .unwrap()
            .is_empty()
    );

    // A parked route outlives the store: a reopened store sees it and retries it the way the
    // run loop does at boot, and it stays parked while the heads are still not local.
    store
        .process(
            &repo,
            &schemas,
            route.clone(),
            Some(unknown.clone()),
            5,
            &mut parked,
        )
        .await
        .unwrap();
    drop(store);
    let reopened = FacetRefStore::new(sql.clone()).await.unwrap();
    let mut reloaded = reopened.parked().await.unwrap();
    assert_eq!(reloaded, parked);
    for (
        route,
        Parked {
            membership,
            revision,
        },
    ) in std::mem::take(&mut reloaded)
    {
        reopened
            .process(
                &repo,
                &schemas,
                route,
                Some(membership),
                revision,
                &mut reloaded,
            )
            .await
            .unwrap();
    }
    assert_eq!(reloaded, parked);
    assert_eq!(progress(&reopened, tag).await, Some(4));
    drop(reopened);
    close(repo, stop, sql).await;
}

#[tokio::test]
async fn reference_lookup_does_not_acquire_sync_demand() {
    let schemas = SchemaRepo::open(Vec::new()).unwrap();
    let (document, facet_key) = branch_document();
    let heads = document.get_heads();
    let (_directory, repo, stop, doc_id) =
        crate::index::test_support::reopen_document(document).await;
    let mut demand = repo.sync_demand();
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = FacetRefStore::new(sql.clone()).await.unwrap();
    let route = FacetRoute {
        doc_id: doc_id.clone(),
        facet_key,
    };
    let mut parked = BTreeMap::new();
    store
        .process(
            &repo,
            &schemas,
            route.clone(),
            Some(FacetMembership::Present {
                doc_heads: heads,
                facet_heads: Vec::new(),
                clock: 0,
            }),
            1,
            &mut parked,
        )
        .await
        .unwrap();
    assert!(parked.is_empty());
    assert_eq!(
        progress(&store, big_cabinet_core::dmeta::BRANCH_FACET_TAG).await,
        Some(1)
    );
    assert!(
        store
            .query(FacetRefQuery::Origin(route.clone()))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(demand.documents().is_empty());
    let pending = FacetMembership::Present {
        doc_heads: vec![ChangeHash([9; 32])],
        facet_heads: Vec::new(),
        clock: 0,
    };
    store
        .process(
            &repo,
            &schemas,
            route.clone(),
            Some(pending.clone()),
            2,
            &mut parked,
        )
        .await
        .unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[&route].membership, pending);
    assert_eq!(parked[&route].revision, 2);
    assert!(demand.documents().is_empty());
    let mut changed = Box::pin(demand.changed());
    assert!(futures::poll!(&mut changed).is_pending());
    drop(changed);
    let caller = repo
        .get_doc(&doc_id)
        .await
        .unwrap()
        .into_ready(doc_id.clone())
        .unwrap();
    assert_eq!(
        *demand.documents(),
        std::collections::HashSet::from([doc_id])
    );
    drop(caller);
    drop(store);
    close(repo, stop, sql).await;
}

#[test]
fn an_unsigned_value_publishes_no_edges() {
    let schema = CompiledSchema::compile(serde_json::json!({
        "type": "object",
        "properties": {"link": {"type": "string", "x-automerge": "reference"}},
    }))
    .unwrap();
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let facets = tx
        .put_object(automerge::ROOT, "facets", ObjType::Map)
        .unwrap();
    let facet = tx
        .put_object(&facets, "org.example.note/main", ObjType::Map)
        .unwrap();
    tx.put(&facet, "link", "db+facet:/self/org.example.note/other")
        .unwrap();
    tx.commit();
    let heads = doc.get_heads();
    // The gate rejects a value no signature covers; nothing it states becomes an edge.
    assert_eq!(
        edges_from(&mut doc, &route(1, "main"), &heads, &schema).unwrap(),
        Some(Vec::new())
    );
    // Heads the document does not have park the route instead.
    assert_eq!(
        edges_from(&mut doc, &route(1, "main"), &[ChangeHash([9; 32])], &schema).unwrap(),
        None
    );
}
