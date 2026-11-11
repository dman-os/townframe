use super::*;
use big_cabinet_core::drawer::GroupKey;
use sqlite_rs::SqliteRuntime;

#[tokio::test]
async fn roster_batches_rollback_together_and_notify_only_after_commit() {
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[]).unwrap();
    let state = State::open(sql.clone()).await.unwrap();
    let [drawer, old_doc, old_child] = [240_u8, 241, 242].map(|value| DocId::new([value; 32]));
    let before_docs = BTreeMap::from([(old_doc.clone(), Access::Read)]);
    let before_children = BTreeMap::from([(old_child.clone(), Access::Write)]);
    let before_meta = BTreeMap::from([(
        MetaRoot::Public,
        BTreeMap::from([("timeline/main".to_owned(), DocId::new([243; 32]))]),
    )]);
    state
        .replace_roster(&drawer, &before_docs, &before_children, &before_meta)
        .await
        .unwrap();
    let mut changes = state.subscribe_changes();
    let snapshot = sql.begin_read().await.unwrap();
    let docs: BTreeMap<_, _> = (1_u16..=513)
        .map(|value| {
            let mut bytes = [0; 32];
            bytes[..2].copy_from_slice(&value.to_be_bytes());
            (DocId::new(bytes), Access::Admin)
        })
        .collect();
    let mut children: BTreeMap<_, _> = docs
        .keys()
        .take(257)
        .cloned()
        .map(|id| (id, Access::Read))
        .collect();
    *children.last_entry().unwrap().get_mut() = Access::Relay;
    let meta = BTreeMap::from([
        (
            MetaRoot::Public,
            BTreeMap::from([
                (
                    "timeline/main".to_owned(),
                    docs.keys().next().unwrap().clone(),
                ),
                ("index/main".to_owned(), DocId::new([244; 32])),
            ]),
        ),
        (
            MetaRoot::Sensitive,
            BTreeMap::from([("notes/private".to_owned(), DocId::new([245; 32]))]),
        ),
    ]);
    sql.execute_batch(
        "CREATE TRIGGER fail_roster_child BEFORE INSERT ON big_cabinet_drawer_children
         WHEN NEW.access = 'Relay'
         BEGIN SELECT RAISE(ABORT, 'injected roster failure'); END;",
    )
    .await
    .unwrap();
    let error = state
        .replace_roster(&drawer, &docs, &children, &meta)
        .await
        .unwrap_err();
    assert!(
        matches!(error, DrawersError::Storage(ref message) if message.contains("injected roster failure"))
    );
    assert!(!changes.has_changed().unwrap());
    assert_eq!(state.docs_of(&drawer).await.unwrap(), before_docs);
    assert_eq!(state.children_of(&drawer).await.unwrap(), before_children);
    assert_eq!(state.meta_docs_of(&drawer).await.unwrap(), before_meta);
    sql.execute_batch("DROP TRIGGER fail_roster_child")
        .await
        .unwrap();
    state
        .replace_roster(&drawer, &docs, &children, &meta)
        .await
        .unwrap();
    changes.changed().await.unwrap();
    assert_eq!(*changes.borrow_and_update(), 2);
    assert_eq!(state.docs_of(&drawer).await.unwrap(), docs);
    assert_eq!(state.children_of(&drawer).await.unwrap(), children);
    assert_eq!(state.meta_docs_of(&drawer).await.unwrap(), meta);
    assert_eq!(
        sqlite::doc_entry(&snapshot, drawer.to_string(), old_doc.to_string())
            .await
            .unwrap(),
        Some(Access::Read.as_str().to_owned()),
    );
    assert_eq!(
        sqlite::child_entry(&snapshot, drawer.to_string(), old_child.to_string())
            .await
            .unwrap(),
        Some(Access::Write.as_str().to_owned()),
    );
    snapshot.rollback().await.unwrap();
    state
        .replace_roster(&drawer, &docs, &children, &meta)
        .await
        .unwrap();
    assert!(!changes.has_changed().unwrap());
    state
        .replace_roster(
            &drawer,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(changes.has_changed().unwrap());
    assert!(state.docs_of(&drawer).await.unwrap().is_empty());
    assert!(state.children_of(&drawer).await.unwrap().is_empty());
    assert!(state.meta_docs_of(&drawer).await.unwrap().is_empty());
}

#[tokio::test]
async fn migrations_reopen_existing_projection_and_local_drawers() {
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[]).unwrap();
    sql.execute_batch(sqlite::MIGRATIONS[0].sql).await.unwrap();
    let [
        present_id,
        originated_id,
        known_drawers_id,
        config_id,
        announced_id,
    ] = [10_u8, 11, 12, 13, 14].map(|value| DocId::new([value; 32]));
    let local = LocalDrawers {
        present_id,
        originated_id,
        known_drawers_id,
        config_id,
    };
    let descriptor: Drawer = serde_json::from_value(serde_json::json!({
        "$type": "org.example.daybook.drawer",
        "layout": {"$type": "org.example.future.layout", "opaque": [1, 2, 3]}
    }))
    .unwrap();
    sqlite::put_drawer(
        &sql,
        local.present_id.to_string(),
        serde_json::to_string(&descriptor).unwrap(),
    )
    .await
    .unwrap();
    let state = State::open(sql.clone()).await.unwrap();
    assert_eq!(
        state
            .drawer(&local.present_id)
            .await
            .unwrap()
            .unwrap()
            .descriptor,
        descriptor
    );
    state.set_local(local.clone()).await.unwrap();
    state
        .put_child(&local.present_id, &local.originated_id, Access::Read)
        .await
        .unwrap();
    drop(state);
    let reopened = State::open(sql).await.unwrap();
    assert_eq!(reopened.local(), local);
    assert_eq!(
        reopened.drawers().await.unwrap(),
        vec![DrawerRecord {
            drawer_id: local.present_id.clone(),
            descriptor,
        }]
    );
    let mut expected = vec![local.originated_id.clone(), announced_id.clone()];
    expected.sort();
    assert_eq!(
        reopened
            .unprojected(&BTreeSet::from([local.present_id.clone(), announced_id]))
            .await
            .unwrap(),
        expected,
    );
}

#[tokio::test]
async fn document_reach_uses_reverse_indexes() {
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[]).unwrap();
    let _state = State::open(sql.clone()).await.unwrap();
    let plan = sql
        .query(
            &format!("EXPLAIN QUERY PLAN {}", sqlite::ANCESTORS_SQL),
            (true, DocId::new([42; 32]).to_string()),
        )
        .await
        .unwrap();
    let details: Vec<_> = plan
        .rows
        .iter()
        .map(|row| row.get::<String>("detail").unwrap())
        .collect();
    for index in [
        "big_cabinet_drawers_by_sensitive_root",
        "big_cabinet_drawer_meta_docs_by_doc",
        "big_cabinet_drawer_docs_by_doc",
        "big_cabinet_drawer_children_by_child",
    ] {
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("SEARCH") && detail.contains(index)),
            "missing {index}: {details:?}"
        );
    }
}

#[tokio::test]
async fn metadata_reach_preserves_audiences_through_child_attenuation() {
    let runtime = SqliteRuntime::new(Default::default());
    let state = State::open(runtime.ephemeral(&[]).unwrap()).await.unwrap();
    let [
        parent,
        child,
        leaf,
        sensitive,
        public_doc,
        sensitive_doc,
        content,
    ] = [20_u8, 21, 22, 23, 24, 25, 26].map(|value| DocId::new([value; 32]));
    for (index, drawer) in [&parent, &child, &leaf].into_iter().enumerate() {
        state
            .put_drawer(
                drawer,
                &Drawer::single_group(
                    GroupKey([8; 32]),
                    GroupKey([9; 32]),
                    GroupKey([10; 32]),
                    if drawer == &leaf {
                        sensitive.clone()
                    } else {
                        DocId::new([30 + index as u8; 32])
                    },
                ),
            )
            .await
            .unwrap();
    }
    state
        .replace_roster(
            &leaf,
            &BTreeMap::from([(content.clone(), Access::Write)]),
            &BTreeMap::from([(parent.clone(), Access::Admin)]),
            &BTreeMap::from([
                (
                    MetaRoot::Public,
                    BTreeMap::from([("notes/main".into(), public_doc.clone())]),
                ),
                (
                    MetaRoot::Sensitive,
                    BTreeMap::from([("notes/main".into(), sensitive_doc.clone())]),
                ),
            ]),
        )
        .await
        .unwrap();
    for first in Access::ALL {
        state.put_child(&parent, &child, first).await.unwrap();
        for second in Access::ALL {
            state.put_child(&child, &leaf, second).await.unwrap();
            for (doc, audience) in [
                (&leaf, "public"),
                (&public_doc, "public"),
                (&sensitive, "sensitive"),
                (&sensitive_doc, "sensitive"),
                (&content, "ordinary"),
            ] {
                let ancestors = state.ancestors(ScopeTarget::Doc(doc)).await.unwrap();
                for held in Access::ALL {
                    let ceiling = held.min(first).min(second);
                    let expected = match audience {
                        "public" => Some(ceiling.max(Access::Read)),
                        "sensitive" => (ceiling >= Access::Read).then_some(ceiling),
                        "ordinary" => Some(ceiling.min(Access::Write)),
                        _ => unreachable!(),
                    };
                    assert_eq!(
                        ancestors[&parent].apply(held),
                        expected,
                        "{audience}: {held:?}/{first:?}/{second:?}"
                    );
                }
            }
        }
    }
    for doc in [&public_doc, &sensitive, &sensitive_doc, &content] {
        assert!(
            state
                .ancestors(ScopeTarget::Drawer(doc))
                .await
                .unwrap()
                .is_empty()
        );
    }
    for layout in [
        Drawer::self_doc(),
        serde_json::from_value(serde_json::json!({
            "$type": "org.example.daybook.drawer",
            "layout": {"$type": "org.example.future.layout", "sensitiveRoot": sensitive.to_string()}
        }))
        .unwrap(),
    ] {
        state.put_drawer(&leaf, &layout).await.unwrap();
        for doc in [&public_doc, &sensitive, &sensitive_doc, &content] {
            assert!(
                state
                    .ancestors(ScopeTarget::Doc(doc))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert_eq!(
            state.ancestors(ScopeTarget::Doc(&leaf)).await.unwrap()[&parent].apply(Access::Relay),
            Some(Access::Relay),
        );
    }
}

#[tokio::test]
async fn scoped_sql_terminates_cycles_and_selects_the_strongest_path() {
    let runtime = SqliteRuntime::new(Default::default());
    let state = State::open(runtime.ephemeral(&[]).unwrap()).await.unwrap();
    let [root, left, right, leaf, doc] = [1_u8, 2, 3, 4, 5].map(|value| DocId::new([value; 32]));
    for id in [&root, &left, &right] {
        state
            .put_drawer(
                id,
                &Drawer::single_group(
                    GroupKey([8; 32]),
                    GroupKey([9; 32]),
                    GroupKey([10; 32]),
                    DocId::new([11; 32]),
                ),
            )
            .await
            .unwrap();
    }
    state.put_drawer(&leaf, &Drawer::self_doc()).await.unwrap();
    for (parent, child, access) in [
        (&root, &left, Access::Read),
        (&root, &right, Access::Write),
        (&left, &right, Access::Admin),
        (&right, &root, Access::Relay),
        (&right, &leaf, Access::Read),
    ] {
        state.put_child(parent, child, access).await.unwrap();
    }
    state.put_doc(&right, &doc, Access::Admin).await.unwrap();
    assert_eq!(
        state.scoped_docs(&root, Access::Admin).await.unwrap(),
        BTreeMap::from([
            (doc.clone(), (Access::Write, Some(right.clone()))),
            (leaf.clone(), (Access::Read, Some(leaf.clone()))),
        ]),
    );
    assert_eq!(
        state.ancestors(ScopeTarget::Doc(&doc)).await.unwrap(),
        BTreeMap::from([
            (root.clone(), Reach::ordinary(Access::Write)),
            (left.clone(), Reach::ordinary(Access::Admin)),
            (right.clone(), Reach::ordinary(Access::Admin)),
        ]),
    );
    assert_eq!(
        state.ancestors(ScopeTarget::Drawer(&leaf)).await.unwrap(),
        BTreeMap::from([
            (root.clone(), Reach::ordinary(Access::Read)),
            (left.clone(), Reach::ordinary(Access::Read)),
            (right.clone(), Reach::ordinary(Access::Read)),
            (leaf.clone(), Reach::ordinary(Access::Admin)),
        ]),
    );
    assert_eq!(
        state.scoped_docs(&root, Access::Relay).await.unwrap(),
        BTreeMap::from([
            (doc.clone(), (Access::Relay, Some(right.clone()))),
            (leaf.clone(), (Access::Relay, Some(leaf.clone()))),
        ]),
    );
    let roots = BTreeMap::from([(root.clone(), Access::Read), (right.clone(), Access::Admin)]);
    assert_eq!(
        state.scoped_doc_access(&roots, None).await.unwrap(),
        BTreeMap::from([(doc.clone(), Access::Admin), (leaf.clone(), Access::Read)]),
    );
    assert_eq!(
        state
            .scoped_doc_access(&roots, Some(&BTreeSet::from([leaf.clone(), root.clone()])))
            .await
            .unwrap(),
        BTreeMap::from([(leaf.clone(), Access::Read)]),
    );
    assert!(
        state
            .scoped_doc_access(&roots, Some(&BTreeSet::new()))
            .await
            .unwrap()
            .is_empty()
    );
    let mut expected = Vec::new();
    for (drawer, access) in [
        (&root, Access::Read),
        (&left, Access::Read),
        (&right, Access::Admin),
        (&leaf, Access::Read),
    ] {
        expected.push((state.drawer(drawer).await.unwrap().unwrap(), access));
    }
    expected.sort_by(|(left, _), (right, _)| left.drawer_id.cmp(&right.drawer_id));
    assert_eq!(state.scoped_drawers(&roots).await.unwrap(), expected);
    state.put_drawer(&right, &Drawer::self_doc()).await.unwrap();
    assert_eq!(
        state.scoped_docs(&root, Access::Admin).await.unwrap(),
        BTreeMap::from([(right.clone(), (Access::Write, Some(right)))]),
    );
    assert!(
        state
            .ancestors(ScopeTarget::Doc(&doc))
            .await
            .unwrap()
            .is_empty()
    );
}
