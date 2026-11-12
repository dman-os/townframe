use super::*;
use crate::index::DatumRefQuery;
use crate::index::DatumRefStore;
use automerge::transaction::Transactable;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::BranchPath;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use sqlite_rs::IntoValue;

/// An empty schema repo: these tests project memberships, and a tag with no known schema
/// declares no reference position, so nothing here extracts edges. The reference tests below
/// build their own repo.
fn schemas() -> Arc<SchemaRepo> {
    Arc::new(SchemaRepo::open(Vec::new()).unwrap())
}

const NOTE: &str = "org.example.note";

fn key(name: &str) -> DatumKey {
    format!("{NOTE}/{name}").parse().unwrap()
}

/// A repo whose `org.example.note` declares a `datumRef` position: the settle, removal, and
/// backfill paths extract and clear edges only for tags whose schema is known to declare one.
fn ref_schemas() -> Arc<SchemaRepo> {
    let lexicon = serde_json::json!({
        "lexicon": 1,
        "id": NOTE,
        "defs": {"main": {"type": "record", "key": "tid", "record": {
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": {"type": "string"},
                "link": {"type": "ref", "ref": "org.example.daybook.defs#datumRef"}
            }
        }}}
    });
    Arc::new(
        SchemaRepo::open([crate::schema::SchemaRegistrationGroup::new(
            None,
            [crate::schema::SchemaRegistration::new(NOTE, lexicon)],
            Vec::new(),
        )])
        .unwrap(),
    )
}

fn ephemeral() -> Database {
    sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap()
}

fn route(doc: u8, name: &str) -> DatumRoute {
    DatumRoute {
        doc_id: DocId::new([doc; 32]),
        datum_key: key(name),
    }
}

fn edge(origin: &DatumRoute, target: &DatumRoute) -> DatumRefEdge {
    DatumRefEdge {
        origin: origin.clone(),
        path: "/link".to_owned(),
        target: DatumRefTarget::Route {
            route: target.clone(),
            heads: vec![ChangeHash([7; 32])],
        },
    }
}

/// Put rows in the shared edge table directly: the removal and backfill paths must react to
/// edges a previous projection wrote, however they got there.
async fn seed_edges(sql: &Database, edges: &[DatumRefEdge]) {
    for edge in edges {
        let (target_doc, target_datum, ref_heads) = edge.target.columns().unwrap();
        Sql::new(
            r#"INSERT INTO big_cabinet_datum_ref_edges (
                   origin_doc
                 , origin_datum
                 , origin_path
                 , target_doc
                 , target_datum
                 , ref_heads
               ) VALUES "#,
        )
        .bind_list([
            edge.origin.doc_id.to_string().into_value(),
            edge.origin.datum_key.to_string().into_value(),
            edge.path.clone().into_value(),
            target_doc.into_value(),
            target_datum.into_value(),
            ref_heads.into_value(),
        ])
        .execute(sql)
        .await
        .unwrap();
    }
}

fn heads_clock(doc: &Automerge, heads: &[automerge::ChangeHash]) -> u64 {
    heads
        .iter()
        .filter_map(|head| doc.get_change_by_hash(head))
        .map(|change| change.max_op())
        .max()
        .unwrap_or(0)
}

fn state(doc: &Automerge, cursor: u64) -> DocState {
    let mut heads = doc.get_heads();
    heads.sort_unstable();
    DocState {
        heads: Some(heads),
        epoch: None,
        cursor,
    }
}

fn fixture() -> (Automerge, ObjId, ObjId, ObjId) {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let datums = tx.put_object(ROOT, "datums", ObjType::Map).unwrap();
    let first = tx
        .put_object(&datums, key("a").to_string(), ObjType::Map)
        .unwrap();
    tx.put(&first, "value", "a").unwrap();
    tx.put(&datums, key("b").to_string(), "b").unwrap();
    let dmeta = tx.put_object(&datums, DMETA_KEY, ObjType::Map).unwrap();
    let metadata = tx.put_object(&dmeta, "datums", ObjType::Map).unwrap();
    let entry = tx
        .put_object(&metadata, key("a").to_string(), ObjType::Map)
        .unwrap();
    let signatures = tx.put_object(&entry, "signatures", ObjType::List).unwrap();
    tx.insert_object(&signatures, 0, ObjType::Map).unwrap();
    tx.commit();
    (doc, datums, first, metadata)
}

fn project(doc: &Automerge, previous: Option<&DocState>, cursor: u64) -> Prepared {
    let next = state(doc, cursor);
    let (slots, replace_all) = DocView::new(doc, next.heads.as_ref().unwrap())
        .unwrap()
        .project(previous)
        .unwrap();
    Prepared {
        work: Work {
            doc_id: DocId::new([1; 32]),
            state: next,
        },
        previous: previous.cloned(),
        slots,
        replace_all,
        ref_edges: BTreeMap::new(),
    }
}

#[test]
fn raw_initial_and_historical_views_do_not_verify_or_hydrate() {
    let (mut doc, datums, _, _) = fixture();
    let initial = state(&doc, 1);
    let first = project(&doc, None, 1);
    assert!(first.replace_all);
    assert_eq!(
        first.slots,
        BTreeMap::from([
            (
                key("a"),
                DatumMembership::Present {
                    doc_heads: initial.heads.clone().unwrap(),
                    clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                    datum_heads: initial.heads.clone().unwrap()
                }
            ),
            (
                key("b"),
                DatumMembership::Present {
                    doc_heads: initial.heads.clone().unwrap(),
                    datum_heads: Vec::new(),
                    clock: 0
                }
            ),
        ])
    );
    let mut tx = doc.transaction();
    tx.delete(&datums, key("a").to_string()).unwrap();
    tx.commit();
    let view = DocView::new(&doc, initial.heads.as_ref().unwrap()).unwrap();
    assert!(!view.current);
    assert_eq!(view.project(None).unwrap().0, first.slots);
    let next = project(&doc, Some(&initial), 2);
    assert_eq!(
        next.slots,
        BTreeMap::from([(
            key("a"),
            DatumMembership::Removed {
                doc_heads: Some(doc.get_heads()),
                clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                datum_heads: initial.heads.unwrap(),
            }
        )])
    );
}

#[test]
fn nested_unsigned_edits_emit_only_the_touched_slot_with_unchanged_signature_heads() {
    let (mut doc, _, first, _) = fixture();
    let initial = state(&doc, 1);
    let mut tx = doc.transaction();
    tx.put(&first, "value", "changed").unwrap();
    tx.commit();
    let next = project(&doc, Some(&initial), 2);
    assert!(!next.replace_all);
    assert_eq!(
        next.slots,
        BTreeMap::from([(
            key("a"),
            DatumMembership::Present {
                doc_heads: doc.get_heads(),
                clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                datum_heads: initial.heads.unwrap(),
            }
        )])
    );
}

#[test]
fn hidden_and_deleted_object_operations_still_touch_their_datum() {
    let (mut doc, datums, first, _) = fixture();
    let base = state(&doc, 1);
    let mut peer = doc.fork();
    let mut tx = doc.transaction();
    tx.put(&first, "value", "same").unwrap();
    tx.commit();
    let before = state(&doc, 2);
    let mut tx = peer.transaction();
    tx.put(&first, "value", "same").unwrap();
    tx.commit();
    doc.merge(&mut peer).unwrap();
    assert_eq!(
        project(&doc, Some(&before), 3)
            .slots
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [key("a")]
    );
    let mut tx = doc.transaction();
    tx.delete(&datums, key("a").to_string()).unwrap();
    tx.commit();
    let before = state(&doc, 4);
    let mut peer = doc.fork_at(base.heads.as_ref().unwrap()).unwrap();
    let mut tx = peer.transaction();
    tx.put(&first, "hidden", "invisible").unwrap();
    tx.commit();
    doc.merge(&mut peer).unwrap();
    assert_eq!(
        project(&doc, Some(&before), 5)
            .slots
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [key("a")]
    );
}

#[test]
fn concurrent_metadata_maps_contribute_all_signature_introductions() {
    let (mut doc, _, _, metadata) = fixture();
    let before = state(&doc, 1);
    let mut peer = doc.fork();
    for branch in [&mut doc, &mut peer] {
        let mut tx = branch.transaction();
        let entry = tx
            .put_object(&metadata, key("b").to_string(), ObjType::Map)
            .unwrap();
        let list = tx.put_object(&entry, "signatures", ObjType::List).unwrap();
        tx.insert_object(&list, 0, ObjType::Map).unwrap();
        tx.commit();
    }
    let mut introductions = doc.get_heads();
    introductions.extend(peer.get_heads());
    introductions.sort_unstable();
    doc.merge(&mut peer).unwrap();
    let next = project(&doc, Some(&before), 2);
    assert_eq!(
        next.slots,
        BTreeMap::from([(
            key("b"),
            DatumMembership::Present {
                doc_heads: introductions.clone(),
                clock: heads_clock(&doc, &introductions),
                datum_heads: introductions,
            }
        )])
    );
}

#[test]
fn dmeta_fields_other_than_datums_do_not_touch_slots() {
    let (mut doc, datums, _, _) = fixture();
    let before = state(&doc, 1);
    let (_, dmeta) = doc.get(&datums, DMETA_KEY).unwrap().unwrap();
    let mut tx = doc.transaction();
    tx.put(&dmeta, "aliases", "renamed").unwrap();
    tx.commit();
    let next = project(&doc, Some(&before), 2);
    assert!(!next.replace_all);
    assert!(next.slots.is_empty(), "{:?}", next.slots);
}

#[test]
fn projection_across_a_tombstoned_signature_uses_historical_paths() {
    let (mut doc, _, _, metadata) = fixture();
    let before = state(&doc, 1);
    let (_, entry) = doc.get(&metadata, key("a").to_string()).unwrap().unwrap();
    let (_, signatures) = doc.get(&entry, "signatures").unwrap().unwrap();
    let (_, signature) = doc.get(&signatures, 0).unwrap().unwrap();
    let mut tx = doc.transaction();
    tx.put(&signature, "actor", "z").unwrap();
    tx.commit();
    // Tombstone the signature map in a later change of the same
    // projection window: its creator insert becomes the list's only,
    // invisible op, which automerge's clock-less parent walk cannot
    // seek (fast/slow `seek_list_opid` divergence).
    let mut tx = doc.transaction();
    tx.delete(&signatures, 0).unwrap();
    tx.commit();
    let next = project(&doc, Some(&before), 2);
    assert!(!next.replace_all);
    assert_eq!(
        next.slots,
        BTreeMap::from([(
            key("a"),
            DatumMembership::Present {
                doc_heads: doc.get_heads(),
                datum_heads: Vec::new(),
                clock: 0,
            }
        )])
    );
}

#[tokio::test]
async fn key_query_returns_records_with_signature_clocks() {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = DatumSetStore::new(sql, schemas()).await.unwrap();
    let (mut doc, _, first, _) = fixture();
    let initial = state(&doc, 1);
    store.settle(project(&doc, None, 1)).await.unwrap();
    let records = store.query(DatumSetQuery::Key(key("a"))).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].route.doc_id, DocId::new([1; 32]));
    assert_eq!(
        records[0].clock,
        heads_clock(&doc, initial.heads.as_ref().unwrap())
    );
    assert!(records[0].clock > 0);
    let first_clock = records[0].clock;
    let mut tx = doc.transaction();
    tx.put(&first, "value", "changed").unwrap();
    tx.commit();
    // An unsigned value edit does not move the datum version: its clock is
    // over the signature introductions, which are unchanged.
    store
        .settle(project(&doc, Some(&initial), 2))
        .await
        .unwrap();
    let records = store.query(DatumSetQuery::Key(key("a"))).await.unwrap();
    assert_eq!(records[0].clock, first_clock);
    assert!(
        store
            .query(DatumSetQuery::Key(key("missing")))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn selectors_and_document_batches_preserve_routes_and_filter_intersections() {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = DatumSetStore::new(sql, schemas()).await.unwrap();
    let (doc, _, _, _) = fixture();
    let tag = DatumTag::new("org.example.note").unwrap();
    let first_id = DocId::new([1; 32]);
    let second_id = DocId::new([2; 32]);
    store.settle(project(&doc, None, 1)).await.unwrap();
    let mut second = project(&doc, None, 2);
    second.work.doc_id = second_id.clone();
    store.settle(second).await.unwrap();
    let heads = state(&doc, 1).heads.unwrap();
    let expected: Vec<_> = [first_id.clone(), second_id.clone()]
        .into_iter()
        .flat_map(|doc_id| {
            [
                DatumRecord {
                    route: DatumRoute {
                        doc_id: doc_id.clone(),
                        datum_key: key("a"),
                    },
                    doc_heads: heads.clone(),
                    datum_heads: heads.clone(),
                    clock: heads_clock(&doc, &heads),
                },
                DatumRecord {
                    route: DatumRoute {
                        doc_id,
                        datum_key: key("b"),
                    },
                    doc_heads: heads.clone(),
                    datum_heads: Vec::new(),
                    clock: 0,
                },
            ]
        })
        .collect();
    let mut expected = expected;
    expected.sort_by(|left, right| left.route.cmp(&right.route));
    assert_eq!(
        store.query(DatumSetQuery::Tag(tag.clone())).await.unwrap(),
        expected
    );
    for record in &expected {
        assert_eq!(
            store
                .query(DatumSetQuery::Route(record.route.clone()))
                .await
                .unwrap(),
            vec![record.clone()]
        );
    }
    let docs = BTreeSet::from([first_id, second_id, DocId::new([3; 32])]);
    assert_eq!(store.query_docs(&docs, None, None).await.unwrap(), expected);
    assert_eq!(
        store.query_docs(&docs, Some(&tag), None).await.unwrap(),
        expected
    );
    let selected: Vec<_> = expected
        .into_iter()
        .filter(|record| record.route.datum_key == key("a"))
        .collect();
    assert_eq!(
        store.query(DatumSetQuery::Key(key("a"))).await.unwrap(),
        selected
    );
    assert_eq!(
        store
            .query_docs(&docs, None, Some(&key("a")))
            .await
            .unwrap(),
        selected
    );
    assert_eq!(
        store
            .query_docs(&docs, Some(&tag), Some(&key("a")))
            .await
            .unwrap(),
        selected
    );
    assert!(
        store
            .query_docs(
                &docs,
                Some(&DatumTag::new("org.example.other").unwrap()),
                Some(&key("a"))
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .query_docs(&BTreeSet::new(), Some(&tag), Some(&key("a")))
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn envelope_replacement_rescans_and_ignores_later_materialized_changes() {
    let (mut doc, _, _, _) = fixture();
    let before = state(&doc, 1);
    let mut tx = doc.transaction();
    let datums = tx.put_object(ROOT, "datums", ObjType::Map).unwrap();
    tx.put(&datums, key("c").to_string(), "c").unwrap();
    tx.commit();
    let replacement = state(&doc, 2);
    let next = project(&doc, Some(&before), 2);
    assert!(next.replace_all);
    assert_eq!(next.slots.keys().cloned().collect::<Vec<_>>(), [key("c")]);
    let mut tx = doc.transaction();
    tx.put(&datums, key("later").to_string(), "later").unwrap();
    tx.commit();
    let historic = DocView::new(&doc, replacement.heads.as_ref().unwrap())
        .unwrap()
        .project(Some(&before))
        .unwrap();
    assert_eq!(historic, (next.slots, next.replace_all));
}

async fn next_entries(
    store: &DatumSetStore,
    tag: &str,
    after: u64,
) -> (u64, Vec<FrontierEntry<DatumRoute, DatumMembership>>) {
    let mut reader = store
        .open(DatumTag::new(tag).unwrap(), after)
        .await
        .unwrap();
    loop {
        match reader.next(RevisionReadLimits::default()).await.unwrap() {
            RevisionRead::Entries { revision, entries } if !entries.is_empty() => {
                return (revision, entries);
            }
            RevisionRead::Entries { .. } | RevisionRead::ReplayComplete { .. } => {}
        }
    }
}

#[tokio::test]
async fn settlements_are_atomic_idempotent_and_removals_preserve_tombstone_heads() {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let (mut doc, datums, _, _) = fixture();
    let initial = state(&doc, 1);
    sql.execute_batch("CREATE TRIGGER reject_datum_set BEFORE INSERT ON big_cabinet_datum_set_docs BEGIN SELECT RAISE(ABORT, 'injected'); END")
        .await.unwrap();
    assert!(store.settle(project(&doc, None, 1)).await.is_err());
    assert_eq!(store.latest_revision().await.unwrap(), 0);
    assert_eq!(store.doc_state(&DocId::new([1; 32])).await.unwrap(), None);
    assert!(
        store
            .query(DatumSetQuery::Tag(
                DatumTag::new("org.example.note").unwrap()
            ))
            .await
            .unwrap()
            .is_empty()
    );
    sql.execute_batch("DROP TRIGGER reject_datum_set")
        .await
        .unwrap();
    store.settle(project(&doc, None, 1)).await.unwrap();
    let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
    assert_eq!((revision, entries.len()), (1, 2));
    store.settle(project(&doc, None, 1)).await.unwrap();
    assert_eq!(store.latest_revision().await.unwrap(), 1);
    let mut tx = doc.transaction();
    tx.delete(&datums, key("a").to_string()).unwrap();
    tx.commit();
    store
        .settle(project(&doc, Some(&initial), 2))
        .await
        .unwrap();
    let (revision, entries) = next_entries(&store, "org.example.note", 1).await;
    assert_eq!(
        entries,
        vec![FrontierEntry {
            revision,
            key: DatumRoute {
                doc_id: DocId::new([1; 32]),
                datum_key: key("a")
            },
            value: Some(DatumMembership::Removed {
                doc_heads: Some(doc.get_heads()),
                clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                datum_heads: initial.heads.unwrap()
            })
        }]
    );
    let reopened = DatumSetStore::new(sql, schemas()).await.unwrap();
    assert_eq!(
        reopened
            .query(DatumSetQuery::Doc(DocId::new([1; 32])))
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.route.datum_key)
            .collect::<Vec<_>>(),
        [key("b")]
    );
    assert_eq!(store.source_revision().await.unwrap(), 0);
}

#[tokio::test]
async fn replacement_and_physical_removal_remove_all_old_routes_without_regressing_state() {
    let store = DatumSetStore::new(
        sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap(),
        schemas(),
    )
    .await
    .unwrap();
    let (mut doc, _, _, _) = fixture();
    let initial = state(&doc, 1);
    store.settle(project(&doc, None, 1)).await.unwrap();
    let stale = project(&doc, Some(&initial), 2);
    let mut tx = doc.transaction();
    tx.put_object(ROOT, "datums", ObjType::Map).unwrap();
    tx.commit();
    store
        .settle(project(&doc, Some(&initial), 3))
        .await
        .unwrap();
    store.settle(stale).await.unwrap();
    assert_eq!(
        store.doc_state(&DocId::new([1; 32])).await.unwrap(),
        Some(state(&doc, 3))
    );
    assert!(
        store
            .query(DatumSetQuery::Doc(DocId::new([1; 32])))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn projection_does_not_acquire_sync_demand() {
    let (document, _, _, _) = fixture();
    let next = state(&document, 1);
    let heads = next.heads.clone().unwrap();
    let (_directory, repo, stop, doc_id) =
        crate::index::test_support::reopen_document(document).await;
    let mut demand = repo.sync_demand();
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let prepared = store
        .prepare(
            &repo,
            Work {
                doc_id: doc_id.clone(),
                state: next,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        prepared.slots.keys().cloned().collect::<Vec<_>>(),
        [key("a"), key("b")]
    );
    assert!(demand.documents().is_empty());
    store.settle(prepared).await.unwrap();
    assert!(store.latest_revision().await.unwrap() > 0);
    assert_eq!(
        store
            .query(DatumSetQuery::Doc(doc_id.clone()))
            .await
            .unwrap()
            .into_iter()
            .map(|record| (record.route.datum_key, record.doc_heads))
            .collect::<Vec<_>>(),
        [(key("a"), heads.clone()), (key("b"), heads)]
    );
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
    drop(repo);
    stop.stop().await.unwrap();
    drop(store);
    drop(sql);
}

struct ScriptSource {
    receiver: std::sync::Mutex<
        Option<tokio::sync::mpsc::UnboundedReceiver<RevisionRead<u64, AutomergeFrontierEvent>>>,
    >,
    replay: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

struct ScriptReader {
    receiver: tokio::sync::mpsc::UnboundedReceiver<RevisionRead<u64, AutomergeFrontierEvent>>,
    replay: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait]
impl RevisionedStore for ScriptSource {
    type Revision = u64;
    type Entry = AutomergeFrontierEvent;
    type Selector = ();
    type Error = utils_rs::prelude::eyre::Report;
    type Reader<'a> = ScriptReader;

    async fn latest_revision(&self) -> Res<u64> {
        Ok(0)
    }

    async fn open<'a>(&'a self, (): (), _after: u64) -> Res<ScriptReader> {
        Ok(ScriptReader {
            receiver: self.receiver.lock().unwrap().take().unwrap(),
            replay: self.replay.lock().unwrap().take(),
        })
    }
}

#[async_trait]
impl RevisionedStoreReader<u64, AutomergeFrontierEvent, utils_rs::prelude::eyre::Report>
    for ScriptReader
{
    async fn next(
        &mut self,
        _limits: RevisionReadLimits,
    ) -> Res<RevisionRead<u64, AutomergeFrontierEvent>> {
        let read = self
            .receiver
            .recv()
            .await
            .expect("script sender stays alive");
        if matches!(read, RevisionRead::ReplayComplete { .. })
            && let Some(replay) = self.replay.take()
        {
            replay
                .send(())
                .expect("script replay observer remains open");
        }
        Ok(read)
    }
}

fn changed(
    doc_id: &DocId,
    doc: &Automerge,
    revision: u64,
) -> RevisionRead<u64, AutomergeFrontierEvent> {
    RevisionRead::Entries {
        revision,
        entries: vec![AutomergeFrontierEvent::Changed {
            doc_id: doc_id.clone(),
            heads: doc.get_heads().into(),
            causal_epoch: None,
            routes: Vec::new(),
            revision,
        }],
    }
}

async fn boot_repo() -> (SharedBigRepo, big_repo::BigRepoStopToken) {
    big_repo::BigRepo::boot(big_repo::Config {
        node_identity_seed: [31; 32],
        storage: big_repo::Storage::memory(&sqlite_rs::SqliteRuntime::new(Default::default()))
            .unwrap(),
        scope_key: "datum-set-test".into(),
        hidden_parts: Default::default(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
    })
    .await
    .unwrap()
}

async fn start_script(
    store: DatumSetStore,
    repo: SharedBigRepo,
    source: ScriptSource,
) -> (service_utils_rs::StopToken, service_utils_rs::Spawner) {
    let (stop, tasks) = service_utils_rs::StopToken::new();
    let (ready, started) = tokio::sync::oneshot::channel();
    let owner = tasks.clone();
    tasks
        .spawn(async move {
            store
                .drive(
                    &source,
                    (),
                    repo,
                    NonZeroUsize::new(2).unwrap(),
                    owner.cancelled(),
                    &mut Some(ready),
                )
                .await
                .unwrap();
        })
        .unwrap();
    started.await.unwrap().unwrap();
    (stop, tasks)
}

#[tokio::test]
async fn a_parked_doc_does_not_block_other_docs_and_newer_work_supersedes_it() {
    let (repo, stop) = boot_repo().await;
    let store = DatumSetStore::new(
        sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap(),
        schemas(),
    )
    .await
    .unwrap();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let source = ScriptSource {
        receiver: std::sync::Mutex::new(Some(receiver)),
        replay: std::sync::Mutex::new(None),
    };
    let (worker_stop, worker_tasks) = start_script(store.clone(), Arc::clone(&repo), source).await;
    let delayed_id = repo.allocate_id().await.unwrap();
    let (mut delayed, _, first, _) = fixture();
    sender.send(changed(&delayed_id, &delayed, 1)).unwrap();
    let other_id = repo.allocate_id().await.unwrap();
    let (other, _, _, _) = fixture();
    sender.send(changed(&other_id, &other, 2)).unwrap();
    let other = repo
        .commit_id(other_id.clone(), other, Vec::new(), &[])
        .await
        .unwrap();
    let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
    assert!(entries.iter().all(|entry| entry.key.doc_id == other_id));
    assert_eq!(store.source_revision().await.unwrap(), 0);
    let mut tx = delayed.transaction();
    tx.put(&first, "value", "superseding").unwrap();
    tx.commit();
    let latest_heads = delayed.get_heads();
    sender.send(changed(&delayed_id, &delayed, 3)).unwrap();
    let delayed = repo
        .commit_id(delayed_id.clone(), delayed, Vec::new(), &[])
        .await
        .unwrap();
    let mut after = revision;
    loop {
        let (revision, entries) = next_entries(&store, "org.example.note", after).await;
        after = revision;
        if entries.iter().any(|entry| {
            entry.key.doc_id == delayed_id
                && matches!(&entry.value,
            Some(DatumMembership::Present { doc_heads, .. }) if *doc_heads == latest_heads)
        }) {
            break;
        }
    }
    assert_eq!(
        store.doc_state(&delayed_id).await.unwrap().unwrap().cursor,
        3
    );
    sender
        .send(RevisionRead::Entries {
            revision: 4,
            entries: vec![AutomergeFrontierEvent::Removed {
                doc_id: delayed_id.clone(),
                route: big_sync_core::PartKey::new(b"removed-route"),
                revision: 4,
            }],
        })
        .unwrap();
    let (_, entries) = next_entries(&store, "org.example.note", after).await;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.key.doc_id == delayed_id
        && matches!(
            entry.value,
            Some(DatumMembership::Removed {
                doc_heads: None,
                ..
            })
        )));
    assert!(
        store
            .query(DatumSetQuery::Doc(delayed_id))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .query(DatumSetQuery::Doc(other_id))
            .await
            .unwrap()
            .len(),
        2
    );
    worker_stop.stop().await;
    assert_eq!(worker_tasks.live_tasks(), 0);
    drop(delayed);
    drop(other);
    drop(repo);
    stop.stop().await.unwrap();
}

#[tokio::test]
async fn cooperative_stop_preserves_parked_work_and_replays_without_duplicate_output() {
    let (repo, stop) = boot_repo().await;
    let sql = sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (worker_stop, worker_tasks) = start_script(
        store.clone(),
        Arc::clone(&repo),
        ScriptSource {
            receiver: std::sync::Mutex::new(Some(receiver)),
            replay: std::sync::Mutex::new(None),
        },
    )
    .await;
    let delayed_id = repo.allocate_id().await.unwrap();
    let (delayed, _, _, _) = fixture();
    sender.send(changed(&delayed_id, &delayed, 1)).unwrap();
    let other_id = repo.allocate_id().await.unwrap();
    let (other, _, _, _) = fixture();
    sender.send(changed(&other_id, &other, 2)).unwrap();
    let other = repo
        .commit_id(other_id.clone(), other, Vec::new(), &[])
        .await
        .unwrap();
    let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
    assert!(entries.iter().all(|entry| entry.key.doc_id == other_id));
    worker_stop.stop().await;
    assert_eq!(worker_tasks.live_tasks(), 0);
    assert_eq!(store.source_revision().await.unwrap(), 0);
    assert!(store.doc_state(&delayed_id).await.unwrap().is_none());
    let other_state = store.doc_state(&other_id).await.unwrap().unwrap();
    assert_eq!(other_state.cursor, 2);
    drop(sender);
    drop(store);

    let store = DatumSetStore::new(sql, schemas()).await.unwrap();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    sender.send(changed(&delayed_id, &delayed, 1)).unwrap();
    let delayed = repo
        .commit_id(delayed_id.clone(), delayed, Vec::new(), &[])
        .await
        .unwrap();
    sender
        .send(RevisionRead::Entries {
            revision: 2,
            entries: vec![AutomergeFrontierEvent::Changed {
                doc_id: other_id.clone(),
                heads: other_state.heads.clone().unwrap().into(),
                causal_epoch: None,
                routes: Vec::new(),
                revision: 2,
            }],
        })
        .unwrap();
    let (replay, completed) = tokio::sync::oneshot::channel();
    sender
        .send(RevisionRead::ReplayComplete { through: 2 })
        .unwrap();
    let (worker_stop, worker_tasks) = start_script(
        store.clone(),
        Arc::clone(&repo),
        ScriptSource {
            receiver: std::sync::Mutex::new(Some(receiver)),
            replay: std::sync::Mutex::new(Some(replay)),
        },
    )
    .await;
    completed.await.unwrap();
    let (_, entries) = next_entries(&store, "org.example.note", revision).await;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.key.doc_id == delayed_id));
    worker_stop.stop().await;
    assert_eq!(worker_tasks.live_tasks(), 0);
    assert_eq!(store.source_revision().await.unwrap(), 2);
    assert_eq!(
        store.doc_state(&delayed_id).await.unwrap().unwrap().cursor,
        1
    );
    assert_eq!(
        store.doc_state(&other_id).await.unwrap().unwrap(),
        other_state
    );
    drop(delayed);
    drop(other);
    drop(repo);
    stop.stop().await.unwrap();
}

#[tokio::test]
async fn cooperative_stop_drains_an_inflight_preparation_before_acknowledging_it() {
    let (repo, stop) = boot_repo().await;
    let config = sqlite_rs::RuntimeConfig {
        max_readers_per_db: 1,
        ..Default::default()
    };
    let reader_count = config.max_readers_per_db;
    let sql = sqlite_rs::SqliteRuntime::new(config)
        .ephemeral(&[])
        .unwrap();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let doc_id = repo.allocate_id().await.unwrap();
    let (doc, _, _, _) = fixture();
    let event = changed(&doc_id, &doc, 1);
    let handle = repo
        .commit_id(doc_id.clone(), doc, Vec::new(), &[])
        .await
        .unwrap();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (replay, completed) = tokio::sync::oneshot::channel();
    let (worker_stop, worker_tasks) = start_script(
        store.clone(),
        Arc::clone(&repo),
        ScriptSource {
            receiver: std::sync::Mutex::new(Some(receiver)),
            replay: std::sync::Mutex::new(Some(replay)),
        },
    )
    .await;
    let mut readers: Vec<sqlite_rs::ReadTx> = Vec::new();
    for _ in 0..reader_count {
        readers.push(sql.begin_read().await.unwrap());
    }
    sender.send(event).unwrap();
    sender
        .send(RevisionRead::ReplayComplete { through: 1 })
        .unwrap();
    completed.await.unwrap();
    let mut stopping = Box::pin(worker_stop.stop());
    assert!(futures::poll!(&mut stopping).is_pending());
    assert!(worker_tasks.is_cancelled());
    assert_eq!(worker_tasks.live_tasks(), 1);
    for reader in readers {
        reader.rollback().await.unwrap();
    }
    stopping.await;
    assert_eq!(worker_tasks.live_tasks(), 0);
    assert_eq!(store.doc_state(&doc_id).await.unwrap().unwrap().cursor, 1);
    assert_eq!(store.source_revision().await.unwrap(), 1);
    assert_eq!(
        store.query(DatumSetQuery::Doc(doc_id)).await.unwrap().len(),
        2
    );
    drop(handle);
    drop(repo);
    stop.stop().await.unwrap();
}

#[tokio::test]
async fn live_afw_projection_needs_no_docs_service_and_replays_without_duplicate_output() {
    let (repo, stop) = boot_repo().await;
    let store = DatumSetStore::new(
        sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap(),
        schemas(),
    )
    .await
    .unwrap();
    let start_worker = || {
        let store = store.clone();
        let repo = Arc::clone(&repo);
        async move {
            let (stop, tasks) = service_utils_rs::StopToken::new();
            let (ready, started) = tokio::sync::oneshot::channel();
            let owner = tasks.clone();
            tasks
                .spawn(async move {
                    store
                        .run_until(
                            repo,
                            NonZeroUsize::new(2).unwrap(),
                            owner.cancelled(),
                            &mut Some(ready),
                        )
                        .await
                        .unwrap();
                })
                .unwrap();
            started.await.unwrap().unwrap();
            (stop, tasks)
        }
    };
    let (worker_stop, worker_tasks) = start_worker().await;
    let doc_id = repo.allocate_id().await.unwrap();
    let (doc, _, _, _) = fixture();
    let handle = repo
        .commit_id(doc_id.clone(), doc, Vec::new(), &[])
        .await
        .unwrap();
    let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.key.doc_id == doc_id));
    let before = store.doc_state(&doc_id).await.unwrap().unwrap();
    worker_stop.stop().await;
    assert_eq!(worker_tasks.live_tasks(), 0);
    let work = Work {
        doc_id: doc_id.clone(),
        state: before.clone(),
    };
    store
        .settle(store.prepare(&repo, work).await.unwrap().unwrap())
        .await
        .unwrap();
    assert_eq!(store.latest_revision().await.unwrap(), revision);
    let (worker_stop, worker_tasks) = start_worker().await;
    handle
        .with_document(|doc| {
            let datums = doc.get(ROOT, "datums").unwrap().unwrap().1;
            let first = doc.get(&datums, key("a").to_string()).unwrap().unwrap().1;
            let mut tx = doc.transaction();
            tx.put(&first, "value", "remote-like raw edit").unwrap();
            tx.commit();
        })
        .await
        .unwrap();
    let (_, entries) = next_entries(&store, "org.example.note", revision).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.datum_key, key("a"));
    let DatumMembership::Present { datum_heads, .. } = entries[0].value.as_ref().unwrap() else {
        panic!("live datum")
    };
    assert_eq!(Some(datum_heads.clone()), before.heads);
    worker_stop.stop().await;
    assert_eq!(worker_tasks.live_tasks(), 0);
    drop(handle);
    drop(repo);
    stop.stop().await.unwrap();
}

#[test]
fn a_value_the_gate_does_not_accept_publishes_no_edges() {
    let (mut doc, datums, _, _) = fixture();
    // Give the unsigned datum a well-formed reference value: extraction is gated on the
    // signatures covering the value, not on its shape.
    let mut tx = doc.transaction();
    let unsigned = tx
        .put_object(&datums, key("b").to_string(), ObjType::Map)
        .unwrap();
    tx.put(&unsigned, "value", "text").unwrap();
    tx.put(
        &unsigned,
        "link",
        format!("dtxt:datum:{}/{NOTE}/main", DocId::new([2; 32])),
    )
    .unwrap();
    tx.commit();
    let repo = ref_schemas();
    let schema = repo.resolve_for_read(NOTE).unwrap();
    let heads = doc.get_heads();
    let edges = edges_from(&mut doc, &route(1, "b"), &heads, schema.compiled()).unwrap();
    assert!(edges.is_empty(), "{edges:?}");
}

#[tokio::test]
async fn settle_replaces_the_edges_of_the_routes_it_scanned_and_keeps_the_rest() {
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), ref_schemas())
        .await
        .unwrap();
    let refs = DatumRefStore::new(sql.clone()).await.unwrap();
    let (mut doc, _, first, _) = fixture();
    let initial = state(&doc, 1);
    let (a, b) = (route(1, "a"), route(1, "b"));
    let target = route(2, "main");
    let mut prepared = project(&doc, None, 1);
    prepared.ref_edges = BTreeMap::from([
        (a.clone(), vec![edge(&a, &target)]),
        (b.clone(), vec![edge(&b, &target)]),
    ]);
    store.settle(prepared).await.unwrap();
    assert_eq!(
        refs.query(DatumRefQuery::Origin(a.clone())).await.unwrap(),
        vec![edge(&a, &target)]
    );
    assert_eq!(
        refs.query(DatumRefQuery::Origin(b.clone())).await.unwrap(),
        vec![edge(&b, &target)]
    );
    // A later projection that only re-read `a` publishes its edges as empty and says nothing
    // about `b`, whose stored edges must survive the settle.
    let mut tx = doc.transaction();
    tx.put(&first, "value", "changed").unwrap();
    tx.commit();
    let mut next = project(&doc, Some(&initial), 2);
    next.ref_edges = BTreeMap::from([(a.clone(), Vec::new())]);
    store.settle(next).await.unwrap();
    assert!(
        refs.query(DatumRefQuery::Origin(a))
            .await
            .unwrap()
            .is_empty()
    );
    let kept = vec![edge(&b, &target)];
    assert_eq!(refs.query(DatumRefQuery::Origin(b)).await.unwrap(), kept);
}

#[tokio::test]
async fn a_removed_datum_clears_its_edges_only_when_its_tag_declares_references() {
    let (a, target) = (route(1, "a"), route(2, "main"));
    for declares_references in [true, false] {
        let sql = ephemeral();
        let schemas = if declares_references {
            ref_schemas()
        } else {
            schemas()
        };
        let store = DatumSetStore::new(sql.clone(), schemas).await.unwrap();
        seed_edges(&sql, &[edge(&a, &target)]).await;
        let (mut doc, datums, _, _) = fixture();
        let initial = state(&doc, 1);
        store.settle(project(&doc, None, 1)).await.unwrap();
        let mut tx = doc.transaction();
        tx.delete(&datums, key("a").to_string()).unwrap();
        tx.commit();
        store
            .settle(project(&doc, Some(&initial), 2))
            .await
            .unwrap();
        let refs = DatumRefStore::new(sql.clone()).await.unwrap();
        let remaining = refs.query(DatumRefQuery::Origin(a.clone())).await.unwrap();
        assert_eq!(
            remaining.is_empty(),
            declares_references,
            "declares_references={declares_references}: {remaining:?}"
        );
    }
}

#[tokio::test]
async fn backfill_records_a_tag_only_once_every_stored_document_was_readable() {
    let (document, _, _, _) = fixture();
    let prepared = project(&document, None, 1);
    let (_directory, repo, stop, doc_id) =
        crate::index::test_support::reopen_document(document).await;
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), ref_schemas())
        .await
        .unwrap();
    store
        .settle(Prepared {
            work: Work {
                doc_id: doc_id.clone(),
                ..prepared.work
            },
            ..prepared
        })
        .await
        .unwrap();
    store
        .backfill_reference_tags(&repo, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    // The repo also always knows the core `BranchPath` datum, whose `from` position declares a
    // reference: it has no stored entries here, so it is trivially complete and seen too.
    let mut expected = [NOTE.to_owned(), BranchPath::ID.to_owned()];
    expected.sort();
    let mut seen = queries::ref_tags(&sql).await.unwrap();
    seen.sort();
    assert_eq!(seen, expected);
    // A second pass finds the tags seen and does no work.
    store
        .backfill_reference_tags(&repo, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    let mut seen = queries::ref_tags(&sql).await.unwrap();
    seen.sort();
    assert_eq!(seen, expected);
    stop.stop().await.unwrap();
}

#[tokio::test]
async fn backfill_leaves_a_tag_unseen_while_a_stored_document_is_not_local() {
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), ref_schemas())
        .await
        .unwrap();
    let (document, _, _, _) = fixture();
    // Settled under DocId([1; 32]), which no repo below ever holds.
    store.settle(project(&document, None, 1)).await.unwrap();
    let (elsewhere, _, _, _) = fixture();
    let (_directory, repo, stop, _doc_id) =
        crate::index::test_support::reopen_document(elsewhere).await;
    store
        .backfill_reference_tags(&repo, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert!(queries::ref_tags(&sql).await.unwrap().is_empty());
    stop.stop().await.unwrap();
}

fn doc(byte: u8) -> DocId {
    DocId::new([byte; 32])
}

fn key_in(tag: &str, slot: &str) -> DatumKey {
    DatumKey::from_text(tag, slot).unwrap()
}

fn tag(name: &str) -> DatumTag {
    DatumTag::new(name).unwrap()
}

/// Insert one live entry directly: predicate selection only reads the route columns, so the heads
/// and clock do not matter to it.
async fn seed_entry(sql: &Database, route: &DatumRoute) {
    Sql::new(
        r#"INSERT INTO big_cabinet_datum_set_entries (
               doc_id
             , datum_key
             , datum_tag
             , doc_heads
             , datum_heads
             , clock
           ) VALUES "#,
    )
    .bind_list([
        route.doc_id.to_string().into_value(),
        route.datum_key.to_string().into_value(),
        route.datum_key.tag().to_string().into_value(),
        b"[]".to_vec().into_value(),
        b"[]".to_vec().into_value(),
        0i64.into_value(),
    ])
    .execute(sql)
    .await
    .unwrap();
}

async fn seed_drawers(sql: &Database) {
    sql.execute_batch(include_str!(
        "../../drawer/sqlite/migrations/001_drawers.sql"
    ))
    .await
    .unwrap();
}

async fn seed_drawer(sql: &Database, drawer: &DocId, layout: &str) {
    Sql::new("INSERT INTO big_cabinet_drawers (drawer_id, descriptor_json) VALUES ")
        .bind_list([
            drawer.to_string().into_value(),
            serde_json::json!({ "layout": { "$type": layout } })
                .to_string()
                .into_value(),
        ])
        .execute(sql)
        .await
        .unwrap();
}

async fn seed_drawer_doc(sql: &Database, drawer: &DocId, doc: &DocId, access: &str) {
    Sql::new("INSERT INTO big_cabinet_drawer_docs (drawer_id, doc_id, access) VALUES ")
        .bind_list([
            drawer.to_string().into_value(),
            doc.to_string().into_value(),
            access.to_owned().into_value(),
        ])
        .execute(sql)
        .await
        .unwrap();
}

async fn seed_drawer_child(sql: &Database, parent: &DocId, child: &DocId, access: &str) {
    Sql::new("INSERT INTO big_cabinet_drawer_children (parent_id, child_id, access) VALUES ")
        .bind_list([
            parent.to_string().into_value(),
            child.to_string().into_value(),
            access.to_owned().into_value(),
        ])
        .execute(sql)
        .await
        .unwrap();
}

fn routes(records: Vec<DatumRecord>) -> Vec<(DocId, DatumKey)> {
    records
        .into_iter()
        .map(|record| (record.route.doc_id, record.route.datum_key))
        .collect()
}

#[tokio::test]
async fn predicate_clauses_select_documents_and_return_every_live_route_of_them() {
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let first = doc(1);
    let second = doc(2);
    let third = doc(3);
    let blob = tag("org.example.blob");
    for route in [
        DatumRoute {
            doc_id: first.clone(),
            datum_key: key("a"),
        },
        DatumRoute {
            doc_id: first.clone(),
            datum_key: key("b"),
        },
        DatumRoute {
            doc_id: second.clone(),
            datum_key: key("a"),
        },
        DatumRoute {
            doc_id: third.clone(),
            datum_key: key_in(blob.as_str(), "main"),
        },
    ] {
        seed_entry(&sql, &route).await;
    }
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::HasTag { tag: tag(NOTE) })
                .await
                .unwrap()
        ),
        vec![
            (first.clone(), key("a")),
            (first.clone(), key("b")),
            (second.clone(), key("a")),
        ]
    );
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::HasKey { key: key("a") })
                .await
                .unwrap()
        ),
        vec![
            (first.clone(), key("a")),
            (first.clone(), key("b")),
            (second.clone(), key("a")),
        ]
    );
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::IsDoc {
                    doc_id: third.clone(),
                })
                .await
                .unwrap()
        ),
        vec![(third.clone(), key_in(blob.as_str(), "main"))]
    );
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::And {
                    clauses: vec![
                        Predicate::HasTag { tag: tag(NOTE) },
                        Predicate::HasKey { key: key("b") },
                    ],
                })
                .await
                .unwrap()
        ),
        vec![(first.clone(), key("a")), (first.clone(), key("b"))]
    );
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::Or {
                    clauses: vec![
                        Predicate::IsDoc {
                            doc_id: third.clone(),
                        },
                        Predicate::HasKey { key: key("a") },
                    ],
                })
                .await
                .unwrap()
        ),
        vec![
            (first.clone(), key("a")),
            (first.clone(), key("b")),
            (second.clone(), key("a")),
            (third.clone(), key_in(blob.as_str(), "main")),
        ]
    );
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::Not {
                    inner: Box::new(Predicate::HasTag { tag: tag(NOTE) }),
                })
                .await
                .unwrap()
        ),
        vec![(third.clone(), key_in(blob.as_str(), "main"))]
    );
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::And {
                    clauses: Vec::new()
                })
                .await
                .unwrap()
        )
        .len(),
        4
    );
    assert!(
        store
            .query_predicate(&Predicate::Or {
                clauses: Vec::new()
            })
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn matching_docs_bounds_the_predicate_to_its_candidates_without_needing_entries() {
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let first = doc(1);
    let second = doc(2);
    let third = doc(3);
    let blob_doc = doc(4);
    let absent = doc(9);
    let blob = tag("org.example.blob");
    for route in [
        DatumRoute {
            doc_id: first.clone(),
            datum_key: key("a"),
        },
        DatumRoute {
            doc_id: first.clone(),
            datum_key: key("b"),
        },
        DatumRoute {
            doc_id: second.clone(),
            datum_key: key("a"),
        },
        DatumRoute {
            doc_id: blob_doc.clone(),
            datum_key: key_in(blob.as_str(), "main"),
        },
    ] {
        seed_entry(&sql, &route).await;
    }
    let candidates = BTreeSet::from([
        first.clone(),
        second.clone(),
        third.clone(),
        blob_doc.clone(),
        absent,
    ]);
    assert_eq!(
        store
            .matching_docs(&Predicate::HasTag { tag: tag(NOTE) }, &candidates)
            .await
            .unwrap(),
        BTreeSet::from([first.clone(), second.clone()])
    );
    assert_eq!(
        store
            .matching_docs(&Predicate::IsDoc { doc_id: doc(9) }, &candidates)
            .await
            .unwrap(),
        BTreeSet::from([doc(9)])
    );
    assert!(
        store
            .matching_docs(&Predicate::IsDoc { doc_id: doc(7) }, &candidates)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::Not {
                    inner: Box::new(Predicate::HasTag { tag: tag(NOTE) }),
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([third, blob_doc, doc(9)])
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::And {
                    clauses: vec![
                        Predicate::HasTag { tag: tag(NOTE) },
                        Predicate::HasKey { key: key("b") },
                    ],
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([first])
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::And {
                    clauses: Vec::new()
                },
                &candidates
            )
            .await
            .unwrap(),
        candidates
    );
    assert!(
        store
            .matching_docs(
                &Predicate::Or {
                    clauses: Vec::new()
                },
                &candidates
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .matching_docs(&Predicate::HasTag { tag: tag(NOTE) }, &BTreeSet::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn in_drawer_follows_the_reach_cte_with_and_without_transitivity() {
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    seed_drawers(&sql).await;
    let root = doc(10);
    let child = doc(11);
    let self_doc = doc(12);
    seed_drawer(&sql, &root, "org.example.daybook.drawer.singleGroup").await;
    seed_drawer(&sql, &child, "org.example.daybook.drawer.singleGroup").await;
    seed_drawer(&sql, &self_doc, "org.example.daybook.drawer.selfDoc").await;
    seed_drawer_child(&sql, &root, &child, "Write").await;
    seed_drawer_child(&sql, &root, &self_doc, "Read").await;
    for (doc_id, access) in [
        (doc(1), "Read"),
        (doc(2), "Write"),
        (doc(3), "Relay"),
        (doc(4), "Write"),
        (doc(5), "Read"),
    ] {
        seed_drawer_doc(
            &sql,
            if doc_id == doc(4) || doc_id == doc(5) {
                &child
            } else {
                &root
            },
            &doc_id,
            access,
        )
        .await;
    }
    for doc_id in [doc(1), doc(4)] {
        seed_entry(
            &sql,
            &DatumRoute {
                doc_id,
                datum_key: key("a"),
            },
        )
        .await;
    }
    let candidates = BTreeSet::from([doc(1), doc(2), doc(3), doc(4), doc(5), doc(12), doc(99)]);
    let in_drawer = |drawer_id: DocId, transitive: bool, at_least| Predicate::InDrawer {
        drawer_id,
        transitive,
        at_least,
    };
    assert_eq!(
        store
            .matching_docs(
                &Predicate::InDrawer {
                    drawer_id: root.clone(),
                    transitive: true,
                    at_least: DrawerAccess::Read,
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([doc(1), doc(2), doc(4), doc(5), doc(12)])
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::InDrawer {
                    drawer_id: root.clone(),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([doc(1), doc(2), doc(12)])
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::InDrawer {
                    drawer_id: root.clone(),
                    transitive: true,
                    at_least: DrawerAccess::Write,
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([doc(2), doc(4)])
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::InDrawer {
                    drawer_id: child.clone(),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([doc(4), doc(5)])
    );
    assert_eq!(
        store
            .reachable_docs(&root, false, DrawerAccess::Read)
            .await
            .unwrap(),
        BTreeSet::from([doc(1), doc(2), doc(12)])
    );
    assert_eq!(
        store
            .reachable_docs(&root, true, DrawerAccess::Read)
            .await
            .unwrap(),
        BTreeSet::from([doc(1), doc(2), doc(4), doc(5), doc(12)])
    );
    // A `selfDoc` drawer reaches its own document, which need not carry an entry.
    assert_eq!(
        store
            .matching_docs(
                &Predicate::InDrawer {
                    drawer_id: self_doc.clone(),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([doc(12)])
    );
    // The blob-worker shape: intersect the drawer anchor with a tag.
    assert_eq!(
        routes(
            store
                .query_predicate(&Predicate::And {
                    clauses: vec![
                        in_drawer(root.clone(), true, DrawerAccess::Read),
                        Predicate::HasTag { tag: tag(NOTE) },
                    ],
                })
                .await
                .unwrap()
        ),
        vec![(doc(1), key("a")), (doc(4), key("a"))]
    );
    assert_eq!(
        store
            .matching_docs(
                &Predicate::And {
                    clauses: vec![
                        Predicate::InDrawer {
                            drawer_id: root,
                            transitive: false,
                            at_least: DrawerAccess::Read,
                        },
                        Predicate::Not {
                            inner: Box::new(Predicate::HasKey { key: key("a") }),
                        },
                    ],
                },
                &candidates,
            )
            .await
            .unwrap(),
        BTreeSet::from([doc(2), doc(12)])
    );
}

#[tokio::test]
async fn has_reference_to_joins_the_edge_table_to_the_targets_entry_row() {
    let sql = ephemeral();
    let store = DatumSetStore::new(sql.clone(), schemas()).await.unwrap();
    let note = tag(NOTE);
    let cipher = tag("org.example.cipherBlob");
    let origin = DatumRoute {
        doc_id: doc(1),
        datum_key: key("a"),
    };
    let target = DatumRoute {
        doc_id: doc(2),
        datum_key: key_in(cipher.as_str(), "main"),
    };
    let other_target = DatumRoute {
        doc_id: doc(3),
        datum_key: key_in(cipher.as_str(), "other"),
    };
    seed_entry(&sql, &origin).await;
    seed_entry(&sql, &target).await;
    seed_entry(&sql, &other_target).await;
    seed_edges(
        &sql,
        &[edge(&origin, &target), edge(&origin, &other_target)],
    )
    .await;
    let candidates = BTreeSet::from([doc(1), doc(2), doc(3)]);
    let reference_to = |target_doc| Predicate::HasReferenceTo {
        target_tag: cipher.clone(),
        target_doc,
    };
    assert_eq!(
        store
            .matching_docs(&reference_to(None), &candidates)
            .await
            .unwrap(),
        BTreeSet::from([doc(1)])
    );
    assert_eq!(
        store
            .matching_docs(&reference_to(Some(doc(2))), &candidates)
            .await
            .unwrap(),
        BTreeSet::from([doc(1)])
    );
    assert_eq!(
        store
            .matching_docs(&reference_to(Some(doc(3))), &candidates)
            .await
            .unwrap(),
        BTreeSet::from([doc(1)])
    );
    assert!(
        store
            .matching_docs(&reference_to(Some(doc(9))), &candidates)
            .await
            .unwrap()
            .is_empty()
    );
    // The target's entry row supplies the tag; without one matching `note`, no origin matches.
    assert!(
        store
            .matching_docs(
                &Predicate::HasReferenceTo {
                    target_tag: note,
                    target_doc: None,
                },
                &candidates,
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        routes(store.query_predicate(&reference_to(None)).await.unwrap()),
        vec![(doc(1), key("a"))]
    );
}
