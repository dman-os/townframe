use super::*;
use automerge::transaction::Transactable;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;

fn key(name: &str) -> DatumKey {
    format!("org.example.note/{name}").parse().unwrap()
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
    let store = DatumSetStore::new(sql).await.unwrap();
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
    let store = DatumSetStore::new(sql).await.unwrap();
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
    let store = DatumSetStore::new(sql.clone()).await.unwrap();
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
    let reopened = DatumSetStore::new(sql).await.unwrap();
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
    let store = DatumSetStore::new(sql.clone()).await.unwrap();
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
    big_repo::BigRepo::boot(
        Arc::new(sqlite_rs::SqliteRuntime::new(Default::default())),
        big_repo::Config {
            node_identity_seed: [31; 32],
            storage: big_repo::StorageConfig::Memory,
            scope_key: "datum-set-test".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        },
    )
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
    let store = DatumSetStore::new(sql.clone()).await.unwrap();
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

    let store = DatumSetStore::new(sql).await.unwrap();
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
    let store = DatumSetStore::new(sql.clone()).await.unwrap();
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
