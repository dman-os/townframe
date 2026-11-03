use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use clay_store::BatchOp;
use clay_store::MemoryStore;
use clay_store::OrderedStore;
use clay_store::SqliteStore;
use clay_store::StoreError;
use proptest::prelude::*;

use super::durable::collection_prefix;
use super::durable::decode_row;
use super::durable::encode_row;
use super::durable::row_key;
use super::durable::scope_prefix;
use super::durable::split_row_key;
use super::semantics_tests::Feed;
use super::semantics_tests::ready;
use super::semantics_tests::writer_id;
use super::*;

type Collection = RegistrationCollection<u32, String>;

fn codec() -> DurableCodec<u32, String> {
    DurableCodec {
        encode_key: |key| key.to_be_bytes().to_vec(),
        decode_key: |bytes| u32::from_be_bytes(bytes.try_into().expect("u32 key")),
        encode_value: |value| value.as_bytes().to_vec(),
        decode_value: |bytes| String::from_utf8(bytes.to_vec()).expect("utf8 value"),
    }
}

/// Install rows; `None` is a tombstone.
fn rows(entries: &[(u32, Option<&str>)]) -> BTreeMap<u32, Option<String>> {
    entries
        .iter()
        .map(|(key, value)| (*key, value.map(str::to_owned)))
        .collect()
}

async fn open(store: &Arc<impl OrderedStore + Send + Sync + 'static>) -> Collection {
    Collection::new_durable(Arc::clone(store), b"toolbar".to_vec(), codec(), [1, 2])
        .await
        .unwrap()
}

async fn install(
    collection: &Collection,
    scope_key: &[u8],
    retention: Retention,
    entries: &[(u32, Option<&str>)],
) {
    collection
        .install(
            scope_key,
            retention,
            writer_id(&String::from_utf8_lossy(scope_key)),
            rows(entries),
        )
        .await
        .unwrap()
        .dispatch();
}

fn winners(collection: &Collection) -> Vec<(u32, String)> {
    collection
        .snapshot()
        .into_iter()
        .filter_map(|(key, row)| Some((key, row.value?)))
        .collect()
}

async fn stored_keys(store: &impl OrderedStore) -> Vec<Vec<u8>> {
    store
        .scan_prefix(b"")
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

/// Fails every `apply` while armed; reads always pass through.
struct FailingStore {
    inner: MemoryStore,
    armed: AtomicBool,
}

impl OrderedStore for FailingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(key).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        self.inner.scan_prefix(prefix).await
    }

    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError> {
        if self.armed.load(Ordering::SeqCst) {
            return Err(StoreError::backend("injected failure"));
        }
        self.inner.apply(batch).await
    }
}

/// Install, uninstall and reload through `reopen`, which must hand back a collection
/// over the same persisted state each time (plus whatever keeps its store alive), and
/// check that winners, stamps and the bootstrap feed come back identical every time.
async fn install_uninstall_reload_reproduces_winners<Keep, Fut>(reopen: impl Fn() -> Fut)
where
    Fut: Future<Output = (Keep, Collection)>,
{
    let (keep, collection) = reopen().await;
    // Key 1: `a` and `b` tie on lamport 1, `b` wins. Key 2: the permanent tombstone
    // outranks `a`. Key 3: `b` outranks `a`'s tombstone. Key 4: only the permanent scope.
    install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("a1")), (2, Some("a2")), (3, None)],
    )
    .await;
    install(
        &collection,
        b"b",
        Retention::Installation,
        &[(1, Some("b1")), (3, Some("b3"))],
    )
    .await;
    install(
        &collection,
        b"p",
        Retention::Permanent,
        &[(2, None), (4, Some("perm"))],
    )
    .await;
    let both = collection.snapshot();
    assert_eq!(
        winners(&collection),
        [(1, "b1".into()), (3, "b3".into()), (4, "perm".into())]
    );
    drop(collection);
    drop(keep);

    let (keep, collection) = reopen().await;
    assert_eq!(collection.snapshot(), both);
    let (_, bootstrap) = Feed::open(&collection);
    assert_eq!(bootstrap.len(), both.len());

    // Uninstalling `b` resurfaces what it shadowed, including `a`'s tombstone.
    let (mut feed, _) = Feed::open(&collection);
    collection.uninstall(b"b").await.unwrap().dispatch();
    let published = feed.transactions();
    let [tx] = &published[..] else {
        panic!("expected one transaction")
    };
    assert_eq!(
        tx.iter()
            .map(|change| match change {
                Change::Upsert { key, value, .. } => (*key, value.value.clone()),
                Change::Remove { key, .. } => (*key, None),
            })
            .collect::<Vec<_>>(),
        [(1, Some("a1".to_owned())), (3, None)]
    );
    let after = collection.snapshot();
    assert_eq!(winners(&collection), [(1, "a1".into()), (4, "perm".into())]);
    drop(collection);
    drop(keep);

    let (_keep, collection) = reopen().await;
    assert_eq!(collection.snapshot(), after);
}

#[test]
fn memory_store_reload_reproduces_winners() {
    let store = Arc::new(MemoryStore::new());
    ready(install_uninstall_reload_reproduces_winners(|| async {
        ((), open(&store).await)
    }));
}

#[tokio::test]
async fn sqlite_store_reload_reproduces_winners() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("clay.sqlite");
    install_uninstall_reload_reproduces_winners(|| async {
        let runtime = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default());
        let store = Arc::new(
            SqliteStore::open(runtime.database(&path, &[], "_unused"))
                .await
                .unwrap(),
        );
        let collection = open(&store).await;
        (runtime, collection)
    })
    .await;
}

#[test]
fn untouched_durable_collection_bootstraps_known_empty() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    let (_, bootstrap) = Feed::open(&collection);
    assert!(bootstrap.is_empty());
    assert!(collection.snapshot().is_empty());
}

#[test]
fn reinstall_replaces_the_scope_in_one_transaction_and_keeps_unchanged_stamps() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("one")), (2, Some("two"))],
    ));
    let (mut feed, _) = Feed::open(&collection);
    let unchanged = collection.get(&2).unwrap();

    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(2, Some("two")), (3, Some("three"))],
    ));
    let published = feed.transactions();
    let [tx] = &published[..] else {
        panic!("expected one transaction")
    };
    assert_eq!(
        tx.iter()
            .map(|change| match change {
                Change::Upsert { key, value, .. } => (*key, value.value.clone()),
                Change::Remove { key, .. } => (*key, None),
            })
            .collect::<Vec<_>>(),
        [(1, None), (3, Some("three".to_owned()))]
    );
    assert_eq!(collection.get(&2).unwrap(), unchanged);
    assert_eq!(ready(stored_keys(&*store)).len(), 2);
}

#[test]
fn uninstall_removes_only_its_scope_and_unknown_scopes_are_a_no_op() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("a1")), (2, Some("a2"))],
    ));
    ready(install(
        &collection,
        b"b",
        Retention::Installation,
        &[(1, Some("b1"))],
    ));
    let incarnation = collection.create_scope(writer_id("volatile"));
    incarnation
        .writer()
        .unwrap()
        .put(9, WriteBasis::Concurrent, "volatile".into())
        .unwrap()
        .dispatch();

    ready(collection.uninstall(b"a")).unwrap().dispatch();
    assert_eq!(
        winners(&collection),
        [(1, "b1".into()), (9, "volatile".into())]
    );
    assert_eq!(ready(stored_keys(&*store)).len(), 1);
    let (mut feed, _) = Feed::open(&collection);
    ready(collection.uninstall(b"never")).unwrap().dispatch();
    feed.assert_quiet();
    assert_eq!(ready(stored_keys(&*store)).len(), 1);
}

#[test]
fn incarnation_scopes_are_never_persisted_and_shadow_installed_rows_only_while_live() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("installed"))],
    ));
    let installed = collection.get(&1).unwrap();
    let stored = ready(stored_keys(&*store));

    let scope = collection.create_scope(writer_id("runtime"));
    let writer = scope.writer().unwrap();
    writer
        .put(
            1,
            WriteBasis::Observed(installed.stamp.clone()),
            "runtime".into(),
        )
        .unwrap()
        .dispatch();
    writer
        .put(2, WriteBasis::Concurrent, "volatile".into())
        .unwrap()
        .dispatch();
    assert_eq!(
        winners(&collection),
        [(1, "runtime".into()), (2, "volatile".into())]
    );
    assert_eq!(ready(stored_keys(&*store)), stored);

    scope.close().dispatch();
    assert_eq!(collection.get(&1).unwrap(), installed);
    ready(collection.uninstall(b"a")).unwrap().dispatch();
    assert!(ready(stored_keys(&*store)).is_empty());
    assert!(ready(open(&store)).snapshot().is_empty());
}

#[test]
fn failed_store_apply_changes_nothing_visible() {
    let store = Arc::new(FailingStore {
        inner: MemoryStore::new(),
        armed: AtomicBool::new(false),
    });
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("one"))],
    ));
    let (mut feed, _) = Feed::open(&collection);
    let before = collection.snapshot();
    let stored = ready(stored_keys(&*store));

    store.armed.store(true, Ordering::SeqCst);
    let failing = |scope_key: &[u8], entries: &[(u32, Option<&str>)]| {
        ready(collection.install(
            scope_key,
            Retention::Installation,
            writer_id(&String::from_utf8_lossy(scope_key)),
            rows(entries),
        ))
        .is_err()
    };
    assert!(failing(b"a", &[(2, Some("two"))]));
    assert!(failing(b"new", &[(5, Some("five"))]));
    assert!(ready(collection.uninstall(b"a")).is_err());

    assert_eq!(collection.snapshot(), before);
    assert_eq!(ready(stored_keys(&*store)), stored);
    feed.assert_quiet();
}

#[test]
fn permanent_scopes_survive_everything_that_retracts_others() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"inst",
        Retention::Installation,
        &[(1, Some("installed"))],
    ));
    ready(install(
        &collection,
        b"perm",
        Retention::Permanent,
        &[(2, Some("permanent"))],
    ));
    let scope = collection.create_scope(writer_id("zz"));
    let writer = scope.writer().unwrap();
    writer
        .put(3, WriteBasis::Concurrent, "incarnation".into())
        .unwrap()
        .dispatch();

    // Owner close retracts only the incarnation.
    scope.close().dispatch();
    assert_eq!(
        winners(&collection),
        [(1, "installed".into()), (2, "permanent".into())]
    );

    // Reset keeps both durable kinds and republishes them into the new generation.
    collection.reset().dispatch();
    let (_, bootstrap) = Feed::open(&collection);
    assert_eq!(bootstrap.len(), 2);

    // Uninstalling a permanent scope is a programming error and changes nothing.
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ready(collection.uninstall(b"perm"))
    }));
    assert!(refused.is_err());
    ready(collection.uninstall(b"inst")).unwrap().dispatch();
    assert_eq!(winners(&collection), [(2, "permanent".into())]);

    // It survives a reload; its stamp comes back from the store.
    let before = collection.snapshot();
    assert_eq!(ready(open(&store)).snapshot(), before);

    // Retention and writer are fixed at first install.
    let wrong_retention = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ready(collection.install(
            b"perm",
            Retention::Installation,
            writer_id("perm"),
            rows(&[]),
        ))
    }));
    assert!(wrong_retention.is_err());
    let wrong_writer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ready(collection.install(b"perm", Retention::Permanent, writer_id("other"), rows(&[])))
    }));
    assert!(wrong_writer.is_err());
    assert_eq!(collection.snapshot(), before);
}

#[test]
fn reset_keeps_installed_scopes_and_drops_incarnations() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("one"))],
    ));
    let scope = collection.create_scope(writer_id("runtime"));
    scope
        .writer()
        .unwrap()
        .put(2, WriteBasis::Concurrent, "volatile".into())
        .unwrap()
        .dispatch();
    collection.reset().dispatch();
    let (_, bootstrap) = Feed::open(&collection);
    assert!(matches!(&bootstrap[..], [Change::Upsert { key: 1, .. }]));
    assert_eq!(collection.snapshot().len(), 1);
}

/// Runtime writes through `installed_writer` persist, reload with identical stamps,
/// and are retracted with their scope by `uninstall`; permanent scopes keep theirs.
async fn runtime_writes_survive_reload_and_uninstall_retracts_them<Keep, Fut>(
    reopen: impl Fn() -> Fut,
) where
    Fut: Future<Output = (Keep, Collection)>,
{
    let (keep, collection) = reopen().await;
    install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("a1"))],
    )
    .await;
    install(&collection, b"p", Retention::Permanent, &[]).await;
    let installation = collection.installed_writer(b"a").unwrap();
    let permanent = collection.installed_writer(b"p").unwrap();
    installation
        .put(2, WriteBasis::Concurrent, "a2".into())
        .await
        .unwrap()
        .dispatch();
    permanent
        .put(3, WriteBasis::Concurrent, "p3".into())
        .await
        .unwrap()
        .dispatch();
    // Deleting an installed row writes a tombstone one lamport above it.
    let tombstone = installation
        .delete(
            1,
            WriteBasis::CompareAndSet(Some(collection.get(&1).unwrap().stamp)),
        )
        .await
        .unwrap()
        .dispatch();
    assert_eq!(
        tombstone,
        Stamp {
            lamport: 2,
            writer: writer_id("a")
        }
    );
    assert_eq!(winners(&collection), [(2, "a2".into()), (3, "p3".into())]);
    let before = collection.snapshot();
    drop((installation, permanent, collection));
    drop(keep);

    let (keep, collection) = reopen().await;
    assert_eq!(collection.snapshot(), before);
    // A tombstone also came back: creating over it with `CompareAndSet(None)` outranks it.
    let created = collection
        .installed_writer(b"a")
        .unwrap()
        .put(1, WriteBasis::CompareAndSet(None), "again".into())
        .await
        .unwrap()
        .dispatch();
    assert_eq!(created.lamport, 3);

    collection.uninstall(b"a").await.unwrap().dispatch();
    assert_eq!(winners(&collection), [(3, "p3".into())]);
    assert!(matches!(
        collection.installed_writer(b"a"),
        Err(MutationError::Closed)
    ));
    drop(collection);
    drop(keep);

    let (_keep, collection) = reopen().await;
    assert_eq!(winners(&collection), [(3, "p3".into())]);
}

#[test]
fn memory_store_runtime_writes_survive_reload() {
    let store = Arc::new(MemoryStore::new());
    ready(runtime_writes_survive_reload_and_uninstall_retracts_them(
        || async { ((), open(&store).await) },
    ));
}

#[tokio::test]
async fn sqlite_store_runtime_writes_survive_reload() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("clay.sqlite");
    runtime_writes_survive_reload_and_uninstall_retracts_them(|| async {
        let runtime = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default());
        let store = Arc::new(
            SqliteStore::open(runtime.database(&path, &[], "_unused"))
                .await
                .unwrap(),
        );
        let collection = open(&store).await;
        (runtime, collection)
    })
    .await;
}

#[test]
fn failed_runtime_write_changes_nothing_visible_and_leaves_no_stamp_gap() {
    let store = Arc::new(FailingStore {
        inner: MemoryStore::new(),
        armed: AtomicBool::new(false),
    });
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("one"))],
    ));
    let writer = collection.installed_writer(b"a").unwrap();
    let (mut feed, _) = Feed::open(&collection);
    let before = collection.snapshot();
    let stored = ready(stored_keys(&*store));

    store.armed.store(true, Ordering::SeqCst);
    for result in [
        ready(writer.put(1, WriteBasis::Concurrent, "lost".into())),
        ready(writer.delete(1, WriteBasis::Concurrent)),
        ready(writer.put(2, WriteBasis::Concurrent, "lost".into())),
    ] {
        assert!(matches!(result, Err(InstalledWriteError::Store(_))));
    }
    assert_eq!(collection.snapshot(), before);
    assert_eq!(ready(stored_keys(&*store)), stored);
    feed.assert_quiet();
    // The failed attempts released their key slots and burned no lamport.
    store.armed.store(false, Ordering::SeqCst);
    let stamp = ready(writer.put(1, WriteBasis::Concurrent, "kept".into()))
        .unwrap()
        .dispatch();
    assert_eq!(stamp.lamport, 2);
}

/// Yields once before every `apply`, so concurrently polled writers interleave at the
/// store write, then runs the one-shot hook.
struct InterleavingStore {
    inner: MemoryStore,
    hook: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl OrderedStore for InterleavingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(key).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        self.inner.scan_prefix(prefix).await
    }

    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError> {
        tokio::task::yield_now().await;
        if let Some(hook) = self.hook.lock().unwrap().take() {
            hook();
        }
        self.inner.apply(batch).await
    }
}

#[tokio::test]
async fn concurrent_compare_and_set_on_durable_scopes_admits_exactly_one() {
    let store = Arc::new(InterleavingStore {
        inner: MemoryStore::new(),
        hook: std::sync::Mutex::new(None),
    });
    let collection = open(&store).await;
    install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("seed"))],
    )
    .await;
    install(&collection, b"b", Retention::Installation, &[]).await;
    install(&collection, b"p", Retention::Permanent, &[]).await;
    let first = collection.get(&1).unwrap().stamp;
    let writers = [b"a".as_slice(), b"b", b"p", b"a"]
        .map(|scope| collection.installed_writer(scope).unwrap());
    let attempt = |index: usize| {
        writers[index].put(
            1,
            WriteBasis::CompareAndSet(Some(first.clone())),
            format!("contender {index}"),
        )
    };
    let outcomes = tokio::join!(attempt(0), attempt(1), attempt(2), attempt(3));
    let outcomes = [outcomes.0, outcomes.1, outcomes.2, outcomes.3];
    let winners_: Vec<Stamp> = outcomes
        .iter()
        .filter_map(|outcome| {
            outcome
                .as_ref()
                .ok()
                .map(|dispatch| dispatch.output().clone())
        })
        .collect();
    assert_eq!(winners_.len(), 1);
    for outcome in outcomes {
        match outcome {
            Ok(dispatch) => {
                dispatch.dispatch();
            }
            Err(InstalledWriteError::Mutation(MutationError::Conflict { current })) => {
                assert_eq!(current.as_ref(), Some(&winners_[0]));
            }
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(collection.get(&1).unwrap().stamp, winners_[0]);
    // Exactly the winner's row is stored for the key beyond the seeded one.
    let reloaded = open(&store).await;
    assert_eq!(reloaded.snapshot(), collection.snapshot());
}

#[tokio::test]
async fn compare_and_set_beaten_by_an_incarnation_write_restores_the_stored_row() {
    let store = Arc::new(InterleavingStore {
        inner: MemoryStore::new(),
        hook: std::sync::Mutex::new(None),
    });
    let collection = open(&store).await;
    install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("installed"))],
    )
    .await;
    let first = collection.get(&1).unwrap().stamp;
    let writer = collection.installed_writer(b"a").unwrap();
    let scope = collection.create_scope(writer_id("runtime"));
    let incarnation = scope.writer().unwrap();
    let observed = first.clone();
    // Lands while the durable write is parked at the store.
    *store.hook.lock().unwrap() = Some(Box::new(move || {
        incarnation
            .put(1, WriteBasis::Observed(observed), "runtime".into())
            .unwrap()
            .dispatch();
    }));
    let result = writer
        .put(1, WriteBasis::CompareAndSet(Some(first)), "durable".into())
        .await;
    let Err(InstalledWriteError::Mutation(MutationError::Conflict { current })) = result else {
        panic!("expected conflict")
    };
    assert_eq!(current, Some(collection.get(&1).unwrap().stamp));
    assert_eq!(
        collection.get(&1).unwrap().value.as_deref(),
        Some("runtime")
    );
    // The store still holds the installed row, not the refused write.
    scope.close().dispatch();
    let reloaded = open(&store).await;
    assert_eq!(winners(&reloaded), [(1, "installed".into())]);
    assert_eq!(reloaded.get(&1).unwrap().stamp.lamport, 1);
}

#[test]
fn reinstall_keeps_unchanged_stamps_and_advances_changed_ones() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[(1, Some("x")), (2, Some("y")), (3, None)],
    ));
    let lamports = |collection: &Collection| {
        let inner = collection.lock();
        inner
            .candidates
            .iter()
            .map(|(key, holders)| (*key, holders.values().next().unwrap().stamp.lamport))
            .collect::<Vec<_>>()
    };
    assert_eq!(lamports(&collection), [(1, 1), (2, 1), (3, 1)]);
    let observed = collection.get(&2).unwrap().stamp;

    ready(install(
        &collection,
        b"a",
        Retention::Installation,
        &[
            (1, Some("x")),
            (2, Some("y2")),
            (3, Some("z")),
            (4, Some("new")),
        ],
    ));
    // 1 unchanged, 2 changed, 3 tombstone to value, 4 created concurrently.
    assert_eq!(lamports(&collection), [(1, 1), (2, 2), (3, 2), (4, 1)]);

    // A write that observed the old row at lamport 1 no longer outranks the update.
    let scope = collection.create_scope(writer_id("0"));
    scope
        .writer()
        .unwrap()
        .put(2, WriteBasis::Observed(observed), "stale".into())
        .unwrap()
        .dispatch();
    assert_eq!(collection.get(&2).unwrap().value.as_deref(), Some("y2"));
    // The stored rows reload with the same stamps.
    assert_eq!(ready(open(&store)).snapshot(), collection.snapshot());
}

proptest! {
    #[test]
    fn store_keys_are_injective_and_prefix_free(
        a in (prop::collection::vec(any::<u8>(), 0..4), prop::collection::vec(any::<u8>(), 0..4), prop::collection::vec(any::<u8>(), 0..4)),
        b in (prop::collection::vec(any::<u8>(), 0..4), prop::collection::vec(any::<u8>(), 0..4), prop::collection::vec(any::<u8>(), 0..4)),
    ) {
        let key = |(collection, scope, local): &(Vec<u8>, Vec<u8>, Vec<u8>)| {
            row_key(&scope_prefix(collection, scope), local)
        };
        let (key_a, key_b) = (key(&a), key(&b));
        prop_assert_eq!(key_a == key_b, a == b);
        // A collection range or a scope range contains a key only if the identity matches.
        prop_assert_eq!(
            key_b.starts_with(&collection_prefix(&a.0)),
            a.0 == b.0
        );
        prop_assert_eq!(
            key_b.starts_with(&scope_prefix(&a.0, &a.1)),
            a.0 == b.0 && a.1 == b.1
        );
        // And the scope and local key decode back out.
        let prefix_len = collection_prefix(&b.0).len();
        let (scope, local) = split_row_key(prefix_len, &key_b);
        prop_assert_eq!((scope, local), (b.1.as_slice(), b.2.as_slice()));
    }

    #[test]
    fn stored_candidates_round_trip_with_stamp_writer_and_retention(
        lamport in any::<u64>(),
        writer in prop::collection::vec(any::<u8>(), 0..8),
        slot in prop::option::of("[a-z]{0,6}"),
        permanent in any::<bool>(),
    ) {
        let retention = if permanent { Retention::Permanent } else { Retention::Installation };
        let candidate = Candidate {
            stamp: Stamp { lamport, writer: WriterId::new(&writer) },
            slot,
        };
        let (decoded_retention, decoded) = decode_row(codec(), &encode_row(codec(), retention, &candidate));
        prop_assert_eq!(decoded_retention, retention);
        prop_assert_eq!(decoded.stamp, candidate.stamp);
        prop_assert_eq!(decoded.slot, candidate.slot);
    }
}
