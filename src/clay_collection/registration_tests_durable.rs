use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use clay_store::BatchOp;
use clay_store::MemoryStore;
use clay_store::OrderedStore;
use clay_store::SqliteStore;
use clay_store::StoreError;
use proptest::prelude::*;

use super::durable::collection_prefix;
use super::durable::row_key;
use super::durable::scope_prefix;
use super::durable::split_row_key;
use super::*;
use crate::types::Change;
use crate::types::Page;
use crate::types::ReadLimits;

type Collection = RegistrationCollection<u32, String>;

fn codec() -> DurableCodec<u32, String> {
    DurableCodec {
        encode_key: |key| key.to_be_bytes().to_vec(),
        decode_key: |bytes| u32::from_be_bytes(bytes.try_into().expect("u32 key")),
        encode_value: |value| value.as_bytes().to_vec(),
        decode_value: |bytes| String::from_utf8(bytes.to_vec()).expect("utf8 value"),
    }
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let Poll::Ready(value) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected a ready future");
    };
    value
}

fn rows(entries: &[(u32, &str)]) -> BTreeMap<u32, String> {
    entries
        .iter()
        .map(|(key, value)| (*key, (*value).to_owned()))
        .collect()
}

async fn open(store: &Arc<impl OrderedStore + Send + Sync + 'static>) -> Collection {
    Collection::new_durable(Arc::clone(store), b"toolbar".to_vec(), codec(), [1, 2])
        .await
        .unwrap()
}

/// Upserted `(local key, value)` pairs of a page, in change order.
fn upserts(page: &Page<(RegistrationScopeId, u32), String>) -> Vec<(u32, String)> {
    page.transactions
        .iter()
        .flat_map(|transaction| &transaction.changes)
        .filter_map(|change| match change {
            Change::Upsert { key, value, .. } => Some((key.1, value.clone())),
            Change::Remove { .. } => None,
        })
        .collect()
}

fn bootstrap(collection: &Collection) -> Page<(RegistrationScopeId, u32), String> {
    let mut lease = collection.projection().acquire((), None).unwrap();
    ready(lease.read(None, ReadLimits::default())).unwrap()
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

#[tokio::test]
async fn installed_rows_survive_reopen_with_bootstrap_complete() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("clay.sqlite");
    {
        let runtime = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default());
        let store = Arc::new(
            SqliteStore::open(runtime.database(&path, &[], "_unused"))
                .await
                .unwrap(),
        );
        let collection = open(&store).await;
        collection
            .install(b"micro-a", rows(&[(1, "one"), (2, "two")]))
            .await
            .unwrap()
            .dispatch();
        collection
            .install(b"micro-b", rows(&[(1, "other")]))
            .await
            .unwrap()
            .dispatch();
    }
    let runtime = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default());
    let store = Arc::new(
        SqliteStore::open(runtime.database(&path, &[], "_unused"))
            .await
            .unwrap(),
    );
    let collection = open(&store).await;
    let page = bootstrap(&collection);
    assert!(page.bootstrap_complete);
    let mut loaded = upserts(&page);
    loaded.sort();
    assert_eq!(
        loaded,
        [(1, "one".into()), (1, "other".into()), (2, "two".into())]
    );
}

#[test]
fn untouched_durable_collection_bootstraps_known_empty() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    let page = bootstrap(&collection);
    assert!(page.bootstrap_complete);
    assert!(upserts(&page).is_empty());
    assert!(collection.snapshot().is_empty());
}

#[test]
fn reinstall_replaces_the_scope_in_one_transaction() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(collection.install(b"a", rows(&[(1, "one"), (2, "two")])))
        .unwrap()
        .dispatch();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let first = ready(lease.read(None, ReadLimits::default())).unwrap();

    ready(collection.install(b"a", rows(&[(2, "two!"), (3, "three")])))
        .unwrap()
        .dispatch();
    let page = ready(lease.read(Some(first.through), ReadLimits::default())).unwrap();
    assert_eq!(page.transactions.len(), 1);
    let changes = &page.transactions[0].changes;
    let upserted: Vec<_> = changes
        .iter()
        .filter_map(|change| match change {
            Change::Upsert { key, value, .. } => Some((key.1, value.as_str())),
            Change::Remove { .. } => None,
        })
        .collect();
    let removed: Vec<_> = changes
        .iter()
        .filter_map(|change| match change {
            Change::Remove { key, .. } => Some(key.1),
            Change::Upsert { .. } => None,
        })
        .collect();
    assert_eq!(upserted, [(2, "two!"), (3, "three")]);
    assert_eq!(removed, [1]);
    assert_eq!(ready(stored_keys(&*store)).len(), 2);
}

#[test]
fn uninstall_removes_only_its_scope() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(collection.install(b"a", rows(&[(1, "a1"), (2, "a2")])))
        .unwrap()
        .dispatch();
    ready(collection.install(b"b", rows(&[(1, "b1")])))
        .unwrap()
        .dispatch();
    let incarnation = collection.create_scope();
    incarnation
        .writer()
        .unwrap()
        .put(9, None, "volatile".into())
        .unwrap()
        .dispatch();

    ready(collection.uninstall(b"a")).unwrap().dispatch();
    let mut remaining: Vec<_> = collection
        .snapshot()
        .into_values()
        .map(|entry| entry.value)
        .collect();
    remaining.sort();
    assert_eq!(remaining, ["b1", "volatile"]);
    let mut loaded = upserts(&bootstrap(&collection));
    loaded.sort();
    assert_eq!(loaded, [(1, "b1".into()), (9, "volatile".into())]);
    assert_eq!(ready(stored_keys(&*store)).len(), 1);
    // Uninstalling an unknown scope is a no-op.
    ready(collection.uninstall(b"never")).unwrap().dispatch();
    assert_eq!(ready(stored_keys(&*store)).len(), 1);
}

#[test]
fn incarnation_scopes_are_never_persisted() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    let scope = collection.create_scope();
    let writer = scope.writer().unwrap();
    let version = writer.put(1, None, "v".into()).unwrap().dispatch();
    assert!(ready(stored_keys(&*store)).is_empty());
    ready(collection.install(b"a", rows(&[(2, "kept")])))
        .unwrap()
        .dispatch();
    writer.remove(1, Some(version)).unwrap().dispatch();
    writer.put(3, None, "w".into()).unwrap().dispatch();
    writer.clear().unwrap().dispatch();
    scope.close().dispatch();
    ready(collection.uninstall(b"a")).unwrap().dispatch();
    assert!(ready(stored_keys(&*store)).is_empty());
    // Only the installation scope's rows ever reached the store.
    let reopened = ready(open(&store));
    assert!(reopened.snapshot().is_empty());
}

#[test]
fn failed_store_apply_changes_nothing_visible() {
    let store = Arc::new(FailingStore {
        inner: MemoryStore::new(),
        armed: AtomicBool::new(false),
    });
    let collection = ready(open(&store));
    ready(collection.install(b"a", rows(&[(1, "one")])))
        .unwrap()
        .dispatch();
    let mut lease = collection.projection().acquire((), None).unwrap();
    let first = ready(lease.read(None, ReadLimits::default())).unwrap();
    let before = collection.snapshot();
    let stored = ready(stored_keys(&*store));

    store.armed.store(true, Ordering::SeqCst);
    assert!(ready(collection.install(b"a", rows(&[(2, "two")]))).is_err());
    assert!(ready(collection.install(b"new", rows(&[(5, "five")]))).is_err());
    assert!(ready(collection.uninstall(b"a")).is_err());

    assert_eq!(collection.snapshot(), before);
    assert_eq!(ready(stored_keys(&*store)), stored);
    assert_eq!(upserts(&bootstrap(&collection)), [(1, "one".into())]);
    // The open lease sees no new transaction: a read would park, so poll it once.
    let mut read = std::pin::pin!(lease.read(Some(first.through), ReadLimits::default()));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

#[test]
fn reset_keeps_installation_scopes_and_drops_incarnations() {
    let store = Arc::new(MemoryStore::new());
    let collection = ready(open(&store));
    ready(collection.install(b"a", rows(&[(1, "one")])))
        .unwrap()
        .dispatch();
    let scope = collection.create_scope();
    scope
        .writer()
        .unwrap()
        .put(2, None, "volatile".into())
        .unwrap()
        .dispatch();
    collection.reset().dispatch();
    let page = bootstrap(&collection);
    assert!(page.bootstrap_complete);
    assert_eq!(upserts(&page), [(1, "one".into())]);
    assert_eq!(collection.snapshot().len(), 1);
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
}
