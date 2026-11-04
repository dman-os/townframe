//! One backend-agnostic contract, written as generic async functions over
//! [`OrderedStore`] and instantiated for every backend that can run natively.
//! `IndexedDbStore` is only compiled for `wasm32-unknown-unknown` in this
//! checkpoint; it has no browser harness yet.

use crate::BatchOp;
use crate::MemoryStore;
use crate::OrderedStore;
use crate::SqliteStore;
use crate::Subspace;
use crate::prefix_successor;
use sqlite_rs::Database;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;
use std::sync::Arc;

fn put(key: &[u8], value: &[u8]) -> BatchOp {
    BatchOp::Put(key.to_vec(), value.to_vec())
}

fn entries(pairs: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}

async fn put_all(store: &impl OrderedStore, keys: &[&[u8]]) {
    // The value echoes the key so a scan result shows which key it came from.
    let batch = keys.iter().map(|key| put(key, key)).collect();
    store.apply(batch).await.unwrap();
}

async fn scanned_keys(store: &impl OrderedStore, prefix: &[u8]) -> Vec<Vec<u8>> {
    store
        .scan_prefix(prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

mod contract {
    use super::*;

    pub(super) async fn point_reads_and_overwrites(store: &impl OrderedStore) {
        assert_eq!(store.get(b"k").await.unwrap(), None);
        store.apply(vec![put(b"k", b"v1")]).await.unwrap();
        assert_eq!(store.get(b"k").await.unwrap(), Some(b"v1".to_vec()));
        store.apply(vec![put(b"k", b"v2")]).await.unwrap();
        assert_eq!(store.get(b"k").await.unwrap(), Some(b"v2".to_vec()));
        // An empty value and an empty key are real entries, not absence.
        store
            .apply(vec![put(b"e", b""), put(b"", b"root")])
            .await
            .unwrap();
        assert_eq!(store.get(b"e").await.unwrap(), Some(Vec::new()));
        assert_eq!(store.get(b"").await.unwrap(), Some(b"root".to_vec()));
        store
            .apply(vec![
                BatchOp::Delete(b"k".to_vec()),
                BatchOp::Delete(b"absent".to_vec()),
            ])
            .await
            .unwrap();
        assert_eq!(store.get(b"k").await.unwrap(), None);
    }

    pub(super) async fn scan_orders_bytewise(store: &impl OrderedStore) {
        let ascending: Vec<&[u8]> = vec![
            b"",
            &[0],
            &[0, 0],
            &[0, 1],
            &[1],
            &[0x7F],
            &[0x80],
            &[0xFE, 0xFF],
            &[0xFF],
            &[0xFF, 0],
            &[0xFF, 0xFF],
        ];
        // Insert in a scrambled order so ordering cannot come from insertion.
        let scrambled: Vec<&[u8]> = [7, 2, 10, 0, 5, 9, 3, 1, 8, 4, 6]
            .iter()
            .map(|index| ascending[*index])
            .collect();
        put_all(store, &scrambled).await;
        let expected: Vec<Vec<u8>> = ascending.iter().map(|key| key.to_vec()).collect();
        assert_eq!(scanned_keys(store, b"").await, expected);
    }

    pub(super) async fn scan_prefix_boundaries(store: &impl OrderedStore) {
        put_all(
            store,
            &[
                &[1],
                &[1, 1],
                &[1, 2],
                &[1, 2, 0],
                &[1, 2, 0xFF],
                &[1, 3],
                &[2],
            ],
        )
        .await;
        // The prefix itself matches; its neighbours on both sides do not.
        let expected: Vec<Vec<u8>> = vec![vec![1, 2], vec![1, 2, 0], vec![1, 2, 0xFF]];
        assert_eq!(scanned_keys(store, &[1, 2]).await, expected);
        assert_eq!(scanned_keys(store, &[9]).await, Vec::<Vec<u8>>::new());
        assert_eq!(
            store.scan_prefix(&[1, 2, 0xFF]).await.unwrap(),
            entries(&[(&[1, 2, 0xFF], &[1, 2, 0xFF])])
        );
    }

    pub(super) async fn scan_prefix_with_ff_bytes(store: &impl OrderedStore) {
        put_all(
            store,
            &[
                &[0xFE, 0xFF],
                &[0xFF],
                &[0xFF, 0xFF],
                &[0xFF, 0xFF, 1],
                &[1, 0xFE],
                &[1, 0xFF],
                &[1, 0xFF, 0],
                &[2],
            ],
        )
        .await;
        // All-0xFF prefixes have no successor: the range is open above.
        assert_eq!(
            scanned_keys(store, &[0xFF]).await,
            vec![vec![0xFF], vec![0xFF, 0xFF], vec![0xFF, 0xFF, 1]]
        );
        assert_eq!(
            scanned_keys(store, &[0xFF, 0xFF]).await,
            vec![vec![0xFF, 0xFF], vec![0xFF, 0xFF, 1]]
        );
        // A trailing 0xFF carries into the previous byte of the bound.
        assert_eq!(
            scanned_keys(store, &[1, 0xFF]).await,
            vec![vec![1, 0xFF], vec![1, 0xFF, 0]]
        );
    }

    pub(super) async fn empty_prefix_scans_everything(store: &impl OrderedStore) {
        assert!(store.scan_prefix(b"").await.unwrap().is_empty());
        put_all(store, &[b"b", b"a", b""]).await;
        assert_eq!(
            store.scan_prefix(b"").await.unwrap(),
            entries(&[(b"", b""), (b"a", b"a"), (b"b", b"b")])
        );
    }

    pub(super) async fn batch_ops_apply_in_order(store: &impl OrderedStore) {
        store
            .apply(vec![
                put(b"k", b"1"),
                put(b"k", b"2"),
                BatchOp::Delete(b"k".to_vec()),
                put(b"k", b"3"),
                put(b"p/a", b"a"),
                BatchOp::DeletePrefix(b"p/".to_vec()),
                put(b"p/b", b"b"),
            ])
            .await
            .unwrap();
        assert_eq!(
            store.scan_prefix(b"").await.unwrap(),
            entries(&[(b"k", b"3"), (b"p/b", b"b")])
        );
        store.apply(Vec::new()).await.unwrap();
        assert_eq!(store.scan_prefix(b"").await.unwrap().len(), 2);
    }

    pub(super) async fn delete_prefix_spares_siblings(store: &impl OrderedStore) {
        put_all(
            store,
            &[
                b"a",
                b"a/1",
                b"a/2",
                b"a0",
                b"aa",
                b"b/1",
                &[0xFF],
                &[0xFF, 1],
                &[0xFE],
            ],
        )
        .await;
        store
            .apply(vec![BatchOp::DeletePrefix(b"a/".to_vec())])
            .await
            .unwrap();
        // `a/` is not a prefix of `a`, `a0`, or `aa`; only `a/1` and `a/2` go.
        assert_eq!(
            scanned_keys(store, b"").await,
            vec![
                b"a".to_vec(),
                b"a0".to_vec(),
                b"aa".to_vec(),
                b"b/1".to_vec(),
                vec![0xFE],
                vec![0xFF],
                vec![0xFF, 1],
            ]
        );
        // The unbounded-above delete takes the prefix itself and its extensions only.
        store
            .apply(vec![BatchOp::DeletePrefix(vec![0xFF])])
            .await
            .unwrap();
        assert_eq!(scanned_keys(store, &[0xFE]).await, vec![vec![0xFE]],);
        assert_eq!(scanned_keys(store, b"").await.len(), 5);
        // The empty prefix clears everything.
        store
            .apply(vec![BatchOp::DeletePrefix(Vec::new())])
            .await
            .unwrap();
        assert!(store.scan_prefix(b"").await.unwrap().is_empty());
    }
}

/// Instantiates every contract function as its own test for one backend.
/// `$fresh` is an async fn returning `(store, guard)`; the guard keeps temp
/// files alive for the test's duration.
macro_rules! contract_suite {
    ($backend:ident, $fresh:path) => {
        mod $backend {
            use super::*;

            macro_rules! case {
                ($name:ident) => {
                    #[tokio::test]
                    async fn $name() {
                        let (store, _guard) = $fresh().await;
                        contract::$name(&store).await;
                    }
                };
            }

            case!(point_reads_and_overwrites);
            case!(scan_orders_bytewise);
            case!(scan_prefix_boundaries);
            case!(scan_prefix_with_ff_bytes);
            case!(empty_prefix_scans_everything);
            case!(batch_ops_apply_in_order);
            case!(delete_prefix_spares_siblings);
        }
    };
}

async fn fresh_memory() -> (MemoryStore, ()) {
    (MemoryStore::new(), ())
}

struct SqliteGuard {
    _directory: tempfile::TempDir,
    _runtime: SqliteRuntime,
}

fn sqlite_database(runtime: &SqliteRuntime, path: &std::path::Path) -> Database {
    runtime.database(path, &[], "_clay_store_test_unused")
}

async fn fresh_sqlite() -> (SqliteStore, SqliteGuard) {
    let directory = tempfile::tempdir().unwrap();
    let runtime = SqliteRuntime::new(RuntimeConfig::default());
    let db = sqlite_database(&runtime, &directory.path().join("store.sqlite"));
    let store = SqliteStore::open(db).await.unwrap();
    (
        store,
        SqliteGuard {
            _directory: directory,
            _runtime: runtime,
        },
    )
}

contract_suite!(memory, fresh_memory);
contract_suite!(sqlite, fresh_sqlite);

async fn fresh_memory_subspace() -> (Subspace<MemoryStore>, ()) {
    (Arc::new(MemoryStore::new()).subspace("ns"), ())
}

// The whole contract must hold inside a subspace too, including the empty-prefix cases.
contract_suite!(memory_subspace, fresh_memory_subspace);

mod subspace_specific {
    use super::*;

    #[tokio::test]
    async fn sibling_subspaces_and_the_root_are_disjoint() {
        let root = Arc::new(MemoryStore::new());
        // "a" and "ab" would collide under a separator-free prefix scheme; the length prefix
        // keeps one name from being a prefix of the other.
        let a = Arc::clone(&root).subspace("a");
        let ab = Arc::clone(&root).subspace("ab");
        put_all(&a, &[b"k", b"b", b""]).await;
        put_all(&ab, &[b"k", b"c"]).await;
        put_all(&*root, &[b"k"]).await;

        assert_eq!(
            scanned_keys(&a, b"").await,
            vec![b"".to_vec(), b"b".to_vec(), b"k".to_vec()]
        );
        assert_eq!(
            scanned_keys(&ab, b"").await,
            vec![b"c".to_vec(), b"k".to_vec()]
        );
        assert_eq!(a.get(b"c").await.unwrap(), None);
        assert_eq!(root.get(b"c").await.unwrap(), None);

        // Clearing one subspace leaves its sibling and the root untouched.
        a.apply(vec![BatchOp::DeletePrefix(Vec::new())])
            .await
            .unwrap();
        assert!(a.scan_prefix(b"").await.unwrap().is_empty());
        assert_eq!(scanned_keys(&ab, b"").await.len(), 2);
        assert_eq!(root.get(b"k").await.unwrap(), Some(b"k".to_vec()));
        // Two views over the same name share data.
        let again = Arc::clone(&root).subspace("ab");
        assert_eq!(again.get(b"k").await.unwrap(), Some(b"k".to_vec()));
    }
}

mod sqlite_specific {
    use super::*;

    #[tokio::test]
    async fn committed_batches_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite");
        {
            let runtime = SqliteRuntime::new(RuntimeConfig::default());
            let store = SqliteStore::open(sqlite_database(&runtime, &path))
                .await
                .unwrap();
            store
                .apply(vec![
                    put(b"s/1", b"one"),
                    put(b"s/2", b"two"),
                    put(b"t/1", b"x"),
                ])
                .await
                .unwrap();
            store
                .apply(vec![BatchOp::DeletePrefix(b"t/".to_vec())])
                .await
                .unwrap();
        }
        let runtime = SqliteRuntime::new(RuntimeConfig::default());
        // Reopening re-applies migrations over the existing file; it must not
        // clobber the data.
        let store = SqliteStore::open(sqlite_database(&runtime, &path))
            .await
            .unwrap();
        assert_eq!(
            store.scan_prefix(b"").await.unwrap(),
            entries(&[(b"s/1", b"one"), (b"s/2", b"two")])
        );
    }

    /// A batch that fails partway leaves no trace. SQLite cannot reject a blob
    /// put on its own, so a trigger on the store's table aborts the statement
    /// for one poisoned key; the ops before it in the batch have already run
    /// inside the transaction by then.
    #[tokio::test]
    async fn failed_batch_rolls_back_every_op() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = SqliteRuntime::new(RuntimeConfig::default());
        let db = sqlite_database(&runtime, &directory.path().join("store.sqlite"));
        let store = SqliteStore::open(db.clone()).await.unwrap();
        put_all(&store, &[b"keep/1", b"keep/2"]).await;
        db.execute_batch(
            "CREATE TRIGGER poison BEFORE INSERT ON clay_store_entry
             WHEN NEW.key = x'DEAD'
             BEGIN SELECT RAISE(ABORT, 'poisoned key'); END",
        )
        .await
        .unwrap();

        // `sqlite_rs` reports every `SqliteFailure` (this trigger's abort, but
        // also a full disk) as `InvalidQuery`, which the store treats as an
        // invariant break and panics on. The panic unwinds out of `apply` with
        // the transaction still open, which is the rollback path under test.
        let failing = store.clone();
        let result = tokio::spawn(async move {
            failing
                .apply(vec![
                    put(b"new", b"1"),
                    BatchOp::DeletePrefix(b"keep/".to_vec()),
                    put(b"keep/1", b"overwritten"),
                    put(&[0xDE, 0xAD], b"boom"),
                    put(b"after", b"1"),
                ])
                .await
        })
        .await;
        assert!(result.unwrap_err().is_panic());
        assert_eq!(
            store.scan_prefix(b"").await.unwrap(),
            entries(&[(b"keep/1", b"keep/1"), (b"keep/2", b"keep/2")])
        );
        // The store stays usable after the rollback.
        store.apply(vec![put(b"after", b"1")]).await.unwrap();
        assert_eq!(store.get(b"after").await.unwrap(), Some(b"1".to_vec()));
    }
}

#[test]
fn prefix_successor_carries_and_stops() {
    assert_eq!(prefix_successor(b""), None);
    assert_eq!(prefix_successor(&[0xFF]), None);
    assert_eq!(prefix_successor(&[0xFF, 0xFF]), None);
    assert_eq!(prefix_successor(&[1, 2]), Some(vec![1, 3]));
    assert_eq!(prefix_successor(&[1, 0xFF]), Some(vec![2]));
    assert_eq!(
        prefix_successor(&[1, 0xFE, 0xFF, 0xFF]),
        Some(vec![1, 0xFF])
    );
    assert_eq!(prefix_successor(&[0]), Some(vec![1]));
}
