//! Tests for the SQLite store.
//!
//! The shared contract suite does the heavy lifting; everything here is either a property the
//! suite cannot see from the trait (schema invariants, scope partitioning, the query plan) or a
//! regression guard for a specific encoding decision.

use std::sync::Arc;

use sqlx::Row;
use sqlx_utils_rs::SqlCtx;
use willow25::prelude::*;

use super::{SqliteWillowStore, queries};
use crate::path_codec::encode_path;
use crate::store::contract::contract_suite;
use crate::store::{AreaReadLimits, InsertOutcome, WillowStore};

/// A namespace and subspace to author entries in, mirroring the contract suite's helper.
struct Author {
    namespace_id: NamespaceId,
    subspace_id: SubspaceId,
    capability: WriteCapability,
    secret: SubspaceSecret,
}

impl Author {
    fn new(namespace_byte: u8, subspace_byte: u8) -> Self {
        // `NamespaceId::is_communal` is "the least significant bit of the id is zero", and this
        // fixture builds a communal capability. An odd last byte would make an owned namespace,
        // which `WriteCapability::new_communal` still accepts but `WriteCapability`'s decoder
        // rejects, so such an entry could be held in memory but never read back from storage.
        let mut namespace_bytes = [namespace_byte; NAMESPACE_ID_WIDTH];
        namespace_bytes[NAMESPACE_ID_WIDTH - 1] = 0;
        let namespace_id = NamespaceId::from_bytes(&namespace_bytes);
        assert!(
            namespace_id.is_communal(),
            "the fixture pairs a communal capability with its namespace",
        );

        // 32 is the ed25519 secret key length, which `willow25` does not re-export.
        let secret = SubspaceSecret::from_bytes(&[subspace_byte; 32]);
        let subspace_id = secret.corresponding_subspace_id();
        let capability = WriteCapability::new_communal(namespace_id.clone(), subspace_id.clone());

        Self {
            namespace_id,
            subspace_id,
            capability,
            secret,
        }
    }

    fn path(&self, components: &[&[u8]]) -> Path {
        Path::from_slices(components).expect("test paths are valid")
    }

    fn area(&self) -> Area {
        Area::new_subspace_area(self.subspace_id.clone())
    }

    fn entry(&self, path: &Path, timestamp: u64, payload: &[u8]) -> AuthorisedEntry {
        Entry::builder()
            .namespace_id(self.namespace_id.clone())
            .subspace_id(self.subspace_id.clone())
            .path(path.clone())
            .timestamp(timestamp)
            .payload_length(payload.len() as u64)
            .payload_digest(PayloadDigest::from_payload(payload))
            .build()
            .into_authorised_entry(&self.capability, &self.secret)
            .expect("a communal capability authorises its own subspace")
    }
}

async fn store(scope_key: &str) -> SqliteWillowStore {
    let ctx = SqlCtx::memory()
        .await
        .expect("an ephemeral sqlite database opens");
    SqliteWillowStore::new(ctx, scope_key)
        .await
        .expect("the store opens")
}

async fn insert(
    store: &SqliteWillowStore,
    author: &Author,
    path: &Path,
    timestamp: u64,
    payload: &[u8],
) {
    store
        .insert_entry_with_payload(author.entry(path, timestamp, payload), payload)
        .await
        .expect("insert succeeds");
}

async fn stored(
    store: &SqliteWillowStore,
    author: &Author,
    path: &Path,
) -> Option<AuthorisedEntry> {
    store
        .get_entry(&author.namespace_id, &author.subspace_id, path)
        .await
        .expect("get_entry succeeds")
}

async fn orphan_payloads(store: &SqliteWillowStore) -> i64 {
    sqlx::query_scalar(queries::COUNT_ORPHAN_PAYLOADS)
        .bind(store.scope_id)
        .fetch_one(&store.ctx.read_pool)
        .await
        .expect("the orphan count runs")
}

#[tokio::test]
async fn sqlite_store_satisfies_the_store_contract() {
    let store: Arc<dyn WillowStore> = Arc::new(store("contract").await);
    contract_suite(store).await;
}

/// The shared suite authors communal namespaces. ADR section 3 makes the *node* namespace owned,
/// with a write capability minted from the namespace secret, so this pins the production shape
/// end to end: an owned namespace must survive the token round trip through storage.
#[tokio::test]
async fn an_owned_namespace_entry_round_trips() {
    let store = store("owned-namespace").await;

    let mut csprng = rand_core::OsRng;
    let (namespace_id, namespace_secret) = randomly_generate_owned_namespace(&mut csprng);
    assert!(namespace_id.is_owned());

    let secret = SubspaceSecret::from_bytes(&[7; 32]);
    let subspace_id = secret.corresponding_subspace_id();
    let capability = WriteCapability::new_owned(&namespace_secret, subspace_id.clone());

    let path = Path::from_slices(&[b"a"]).expect("the test path is valid");
    let entry = Entry::builder()
        .namespace_id(namespace_id.clone())
        .subspace_id(subspace_id.clone())
        .path(path.clone())
        .timestamp(10)
        .payload_length(7)
        .payload_digest(PayloadDigest::from_payload(b"payload"))
        .build()
        .into_authorised_entry(&capability, &secret)
        .expect("the namespace secret authorises its own subspace");

    store
        .insert_entry_with_payload(entry.clone(), b"payload")
        .await
        .expect("insert succeeds");

    assert!(
        store
            .get_entry(&namespace_id, &subspace_id, &path)
            .await
            .expect("get_entry succeeds")
            == Some(entry)
    );
    assert!(
        store
            .get_payload(&namespace_id, &subspace_id, &path)
            .await
            .expect("get_payload succeeds")
            == Some(b"payload".to_vec())
    );
}

#[tokio::test]
async fn sqlite_store_is_send_sync_and_object_safe() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<SqliteWillowStore>();

    let store: Arc<dyn WillowStore> = Arc::new(store("object-safe").await);
    let shared: Arc<dyn WillowStore> = Arc::clone(&store);
    assert!(Arc::ptr_eq(&store, &shared));
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_store_can_be_driven_from_another_thread() -> Result<(), tokio::task::JoinError> {
    let store: Arc<dyn WillowStore> = Arc::new(store("threads").await);

    tokio::spawn({
        let store = Arc::clone(&store);
        async move { store.flush().await.expect("flush succeeds") }
    })
    .await
}

/// The read order must be `Path`'s own order, which is component-wise: `["a","b"]` sorts before
/// `["ab"]`, and `["ab"]` sorts before `["b"]`. Concatenating components makes the first pair
/// collide, and prefixing each component with its length inverts the second pair, so a store
/// keyed either way fails this.
#[tokio::test]
async fn the_read_order_follows_path_order_not_a_naive_byte_encoding() {
    let store = store("ordering").await;
    let author = Author::new(1, 1);

    // No path here is a prefix of another, so none of them prunes any other.
    let paths = [
        author.path(&[b"ab"]),
        author.path(&[b"a", b"b"]),
        author.path(&[b"b"]),
        author.path(&[b"a", b"", b"c"]),
        author.path(&[b""]),
    ];

    for (index, path) in paths.iter().enumerate() {
        let payload = [index as u8];
        insert(&store, &author, path, 10, &payload).await;
    }

    let mut expected = paths.to_vec();
    expected.sort();

    let page = store
        .read_area(
            &author.namespace_id,
            &author.area(),
            None,
            AreaReadLimits::default(),
        )
        .await
        .expect("read_area succeeds");
    let actual: Vec<Path> = page.entries.iter().map(|e| e.path().clone()).collect();

    assert!(
        actual == expected,
        "read order {actual:?} is not the path order {expected:?}"
    );
}

#[tokio::test]
async fn pruning_removes_exactly_the_descendants() {
    let store = store("pruning").await;
    let author = Author::new(2, 1);

    let deep = author.path(&[b"a", b"b", b"c"]);
    let empty_component = author.path(&[b"a", b""]);
    let sibling = author.path(&[b"ab"]);
    let top = author.path(&[b"a"]);

    insert(&store, &author, &deep, 10, b"deep").await;
    insert(&store, &author, &empty_component, 11, b"empty").await;
    insert(&store, &author, &sibling, 12, b"sibling").await;

    // `/a` is a prefix of `/a/b/c` and of `/a/<empty>`, but not of `/ab`, which merely shares a
    // leading byte with the component `a`.
    let outcome = store
        .insert_entry_with_payload(author.entry(&top, 20, b"top"), b"top")
        .await
        .expect("insert succeeds");
    assert!(outcome == InsertOutcome::Inserted { pruned: 2 });

    assert!(stored(&store, &author, &top).await.is_some());
    assert!(stored(&store, &author, &deep).await.is_none());
    assert!(stored(&store, &author, &empty_component).await.is_none());
    assert!(
        stored(&store, &author, &sibling).await.is_some(),
        "a sibling sharing a leading byte is not a descendant"
    );
}

#[tokio::test]
async fn a_payload_never_outlives_its_entry() {
    let store = store("orphans").await;
    let author = Author::new(3, 1);
    let path = author.path(&[b"a"]);

    insert(&store, &author, &path, 10, b"payload").await;
    assert!(orphan_payloads(&store).await == 0);

    // Forgetting removes the entry, and the cascade must take its payload.
    assert!(
        store
            .forget_entry(&author.namespace_id, &author.subspace_id, &path)
            .await
            .expect("forget_entry succeeds")
    );
    assert!(orphan_payloads(&store).await == 0);
    assert!(
        store
            .get_payload(&author.namespace_id, &author.subspace_id, &path)
            .await
            .expect("get_payload succeeds")
            .is_none()
    );

    // Pruning is the other way a payload can lose its entry.
    let deep = author.path(&[b"b", b"c"]);
    insert(&store, &author, &deep, 10, b"payload").await;
    insert(&store, &author, &author.path(&[b"b"]), 20, b"top").await;
    assert!(stored(&store, &author, &deep).await.is_none());
    assert!(orphan_payloads(&store).await == 0);
}

#[tokio::test]
async fn a_metadata_only_insert_records_no_payload() {
    let store = store("metadata-only").await;
    let author = Author::new(4, 1);
    let path = author.path(&[b"a"]);

    store
        .insert_entry(author.entry(&path, 10, b"payload"))
        .await
        .expect("insert_entry succeeds");

    assert!(stored(&store, &author, &path).await.is_some());
    assert!(
        store
            .get_payload(&author.namespace_id, &author.subspace_id, &path)
            .await
            .expect("get_payload succeeds")
            .is_none()
    );
    assert!(orphan_payloads(&store).await == 0);
}

#[tokio::test]
async fn two_scopes_in_one_database_do_not_interfere() {
    let ctx = SqlCtx::memory()
        .await
        .expect("an ephemeral sqlite database opens");
    let first = SqliteWillowStore::new(ctx.clone(), "first")
        .await
        .expect("the first scope opens");
    let second = SqliteWillowStore::new(ctx, "second")
        .await
        .expect("the second scope opens");

    let author = Author::new(5, 1);
    let path = author.path(&[b"a"]);

    insert(&first, &author, &path, 10, b"first").await;
    assert!(stored(&first, &author, &path).await.is_some());
    assert!(stored(&second, &author, &path).await.is_none());

    // The same key in the other scope is independent, including for pruning and forgetting.
    insert(&second, &author, &path, 20, b"second").await;
    assert!(
        second
            .forget_entry(&author.namespace_id, &author.subspace_id, &path)
            .await
            .expect("forget_entry succeeds")
    );
    assert!(stored(&first, &author, &path).await.is_some());
    assert!(
        second
            .read_area(
                &author.namespace_id,
                &author.area(),
                None,
                AreaReadLimits::default()
            )
            .await
            .expect("read_area succeeds")
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn the_recency_columns_agree_with_the_stored_entry() {
    let store = store("recency").await;
    let author = Author::new(6, 1);
    let path = author.path(&[b"a"]);
    let entry = author.entry(&path, 10, b"payload");

    store
        .insert_entry_with_payload(entry.clone(), b"payload")
        .await
        .expect("insert succeeds");

    let row = sqlx::query(queries::SELECT_DENORMALISED)
        .bind(store.scope_id)
        .bind(author.namespace_id.as_bytes().to_vec())
        .bind(author.subspace_id.as_bytes().to_vec())
        .bind(encode_path(&path))
        .fetch_one(&store.ctx.read_pool)
        .await
        .expect("the row exists");

    assert!(super::u64_from_bytes(&row.get::<Vec<u8>, _>("timestamp")) == u64::from(entry.timestamp()));
    assert!(row.get::<Vec<u8>, _>("payload_digest") == entry.payload_digest().as_bytes().to_vec());
    assert!(
        super::u64_from_bytes(&row.get::<Vec<u8>, _>("payload_length")) == entry.payload_length()
    );
    assert!(super::decode_authorised_entry(&row.get::<Vec<u8>, _>("entry")).await == entry);
}

/// A timestamp above `i64::MAX` must still be newer than an ordinary one, in both the pruning
/// predicate and the time bounds. Storing these as INTEGER columns would wrap them negative and
/// invert every comparison.
#[tokio::test]
async fn timestamps_order_as_unsigned_integers() {
    let store = store("unsigned").await;
    let author = Author::new(7, 1);
    let deep = author.path(&[b"a", b"b"]);
    let top = author.path(&[b"a"]);
    let huge = u64::MAX;

    insert(&store, &author, &deep, 10, b"old").await;

    // Pruning compares the new timestamp against the stored one.
    let outcome = store
        .insert_entry_with_payload(author.entry(&top, huge, b"new"), b"new")
        .await
        .expect("insert succeeds");
    assert!(outcome == InsertOutcome::Inserted { pruned: 1 });
    assert!(stored(&store, &author, &deep).await.is_none());
    assert!(
        stored(&store, &author, &top)
            .await
            .expect("the entry is stored")
            .timestamp()
            == Timestamp::from(huge)
    );

    // The lower time bound is inclusive, so it selects the entry at exactly `huge`.
    let at_huge = Area::new(
        Some(author.subspace_id.clone()),
        Path::new(),
        TimeRange::new_open(Timestamp::from(huge)),
    );
    assert!(
        store
            .read_area(
                &author.namespace_id,
                &at_huge,
                None,
                AreaReadLimits::default()
            )
            .await
            .expect("read_area succeeds")
            .entries
            .len()
            == 1
    );

    // The upper bound is exclusive, so a window ending at `huge` excludes it.
    let below_huge = Area::new(
        Some(author.subspace_id.clone()),
        Path::new(),
        TimeRange::new_closed(Timestamp::from(0), Timestamp::from(huge)),
    );
    assert!(
        store
            .read_area(
                &author.namespace_id,
                &below_huge,
                None,
                AreaReadLimits::default()
            )
            .await
            .expect("read_area succeeds")
            .entries
            .is_empty()
    );
}

/// The plan query must be exactly the query it explains, or the plan a test checks stops
/// describing what the store runs.
#[test]
fn the_explain_files_quote_the_queries_they_explain() {
    for (explain, query) in [
        (
            queries::EXPLAIN_READ_AREA_IN_SUBSPACE,
            queries::READ_AREA_IN_SUBSPACE,
        ),
        (
            queries::EXPLAIN_READ_AREA_ANY_SUBSPACE,
            queries::READ_AREA_ANY_SUBSPACE,
        ),
    ] {
        assert!(
            explain.strip_prefix(queries::EXPLAIN_QUERY_PLAN) == Some(query),
            "the plan query is not its query prefixed with the plan keyword",
        );
    }
}

/// An area read seeks to `(scope, namespace[, subspace])` and walks the primary key in order, so
/// it never needs a sort.
#[tokio::test]
async fn the_area_read_walks_the_primary_key_without_sorting() {
    let store = store("plan").await;

    for (sql, spans_subspaces) in [
        (queries::EXPLAIN_READ_AREA_IN_SUBSPACE, false),
        (queries::EXPLAIN_READ_AREA_ANY_SUBSPACE, true),
    ] {
        let mut query = sqlx::query(sql)
            .bind(store.scope_id)
            .bind(vec![1u8; NAMESPACE_ID_WIDTH]);
        query = if spans_subspaces {
            query.bind(Option::<Vec<u8>>::None)
        } else {
            query.bind(vec![1u8; SUBSPACE_ID_WIDTH])
        };

        let rows = query
            .bind(Option::<Vec<u8>>::None)
            .bind(Option::<Vec<u8>>::None)
            .bind(Option::<Vec<u8>>::None)
            .bind(vec![0u8; 8])
            .bind(Option::<Vec<u8>>::None)
            .bind(64i64)
            .fetch_all(&store.ctx.read_pool)
            .await
            .expect("the plan query runs");

        let details: Vec<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
        let plan = details.join("\n");

        assert!(
            plan.contains("PRIMARY KEY"),
            "the read must seek the primary key, got: {plan}"
        );
        assert!(
            !plan.contains("TEMP B-TREE"),
            "the read must not sort, got: {plan}"
        );
    }
}

