//! Every behaviour a [`WillowStore`] must have.
//!
//! The suite is shared so that a persistent store runs exactly the same checks as the
//! in-memory reference implementation. Each check uses its own namespace, so one store can
//! run the whole suite.
//!
//! What it pins:
//!
//! - prefix pruning, that it is atomic, and that `Inserted::pruned` counts what it removed —
//!   including the non-strict tie rule and the no-op re-insert that the `store_pruning` corpus
//!   pins;
//! - `Outdated` for an entry that an existing entry prunes;
//! - subspace-scoped and namespace-scoped isolation;
//! - retained tombstones, read back as a stored entry whose payload is empty;
//! - payload validation on [`WillowStore::insert_entry_with_payload`], and payload retention
//!   across a metadata-only [`WillowStore::insert_entry`] of an unchanged entry;
//! - area selection by subspace, path prefix, and time range, including a cursor bound;
//! - the paging contract: `resume_after` is exclusive and [`AreaPage::next`] is `None`
//!   exactly when the query is drained;
//! - local-only forgetting;
//! - agreement between the store and the `willow25::Store` adapter.

use std::num::NonZeroUsize;
use std::sync::Arc;

use ufotofu::prelude::*;
use willow25::prelude::*;
use willow25::storage::NondestructiveInsert;

use crate::store::{
    AreaPage, AreaReadLimits, EntryKey, InsertOutcome, StoreError, WillowStore,
};
use crate::upstream::UpstreamStore;

/// Runs every store-contract check against `store`.
pub async fn contract_suite(store: Arc<dyn WillowStore>) {
    insert_stores_entry_and_payload(&store).await;
    newer_entry_replaces_the_entry_at_the_same_path(&store).await;
    older_entry_at_the_same_path_is_outdated(&store).await;
    entry_prunes_the_entries_under_its_path(&store).await;
    an_exact_tie_prunes_the_entry_under_it(&store).await;
    reinserting_a_stored_entry_prunes_nothing(&store).await;
    subspaces_do_not_prune_each_other(&store).await;
    namespaces_do_not_prune_each_other(&store).await;
    tombstone_replaces_the_entry_and_stays_readable(&store).await;
    area_selects_by_subspace(&store).await;
    area_selects_by_path_prefix(&store).await;
    a_time_range_on_the_area_is_the_cursor(&store).await;
    paging_yields_every_entry_once(&store).await;
    next_is_none_exactly_when_drained(&store).await;
    metadata_only_insert_can_be_completed_with_a_payload(&store).await;
    metadata_only_insert_retains_an_unchanged_entries_payload(&store).await;
    metadata_only_insert_drops_the_payload_when_the_entry_changes(&store).await;
    forget_entry_is_local(&store).await;
    forget_area_removes_only_the_selected_area(&store).await;
    forget_namespace_removes_only_that_namespace(&store).await;
    payload_length_mismatch_is_rejected(&store).await;
    payload_digest_mismatch_is_rejected(&store).await;
    flush_succeeds(&store).await;
    upstream_adapter_agrees_with_the_store(&store).await;
    upstream_adapter_respects_pruning(&store).await;
    upstream_adapter_reads_an_entry_without_its_payload(&store).await;
    upstream_adapter_pages_through_a_large_area(&store).await;
}

/// A namespace and subspace used to author entries in one check.
struct Author {
    namespace_id: NamespaceId,
    subspace_id: SubspaceId,
    capability: WriteCapability,
    secret: SubspaceSecret,
}

impl Author {
    /// `namespace_byte` selects the namespace, `subspace_byte` the subspace. Two authors
    /// with the same namespace byte share a namespace; two with the same subspace byte share
    /// a subspace id even across namespaces.
    ///
    /// The namespace id is forced communal, because this fixture mints a communal capability.
    /// `NamespaceId::is_communal` is "the least significant bit of the id is zero", so the
    /// natural `[namespace_byte; NAMESPACE_ID_WIDTH]` would be an *owned* namespace for every
    /// odd byte. A communal capability over an owned namespace is constructible —
    /// `WriteCapability::new_communal` checks only the receiver and the granted area — but
    /// `WriteCapability`'s decoder rejects it, so such an entry could be built and never read
    /// back from any store that persists the authorisation token. `MemStore` never noticed,
    /// because it keeps the `AuthorisedEntry` in memory. Clearing the last byte keeps the ids
    /// distinct, since the leading bytes still vary.
    fn new(namespace_byte: u8, subspace_byte: u8) -> Self {
        let mut namespace_bytes = [namespace_byte; NAMESPACE_ID_WIDTH];
        namespace_bytes[NAMESPACE_ID_WIDTH - 1] = 0;
        let namespace_id = NamespaceId::from_bytes(&namespace_bytes);
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

    fn entry(&self, path: &str, timestamp: u64, payload: &[u8]) -> AuthorisedEntry {
        Entry::builder()
            .namespace_id(self.namespace_id.clone())
            .subspace_id(self.subspace_id.clone())
            .path(path_of(path))
            .timestamp(timestamp)
            .payload_length(payload.len() as u64)
            .payload_digest(PayloadDigest::from_payload(payload))
            .build()
            .into_authorised_entry(&self.capability, &self.secret)
            .expect("a communal capability authorises its own subspace")
    }

    fn area(&self) -> Area {
        Area::new_subspace_area(self.subspace_id.clone())
    }

    fn key(&self, path: &str) -> EntryKey {
        (self.subspace_id.clone(), path_of(path))
    }
}

/// Builds a path from a `/`-separated string.
/// Components are split literally and no leading-slash sugar is applied, so `path_of("/a")`
/// is the two-component path `["", "a"]` rather than the `["a"]` that the `path!` macro
/// would build. A caller holding a [`Path`] must not pass its `Display` output back
/// through here: `Display` prefixes every component with `/`.
fn path_of(path: &str) -> Path {
    let components: Vec<&[u8]> = path.split('/').map(str::as_bytes).collect();
    Path::from_slices(&components).expect("suite paths are valid")
}

fn paths_of(entries: &[AuthorisedEntry]) -> Vec<Path> {
    entries.iter().map(|entry| entry.path().clone()).collect()
}

fn limits(max_entries: usize) -> AreaReadLimits {
    AreaReadLimits {
        max_entries: NonZeroUsize::new(max_entries).expect("suite page sizes are non-zero"),
    }
}

/// Reads every entry an area query matches, one page at a time.
async fn drain(
    store: &Arc<dyn WillowStore>,
    namespace_id: &NamespaceId,
    area: &Area,
    page_size: usize,
) -> Vec<AuthorisedEntry> {
    let mut drained = Vec::new();
    let mut resume: Option<EntryKey> = None;

    loop {
        let page = read_page(store, namespace_id, area, resume.as_ref(), page_size).await;
        let next = page.next;
        drained.extend(page.entries);

        match next {
            Some(key) => resume = Some(key),
            None => return drained,
        }
    }
}

async fn read_page(
    store: &Arc<dyn WillowStore>,
    namespace_id: &NamespaceId,
    area: &Area,
    resume_after: Option<&EntryKey>,
    page_size: usize,
) -> AreaPage {
    store
        .read_area(namespace_id, area, resume_after, limits(page_size))
        .await
        .expect("read_area succeeds")
}

async fn insert_with_payload(
    store: &Arc<dyn WillowStore>,
    entry: AuthorisedEntry,
    payload: &[u8],
) {
    store
        .insert_entry_with_payload(entry, payload)
        .await
        .expect("insert succeeds");
}

async fn stored(
    store: &Arc<dyn WillowStore>,
    author: &Author,
    path: &str,
) -> Option<AuthorisedEntry> {
    store
        .get_entry(&author.namespace_id, &author.subspace_id, &path_of(path))
        .await
        .expect("get_entry succeeds")
}

async fn stored_payload(
    store: &Arc<dyn WillowStore>,
    author: &Author,
    path: &str,
) -> Option<Vec<u8>> {
    store
        .get_payload(&author.namespace_id, &author.subspace_id, &path_of(path))
        .await
        .expect("get_payload succeeds")
}

async fn insert_stores_entry_and_payload(store: &Arc<dyn WillowStore>) {
    let author = Author::new(1, 1);
    let entry = author.entry("a", 10, b"payload");

    let outcome = store
        .insert_entry_with_payload(entry.clone(), b"payload")
        .await
        .expect("insert succeeds");
    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &author, "a").await == Some(entry));
    assert!(stored_payload(store, &author, "a").await == Some(b"payload".to_vec()));
}

async fn newer_entry_replaces_the_entry_at_the_same_path(store: &Arc<dyn WillowStore>) {
    let author = Author::new(2, 1);
    let newer = author.entry("a", 20, b"two");

    insert_with_payload(store, author.entry("a", 10, b"one"), b"one").await;
    let outcome = store
        .insert_entry_with_payload(newer.clone(), b"two")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 1 });
    assert!(stored(store, &author, "a").await == Some(newer));
    assert!(stored_payload(store, &author, "a").await == Some(b"two".to_vec()));
}

async fn older_entry_at_the_same_path_is_outdated(store: &Arc<dyn WillowStore>) {
    let author = Author::new(3, 1);
    let newer = author.entry("a", 20, b"two");

    insert_with_payload(store, newer.clone(), b"two").await;
    let outcome = store
        .insert_entry_with_payload(author.entry("a", 10, b"one"), b"one")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Outdated);
    assert!(stored(store, &author, "a").await == Some(newer));
    assert!(stored_payload(store, &author, "a").await == Some(b"two".to_vec()));
}

async fn entry_prunes_the_entries_under_its_path(store: &Arc<dyn WillowStore>) {
    let author = Author::new(4, 1);

    insert_with_payload(store, author.entry("a/b/c", 10, b"deep"), b"deep").await;
    insert_with_payload(store, author.entry("a/b", 20, b"mid"), b"mid").await;
    assert!(stored(store, &author, "a/b/c").await.is_none());

    let outcome = store
        .insert_entry_with_payload(author.entry("a", 30, b"top"), b"top")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 1 });
    assert!(stored(store, &author, "a/b").await.is_none());
    assert!(stored(store, &author, "a").await.is_some());
}

/// An entry that ties the new one exactly is pruned when it sits under the new entry's path.
///
/// A tie means equal timestamp, digest and length. The specification's `prunes` relation is
/// strict, so a tie is not what the specification demands; it is what the reference
/// implementation does. `willow25`'s in-memory store prunes on `is_older_than_or_equal_to`,
/// and the published `store_pruning` vectors were generated by running it, so they require a
/// tie to prune. See the `conformance::store_pruning` module for why the reference wins.
async fn an_exact_tie_prunes_the_entry_under_it(store: &Arc<dyn WillowStore>) {
    let author = Author::new(27, 1);

    insert_with_payload(store, author.entry("a/b", 10, b"same"), b"same").await;

    let outcome = store
        .insert_entry_with_payload(author.entry("a", 10, b"same"), b"same")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 1 });
    assert!(stored(store, &author, "a/b").await.is_none());
    assert!(stored(store, &author, "a").await.is_some());
}

/// Re-inserting the entry that is already stored prunes nothing, not even a tie under it.
///
/// The no-op is observable rather than an optimisation, and the `store_pruning` corpus pins
/// it: one vector stores `/`, then a descendant that ties it, then `/` again, and expects the
/// descendant to survive. Without the no-op the third insert would prune the tie.
async fn reinserting_a_stored_entry_prunes_nothing(store: &Arc<dyn WillowStore>) {
    let author = Author::new(28, 1);
    let entry = author.entry("a", 10, b"same");

    insert_with_payload(store, entry.clone(), b"same").await;
    insert_with_payload(store, author.entry("a/b", 10, b"same"), b"same").await;

    let outcome = store
        .insert_entry(entry.clone())
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &author, "a/b").await.is_some());
    assert!(stored(store, &author, "a").await == Some(entry));
}

async fn subspaces_do_not_prune_each_other(store: &Arc<dyn WillowStore>) {
    let first = Author::new(5, 1);
    let second = Author::new(5, 2);
    assert!(first.namespace_id == second.namespace_id);

    insert_with_payload(store, first.entry("a", 10, b"first"), b"first").await;
    let outcome = store
        .insert_entry_with_payload(second.entry("a", 20, b"second"), b"second")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &first, "a").await.is_some());
    assert!(stored(store, &second, "a").await.is_some());
}

async fn namespaces_do_not_prune_each_other(store: &Arc<dyn WillowStore>) {
    let first = Author::new(6, 1);
    let second = Author::new(7, 1);
    assert!(first.subspace_id == second.subspace_id);

    insert_with_payload(store, first.entry("a", 10, b"first"), b"first").await;
    let outcome = store
        .insert_entry_with_payload(second.entry("a", 20, b"second"), b"second")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &first, "a").await.is_some());
    assert!(stored(store, &second, "a").await.is_some());
}

async fn tombstone_replaces_the_entry_and_stays_readable(store: &Arc<dyn WillowStore>) {
    let author = Author::new(8, 1);
    let tombstone = author.entry("doc", 20, b"");

    insert_with_payload(store, author.entry("doc", 10, b"version one"), b"version one").await;
    let outcome = store
        .insert_entry_with_payload(tombstone.clone(), b"")
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 1 });
    assert!(stored(store, &author, "doc").await == Some(tombstone));
    assert!(stored_payload(store, &author, "doc").await == Some(Vec::new()));
}

async fn area_selects_by_subspace(store: &Arc<dyn WillowStore>) {
    let first = Author::new(9, 1);
    let second = Author::new(9, 2);

    insert_with_payload(store, first.entry("a", 10, b"first"), b"first").await;
    insert_with_payload(store, second.entry("a", 10, b"second"), b"second").await;

    let entries = drain(store, &first.namespace_id, &first.area(), 8).await;
    assert!(paths_of(&entries) == vec![path_of("a")]);
    assert!(entries[0].subspace_id() == &first.subspace_id);
}

async fn area_selects_by_path_prefix(store: &Arc<dyn WillowStore>) {
    let author = Author::new(10, 1);

    insert_with_payload(store, author.entry("a", 10, b"a"), b"a").await;
    insert_with_payload(store, author.entry("a/b", 20, b"a/b"), b"a/b").await;
    insert_with_payload(store, author.entry("b", 30, b"b"), b"b").await;

    let area = Area::new(
        Some(author.subspace_id.clone()),
        path_of("a"),
        TimeRange::full(),
    );
    let entries = drain(store, &author.namespace_id, &area, 8).await;

    assert!(paths_of(&entries) == vec![path_of("a"), path_of("a/b")]);
}

async fn a_time_range_on_the_area_is_the_cursor(store: &Arc<dyn WillowStore>) {
    let author = Author::new(11, 1);

    insert_with_payload(store, author.entry("early", 10, b"early"), b"early").await;
    insert_with_payload(store, author.entry("inside", 20, b"inside"), b"inside").await;
    insert_with_payload(store, author.entry("late", 30, b"late"), b"late").await;

    // Reading strictly after a cursor is expressed as the area's time range.
    let cursor = Timestamp::from(20);
    let after_cursor = Area::new(
        Some(author.subspace_id.clone()),
        Path::new(),
        TimeRange::new_open(Timestamp::from(u64::from(cursor) + 1)),
    );
    let entries = drain(store, &author.namespace_id, &after_cursor, 8).await;
    assert!(paths_of(&entries) == vec![path_of("late")]);

    // A closed `TimeRange` is half open: `[start, end)`.
    let window = Area::new(
        Some(author.subspace_id.clone()),
        Path::new(),
        TimeRange::new_closed(Timestamp::from(15), Timestamp::from(30)),
    );
    let entries = drain(store, &author.namespace_id, &window, 8).await;
    assert!(paths_of(&entries) == vec![path_of("inside")]);
}

async fn paging_yields_every_entry_once(store: &Arc<dyn WillowStore>) {
    let author = Author::new(12, 1);
    // The path strings are used directly rather than round-tripped through `Path`'s
    // `Display`. `Display` prefixes every component with `/`, so reparsing `/a` would
    // yield the two-component path `["", "a"]` instead of `["a"]`.
    let names = ["a", "b", "c", "d", "e"];
    let expected: Vec<Path> = names.into_iter().map(path_of).collect();

    // One publication: every entry shares a timestamp, so the timestamp cannot serve as a
    // page continuation.
    for name in names {
        insert_with_payload(store, author.entry(name, 10, name.as_bytes()), name.as_bytes()).await;
    }

    let entries = drain(store, &author.namespace_id, &author.area(), 2).await;
    assert!(paths_of(&entries) == expected);
}

async fn next_is_none_exactly_when_drained(store: &Arc<dyn WillowStore>) {
    let author = Author::new(13, 1);

    insert_with_payload(store, author.entry("a", 10, b"a"), b"a").await;
    insert_with_payload(store, author.entry("b", 20, b"b"), b"b").await;
    insert_with_payload(store, author.entry("c", 30, b"c"), b"c").await;

    // A page that stops short of the end resumes from its own last entry.
    let page = read_page(store, &author.namespace_id, &author.area(), None, 2).await;
    assert!(paths_of(&page.entries) == vec![path_of("a"), path_of("b")]);
    assert!(page.next == Some(author.key("b")));

    // `next` is exclusive, so resuming from it yields exactly the remaining entry.
    let page = read_page(
        store,
        &author.namespace_id,
        &author.area(),
        page.next.as_ref(),
        2,
    )
    .await;
    assert!(paths_of(&page.entries) == vec![path_of("c")]);
    assert!(page.next.is_none());

    // A page that fills exactly on the last entry is drained: the store peeks past the page
    // rather than assuming that a full page implies more.
    let page = read_page(store, &author.namespace_id, &author.area(), None, 3).await;
    assert!(paths_of(&page.entries) == vec![path_of("a"), path_of("b"), path_of("c")]);
    assert!(page.next.is_none());

    // An area that matches nothing is drained immediately.
    let empty = Area::new(
        Some(author.subspace_id.clone()),
        path_of("missing"),
        TimeRange::full(),
    );
    let page = read_page(store, &author.namespace_id, &empty, None, 3).await;
    assert!(page.entries.is_empty());
    assert!(page.next.is_none());
}

async fn metadata_only_insert_can_be_completed_with_a_payload(store: &Arc<dyn WillowStore>) {
    let author = Author::new(14, 1);
    let entry = author.entry("a", 10, b"payload");

    let outcome = store
        .insert_entry(entry.clone())
        .await
        .expect("insert succeeds");
    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &author, "a").await == Some(entry.clone()));
    assert!(stored_payload(store, &author, "a").await.is_none());

    let outcome = store
        .insert_entry_with_payload(entry.clone(), b"payload")
        .await
        .expect("insert succeeds");
    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &author, "a").await == Some(entry));
    assert!(stored_payload(store, &author, "a").await == Some(b"payload".to_vec()));
}

async fn metadata_only_insert_retains_an_unchanged_entries_payload(store: &Arc<dyn WillowStore>) {
    let author = Author::new(15, 1);
    let entry = author.entry("a", 10, b"payload");

    insert_with_payload(store, entry.clone(), b"payload").await;
    let outcome = store
        .insert_entry(entry.clone())
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 0 });
    assert!(stored(store, &author, "a").await == Some(entry));
    assert!(stored_payload(store, &author, "a").await == Some(b"payload".to_vec()));
}

async fn metadata_only_insert_drops_the_payload_when_the_entry_changes(
    store: &Arc<dyn WillowStore>,
) {
    let author = Author::new(16, 1);
    let newer = author.entry("a", 20, b"newer payload");

    insert_with_payload(store, author.entry("a", 10, b"payload"), b"payload").await;
    let outcome = store
        .insert_entry(newer.clone())
        .await
        .expect("insert succeeds");

    assert!(outcome == InsertOutcome::Inserted { pruned: 1 });
    assert!(stored(store, &author, "a").await == Some(newer));
    // The retained bytes would no longer describe the entry, so they are dropped.
    assert!(stored_payload(store, &author, "a").await.is_none());
}

async fn forget_entry_is_local(store: &Arc<dyn WillowStore>) {
    let author = Author::new(17, 1);
    insert_with_payload(store, author.entry("a", 10, b"payload"), b"payload").await;

    let forgotten = store
        .forget_entry(&author.namespace_id, &author.subspace_id, &path_of("a"))
        .await
        .expect("forget_entry succeeds");
    assert!(forgotten);
    assert!(stored(store, &author, "a").await.is_none());

    let forgotten = store
        .forget_entry(&author.namespace_id, &author.subspace_id, &path_of("a"))
        .await
        .expect("forget_entry succeeds");
    assert!(!forgotten);

    // Forgetting is local, so the entry can come back.
    insert_with_payload(store, author.entry("a", 10, b"payload"), b"payload").await;
    assert!(stored(store, &author, "a").await.is_some());
}

async fn forget_area_removes_only_the_selected_area(store: &Arc<dyn WillowStore>) {
    let author = Author::new(18, 1);

    insert_with_payload(store, author.entry("a", 10, b"a"), b"a").await;
    insert_with_payload(store, author.entry("a/b", 20, b"a/b"), b"a/b").await;
    insert_with_payload(store, author.entry("b", 30, b"b"), b"b").await;

    let area = Area::new(
        Some(author.subspace_id.clone()),
        path_of("a"),
        TimeRange::full(),
    );
    store
        .forget_area(&author.namespace_id, &area)
        .await
        .expect("forget_area succeeds");

    assert!(stored(store, &author, "a").await.is_none());
    assert!(stored(store, &author, "a/b").await.is_none());
    assert!(stored(store, &author, "b").await.is_some());
}

async fn forget_namespace_removes_only_that_namespace(store: &Arc<dyn WillowStore>) {
    let first = Author::new(19, 1);
    let second = Author::new(20, 1);

    insert_with_payload(store, first.entry("a", 10, b"first"), b"first").await;
    insert_with_payload(store, second.entry("a", 10, b"second"), b"second").await;

    store
        .forget_namespace(&first.namespace_id)
        .await
        .expect("forget_namespace succeeds");

    assert!(stored(store, &first, "a").await.is_none());
    assert!(stored(store, &second, "a").await.is_some());
}

async fn payload_length_mismatch_is_rejected(store: &Arc<dyn WillowStore>) {
    let author = Author::new(21, 1);
    let entry = author.entry("a", 10, b"payload");

    let error = store
        .insert_entry_with_payload(entry, b"short")
        .await
        .expect_err("a payload of the wrong length is rejected");

    assert!(matches!(
        error,
        StoreError::PayloadLengthMismatch {
            expected: 7,
            actual: 5
        }
    ));
    assert!(stored(store, &author, "a").await.is_none());
}

async fn payload_digest_mismatch_is_rejected(store: &Arc<dyn WillowStore>) {
    let author = Author::new(22, 1);
    let entry = author.entry("a", 10, b"payload");

    let error = store
        .insert_entry_with_payload(entry, b"payloaD")
        .await
        .expect_err("a payload with the wrong digest is rejected");

    assert!(matches!(error, StoreError::PayloadDigestMismatch));
    assert!(stored(store, &author, "a").await.is_none());
}

async fn flush_succeeds(store: &Arc<dyn WillowStore>) {
    store.flush().await.expect("flush succeeds");
}

/// Creates an entry through the `willow25::Store` adapter.
async fn upstream_create(
    upstream: &mut UpstreamStore,
    author: &Author,
    path: &str,
    timestamp: u64,
    payload: &[u8],
) -> AuthorisedEntry {
    let mut producer = payload.to_vec().into_producer();

    upstream
        .create_entry(
            &author.namespace_id,
            &author.subspace_id,
            &path_of(path),
            Timestamp::from(timestamp),
            &mut producer,
            payload.len() as u64,
            &author.capability,
            &author.secret,
        )
        .await
        .expect("create_entry succeeds")
        .expect("the created entry is not outdated")
}

async fn upstream_adapter_agrees_with_the_store(store: &Arc<dyn WillowStore>) {
    let author = Author::new(23, 1);
    let mut upstream = UpstreamStore::new(Arc::clone(store));
    let payload = b"adapter payload".to_vec();

    let created = upstream_create(&mut upstream, &author, "a", 10, &payload).await;

    assert!(stored(store, &author, "a").await == Some(created.clone()));
    assert!(stored_payload(store, &author, "a").await == Some(payload.clone()));

    // Reading a payload slice through the adapter yields the same bytes.
    let mut consumer = Vec::<u8>::new().into_consumer();
    let (sliced, written) = upstream
        .get_entry_and_payload_slice(
            &author.namespace_id,
            &author.key("a"),
            None,
            3,
            6,
            &mut consumer,
        )
        .await
        .expect("get_entry_and_payload_slice succeeds")
        .expect("the entry exists");

    assert!(sliced == created);
    assert!(written == 6);
    assert!(Vec::<u8>::from(consumer) == payload[3..9].to_vec());
}

async fn upstream_adapter_respects_pruning(store: &Arc<dyn WillowStore>) {
    let author = Author::new(24, 1);
    let mut upstream = UpstreamStore::new(Arc::clone(store));

    // `/a/b` already exists, so creating `/a` would prune it.
    upstream_create(&mut upstream, &author, "a/b", 10, b"deep").await;

    let payload = b"top";
    let mut producer = payload.to_vec().into_producer();
    let prevented = upstream
        .create_entry_nondestructive(
            &author.namespace_id,
            &author.subspace_id,
            &path_of("a"),
            Timestamp::from(20),
            &mut producer,
            payload.len() as u64,
            &author.capability,
            &author.secret,
        )
        .await
        .expect("create_entry_nondestructive succeeds");

    assert!(prevented == NondestructiveInsert::Prevented);
    assert!(stored(store, &author, "a").await.is_none());
    assert!(stored(store, &author, "a/b").await.is_some());

    // `/a/b` is newer than the entry being created at the same path.
    let older = b"older";
    let mut producer = older.to_vec().into_producer();
    let outdated = upstream
        .create_entry_nondestructive(
            &author.namespace_id,
            &author.subspace_id,
            &path_of("a/b"),
            Timestamp::from(5),
            &mut producer,
            older.len() as u64,
            &author.capability,
            &author.secret,
        )
        .await
        .expect("create_entry_nondestructive succeeds");

    assert!(outdated == NondestructiveInsert::Outdated);
    assert!(stored(store, &author, "a/b").await.is_some());
}

async fn upstream_adapter_reads_an_entry_without_its_payload(store: &Arc<dyn WillowStore>) {
    let author = Author::new(25, 1);
    let entry = author.entry("a", 10, b"payload");
    let mut upstream = UpstreamStore::new(Arc::clone(store));

    assert!(
        upstream
            .insert_entry(entry.clone())
            .await
            .expect("insert_entry succeeds")
    );
    assert!(stored(store, &author, "a").await == Some(entry.clone()));
    assert!(stored_payload(store, &author, "a").await.is_none());

    // A payload that has not arrived contributes no bytes, and the entry is still reported.
    let mut consumer = Vec::<u8>::new().into_consumer();
    let (read, written) = upstream
        .get_entry_and_payload_slice(
            &author.namespace_id,
            &author.key("a"),
            None,
            0,
            7,
            &mut consumer,
        )
        .await
        .expect("get_entry_and_payload_slice succeeds")
        .expect("the entry exists");

    assert!(read == entry);
    assert!(written == 0);
    assert!(Vec::<u8>::from(consumer).is_empty());
}

async fn upstream_adapter_pages_through_a_large_area(store: &Arc<dyn WillowStore>) {
    let author = Author::new(26, 1);
    // More than one adapter page, so the paging loop is exercised.
    let count = 300usize;
    let mut expected = Vec::with_capacity(count);

    for index in 0..count {
        let path = format!("a/{index:03}");
        insert_with_payload(
            store,
            author.entry(&path, 10, path.as_bytes()),
            path.as_bytes(),
        )
        .await;
        expected.push(path_of(&path));
    }

    let mut upstream = UpstreamStore::new(Arc::clone(store));
    let mut consumer = Vec::<AuthorisedEntry>::new().into_consumer();
    upstream
        .get_area(&author.namespace_id, &author.area(), &mut consumer)
        .await
        .expect("get_area succeeds");

    let entries: Vec<AuthorisedEntry> = consumer.into();
    assert!(paths_of(&entries) == expected);
}
