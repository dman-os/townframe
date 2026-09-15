//! The `Send + Sync`, object-safe entry store contract that `big_willow` owns.
//!
//! `big_willow` owns its store rather than wrapping the upstream `willow25` stores: those
//! hold `Rc` over a single-threaded lock, and the upstream `Store` trait is neither object
//! safe nor able to bound a range read. This module states the contract a replacement must
//! satisfy. [`crate::UpstreamStore`] adapts it back to `willow25::Store`.
//!
//! # What the store owns
//!
//! - **Prefix-pruning atomicity.** An insert either stores the entry and removes every
//!   entry it prunes, or leaves the store untouched. There is no intermediate state.
//! - **A re-insert of a stored entry is a no-op.** Storing the entry that already occupies its
//!   key prunes nothing, not even the entries it would otherwise prune. The no-op is
//!   observable, so it is part of the contract rather than an optimisation.
//! - **Retained tombstones.** A newer entry at an existing path replaces the older one and
//!   stays readable, so removal is a stored fact rather than an absence. Forgetting is
//!   local: a later insert can bring the entry back.
//! - **Payload/entry consistency.** A payload retained for a key always matches that
//!   entry's `payload_length` and `payload_digest`. A key may legitimately hold an entry
//!   whose payload has not arrived; see [`WillowStore::get_payload`].
//! - **Key ordering.** [`WillowStore::read_area`] yields entries in [`EntryKey`] order, and
//!   [`AreaPage::next`] resumes that order without gaps or repeats.
//! - **The resume contract.** `resume_after` is exclusive, and [`AreaPage::next`] is `None`
//!   exactly when the query is drained.
//!
//! # What callers own
//!
//! - **Cursor advancement.** The store never records how far a caller has read.
//! - **Area construction.** Which entries a read matches, including any cursor bound, is
//!   entirely the caller's [`Area`]. See [`WillowStore::read_area`].
//! - **Interpreting absence.** A key with no retained payload may still hold an entry, so a
//!   caller that must distinguish "no entry" from "no payload yet" calls
//!   [`WillowStore::get_entry`] first.

use crate::interlude::*;

use std::num::NonZeroUsize;

use willow25::prelude::*;

#[cfg(any(test, feature = "test-support"))]
pub mod contract;

/// Position of one entry inside a namespace, in the order [`WillowStore::read_area`]
/// returns entries.
///
/// Willow identifies an entry within a namespace by its `(subspace, path)`: two entries
/// agreeing on namespace, subspace, and path always [prefix
/// prune](https://willowprotocol.org/specs/data-model/index.html#prefix_pruning) each
/// other, so a store retains at most one of them. This is `willow25`'s own `Keylike` tuple,
/// so it already carries the trait impls and orders lexicographically, which is exactly the
/// order `read_area` yields.
pub type EntryKey = (SubspaceId, Path);

/// Whether storing `new` removes the already stored entry `existing`.
///
/// This is the store's prune rule, which is the reference implementation's rather than the
/// specification's strict [`EntrylikeExt::prunes`] relation: an entry that ties the new one
/// exactly on recency is removed when it sits under the new entry's path. The one exception is a
/// tie at the new entry's own path, which the store replaces in place so that the payload
/// retained for it survives, and which it does not count as pruned.
///
/// `delete_pruned_entries.sql` states the same rule in SQL, and the `store_pruning` conformance
/// suite is what pins it.
pub(crate) fn prunes(existing: &AuthorisedEntry, new: &AuthorisedEntry) -> bool {
    !existing.is_newer_than(new) && !existing.entry_eq(new)
}

/// Errors a store reports. Every other failure is a programming error and panics.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The supplied payload length disagrees with the entry it is being stored for.
    #[error("payload length {actual} does not match the entry payload length {expected}")]
    PayloadLengthMismatch { expected: u64, actual: u64 },
    /// The supplied payload does not hash to the entry's payload digest.
    #[error("payload does not hash to the entry payload digest")]
    PayloadDigestMismatch,
    /// The backing storage failed.
    ///
    /// Entries and payloads can originate from peers, so a storage failure is a boundary
    /// error rather than an invariant break.
    #[error("store backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

/// What happened to an inserted entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The entry is stored. `pruned` counts the entries it removed.
    ///
    /// A row at the new entry's own key counts only when the entry there changed, because an
    /// unchanged row is updated in place rather than removed.
    Inserted { pruned: usize },
    /// An entry already in the store prunes the new one, so nothing was stored.
    Outdated,
}

/// Bounds one page of [`WillowStore::read_area`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AreaReadLimits {
    /// Soft limit: a page boundary may split one publication, so a caller advancing a
    /// timestamp cursor drains to [`AreaPage::next`] being `None` before doing so.
    pub max_entries: NonZeroUsize,
}

impl Default for AreaReadLimits {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(256).expect("literal is non-zero"),
        }
    }
}

/// One page of [`WillowStore::read_area`].
#[derive(Debug, Clone)]
pub struct AreaPage {
    /// The matching entries, in [`EntryKey`] order.
    pub entries: Vec<AuthorisedEntry>,
    /// The key to pass as `resume_after` for the following page, or `None` iff the query is
    /// fully drained.
    ///
    /// It is the key of the last entry in `entries`. Because `resume_after` is exclusive,
    /// resuming from it continues exactly where this page stopped.
    pub next: Option<EntryKey>,
}

/// A store of authorised Willow entries.
///
/// Implementations must be `Send + Sync` and usable through `Arc<dyn WillowStore>`. BigRepo
/// drives stores from multi-threaded tasks, which the upstream `willow25` stores cannot
/// support.
///
/// The invariants an implementation owns are listed in the [module
/// docs](self).
#[async_trait]
pub trait WillowStore: Send + Sync + 'static {
    /// Stores `entry`'s metadata without a payload.
    ///
    /// If the key already holds an entry that [`entry_eq`](EntrylikeExt::entry_eq) to
    /// `entry`, the stored payload is retained. Otherwise the payload is recorded as
    /// absent, and a later [`insert_entry_with_payload`](WillowStore::insert_entry_with_payload)
    /// supplies it.
    ///
    /// No length or digest validation applies here: there is no payload to validate. This
    /// is the entry-first shape of `willow25::Store::insert_entry`, where payloads travel
    /// separately from entry metadata.
    ///
    /// Storing the entry that is already stored at the key is a no-op that prunes nothing, not
    /// even the entries it would otherwise prune. `willow25`'s in-memory store returns early on
    /// the same case, and the `store_pruning` conformance suite pins it.
    ///
    /// Returns [`InsertOutcome::Outdated`] when an entry already in the store prunes
    /// `entry`, in which case nothing is stored.
    async fn insert_entry(&self, entry: AuthorisedEntry) -> Result<InsertOutcome, StoreError>;

    /// Stores `entry` together with its payload, pruning the entries it replaces.
    ///
    /// The payload must match the entry: `payload.len()` must equal `entry.payload_length()`
    /// and the payload must hash to `entry.payload_digest()`. A mismatch is rejected and
    /// nothing is stored.
    ///
    /// Re-inserting an already stored entry prunes nothing, but still stores `payload`, which is
    /// how a payload that had not arrived is completed.
    ///
    /// Returns [`InsertOutcome::Outdated`] when an entry already in the store prunes
    /// `entry`, in which case nothing is stored.
    async fn insert_entry_with_payload(
        &self,
        entry: AuthorisedEntry,
        payload: &[u8],
    ) -> Result<InsertOutcome, StoreError>;

    /// Returns the entry at `(namespace_id, subspace_id, path)`, if any.
    async fn get_entry(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<Option<AuthorisedEntry>, StoreError>;

    /// Returns the payload retained for the entry at `(namespace_id, subspace_id, path)`.
    ///
    /// `None` means no payload is retained for that key. That covers both "no entry" and
    /// "entry present, payload not received". A caller that must distinguish the two calls
    /// [`get_entry`](WillowStore::get_entry) first.
    async fn get_payload(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<Option<Vec<u8>>, StoreError>;

    /// Reads one page of the entries of `namespace_id` inside `area`.
    ///
    /// Entries are yielded in [`EntryKey`] order. `resume_after` continues a previous page
    /// and is **exclusive**: only entries strictly after that key are returned. `limits`
    /// bounds the page.
    ///
    /// Any cursor bound belongs on `area` rather than in a separate argument, because
    /// [`Area`] already owns the time range. Reading strictly after a cursor `C` is:
    ///
    /// ```
    /// use willow25::prelude::*;
    ///
    /// let cursor = Timestamp::from(1_000);
    /// // `WillowRange::new_open` includes its start, so the cursor advances by one.
    /// let times = TimeRange::new_open(Timestamp::from(u64::from(cursor) + 1));
    /// let area = Area::new(None, Path::new(), times);
    ///
    /// assert!(!area.times().includes_value(&cursor));
    /// assert!(area.times().includes_value(&(cursor + Timestamp::from(1))));
    /// ```
    ///
    /// The cursor is a filter on a key-ordered scan, not a range bound: it selects which
    /// entries match, while ordering and `resume_after` stay keyed on [`EntryKey`]. A page
    /// boundary can therefore split the entries of one publication, and a caller advancing
    /// a timestamp cursor drains to [`AreaPage::next`] being `None` first.
    async fn read_area(
        &self,
        namespace_id: &NamespaceId,
        area: &Area,
        resume_after: Option<&EntryKey>,
        limits: AreaReadLimits,
    ) -> Result<AreaPage, StoreError>;

    /// Removes the entry at `(namespace_id, subspace_id, path)`, reporting whether anything
    /// was removed.
    ///
    /// Forgetting is local: a later insert, or a join with another store, can bring the
    /// entry back. It is not a replicated deletion.
    async fn forget_entry(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<bool, StoreError>;

    /// Removes every entry of `namespace_id` inside `area`. Local only.
    async fn forget_area(&self, namespace_id: &NamespaceId, area: &Area) -> Result<(), StoreError>;

    /// Removes every entry of `namespace_id`. Local only.
    async fn forget_namespace(&self, namespace_id: &NamespaceId) -> Result<(), StoreError>;

    /// Persists every prior mutation, if the backing storage is persistent.
    async fn flush(&self) -> Result<(), StoreError>;
}
