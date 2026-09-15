//! The store: reps as rows, one row per path (ADR 010 §3.1–§3.3).
//!
//! A **rep** is the bridge's record of one backend's latest-known state. It is
//! a set of rows keyed by path, updated in place, and ordered by path — so the
//! primary key is the ordering structure, and every walk and every comparison
//! is an index scan in the same order.
//!
//! Two consequences are worth stating because a lot of design follows from
//! them:
//!
//! - there is no tree, no node hash, and nothing to garbage collect: a rep is a
//!   state, not a version (ADR 010 §2.2);
//! - bytes are never stored here. Rows carry provenance ([`Entry`]), and the
//!   bytes stay with whichever backend owns them.

use crate::backend::BackendId;
use crate::delta::Delta;
use crate::entry::Entry;
use crate::interlude::*;
use crate::path::RelPath;

pub mod mem;
#[cfg(feature = "sqlite")]
pub mod sqlite;

/// How many rows one page of a scan carries.
const SCAN_PAGE: usize = 512;

/// The store the bridge records backend state in.
#[async_trait]
pub trait VtreeStore: Send + Sync {
    /// Record one rep's deltas in a single transaction, and return the rep's
    /// new generation.
    ///
    /// A rep update is atomic: a crash between two updates loses nothing but
    /// the cache, because the next change report rebuilds it (ADR 010 §2.2).
    /// Applying an empty batch changes nothing and does not bump the
    /// generation.
    async fn apply(&self, rep: &BackendId, deltas: &[Delta]) -> Result<u64>;

    /// The recorded entry at one path.
    async fn entry(&self, rep: &BackendId, path: &RelPath) -> Result<Option<Entry>>;

    /// A page of recorded entries in canonical path order, strictly after
    /// `from`, at most `limit` of them.
    ///
    /// This is the one primitive the walks are built from: [`RepScan`] pages
    /// through it, and so does every bulk pass that needs to crash-resume at a
    /// cursor (ADR 011 §6).
    async fn scan_page(
        &self,
        rep: &BackendId,
        from: Option<&RelPath>,
        limit: usize,
    ) -> Result<Vec<(RelPath, Entry)>>;

    /// A rep's generation: absent if the rep has never been written.
    async fn generation(&self, rep: &BackendId) -> Result<Option<u64>>;

    /// Every rep, ordered by name.
    async fn reps(&self) -> Result<Vec<BackendId>>;

    /// Forget a rep and its rows, reporting whether it existed.
    async fn drop_rep(&self, rep: &BackendId) -> Result<bool>;

    /// Record one entry, and only that entry.
    ///
    /// What a bridge says after it materializes something: the bytes are now
    /// here, with this provenance, and this is the stat we observed.
    async fn put_entry(&self, rep: &BackendId, path: &RelPath, entry: &Entry) -> Result<()> {
        self.apply(
            rep,
            &[Delta::Added {
                path: path.clone(),
                entry: entry.clone(),
            }],
        )
        .await?;
        Ok(())
    }
}

/// The directories a path implies, shallowest first, excluding the root and
/// the path itself.
///
/// A report or a store write may mention a file whose parent directory was
/// never recorded (a producer emitting one file, a delta applied out of order);
/// inserting the implied directories keeps "is there a directory here"
/// answerable.
pub(crate) fn implied_parents(path: &RelPath) -> Vec<RelPath> {
    let mut parents = path.ancestors_inclusive().skip(1).collect::<Vec<_>>();
    parents.pop();
    parents
}

/// A path-ordered cursor over one rep's recorded rows.
///
/// Consumption is ordered and one-way, which is what makes a change report a
/// merge join: the backend walks its own source in path order, and asks the
/// cursor about each path it reaches. `take_earlier` is what turns the rows the
/// walk *did not* reach into removals without a second pass.
pub struct RepScan {
    store: Arc<dyn VtreeStore>,
    rep: BackendId,
    page: VecDeque<(RelPath, Entry)>,
    cursor: Option<RelPath>,
    exhausted: bool,
}

impl RepScan {
    /// Start at the beginning of `rep`.
    #[must_use]
    pub fn new(store: Arc<dyn VtreeStore>, rep: BackendId) -> Self {
        Self {
            store,
            rep,
            page: VecDeque::new(),
            cursor: None,
            exhausted: false,
        }
    }

    /// The next recorded row, whatever its path.
    pub async fn take_next(&mut self) -> Result<Option<(RelPath, Entry)>> {
        self.refill().await?;
        Ok(self.page.pop_front())
    }

    /// The recorded row at exactly `path`, if that is the row in hand.
    ///
    /// Returns `None` when the row in hand sorts after `path` (nothing was
    /// recorded there) and leaves it in hand. A row that sorts *before* `path`
    /// means the caller skipped a path, which is what
    /// [`Report::removed_before`](crate::backend::Report::removed_before) is
    /// for; it is left in hand so the caller can still notice it.
    pub async fn take_at(&mut self, path: &RelPath) -> Result<Option<Entry>> {
        self.refill().await?;
        match self.page.front() {
            Some((front, _)) if front == path => Ok(self.page.pop_front().map(|(_, entry)| entry)),
            _ => Ok(None),
        }
    }

    /// The next recorded row strictly before `path`, consuming it.
    pub async fn take_earlier(&mut self, path: &RelPath) -> Result<Option<(RelPath, Entry)>> {
        self.refill().await?;
        match self.page.front() {
            Some((front, _)) if front < path => Ok(self.page.pop_front()),
            _ => Ok(None),
        }
    }

    /// Pull the next page, if the one in hand is empty.
    async fn refill(&mut self) -> Result<()> {
        if !self.page.is_empty() || self.exhausted {
            return Ok(());
        }
        let page = self
            .store
            .scan_page(&self.rep, self.cursor.as_ref(), SCAN_PAGE)
            .await?;
        if page.is_empty() {
            self.exhausted = true;
            return Ok(());
        }
        self.cursor = page.last().map(|(path, _)| path.clone());
        self.page.extend(page);
        Ok(())
    }
}
