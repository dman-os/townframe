//! The read contract every revisioned source implements, and its one consumer.
//!
//! A source assigns one [`Rev`] per committed atomic batch, strictly increasing, and keeps only
//! the latest row per key (an overwrite moves the key to the overwriting rev; a delete is a kept
//! tombstone). It implements three verbs and nothing else ([`RevSource`]): `latest_rev`, a
//! stateless `read` of one bounded window into a caller-owned [`Page`], and `watch`, a handle on
//! the latest committed rev that the source moves from its commit path.
//!
//! Nobody writes a reader. [`RevStream`] is the single consumer: it replays up to the rev it
//! captured on its first read, then follows commits, handing out pages into a buffer the caller
//! reuses. Projections are built with [`RevSourceExt::map`] and [`RevSourceExt::filter_map`]
//! instead of hand-written plumbing.

use std::num::NonZeroUsize;

use tokio::sync::watch;

use crate::interlude::*;

/// One per committed atomic batch, strictly increasing per source. `0` is the rev of a source
/// that has committed nothing.
pub type Rev = u64;

/// The latest row of one key, written by the batch at `rev`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry<K, V> {
    pub key: K,
    pub rev: Rev,
    /// `None` is a kept tombstone: the key was deleted at `rev`.
    pub value: Option<V>,
}

/// One read's worth of entries. The caller owns it and reuses it across reads: a read clears
/// `entries` and fills them, so the vector's capacity is kept. Pages are deliberately not
/// `Clone`; hold on to the entries you need, not the page.
///
/// - `entries` are in `(rev, key)` order and a page never splits a revision: it holds all of a
///   revision's matching rows or none of them.
/// - `through` is the read's progress: every key whose *current* row has a rev in
///   `(after, through]` and matches the selector is in this page or an earlier one.
///   Completeness is about the latest state per key, so a key overwritten while a stream is
///   replaying turns up later, at its new rev.
#[derive(Debug, PartialEq, Eq)]
pub struct Page<K, V> {
    pub entries: Vec<Entry<K, V>>,
    pub through: Rev,
}

impl<K, V> Default for Page<K, V> {
    fn default() -> Self {
        Self {
            entries: default(),
            through: 0,
        }
    }
}

impl<K, V> Page<K, V> {
    /// The page's atomic revisions, each as the contiguous slice of its entries, oldest first.
    /// Entries are already grouped by rev, so this only finds the boundaries.
    pub fn revisions(&self) -> impl Iterator<Item = &[Entry<K, V>]> {
        self.entries.chunk_by(|left, right| left.rev == right.rev)
    }
}

/// Bounds on one read. Both are soft by exactly one revision: a page goes past a bound only when
/// its first revision alone does, and then that revision is the whole page. Otherwise a page
/// holds as many whole revisions as fit under both bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_entries: NonZeroUsize,
    /// What an entry costs is the source's to define (okv charges key plus value bytes).
    pub max_bytes: Option<NonZeroUsize>,
}

/// A source of revisioned rows: the contract every store, table or projection implements.
///
/// It is inert. It holds no per-reader state, keeps no cursor, and never waits: replay
/// boundaries and live following belong to [`RevStream`]. An implementation only has to answer
/// three questions consistently with its commits:
///
/// - `latest_rev`: the rev of the last committed batch.
/// - `read`: one bounded window `(after, until]`, filled into the caller's page.
/// - `watch`: a [`RevWatch`] the source moves to each new rev from its commit path, after the
///   commit is visible to `read` and never for a batch that rolled back.
#[async_trait]
pub trait RevSource: Send + Sync {
    type Key: Send + Sync;
    type Value: Send + Sync;
    /// Which keys a read is about. Sources pick their own shape.
    type Selector: Send + Sync;

    async fn latest_rev(&self) -> Res<Rev>;

    /// Clear `page` and fill it with the current rows of the selected keys whose rev is in
    /// `(after, until]`, bounded by `limits` (see [`Page`] and [`Limits`] for the shape).
    ///
    /// `page.through` is `until` when the page holds every matching row up to `until` (an empty
    /// page then reports progress over a window with nothing selected), and otherwise the rev
    /// of the page's last entry.
    ///
    /// The caller guarantees `after < until` and that `until` is a rev it saw committed (from
    /// `latest_rev` or the watch); a source may assert both.
    async fn read(
        &self,
        selector: &Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<Self::Key, Self::Value>,
    ) -> Res<()>;

    fn watch(&self) -> RevWatch;
}

/// A handle on a source's latest committed rev.
///
/// The source keeps the sending half of a `tokio::sync::watch` channel and `send_replace`s each
/// rev from its commit path, so the value is never ahead of what `read` can see. The channel is
/// runtime agnostic (no timers, no executor), and because it carries the rev, a waiter that wakes
/// already knows how far to read without asking the source again.
#[derive(Debug, Clone)]
pub struct RevWatch {
    rx: watch::Receiver<Rev>,
}

impl RevWatch {
    pub fn new(rx: watch::Receiver<Rev>) -> Self {
        Self { rx }
    }

    pub fn latest(&self) -> Rev {
        *self.rx.borrow()
    }

    /// Wait until the source commits a rev past `after`, and return the latest one.
    ///
    /// The sender lives as long as the source, and a waiter holds the source, so a closed
    /// channel is a lifecycle bug and panics.
    pub async fn wait_past(&mut self, after: Rev) -> Rev {
        *self
            .rx
            .wait_for(|rev| *rev > after)
            .await
            .expect(ERROR_CHANNEL)
    }
}

/// The one consumer of a [`RevSource`]: replay, then follow.
///
/// On its first read it captures `snapshot_rev`, the source's latest rev, and reads
/// `(after, snapshot_rev]` page by page. Once a page's `through` reaches the snapshot it is
/// [`caught_up`](Self::caught_up) and every later page comes from commits made after it. Live
/// reads use the watch's rev as their bound and wait on the watch only when that rev is at most
/// the stream's progress, so a stream with work queued never waits.
///
/// Every page the stream returns advances `through`. A page may be empty: that is progress over
/// revisions the selector skipped, which walkers persist. The stream is not a
/// `futures::Stream`, so the caller's page is reused rather than a page allocated per item.
///
/// `next_into` is cancel safe: progress moves only when a read completes, so dropping the future
/// part way re-reads the same window next time.
pub struct RevStream<S: RevSource> {
    source: S,
    selector: S::Selector,
    /// Progress: everything selected at or below this has been handed out.
    after: Rev,
    snapshot_rev: Option<Rev>,
    watch: RevWatch,
}

impl<S: RevSource> RevStream<S> {
    pub fn new(source: S, selector: S::Selector, after: Rev) -> Self {
        let watch = source.watch();
        Self {
            source,
            selector,
            after,
            snapshot_rev: None,
            watch,
        }
    }

    /// The rev the replay runs to, captured on the first read.
    pub fn snapshot_rev(&self) -> Option<Rev> {
        self.snapshot_rev
    }

    /// Whether the replay is done: every page from here on is live.
    pub fn caught_up(&self) -> bool {
        self.snapshot_rev
            .is_some_and(|snapshot_rev| self.after >= snapshot_rev)
    }

    /// Fill `page` with the next window. Returns once the page advances `through`, waiting for
    /// a commit when the stream is caught up and nothing new is committed.
    pub async fn next_into(
        &mut self,
        page: &mut Page<S::Key, S::Value>,
        limits: Limits,
    ) -> Res<()> {
        let snapshot_rev = match self.snapshot_rev {
            Some(snapshot_rev) => snapshot_rev,
            None => {
                let snapshot_rev = self.source.latest_rev().await?;
                self.snapshot_rev = Some(snapshot_rev);
                snapshot_rev
            }
        };
        let until = if self.after < snapshot_rev {
            snapshot_rev
        } else {
            let latest = self.watch.latest();
            if latest > self.after {
                latest
            } else {
                self.watch.wait_past(self.after).await
            }
        };
        self.source
            .read(&self.selector, self.after, until, limits, page)
            .await?;
        assert!(
            page.through > self.after && page.through <= until,
            "source read of ({after}, {until}] reported through {through}",
            after = self.after,
            through = page.through,
        );
        self.after = page.through;
        Ok(())
    }
}

/// Projections over a [`RevSource`] that keep its revs, selector and watch.
pub trait RevSourceExt: RevSource + Sized {
    /// Turn each value into another. Tombstones stay tombstones.
    fn map<W, F>(
        self,
        map: F,
    ) -> impl RevSource<Key = Self::Key, Value = W, Selector = Self::Selector>
    where
        W: Send + Sync,
        F: Fn(&Self::Key, Self::Value) -> W + Send + Sync,
    {
        Projection {
            source: self,
            project: move |Entry { key, rev, value }: Entry<Self::Key, Self::Value>| {
                let value = value.map(|value| map(&key, value));
                Some(Entry { key, rev, value })
            },
            scratch: surelock::mutex::Mutex::new(None),
        }
    }

    /// Turn each value into another, or into a tombstone with `None`.
    ///
    /// A value the closure refuses becomes a tombstone for its key at its rev rather than
    /// disappearing: the closure may look at the value, so a key can be kept at one rev and
    /// refused at a later one, and a consumer folding pages into a view must hear about the
    /// later rev to forget the earlier value. Tombstones stay tombstones, and a consumer treats
    /// one for a key it never kept as a no-op. Pages keep their size; to drop entries, filter on
    /// the key with [`filter_keys`](Self::filter_keys).
    fn filter_map<W, F>(
        self,
        filter_map: F,
    ) -> impl RevSource<Key = Self::Key, Value = W, Selector = Self::Selector>
    where
        W: Send + Sync,
        F: Fn(&Self::Key, Self::Value) -> Option<W> + Send + Sync,
    {
        Projection {
            source: self,
            project: move |Entry { key, rev, value }: Entry<Self::Key, Self::Value>| {
                let value = value.and_then(|value| filter_map(&key, value));
                Some(Entry { key, rev, value })
            },
            scratch: surelock::mutex::Mutex::new(None),
        }
    }

    /// Keep only the entries of keys `keep` accepts, tombstones included; the rest are dropped
    /// from the page, which still reports its progress.
    ///
    /// Dropping is safe here, unlike in `filter_map`, because the verdict depends on the key
    /// alone: a key is kept at every rev or at none, so no consumer ever holds a row that a
    /// dropped entry should have replaced.
    fn filter_keys<F>(
        self,
        keep: F,
    ) -> impl RevSource<Key = Self::Key, Value = Self::Value, Selector = Self::Selector>
    where
        F: Fn(&Self::Key) -> bool + Send + Sync,
    {
        Projection {
            source: self,
            project: move |entry: Entry<Self::Key, Self::Value>| keep(&entry.key).then_some(entry),
            scratch: surelock::mutex::Mutex::new(None),
        }
    }
}

impl<S: RevSource> RevSourceExt for S {}

/// Every combinator: reads the inner source into a scratch page and projects it into the
/// caller's, entry by entry; `project` keeps each entry's key and rev and may drop the entry.
/// The scratch page is parked in `scratch` between reads so its capacity is reused; the lock is
/// held only to take or return it, never across the inner read, so concurrent reads each work
/// on their own page (a second one in flight allocates its own).
struct Projection<S: RevSource, F> {
    source: S,
    project: F,
    scratch: surelock::mutex::Mutex<Option<Page<S::Key, S::Value>>>,
}

#[async_trait]
impl<S, F, W> RevSource for Projection<S, F>
where
    S: RevSource,
    W: Send + Sync,
    F: Fn(Entry<S::Key, S::Value>) -> Option<Entry<S::Key, W>> + Send + Sync,
{
    type Key = S::Key;
    type Value = W;
    type Selector = S::Selector;

    async fn latest_rev(&self) -> Res<Rev> {
        self.source.latest_rev().await
    }

    async fn read(
        &self,
        selector: &Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<Self::Key, Self::Value>,
    ) -> Res<()> {
        let mut scratch = surelock::key::lock_scope(|key| {
            let (mut slot, _key) = key.lock(&self.scratch);
            slot.take()
        })
        .unwrap_or_default();
        self.source
            .read(selector, after, until, limits, &mut scratch)
            .await?;
        page.entries.clear();
        page.through = scratch.through;
        page.entries
            .extend(scratch.entries.drain(..).filter_map(&self.project));
        surelock::key::lock_scope(|key| {
            let (mut slot, _key) = key.lock(&self.scratch);
            *slot = Some(scratch);
        });
        Ok(())
    }

    fn watch(&self) -> RevWatch {
        self.source.watch()
    }
}

/// A shared source is a source: lets a projection, whose type can't be named, be held as
/// `Arc<dyn RevSource<..>>`.
#[async_trait]
impl<S: RevSource + ?Sized> RevSource for Arc<S> {
    type Key = S::Key;
    type Value = S::Value;
    type Selector = S::Selector;

    async fn latest_rev(&self) -> Res<Rev> {
        (**self).latest_rev().await
    }

    async fn read(
        &self,
        selector: &Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<Self::Key, Self::Value>,
    ) -> Res<()> {
        (**self).read(selector, after, until, limits, page).await
    }

    fn watch(&self) -> RevWatch {
        (**self).watch()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod laws;

#[cfg(test)]
mod tests;
