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

use std::marker::PhantomData;
use std::num::NonZeroUsize;

use future_form::Local;
use future_form::Sendable;
use future_form::future_form;

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
///
/// Generic over the [`FutureForm`]: `Sendable` for multi-threaded runtimes, `Local` for the
/// single-threaded web node. Implementations write one body for both with `#[future_form]`.
pub trait RevSource<Async: FutureForm> {
    type Key;
    type Value;
    /// Which keys a read is about. Sources pick their own shape.
    type Selector;

    fn latest_rev(&self) -> Async::Future<'_, Res<Rev>>;

    /// Clear `page` and fill it with the current rows of the selected keys whose rev is in
    /// `(after, until]`, bounded by `limits` (see [`Page`] and [`Limits`] for the shape).
    ///
    /// `page.through` is `until` when the page holds every matching row up to `until` (an empty
    /// page then reports progress over a window with nothing selected), and otherwise the rev
    /// of the page's last entry.
    ///
    /// The caller guarantees `after < until` and that `until` is a rev it saw committed (from
    /// `latest_rev` or the watch); a source may assert both.
    fn read<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut Page<Self::Key, Self::Value>,
    ) -> Async::Future<'a, Res<()>>;

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
pub struct RevStream<Async: FutureForm, S: RevSource<Async>> {
    source: S,
    selector: S::Selector,
    /// Progress: everything selected at or below this has been handed out.
    after: Rev,
    snapshot_rev: Option<Rev>,
    watch: RevWatch,
    _form: PhantomData<Async>,
}

impl<Async: FutureForm, S: RevSource<Async>> RevStream<Async, S> {
    pub fn new(source: S, selector: S::Selector, after: Rev) -> Self {
        let watch = source.watch();
        Self {
            source,
            selector,
            after,
            snapshot_rev: None,
            watch,
            _form: PhantomData,
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
///
/// Generic over the form only so that it extends sources and nothing else; a store that
/// implements both forms names one, `RevSourceExt::<Sendable>::map(store, ..)`.
///
/// Each returns a named type, as `Iterator`'s adapters do, so a projection is `Send` and `Sync`
/// exactly when its source and closure are: an opaque return type would hide that, and the
/// `Local` form must not demand it of a `!Send` source.
pub trait RevSourceExt<Async: FutureForm>: RevSource<Async> + Sized {
    /// Turn each value into another. Tombstones stay tombstones.
    fn map<F>(self, map: F) -> RevMap<Self, F> {
        RevMap(Projection::new(self, map))
    }

    /// Turn each value into another, or into a tombstone with `None`.
    ///
    /// A value the closure refuses becomes a tombstone for its key at its rev rather than
    /// disappearing: the closure may look at the value, so a key can be kept at one rev and
    /// refused at a later one, and a consumer folding pages into a view must hear about the
    /// later rev to forget the earlier value. Tombstones stay tombstones, and a consumer treats
    /// one for a key it never kept as a no-op. Pages keep their size; to drop entries, filter on
    /// the key with [`filter_keys`](Self::filter_keys).
    fn filter_map<F>(self, filter_map: F) -> RevFilterMap<Self, F> {
        RevFilterMap(Projection::new(self, filter_map))
    }

    /// Keep only the entries of keys `keep` accepts, tombstones included; the rest are dropped
    /// from the page, which still reports its progress.
    ///
    /// Dropping is safe here, unlike in `filter_map`, because the verdict depends on the key
    /// alone: a key is kept at every rev or at none, so no consumer ever holds a row that a
    /// dropped entry should have replaced.
    fn filter_keys<F>(self, keep: F) -> RevFilterKeys<Self, F> {
        RevFilterKeys(Projection::new(self, keep))
    }
}

impl<Async: FutureForm, S: RevSource<Async>> RevSourceExt<Async> for S {}

/// [`RevSourceExt::map`].
pub struct RevMap<S, F>(Projection<S, F>);
/// [`RevSourceExt::filter_map`].
pub struct RevFilterMap<S, F>(Projection<S, F>);
/// [`RevSourceExt::filter_keys`].
pub struct RevFilterKeys<S, F>(Projection<S, F>);

/// What every combinator shares: the inner source, the user's closure, and a scratch page.
///
/// A read fills the scratch page from the inner source and projects it into the caller's page
/// entry by entry. The scratch page is parked in `scratch` between reads so its capacity is
/// reused; the lock is held only to take or return it, never across the inner read, so
/// concurrent reads each work on their own page (a second one in flight allocates its own).
/// The scratch is type-erased so the projection's type need not name the inner key and value.
struct Projection<S, F> {
    source: S,
    project: F,
    scratch: surelock::mutex::Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

impl<S, F> Projection<S, F> {
    fn new(source: S, project: F) -> Self {
        Self {
            source,
            project,
            scratch: surelock::mutex::Mutex::new(None),
        }
    }

    /// Read the inner window into the scratch page and project it into `page` with `step`.
    async fn read<Async, W>(
        &self,
        selector: &S::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<S::Key, W>,
        step: impl FnMut(Entry<S::Key, S::Value>) -> Option<Entry<S::Key, W>>,
    ) -> Res<()>
    where
        Async: FutureForm,
        S: RevSource<Async>,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
    {
        let mut scratch = surelock::key::lock_scope(|key| {
            let (mut slot, _key) = key.lock(&self.scratch);
            slot.take()
        })
        .map_or_else(Page::default, |scratch| {
            *scratch
                .downcast::<Page<S::Key, S::Value>>()
                .expect(ERROR_IMPOSSIBLE)
        });
        self.source
            .read(selector, after, until, limits, &mut scratch)
            .await?;
        page.entries.clear();
        page.through = scratch.through;
        page.entries
            .extend(scratch.entries.drain(..).filter_map(step));
        surelock::key::lock_scope(|key| {
            let (mut slot, _key) = key.lock(&self.scratch);
            *slot = Some(Box::new(scratch));
        });
        Ok(())
    }
}

#[future_form(
    Sendable where
        S: RevSource<Sendable> + Sync,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
        S::Selector: Sync,
        W: Send,
        F: Fn(&S::Key, S::Value) -> W + Sync,
    Local where
        S: RevSource<Local>,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
        F: Fn(&S::Key, S::Value) -> W
)]
impl<Async: FutureForm, S, F, W> RevSource<Async> for RevMap<S, F> {
    type Key = S::Key;
    type Value = W;
    type Selector = S::Selector;

    fn latest_rev(&self) -> Async::Future<'_, Res<Rev>> {
        self.0.source.latest_rev()
    }

    fn read<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut Page<Self::Key, Self::Value>,
    ) -> Async::Future<'a, Res<()>> {
        Async::from_future(self.0.read::<Async, W>(
            selector,
            after,
            until,
            limits,
            page,
            |Entry { key, rev, value }| {
                let value = value.map(|value| (self.0.project)(&key, value));
                Some(Entry { key, rev, value })
            },
        ))
    }

    fn watch(&self) -> RevWatch {
        self.0.source.watch()
    }
}

#[future_form(
    Sendable where
        S: RevSource<Sendable> + Sync,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
        S::Selector: Sync,
        W: Send,
        F: Fn(&S::Key, S::Value) -> Option<W> + Sync,
    Local where
        S: RevSource<Local>,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
        F: Fn(&S::Key, S::Value) -> Option<W>
)]
impl<Async: FutureForm, S, F, W> RevSource<Async> for RevFilterMap<S, F> {
    type Key = S::Key;
    type Value = W;
    type Selector = S::Selector;

    fn latest_rev(&self) -> Async::Future<'_, Res<Rev>> {
        self.0.source.latest_rev()
    }

    fn read<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut Page<Self::Key, Self::Value>,
    ) -> Async::Future<'a, Res<()>> {
        Async::from_future(self.0.read::<Async, W>(
            selector,
            after,
            until,
            limits,
            page,
            |Entry { key, rev, value }| {
                let value = value.and_then(|value| (self.0.project)(&key, value));
                Some(Entry { key, rev, value })
            },
        ))
    }

    fn watch(&self) -> RevWatch {
        self.0.source.watch()
    }
}

#[future_form(
    Sendable where
        S: RevSource<Sendable> + Sync,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
        S::Selector: Sync,
        F: Fn(&S::Key) -> bool + Sync,
    Local where
        S: RevSource<Local>,
        S::Key: Send + 'static,
        S::Value: Send + 'static,
        F: Fn(&S::Key) -> bool
)]
impl<Async: FutureForm, S, F> RevSource<Async> for RevFilterKeys<S, F> {
    type Key = S::Key;
    type Value = S::Value;
    type Selector = S::Selector;

    fn latest_rev(&self) -> Async::Future<'_, Res<Rev>> {
        self.0.source.latest_rev()
    }

    fn read<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut Page<Self::Key, Self::Value>,
    ) -> Async::Future<'a, Res<()>> {
        Async::from_future(self.0.read::<Async, S::Value>(
            selector,
            after,
            until,
            limits,
            page,
            |entry| (self.0.project)(&entry.key).then_some(entry),
        ))
    }

    fn watch(&self) -> RevWatch {
        self.0.source.watch()
    }
}

/// A shared source is a source: lets a source be held as `Arc<dyn RevSource<..>>`.
#[future_form(
    Sendable where S: RevSource<Sendable> + ?Sized,
    Local where S: RevSource<Local> + ?Sized
)]
impl<Async: FutureForm, S> RevSource<Async> for Arc<S> {
    type Key = S::Key;
    type Value = S::Value;
    type Selector = S::Selector;

    fn latest_rev(&self) -> Async::Future<'_, Res<Rev>> {
        (**self).latest_rev()
    }

    fn read<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut Page<Self::Key, Self::Value>,
    ) -> Async::Future<'a, Res<()>> {
        (**self).read(selector, after, until, limits, page)
    }

    fn watch(&self) -> RevWatch {
        (**self).watch()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod laws;

#[cfg(test)]
mod tests;
