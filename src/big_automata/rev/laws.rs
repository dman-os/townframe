//! The Tail laws: what a [`RevStream`] over any [`RevSource`] guarantees, as a harness the
//! sources' own tests call.
//!
//! A source under test supplies a [`StreamBackend`]: a way to commit okv-shaped batches through
//! the source's own write path (the commit hook), and what the source must return for a read
//! given the okv [`Oracle`] of everything committed (the oracle mapping). An okv store's mapping
//! is the oracle's read; a projection's maps that read; a source with its own model derives its
//! pages from the log. The hook must commit each batch as one rev, numbered as the oracle
//! numbers them.
//!
//! | Law | Where |
//! | --- | --- |
//! | the snapshot is the latest rev at the first read and never moves | every step of [`stream_follows`] |
//! | replay pages stay at or below the snapshot; `caught_up` flips once and stays | every step |
//! | each page is the source's read of the window the stream must have used | every step |
//! | live delivers every later commit; resuming from any `after` loses nothing | the final view of [`stream_follows`] |
//! | commits during replay, during a read, and over an unread key | [`stream_commit_during_replay`], [`stream_commit_during_read`], [`stream_overwrite_of_unread_key`] |
//! | a projection's visibility follows the key's current value, both orders, replay and live | [`stream_follows_current_state`] |

use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use proptest::prelude::*;

use crate::interlude::*;
use crate::okv::Mutation;
use crate::okv::Selector;
use crate::okv::laws::Bytes;
use crate::okv::laws::put;
use crate::okv::laws::selector;
use crate::okv::laws::transaction;
use crate::okv::oracle::Oracle;
use crate::rev::Entry;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::rev::RevStream;
use crate::rev::RevWatch;

type Batches = Vec<Vec<Mutation<Bytes, Bytes>>>;
type Key<B> = <<B as StreamBackend>::Source as RevSource>::Key;
type Value<B> = <<B as StreamBackend>::Source as RevSource>::Value;

/// What a source supplies to run the stream laws.
#[async_trait]
pub trait StreamBackend: Clone + Send + Sync + 'static {
    type Source: RevSource<Key: Ord + Clone + Debug, Value: Clone + PartialEq + Debug>
        + Clone
        + 'static;
    /// Whatever the commit hook writes through.
    type Writer: Clone + Send + Sync + 'static;

    /// A new source with nothing committed, and its writer.
    async fn open(&self) -> Res<(Self::Source, Self::Writer)>;
    /// Commit `batches` in one transaction, one rev per batch.
    async fn commit(&self, writer: &Self::Writer, batches: Batches) -> Res<()>;
    /// Stage `batches` in one transaction and roll it back.
    async fn rollback(&self, writer: &Self::Writer, batches: Batches) -> Res<()>;
    /// The source's selector for an okv selector over the written keys.
    fn selector(&self, selector: &Selector<Bytes>) -> <Self::Source as RevSource>::Selector;
    /// The page the source must return for a read of `(after, until]`, given the log.
    fn expected(
        &self,
        oracle: &Oracle<Bytes, Bytes>,
        selector: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Page<Key<Self>, Value<Self>>;
}

strike! {
    #[structstruck::each[derive(Debug, Clone)]]
    /// A [`RevStream`] run: commits made before it opens, then steps interleaving commits with
    /// reads.
    pub struct StreamScript {
        pub selector: Selector<Bytes>,
        pub seed: Vec<Batches>,
        /// Where the stream starts, as a fraction (of 256) of the seeded revs, inclusive.
        pub after: u8,
        pub steps: Vec<pub enum StreamStep {
            Commit(Batches),
            Rollback(Batches),
            /// Read the next page, when there is one to read.
            Next(Limits),
            /// Drop the stream and open a new one at the consumer's progress.
            Resume,
        }>,
    }
}

/// A source, its writer, and the log it must agree with.
struct StreamHarness<B: StreamBackend> {
    backend: B,
    source: B::Source,
    writer: B::Writer,
    oracle: Oracle<Bytes, Bytes>,
}

impl<B: StreamBackend> StreamHarness<B> {
    async fn open(backend: &B) -> Res<Self> {
        let (source, writer) = backend.open().await?;
        assert_eq!(
            source.latest_rev().await?,
            0,
            "a fresh source has committed nothing"
        );
        Ok(Self {
            backend: backend.clone(),
            source,
            writer,
            oracle: default(),
        })
    }

    async fn commit(&mut self, batches: Batches) -> Res<()> {
        self.backend.commit(&self.writer, batches.clone()).await?;
        for batch in batches {
            self.oracle.commit(batch);
        }
        assert_eq!(self.source.latest_rev().await?, self.oracle.latest_rev());
        Ok(())
    }

    async fn rollback(&mut self, batches: Batches) -> Res<()> {
        self.backend.rollback(&self.writer, batches).await?;
        assert_eq!(self.source.latest_rev().await?, self.oracle.latest_rev());
        Ok(())
    }

    /// Key `n` written alone at rev `n`, valued with its own digit, for `n` in `1..=count`.
    async fn seed_one_key_per_rev(&mut self, count: u8) -> Res<()> {
        for key in 1..=count {
            self.commit(vec![vec![put(key, &[b'0' + key])]]).await?;
        }
        Ok(())
    }

    fn stream(&self, selector: &Selector<Bytes>, after: Rev) -> RevStream<B::Source> {
        RevStream::new(self.source.clone(), self.backend.selector(selector), after)
    }

    /// The live rows a consumer that started at `start` holds once it has read to the latest
    /// rev: the latest row of every key the source has after `start`, where that row has a value.
    ///
    /// Views are compared on live rows only. To a consumer a tombstone and an absent key are the
    /// same, and a projection can hand out a tombstone for a key whose later rows it drops (a
    /// delete passes through `filter_map`, the put after it may not), which a one-shot read of
    /// the same window does not show. Every page is still compared exactly, tombstones included,
    /// and a missed delete would leave a stale live row here.
    fn expected_view(
        &self,
        selector: &Selector<Bytes>,
        start: Rev,
    ) -> BTreeMap<Key<B>, Entry<Key<B>, Value<B>>> {
        let latest = self.oracle.latest_rev();
        if start >= latest {
            return default();
        }
        self.backend
            .expected(
                &self.oracle,
                selector,
                start,
                latest,
                limits(usize::MAX, None),
            )
            .entries
            .into_iter()
            .filter(|entry| entry.value.is_some())
            .map(|entry| (entry.key.clone(), entry))
            .collect()
    }
}

/// The stream's consumer: folds pages into its view and checks each against the laws.
struct Consumer<B: StreamBackend> {
    start: Rev,
    progress: Rev,
    view: BTreeMap<Key<B>, Entry<Key<B>, Value<B>>>,
    page: Page<Key<B>, Value<B>>,
}

impl<B: StreamBackend> Consumer<B> {
    fn new(start: Rev) -> Self {
        Self {
            start,
            progress: start,
            view: default(),
            page: default(),
        }
    }

    /// The view's rows that have a value; see [`StreamHarness::expected_view`].
    fn live_view(&self) -> BTreeMap<Key<B>, Entry<Key<B>, Value<B>>> {
        self.view
            .iter()
            .filter(|(_, entry)| entry.value.is_some())
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect()
    }

    /// Read the next page and check it. `race` is a batch the source commits inside this read
    /// (see [`Racing`]); it is mirrored into the oracle once the read returns.
    async fn next<S>(
        &mut self,
        stream: &mut RevStream<S>,
        harness: &mut StreamHarness<B>,
        selector: &Selector<Bytes>,
        limits: Limits,
        race: Option<Vec<Mutation<Bytes, Bytes>>>,
    ) -> Res<()>
    where
        S: RevSource<Key = Key<B>, Value = Value<B>>,
    {
        let latest = harness.oracle.latest_rev();
        let snapshot_before = stream.snapshot_rev();
        let caught_up_before = stream.caught_up();
        stream.next_into(&mut self.page, limits).await?;
        if let Some(race) = race {
            harness.oracle.commit(race);
        }

        let snapshot_rev = stream.snapshot_rev().expect("a read captures the snapshot");
        if let Some(before) = snapshot_before {
            assert_eq!(snapshot_rev, before, "the snapshot moved");
        } else {
            assert_eq!(
                snapshot_rev, latest,
                "the snapshot is the latest rev at the first read"
            );
        }
        if caught_up_before {
            assert!(stream.caught_up(), "caught_up flipped back");
        }
        assert_eq!(stream.caught_up(), self.page.through >= snapshot_rev);
        // Replay reads up to the snapshot; live reads up to the latest commit the stream saw.
        let until = if self.progress < snapshot_rev {
            snapshot_rev
        } else {
            latest
        };
        assert_eq!(
            self.page,
            harness
                .backend
                .expected(&harness.oracle, selector, self.progress, until, limits),
            "page after {progress} until {until}",
            progress = self.progress
        );
        self.progress = self.page.through;
        for entry in self.page.entries.drain(..) {
            self.view.insert(entry.key.clone(), entry);
        }
        Ok(())
    }
}

/// The Tail laws over an arbitrary run: see the module table.
pub async fn stream_follows<B: StreamBackend>(backend: &B, script: StreamScript) -> Res<()> {
    let StreamScript {
        selector,
        seed,
        after,
        steps,
    } = script;
    let mut harness = StreamHarness::open(backend).await?;
    for batches in seed {
        harness.commit(batches).await?;
    }
    let start = u64::from(after) * (harness.oracle.latest_rev() + 1) / 256;
    let mut consumer = Consumer::<B>::new(start);
    let mut stream = harness.stream(&selector, start);
    for step in steps {
        match step {
            StreamStep::Commit(batches) => harness.commit(batches).await?,
            StreamStep::Rollback(batches) => harness.rollback(batches).await?,
            StreamStep::Next(limits) => {
                if consumer.progress < harness.oracle.latest_rev() {
                    consumer
                        .next(&mut stream, &mut harness, &selector, limits, None)
                        .await?;
                }
            }
            StreamStep::Resume => stream = harness.stream(&selector, consumer.progress),
        }
    }
    while consumer.progress < harness.oracle.latest_rev() {
        consumer
            .next(&mut stream, &mut harness, &selector, limits(2, None), None)
            .await?;
    }
    assert_eq!(
        consumer.live_view(),
        harness.expected_view(&selector, consumer.start)
    );
    Ok(())
}

/// Commits land during replay: the stream finishes the replay at its snapshot and delivers them
/// live.
pub async fn stream_commit_during_replay<B: StreamBackend>(backend: &B) -> Res<()> {
    let mut harness = StreamHarness::open(backend).await?;
    harness.seed_one_key_per_rev(4).await?;
    let all = Selector::All;
    let mut stream = harness.stream(&all, 0);
    let mut consumer = Consumer::<B>::new(0);

    consumer
        .next(&mut stream, &mut harness, &all, limits(2, None), None)
        .await?;
    assert_eq!((consumer.progress, stream.snapshot_rev()), (2, Some(4)));

    harness
        .commit(vec![vec![put(5, b"5")], vec![put(6, b"6")]])
        .await?;
    consumer
        .next(&mut stream, &mut harness, &all, limits(2, None), None)
        .await?;
    assert_eq!(consumer.progress, 4);
    assert!(stream.caught_up());

    consumer
        .next(&mut stream, &mut harness, &all, limits(2, None), None)
        .await?;
    assert_eq!(consumer.progress, 6);
    assert_eq!(consumer.live_view(), harness.expected_view(&all, 0));
    Ok(())
}

/// A key the replay has not reached yet is overwritten: the replay skips its old row and the new
/// one arrives live, at its new rev.
pub async fn stream_overwrite_of_unread_key<B: StreamBackend>(backend: &B) -> Res<()> {
    let mut harness = StreamHarness::open(backend).await?;
    harness.seed_one_key_per_rev(4).await?;
    let all = Selector::All;
    let mut stream = harness.stream(&all, 0);
    let mut consumer = Consumer::<B>::new(0);

    consumer
        .next(&mut stream, &mut harness, &all, limits(2, None), None)
        .await?;
    harness.commit(vec![vec![put(3, b"x")]]).await?;
    consumer
        .next(&mut stream, &mut harness, &all, limits(2, None), None)
        .await?;
    assert!(stream.caught_up());
    assert!(
        consumer.view.values().all(|entry| entry.rev != 3),
        "the replay handed out the overwritten row"
    );

    consumer
        .next(&mut stream, &mut harness, &all, limits(2, None), None)
        .await?;
    assert_eq!(consumer.progress, 5);
    assert_eq!(consumer.live_view(), harness.expected_view(&all, 0));
    Ok(())
}

/// A commit lands inside a read, after the stream fixed the window: the read keeps to the
/// window, the overwritten key leaves it, and the commit arrives on the next read.
pub async fn stream_commit_during_read<B: StreamBackend>(backend: &B) -> Res<()> {
    let mut harness = StreamHarness::open(backend).await?;
    harness.seed_one_key_per_rev(3).await?;
    let all = Selector::All;
    let race = vec![put(2, b"x"), put(9, b"y")];
    let racing = Racing::new(&harness, race.clone());
    let mut stream = RevStream::new(racing, backend.selector(&all), 0);
    let mut consumer = Consumer::<B>::new(0);

    consumer
        .next(
            &mut stream,
            &mut harness,
            &all,
            limits(10, None),
            Some(race),
        )
        .await?;
    assert_eq!((consumer.progress, stream.snapshot_rev()), (3, Some(3)));
    assert!(stream.caught_up());
    assert!(
        consumer.view.values().all(|entry| entry.rev != 2),
        "the read handed out a row its commit moved out of the window"
    );

    consumer
        .next(&mut stream, &mut harness, &all, limits(10, None), None)
        .await?;
    assert_eq!(consumer.progress, 4);
    assert_eq!(consumer.live_view(), harness.expected_view(&all, 0));
    Ok(())
}

/// When a projection reads one key's writes, which ones become visible.
#[derive(Debug, Clone, Copy)]
enum Phase {
    /// Both writes commit before the stream opens: the replay sees only the second.
    BeforeOpen,
    /// The replay hands out the first write, then the second commits before it is caught up.
    DuringReplay,
    /// Both writes commit after the stream is caught up.
    Live,
}

/// For a projection that keeps the value `kept` and refuses the value `dropped`: what a consumer
/// sees follows the key's *current* value, in both orders (kept then dropped, dropped then
/// kept) and in every [`Phase`]. Every page is compared with the oracle mapping, and at the end
/// the key is live in the consumer's view exactly when its last write was `kept`.
///
/// For projections only: on a source that keeps every value the dropped write stays visible and
/// the last check fails.
pub async fn stream_follows_current_state<B: StreamBackend>(
    backend: &B,
    kept: &[u8],
    dropped: &[u8],
) -> Res<()> {
    for (first, second) in [(kept, dropped), (dropped, kept)] {
        for phase in [Phase::BeforeOpen, Phase::DuringReplay, Phase::Live] {
            let mut harness = StreamHarness::open(backend).await?;
            let all = Selector::All;
            let mut consumer = Consumer::<B>::new(0);
            let one = limits(1, None);
            // A second key, always refused, gives the replay a rev past the key's first write.
            let filler = vec![vec![put(2, dropped)]];
            let stream = match phase {
                Phase::BeforeOpen => {
                    harness.commit(vec![vec![put(1, first)]]).await?;
                    harness.commit(vec![vec![put(1, second)]]).await?;
                    harness.commit(filler).await?;
                    harness.stream(&all, 0)
                }
                Phase::DuringReplay => {
                    harness.commit(vec![vec![put(1, first)]]).await?;
                    harness.commit(filler).await?;
                    let mut stream = harness.stream(&all, 0);
                    consumer
                        .next(&mut stream, &mut harness, &all, one, None)
                        .await?;
                    assert!(!stream.caught_up(), "{phase:?}: the replay ended early");
                    harness.commit(vec![vec![put(1, second)]]).await?;
                    stream
                }
                Phase::Live => {
                    harness.commit(filler).await?;
                    let mut stream = harness.stream(&all, 0);
                    consumer
                        .next(&mut stream, &mut harness, &all, one, None)
                        .await?;
                    assert!(stream.caught_up());
                    harness.commit(vec![vec![put(1, first)]]).await?;
                    consumer
                        .next(&mut stream, &mut harness, &all, one, None)
                        .await?;
                    harness.commit(vec![vec![put(1, second)]]).await?;
                    stream
                }
            };
            let mut stream = stream;
            while consumer.progress < harness.oracle.latest_rev() {
                consumer
                    .next(&mut stream, &mut harness, &all, one, None)
                    .await?;
            }
            let live = consumer.live_view();
            assert_eq!(live, harness.expected_view(&all, 0), "{phase:?}");
            assert_eq!(
                live.len(),
                usize::from(second == kept),
                "{phase:?}: the key must be live exactly when its last write is kept"
            );
        }
    }
    Ok(())
}

type RaceCommit = Pin<Box<dyn Future<Output = Res<()>> + Send>>;

/// A source that commits one batch inside its next read, after the stream has fixed the read's
/// window and before the source reads it.
struct Racing<S> {
    inner: S,
    race: surelock::mutex::Mutex<Option<RaceCommit>>,
}

impl<S: RevSource + Clone> Racing<S> {
    fn new<B: StreamBackend<Source = S>>(
        harness: &StreamHarness<B>,
        batch: Vec<Mutation<Bytes, Bytes>>,
    ) -> Self {
        let backend = harness.backend.clone();
        let writer = harness.writer.clone();
        let race: RaceCommit = Box::pin(async move { backend.commit(&writer, vec![batch]).await });
        Self {
            inner: harness.source.clone(),
            race: surelock::mutex::Mutex::new(Some(race)),
        }
    }
}

#[async_trait]
impl<S: RevSource> RevSource for Racing<S> {
    type Key = S::Key;
    type Value = S::Value;
    type Selector = S::Selector;

    async fn latest_rev(&self) -> Res<Rev> {
        self.inner.latest_rev().await
    }

    async fn read(
        &self,
        selector: &Self::Selector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<Self::Key, Self::Value>,
    ) -> Res<()> {
        let race = surelock::key::lock_scope(|key| {
            let (mut race, _key) = key.lock(&self.race);
            race.take()
        });
        if let Some(race) = race {
            race.await?;
        }
        self.inner.read(selector, after, until, limits, page).await
    }

    fn watch(&self) -> RevWatch {
        self.inner.watch()
    }
}

/// Polls a future by hand to observe what an executor hides: whether it is pending, and how
/// many times it was woken.
pub struct WakeCount {
    count: Arc<WakeCounter>,
    waker: Waker,
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Default for WakeCount {
    fn default() -> Self {
        let count = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        Self { count, waker }
    }
}

impl WakeCount {
    pub fn poll<F: Future>(&self, future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(&self.waker))
    }

    pub fn count(&self) -> usize {
        self.count.0.load(Ordering::SeqCst)
    }
}

pub fn stream_scripts() -> impl Strategy<Value = StreamScript> {
    let step = prop_oneof![
        2 => transaction().prop_map(StreamStep::Commit),
        1 => transaction().prop_map(StreamStep::Rollback),
        3 => any_limits().prop_map(StreamStep::Next),
        1 => Just(StreamStep::Resume),
    ];
    (
        selector(),
        proptest::collection::vec(transaction(), 0..6),
        any::<u8>(),
        proptest::collection::vec(step, 0..30),
    )
        .prop_map(|(selector, seed, after, steps)| StreamScript {
            selector,
            seed,
            after,
            steps,
        })
}

pub(crate) fn any_limits() -> impl Strategy<Value = Limits> {
    (1usize..5, proptest::option::of(1usize..8))
        .prop_map(|(max_entries, max_bytes)| limits(max_entries, max_bytes))
}

pub fn limits(max_entries: usize, max_bytes: Option<usize>) -> Limits {
    Limits {
        max_entries: NonZeroUsize::new(max_entries).expect("max_entries is at least 1"),
        max_bytes: max_bytes
            .map(|max_bytes| NonZeroUsize::new(max_bytes).expect("max_bytes is at least 1")),
    }
}
