//! The laws every okv backend obeys, as a harness the backends' own tests call.
//!
//! A backend supplies a [`Backend`] (open a fresh db, make tables in it, begin, commit and roll
//! back its transactions) and calls each law from a test of its own. Every check runs against
//! the [`Oracle`]: the harness mirrors each committed batch into it and compares whole pages.
//! The stream (Tail) laws are generic over any source and live in [`crate::rev::laws`];
//! [`OkvStreams`] runs them against an okv backend.
//!
//! | Law | What it says | Where |
//! | --- | --- | --- |
//! | L1 collapse | a key's row is its last mutation, at that mutation's rev | [`collapse`], [`differential`] |
//! | L2 order and bounds | `(rev, key)` order, entries in `(after, through]`, `after < through <= until` | every read |
//! | L3 atomic revisions | a page holds all of a revision's matching rows or none | every read |
//! | L4 completeness | pages swept from `after` to `latest` equal the oracle's window | [`differential`] (`Sweep`) |
//! | L5 soft limits | a page exceeds a limit only as one whole revision | every read, [`soft_limits`] |
//! | L6 tombstones | a delete reads as `None`; a later put comes back at a new rev | [`tombstones`] |
//! | L7 revs | strictly increasing, one per batch; an empty batch panics | every commit, [`revs_strictly_increase`], [`empty_batch_panics`] |
//! | L8 rollback | nothing visible, no rev consumed, no wakeup | every rollback, [`rollback_leaves_no_trace`] |
//! | L9 wakeups | the watch moves after commit, never before | every commit, [`wakeups_follow_commit`] |
//! | get | a transaction reads its own staged writes (a staged delete as `None`), then the committed row | before every batch, [`get_reads_your_writes`] |
//! | tables | tables written in one transaction become visible and wake together, and neither on rollback | [`tables_commit_together`] |
//! | P1 register | registering is idempotent and returns the stored cursor, `0` when new | [`consumer_cursors`], [`differential`] |
//! | P2 report | a cursor only moves forward; unregistered consumer or cursor past the latest rev panics | [`consumer_cursors`], [`report_past_the_latest_rev_panics`], [`report_unregistered_panics`] |
//! | P3 prune | drops exactly the tombstones at or below the lowest registered cursor, never a live row, nothing without consumers | [`prune_by_lowest_cursor`], [`differential`] |
//! | P4 prune is silent | no rev consumed, no wakeup, reads equal the oracle's | [`prune_by_lowest_cursor`], [`differential`] |
//!
//! The laws are async and executor agnostic; the one place they poll by hand
//! ([`WakeCount`]) is to see whether a wait is pending or was woken, which no executor reports.

use std::num::NonZeroUsize;
use std::ops::Bound;
use std::pin::pin;
use std::task::Poll;

use proptest::prelude::*;

use crate::interlude::*;
use crate::okv::KeyRange;
use crate::okv::Mutation;
use crate::okv::OkvStore;
use crate::okv::Pruned;
use crate::okv::Selector;
use crate::okv::oracle::Oracle;
use crate::okv::row_bytes;
use crate::rev::Entry;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::rev::RevWatch;
use crate::rev::laws::StreamBackend;
use crate::rev::laws::WakeCount;
use crate::rev::laws::any_limits;
use crate::rev::laws::limits;

pub type Bytes = Vec<u8>;
type Batches = Vec<Vec<Mutation<Bytes, Bytes>>>;
type Tx<B> = <<B as Backend>::Store as OkvStore>::Tx;

/// What a backend supplies to run the laws: a fresh db per law, tables in it, and its
/// transactions, which span the db's tables.
#[async_trait]
pub trait Backend: Clone + Send + Sync + 'static {
    type Db: Clone + Send + Sync + 'static;
    type Store: OkvStore<Key = Bytes, Value = Bytes, Tx: Send> + Clone + Send + Sync + 'static;

    /// A new, empty db, sharing nothing with any other.
    async fn open(&self) -> Res<Self::Db>;
    /// A new, empty table in `db`.
    async fn table(&self, db: &Self::Db) -> Res<Self::Store>;
    async fn begin(&self, db: &Self::Db) -> Res<Tx<Self>>;
    async fn commit(&self, tx: Tx<Self>) -> Res<()>;
    async fn rollback(&self, tx: Tx<Self>) -> Res<()>;
}

strike! {
    #[structstruck::each[derive(Debug, Clone)]]
    /// One step of the differential run.
    pub enum Op {
        /// A transaction of one or more batches that commits.
        Commit(Batches),
        /// A transaction of one or more batches that rolls back.
        Rollback(Batches),
        /// One read. `after` and `until` are fractions (of 256) of the committed revs, mapped
        /// onto a valid window `after < until <= latest`.
        Read {
            selector: Selector<Bytes>,
            after: u8,
            until: u8,
            limits: Limits,
        },
        /// Page from `after` (a fraction of the committed revs) to the latest rev.
        Sweep {
            selector: Selector<Bytes>,
            after: u8,
            limits: Limits,
        },
        /// Register consumer `n % CONSUMERS`.
        Register(u8),
        /// Report consumer `n % CONSUMERS` (if registered) at a fraction (of 256) of the
        /// committed revs, the latest rev included.
        Report { consumer: u8, cursor: u8 },
        Unregister(u8),
        Prune,
    }
}

const CONSUMERS: u8 = 3;

fn consumer_name(consumer: u8) -> String {
    format!("consumer-{}", consumer % CONSUMERS)
}

/// A store and the oracle it must agree with.
struct Harness<B: Backend> {
    backend: B,
    db: B::Db,
    store: B::Store,
    watch: RevWatch,
    oracle: Oracle<Bytes, Bytes>,
}

impl<B: Backend> Harness<B> {
    async fn open(backend: &B) -> Res<Self> {
        let db = backend.open().await?;
        let store = backend.table(&db).await?;
        let watch = store.watch();
        assert_eq!(
            store.latest_rev().await?,
            0,
            "a fresh store has committed nothing"
        );
        Ok(Self {
            backend: backend.clone(),
            db,
            store,
            watch,
            oracle: default(),
        })
    }

    /// Commit `batches` in one transaction and mirror them into the oracle. Checks L7, L9 and
    /// read-your-writes.
    async fn commit(&mut self, batches: Batches) -> Res<Vec<Rev>> {
        let committed_before = self.oracle.latest_rev();
        let mut tx = self.backend.begin(&self.db).await?;
        let revs = self.stage(&mut tx, &batches).await?;
        assert_eq!(
            self.watch.latest(),
            committed_before,
            "the watch moved before commit"
        );
        self.backend.commit(tx).await?;
        let expected = batches
            .into_iter()
            .map(|batch| self.oracle.commit(batch))
            .collect::<Vec<_>>();
        assert_eq!(revs, expected, "one rev per batch, strictly increasing");
        assert_eq!(self.store.latest_rev().await?, self.oracle.latest_rev());
        assert_eq!(
            self.watch.latest(),
            self.oracle.latest_rev(),
            "the watch did not move to the committed rev"
        );
        Ok(revs)
    }

    /// Apply `batches` in one transaction and roll it back. Checks L8 and read-your-writes.
    async fn rollback(&mut self, batches: Batches) -> Res<()> {
        let latest = self.oracle.latest_rev();
        let mut tx = self.backend.begin(&self.db).await?;
        self.stage(&mut tx, &batches).await?;
        self.backend.rollback(tx).await?;
        assert_eq!(
            self.store.latest_rev().await?,
            latest,
            "a rollback consumed a rev"
        );
        assert_eq!(self.watch.latest(), latest, "a rollback moved the watch");
        Ok(())
    }

    /// Apply each batch, first reading every key it writes through `get` and checking it sees
    /// the transaction's own staged writes over the committed rows.
    async fn stage(&self, tx: &mut Tx<B>, batches: &Batches) -> Res<Vec<Rev>> {
        let mut staged = BTreeMap::<Bytes, Option<Bytes>>::new();
        let mut revs = Vec::with_capacity(batches.len());
        for batch in batches {
            for mutation in batch {
                let (key, _) = mutation.clone().into_row();
                let expected = match staged.get(&key) {
                    Some(value) => value.clone(),
                    None => self.oracle.get(&key),
                };
                assert_eq!(self.store.get(tx, &key).await?, expected, "get of {key:?}");
            }
            revs.push(self.store.apply(tx, batch.clone()).await?);
            for mutation in batch {
                let (key, value) = mutation.clone().into_row();
                staged.insert(key, value);
            }
        }
        Ok(revs)
    }

    /// One read, checked against L2, L3, L5 and the oracle's page.
    async fn read(
        &self,
        selector: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Res<Page<Bytes, Bytes>> {
        let mut page = Page::default();
        self.store
            .read(selector, after, until, limits, &mut page)
            .await?;
        self.check_page(&page, selector, after, until, limits);
        Ok(page)
    }

    fn check_page(
        &self,
        page: &Page<Bytes, Bytes>,
        selector: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) {
        // L2: order and bounds.
        assert!(
            after < page.through && page.through <= until,
            "through {through} outside ({after}, {until}]",
            through = page.through
        );
        assert!(
            page.entries
                .windows(2)
                .all(|pair| (pair[0].rev, &pair[0].key) < (pair[1].rev, &pair[1].key)),
            "entries not in (rev, key) order: {page:?}"
        );
        assert!(
            page.entries
                .iter()
                .all(|entry| after < entry.rev && entry.rev <= page.through),
            "entries outside ({after}, {through}]: {page:?}",
            through = page.through
        );
        // L3: each revision is whole.
        for revision in page.revisions() {
            let rev = revision[0].rev;
            assert_eq!(
                revision,
                self.oracle.window(selector, rev - 1, rev).as_slice(),
                "revision {rev} split"
            );
        }
        // L5: a limit is exceeded only by a page that is one revision.
        let bytes = page
            .entries
            .iter()
            .map(|entry| row_bytes(&entry.key, entry.value.as_ref()))
            .sum::<usize>();
        let max_bytes = limits.max_bytes.map_or(usize::MAX, NonZeroUsize::get);
        if page.entries.len() > limits.max_entries.get() || bytes > max_bytes {
            assert_eq!(page.revisions().count(), 1, "limits exceeded: {page:?}");
        }
        assert_eq!(*page, self.oracle.read(selector, after, until, limits));
    }

    /// P1: register, and check the cursor and the consumer list against the oracle.
    async fn register(&mut self, consumer: &str) -> Res<Rev> {
        let cursor = self.store.register_consumer(consumer).await?;
        assert_eq!(
            cursor,
            self.oracle.register(consumer),
            "register {consumer}"
        );
        self.check_consumers().await?;
        Ok(cursor)
    }

    /// P2: report a cursor to both and compare the consumer lists.
    async fn report(&mut self, consumer: &str, cursor: Rev) -> Res<()> {
        self.store.report_cursor(consumer, cursor).await?;
        self.oracle.report(consumer, cursor);
        self.check_consumers().await
    }

    async fn unregister(&mut self, consumer: &str) -> Res<()> {
        self.store.unregister_consumer(consumer).await?;
        self.oracle.unregister(consumer);
        self.check_consumers().await
    }

    async fn check_consumers(&self) -> Res<()> {
        assert_eq!(
            &self.store.consumers().await?,
            self.oracle.consumers(),
            "registered consumers"
        );
        Ok(())
    }

    /// P3 and P4: prune, then check what it reports, that it consumed no rev and woke nobody,
    /// and that every row still reads as the oracle says.
    async fn prune(&mut self) -> Res<Pruned> {
        let latest = self.oracle.latest_rev();
        let pruned = self.store.prune().await?;
        assert_eq!(pruned, self.oracle.prune(), "prune");
        assert_eq!(self.store.latest_rev().await?, latest, "a prune took a rev");
        assert_eq!(self.watch.latest(), latest, "a prune moved the watch");
        if latest > 0 {
            self.read(&Selector::All, 0, latest, limits(usize::MAX, None))
                .await?;
        }
        Ok(pruned)
    }

    /// L4: page from `after` to the latest rev and compare the whole sweep with the oracle.
    async fn sweep(&self, selector: &Selector<Bytes>, after: Rev, limits: Limits) -> Res<()> {
        let latest = self.oracle.latest_rev();
        let mut swept = Vec::new();
        let mut at = after;
        while at < latest {
            let page = self.read(selector, at, latest, limits).await?;
            at = page.through;
            swept.extend(page.entries);
        }
        assert_eq!(swept, self.oracle.window(selector, after, latest));
        Ok(())
    }
}

/// L1: a key's row is its last mutation, at that mutation's rev; within a batch the last
/// mutation of a key wins.
pub async fn collapse<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![vec![put(1, b"a"), put(2, b"b"), put(3, b"c")]])
        .await?;
    harness
        .commit(vec![vec![put(1, b"x"), put(3, b"y"), put(1, b"z")]])
        .await?;
    let page = harness.read(&Selector::All, 0, 2, limits(10, None)).await?;
    assert_eq!(
        page,
        Page {
            entries: vec![
                entry(2, 1, Some(b"b")),
                entry(1, 2, Some(b"z")),
                entry(3, 2, Some(b"y")),
            ],
            through: 2,
        }
    );
    Ok(())
}

/// L6: a delete reads as a `None` row at its rev (also for a key never written), and a put after
/// it comes back at a new rev.
pub async fn tombstones<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness.commit(vec![vec![put(1, b"a")]]).await?;
    harness.commit(vec![vec![delete(1), delete(2)]]).await?;
    let page = harness.read(&Selector::All, 0, 2, limits(10, None)).await?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 2, None), entry(2, 2, None)],
            through: 2,
        }
    );
    harness.commit(vec![vec![put(1, b"b")]]).await?;
    let page = harness.read(&Selector::All, 0, 3, limits(10, None)).await?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(2, 2, None), entry(1, 3, Some(b"b"))],
            through: 3,
        }
    );
    Ok(())
}

/// L7: one rev per batch, strictly increasing, across transactions and within one; a rollback in
/// between consumes none.
pub async fn revs_strictly_increase<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    assert_eq!(harness.commit(vec![vec![put(1, b"a")]]).await?, vec![1]);
    assert_eq!(
        harness
            .commit(vec![
                vec![put(1, b"b")],
                vec![put(2, b"c")],
                vec![delete(1)]
            ])
            .await?,
        vec![2, 3, 4]
    );
    harness.rollback(vec![vec![put(3, b"d")]]).await?;
    assert_eq!(harness.commit(vec![vec![put(3, b"e")]]).await?, vec![5]);
    Ok(())
}

/// L7: an empty batch is a caller bug. The calling test is `#[should_panic]`.
pub async fn empty_batch_panics<B: Backend>(backend: &B) -> Res<()> {
    let db = backend.open().await?;
    let store = backend.table(&db).await?;
    let mut tx = backend.begin(&db).await?;
    store.apply(&mut tx, Vec::new()).await?;
    unreachable!("an empty batch was accepted");
}

/// L8: a rolled back transaction leaves no rows, consumes no rev and wakes nobody.
pub async fn rollback_leaves_no_trace<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness.commit(vec![vec![put(1, b"a")]]).await?;
    let wakes = WakeCount::default();
    let mut watch = harness.store.watch();
    let mut wait = pin!(watch.wait_past(1));
    assert!(wakes.poll(wait.as_mut()).is_pending());

    harness
        .rollback(vec![vec![put(1, b"b"), put(2, b"c")], vec![delete(1)]])
        .await?;
    assert_eq!(wakes.count(), 0, "a rollback woke a waiter");
    assert!(wakes.poll(wait.as_mut()).is_pending());
    harness.read(&Selector::All, 0, 1, limits(10, None)).await?;

    assert_eq!(harness.commit(vec![vec![put(2, b"d")]]).await?, vec![2]);
    assert!(wakes.count() > 0, "a commit woke nobody");
    assert_eq!(wakes.poll(wait.as_mut()), Poll::Ready(2));
    Ok(())
}

/// L9: the watch moves after commit, never before, and a woken reader finds the commit.
pub async fn wakeups_follow_commit<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    let wakes = WakeCount::default();
    let mut watch = harness.store.watch();
    let mut wait = pin!(watch.wait_past(0));
    assert!(wakes.poll(wait.as_mut()).is_pending());

    let mut tx = harness.backend.begin(&harness.db).await?;
    let rev = harness.store.apply(&mut tx, vec![put(1, b"a")]).await?;
    assert_eq!(rev, 1);
    assert_eq!(wakes.count(), 0, "a staged batch woke a waiter");
    assert_eq!(harness.store.latest_rev().await?, 0);
    harness.backend.commit(tx).await?;
    harness.oracle.commit(vec![put(1, b"a")]);

    assert!(wakes.count() > 0, "a commit woke nobody");
    assert_eq!(wakes.poll(wait.as_mut()), Poll::Ready(1));
    let page = harness.read(&Selector::All, 0, 1, limits(10, None)).await?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 1, Some(b"a"))],
            through: 1,
        }
    );
    Ok(())
}

/// L5: whole revisions fill a page up to each limit; a revision alone over a limit is a page of
/// its own.
pub async fn soft_limits<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            vec![put(1, b"aa"), put(2, b"bb"), put(3, b"cc")],
            vec![put(4, b"d")],
            vec![put(5, b"e")],
        ])
        .await?;
    let all = Selector::All;
    let first = Page {
        entries: vec![
            entry(1, 1, Some(b"aa")),
            entry(2, 1, Some(b"bb")),
            entry(3, 1, Some(b"cc")),
        ],
        through: 1,
    };
    // Three rows (9 bytes) is over both limits, but a page always takes its first revision.
    assert_eq!(harness.read(&all, 0, 3, limits(2, None)).await?, first);
    assert_eq!(harness.read(&all, 0, 3, limits(10, Some(4))).await?, first);
    // The next revision would go over the limit, so the page stops before it.
    assert_eq!(harness.read(&all, 0, 3, limits(3, None)).await?, first);
    assert_eq!(harness.read(&all, 0, 3, limits(10, Some(9))).await?, first);
    assert_eq!(
        harness.read(&all, 0, 3, limits(4, Some(11))).await?,
        Page {
            entries: vec![
                entry(1, 1, Some(b"aa")),
                entry(2, 1, Some(b"bb")),
                entry(3, 1, Some(b"cc")),
                entry(4, 2, Some(b"d")),
            ],
            through: 2,
        }
    );
    // A selector that skips the cut revision still stops at the last revision taken.
    assert_eq!(
        harness
            .read(
                &Selector::Keys([vec![1], vec![5]].into()),
                0,
                3,
                limits(1, None)
            )
            .await?,
        Page {
            entries: vec![entry(1, 1, Some(b"aa"))],
            through: 1,
        }
    );
    Ok(())
}

/// L1–L8 against the oracle over an arbitrary run of commits, rollbacks, reads and sweeps.
pub async fn differential<B: Backend>(backend: &B, ops: Vec<Op>) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    for op in ops {
        let latest = harness.oracle.latest_rev();
        match op {
            Op::Commit(batches) => {
                harness.commit(batches).await?;
            }
            Op::Rollback(batches) => harness.rollback(batches).await?,
            Op::Read {
                selector,
                after,
                until,
                limits,
            } => {
                if latest == 0 {
                    continue;
                }
                let after = fraction_of(after, latest);
                let until = after + 1 + fraction_of(until, latest - after);
                harness.read(&selector, after, until, limits).await?;
            }
            Op::Sweep {
                selector,
                after,
                limits,
            } => {
                harness
                    .sweep(&selector, fraction_of(after, latest), limits)
                    .await?;
            }
            Op::Register(consumer) => {
                harness.register(&consumer_name(consumer)).await?;
            }
            Op::Report { consumer, cursor } => {
                let name = consumer_name(consumer);
                if harness.oracle.consumers().contains_key(&name) {
                    harness
                        .report(&name, fraction_of(cursor, latest + 1))
                        .await?;
                }
            }
            Op::Unregister(consumer) => harness.unregister(&consumer_name(consumer)).await?,
            Op::Prune => {
                harness.prune().await?;
            }
        }
    }
    let latest = harness.oracle.latest_rev();
    harness
        .sweep(&Selector::All, 0, limits(usize::MAX, None))
        .await?;
    if latest > 0 {
        // L1 over the whole store: one row per key ever written, at its last rev.
        let page = harness
            .read(&Selector::All, 0, latest, limits(usize::MAX, None))
            .await?;
        assert_eq!(
            page.entries,
            sorted_by_rev(harness.oracle.state().into_values().collect())
        );
    }
    Ok(())
}

/// `get` reads the transaction's own staged writes first (a staged delete as `None`), then the
/// committed row, and never another table's staged writes; a rolled back transaction leaves the
/// committed rows to the next one.
pub async fn get_reads_your_writes<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![vec![put(1, b"a"), put(2, b"b")]])
        .await?;
    let store = harness.store.clone();
    let other = backend.table(&harness.db).await?;
    let (one, two, three) = (vec![1], vec![2], vec![3]);

    let mut tx = backend.begin(&harness.db).await?;
    assert_eq!(store.get(&mut tx, &one).await?, Some(b"a".to_vec()));
    assert_eq!(store.get(&mut tx, &three).await?, None);
    store
        .apply(&mut tx, vec![put(1, b"x"), delete(2), put(3, b"c")])
        .await?;
    other.apply(&mut tx, vec![put(1, b"o")]).await?;
    assert_eq!(store.get(&mut tx, &one).await?, Some(b"x".to_vec()));
    assert_eq!(store.get(&mut tx, &two).await?, None, "a staged delete");
    assert_eq!(store.get(&mut tx, &three).await?, Some(b"c".to_vec()));
    assert_eq!(other.get(&mut tx, &one).await?, Some(b"o".to_vec()));
    assert_eq!(other.get(&mut tx, &two).await?, None);
    backend.rollback(tx).await?;

    let mut tx = backend.begin(&harness.db).await?;
    assert_eq!(store.get(&mut tx, &one).await?, Some(b"a".to_vec()));
    assert_eq!(store.get(&mut tx, &two).await?, Some(b"b".to_vec()));
    assert_eq!(store.get(&mut tx, &three).await?, None);
    assert_eq!(other.get(&mut tx, &one).await?, None);
    backend.rollback(tx).await?;

    harness.commit(vec![vec![delete(1)]]).await?;
    let mut tx = backend.begin(&harness.db).await?;
    assert_eq!(
        store.get(&mut tx, &one).await?,
        None,
        "a committed tombstone"
    );
    backend.rollback(tx).await?;
    Ok(())
}

/// Two tables written in one transaction: neither is visible nor wakes anyone before commit or
/// after a rollback, and both are visible and woken once it commits. Each table numbers its own
/// revs.
pub async fn tables_commit_together<B: Backend>(backend: &B) -> Res<()> {
    let db = backend.open().await?;
    let left = backend.table(&db).await?;
    let right = backend.table(&db).await?;
    let (left_wakes, right_wakes) = (WakeCount::default(), WakeCount::default());
    let (mut left_watch, mut right_watch) = (left.watch(), right.watch());
    let mut left_wait = pin!(left_watch.wait_past(0));
    let mut right_wait = pin!(right_watch.wait_past(0));
    assert!(left_wakes.poll(left_wait.as_mut()).is_pending());
    assert!(right_wakes.poll(right_wait.as_mut()).is_pending());

    let mut tx = backend.begin(&db).await?;
    assert_eq!(left.apply(&mut tx, vec![put(1, b"l")]).await?, 1);
    assert_eq!(right.apply(&mut tx, vec![put(1, b"r")]).await?, 1);
    backend.rollback(tx).await?;
    assert_eq!(
        (left.latest_rev().await?, right.latest_rev().await?),
        (0, 0)
    );
    assert_eq!((left_wakes.count(), right_wakes.count()), (0, 0));

    let mut tx = backend.begin(&db).await?;
    left.apply(&mut tx, vec![put(1, b"l")]).await?;
    right.apply(&mut tx, vec![put(1, b"r")]).await?;
    assert_eq!(
        (left.latest_rev().await?, right.latest_rev().await?),
        (0, 0)
    );
    backend.commit(tx).await?;
    assert_eq!(
        (left.latest_rev().await?, right.latest_rev().await?),
        (1, 1)
    );
    assert!(
        left_wakes.count() > 0 && right_wakes.count() > 0,
        "a table was not woken"
    );
    assert_eq!(left_wakes.poll(left_wait.as_mut()), Poll::Ready(1));
    assert_eq!(right_wakes.poll(right_wait.as_mut()), Poll::Ready(1));

    let mut page = Page::default();
    left.read(&Selector::All, 0, 1, limits(10, None), &mut page)
        .await?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 1, Some(b"l"))],
            through: 1,
        }
    );
    right
        .read(&Selector::All, 0, 1, limits(10, None), &mut page)
        .await?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 1, Some(b"r"))],
            through: 1,
        }
    );
    Ok(())
}

/// P1 and P2: registering returns the stored cursor (`0` when new) and is idempotent, a cursor
/// only moves forward, and unregistering forgets the consumer.
pub async fn consumer_cursors<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            vec![put(1, b"a")],
            vec![put(2, b"b")],
            vec![put(3, b"c")],
        ])
        .await?;
    assert_eq!(
        harness.register("a").await?,
        0,
        "a new consumer starts at 0"
    );
    harness.report("a", 2).await?;
    assert_eq!(harness.register("a").await?, 2, "registering again resumes");
    harness.report("a", 1).await?;
    harness.report("a", 2).await?;
    assert_eq!(harness.register("a").await?, 2, "a cursor never goes back");
    harness.report("a", 3).await?;
    assert_eq!(harness.register("b").await?, 0);
    assert_eq!(
        harness.store.consumers().await?,
        BTreeMap::from([("a".to_owned(), 3), ("b".to_owned(), 0)])
    );
    harness.unregister("a").await?;
    harness.unregister("a").await?;
    assert_eq!(
        harness.register("a").await?,
        0,
        "a forgotten consumer is new"
    );
    Ok(())
}

/// P2: a cursor past the latest rev is a caller bug. The calling test is `#[should_panic]`.
pub async fn report_past_the_latest_rev_panics<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness.commit(vec![vec![put(1, b"a")]]).await?;
    harness.store.register_consumer("a").await?;
    harness.store.report_cursor("a", 2).await?;
    unreachable!("a cursor past the latest rev was accepted");
}

/// P2: a report for a consumer that never registered is a caller bug. The calling test is
/// `#[should_panic]`.
pub async fn report_unregistered_panics<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness.commit(vec![vec![put(1, b"a")]]).await?;
    harness.store.report_cursor("nobody", 1).await?;
    unreachable!("a report for an unregistered consumer was accepted");
}

/// P3 and P4: the lowest registered cursor is the floor, a lagging consumer holds tombstones
/// back, live rows are never dropped, and a tombstone written after a prune is kept until the
/// cursors pass it.
pub async fn prune_by_lowest_cursor<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            vec![put(1, b"a"), put(2, b"b"), put(3, b"c")],
            vec![delete(1)],
            vec![delete(2)],
            vec![put(4, b"d")],
            vec![delete(4)],
        ])
        .await?;
    let all = Selector::All;
    let rows = |page: Page<Bytes, Bytes>| {
        page.entries
            .into_iter()
            .map(|entry| (entry.key[0], entry.rev, entry.value.is_some()))
            .collect::<Vec<_>>()
    };

    let none = Pruned {
        floor: 0,
        tombstones: 0,
    };
    assert_eq!(harness.prune().await?, none, "no consumer, no prune");
    assert_eq!(
        rows(harness.read(&all, 0, 5, limits(10, None)).await?),
        vec![(3, 1, true), (1, 2, false), (2, 3, false), (4, 5, false)]
    );

    harness.register("fast").await?;
    harness.register("slow").await?;
    harness.report("fast", 5).await?;
    assert_eq!(
        harness.prune().await?,
        none,
        "a consumer that never reported holds all back"
    );

    harness.report("slow", 3).await?;
    assert_eq!(
        harness.prune().await?,
        Pruned {
            floor: 3,
            tombstones: 2
        }
    );
    assert_eq!(
        rows(harness.read(&all, 0, 5, limits(10, None)).await?),
        vec![(3, 1, true), (4, 5, false)],
        "the live row stays, the tombstone above the lagging cursor stays"
    );

    harness.report("slow", 5).await?;
    assert_eq!(
        harness.prune().await?,
        Pruned {
            floor: 5,
            tombstones: 1
        }
    );
    assert_eq!(
        rows(harness.read(&all, 0, 5, limits(10, None)).await?),
        vec![(3, 1, true)]
    );
    assert_eq!(
        harness.prune().await?,
        Pruned {
            floor: 5,
            tombstones: 0
        }
    );

    // A key written again after its tombstone was pruned starts a new row, and its own
    // tombstone waits for the cursors.
    harness
        .commit(vec![vec![put(1, b"e")], vec![delete(1)]])
        .await?;
    assert_eq!(
        harness.prune().await?,
        Pruned {
            floor: 5,
            tombstones: 0
        }
    );
    harness.report("fast", 7).await?;
    harness.report("slow", 7).await?;
    assert_eq!(
        harness.prune().await?,
        Pruned {
            floor: 7,
            tombstones: 1
        }
    );
    Ok(())
}

/// Runs the stream laws of [`crate::rev::laws`] against an okv backend: a table is the source,
/// a transaction per call is the commit hook, and the oracle's read is the mapping.
#[derive(Clone)]
pub struct OkvStreams<B>(pub B);

#[async_trait]
impl<B: Backend> StreamBackend for OkvStreams<B> {
    type Source = B::Store;
    type Writer = (B::Db, B::Store);

    async fn open(&self) -> Res<(B::Store, (B::Db, B::Store))> {
        let db = self.0.open().await?;
        let store = self.0.table(&db).await?;
        Ok((store.clone(), (db, store)))
    }

    async fn commit(&self, writer: &(B::Db, B::Store), batches: Batches) -> Res<()> {
        let (db, store) = writer;
        let mut tx = self.0.begin(db).await?;
        for batch in batches {
            store.apply(&mut tx, batch).await?;
        }
        self.0.commit(tx).await
    }

    async fn rollback(&self, writer: &(B::Db, B::Store), batches: Batches) -> Res<()> {
        let (db, store) = writer;
        let mut tx = self.0.begin(db).await?;
        for batch in batches {
            store.apply(&mut tx, batch).await?;
        }
        self.0.rollback(tx).await
    }

    fn selector(&self, selector: &Selector<Bytes>) -> Selector<Bytes> {
        selector.clone()
    }

    fn expected(
        &self,
        oracle: &Oracle<Bytes, Bytes>,
        selector: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Page<Bytes, Bytes> {
        oracle.read(selector, after, until, limits)
    }
}

pub fn ops() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        3 => transaction().prop_map(Op::Commit),
        1 => transaction().prop_map(Op::Rollback),
        3 => (selector(), any::<u8>(), any::<u8>(), any_limits()).prop_map(
            |(selector, after, until, limits)| Op::Read {
                selector,
                after,
                until,
                limits,
            }
        ),
        1 => (selector(), any::<u8>(), any_limits()).prop_map(|(selector, after, limits)| {
            Op::Sweep {
                selector,
                after,
                limits,
            }
        }),
        1 => any::<u8>().prop_map(Op::Register),
        2 => (any::<u8>(), any::<u8>())
            .prop_map(|(consumer, cursor)| Op::Report { consumer, cursor }),
        1 => any::<u8>().prop_map(Op::Unregister),
        2 => Just(Op::Prune),
    ];
    proptest::collection::vec(op, 0..40)
}

fn key() -> impl Strategy<Value = Bytes> {
    (0u8..6).prop_map(|key| vec![key])
}

fn mutation() -> impl Strategy<Value = Mutation<Bytes, Bytes>> {
    prop_oneof![
        3 => (key(), proptest::collection::vec(any::<u8>(), 0..3))
            .prop_map(|(key, value)| Mutation::Put { key, value }),
        1 => key().prop_map(|key| Mutation::Delete { key }),
    ]
}

pub(crate) fn transaction() -> impl Strategy<Value = Batches> {
    proptest::collection::vec(proptest::collection::vec(mutation(), 1..4), 1..3)
}

pub(crate) fn selector() -> impl Strategy<Value = Selector<Bytes>> {
    fn bound() -> impl Strategy<Value = Bound<Bytes>> {
        prop_oneof![
            Just(Bound::Unbounded),
            key().prop_map(Bound::Included),
            key().prop_map(Bound::Excluded),
        ]
    }
    prop_oneof![
        Just(Selector::All),
        proptest::collection::btree_set(key(), 0..4).prop_map(Selector::Keys),
        (bound(), bound()).prop_map(|(start, end)| Selector::Range(KeyRange { start, end })),
    ]
}

/// `fraction` of 256 of `span`: always below `span`.
fn fraction_of(fraction: u8, span: Rev) -> Rev {
    u64::from(fraction) * span / 256
}

pub(crate) fn put(key: u8, value: &[u8]) -> Mutation<Bytes, Bytes> {
    Mutation::Put {
        key: vec![key],
        value: value.to_vec(),
    }
}

fn delete(key: u8) -> Mutation<Bytes, Bytes> {
    Mutation::Delete { key: vec![key] }
}

fn entry(key: u8, rev: Rev, value: Option<&[u8]>) -> Entry<Bytes, Bytes> {
    Entry {
        key: vec![key],
        rev,
        value: value.map(<[u8]>::to_vec),
    }
}

fn sorted_by_rev(mut entries: Vec<Entry<Bytes, Bytes>>) -> Vec<Entry<Bytes, Bytes>> {
    entries.sort_by(|left, right| (left.rev, &left.key).cmp(&(right.rev, &right.key)));
    entries
}
