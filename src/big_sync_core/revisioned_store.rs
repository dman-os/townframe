//! Inert source boundary for ordered, replayable revisions.
//!
//! A store owns revision assignment and replay-boundary capture. A reader
//! yields complete atomic revisions, including revisions whose projection has
//! no entries, then emits one replay-complete marker before following later
//! revisions. The contract contains no acknowledgements, persistence policy,
//! tasks, or consumer execution.
//!
//! Revisions are strictly increasing: a store must never emit a revision at
//! or below one already emitted for the same reader. The concurrent walker
//! treats such a revision as a replay of an already-acked cursor and silently
//! drops it, so a non-monotonic source loses data without an error — the
//! source owns this invariant.

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::keyed_frontier::{
    FrontierEntry, FrontierRead, FrontierRevision, KeyedFrontierError, KeyedFrontierReader,
};

/// One reader result. `Entries` is one complete atomic revision; its entries
/// may be empty when a projection advances progress without typed output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionRead<R, E> {
    Entries { revision: R, entries: Vec<E> },
    ReplayComplete { through: R },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevisionReadLimits {
    pub max_entries: NonZeroUsize,
}

impl Default for RevisionReadLimits {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(256).expect("literal is non-zero"),
        }
    }
}

/// Stateful session opened against a revisioned store.
#[async_trait]
pub trait RevisionedStoreReader<R, E, Error>: Send
where
    R: Send + Sync,
    E: Send + Sync,
    Error: Send + Sync,
{
    /// Returns atomic revisions, exactly one replay boundary, then live
    /// revisions. A live reader may wait inside this call for new work.
    async fn next(&mut self, limits: RevisionReadLimits) -> Result<RevisionRead<R, E>, Error>;
}

/// Inert source contract for a replayable revision stream.
#[async_trait]
pub trait RevisionedStore: Send + Sync {
    type Revision: Ord + Clone + Send + Sync + 'static;
    type Entry: Send + Sync + 'static;
    type Selector: Send + Sync + 'static;
    type Error: Send + Sync + 'static;
    type Reader<'a>: RevisionedStoreReader<Self::Revision, Self::Entry, Self::Error> + 'a
    where
        Self: 'a;

    /// Return the source's current global revision without opening a reader.
    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error>;

    /// Opens a stateful reader and captures its replay boundary once.
    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: Self::Revision,
    ) -> Result<Self::Reader<'a>, Self::Error>;
}

/// Adapts a keyed-frontier reader to the source-level revision contract.
pub struct KeyedFrontierRevisionReader<'a, K, V> {
    inner: Box<dyn KeyedFrontierReader<K, V> + 'a>,
    pending: std::collections::VecDeque<RevisionRead<FrontierRevision, FrontierEntry<K, V>>>,
    pending_progress: Option<FrontierRevision>,
    replay_complete_seen: bool,
}

impl<'a, K, V> KeyedFrontierRevisionReader<'a, K, V> {
    pub fn new(inner: Box<dyn KeyedFrontierReader<K, V> + 'a>) -> Self {
        Self {
            inner,
            pending: Default::default(),
            pending_progress: None,
            replay_complete_seen: false,
        }
    }
}

#[async_trait]
impl<K, V> RevisionedStoreReader<FrontierRevision, FrontierEntry<K, V>, KeyedFrontierError>
    for KeyedFrontierRevisionReader<'_, K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<FrontierRevision, FrontierEntry<K, V>>, KeyedFrontierError> {
        if let Some(read) = self.pending.pop_front() {
            return Ok(read);
        }
        if let Some(revision) = self.pending_progress.take() {
            return Ok(RevisionRead::Entries {
                revision,
                entries: Vec::new(),
            });
        }
        match self
            .inner
            .next(crate::keyed_frontier::FrontierReadLimits {
                max_entries: limits.max_entries,
            })
            .await?
        {
            FrontierRead::Entries { entries, through } if entries.is_empty() => {
                Ok(RevisionRead::Entries {
                    revision: through,
                    entries: Vec::new(),
                })
            }
            FrontierRead::Entries { entries, through } => {
                let mut grouped = BTreeMap::<FrontierRevision, Vec<_>>::new();
                for entry in entries {
                    grouped.entry(entry.revision).or_default().push(entry);
                }
                let last = grouped
                    .keys()
                    .next_back()
                    .copied()
                    .expect("entries non-empty");
                self.pending.extend(
                    grouped
                        .into_iter()
                        .map(|(revision, entries)| RevisionRead::Entries { revision, entries }),
                );
                if through > last {
                    self.pending_progress = Some(through);
                }
                Ok(self.pending.pop_front().expect("grouped entries non-empty"))
            }
            FrontierRead::ReplayComplete { through } => {
                if self.replay_complete_seen {
                    return Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "keyed frontier emitted ReplayComplete twice",
                    ))));
                }
                self.replay_complete_seen = true;
                Ok(RevisionRead::ReplayComplete { through })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_revision_watch::LiveRevisionWatch;
    use std::collections::VecDeque;

    #[derive(Debug, thiserror::Error)]
    #[error("script exhausted")]
    struct ScriptError;

    struct ScriptedStore {
        reads: VecDeque<RevisionRead<u64, u64>>,
    }

    struct ScriptedReader {
        reads: VecDeque<RevisionRead<u64, u64>>,
    }

    #[async_trait]
    impl RevisionedStoreReader<u64, u64, ScriptError> for ScriptedReader {
        async fn next(
            &mut self,
            _limits: RevisionReadLimits,
        ) -> Result<RevisionRead<u64, u64>, ScriptError> {
            self.reads.pop_front().ok_or(ScriptError)
        }
    }

    #[async_trait]
    impl RevisionedStore for ScriptedStore {
        type Revision = u64;
        type Entry = u64;
        type Selector = ();
        type Error = ScriptError;
        type Reader<'a>
            = ScriptedReader
        where
            Self: 'a;

        async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
            Ok(0)
        }

        async fn open<'a>(
            &'a self,
            (): Self::Selector,
            _after: u64,
        ) -> Result<Self::Reader<'a>, Self::Error> {
            Ok(ScriptedReader {
                reads: self.reads.clone(),
            })
        }
    }

    #[test]
    fn preserves_atomic_batches_empty_progress_and_live_tail() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                reads: VecDeque::from([
                    RevisionRead::Entries {
                        revision: 3,
                        entries: vec![1, 2],
                    },
                    RevisionRead::Entries {
                        revision: 4,
                        entries: Vec::new(),
                    },
                    RevisionRead::ReplayComplete { through: 4 },
                    RevisionRead::Entries {
                        revision: 5,
                        entries: vec![3],
                    },
                ]),
            };
            let mut reader = store.open((), 0).await.unwrap();
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 3,
                    entries: vec![1, 2]
                }
            );
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 4,
                    entries: Vec::new()
                }
            );
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::ReplayComplete { through: 4 }
            );
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 5,
                    entries: vec![3]
                }
            );
        });
    }

    #[test]
    fn live_watch_hides_initial_replay_boundary() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                reads: VecDeque::from([
                    RevisionRead::Entries {
                        revision: 2,
                        entries: vec![7],
                    },
                    RevisionRead::ReplayComplete { through: 2 },
                    RevisionRead::Entries {
                        revision: 3,
                        entries: vec![8],
                    },
                ]),
            };
            let store_ref = &store;
            let mut watch =
                LiveRevisionWatch::open(
                    &store,
                    |after| async move { store_ref.open((), after).await },
                )
                .await
                .unwrap();
            assert_eq!(
                watch.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 2,
                    entries: vec![7]
                }
            );
            assert_eq!(
                watch.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 3,
                    entries: vec![8]
                }
            );
        });
    }

    struct FrontierScriptReader {
        reads: VecDeque<crate::keyed_frontier::FrontierRead<u64, u64>>,
    }

    #[async_trait]
    impl crate::keyed_frontier::KeyedFrontierReader<u64, u64> for FrontierScriptReader {
        async fn next(
            &mut self,
            _limits: crate::keyed_frontier::FrontierReadLimits,
        ) -> Result<crate::keyed_frontier::FrontierRead<u64, u64>, KeyedFrontierError> {
            self.reads.pop_front().ok_or_else(|| {
                KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "script exhausted",
                )))
            })
        }
    }

    #[test]
    fn keyed_adapter_groups_entries_and_preserves_progress() {
        futures::executor::block_on(async {
            let mut reader = KeyedFrontierRevisionReader {
                inner: Box::new(FrontierScriptReader {
                    reads: VecDeque::from([
                        crate::keyed_frontier::FrontierRead::Entries {
                            entries: vec![
                                FrontierEntry {
                                    key: 1,
                                    revision: 3,
                                    value: Some(10),
                                },
                                FrontierEntry {
                                    key: 2,
                                    revision: 3,
                                    value: Some(20),
                                },
                            ],
                            through: 3,
                        },
                        crate::keyed_frontier::FrontierRead::Entries {
                            entries: vec![FrontierEntry {
                                key: 3,
                                revision: 4,
                                value: Some(30),
                            }],
                            through: 5,
                        },
                        crate::keyed_frontier::FrontierRead::ReplayComplete { through: 5 },
                        crate::keyed_frontier::FrontierRead::Entries {
                            entries: vec![FrontierEntry {
                                key: 4,
                                revision: 6,
                                value: Some(40),
                            }],
                            through: 6,
                        },
                    ]),
                }),
                pending: VecDeque::new(),
                pending_progress: None,
                replay_complete_seen: false,
            };
            let RevisionRead::Entries { revision, entries } =
                reader.next(RevisionReadLimits::default()).await.unwrap()
            else {
                panic!("expected grouped entries");
            };
            assert_eq!(revision, 3);
            assert_eq!(entries.len(), 2);
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 4,
                    entries: vec![FrontierEntry {
                        key: 3,
                        revision: 4,
                        value: Some(30)
                    }],
                }
            );
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries {
                    revision: 5,
                    entries: Vec::new(),
                }
            );
            assert_eq!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::ReplayComplete { through: 5 }
            );
            assert!(matches!(
                reader.next(RevisionReadLimits::default()).await.unwrap(),
                RevisionRead::Entries { revision: 6, .. }
            ));
        });
    }
}

/// Portable contract suite for revisioned-store backends.
///
/// The trait is inert: it owns no write path, so the harness supplies the
/// commit hook and the runner pins the stream semantics every consumer
/// (serial/concurrent walkers, live watches) relies on. Backends implement
/// [`RevisionedStoreContractHarness`] once and run
/// [`assert_revisioned_store_contract`] against their own storage.
///
/// The runner is deliberately tolerant of backend batching: a backend may
/// deliver several committed revisions in one `Entries` page (stamped with
/// the page's highest revision), so assertions are order/set-based rather
/// than exact-sequence. Progress-only revisions (empty `Entries`) are a
/// per-backend capability, not part of this contract.
#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use super::*;

    /// Supplies a store, its commit hook, and entry constructors to the
    /// reusable RevisionedStore contract suite.
    #[async_trait]
    pub trait RevisionedStoreContractHarness: Sync
    where
        <Self::Store as RevisionedStore>::Entry: Clone + PartialEq + std::fmt::Debug,
        <Self::Store as RevisionedStore>::Error: std::fmt::Debug,
    {
        type Store: RevisionedStore<Revision = u64> + Sync;

        fn store(&self) -> &Self::Store;

        /// Commit one atomic revision carrying `entry`; returns the assigned
        /// revision. Revisions must be strictly increasing across calls.
        async fn commit(
            &self,
            entry: <Self::Store as RevisionedStore>::Entry,
        ) -> Result<u64, <Self::Store as RevisionedStore>::Error>;

        fn entry(&self, index: u64) -> <Self::Store as RevisionedStore>::Entry;

        /// Compare a reader-delivered entry against the harness's expected
        /// value. Defaults to structural equality; backends that stamp
        /// reader-side fields (revision, seq) into entries override this to
        /// ignore them.
        fn entries_match(
            &self,
            expected: &<Self::Store as RevisionedStore>::Entry,
            actual: &<Self::Store as RevisionedStore>::Entry,
        ) -> bool {
            expected == actual
        }

        fn all_selector(&self, after: u64) -> <Self::Store as RevisionedStore>::Selector;
    }

    /// Replay a reader to its replay-complete boundary, returning the
    /// delivered entries in order. Empty progress revisions are skipped.
    async fn replay<R, E, Error>(reader: &mut R) -> Result<Vec<E>, Error>
    where
        R: RevisionedStoreReader<u64, E, Error> + ?Sized,
        E: Send + Sync,
        Error: Send + Sync,
    {
        let mut out = Vec::new();
        loop {
            match reader.next(RevisionReadLimits::default()).await? {
                RevisionRead::Entries { entries, .. } => out.extend(entries),
                RevisionRead::ReplayComplete { .. } => return Ok(out),
            }
        }
    }

    /// Drain empty progress revisions until the replay boundary. Some
    /// backends synthesize an empty `Entries` (progress-only revision)
    /// before the boundary; consumers must tolerate them.
    async fn drain_to_boundary<R, E, Error>(reader: &mut R) -> Result<(), Error>
    where
        R: RevisionedStoreReader<u64, E, Error> + ?Sized,
        E: Send + Sync,
        Error: Send + Sync,
    {
        loop {
            match reader.next(RevisionReadLimits::default()).await? {
                RevisionRead::Entries { entries, .. } => {
                    assert!(
                        entries.is_empty(),
                        "expected only empty progress revisions before the boundary"
                    );
                }
                RevisionRead::ReplayComplete { .. } => return Ok(()),
            }
        }
    }

    fn assert_replay<H>(
        harness: &H,
        actual: Vec<<H::Store as RevisionedStore>::Entry>,
        expected: Vec<<H::Store as RevisionedStore>::Entry>,
    ) where
        H: RevisionedStoreContractHarness,
    {
        assert_eq!(
            actual.len(),
            expected.len(),
            "replay delivered a different number of entries"
        );
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            assert!(
                harness.entries_match(expected, actual),
                "replay entry mismatch: expected {expected:?}, got {actual:?}"
            );
        }
    }

    /// Runs the portable replay, boundary, monotonicity and live-handoff
    /// contract.
    pub async fn assert_revisioned_store_contract<H>(harness: &H)
    where
        H: RevisionedStoreContractHarness,
    {
        let store = harness.store();
        let e1 = harness.entry(1);
        let e2 = harness.entry(2);
        let e3 = harness.entry(3);

        // A fresh store replays nothing and completes immediately (possibly
        // after empty progress revisions).
        let mut fresh = store
            .open(harness.all_selector(0), 0)
            .await
            .expect("open fresh store");
        drain_to_boundary(&mut fresh)
            .await
            .expect("fresh replay boundary");

        // Committed revisions replay in order, after-exclusive. Revisions
        // are backend-assigned (a backend may advance its counter per
        // mutation), so only relative ordering is asserted.
        let r1 = harness.commit(e1.clone()).await.expect("commit revision 1");
        let r2 = harness.commit(e2.clone()).await.expect("commit revision 2");
        assert!(r2 > r1, "revision {r2} must advance beyond {r1}");
        let mut reader = store
            .open(harness.all_selector(0), 0)
            .await
            .expect("open replay reader");
        assert_replay(
            harness,
            replay(&mut reader)
                .await
                .expect("replay committed revisions"),
            vec![e1.clone(), e2.clone()],
        );

        // Reopening after a revision replays only newer revisions.
        let mut partial = store
            .open(harness.all_selector(r1), r1)
            .await
            .expect("open after revision 1");
        assert_replay(
            harness,
            replay(&mut partial).await.expect("partial replay"),
            vec![e2.clone()],
        );

        // Reopening at the head replays nothing and completes immediately
        // (possibly after empty progress revisions).
        let mut at_head = store
            .open(harness.all_selector(r2), r2)
            .await
            .expect("open at head");
        drain_to_boundary(&mut at_head)
            .await
            .expect("head replay boundary");

        // A commit after open is delivered only after the replay boundary.
        let mut boundary = store
            .open(harness.all_selector(0), 0)
            .await
            .expect("open boundary reader");
        let r3 = harness.commit(e3.clone()).await.expect("commit revision 3");
        assert!(r3 > r2, "revision {r3} must advance beyond {r2}");
        assert_replay(
            harness,
            replay(&mut boundary).await.expect("replay before boundary"),
            vec![e1.clone(), e2.clone()],
        );
        let live = boundary
            .next(RevisionReadLimits::default())
            .await
            .expect("live entry after boundary");
        assert!(
            matches!(live, RevisionRead::Entries { revision, .. } if revision > r2),
            "live entry after boundary must be stamped with a revision beyond the boundary"
        );
        let RevisionRead::Entries { entries, .. } = live else {
            unreachable!("live entry matched above")
        };
        assert_eq!(
            entries.len(),
            1,
            "live entry must carry the committed entry"
        );
        assert!(
            harness.entries_match(&e3, &entries[0]),
            "live entry mismatch: expected {e3:?}, got {:?}",
            entries[0]
        );

        // Strict monotonicity: a reader never sees a revision at or below a
        // previously delivered one.
        let mut mono = store
            .open(harness.all_selector(0), 0)
            .await
            .expect("open monotonicity reader");
        let mut last = 0u64;
        loop {
            match mono
                .next(RevisionReadLimits::default())
                .await
                .expect("monotonic read")
            {
                RevisionRead::Entries { revision, .. } => {
                    assert!(
                        revision > last,
                        "revision {revision} did not advance beyond {last}"
                    );
                    last = revision;
                }
                RevisionRead::ReplayComplete { through } => {
                    assert!(through >= last, "replay boundary regressed");
                    break;
                }
            }
        }

        // A reader opened beyond the head completes immediately (possibly
        // after empty progress revisions).
        let mut beyond = store
            .open(harness.all_selector(100), 100)
            .await
            .expect("open beyond head");
        drain_to_boundary(&mut beyond)
            .await
            .expect("beyond-head replay boundary");
    }
}
