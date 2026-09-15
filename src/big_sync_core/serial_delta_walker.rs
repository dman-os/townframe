//! Embedder-driven serial settlement machine for a revisioned source.
//!
//! The source is borrowed and opened at the consumer's durable progress. A
//! session may fetch one revision, let its embedder perform projection work,
//! and then settle that revision in the same state transaction. It owns no
//! task, callback, outbox, or external event loop.
//!
//! The walker never sees source selection: the caller opens the store's
//! reader with whatever filter it needs (selection is a store-open concern,
//! compiled into the store's query) and hands the opened reader to
//! [`SerialDeltaWalker::open`].

use crate::delta_walker_sparse_state::DeltaWalkerSparseStateTransaction;
use crate::delta_walker_state::{
    DeltaWalkerStateError, DeltaWalkerStateRepo, DeltaWalkerStateTransaction,
};
use crate::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialDeltaPhase {
    Replay,
    Live,
}

#[derive(Debug, thiserror::Error)]
pub enum SerialDeltaWalkerError<E = ()> {
    #[error("revision source failed")]
    Source(E),
    #[error("revision did not advance")]
    NonAdvancingRevision,
    #[error("replay completion did not advance through the source")]
    NonAdvancingReplayCompletion,
    #[error("replay completion was received after replay ended")]
    DuplicateReplayCompletion,
    #[error("an Entries revision is still unsettled")]
    UnsettledRevision,
    #[error("settlement requested for revision {requested}, pending revision is {pending}")]
    WrongSettlementRevision { requested: u64, pending: u64 },
    #[error("no Entries revision is pending settlement")]
    NoPendingRevision,
    #[error("state operation failed: {0}")]
    State(#[from] DeltaWalkerStateError),
}

struct PendingRevision {
    expected: u64,
    revision: u64,
}

/// A source session for one consumer's durable serial state.
pub struct SerialDeltaWalker<'a, S, R>
where
    S: RevisionedStore<Revision = u64> + 'a,
    R: DeltaWalkerStateRepo + 'a,
{
    state: &'a R,
    reader: S::Reader<'a>,
    durable_revision: u64,
    source_revision: u64,
    phase: SerialDeltaPhase,
    pending: Option<PendingRevision>,
}

impl<'a, S, R> SerialDeltaWalker<'a, S, R>
where
    S: RevisionedStore<Revision = u64> + 'a,
    R: DeltaWalkerStateRepo + 'a,
{
    /// Take an already-opened reader and the consumer's state repo.
    ///
    /// The reader was opened by the caller at the state repo's durable
    /// progress with the caller's own selection; the walker only reads
    /// `state.progress()` for its durable revision and takes ownership of
    /// settlement. The state repo is exclusively owned by this consumer, so
    /// the caller's `after` and the progress read here agree.
    pub async fn open(
        reader: S::Reader<'a>,
        state: &'a R,
    ) -> Result<Self, SerialDeltaWalkerError<S::Error>> {
        let durable_revision = state
            .progress()
            .await
            .map_err(SerialDeltaWalkerError::State)?
            .upstream_revision;
        Ok(Self {
            state,
            reader,
            durable_revision,
            source_revision: durable_revision,
            phase: SerialDeltaPhase::Replay,
            pending: None,
        })
    }

    pub fn durable_revision(&self) -> u64 {
        self.durable_revision
    }

    pub fn phase(&self) -> SerialDeltaPhase {
        self.phase
    }

    /// Fetch the next source item without advancing durable state.
    ///
    /// At most one Entries item may be outstanding. ReplayComplete is a
    /// transient phase marker and never requires settlement.
    pub async fn next(
        &mut self,
    ) -> Result<RevisionRead<u64, S::Entry>, SerialDeltaWalkerError<S::Error>> {
        if self.pending.is_some() {
            return Err(SerialDeltaWalkerError::UnsettledRevision);
        }
        loop {
            let read = self
                .reader
                .next(RevisionReadLimits::default())
                .await
                .map_err(SerialDeltaWalkerError::Source)?;
            match &read {
                RevisionRead::Entries { revision, entries } => {
                    if *revision <= self.source_revision {
                        if entries.is_empty() {
                            // An empty page stamped at or below the durable
                            // floor is a benign no-op, not a regression. A
                            // store may own a committed revision with no
                            // typed output for this consumer, and a reopen
                            // whose durable progress equals the source head
                            // legitimately observes it (e.g. a changed
                            // selector over a quiescent source). Settling it
                            // would itself fail as non-advancing, so skip to
                            // the next read, which surfaces ReplayComplete.
                            continue;
                        }
                        return Err(SerialDeltaWalkerError::NonAdvancingRevision);
                    }
                    self.source_revision = *revision;
                    self.pending = Some(PendingRevision {
                        expected: self.durable_revision,
                        revision: *revision,
                    });
                    return Ok(read);
                }
                RevisionRead::ReplayComplete { through } => {
                    if self.phase == SerialDeltaPhase::Live {
                        return Err(SerialDeltaWalkerError::DuplicateReplayCompletion);
                    }
                    if *through < self.source_revision {
                        return Err(SerialDeltaWalkerError::NonAdvancingReplayCompletion);
                    }
                    self.source_revision = *through;
                    self.phase = SerialDeltaPhase::Live;
                    return Ok(read);
                }
            }
        }
    }

    /// Settle an Entries revision without projection writes.
    pub async fn settle(&mut self, revision: u64) -> Result<(), SerialDeltaWalkerError<S::Error>> {
        let settlement = self.begin_settlement(revision).await?;
        settlement
            .settle()
            .await
            .map_err(SerialDeltaWalkerError::State)
    }

    /// Begin a caller-owned settlement transaction. Projection writes made
    /// through `context_mut` and cursor advancement commit atomically.
    pub async fn begin_settlement<'s>(
        &'s mut self,
        revision: u64,
    ) -> Result<SerialDeltaSettlement<'s, 'a, R>, SerialDeltaWalkerError<S::Error>> {
        let expected = self
            .pending
            .as_ref()
            .ok_or(SerialDeltaWalkerError::NoPendingRevision)?
            .expected;
        let pending_revision = self.pending.as_ref().expect("pending revision").revision;
        if pending_revision != revision {
            return Err(SerialDeltaWalkerError::WrongSettlementRevision {
                requested: revision,
                pending: pending_revision,
            });
        }
        let mut transaction = self
            .state
            .begin()
            .await
            .map_err(SerialDeltaWalkerError::State)?;
        let actual = transaction
            .progress()
            .await
            .map_err(SerialDeltaWalkerError::State)?
            .upstream_revision;
        if actual != expected {
            return Err(SerialDeltaWalkerError::State(
                DeltaWalkerStateError::StaleProgress { expected },
            ));
        }
        Ok(SerialDeltaSettlement {
            pending: &mut self.pending,
            durable_revision: &mut self.durable_revision,
            transaction,
            expected,
            revision,
        })
    }
}

/// A state transaction held while the embedder applies its projection.
pub struct SerialDeltaSettlement<'s, 'a, R>
where
    R: DeltaWalkerStateRepo + 'a,
{
    pending: &'s mut Option<PendingRevision>,
    durable_revision: &'s mut u64,
    transaction: R::Transaction<'a>,
    expected: u64,
    revision: u64,
}

impl<'s, 'a, R> SerialDeltaSettlement<'s, 'a, R>
where
    R: DeltaWalkerStateRepo + 'a,
{
    pub fn context_mut(&mut self) -> &mut R::Context<'a> {
        self.transaction.context_mut()
    }

    pub async fn settle(self) -> Result<(), DeltaWalkerStateError> {
        let Self {
            pending,
            durable_revision,
            mut transaction,
            expected,
            revision,
        } = self;
        if let Err(error) = transaction.advance_from(expected, revision).await {
            return match transaction.rollback().await {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_error),
            };
        }
        transaction.commit().await?;
        *pending = None;
        *durable_revision = revision;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), DeltaWalkerStateError> {
        self.transaction.rollback().await
    }
}

impl<'s, 'a, R> SerialDeltaSettlement<'s, 'a, R>
where
    R: DeltaWalkerStateRepo + 'a,
    R::Transaction<'a>: DeltaWalkerSparseStateTransaction,
{
    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DeltaWalkerStateError> {
        self.transaction.put(key, value).await
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), DeltaWalkerStateError> {
        self.transaction.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_walker_sparse_state::{
        DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
    };
    use crate::delta_walker_state::{DeltaWalkerProgress, DeltaWalkerStateResult};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, VecDeque};
    use std::ops::Bound;
    use std::sync::Mutex;

    // In-memory durable state double. `advance_from` mirrors the SQLite
    // backend semantics: the next revision must strictly exceed the expected
    // one, otherwise the settlement itself would fail as non-advancing.
    #[derive(Default)]
    struct MemoryState {
        progress: u64,
        keys: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    struct MemoryStateRepo {
        inner: Mutex<MemoryState>,
    }

    impl MemoryStateRepo {
        fn new(progress: u64) -> Self {
            Self {
                inner: Mutex::new(MemoryState {
                    progress,
                    keys: Default::default(),
                }),
            }
        }
    }

    async fn open_walker<'a>(
        store: &'a ScriptedStore,
        state: &'a MemoryStateRepo,
    ) -> SerialDeltaWalker<'a, ScriptedStore, MemoryStateRepo> {
        let durable = state.progress().await.unwrap().upstream_revision;
        let reader = store
            .open(0, durable)
            .await
            .expect("scripted store open must succeed");
        SerialDeltaWalker::open(reader, state)
            .await
            .expect("walker open must succeed")
    }

    struct MemoryStateTx<'a> {
        inner: &'a Mutex<MemoryState>,
        ctx: (),
        staged_progress: Option<u64>,
    }

    #[async_trait]
    impl<'a> DeltaWalkerStateTransaction for MemoryStateTx<'a> {
        type Context = ();

        fn context_mut(&mut self) -> &mut Self::Context {
            &mut self.ctx
        }

        async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            let state = self.inner.lock().unwrap();
            Ok(DeltaWalkerProgress {
                upstream_revision: state.progress,
            })
        }

        async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()> {
            let state = self.inner.lock().unwrap();
            if state.progress != expected {
                return Err(DeltaWalkerStateError::StaleProgress { expected });
            }
            if next <= state.progress {
                return Err(DeltaWalkerStateError::NonAdvancingRevision {
                    current: state.progress,
                    next,
                });
            }
            self.staged_progress = Some(next);
            Ok(())
        }

        async fn commit(self) -> DeltaWalkerStateResult<()> {
            if let Some(next) = self.staged_progress {
                self.inner.lock().unwrap().progress = next;
            }
            Ok(())
        }

        async fn rollback(self) -> DeltaWalkerStateResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl DeltaWalkerSparseStateTransaction for MemoryStateTx<'_> {
        async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            Ok(self.inner.lock().unwrap().keys.get(key).cloned())
        }

        async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()> {
            self.inner.lock().unwrap().keys.insert(key, value);
            Ok(())
        }

        async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()> {
            self.inner.lock().unwrap().keys.remove(key);
            Ok(())
        }
    }

    #[async_trait]
    impl DeltaWalkerStateRepo for MemoryStateRepo {
        type Context<'a>
            = ()
        where
            Self: 'a;
        type Transaction<'a>
            = MemoryStateTx<'a>
        where
            Self: 'a;

        async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            let state = self.inner.lock().unwrap();
            Ok(DeltaWalkerProgress {
                upstream_revision: state.progress,
            })
        }

        async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemoryStateTx {
                inner: &self.inner,
                ctx: (),
                staged_progress: None,
            })
        }

        async fn begin_with_context<'a>(
            &'a self,
            context: Self::Context<'a>,
        ) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemoryStateTx {
                inner: &self.inner,
                ctx: context,
                staged_progress: None,
            })
        }
    }

    #[async_trait]
    impl DeltaWalkerSparseStateRepo for MemoryStateRepo {
        async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            Ok(self.inner.lock().unwrap().keys.get(key).cloned())
        }

        async fn get_many(
            &self,
            keys: &[Vec<u8>],
        ) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
            let state = self.inner.lock().unwrap();
            Ok(keys
                .iter()
                .filter_map(|key| {
                    state
                        .keys
                        .get(key)
                        .map(|value| (key.clone(), value.clone()))
                })
                .collect())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("script exhausted")]
    struct ScriptError;

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

    /// Faithful replay model of the keyed-frontier reader.
    ///
    /// `open(after)` yields every stored revision above `after`, then - when
    /// nothing qualified - one empty batch stamped at the head cursor
    /// (mirroring `read_page` stamping the committed revision on an empty
    /// page), then exactly one `ReplayComplete`, then the live tail. A new
    /// store whose `revisions` include the head revision materializes data at
    /// that revision instead of the empty marker.
    struct ScriptedStore {
        revisions: BTreeMap<u64, Vec<u64>>,
        head: u64,
        live: VecDeque<RevisionRead<u64, u64>>,
        /// When set, `open` returns exactly `live` without synthesizing the
        /// replay boundary, for contracts the walker must uphold even against
        /// a misbehaving source.
        explicit_reads: bool,
    }

    impl ScriptedStore {
        /// A scripted store bypassing the replay model, for contracts the
        /// walker must uphold even against a misbehaving source.
        fn explicit(reads: VecDeque<RevisionRead<u64, u64>>) -> Self {
            Self {
                revisions: BTreeMap::new(),
                head: 0,
                live: reads,
                explicit_reads: true,
            }
        }
    }

    #[async_trait]
    impl RevisionedStore for ScriptedStore {
        type Revision = u64;
        type Entry = u64;
        type Selector = u64;
        type Error = ScriptError;
        type Reader<'a>
            = ScriptedReader
        where
            Self: 'a;

        async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
            Ok(self.head)
        }

        async fn open<'a>(
            &'a self,
            _selector: Self::Selector,
            after: u64,
        ) -> Result<Self::Reader<'a>, Self::Error> {
            if self.explicit_reads {
                return Ok(ScriptedReader {
                    reads: self.live.clone(),
                });
            }
            let mut reads = VecDeque::new();
            for (revision, entries) in self
                .revisions
                .range((Bound::Excluded(after), Bound::Unbounded))
            {
                reads.push_back(RevisionRead::Entries {
                    revision: *revision,
                    entries: entries.clone(),
                });
            }
            if reads.is_empty() && self.head > 0 && self.head >= after {
                reads.push_back(RevisionRead::Entries {
                    revision: self.head,
                    entries: Vec::new(),
                });
            }
            reads.push_back(RevisionRead::ReplayComplete { through: self.head });
            reads.extend(self.live.iter().cloned());
            Ok(ScriptedReader { reads })
        }
    }

    /// A reassession at `durable == source head` is the exact crash shape
    /// seen in daybook_core: the reader surfaces an empty batch stamped at the
    /// head revision (nothing qualified beyond the per-key floor), which the
    /// walker previously misclassified as a non-advancing regression. It must
    /// surface `ReplayComplete` instead and keep streaming the live tail.
    #[test]
    fn reopen_at_durable_head_is_not_a_regression() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                revisions: BTreeMap::from([(2, vec![20]), (4, vec![40])]),
                head: 4,
                live: VecDeque::from([RevisionRead::Entries {
                    revision: 5,
                    entries: vec![50],
                }]),
                explicit_reads: false,
            };
            let state = MemoryStateRepo::new(4); // durable == head
            let mut walker = open_walker(&store, &state).await;
            assert_eq!(
                walker.next().await.unwrap(),
                RevisionRead::ReplayComplete { through: 4 }
            );
            assert_eq!(
                walker.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 5,
                    entries: vec![50],
                }
            );
        });
    }

    /// A reopen over a changed selector (e.g. a grown facet-tag set) against
    /// a quiescent source must neither error nor re-emit data at or below the
    /// durable floor.
    #[test]
    fn changed_selector_over_quiescent_source_never_replays_below_durable() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                revisions: BTreeMap::from([(1, vec![10]), (2, vec![20])]),
                head: 2,
                live: VecDeque::new(),
                explicit_reads: false,
            };
            let state = MemoryStateRepo::new(2);
            let mut walker = open_walker(&store, &state).await;
            assert_eq!(
                walker.next().await.unwrap(),
                RevisionRead::ReplayComplete { through: 2 }
            );
        });
    }

    /// Empty batches above the durable floor are legitimate progress markers
    /// (ADR 008: a projection may advance without typed output). They must
    /// still reach the consumer and settle, advancing durable progress.
    #[test]
    fn empty_advancing_batch_still_settles_and_advances() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                revisions: BTreeMap::from([(3, Vec::new())]),
                head: 3,
                live: VecDeque::new(),
                explicit_reads: false,
            };
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&store, &state).await;
            assert_eq!(
                walker.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 3,
                    entries: Vec::new(),
                }
            );
            walker.settle(3).await.unwrap();
            assert_eq!(state.progress().await.unwrap().upstream_revision, 3);
        });
    }

    /// A non-empty batch at or below the durable floor is still a regression
    /// and must keep failing loudly; only empty no-op batches are tolerated.
    #[test]
    fn non_empty_non_advancing_batch_remains_a_regression() {
        futures::executor::block_on(async {
            let store = ScriptedStore::explicit(VecDeque::from([RevisionRead::Entries {
                revision: 5,
                entries: vec![50],
            }]));
            let state = MemoryStateRepo::new(5);
            let mut walker = open_walker(&store, &state).await;
            assert!(matches!(
                walker.next().await,
                Err(SerialDeltaWalkerError::NonAdvancingRevision)
            ));
        });
    }

    /// Durable progress is monotonic across open/next/settle cycles and a
    /// reopen at the settled head observes only the replay boundary.
    #[test]
    fn progress_is_monotonic_across_reopen_settle_cycles() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                revisions: BTreeMap::from([(1, vec![10]), (2, vec![20])]),
                head: 2,
                live: VecDeque::new(),
                explicit_reads: false,
            };
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&store, &state).await;
            assert_eq!(
                walker.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 1,
                    entries: vec![10],
                }
            );
            walker.settle(1).await.unwrap();
            assert_eq!(state.progress().await.unwrap().upstream_revision, 1);
            assert_eq!(
                walker.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 2,
                    entries: vec![20],
                }
            );
            walker.settle(2).await.unwrap();
            assert_eq!(state.progress().await.unwrap().upstream_revision, 2);
            drop(walker);

            let mut reopened = open_walker(&store, &state).await;
            assert_eq!(
                reopened.next().await.unwrap(),
                RevisionRead::ReplayComplete { through: 2 }
            );
            assert_eq!(state.progress().await.unwrap().upstream_revision, 2);
        });
    }

    /// A revision retained by a consumer and not settled is replayed verbatim
    /// by a reopened session from the same durable floor - never dropped, and
    /// never shifted onto a different revision.
    #[test]
    fn deferred_revision_replays_after_reopen() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                revisions: BTreeMap::from([(1, vec![10]), (2, vec![20])]),
                head: 2,
                live: VecDeque::new(),
                explicit_reads: false,
            };
            let state = MemoryStateRepo::new(0);
            {
                let mut first = open_walker(&store, &state).await;
                assert_eq!(
                    first.next().await.unwrap(),
                    RevisionRead::Entries {
                        revision: 1,
                        entries: vec![10],
                    }
                );
                // consumer retains (revision 1, entries) and the session is
                // dropped without settling, exactly like a reopen mid-defer.
            }
            let mut reopened = open_walker(&store, &state).await;
            assert_eq!(
                reopened.next().await.unwrap(),
                RevisionRead::Entries {
                    revision: 1,
                    entries: vec![10],
                }
            );
            reopened.settle(1).await.unwrap();
            assert_eq!(state.progress().await.unwrap().upstream_revision, 1);
        });
    }

    /// A retained revision cannot be settled on a fresh session that never
    /// read it: settlement requires the walker to have observed the revision
    /// itself, so a reopened walker must replay (see the replay test) rather
    /// than blindly accepting a stale batch.
    #[test]
    fn deferred_revision_cannot_settle_on_a_fresh_session() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                revisions: BTreeMap::from([(1, vec![10])]),
                head: 1,
                live: VecDeque::new(),
                explicit_reads: false,
            };
            let state = MemoryStateRepo::new(0);
            {
                let mut first = open_walker(&store, &state).await;
                assert_eq!(
                    first.next().await.unwrap(),
                    RevisionRead::Entries {
                        revision: 1,
                        entries: vec![10],
                    }
                );
            }
            let mut fresh = open_walker(&store, &state).await;
            assert!(matches!(
                fresh.begin_settlement(1).await,
                Err(SerialDeltaWalkerError::NoPendingRevision)
            ));
        });
    }
}
