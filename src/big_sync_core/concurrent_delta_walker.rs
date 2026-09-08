//! Concurrent, embedder-driven settlement of a revisioned source.
//!
//! The walker owns source reading and durable source progress.  It delegates
//! keyed cursor coverage to [`crate::watermark::WatermarkMachine`].  It does
//! not know about logical work, task generations, merging, or a runtime.
//!
//! The walker never sees source selection: the caller opens the store's
//! reader with whatever filter it needs (selection is a store-open concern,
//! compiled into the store's query) and hands the opened reader to
//! [`ConcurrentDeltaWalker::open`].

use crate::delta_walker_state::{
    DeltaWalkerStateError, DeltaWalkerStateRepo, DeltaWalkerStateTransaction,
};
use crate::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use crate::watermark::WatermarkMachine;
use std::collections::VecDeque;
use std::num::NonZeroUsize;

/// A source entry with the key used by the embedder when acknowledging it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrentDelta<K, E> {
    pub key: K,
    pub cursor: u64,
    pub entry: E,
}

/// One result from [`ConcurrentDeltaWalker::next`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrentDeltaRead<K, E> {
    Entries {
        revision: u64,
        entries: Vec<ConcurrentDelta<K, E>>,
    },
    ReplayComplete {
        through: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaAck {
    Stale,
    Accepted { through: Option<u64> },
}

#[derive(Debug, thiserror::Error)]
pub enum ConcurrentDeltaWalkerError<E> {
    #[error("revision source failed: {0}")]
    Source(E),
    #[error("delta walker state failed: {0}")]
    State(#[from] DeltaWalkerStateError),
}

type DeltaKeyFn<S, K> = Box<dyn Fn(&<S as RevisionedStore>::Entry) -> K + Send + Sync>;

/// Concurrent revision source and durable consumer state.
///
/// This type intentionally handles one source.  An application with several
/// independent revision sources owns several walkers and feeds their deltas
/// into one task scheduler.
pub struct ConcurrentDeltaWalker<'a, S, R, K>
where
    S: RevisionedStore<Revision = u64> + 'a,
    R: DeltaWalkerStateRepo,
    K: Ord + Copy,
{
    reader: S::Reader<'a>,
    state: R,
    key_of: DeltaKeyFn<S, K>,
    durable_revision: u64,
    watermarks: WatermarkMachine<(), K, (), (), u64>,
    buffered: VecDeque<ConcurrentDelta<K, S::Entry>>,
}

impl<'a, S, R, K> ConcurrentDeltaWalker<'a, S, R, K>
where
    S: RevisionedStore<Revision = u64> + 'a,
    R: DeltaWalkerStateRepo,
    K: Ord + Copy,
{
    /// Take an already-opened reader and the consumer's state repo.
    ///
    /// The reader was opened by the caller at the state repo's durable
    /// progress with the caller's own selection; the walker only reads
    /// `state.progress()` for its durable watermark bookkeeping and takes
    /// ownership of settlement. The state repo is exclusively owned by this
    /// consumer, so the caller's `after` and the progress read here agree.
    pub async fn open(
        reader: S::Reader<'a>,
        state: R,
        key_of: impl Fn(&S::Entry) -> K + Send + Sync + 'static,
    ) -> Result<Self, ConcurrentDeltaWalkerError<S::Error>> {
        let durable_revision = state.progress().await?.upstream_revision;
        Ok(Self {
            reader,
            state,
            key_of: Box::new(key_of),
            durable_revision,
            watermarks: WatermarkMachine::default(),
            buffered: VecDeque::new(),
        })
    }

    pub fn durable_revision(&self) -> u64 {
        self.durable_revision
    }

    /// Diagnostics: the job keys with unresolved waiters, each with the
    /// cursor still gating it.
    pub fn pending_jobs(&self) -> Vec<(K, u64)> {
        self.watermarks.pending_jobs()
    }

    /// Read at most `limit` source entries.  A source revision larger than the
    /// limit is returned over several calls, while its keyed dependencies are
    /// registered atomically on the first call.
    pub async fn next(
        &mut self,
        limit: NonZeroUsize,
    ) -> Result<ConcurrentDeltaRead<K, S::Entry>, ConcurrentDeltaWalkerError<S::Error>> {
        if !self.buffered.is_empty() {
            let count = limit.get().min(self.buffered.len());
            return Ok(ConcurrentDeltaRead::Entries {
                revision: self.buffered.front().expect("buffer is non-empty").cursor,
                entries: self.buffered.drain(..count).collect(),
            });
        }

        loop {
            let read = self
                .reader
                .next(RevisionReadLimits { max_entries: limit })
                .await
                .map_err(ConcurrentDeltaWalkerError::Source)?;
            match read {
                RevisionRead::ReplayComplete { through } => {
                    return Ok(ConcurrentDeltaRead::ReplayComplete { through });
                }
                RevisionRead::Entries { revision, entries } => {
                    if !self.watermarks.admit((), revision) {
                        continue;
                    }

                    self.buffered.extend(entries.into_iter().map(|entry| {
                        let key = (self.key_of)(&entry);
                        self.watermarks.track((), key, revision, [()], ());
                        ConcurrentDelta {
                            key,
                            cursor: revision,
                            entry,
                        }
                    }));
                    if self.buffered.is_empty() {
                        let through = self.watermarks.finish((), revision);
                        self.persist_ready(through).await?;
                        continue;
                    }

                    let count = limit.get().min(self.buffered.len());
                    return Ok(ConcurrentDeltaRead::Entries {
                        revision,
                        entries: self.buffered.drain(..count).collect(),
                    });
                }
            }
        }
    }

    /// Acknowledge the newest completed cursor for a key.  Completion of that
    /// cursor supersedes older pending cursors for the same key before the
    /// source watermark is drained.
    pub async fn ack(
        &mut self,
        key: K,
        cursor: u64,
    ) -> Result<DeltaAck, ConcurrentDeltaWalkerError<S::Error>> {
        let settled = self.watermarks.settle(key, cursor, ());
        if settled.is_empty() {
            return Ok(DeltaAck::Stale);
        }

        let mut through = settled.into_iter().find_map(|((), reached)| reached);
        for reached in self.watermarks.supersede((), key, cursor, |_| false) {
            through = through.max(reached);
        }
        let persisted = self.persist_ready(through).await?;
        Ok(DeltaAck::Accepted { through: persisted })
    }

    async fn persist_ready(
        &mut self,
        through: Option<u64>,
    ) -> Result<Option<u64>, ConcurrentDeltaWalkerError<S::Error>> {
        let Some(through) = through else {
            return Ok(None);
        };
        if through <= self.durable_revision {
            return Ok(None);
        }
        let mut expected = self.durable_revision;
        loop {
            let mut transaction = self.state.begin().await?;
            if let Err(error) = transaction.advance_from(expected, through).await {
                if let DeltaWalkerStateError::StaleProgress { .. } = error {
                    let actual = transaction.progress().await?;
                    transaction.rollback().await?;
                    if actual.upstream_revision >= through {
                        self.durable_revision = actual.upstream_revision;
                        return Ok(Some(actual.upstream_revision));
                    }
                    expected = actual.upstream_revision;
                    self.durable_revision = expected;
                    continue;
                }
                return Err(ConcurrentDeltaWalkerError::State(error));
            }
            transaction.commit().await?;
            self.durable_revision = through;
            return Ok(Some(through));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_walker_state::{
        DeltaWalkerProgress, DeltaWalkerStateError, DeltaWalkerStateRepo,
        DeltaWalkerStateTransaction,
    };
    use std::sync::{Arc, Mutex};

    const TEST_BATCH_LIMIT: NonZeroUsize = NonZeroUsize::new(10).expect("literal is non-zero");

    // In-memory durable state double, mirroring the SQLite backend's
    // strictly-advancing settlement semantics (see serial_delta_walker
    // tests for the original).
    #[derive(Default)]
    struct MemoryState {
        progress: u64,
    }

    #[derive(Default, Clone)]
    struct MemoryStateRepo {
        inner: Arc<Mutex<MemoryState>>,
    }

    impl MemoryStateRepo {
        fn new(progress: u64) -> Self {
            Self {
                inner: Arc::new(Mutex::new(MemoryState { progress })),
            }
        }
    }

    struct MemoryStateTx {
        inner: Arc<Mutex<MemoryState>>,
        staged: Option<u64>,
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateTransaction for MemoryStateTx {
        type Context = ();

        fn context_mut(&mut self) -> &mut Self::Context {
            unreachable!("walker keeps no per-transaction context")
        }

        async fn progress(
            &mut self,
        ) -> crate::delta_walker_state::DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: self.inner.lock().unwrap().progress,
            })
        }

        async fn advance_from(
            &mut self,
            expected: u64,
            next: u64,
        ) -> crate::delta_walker_state::DeltaWalkerStateResult<()> {
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
            self.staged = Some(next);
            Ok(())
        }

        async fn commit(self) -> crate::delta_walker_state::DeltaWalkerStateResult<()> {
            if let Some(next) = self.staged {
                self.inner.lock().unwrap().progress = next;
            }
            Ok(())
        }

        async fn rollback(self) -> crate::delta_walker_state::DeltaWalkerStateResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateRepo for MemoryStateRepo {
        type Context<'a>
            = ()
        where
            Self: 'a;
        type Transaction<'a>
            = MemoryStateTx
        where
            Self: 'a;

        async fn progress(
            &self,
        ) -> crate::delta_walker_state::DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: self.inner.lock().unwrap().progress,
            })
        }

        async fn begin<'a>(
            &'a self,
        ) -> crate::delta_walker_state::DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemoryStateTx {
                inner: Arc::clone(&self.inner),
                staged: None,
            })
        }

        async fn begin_with_context<'a>(
            &'a self,
            _context: Self::Context<'a>,
        ) -> crate::delta_walker_state::DeltaWalkerStateResult<Self::Transaction<'a>>
        where
            Self: 'a,
        {
            self.begin().await
        }
    }

    /// Scripted source: each entry is `(key, revision)`; reads hand out one
    /// pre-arranged `RevisionRead` per call.
    #[derive(Default)]
    struct ScriptedSource {
        reads: Mutex<VecDeque<RevisionRead<u64, u64>>>,
    }

    impl ScriptedSource {
        fn with(reads: Vec<RevisionRead<u64, u64>>) -> Self {
            Self {
                reads: Mutex::new(reads.into()),
            }
        }
    }

    struct ScriptedReader {
        reads: Mutex<VecDeque<RevisionRead<u64, u64>>>,
    }

    #[async_trait::async_trait]
    impl RevisionedStore for ScriptedSource {
        type Revision = u64;
        type Entry = u64;
        type Selector = ();
        type Error = crate::delta_walker_state::DeltaWalkerStateError;
        type Reader<'a> = ScriptedReader;

        async fn latest_revision(&self) -> Result<u64, DeltaWalkerStateError> {
            Ok(self
                .reads
                .lock()
                .unwrap()
                .back()
                .map(|read| match read {
                    RevisionRead::Entries { revision, .. } => *revision,
                    RevisionRead::ReplayComplete { through } => *through,
                })
                .unwrap_or_default())
        }

        async fn open<'a>(
            &'a self,
            _selector: (),
            _after: u64,
        ) -> Result<Self::Reader<'a>, DeltaWalkerStateError> {
            Ok(ScriptedReader {
                reads: Mutex::new(self.reads.lock().unwrap().clone()),
            })
        }
    }

    #[async_trait::async_trait]
    impl RevisionedStoreReader<u64, u64, crate::delta_walker_state::DeltaWalkerStateError>
        for ScriptedReader
    {
        async fn next(
            &mut self,
            _limits: RevisionReadLimits,
        ) -> Result<RevisionRead<u64, u64>, DeltaWalkerStateError> {
            Ok(self
                .reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted source exhausted"))
        }
    }

    fn batch(revision: u64, keys: &[u64]) -> RevisionRead<u64, u64> {
        RevisionRead::Entries {
            revision,
            entries: keys.to_vec(),
        }
    }

    async fn open_walker<'a>(
        source: &'a ScriptedSource,
        state: MemoryStateRepo,
    ) -> ConcurrentDeltaWalker<'a, ScriptedSource, MemoryStateRepo, u64> {
        let durable = state.progress().await.unwrap().upstream_revision;
        let reader = source
            .open((), durable)
            .await
            .expect("scripted source open must succeed");
        ConcurrentDeltaWalker::open(reader, state, |key| *key)
            .await
            .expect("walker open must succeed")
    }

    #[test]
    fn batch_siblings_gate_the_durable_revision() {
        futures::executor::block_on(async {
            // One batch at revision 10 carrying two keys: settling one key must
            // not durably advance past the still-pending sibling.
            let source = ScriptedSource::with(vec![batch(10, &[7, 8])]);
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&source, state.clone()).await;

            let ConcurrentDeltaRead::Entries { entries, .. } = walker
                .next(TEST_BATCH_LIMIT)
                .await
                .expect("batch read must succeed")
            else {
                panic!("expected entries");
            };
            assert_eq!(entries.len(), 2);

            let ack = walker.ack(7, 10).await.expect("ack must succeed");
            assert!(matches!(ack, DeltaAck::Accepted { through: None }));
            assert_eq!(walker.durable_revision(), 0, "sibling still pending");

            let ack = walker.ack(8, 10).await.expect("ack must succeed");
            assert!(matches!(ack, DeltaAck::Accepted { through: Some(10) }));
            assert_eq!(walker.durable_revision(), 10);
        });
    }

    #[test]
    fn duplicate_key_rows_in_one_batch_settle_the_slot_once() {
        futures::executor::block_on(async {
            // A batch carrying TWO rows for the same document key tracks one
            // merged waiter; a single settle must finish the batch slot. (This
            // regressed when per-slot waiter counts were introduced: the merged
            // waiter double-counted and the slot never finished, freezing the
            // durable cursor.)
            let source = ScriptedSource::with(vec![batch(10, &[7, 7]), batch(11, &[8])]);
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&source, state.clone()).await;

            walker.next(TEST_BATCH_LIMIT).await.expect("first batch");
            walker.next(TEST_BATCH_LIMIT).await.expect("second batch");

            // The single ack for the duplicated key finishes cursor 10's
            // slot (both rows are one waiter), but cursor 11 still gates.
            let ack = walker.ack(7, 10).await.expect("ack must succeed");
            assert!(matches!(ack, DeltaAck::Accepted { through: Some(10) }));
            assert_eq!(walker.durable_revision(), 10);

            let ack = walker.ack(8, 11).await.expect("ack must succeed");
            assert!(matches!(ack, DeltaAck::Accepted { through: Some(11) }));
            assert_eq!(walker.durable_revision(), 11);
        });
    }

    #[test]
    fn durable_revision_advances_only_over_the_contiguous_prefix() {
        futures::executor::block_on(async {
            let source = ScriptedSource::with(vec![batch(10, &[7, 8]), batch(11, &[9])]);
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&source, state.clone()).await;

            walker.next(TEST_BATCH_LIMIT).await.expect("first batch");
            walker.next(TEST_BATCH_LIMIT).await.expect("second batch");

            // Out-of-order settles: neither 9 nor 8 alone unblocks the prefix.
            walker.ack(9, 11).await.expect("ack must succeed");
            assert_eq!(walker.durable_revision(), 0);
            walker.ack(8, 10).await.expect("ack must succeed");
            assert_eq!(walker.durable_revision(), 0);

            // The last pending sibling unblocks 10 and 11 together.
            walker.ack(7, 10).await.expect("ack must succeed");
            assert_eq!(walker.durable_revision(), 11);
        });
    }

    #[test]
    fn newer_ack_supersedes_the_older_pending_cursor_for_the_key() {
        futures::executor::block_on(async {
            let source = ScriptedSource::with(vec![batch(10, &[7]), batch(11, &[7])]);
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&source, state.clone()).await;

            walker.next(TEST_BATCH_LIMIT).await.expect("first batch");
            walker.next(TEST_BATCH_LIMIT).await.expect("second batch");

            // Acking the newest cursor supersedes the older one for the key;
            // both slots free and the watermark drains to 11.
            let ack = walker.ack(7, 11).await.expect("ack must succeed");
            assert!(matches!(ack, DeltaAck::Accepted { through: Some(11) }));
            assert_eq!(walker.durable_revision(), 11);
        });
    }

    #[test]
    fn replay_is_skipped_for_the_durable_prefix_after_restart() {
        futures::executor::block_on(async {
            let source = ScriptedSource::with(vec![
                batch(10, &[7, 8]),
                RevisionRead::ReplayComplete { through: 10 },
            ]);
            let state = MemoryStateRepo::new(0);
            let mut walker = open_walker(&source, state.clone()).await;
            walker.next(TEST_BATCH_LIMIT).await.expect("batch read");
            assert!(matches!(
                walker
                    .next(TEST_BATCH_LIMIT)
                    .await
                    .expect("replay boundary"),
                ConcurrentDeltaRead::ReplayComplete { through: 10 }
            ));
            walker.ack(7, 10).await.expect("ack must succeed");
            // Sibling still pending: the walker must not have persisted 10.
            assert_eq!(walker.durable_revision(), 0);

            // A fresh walker over the same durable state replays from 0: the
            // unsuperseded sibling is re-delivered (at-least-once).
            let mut restarted = open_walker(&source, state.clone()).await;
            let ConcurrentDeltaRead::Entries { entries, .. } = restarted
                .next(TEST_BATCH_LIMIT)
                .await
                .expect("replayed batch")
            else {
                panic!("expected replayed entries");
            };
            assert_eq!(
                entries.iter().map(|d| d.key).collect::<Vec<_>>(),
                vec![7, 8]
            );
        });
    }

    #[test]
    fn stale_acks_do_not_advance_the_durable_revision() {
        futures::executor::block_on(async {
            let source = ScriptedSource::with(vec![batch(10, &[7])]);
            let state = MemoryStateRepo::new(10);
            let mut walker = open_walker(&source, state.clone()).await;
            // Cursor at the already-emitted watermark: stale.
            let ack = walker.ack(7, 10).await.expect("ack must succeed");
            assert!(matches!(ack, DeltaAck::Stale));
            assert_eq!(walker.durable_revision(), 10);
        });
    }
}
