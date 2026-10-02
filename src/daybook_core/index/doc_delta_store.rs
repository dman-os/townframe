//! DocDelta revisioned store: an inert transformation over the Automerge
//! frontier source, consumed through a `ConcurrentDeltaWalker` driven by a
//! keyed worker (see facet_set).
//! The store maps `AutomergeFrontierEvent`s into per-branch head transitions
//! (`DocDelta`), keyed per consumer site by a durable sparse memory of the
//! last heads seen per branch. It talks about BRANCH DOCS — physical
//! automerge objects — not logical documents. Logical identity (which
//! document a branch belongs to) is content-derived (ADR 007) and is resolved
//! by consumers as part of their own projection work, where they hydrate the
//! branch doc anyway. The store owns no cursor, no event loop, and does no
//! content I/O; consumers walk it with `ConcurrentDeltaWalker` and drive
//! keyed tasks through `TokioKeyedScheduler`.
//! 1. the consumer's effect must be idempotent and latest-state;
//! 2. the effect and the memory advance commit in ONE transaction
//!    (`begin_settlement` hands out the effect surface through `context_mut`,
//!    `settle` commits it);
//! 3. the walker cursor is acked only after that commit.
//!
//! With that contract, `memory(branch)` may lead the durable cursor (never
//! lag the durable effect). A crash between the memory commit and the ack
//! replays the entry, the reader diffs the new memory against the same heads,
//! gets an unchanged transition and DROPS the entry — the walker settles the
//! entry-less revision via `finish` and replay catch-up costs zero jobs.
//! Filtering: everything except branch shape is a part-store subscription on
//! the source (keyhive group → group-part sub; specific docs/branches →
//! object subs). Branch-name filtering works without identity resolution
//! because the Branch facet pins the branch name to the physical doc id.

use crate::interlude::*;
use big_repo::{AutomergeFrontierEvent, AutomergeFrontierSelector};
use big_sync_core::delta_walker_sparse_state::{
    DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
};
use big_sync_core::delta_walker_state::{DeltaWalkerStateRepo, DeltaWalkerStateTransaction};
use big_sync_core::revisioned_store::{RevisionRead, RevisionedStore, RevisionedStoreReader};
use daybook_types::doc::{BranchId, ChangeHashSet};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

/// Sparse per-site memory row for one physical branch.
///
/// Exists only once the site has consumed a transition for the branch. A
/// tombstone stores `None`, so a later re-add is an ordinary
/// `None -> heads` transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocDeltaBranchState {
    pub heads: Option<ChangeHashSet>,
    #[serde(default)]
    pub causal_epoch: Option<[u8; 32]>,
}

/// One head transition for one physical branch, as seen by one site.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocDelta {
    pub branch_id: BranchId,
    pub previous_heads: Option<ChangeHashSet>,
    pub current_heads: Option<ChangeHashSet>,
    pub previous_causal_epoch: Option<[u8; 32]>,
    pub current_causal_epoch: Option<[u8; 32]>,
    /// True when only the materialization epoch changed; heads were already acknowledged.
    pub epoch_only: bool,
}

/// Branch filter, applied per event. This is the ONLY post-event filter:
/// everything else — keyhive group scope, specific documents, specific
/// branches — translates to part-store subscriptions on the source (a
/// keyhive group is a group-part subscription; a specific doc's main branch
/// is an object subscription). Group scoping lives in the source selector,
/// mirroring how workers scope themselves; it is never re-filtered per
/// event.
///
/// Branch-name filtering needs no content I/O: the Branch facet pins the
/// branch name to the physical doc id, so the name is derivable from the
/// event alone. Main-branch selection ("branch == document") is
/// intentionally absent — it needs content-derived identity; when main
/// branches get their own keyhive group part it becomes a plain
/// subscription. Until then, main-branch-only consumers are per-document
/// object subscriptions (main branch: BranchId == DocumentId, ADR 007).
///
/// The filter is live across diffs by construction: a branch created after
/// the walker opened is filtered at its first event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocDeltaBranchFilter {
    /// Every branch in the source scope.
    All,
    /// Only branches whose logical branch name (== physical doc id) matches.
    // No production consumer constructs this yet; the branch-name filter is a
    // designed capability (see the module docs above) kept for per-doc
    // consumers, and this module's tests drive it.
    #[cfg_attr(not(test), expect(dead_code))]
    Named { names: BTreeSet<String> },
}

impl DocDeltaBranchFilter {
    fn admits(&self, branch_id: &BranchId) -> bool {
        match self {
            DocDeltaBranchFilter::All => true,
            DocDeltaBranchFilter::Named { names } => names.contains(&branch_id.0),
        }
    }
}

/// Selection for one consuming site: the site's durable memory, the physical
/// source scope, and the branch filter.
///
/// The source scope carries all group/document selection as part-store
/// subscriptions; the filter only shapes branches within that scope. Most
/// stores are single-doc and select one object with
/// `DocDeltaBranchFilter::All`.
#[derive(Clone, Debug)]
pub struct DocDeltaSelector<M> {
    /// The site's memory repo. Its namespace IS the site identity — two sites
    /// never share one, and nothing else about the site is stored anywhere.
    pub memory: M,
    pub source: AutomergeFrontierSelector,
    pub filter: DocDeltaBranchFilter,
}

/// Inert transformation over an Automerge frontier source.
///
/// Never writes the memory, the source, or anything else; performs no
/// content I/O of any kind. All durable writes belong to consumers
/// (`begin_settlement`). The memory repo type is carried by the store only
/// to fix its selector type.
pub struct DocDeltaRevisionStore<S, M> {
    source: S,
    _memory: std::marker::PhantomData<M>,
}

impl<S, M> DocDeltaRevisionStore<S, M> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            _memory: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<S, M> RevisionedStore for DocDeltaRevisionStore<S, M>
where
    S: RevisionedStore<
            Revision = u64,
            Entry = AutomergeFrontierEvent,
            Selector = AutomergeFrontierSelector,
            Error = eyre::Report,
        >,
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'static,
{
    type Revision = u64;
    type Entry = DocDelta;
    type Selector = DocDeltaSelector<M>;
    type Error = eyre::Report;
    type Reader<'a>
        = DocDeltaReader<'a, S, M>
    where
        Self: 'a;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.source.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: u64,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        Ok(DocDeltaReader {
            source: self.source.open(selector.source, after).await?,
            memory: selector.memory,
            filter: selector.filter,
            pending_source_read: None,
        })
    }
}

pub struct DocDeltaReader<'a, S, M>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent> + 'a,
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'a,
{
    source: S::Reader<'a>,
    memory: M,
    filter: DocDeltaBranchFilter,
    pending_source_read: Option<RevisionRead<u64, AutomergeFrontierEvent>>,
}

#[async_trait::async_trait]
impl<S, M> RevisionedStoreReader<u64, DocDelta, eyre::Report> for DocDeltaReader<'_, S, M>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent, Error = eyre::Report>,
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo,
{
    async fn next(
        &mut self,
        limits: big_sync_core::revisioned_store::RevisionReadLimits,
    ) -> Result<RevisionRead<u64, DocDelta>, eyre::Report> {
        {
            if self.pending_source_read.is_none() {
                self.pending_source_read = Some(self.source.next(limits).await?);
            }
            let (revision, entries) = match self
                .pending_source_read
                .as_ref()
                .expect("pending source read initialized")
            {
                RevisionRead::ReplayComplete { through } => {
                    let through = *through;
                    self.pending_source_read = None;
                    return Ok(RevisionRead::ReplayComplete { through });
                }
                RevisionRead::Entries { revision, entries } => (*revision, entries.clone()),
            };

            let keys: Vec<Vec<u8>> = entries
                .iter()
                .map(|event| state_key(&event_branch_id(event)))
                .collect();
            let stored: HashMap<BranchId, DocDeltaBranchState> = self
                .memory
                .get_many(&keys)
                .await
                .map_err(memory_error)?
                .into_iter()
                .map(|(key, bytes)| {
                    let state: DocDeltaBranchState = serde_json::from_slice(&bytes)?;
                    Ok((state_key_to_branch(key), state))
                })
                .collect::<Res<HashMap<_, _>>>()?;

            let mut out = Vec::new();
            'entry: for event in entries {
                let branch_id = event_branch_id(&event);
                if !self.filter.admits(&branch_id) {
                    continue;
                }
                let previous = stored.get(&branch_id);
                let Some(delta) = frontier_delta(
                    branch_id,
                    previous,
                    event_heads(&event),
                    event_causal_epoch(&event),
                ) else {
                    continue 'entry;
                };
                out.push(delta);
            }

            // An all-no-op revision yields zero entries; the concurrent walker
            // settles entry-less revisions directly and re-reads.
            self.pending_source_read = None;
            return Ok(RevisionRead::Entries {
                revision,
                entries: out,
            });
        }
    }
}

/// The consumer's settlement transaction. The effect is applied through
/// `context_mut` (same SQLite connection and commit boundary as the memory
/// advance); `settle` persists the memory row and commits.
pub struct DocDeltaSettlement<'a, M>
where
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'a,
    M::Transaction<'a>: DeltaWalkerSparseStateTransaction,
{
    tx: M::Transaction<'a>,
    delta: DocDelta,
}

impl<'a, M> DocDeltaSettlement<'a, M>
where
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'a,
    M::Transaction<'a>: DeltaWalkerSparseStateTransaction,
{
    pub fn context_mut(&mut self) -> &mut M::Context<'a> {
        self.tx.context_mut()
    }

    pub async fn settle(self) -> Res<()> {
        let mut tx = self.tx;
        let state = DocDeltaBranchState {
            heads: self.delta.current_heads.clone(),
            causal_epoch: self.delta.current_causal_epoch,
        };
        tx.put(
            state_key(&self.delta.branch_id),
            serde_json::to_vec(&state)?,
        )
        .await
        .map_err(memory_error)?;
        tx.commit().await.map_err(memory_error)?;
        Ok(())
    }
}

/// Open the consumer contract's settlement transaction: the effect applied
/// through `context_mut` and the memory row commit together; callers ack the
/// walker cursor only after `settle` returns.
pub async fn begin_settlement<'a, M>(
    memory: &'a M,
    delta: &DocDelta,
) -> Res<DocDeltaSettlement<'a, M>>
where
    M: DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo + 'a,
    M::Transaction<'a>: DeltaWalkerSparseStateTransaction,
{
    let tx = memory.begin().await.map_err(memory_error)?;
    Ok(DocDeltaSettlement {
        tx,
        delta: delta.clone(),
    })
}

fn state_key(branch_id: &BranchId) -> Vec<u8> {
    branch_id.0.as_bytes().to_vec()
}

fn state_key_to_branch(key: Vec<u8>) -> BranchId {
    BranchId(String::from_utf8(key).expect("branch state keys are utf-8 branch ids"))
}

fn frontier_delta(
    branch_id: BranchId,
    stored: Option<&DocDeltaBranchState>,
    current_heads: Option<ChangeHashSet>,
    current_causal_epoch: Option<[u8; 32]>,
) -> Option<DocDelta> {
    let previous_heads = match &current_heads {
        Some(_) => stored.and_then(|state| state.heads.clone()),
        None => match stored {
            Some(state) if state.heads.is_some() => state.heads.clone(),
            _ => return None,
        },
    };
    let previous_causal_epoch = stored.and_then(|state| state.causal_epoch);
    let heads_changed = previous_heads != current_heads;
    let epoch_changed = previous_causal_epoch != current_causal_epoch;
    (heads_changed || epoch_changed).then_some(DocDelta {
        branch_id,
        previous_heads,
        current_heads,
        previous_causal_epoch,
        current_causal_epoch,
        epoch_only: !heads_changed && epoch_changed,
    })
}

fn event_branch_id(event: &AutomergeFrontierEvent) -> BranchId {
    let doc_id = match event {
        AutomergeFrontierEvent::Changed { doc_id, .. }
        | AutomergeFrontierEvent::Removed { doc_id, .. } => doc_id,
    };
    BranchId(doc_id.to_string())
}

fn event_heads(event: &AutomergeFrontierEvent) -> Option<ChangeHashSet> {
    match event {
        AutomergeFrontierEvent::Changed { heads, .. } => Some(ChangeHashSet(Arc::clone(heads))),
        AutomergeFrontierEvent::Removed { .. } => None,
    }
}

fn event_causal_epoch(event: &AutomergeFrontierEvent) -> Option<[u8; 32]> {
    match event {
        AutomergeFrontierEvent::Changed { causal_epoch, .. } => *causal_epoch,
        AutomergeFrontierEvent::Removed { .. } => None,
    }
}

fn memory_error(error: impl std::fmt::Display) -> eyre::Report {
    ferr!("DocDelta memory error: {error}")
}

// ---------------------------------------------------------------------------
// Consumer worker shape (usage sketch, not implemented here)
// ---------------------------------------------------------------------------
//
// struct FacetSetWorker<'a> {
//     walker: ConcurrentDeltaWalker<'a, DocDeltaRevisionStore,
//                                   SqliteDeltaWalkerStateRepo, DocDeltaKey>,
//     tasks:  TokioKeyedScheduler<DocDeltaKey, FacetSetTask, TaskOutput>,
//     pending: HashMap<DocDeltaKey, Vec<ConcurrentDelta<DocDeltaKey, DocDelta>>>,
// }
//
// machine_loop = select! { completions / walker.next(budget) / cancel }:
//   - on delta:  pending[key].push(delta); start_task(key)
//   - task body: for the NEWEST pending delta (keyed `replace` collapses
//                same-key entries; the source surfaces latest-per-doc anyway):
//                    let mut settlement = begin_settlement(&memory, &delta).await?;
//                    apply_projection(settlement.context_mut());
//                    settlement.settle().await?
//   - completion: walker.ack(key, newest_cursor)
//
// `DocDeltaKey` is a Copy key derived from the branch id (e.g. its hash);
// collisions only over-serialize a key, never break correctness.
//
// The loop has no wake arm: the reader never blocks on anything.
//
// Consumer-side identity: a consumer that needs the logical document
// (facet-set routing) resolves it in its own projection, where it hydrates
// the branch doc anyway (hydrate_dmeta_state_at_heads looks the doc up by
// physical branch id and carries the content-derived document_id). A
// single-doc consumer (facet stores) already knows its doc and branch by
// construction.

#[cfg(test)]
mod tests {
    use super::*;
    use big_repo::AutomergeFrontierTarget;
    use big_sync_core::concurrent_delta_walker::{ConcurrentDeltaRead, ConcurrentDeltaWalker};
    use big_sync_core::delta_walker_state::{
        DeltaWalkerProgress, DeltaWalkerStateError, DeltaWalkerStateResult,
    };
    use big_sync_core::revisioned_store::RevisionReadLimits;
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;

    #[test]
    fn equal_heads_with_new_epoch_emit_epoch_only_delta() {
        let heads = ChangeHashSet(vec![automerge::ChangeHash([1; 32])].into());
        let previous_epoch = [2; 32];
        let current_epoch = [3; 32];
        let stored = DocDeltaBranchState {
            heads: Some(heads.clone()),
            causal_epoch: Some(previous_epoch),
        };
        let delta = frontier_delta(
            BranchId::from("branch"),
            Some(&stored),
            Some(heads.clone()),
            Some(current_epoch),
        )
        .expect("epoch transition must be emitted");
        assert!(delta.epoch_only);
        assert_eq!(delta.previous_heads, Some(heads.clone()));
        assert_eq!(delta.current_heads, Some(heads));
        assert_eq!(delta.previous_causal_epoch, Some(previous_epoch));
        assert_eq!(delta.current_causal_epoch, Some(current_epoch));
    }

    // ---- settlement-contract harness --------------------------------------
    //
    // The contract in the module docs is a property of the *pair*: the store
    // decides what a revision means and the settlement transaction decides when
    // it becomes durable. The doubles below are the smallest pair that can hold
    // the store to that contract without the product's runtime — a scripted
    // source handing out pre-arranged revisions, and an in-memory repo whose
    // `context_mut` stages the consumer's effect inside the same transaction
    // that publishes the memory row.

    fn heads(byte: u8) -> ChangeHashSet {
        ChangeHashSet(vec![automerge::ChangeHash([byte; 32])].into())
    }

    fn stored_state(
        heads: Option<ChangeHashSet>,
        causal_epoch: Option<[u8; 32]>,
    ) -> DocDeltaBranchState {
        DocDeltaBranchState {
            heads,
            causal_epoch,
        }
    }

    /// The branch key both sides agree on: `BranchId` is the physical branch
    /// doc's id in its text form, which is what `event_branch_id` derives from
    /// the event and therefore what the memory rows are written under (ADR 007:
    /// a branch doc's identity IS its id).
    fn branch_of(doc_id: &ObjKey) -> BranchId {
        BranchId(doc_id.to_string())
    }

    fn changed(
        doc_id: &ObjKey,
        heads: &[u8],
        causal_epoch: Option<[u8; 32]>,
        revision: u64,
    ) -> AutomergeFrontierEvent {
        AutomergeFrontierEvent::Changed {
            doc_id: doc_id.clone(),
            heads: Arc::from(
                heads
                    .iter()
                    .copied()
                    .map(|byte| automerge::ChangeHash([byte; 32]))
                    .collect::<Vec<_>>(),
            ),
            causal_epoch,
            routes: Vec::new(),
            revision,
        }
    }

    fn frontier_selector() -> AutomergeFrontierSelector {
        AutomergeFrontierSelector {
            targets: vec![AutomergeFrontierTarget::All],
        }
    }

    fn doc_delta_selector(
        repo: MemoryRepo,
        filter: DocDeltaBranchFilter,
    ) -> DocDeltaSelector<MemoryRepo> {
        DocDeltaSelector {
            memory: repo,
            source: frontier_selector(),
            filter,
        }
    }

    fn read_limit(entries: usize) -> NonZeroUsize {
        NonZeroUsize::new(entries).expect("a read limit is non-zero")
    }

    /// Scripted frontier source: each `next` hands out the next pre-arranged
    /// read, and exhausting the script is a test bug rather than a source state.
    /// `open` shares one queue because every test here drives a single reader.
    #[derive(Default)]
    struct ScriptedFrontierSource {
        reads: Arc<Mutex<VecDeque<RevisionRead<u64, AutomergeFrontierEvent>>>>,
    }

    impl ScriptedFrontierSource {
        fn with(reads: Vec<RevisionRead<u64, AutomergeFrontierEvent>>) -> Self {
            Self {
                reads: Arc::new(Mutex::new(reads.into())),
            }
        }
    }

    struct ScriptedFrontierReader {
        reads: Arc<Mutex<VecDeque<RevisionRead<u64, AutomergeFrontierEvent>>>>,
    }

    #[async_trait::async_trait]
    impl RevisionedStore for ScriptedFrontierSource {
        type Revision = u64;
        type Entry = AutomergeFrontierEvent;
        type Selector = AutomergeFrontierSelector;
        type Error = eyre::Report;
        type Reader<'a> = ScriptedFrontierReader;

        async fn latest_revision(&self) -> Result<u64, eyre::Report> {
            Ok(self
                .reads
                .lock()
                .expect("script lock")
                .back()
                .map(|read| match read {
                    RevisionRead::Entries { revision, .. } => *revision,
                    RevisionRead::ReplayComplete { through } => *through,
                })
                .unwrap_or_default())
        }

        async fn open<'a>(
            &'a self,
            _selector: AutomergeFrontierSelector,
            _after: u64,
        ) -> Result<Self::Reader<'a>, eyre::Report> {
            Ok(ScriptedFrontierReader {
                reads: Arc::clone(&self.reads),
            })
        }
    }

    #[async_trait::async_trait]
    impl RevisionedStoreReader<u64, AutomergeFrontierEvent, eyre::Report> for ScriptedFrontierReader {
        async fn next(
            &mut self,
            _limits: RevisionReadLimits,
        ) -> Result<RevisionRead<u64, AutomergeFrontierEvent>, eyre::Report> {
            self.reads
                .lock()
                .expect("script lock")
                .pop_front()
                .ok_or_else(|| ferr!("scripted frontier source exhausted"))
        }
    }

    #[derive(Default)]
    struct MemInner {
        progress: u64,
        /// The walker's sparse memory rows, keyed by `state_key`.
        memory: HashMap<Vec<u8>, Vec<u8>>,
        /// The consumer's effect rows: written through the settlement's context
        /// and published by the same commit as `memory`.
        effect: HashMap<Vec<u8>, Vec<u8>>,
    }

    /// The settlement's effect surface, modelled on the SQLite ctx: writes stage
    /// here, so an effect cannot be observed before the commit that also
    /// publishes the memory row. Staging is what makes "one commit or neither"
    /// an assertion rather than a hope.
    #[derive(Default)]
    struct MemContext {
        staged: HashMap<Vec<u8>, Vec<u8>>,
    }

    impl MemContext {
        fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
            self.staged.insert(key.into(), value.into());
        }
    }

    #[derive(Clone, Default)]
    struct MemoryRepo {
        inner: Arc<Mutex<MemInner>>,
    }

    impl MemoryRepo {
        fn effect(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.inner
                .lock()
                .expect("memory repo lock")
                .effect
                .get(key)
                .cloned()
        }

        fn memory(&self, branch_id: &BranchId) -> Option<Vec<u8>> {
            self.inner
                .lock()
                .expect("memory repo lock")
                .memory
                .get(&state_key(branch_id))
                .cloned()
        }

        /// Seed a memory row directly: the state a site holds after consuming a
        /// previous transition, without going through a settlement.
        fn seed_memory(&self, branch_id: &BranchId, state: &DocDeltaBranchState) {
            self.inner.lock().expect("memory repo lock").memory.insert(
                state_key(branch_id),
                serde_json::to_vec(state).expect("branch state serializes"),
            );
        }
    }

    struct MemTx<'a> {
        inner: &'a Mutex<MemInner>,
        context: MemContext,
        staged_memory: HashMap<Vec<u8>, Vec<u8>>,
        deleted_memory: Vec<Vec<u8>>,
        staged_progress: Option<u64>,
    }

    impl<'a> MemTx<'a> {
        fn new(inner: &'a Mutex<MemInner>, context: MemContext) -> Self {
            Self {
                inner,
                context,
                staged_memory: HashMap::new(),
                deleted_memory: Vec::new(),
                staged_progress: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateTransaction for MemTx<'_> {
        type Context = MemContext;

        fn context_mut(&mut self) -> &mut MemContext {
            &mut self.context
        }

        async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: self.inner.lock().expect("memory repo lock").progress,
            })
        }

        async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()> {
            let current = self.inner.lock().expect("memory repo lock").progress;
            if current != expected {
                return Err(DeltaWalkerStateError::StaleProgress { expected });
            }
            if next <= current {
                return Err(DeltaWalkerStateError::NonAdvancingRevision { current, next });
            }
            self.staged_progress = Some(next);
            Ok(())
        }

        async fn commit(self) -> DeltaWalkerStateResult<()> {
            // One lock, so the effect rows, the memory rows and the progress
            // advance are published together or not at all.
            let mut inner = self.inner.lock().expect("memory repo lock");
            inner.memory.extend(self.staged_memory);
            for key in self.deleted_memory {
                inner.memory.remove(&key);
            }
            inner.effect.extend(self.context.staged);
            if let Some(next) = self.staged_progress {
                inner.progress = next;
            }
            Ok(())
        }

        async fn rollback(self) -> DeltaWalkerStateResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerSparseStateTransaction for MemTx<'_> {
        async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            if let Some(value) = self.staged_memory.get(key) {
                return Ok(Some(value.clone()));
            }
            Ok(self
                .inner
                .lock()
                .expect("memory repo lock")
                .memory
                .get(key)
                .cloned())
        }

        async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()> {
            self.staged_memory.insert(key, value);
            Ok(())
        }

        async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()> {
            self.staged_memory.remove(key);
            self.deleted_memory.push(key.to_vec());
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateRepo for MemoryRepo {
        type Context<'a>
            = MemContext
        where
            Self: 'a;
        type Transaction<'a>
            = MemTx<'a>
        where
            Self: 'a;

        async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: self.inner.lock().expect("memory repo lock").progress,
            })
        }

        async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemTx::new(&self.inner, MemContext::default()))
        }

        async fn begin_with_context<'a>(
            &'a self,
            context: Self::Context<'a>,
        ) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemTx::new(&self.inner, context))
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerSparseStateRepo for MemoryRepo {
        async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            Ok(self
                .inner
                .lock()
                .expect("memory repo lock")
                .memory
                .get(key)
                .cloned())
        }

        async fn get_many(
            &self,
            keys: &[Vec<u8>],
        ) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
            let inner = self.inner.lock().expect("memory repo lock");
            Ok(keys
                .iter()
                .filter_map(|key| {
                    inner
                        .memory
                        .get(key)
                        .map(|value| (key.clone(), value.clone()))
                })
                .collect())
        }
    }

    /// A branch's whole lifecycle, each transition diffed against the state the
    /// site's memory already holds rather than against a caller-carried value.
    /// The two dropped arms are the ones that matter: re-emitting a tombstone for
    /// a branch whose row is already tombstoned (or absent) would resurrect a
    /// deleted branch as a fresh delta on every replay.
    #[test]
    fn added_then_changed_then_removed_emit_head_deltas_from_memory() {
        let branch = BranchId::from("branch");

        // Added: no memory row yet, so the transition is `None -> heads`.
        let added = frontier_delta(branch.clone(), None, Some(heads(1)), None)
            .expect("a first event for a branch is a transition");
        assert_eq!(added.previous_heads, None);
        assert_eq!(added.current_heads, Some(heads(1)));
        assert!(!added.epoch_only);

        // Changed: the previous heads come out of the memory row.
        let remembered = stored_state(Some(heads(1)), None);
        let advanced = frontier_delta(branch.clone(), Some(&remembered), Some(heads(2)), None)
            .expect("new heads are a transition");
        assert_eq!(advanced.previous_heads, Some(heads(1)));
        assert_eq!(advanced.current_heads, Some(heads(2)));

        // Removed on a tracked branch: the tombstone transition.
        let tracked = stored_state(Some(heads(2)), None);
        let tombstone = frontier_delta(branch.clone(), Some(&tracked), None, None)
            .expect("removing a tracked branch is a transition");
        assert_eq!(tombstone.previous_heads, Some(heads(2)));
        assert_eq!(
            tombstone.current_heads, None,
            "the tombstone is stored as `None` heads"
        );

        // Removed for a branch this site never tracked: nothing to tombstone.
        assert_eq!(frontier_delta(branch.clone(), None, None, None), None);

        // Removed for an already-tombstoned branch: dropped, so a replayed
        // removal does not resurrect the branch.
        let tombstoned = stored_state(None, None);
        assert_eq!(frontier_delta(branch, Some(&tombstoned), None, None), None);
    }

    /// Replay catch-up: an event whose heads and epoch the memory already holds is
    /// dropped, so the revision comes back entry-less and the walker settles it
    /// through `finish` without ever tracking a key. The pure half pins that the
    /// drop is the equal-heads rule and not an unrelated early return; the walker
    /// half shows the consequence a fence depends on — catch-up costs zero jobs
    /// while the revision still becomes durable.
    #[tokio::test]
    async fn an_event_at_the_stored_heads_is_dropped_and_costs_no_job() {
        let branch = BranchId::from("branch");
        let remembered = stored_state(Some(heads(1)), Some([9; 32]));

        assert_eq!(
            frontier_delta(
                branch.clone(),
                Some(&remembered),
                Some(heads(1)),
                Some([9; 32]),
            ),
            None,
            "an event at the stored heads and epoch is a no-op"
        );

        assert!(
            frontier_delta(branch, Some(&remembered), Some(heads(2)), Some([9; 32])).is_some(),
            "the same row with different heads must still produce a transition"
        );

        // The walker half: the replaying site already holds this event's heads, so
        // the store hands the walker nothing to key on and the revision settles
        // with no job attached to it.
        let doc = ObjKey::new(b"branch-replayed");
        let repo = MemoryRepo::default();
        repo.seed_memory(
            &branch_of(&doc),
            &stored_state(Some(heads(1)), Some([9; 32])),
        );
        let source = ScriptedFrontierSource::with(vec![
            RevisionRead::Entries {
                revision: 1,
                entries: vec![changed(&doc, &[1], Some([9; 32]), 1)],
            },
            RevisionRead::ReplayComplete { through: 1 },
        ]);
        let store: DocDeltaRevisionStore<ScriptedFrontierSource, MemoryRepo> =
            DocDeltaRevisionStore::new(source);
        let reader = store
            .open(
                doc_delta_selector(repo.clone(), DocDeltaBranchFilter::All),
                0,
            )
            .await
            .expect("open reader");
        let mut walker: ConcurrentDeltaWalker<
            '_,
            DocDeltaRevisionStore<ScriptedFrontierSource, MemoryRepo>,
            MemoryRepo,
            daybook_types::doc::BranchId,
        > = ConcurrentDeltaWalker::open(reader, repo.clone(), |delta: &DocDelta| {
            delta.branch_id.clone()
        })
        .await
        .expect("open walker");

        assert_eq!(
            walker
                .next(read_limit(8))
                .await
                .expect("replay a consumed revision"),
            ConcurrentDeltaRead::ReplayComplete { through: 1 },
            "a replayed event is dropped rather than handed to the caller"
        );
        assert!(
            walker.pending_jobs().is_empty(),
            "a dropped event must not register a key"
        );
        assert_eq!(
            repo.progress()
                .await
                .expect("progress is readable")
                .upstream_revision,
            1,
            "replay catch-up still advances the durable revision"
        );
    }

    /// One durable commit: the settlement hands out the effect surface and the
    /// commit boundary together, so a consumer's effect is never observable before
    /// the memory row that records it as consumed — and a settlement dropped
    /// before `settle` (the normal path when a consumer's task is cancelled)
    /// leaves neither behind. Atomicity of the *storage* is the backend's own
    /// property, pinned by the delta-walker contract suite; what this pins is that
    /// the store's settlement path routes both writes through one transaction.
    #[tokio::test]
    async fn the_effect_and_the_memory_row_land_in_one_commit_or_neither_does() {
        let repo = MemoryRepo::default();
        let branch = BranchId::from("branch");
        let delta = DocDelta {
            branch_id: branch.clone(),
            previous_heads: None,
            current_heads: Some(heads(1)),
            previous_causal_epoch: None,
            current_causal_epoch: None,
            epoch_only: false,
        };

        let mut settlement = begin_settlement(&repo, &delta)
            .await
            .expect("begin settlement");
        settlement.context_mut().put(b"projection", b"visible");
        assert_eq!(
            repo.effect(b"projection"),
            None,
            "the effect must not be observable before the commit that carries it"
        );
        assert_eq!(
            repo.memory(&branch),
            None,
            "and the memory row must not be durable before that same commit"
        );

        settlement.settle().await.expect("settle");

        assert_eq!(
            repo.effect(b"projection").as_deref(),
            Some(b"visible".as_slice()),
            "the effect is durable once the settlement commits"
        );
        let stored: DocDeltaBranchState =
            serde_json::from_slice(&repo.memory(&branch).expect("the memory row is durable"))
                .expect("the memory row decodes");
        assert_eq!(
            stored.heads,
            Some(heads(1)),
            "the same commit advanced the memory"
        );

        let other = BranchId::from("other-branch");
        let abandoned_delta = DocDelta {
            branch_id: other.clone(),
            previous_heads: None,
            current_heads: Some(heads(2)),
            previous_causal_epoch: None,
            current_causal_epoch: None,
            epoch_only: false,
        };
        let mut abandoned = begin_settlement(&repo, &abandoned_delta)
            .await
            .expect("begin settlement");
        abandoned.context_mut().put(b"abandoned", b"never");
        drop(abandoned);

        assert_eq!(
            repo.effect(b"abandoned"),
            None,
            "an abandoned settlement leaves no effect"
        );
        assert_eq!(repo.memory(&other), None, "and no memory advance");
    }

    /// A revision that carries no events at all comes back entry-less, and the
    /// walker settles it through `finish`, advancing the durable revision itself:
    /// no key is tracked, so no job and no waiter exists. A settle path that
    /// demanded a job per revision would stall here, and this is the shape
    /// quiescence produces — a source revision that exists only because something
    /// else advanced it.
    #[tokio::test]
    async fn a_no_op_revision_settles_via_finish_without_a_job() {
        let repo = MemoryRepo::default();

        let source = ScriptedFrontierSource::with(vec![
            RevisionRead::Entries {
                revision: 1,
                entries: Vec::new(),
            },
            RevisionRead::ReplayComplete { through: 1 },
        ]);
        let store: DocDeltaRevisionStore<ScriptedFrontierSource, MemoryRepo> =
            DocDeltaRevisionStore::new(source);
        let reader = store
            .open(
                doc_delta_selector(repo.clone(), DocDeltaBranchFilter::All),
                0,
            )
            .await
            .expect("open reader");
        let mut walker: ConcurrentDeltaWalker<
            '_,
            DocDeltaRevisionStore<ScriptedFrontierSource, MemoryRepo>,
            MemoryRepo,
            daybook_types::doc::BranchId,
        > = ConcurrentDeltaWalker::open(reader, repo.clone(), |delta: &DocDelta| {
            delta.branch_id.clone()
        })
        .await
        .expect("open walker");

        assert_eq!(
            walker
                .next(read_limit(8))
                .await
                .expect("settle the no-op revision"),
            ConcurrentDeltaRead::ReplayComplete { through: 1 },
            "nothing is handed to the caller, and the reader still crosses its replay boundary"
        );
        assert!(
            walker.pending_jobs().is_empty(),
            "an entry-less revision must not register a key"
        );
        assert_eq!(walker.durable_revision(), 1);
        assert_eq!(
            repo.progress()
                .await
                .expect("progress is readable")
                .upstream_revision,
            1,
            "the revision is durable without any keyed job settling it"
        );
    }

    /// Filtering happens on the event alone, so a filtered branch's event must
    /// never become a walker key. If it did, it would hold a watermark waiter for
    /// a branch nobody consumes, and the durable revision — and every fence
    /// waiting on it — would stall behind a key no completion can settle.
    #[tokio::test]
    async fn events_outside_the_branch_name_filter_never_become_keys() {
        let admitted = ObjKey::new(b"branch-admitted");
        let filtered_out = ObjKey::new(b"branch-filtered-out");
        let repo = MemoryRepo::default();

        let source = ScriptedFrontierSource::with(vec![
            RevisionRead::Entries {
                revision: 1,
                entries: vec![
                    changed(&admitted, &[1], None, 1),
                    changed(&filtered_out, &[2], None, 1),
                ],
            },
            RevisionRead::ReplayComplete { through: 1 },
        ]);
        let store: DocDeltaRevisionStore<ScriptedFrontierSource, MemoryRepo> =
            DocDeltaRevisionStore::new(source);
        let filter = DocDeltaBranchFilter::Named {
            names: BTreeSet::from([admitted.to_string()]),
        };
        let reader = store
            .open(doc_delta_selector(repo.clone(), filter), 0)
            .await
            .expect("open reader");
        let mut walker: ConcurrentDeltaWalker<
            '_,
            DocDeltaRevisionStore<ScriptedFrontierSource, MemoryRepo>,
            MemoryRepo,
            daybook_types::doc::BranchId,
        > = ConcurrentDeltaWalker::open(reader, repo.clone(), |delta: &DocDelta| {
            delta.branch_id.clone()
        })
        .await
        .expect("open walker");

        let entries = match walker.next(read_limit(8)).await.expect("read a revision") {
            ConcurrentDeltaRead::Entries { entries, .. } => entries,
            other => panic!("expected entries, got {other:?}"),
        };
        assert_eq!(
            entries.len(),
            1,
            "only the admitted branch's event became a delta"
        );
        assert_eq!(entries[0].key, branch_of(&admitted));
        assert_eq!(
            walker.pending_jobs(),
            vec![(branch_of(&admitted), 1)],
            "the filtered branch must hold no watermark waiter"
        );

        walker
            .ack(branch_of(&admitted), 1)
            .await
            .expect("ack the admitted branch");
        assert!(walker.pending_jobs().is_empty());
        assert_eq!(
            repo.progress()
                .await
                .expect("progress is readable")
                .upstream_revision,
            1,
            "the admitted key alone settles the revision"
        );
    }
}
