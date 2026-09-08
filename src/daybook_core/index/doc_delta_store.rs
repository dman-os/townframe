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
//!    (`commit_transition` codifies this);
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
    // No consumer constructs this yet; the branch-name filter is a designed
    // capability (see the module docs above) kept for per-doc consumers.
    #[expect(dead_code)]
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
        AutomergeFrontierEvent::Added { doc_id, .. }
        | AutomergeFrontierEvent::Changed { doc_id, .. }
        | AutomergeFrontierEvent::Removed { doc_id, .. } => doc_id,
    };
    BranchId(doc_id.to_string())
}

fn event_heads(event: &AutomergeFrontierEvent) -> Option<ChangeHashSet> {
    match event {
        AutomergeFrontierEvent::Added { heads, .. }
        | AutomergeFrontierEvent::Changed { heads, .. } => Some(ChangeHashSet(Arc::clone(heads))),
        AutomergeFrontierEvent::Removed { .. } => None,
    }
}

fn event_causal_epoch(event: &AutomergeFrontierEvent) -> Option<[u8; 32]> {
    match event {
        AutomergeFrontierEvent::Added { causal_epoch, .. }
        | AutomergeFrontierEvent::Changed { causal_epoch, .. } => *causal_epoch,
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
//                    commit_transition(memory, delta, |ctx| apply_projection(ctx)).await
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

    // ---- tests (to be written) --------------------------------------------

    // 1. Added -> delta(None -> heads); Changed -> delta(prev -> heads) from
    //    memory; Removed on tracked branch -> tombstone delta; Removed on
    //    untracked branch and already-tombstoned branch -> dropped.
    // 2. equal-heads drop: memory already at the event's heads -> entry is
    //    dropped (replay catch-up costs zero jobs).
    // 3. commit_transition: effect + memory put land in one commit; memory
    //    leads the cursor afterwards (no progress write anywhere).
    // 4. integration: ConcurrentDeltaWalker over the store with a scripted
    //    source; no-op revision (all entries dropped) settles via finish and
    //    advances progress without any job.
    // 5. Named filter: events for branches outside the name set never become
    //    walker keys (no watermark waiters created for them).
}
