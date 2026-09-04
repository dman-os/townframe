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
//! by a keyed worker (see facet_set).
//! The consumer contract (the only obligation, enforced nowhere mechanically):
//! by a keyed worker (see facet_set).
//! 1. the consumer's effect must be idempotent and latest-state;
//! 2. the effect and the memory advance commit in ONE transaction
//!    (`commit_transition` codifies this);
//! 3. the walker cursor is acked only after that commit.
//! by a keyed worker (see facet_set).
//! With that contract, `memory(branch)` may lead the durable cursor (never
//! lag the durable effect). A crash between the memory commit and the ack
//! replays the entry, the reader diffs the new memory against the same heads,
//! gets an unchanged transition and DROPS the entry — the walker settles the
//! entry-less revision via `finish` and replay catch-up costs zero jobs.
//! by a keyed worker (see facet_set).
//! Filtering: everything except branch shape is a part-store subscription on
//! the source (keyhive group → group-part sub; specific docs/branches →
//! object subs). Branch-name filtering works without identity resolution
//! because the Branch facet pins the branch name to the physical doc id.

use crate::interlude::*;
use big_repo::{AutomergeFrontierEvent, AutomergeFrontierSelector};
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
}

/// One head transition for one physical branch, as seen by one site.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocDelta {
    pub branch_id: BranchId,
    pub previous_heads: Option<ChangeHashSet>,
    pub current_heads: Option<ChangeHashSet>,
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
    M: DeltaWalkerStateRepo + 'static,
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
        })
    }
}

pub struct DocDeltaReader<'a, S, M>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent> + 'a,
    M: DeltaWalkerStateRepo + 'a,
{
    source: S::Reader<'a>,
    memory: M,
    filter: DocDeltaBranchFilter,
}

#[async_trait::async_trait]
impl<S, M> RevisionedStoreReader<u64, DocDelta, eyre::Report> for DocDeltaReader<'_, S, M>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent, Error = eyre::Report>,
    M: DeltaWalkerStateRepo,
{
    async fn next(
        &mut self,
        limits: big_sync_core::revisioned_store::RevisionReadLimits,
    ) -> Result<RevisionRead<u64, DocDelta>, eyre::Report> {
        {
            let (revision, entries) = match self.source.next(limits).await? {
                RevisionRead::ReplayComplete { through } => {
                    return Ok(RevisionRead::ReplayComplete { through });
                }
                RevisionRead::Entries { revision, entries } => (revision, entries),
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
                let heads = event_heads(&event);
                // Tombstone on Removed: only tracked branches transition
                // (an untracked removal carries no transition), and an
                // already-tombstoned branch has nothing to transition.
                let previous_heads = match &heads {
                    Some(_) => stored.get(&branch_id).map(|state| state.heads.clone()),
                    None => match stored.get(&branch_id) {
                        Some(state) if state.heads.is_some() => Some(state.heads.clone()),
                        _ => continue,
                    },
                };
                // No-op transition: the site's memory already holds these
                // heads, so the effect is durably applied (it committed with
                // the memory write). Dropping the entry lets the walker settle
                // the revision through `finish` with no job at all — this is
                // the replay catch-up path after a crash between the memory
                // commit and the ack.
                if previous_heads
                    .as_ref()
                    .is_some_and(|prev| prev.as_ref() == heads.as_ref())
                {
                    continue 'entry;
                }

                out.push(DocDelta {
                    branch_id,
                    previous_heads: previous_heads.flatten(),
                    current_heads: heads,
                });
            }

            // An all-no-op revision yields zero entries; the concurrent walker
            // settles entry-less revisions directly and re-reads.
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
    M: DeltaWalkerStateRepo + 'a,
{
    tx: M::Transaction<'a>,
    delta: DocDelta,
}

impl<'a, M> DocDeltaSettlement<'a, M>
where
    M: DeltaWalkerStateRepo + 'a,
{
    pub fn context_mut(&mut self) -> &mut M::Context<'a> {
        self.tx.context_mut()
    }

    pub async fn settle(self) -> Res<()> {
        let mut tx = self.tx;
        let state = DocDeltaBranchState {
            heads: self.delta.current_heads.clone(),
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

    pub async fn rollback(self) -> Res<()> {
        self.tx.rollback().await.map_err(memory_error)
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
    M: DeltaWalkerStateRepo + 'a,
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
    use big_sync_core::delta_walker_state::{DeltaWalkerProgress, DeltaWalkerStateResult};
    use std::sync::Mutex;

    // ---- memory double (mirrors SqliteDeltaWalkerStateRepo get/put) -------

    #[derive(Clone, Default)]
    struct MemoryRepo {
        rows: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    struct MemoryTx<'a> {
        rows: &'a Mutex<HashMap<Vec<u8>, Vec<u8>>>,
        ctx: (),
        staged: HashMap<Vec<u8>, Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl<'a> DeltaWalkerStateTransaction for MemoryTx<'a> {
        type Context = ();

        fn context_mut(&mut self) -> &mut Self::Context {
            &mut self.ctx
        }

        async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: 0,
            })
        }

        async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            Ok(self
                .staged
                .get(key)
                .cloned()
                .or_else(|| self.rows.lock().unwrap().get(key).cloned()))
        }

        async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()> {
            self.staged.insert(key, value);
            Ok(())
        }

        async fn delete(&mut self, _key: &[u8]) -> DeltaWalkerStateResult<()> {
            Ok(())
        }

        async fn advance_from(&mut self, _expected: u64, _next: u64) -> DeltaWalkerStateResult<()> {
            Ok(())
        }

        async fn commit(self) -> DeltaWalkerStateResult<()> {
            self.rows.lock().unwrap().extend(self.staged);
            Ok(())
        }

        async fn rollback(self) -> DeltaWalkerStateResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DeltaWalkerStateRepo for MemoryRepo {
        type Context<'a>
            = ()
        where
            Self: 'a;
        type Transaction<'a>
            = MemoryTx<'a>
        where
            Self: 'a;

        async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
            Ok(DeltaWalkerProgress {
                upstream_revision: 0,
            })
        }

        async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
            Ok(self.rows.lock().unwrap().get(key).cloned())
        }

        async fn get_many(
            &self,
            keys: &[Vec<u8>],
        ) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
            let rows = self.rows.lock().unwrap();
            Ok(keys
                .iter()
                .filter_map(|k| rows.get(k).map(|v| (k.clone(), v.clone())))
                .collect())
        }

        async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            Ok(MemoryTx {
                rows: &self.rows,
                ctx: (),
                staged: HashMap::new(),
            })
        }

        async fn begin_with_context<'a>(
            &'a self,
            _context: Self::Context<'a>,
        ) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
            self.begin().await
        }
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
