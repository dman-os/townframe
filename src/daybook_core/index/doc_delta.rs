//! The embedder-driven document delta walker.
//!
//! The physical document revision store is only a filtered, inert view of the
//! frontier part store. This module owns the per-consumer state machine that
//! turns that source into logical document head transitions. In particular,
//! it does not own a cursor table, a keyed frontier, or an event loop.

use crate::drawer::DrawerRepo;
use crate::interlude::*;
use big_repo::AutomergeFrontierEvent;
use big_sync_core::delta_walker_state::{DeltaWalkerStateRepo, DeltaWalkerStateTransaction};
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use daybook_types::doc::{BranchId, ChangeHashSet, DocId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The logical identity validated from a system Branch facet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchIdentity {
    pub document_id: DocId,
    pub branch_id: BranchId,
}

/// A sparse state entry owned by one walker instance.
///
/// An entry exists only after the consumer explicitly tracks the physical
/// branch. A tombstone keeps the identity and stores `None`, so a later re-add
/// is still an ordinary transition rather than an untracked event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DocDeltaBranchState {
    pub document_id: DocId,
    pub heads: Option<ChangeHashSet>,
}

/// The wire value emitted by a walker for one tracked physical branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DocDelta {
    pub document_id: DocId,
    pub branch_id: BranchId,
    pub previous_heads: Option<ChangeHashSet>,
    pub current_heads: Option<ChangeHashSet>,
}

/// The only Drawer integration required by this machine.
///
/// Implementations must read and validate the system Branch facet at the
/// supplied exact heads. They must not hydrate dmeta or user facets. The
/// concrete Drawer adapter is supplied by the embedder so this walker never
/// reaches through DrawerRepo into BigRepo itself.
#[async_trait]
pub(crate) trait DocDeltaBranchIdentityResolver: Send + Sync {
    async fn resolve_branch_identity(
        &self,
        physical_branch_id: &BranchId,
        heads: &ChangeHashSet,
    ) -> Res<BranchIdentityResolution>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchIdentityResolution {
    Found(BranchIdentity),
    Deferred,
    Ignored,
    ImportedHistory,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackOutcome {
    Tracked,
    Deferred,
}

/// A source read prepared for acknowledgement.
///
/// Preparing a read is deliberately separate from settling it: consumers may
/// fetch ahead, perform their own work, and only then atomically persist the
/// state changes and source cursor.
#[derive(Debug)]
pub(crate) struct PreparedDocDeltaRevision {
    expected_revision: u64,
    source_revision: u64,
    deltas: Vec<DocDelta>,
    next_states: BTreeMap<BranchId, DocDeltaBranchState>,
    imported_history_only: bool,
}

struct DeferredDocDeltaRevision {
    revision: u64,
    entries: Vec<AutomergeFrontierEvent>,
}

impl PreparedDocDeltaRevision {
    pub(crate) fn deltas(&self) -> &[DocDelta] {
        &self.deltas
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum DocDeltaWalkerRead {
    Entries(PreparedDocDeltaRevision),
    Deferred { revision: u64 },
    ReplayComplete { through: u64 },
}

/// An inert physical source plus one consumer's durable state.
pub(crate) struct DocDeltaWalker<S, R, I> {
    source: std::marker::PhantomData<S>,
    state: R,
    identity_resolver: I,
    deferred: Option<DeferredDocDeltaRevision>,
}

impl<S, R, I> DocDeltaWalker<S, R, I> {
    pub(crate) fn new(state: R, identity_resolver: I) -> Self {
        Self {
            source: std::marker::PhantomData,
            state,
            identity_resolver,
            deferred: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn state(&self) -> &R {
        &self.state
    }
}

impl<S, R, I> DocDeltaWalker<S, R, I>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent>,
    R: DeltaWalkerStateRepo,
{
    /// Open the inert physical source. The selector carries the consumer's
    /// group/part scope; it is source selection, not a DocDelta route model.
    pub(crate) async fn open_source<'a>(
        source: &'a S,
        selector: S::Selector,
        after: u64,
        limits: RevisionReadLimits,
    ) -> Result<S::Reader<'a>, S::Error> {
        source.open(selector, after, limits).await
    }

    pub(crate) async fn progress(&self) -> Res<u64> {
        Ok(self
            .state
            .progress()
            .await
            .map_err(state_error)?
            .upstream_revision)
    }
}

#[allow(dead_code)]
impl<S, R, I> DocDeltaWalker<S, R, I>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent>,
    R: DeltaWalkerStateRepo,
    I: DocDeltaBranchIdentityResolver,
{
    /// Begin tracking a physical branch. Tracking is the sparse admission
    /// operation; source reads never create state for untracked branches.
    pub(crate) async fn track(
        &self,
        physical_branch_id: &BranchId,
        validation_heads: &ChangeHashSet,
    ) -> Res<TrackOutcome> {
        let key = state_key(physical_branch_id);
        if self.state.get(&key).await.map_err(state_error)?.is_some() {
            return Ok(TrackOutcome::Tracked);
        }

        let identity = self
            .identity_resolver
            .resolve_branch_identity(physical_branch_id, validation_heads)
            .await?;
        let identity = match identity {
            BranchIdentityResolution::Found(identity) => identity,
            BranchIdentityResolution::Deferred => return Ok(TrackOutcome::Deferred),
            BranchIdentityResolution::Ignored | BranchIdentityResolution::ImportedHistory => {
                return Ok(TrackOutcome::Tracked);
            }
        };
        if identity.branch_id != *physical_branch_id {
            return Err(ferr!(
                "resolved Branch identity does not match physical branch"
            ));
        }

        let mut tx = self.state.begin().await.map_err(state_error)?;
        if tx.get(&key).await.map_err(state_error)?.is_none() {
            let state = DocDeltaBranchState {
                document_id: identity.document_id,
                heads: None,
            };
            tx.put(key, serde_json::to_vec(&state)?)
                .await
                .map_err(state_error)?;
        }
        tx.commit().await.map_err(state_error)?;
        Ok(TrackOutcome::Tracked)
    }

    /// Prepare one physical source read without changing durable state.
    pub(crate) async fn prepare(
        &mut self,
        read: RevisionRead<u64, AutomergeFrontierEvent>,
    ) -> Res<DocDeltaWalkerRead> {
        if self.deferred.is_some() {
            return Err(ferr!(
                "DocDelta revision is deferred; retry it before fetching"
            ));
        }
        let (revision, entries) = match read {
            RevisionRead::Entries { revision, entries } => (revision, entries),
            RevisionRead::ReplayComplete { through } => {
                return Ok(DocDeltaWalkerRead::ReplayComplete { through });
            }
        };
        self.prepare_entries(revision, entries).await
    }

    /// Retry the exact source revision retained after a deferred identity
    /// lookup. Later source revisions cannot be fetched until this succeeds.
    pub(crate) async fn retry_deferred(&mut self) -> Res<DocDeltaWalkerRead> {
        let deferred = self
            .deferred
            .take()
            .ok_or_else(|| ferr!("no deferred DocDelta revision to retry"))?;
        self.prepare_entries(deferred.revision, deferred.entries)
            .await
    }

    async fn prepare_entries(
        &mut self,
        revision: u64,
        entries: Vec<AutomergeFrontierEvent>,
    ) -> Res<DocDeltaWalkerRead> {
        let original_entries = entries.clone();

        let expected_revision = self.progress().await?;
        let keys = entries
            .iter()
            .map(|event| state_key(&event_branch_id(event)))
            .collect::<Vec<_>>();
        let stored = self
            .state
            .get_many(&keys)
            .await
            .map_err(state_error)?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut prior = BTreeMap::new();
        for event in &entries {
            let branch_id = event_branch_id(event);
            let key = state_key(&branch_id);
            if let Some(bytes) = stored.get(&key) {
                let state: DocDeltaBranchState = serde_json::from_slice(bytes)?;
                prior.insert(branch_id, state);
            }
        }

        let mut admitted_entries = Vec::new();
        let mut imported_history_only = !entries.is_empty();
        for event in entries {
            let branch_id = event_branch_id(&event);
            let Some(heads) = event_heads(&event) else {
                imported_history_only = false;
                if prior.contains_key(&branch_id) {
                    admitted_entries.push(event);
                }
                continue;
            };
            let identity = self
                .identity_resolver
                .resolve_branch_identity(&branch_id, &heads)
                .await?;
            let identity = match identity {
                BranchIdentityResolution::Found(identity) => {
                    imported_history_only = false;
                    identity
                }
                BranchIdentityResolution::Deferred => {
                    self.deferred = Some(DeferredDocDeltaRevision {
                        revision,
                        entries: original_entries,
                    });
                    return Ok(DocDeltaWalkerRead::Deferred { revision });
                }
                BranchIdentityResolution::Ignored => {
                    imported_history_only = false;
                    continue;
                }
                BranchIdentityResolution::ImportedHistory => continue,
            };
            if identity.branch_id != branch_id {
                return Err(ferr!(
                    "resolved Branch identity does not match physical branch"
                ));
            }
            if let Some(prior_state) = prior.get(&branch_id) {
                if prior_state.document_id != identity.document_id {
                    return Err(ferr!(
                        "resolved document identity changed for tracked physical branch"
                    ));
                }
            } else {
                prior.insert(
                    branch_id.clone(),
                    DocDeltaBranchState {
                        document_id: identity.document_id,
                        heads: None,
                    },
                );
            }
            admitted_entries.push(event);
        }

        let mut final_heads = BTreeMap::<BranchId, Option<ChangeHashSet>>::new();
        for event in admitted_entries {
            let branch_id = event_branch_id(&event);
            final_heads.insert(branch_id, event_heads(&event));
        }

        let mut deltas = Vec::new();
        let mut next_states = BTreeMap::new();
        for (branch_id, current_heads) in final_heads {
            let prior_state = &prior[&branch_id];
            let next_state = DocDeltaBranchState {
                document_id: prior_state.document_id.clone(),
                heads: current_heads.clone(),
            };
            if prior_state.heads != current_heads {
                deltas.push(DocDelta {
                    document_id: prior_state.document_id.clone(),
                    branch_id: branch_id.clone(),
                    previous_heads: prior_state.heads.clone(),
                    current_heads,
                });
            }
            next_states.insert(branch_id, next_state);
        }

        Ok(DocDeltaWalkerRead::Entries(PreparedDocDeltaRevision {
            expected_revision,
            source_revision: revision,
            deltas,
            next_states,
            imported_history_only,
        }))
    }

    /// Begin a caller-owned settlement transaction for a prepared read.
    ///
    /// Projection writes made through `context_mut` and the walker's sparse
    /// state/cursor are committed together by `settle`.
    pub(crate) async fn begin_settlement<'a>(
        &'a self,
        prepared: PreparedDocDeltaRevision,
    ) -> Res<DocDeltaSettlement<'a, R>> {
        let mut tx = self.state.begin().await.map_err(state_error)?;
        let actual = tx.progress().await.map_err(state_error)?.upstream_revision;
        if actual != prepared.expected_revision {
            return Err(ferr!(
                "stale DocDelta walker progress: expected {}, found {actual}",
                prepared.expected_revision
            ));
        }
        Ok(DocDeltaSettlement {
            transaction: tx,
            prepared,
        })
    }

    /// Atomically apply a prepared read's sparse state and source progress.
    pub(crate) async fn settle(&self, prepared: PreparedDocDeltaRevision) -> Res<Vec<DocDelta>> {
        self.begin_settlement(prepared).await?.settle().await
    }
}

/// A DocDelta state transaction held while an embedder applies projection
/// writes. The transaction is intentionally exposed only through its context
/// so all writes use the same SQLite connection and commit boundary.
pub(crate) struct DocDeltaSettlement<'a, R>
where
    R: DeltaWalkerStateRepo + 'a,
{
    transaction: R::Transaction<'a>,
    prepared: PreparedDocDeltaRevision,
}

impl<'a, R> DocDeltaSettlement<'a, R>
where
    R: DeltaWalkerStateRepo + 'a,
{
    pub(crate) fn context_mut(&mut self) -> &mut R::Context<'a> {
        self.transaction.context_mut()
    }

    pub(crate) async fn settle(self) -> Res<Vec<DocDelta>> {
        let Self {
            mut transaction,
            prepared,
        } = self;
        for (branch_id, state) in &prepared.next_states {
            transaction
                .put(state_key(branch_id), serde_json::to_vec(state)?)
                .await
                .map_err(state_error)?;
        }
        transaction
            .advance_from(prepared.expected_revision, prepared.source_revision)
            .await
            .map_err(state_error)?;
        let output = prepared.deltas;
        transaction.commit().await.map_err(state_error)?;
        Ok(output)
    }

    pub(crate) async fn rollback(self) -> Res<()> {
        self.transaction.rollback().await.map_err(state_error)
    }
}

impl<S, R, I> DocDeltaWalker<S, R, I>
where
    S: RevisionedStore<Revision = u64, Entry = AutomergeFrontierEvent>,
    R: DeltaWalkerStateRepo,
    I: DocDeltaBranchIdentityResolver,
{
    /// Fetch and prepare one physical source read through the inert
    /// RevisionedStore. Benign empty batches stamped at or below the durable
    /// floor are skipped (see below); everything else is prepared for the
    /// consumer.
    pub(crate) async fn next(&mut self, reader: &mut S::Reader<'_>) -> Res<DocDeltaWalkerRead>
    where
        S::Error: std::fmt::Debug,
    {
        loop {
            let read = reader
                .next()
                .await
                .map_err(|error| ferr!("reading DocDelta source: {error:?}"))?;
            match &read {
                RevisionRead::Entries { revision, entries }
                    if entries.is_empty() && *revision <= self.progress().await? =>
                {
                    // The keyed-frontier part reader replays from a shared
                    // cursor at zero; when the source is quiescent at exactly
                    // this consumer's floor it stamps an empty progress batch
                    // at the committed revision (read_page through semantics).
                    // Settling it would itself fail to advance, so skip to the
                    // next read which surfaces ReplayComplete and then the
                    // live tail. A non-empty batch at or below the floor is a
                    // source regression and is NOT swallowed — it reaches the
                    // consumer and fails settlement as before.
                    continue;
                }
                _ => {
                    let prepared = self.prepare(read).await?;
                    if let DocDeltaWalkerRead::Entries(batch) = &prepared
                        && batch.imported_history_only
                        && batch.source_revision <= batch.expected_revision
                    {
                        continue;
                    }
                    return Ok(prepared);
                }
            }
        }
    }
}

fn state_key(branch_id: &BranchId) -> Vec<u8> {
    branch_id.0.as_bytes().to_vec()
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

fn state_error(error: impl std::fmt::Display) -> eyre::Report {
    ferr!("DocDelta walker state error: {error}")
}

#[async_trait]
impl DocDeltaBranchIdentityResolver for DrawerRepo {
    async fn resolve_branch_identity(
        &self,
        physical_branch_id: &BranchId,
        heads: &ChangeHashSet,
    ) -> Res<BranchIdentityResolution> {
        self.resolve_system_branch_identity_at_heads(physical_branch_id, heads)
            .await
    }
}

#[async_trait]
impl DocDeltaBranchIdentityResolver for Arc<DrawerRepo> {
    async fn resolve_branch_identity(
        &self,
        physical_branch_id: &BranchId,
        heads: &ChangeHashSet,
    ) -> Res<BranchIdentityResolution> {
        self.as_ref()
            .resolve_branch_identity(physical_branch_id, heads)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::delta_walker_state::{
        DeltaWalkerProgress, DeltaWalkerStateRepo, DeltaWalkerStateResult,
        DeltaWalkerStateTransaction,
    };
    use big_sync_core::revisioned_store::{RevisionedStore, RevisionedStoreReader};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct StubResolver;

    #[async_trait]
    impl DocDeltaBranchIdentityResolver for StubResolver {
        async fn resolve_branch_identity(
            &self,
            _physical_branch_id: &BranchId,
            _heads: &ChangeHashSet,
        ) -> Res<BranchIdentityResolution> {
            Ok(BranchIdentityResolution::Ignored)
        }
    }

    struct MemoryState {
        progress: u64,
        keys: HashMap<Vec<u8>, Vec<u8>>,
    }

    struct MemoryStateRepo {
        inner: Mutex<MemoryState>,
    }

    impl MemoryStateRepo {
        fn new(progress: u64) -> Self {
            Self {
                inner: Mutex::new(MemoryState {
                    progress,
                    keys: HashMap::new(),
                }),
            }
        }
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

        async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()> {
            let state = self.inner.lock().unwrap();
            if state.progress != expected {
                return Err(
                    big_sync_core::delta_walker_state::DeltaWalkerStateError::StaleProgress {
                        expected,
                    },
                );
            }
            if next <= state.progress {
                return Err(big_sync_core::delta_walker_state::DeltaWalkerStateError::NonAdvancingRevision {
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

    #[derive(Debug, thiserror::Error)]
    #[error("script exhausted")]
    struct ScriptError;

    struct ScriptedReader {
        reads: VecDeque<RevisionRead<u64, AutomergeFrontierEvent>>,
    }

    #[async_trait]
    impl RevisionedStoreReader<u64, AutomergeFrontierEvent, ScriptError> for ScriptedReader {
        async fn next(&mut self) -> Result<RevisionRead<u64, AutomergeFrontierEvent>, ScriptError> {
            self.reads.pop_front().ok_or(ScriptError)
        }
    }

    struct ScriptedStore {
        reads: VecDeque<RevisionRead<u64, AutomergeFrontierEvent>>,
    }

    #[async_trait]
    impl RevisionedStore for ScriptedStore {
        type Revision = u64;
        type Entry = AutomergeFrontierEvent;
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
            _selector: Self::Selector,
            _after: u64,
            _limits: RevisionReadLimits,
        ) -> Result<Self::Reader<'a>, Self::Error> {
            Ok(ScriptedReader {
                reads: self.reads.clone(),
            })
        }
    }

    fn removed_event() -> AutomergeFrontierEvent {
        use big_repo::DocumentId;
        AutomergeFrontierEvent::Removed {
            doc_id: DocumentId::new([7; 32]),
            route: PartId::new([9; 32]),
            revision: 1,
        }
    }

    fn default_limits() -> RevisionReadLimits {
        RevisionReadLimits::default()
    }

    /// The regression behind the facet-set sync failures: reopening the
    /// source at exactly the durable floor while the store is quiescent yields
    /// a legal empty batch stamped at the floor. The walker must skip that
    /// no-op (settling it cannot advance) and surface the ReplayComplete that
    /// follows, instead of failing `advance_from(current, next)`.
    #[test]
    fn empty_batch_at_durable_floor_is_skipped() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                reads: VecDeque::from([
                    RevisionRead::Entries {
                        revision: 171,
                        entries: vec![],
                    },
                    RevisionRead::ReplayComplete { through: 171 },
                ]),
            };
            let state = MemoryStateRepo::new(171); // durable == floor == head
            let mut walker = DocDeltaWalker::<ScriptedStore, MemoryStateRepo, StubResolver>::new(
                state,
                StubResolver,
            );
            let mut reader =
                DocDeltaWalker::<ScriptedStore, MemoryStateRepo, StubResolver>::open_source(
                    &store,
                    (),
                    171,
                    default_limits(),
                )
                .await
                .unwrap();
            match walker.next(&mut reader).await.unwrap() {
                DocDeltaWalkerRead::ReplayComplete { through } => assert_eq!(through, 171),
                other_val => panic!("expected ReplayComplete, got {other_val:?}"),
            }
            assert!(
                walker.next(&mut reader).await.is_err(),
                "script must be exhausted after ReplayComplete"
            );
        });
    }

    /// A non-empty batch at or below the floor is a source regression and must
    /// NOT be swallowed: it still reaches the consumer and fails settlement.
    #[test]
    fn non_empty_batch_at_floor_is_not_skipped() {
        futures::executor::block_on(async {
            let store = ScriptedStore {
                reads: VecDeque::from([RevisionRead::Entries {
                    revision: 171,
                    entries: vec![removed_event()],
                }]),
            };
            let state = MemoryStateRepo::new(171);
            let mut walker = DocDeltaWalker::<ScriptedStore, MemoryStateRepo, StubResolver>::new(
                state,
                StubResolver,
            );
            let mut reader =
                DocDeltaWalker::<ScriptedStore, MemoryStateRepo, StubResolver>::open_source(
                    &store,
                    (),
                    171,
                    default_limits(),
                )
                .await
                .unwrap();
            match walker.next(&mut reader).await.unwrap() {
                DocDeltaWalkerRead::Entries(prepared) => {
                    assert_eq!(prepared.source_revision, 171);
                    assert!(prepared.deltas().is_empty());
                }
                other_val => panic!("expected Entries, got {other_val:?}"),
            }
        });
    }
}
