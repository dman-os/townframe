#![allow(dead_code)]
use crate::drawer::{BranchIdentityResolution, DrawerRepo};
use crate::index::doc_delta_store::{
    DocDelta, DocDeltaBranchFilter, DocDeltaRevisionStore, DocDeltaSelector, begin_settlement,
};
use crate::index::facet_delta::{FacetDelta, FacetRouteKey, FacetSnapshot};
use crate::interlude::*;
use big_repo::{
    AutomergeFrontierRevisionStore, AutomergeFrontierSelector, AutomergeFrontierTarget,
};
use big_sync::keyed_frontier::{SqliteFrontierCodec, SqliteKeyedFrontier};
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo as _;
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierRead, KeyedFrontier, KeyedFrontierReader,
};
use big_sync_core::revisioned_store::{RevisionRead, RevisionedStore, RevisionedStoreReader};
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use daybook_types::doc::{BranchId, ChangeHashSet, DocId, FacetKey, FacetTag, WellKnownFacetTag};
use sqlx::{QueryBuilder, Row, Sqlite, Transaction};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use tokio_util::sync::CancellationToken;

const FACET_SET_LOCAL_STATE_ID: &str = "@daybook/core/doc-facet-set-index";

#[derive(Debug, Clone)]
pub struct DocFacetTagMembership {
    pub doc_id: DocId,
    pub branch_id: BranchId,
    pub facet_tag: String,
    pub origin_heads: ChangeHashSet,
}

/// The frontier payload for one facet route. Unlike a low-level frontier
/// deletion, `Removed` retains the live branch heads and local provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocFacetMembership {
    Present {
        document_id: DocId,
        branch_id: BranchId,
        facet_key: FacetKey,
        branch_heads: ChangeHashSet,
        facet_heads: ChangeHashSet,
        actor_id: automerge::ActorId,
    },
    Removed {
        document_id: DocId,
        branch_id: BranchId,
        facet_key: FacetKey,
        branch_heads: Option<ChangeHashSet>,
        removed_local: bool,
    },
}

#[derive(Clone)]
struct FacetSetCodec;

impl SqliteFrontierCodec for FacetSetCodec {
    type Key = FacetRouteKey;
    type Value = DocFacetMembership;

    fn encode_key(&self, key: &Self::Key) -> Vec<u8> {
        serde_json::to_vec(key).expect(ERROR_JSON)
    }

    fn decode_key(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Key, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn encode_value(&self, value: &Self::Value) -> Vec<u8> {
        serde_json::to_vec(value).expect(ERROR_JSON)
    }

    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn namespace(&self, key: &Self::Key) -> Option<Vec<u8>> {
        Some(match &key.facet_key.tag {
            FacetTag::WellKnown(tag) => tag.as_str().as_bytes().to_vec(),
            FacetTag::Any(tag) => tag.as_bytes().to_vec(),
        })
    }
}

pub(crate) enum FacetSetSelector {
    All,
    Tag(WellKnownFacetTag),
    Tags(Vec<WellKnownFacetTag>),
    FacetTags(Vec<String>),
    Documents(BTreeSet<DocId>),
    Routes(BTreeSet<FacetRouteKey>),
}

pub struct FacetSetReader<'a> {
    inner: Box<dyn KeyedFrontierReader<FacetRouteKey, DocFacetMembership> + 'a>,
    pending: VecDeque<RevisionRead<u64, FacetDelta>>,
    documents: Option<BTreeSet<DocId>>,
}

#[async_trait]
impl RevisionedStoreReader<u64, FacetDelta, eyre::Report> for FacetSetReader<'_> {
    async fn next(
        &mut self,
        limits: big_sync_core::revisioned_store::RevisionReadLimits,
    ) -> Result<RevisionRead<u64, FacetDelta>, eyre::Report> {
        if let Some(read) = self.pending.pop_front() {
            return Ok(read);
        }
        match self
            .inner
            .next(big_sync_core::keyed_frontier::FrontierReadLimits {
                max_entries: limits.max_entries,
            })
            .await
            .map_err(|error| ferr!("{error}"))?
        {
            FrontierRead::ReplayComplete { through } => {
                Ok(RevisionRead::ReplayComplete { through })
            }
            FrontierRead::Entries { entries, through } => {
                let mut grouped = BTreeMap::<u64, Vec<FacetDelta>>::new();
                for FrontierEntry {
                    revision,
                    key,
                    value,
                } in entries
                {
                    if let Some(documents) = &self.documents
                        && !documents.contains(&key.document_id)
                    {
                        continue;
                    }
                    let (current, current_branch_heads, removed_local) = match value {
                        Some(DocFacetMembership::Present {
                            branch_heads,
                            facet_heads,
                            actor_id,
                            ..
                        }) => (
                            Some(FacetSnapshot {
                                branch_heads: branch_heads.clone(),
                                facet_heads,
                                actor_id,
                            }),
                            Some(branch_heads),
                            false,
                        ),
                        Some(DocFacetMembership::Removed {
                            branch_heads,
                            removed_local,
                            ..
                        }) => (None, branch_heads, removed_local),
                        None => unreachable!("facet-set frontier deletion lost route provenance"),
                    };
                    grouped.entry(revision).or_default().push(FacetDelta {
                        key,
                        current,
                        current_branch_heads,
                        removed_local,
                    });
                }
                if grouped.is_empty() {
                    return Ok(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                self.pending.extend(
                    grouped
                        .into_iter()
                        .map(|(revision, entries)| RevisionRead::Entries { revision, entries }),
                );
                Ok(self.pending.pop_front().expect("grouped entries non-empty"))
            }
        }
    }
}

/// Shared dmeta-derived facet-set projection and inert revision source.
pub(crate) struct FacetSetRevisionStore {
    frontier: SqliteKeyedFrontier<FacetSetCodec>,
    input_state: big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FacetSetApplyOutcome {
    Applied { revision: u64 },
    Deferred,
}

struct PreparedFacetSetProjection {
    desired: BTreeMap<FacetRouteKey, FacetSnapshot>,
    affected: BTreeSet<(DocId, String)>,
    branch_heads: BTreeMap<(DocId, String), Option<ChangeHashSet>>,
    removed_local: BTreeSet<(DocId, String, FacetKey)>,
}

impl FacetSetRevisionStore {
    async fn boot(sql: SqlCtx) -> Res<Self> {
        let frontier = SqliteKeyedFrontier::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            "daybook-facet-set",
            FacetSetCodec,
            Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .map_err(|error| ferr!("initializing facet-set frontier: {error}"))?;
        let input_state = big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            "daybook-facet-set",
            "doc-delta-input",
        )
        .await
        .map_err(|error| ferr!("initializing facet-set input state: {error}"))?;
        Ok(Self {
            frontier,
            input_state,
        })
    }

    pub(crate) fn input_state(&self) -> big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo {
        self.input_state.clone()
    }

    /// Prepare one DocDelta revision from complete dmeta membership. All
    /// hydration is deliberately complete before the caller opens its
    /// settlement transaction.
    async fn prepare_projection(
        &self,
        drawer: &DrawerRepo,
        entries: Vec<DocDelta>,
    ) -> Res<Option<PreparedFacetSetProjection>> {
        let mut desired = BTreeMap::<FacetRouteKey, FacetSnapshot>::new();
        let mut affected = BTreeSet::<(DocId, String)>::new();
        let mut branch_heads = BTreeMap::<(DocId, String), Option<ChangeHashSet>>::new();
        let mut removed_local = BTreeSet::new();
        for delta in entries {
            let Some(heads) = delta.current_heads.as_ref() else {
                eyre::bail!("projection cannot run on un-hydrated deltas");
            };
            // TEMPORARY system-doc filter: the walker source reads
            // GLOBAL_PART_ID, which mirrors every known Automerge object -
            // including system docs (app_doc, drawer_doc, config doc, plug
            // manifest docs) that have not been migrated to the facet-based
            // format yet and carry no Branch facet. Only content docs may be
            // projected here, so resolve the identity at the delta's heads and
            // skip everything else. A genuinely corrupted content doc whose
            // Branch facet is missing at projection time is silently skipped
            // instead of erroring; accepted tradeoff for the temporary hack.
            // The real fix is routing the doc delta source through a
            // content-doc group part instead of GLOBAL_PART_ID, after which
            // this resolution disappears.
            let identity = match drawer
                .resolve_system_branch_identity_at_heads(&delta.branch_id, heads)
                .await?
            {
                BranchIdentityResolution::Ignored | BranchIdentityResolution::ImportedHistory => {
                    continue;
                }
                BranchIdentityResolution::Deferred => {
                    eyre::bail!(
                        "facet-set source event heads are not materialized for branch {}",
                        delta.branch_id.0
                    )
                }
                BranchIdentityResolution::Found(identity) => identity,
            };
            let branch_key = (identity.document_id.clone(), delta.branch_id.0.clone());
            if !affected.insert(branch_key.clone()) {
                return Err(ferr!("duplicate DocDelta branch in one source revision"));
            }
            branch_heads.insert(branch_key, delta.current_heads.clone());
            let Some(state) = drawer
                .hydrate_dmeta_state_at_heads(&delta.branch_id, heads.clone())
                .await?
            else {
                eyre::bail!(
                    "facet-set source event heads have no materialized dmeta for branch {}",
                    delta.branch_id.0
                );
            };
            if state.document_id != identity.document_id {
                return Err(ferr!("dmeta state identity does not match Branch facet"));
            }
            if delta.branch_id.0 == state.document_id {
                for facet_key in drawer
                    .facet_keys_touched_by_local_actor(
                        &state.document_id,
                        daybook_types::doc::BranchPath::new("main"),
                        &state.branch_heads,
                        &state.all_facet_keys,
                    )
                    .await?
                {
                    removed_local.insert((
                        state.document_id.clone(),
                        delta.branch_id.0.clone(),
                        facet_key,
                    ));
                }
            }
            for (facet_key, (facet_heads, actor_id)) in state.facets {
                desired.insert(
                    FacetRouteKey {
                        document_id: state.document_id.clone(),
                        branch_id: state.branch_id.clone(),
                        facet_key,
                    },
                    FacetSnapshot {
                        branch_heads: state.branch_heads.clone(),
                        facet_heads,
                        actor_id,
                    },
                );
            }
        }

        Ok(Some(PreparedFacetSetProjection {
            desired,
            affected,
            branch_heads,
            removed_local,
        }))
    }

    /// Apply a prepared projection inside the caller-owned walker state
    /// transaction. This method never commits; the frontier revision, typed
    /// rows, sparse DocDelta state, and upstream cursor therefore settle as
    /// one SQLite unit.
    async fn apply_projection_in_context(
        &self,
        prepared: &PreparedFacetSetProjection,
        tx: &mut Transaction<'_, Sqlite>,
    ) -> Res<FacetSetApplyOutcome> {
        let old_keys = load_route_keys(tx, &prepared.affected).await?;
        let mut mutations = Vec::new();
        for key in old_keys
            .iter()
            .filter(|key| !prepared.desired.contains_key(key))
        {
            let current_branch_heads = prepared
                .branch_heads
                .get(&(key.document_id.clone(), key.branch_id.0.clone()))
                .expect("affected branch heads present");
            if let Some(current_branch_heads) = current_branch_heads {
                mutations.push((
                    key.clone(),
                    Some(DocFacetMembership::Removed {
                        document_id: key.document_id.clone(),
                        branch_id: key.branch_id.clone(),
                        facet_key: key.facet_key.clone(),
                        branch_heads: Some(current_branch_heads.clone()),
                        removed_local: prepared.removed_local.contains(&(
                            key.document_id.clone(),
                            key.branch_id.0.clone(),
                            key.facet_key.clone(),
                        )),
                    }),
                ));
            } else {
                mutations.push((
                    key.clone(),
                    Some(DocFacetMembership::Removed {
                        document_id: key.document_id.clone(),
                        branch_id: key.branch_id.clone(),
                        facet_key: key.facet_key.clone(),
                        branch_heads: None,
                        removed_local: false,
                    }),
                ));
            }
        }
        for (key, snapshot) in &prepared.desired {
            mutations.push((
                key.clone(),
                Some(DocFacetMembership::Present {
                    document_id: key.document_id.clone(),
                    branch_id: key.branch_id.clone(),
                    facet_key: key.facet_key.clone(),
                    branch_heads: snapshot.branch_heads.clone(),
                    facet_heads: snapshot.facet_heads.clone(),
                    actor_id: snapshot.actor_id.clone(),
                }),
            ));
        }

        delete_routes(tx, &prepared.affected).await?;
        insert_routes(tx, &prepared.desired).await?;
        let revision = self
            .frontier
            .apply_in_context(tx, mutations)
            .await
            .map_err(|error| ferr!("applying facet-set frontier: {error}"))?;

        Ok(FacetSetApplyOutcome::Applied { revision })
    }

    /// Wake downstream walkers/watches after a committed frontier revision.
    /// Notification is a wakeup only: durable state and cursors determine
    /// what must be read.
    pub(crate) fn notify_changed(&self) {
        self.frontier.notify_changed();
    }
}

async fn load_route_keys(
    tx: &mut Transaction<'_, Sqlite>,
    affected: &BTreeSet<(DocId, String)>,
) -> Res<BTreeSet<FacetRouteKey>> {
    if affected.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT document_id, branch_id, facet_tag, facet_id FROM facet_set_doc_facets WHERE (document_id, branch_id) IN (",
    );
    let mut first = true;
    for (document_id, branch_id) in affected {
        if !first {
            query.push(", ");
        }
        first = false;
        query
            .push("(")
            .push_bind(document_id)
            .push(", ")
            .push_bind(branch_id)
            .push(")");
    }
    query.push(")");
    let mut keys = BTreeSet::new();
    for row in query.build().fetch_all(&mut **tx).await? {
        keys.insert(FacetRouteKey {
            document_id: row.try_get("document_id")?,
            branch_id: BranchId(row.try_get("branch_id")?),
            facet_key: FacetKey {
                tag: row.try_get::<String, _>("facet_tag")?.into(),
                id: row.try_get("facet_id")?,
            },
        });
    }
    Ok(keys)
}

async fn delete_routes(
    tx: &mut Transaction<'_, Sqlite>,
    affected: &BTreeSet<(DocId, String)>,
) -> Res<()> {
    if affected.is_empty() {
        return Ok(());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        "DELETE FROM facet_set_doc_facets WHERE (document_id, branch_id) IN (",
    );
    let mut first = true;
    for (document_id, branch_id) in affected {
        if !first {
            query.push(", ");
        }
        first = false;
        query
            .push("(")
            .push_bind(document_id)
            .push(", ")
            .push_bind(branch_id)
            .push(")");
    }
    query.push(")");
    query.build().execute(&mut **tx).await?;
    Ok(())
}

async fn insert_routes(
    tx: &mut Transaction<'_, Sqlite>,
    desired: &BTreeMap<FacetRouteKey, FacetSnapshot>,
) -> Res<()> {
    if desired.is_empty() {
        return Ok(());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        "INSERT INTO facet_set_doc_facets(document_id, branch_id, facet_tag, facet_id, facet_heads_json, actor_id_json, branch_heads_json) VALUES ",
    );
    let mut first = true;
    for (key, snapshot) in desired {
        if !first {
            query.push(", ");
        }
        first = false;
        query
            .push("(")
            .push_bind(&key.document_id)
            .push(", ")
            .push_bind(&key.branch_id.0)
            .push(", ")
            .push_bind(key.facet_key.tag.to_string())
            .push(", ")
            .push_bind(&key.facet_key.id)
            .push(", ")
            .push_bind(serde_json::to_string(&snapshot.facet_heads)?)
            .push(", ")
            .push_bind(serde_json::to_string(&snapshot.actor_id)?)
            .push(", ")
            .push_bind(serde_json::to_string(&snapshot.branch_heads)?)
            .push(")");
    }
    query.build().execute(&mut **tx).await?;
    Ok(())
}

#[async_trait]
impl RevisionedStore for FacetSetRevisionStore {
    type Revision = u64;
    type Entry = FacetDelta;
    type Selector = FacetSetSelector;
    type Error = eyre::Report;
    type Reader<'a>
        = FacetSetReader<'a>
    where
        Self: 'a;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.frontier
            .latest_revision()
            .await
            .map_err(|error| ferr!("reading facet-set frontier revision: {error}"))
    }

    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: u64,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        let (selector, documents) = match selector {
            FacetSetSelector::All => (
                big_sync::keyed_frontier::SqliteFrontierSelector::All { after },
                None,
            ),
            FacetSetSelector::Tag(tag) => (
                big_sync::keyed_frontier::SqliteFrontierSelector::Namespaces(
                    [(tag.as_str().as_bytes().to_vec(), after)]
                        .into_iter()
                        .collect(),
                ),
                None,
            ),
            FacetSetSelector::Tags(tags) => (
                big_sync::keyed_frontier::SqliteFrontierSelector::Namespaces(
                    tags.into_iter()
                        .map(|tag| (tag.as_str().as_bytes().to_vec(), after))
                        .collect(),
                ),
                None,
            ),
            FacetSetSelector::FacetTags(tags) => (
                big_sync::keyed_frontier::SqliteFrontierSelector::Namespaces(
                    tags.into_iter()
                        .map(|tag| (tag.into_bytes(), after))
                        .collect(),
                ),
                None,
            ),
            FacetSetSelector::Documents(documents) => (
                big_sync::keyed_frontier::SqliteFrontierSelector::All { after },
                Some(documents),
            ),
            FacetSetSelector::Routes(routes) => (
                big_sync::keyed_frontier::SqliteFrontierSelector::Keys(
                    routes.into_iter().map(|key| (key, after)).collect(),
                ),
                None,
            ),
        };
        let inner = self
            .frontier
            .open(selector)
            .await
            .map_err(|error| ferr!("{error}"))?;
        Ok(FacetSetReader {
            inner,
            pending: VecDeque::new(),
            documents,
        })
    }
}

pub struct DocFacetSetIndexRepo {
    sql: SqlCtx,
    pub(crate) revision_store: Arc<FacetSetRevisionStore>,
}

// Keyed execution budget for the doc delta machine; mirrors the frontier
// worker's concurrent budget.
const FACET_SET_TASK_BUDGET: usize = 64;

/// Scheduling key for one physical branch. Hash of the branch id: collisions
/// only over-serialize a key, never break correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct DocDeltaKey(u64);

fn doc_delta_key(branch_id: &BranchId) -> DocDeltaKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    branch_id.0.hash(&mut hasher);
    DocDeltaKey(hasher.finish())
}

/// The merged keyed command for one branch: the newest delta with the source
/// cursor it must cover.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FacetSetDeltaTask {
    key: DocDeltaKey,
    cursor: u64,
    delta: DocDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FacetSetTaskOutput {
    Applied,
}

async fn run_facet_set_delta_task(
    task: FacetSetDeltaTask,
    drawer: Arc<DrawerRepo>,
    store: Arc<FacetSetRevisionStore>,
    memory: big_sync::SqliteDeltaWalkerStateRepo,
) -> Res<FacetSetTaskOutput> {
    // All hydration completes before the settlement transaction opens; the
    // projection write and the memory advance then commit as one SQLite unit.
    let prepared = store
        .prepare_projection(&drawer, vec![task.delta.clone()])
        .await?
        .expect("facet-set projection preparation must produce a projection");
    let mut settlement = begin_settlement(&memory, &task.delta).await?;
    store
        .apply_projection_in_context(&prepared, settlement.context_mut())
        .await?;
    settlement.settle().await?;
    store.notify_changed();
    Ok(FacetSetTaskOutput::Applied)
}

impl DocFacetSetIndexRepo {
    /// The keyed doc delta machine: a `ConcurrentDeltaWalker` over the doc
    /// delta store, keyed by branch, with per-key projection tasks. The
    /// machine's mutable state (walker, scheduler, pending, and parked keys) lives
    /// as stack locals here; only the revision store is shared with the
    /// repo's public query surface.
    async fn machine_loop(
        &self,
        drawer: Arc<DrawerRepo>,
        part_store: big_repo::SharedPartStore,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let source =
            DocDeltaRevisionStore::new(AutomergeFrontierRevisionStore::new(part_store));
        // The walker cursor and the site memory share one state repo: the
        // progress table keys are disjoint from the per-branch memory keys.
        // Legacy rows without the cursor key are replayed.
        let state = self.revision_store.input_state();
        let selector = DocDeltaSelector {
            memory: state.clone(),
            source: AutomergeFrontierSelector {
                targets: vec![AutomergeFrontierTarget::Part {
                    part_id: big_repo::GLOBAL_PART_ID,
                }],
            },
            filter: DocDeltaBranchFilter::All,
        };
        let durable = state.progress().await?.upstream_revision;
        let reader = source.open(selector, durable).await?;
        let mut walker = ConcurrentDeltaWalker::open(
            reader,
            state.clone(),
            |delta: &DocDelta| doc_delta_key(&delta.branch_id),
        )
        .await?;
        let mut tasks = TokioKeyedScheduler::new(FACET_SET_TASK_BUDGET);
        // The newest unacked delta per key.
        let mut pending: HashMap<DocDeltaKey, FacetSetDeltaTask> = HashMap::new();
        loop {
            let available = FACET_SET_TASK_BUDGET.saturating_sub(tasks.active_count());
            let next_deadline = tasks.next_deadline();
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                completion = tasks.next_completion() => {
                    self.on_task_completion(
                        &mut walker,
                        &mut tasks,
                        &mut pending,
                        completion?,
                    )
                    .await?
                }
                read = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        walker.next(available).await
                    }
                } => match read? {
                    ConcurrentDeltaRead::ReplayComplete { .. } => {}
                    ConcurrentDeltaRead::Entries { entries, .. } => {
                        for delta in entries {
                            self.on_delta(
                                &drawer,
                                &state,
                                &mut tasks,
                                &mut pending,
                                delta,
                            )?;
                        }
                    }
                },
                _ = async {
                    if let Some(deadline) = next_deadline {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    tasks.tick(std::time::Instant::now())?;
                }
            }
        }
    }

    async fn on_task_completion(
        &self,
        walker: &mut ConcurrentDeltaWalker<
            '_,
            DocDeltaRevisionStore<AutomergeFrontierRevisionStore, big_sync::SqliteDeltaWalkerStateRepo>,
            big_sync::SqliteDeltaWalkerStateRepo,
            DocDeltaKey,
        >,
        tasks: &mut TokioKeyedScheduler<DocDeltaKey, FacetSetDeltaTask, FacetSetTaskOutput>,
        pending: &mut HashMap<DocDeltaKey, FacetSetDeltaTask>,
        completion: TokioTaskCompletion<FacetSetDeltaTask, FacetSetTaskOutput>,
    ) -> Res<()> {
        let task = completion.command;
        match completion.result {
            FacetSetTaskOutput::Applied => {
                // The command's effect is durable; only now may the walker
                // cursor advance past it.
                walker.ack(task.key, task.cursor).await?;
                if pending
                    .get(&task.key)
                    .is_some_and(|t| t.cursor == task.cursor)
                {
                    pending.remove(&task.key);
                }
            }
            Err(error) => panic!("facet-set task failed: {error:?}"),
        }
        Ok(())
    }

    fn on_delta(
        &self,
        drawer: &Arc<DrawerRepo>,
        state: &big_sync::SqliteDeltaWalkerStateRepo,
        tasks: &mut TokioKeyedScheduler<DocDeltaKey, FacetSetDeltaTask, FacetSetTaskOutput>,
        pending: &mut HashMap<DocDeltaKey, FacetSetDeltaTask>,
        delta: ConcurrentDelta<DocDeltaKey, DocDelta>,
    ) -> Res<()> {
        let task = FacetSetDeltaTask {
            key: delta.key,
            cursor: delta.cursor,
            delta: delta.entry,
        };
        match pending.get(&delta.key) {
            Some(existing) if existing.cursor >= task.cursor => return Ok(()),
            _ => {}
        }
        pending.insert(task.key, task.clone());
        self.start_task(drawer, state, tasks, task)
    }

    fn start_task(
        &self,
        drawer: &Arc<DrawerRepo>,
        state: &big_sync::SqliteDeltaWalkerStateRepo,
        tasks: &mut TokioKeyedScheduler<DocDeltaKey, FacetSetDeltaTask, FacetSetTaskOutput>,
        task: FacetSetDeltaTask,
    ) -> Res<()> {
        let future = run_facet_set_delta_task(
            task.clone(),
            Arc::clone(drawer),
            Arc::clone(&self.revision_store),
            state.clone(),
        );
        tasks.replace(task.key, task.clone(), future)?;
        Ok(())
    }

    pub(crate) fn revision_store(&self) -> Arc<FacetSetRevisionStore> {
        Arc::clone(&self.revision_store)
    }

    /// SQLite context for direct queries over the facet-set projection
    /// (test and tooling access).
    pub(crate) fn sql(&self) -> &SqlCtx {
        &self.sql
    }

    pub async fn boot(
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
        drawer: Arc<DrawerRepo>,
        part_store: big_repo::SharedPartStore,
        parent_cancel_token: CancellationToken,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(FACET_SET_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&sql).await?;

        let revision_store = Arc::new(FacetSetRevisionStore::boot(sql.clone()).await?);

        let repo = Arc::new(Self {
            sql,
            revision_store,
        });

        let cancel_token = parent_cancel_token.child_token();
        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let drawer = Arc::clone(&drawer);
            let part_store = part_store.clone();
            let cancel_token = cancel_token.clone();
            async move {
                repo.machine_loop(drawer, part_store, cancel_token)
                    .await
                    .expect("facet-set doc delta machine error")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn init_schema(sql: &SqlCtx) -> Res<()> {
        // The route table is the facet-set side of the FacetDelta current
        // state: membership is keyed by the complete document/branch/tag/key
        // identity, while heads and provenance remain typed JSON for exact
        // head consumers.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS facet_set_doc_facets (
                document_id TEXT NOT NULL
              , branch_id TEXT NOT NULL
              , facet_tag TEXT NOT NULL
              , facet_id TEXT NOT NULL
              , facet_heads_json TEXT NOT NULL
              , actor_id_json TEXT NOT NULL
              , branch_heads_json TEXT NOT NULL
              , PRIMARY KEY(document_id, branch_id, facet_tag, facet_id)
            ) STRICT
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_facet_set_doc_facets_route ON facet_set_doc_facets(facet_tag, facet_id, document_id, branch_id)",
        )
        .execute(&sql.write_pool)
        .await?;

        Ok(())
    }

    pub async fn list_tags_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        let tags: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT facet_tag
              FROM facet_set_doc_facets
             WHERE document_id = ?1
             ORDER BY facet_tag ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(tags)
    }

    pub async fn list_docs_for_tag(&self, facet_tag: &str) -> Res<Vec<DocFacetTagMembership>> {
        let rows = sqlx::query_as::<_, (String, String, String)>(
            r#"
            SELECT document_id, branch_id, branch_heads_json
              FROM facet_set_doc_facets
             WHERE facet_tag = ?1
             GROUP BY document_id, branch_id, branch_heads_json
             ORDER BY document_id ASC, branch_id ASC
            "#,
        )
        .bind(facet_tag)
        .fetch_all(&self.sql.read_pool)
        .await?;

        rows.into_iter()
            .map(|(doc_id, branch_id, branch_heads)| {
                let head_strings: Vec<String> = serde_json::from_str(&branch_heads)?;
                Ok(DocFacetTagMembership {
                    doc_id,
                    branch_id: BranchId(branch_id),
                    facet_tag: facet_tag.to_string(),
                    origin_heads: ChangeHashSet(am_utils_rs::parse_commit_heads(&head_strings)?),
                })
            })
            .collect()
    }

    pub async fn has_tag(&self, doc_id: &DocId, facet_tag: &str) -> Res<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT 1
              FROM facet_set_doc_facets
             WHERE document_id = ?1
               AND facet_tag = ?2
            LIMIT 1
            "#,
        )
        .bind(doc_id)
        .bind(facet_tag)
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(exists.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use big_sync_core::revisioned_store::RevisionReadLimits;
    use daybook_types::doc::{AddDocArgs, BranchPathBuf, FacetKey, FacetRaw, WellKnownFacet};
    use std::collections::VecDeque;

    struct ScriptedFrontierReader {
        reads: VecDeque<
            big_sync_core::keyed_frontier::FrontierRead<FacetRouteKey, DocFacetMembership>,
        >,
    }

    #[async_trait::async_trait]
    impl big_sync_core::keyed_frontier::KeyedFrontierReader<FacetRouteKey, DocFacetMembership>
        for ScriptedFrontierReader
    {
        async fn next(
            &mut self,
            _limits: big_sync_core::keyed_frontier::FrontierReadLimits,
        ) -> big_sync_core::keyed_frontier::KeyedFrontierResult<
            big_sync_core::keyed_frontier::FrontierRead<FacetRouteKey, DocFacetMembership>,
        > {
            self.reads.pop_front().ok_or_else(|| {
                big_sync_core::keyed_frontier::KeyedFrontierError::Backend(Box::new(
                    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "script exhausted"),
                ))
            })
        }
    }

    async fn wait_for_doc_tag(
        repo: &DocFacetSetIndexRepo,
        doc_id: &DocId,
        facet_tag: &str,
    ) -> Res<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            if repo.has_tag(doc_id, facet_tag).await? {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eyre::bail!("timeout waiting for condition");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_facet_set_index_tracks_tags() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_context.rt.doc_facet_set_index_repo);

        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        FacetKey::from(WellKnownFacetTag::Note),
                        FacetRaw::from(WellKnownFacet::Note("hello".to_string().into())),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::LabelGeneric),
                        FacetRaw::from(WellKnownFacet::LabelGeneric("x".to_string())),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        wait_for_doc_tag(&repo, &doc_id, WellKnownFacetTag::Note.as_str()).await?;

        let tags = repo.list_tags_for_doc(&doc_id).await?;
        assert!(tags.contains(&WellKnownFacetTag::Note.as_str().to_string()));
        assert!(tags.contains(&WellKnownFacetTag::LabelGeneric.as_str().to_string()));
        assert!(tags.contains(&WellKnownFacetTag::Dmeta.as_str().to_string()));

        let empty_doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: Default::default(),
                user_path: None,
            })
            .await?;
        wait_for_doc_tag(&repo, &empty_doc_id, WellKnownFacetTag::Dmeta.as_str()).await?;
        let dmeta_docs = repo
            .list_docs_for_tag(WellKnownFacetTag::Dmeta.as_str())
            .await?;
        assert!(dmeta_docs.iter().any(|membership| {
            membership.doc_id == empty_doc_id
                && membership.branch_id.0 == empty_doc_id
                && membership.facet_tag == WellKnownFacetTag::Dmeta.as_str()
        }));

        test_context.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn reader_preserves_typed_removal_provenance_and_namespace() -> Res<()> {
        let key = FacetRouteKey {
            document_id: DocId::from("doc"),
            branch_id: BranchId::from("branch"),
            facet_key: FacetKey::from(WellKnownFacetTag::BlobPin),
        };
        let branch_heads = ChangeHashSet(Vec::new().into());
        let removed = DocFacetMembership::Removed {
            document_id: key.document_id.clone(),
            branch_id: key.branch_id.clone(),
            facet_key: key.facet_key.clone(),
            branch_heads: Some(branch_heads.clone()),
            removed_local: true,
        };
        let mut reader = FacetSetReader {
            inner: Box::new(ScriptedFrontierReader {
                reads: VecDeque::from([
                    FrontierRead::Entries {
                        entries: vec![FrontierEntry {
                            revision: 7,
                            key: key.clone(),
                            value: Some(removed.clone()),
                        }],
                        through: 7,
                    },
                    FrontierRead::ReplayComplete { through: 7 },
                ]),
            }),
            pending: VecDeque::new(),
            documents: None,
        };

        let RevisionRead::Entries { revision, entries } =
            reader.next(RevisionReadLimits::default()).await?
        else {
            eyre::bail!("expected typed removal entry");
        };
        assert_eq!(revision, 7);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, key);
        assert_eq!(entries[0].current, None);
        assert_eq!(entries[0].current_branch_heads, Some(branch_heads));
        assert!(entries[0].removed_local);
        let encoded = FacetSetCodec.encode_value(&removed);
        assert_eq!(
            FacetSetCodec
                .decode_value(&encoded)
                .expect("valid tombstone"),
            removed
        );
        assert_eq!(
            FacetSetCodec.namespace(&entries[0].key),
            Some(WellKnownFacetTag::BlobPin.as_str().as_bytes().to_vec())
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 7 }
        );
        Ok(())
    }

    /// Regression test for the temporary projection-level system-doc filter:
    /// deltas whose physical doc carries no Branch facet (app_doc, drawer_doc,
    /// config doc, plug manifest docs - not yet migrated to facet format) must
    /// be skipped entirely, while a genuine content-doc delta still projects.
    #[tokio::test(flavor = "multi_thread")]
    async fn projection_skips_system_docs_without_branch_facet() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let drawer = Arc::clone(&test_context.drawer_repo);
        let store = Arc::clone(&test_context.rt.doc_facet_set_index_repo.revision_store);

        // A real content doc with a Note facet and its real current heads.
        let content_doc_id: DocId = drawer
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    FacetRaw::from(WellKnownFacet::Note("hello".to_string().into())),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // The drawer's own system doc (no Branch facet by construction).
        let system_doc_id: DocId = drawer.drawer_doc_id().to_string();

        let heads_of = |doc_id: DocId| {
            let drawer = Arc::clone(&drawer);
            async move {
                let physical_id = doc_id.parse::<big_repo::DocumentId>()?;
                let handle = drawer.big_repo.get_doc(&physical_id).await?;
                let handle = handle.into_ready(physical_id)?;
                Ok::<ChangeHashSet, eyre::Report>(
                    handle
                        .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                        .await,
                )
            }
        };

        let content_heads = heads_of(content_doc_id.clone()).await?;
        let system_heads = heads_of(system_doc_id.clone()).await?;

        let content_delta = DocDelta {
            branch_id: BranchId(content_doc_id.clone()),
            previous_heads: None,
            current_heads: Some(content_heads),
        };
        let system_delta = DocDelta {
            branch_id: BranchId(system_doc_id.clone()),
            previous_heads: None,
            current_heads: Some(system_heads),
        };

        let projection = store
            .prepare_projection(&drawer, vec![system_delta, content_delta])
            .await?
            .expect("projection must not defer for ready docs");

        let content_key = (content_doc_id.clone(), content_doc_id.clone());
        let system_key = (system_doc_id.clone(), system_doc_id.clone());
        assert!(
            projection.affected.contains(&content_key),
            "content doc must stay in the projection"
        );
        assert_eq!(projection.affected.len(), 1);
        assert!(
            !projection.affected.contains(&system_key),
            "system doc without Branch facet must be skipped"
        );
        assert!(projection.desired.contains_key(&FacetRouteKey {
            document_id: content_doc_id.clone(),
            branch_id: BranchId(content_doc_id.clone()),
            facet_key: FacetKey::from(WellKnownFacetTag::Note),
        }));

        test_context.stop().await?;
        Ok(())
    }
}
