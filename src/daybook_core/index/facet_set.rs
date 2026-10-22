#![expect(dead_code)]

use crate::interlude::*;

/// Tables of the facet-set index, kept in its own local-state database
/// (`FACET_SET_LOCAL_STATE_ID`).
pub(crate) mod sqlite {
    pub(crate) mod queries {
        include!("facet_set/sqlite/gen.rs");
    }
}
use sqlite::queries;

use crate::blobs::BlobId;
use crate::blobs::encryption_worker::plaintext_blob_id;
use crate::drawer::BranchIdentityResolution;
use crate::drawer::DrawerRepo;
use crate::drawer::ExactFacetValueHydration;
use crate::index::doc_delta_store::DocDelta;
use crate::index::doc_delta_store::DocDeltaBranchFilter;
use crate::index::doc_delta_store::DocDeltaRevisionStore;
use crate::index::doc_delta_store::DocDeltaSelector;
use crate::index::doc_delta_store::begin_settlement;
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_delta::FacetRouteKey;
use crate::index::facet_delta::FacetSnapshot;
use big_repo::AutomergeFrontierRevisionStore;
use big_repo::AutomergeFrontierSelector;
use big_repo::AutomergeFrontierTarget;
use big_sync::keyed_frontier::SqliteFrontierCodec;
use big_sync::keyed_frontier::SqliteKeyedFrontier;
use big_sync_core::concurrent_delta_walker::ConcurrentDelta;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaRead;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaWalker;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo as _;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::keyed_frontier::FrontierRead;
use big_sync_core::keyed_frontier::KeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontierReader;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionedStore;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use big_sync_core::tokio_keyed_scheduler::TokioKeyedScheduler;
use big_sync_core::tokio_keyed_scheduler::TokioTaskCompletion;
use daybook_types::doc::BranchId;
use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use daybook_types::doc::FacetKey;
use daybook_types::doc::FacetTag;
use daybook_types::doc::WellKnownFacet;
use daybook_types::doc::WellKnownFacetTag;
use sqlite_rs::Sql;
use sqlite_rs::Tx;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
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

struct PreparedFacetSetProjection {
    desired: BTreeMap<FacetRouteKey, FacetSnapshot>,
    affected: BTreeSet<(DocId, String)>,
    branch_heads: BTreeMap<(DocId, String), Option<ChangeHashSet>>,
    removed_local: BTreeSet<(DocId, String, FacetKey)>,
    settled_current_heads: Option<ChangeHashSet>,
    /// The plaintext digests the projected `Blob` facets name IN THEIR VALUES,
    /// keyed by (document, physical branch). This is the presence plane's
    /// association key (ADR 003 §13): a production `Blob` facet's key id is
    /// `DEFAULT_FACET_ID` (`FacetKey::from(WellKnownFacetTag::Blob)`), so only
    /// the value names the blob the facet declares.
    blob_digests: BTreeMap<(DocId, String), BTreeSet<BlobId>>,
}

impl FacetSetRevisionStore {
    async fn boot(sql: Database) -> Res<Self> {
        let frontier = SqliteKeyedFrontier::new(
            sql.clone(),
            "daybook-facet-set",
            FacetSetCodec,
            Arc::new(tokio::sync::Notify::new()),
        )
        .await
        .map_err(|error| ferr!("initializing facet-set frontier: {error}"))?;
        let input_state = big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo::new(
            sql.clone(),
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
        let mut settled_current_heads = None;
        let mut blob_digests = BTreeMap::<(DocId, String), BTreeSet<BlobId>>::new();
        for delta in entries {
            let Some(event_heads) = delta.current_heads.as_ref() else {
                // A removed branch has no current heads. Its previous heads
                // still identify the logical branch so the old projection can
                // be removed without hydrating a deleted document.
                let Some(previous_heads) = delta.previous_heads.as_ref() else {
                    eyre::bail!("projection cannot identify removed branch without previous heads");
                };
                let identity = match drawer
                    .resolve_system_branch_identity_at_heads(&delta.branch_id, previous_heads)
                    .await?
                {
                    BranchIdentityResolution::Found(identity) => identity,
                    BranchIdentityResolution::Ignored
                    | BranchIdentityResolution::ImportedHistory => continue,
                    BranchIdentityResolution::Deferred => {
                        return Ok(None);
                    }
                };
                let branch_key = (identity.document_id.clone(), delta.branch_id.0.clone());
                if !affected.insert(branch_key.clone()) {
                    return Err(ferr!("duplicate DocDelta branch in one source revision"));
                }
                branch_heads.insert(branch_key, None);
                continue;
            };

            // The frontier event's heads are the source transition, not
            // necessarily the latest materialized version of the document.
            // Hydrate the latest version so one projection task accounts for
            // all changes already visible to the drawer, and settle sparse
            // memory to those heads atomically with the projection.
            // Do not project an older drawer snapshot when the source event's
            // heads have arrived before the branch document. The latest-head
            // lookup below can still return the old heads in that window.
            match drawer
                .resolve_system_branch_identity_at_heads(&delta.branch_id, event_heads)
                .await?
            {
                BranchIdentityResolution::Deferred => {
                    tracing::debug!(
                        branch_id = %delta.branch_id.0,
                        event_heads = ?event_heads,
                        "facet-set projection deferred until source heads materialize"
                    );
                    return Ok(None);
                }
                BranchIdentityResolution::Ignored | BranchIdentityResolution::ImportedHistory => {
                    continue;
                }
                BranchIdentityResolution::Found(_) => {}
            }
            let physical_id = delta.branch_id.0.parse::<big_repo::DocumentId>()?;
            let Some(latest_heads) = drawer.get_branch_heads_by_doc_id(physical_id).await? else {
                tracing::debug!(
                    branch_id = %delta.branch_id.0,
                    event_heads = ?event_heads,
                    "facet-set projection deferred until document heads materialize"
                );
                return Ok(None);
            };
            tracing::debug!(
                branch_id = %delta.branch_id.0,
                event_heads = ?event_heads,
                latest_heads = ?latest_heads,
                heads_advanced = event_heads != &latest_heads,
                "facet-set projecting latest materialized document heads"
            );
            settled_current_heads = Some(latest_heads.clone());

            // The all-parts source includes system docs (app_doc, drawer_doc,
            // config doc, plug manifest docs) that have not been migrated to
            // the facet-based format and carry no Branch facet. Only content
            // docs may be projected here, so resolve the identity at the latest
            // materialized heads and skip everything else.
            let identity = match drawer
                .resolve_system_branch_identity_at_heads(&delta.branch_id, &latest_heads)
                .await?
            {
                BranchIdentityResolution::Ignored | BranchIdentityResolution::ImportedHistory => {
                    continue;
                }
                BranchIdentityResolution::Deferred => {
                    return Ok(None);
                }
                BranchIdentityResolution::Found(identity) => identity,
            };
            let branch_key = (identity.document_id.clone(), delta.branch_id.0.clone());
            if !affected.insert(branch_key.clone()) {
                return Err(ferr!("duplicate DocDelta branch in one source revision"));
            }
            branch_heads.insert(branch_key, Some(latest_heads.clone()));
            let Some(state) = drawer
                .hydrate_dmeta_state_at_heads(&delta.branch_id, latest_heads.clone())
                .await?
            else {
                tracing::warn!(
                    branch_id = %delta.branch_id.0,
                    latest_heads = ?latest_heads,
                    "facet-set latest document disappeared before dmeta hydration"
                );
                return Ok(None);
            };
            if state.document_id != identity.document_id {
                return Err(ferr!("dmeta state identity does not match Branch facet"));
            }
            if delta.branch_id.0 == state.document_id {
                let touched_local = drawer
                    .facet_keys_touched_by_local_actor(
                        &state.document_id,
                        daybook_types::doc::BranchPath::new("main"),
                        &state.branch_heads,
                        &state.all_facet_keys,
                    )
                    .await?;
                let Some(touched_local) = touched_local else {
                    // Doc not resolvable at these heads; defer the revision
                    // rather than classifying local origin with stale data.
                    return Ok(None);
                };
                for facet_key in touched_local {
                    removed_local.insert((
                        state.document_id.clone(),
                        delta.branch_id.0.clone(),
                        facet_key,
                    ));
                }
            }
            for (facet_key, (facet_heads, actor_id)) in state.facets {
                // The second projection: what the facet VALUE names. Only the
                // `Blob` tag carries a digest in its value; the digest row
                // lands beside the route rows in the same settlement, and its
                // deletion rides `affected`, so a branch's digest projection
                // always mirrors its facet-set rows.
                let digests = if let FacetTag::WellKnown(WellKnownFacetTag::Blob) = &facet_key.tag {
                    let Some(digests) = self
                        .blob_facet_digests(
                            drawer,
                            &delta.branch_id,
                            &state.branch_heads,
                            &facet_key,
                        )
                        .await?
                    else {
                        // The value is not materialized at these heads; defer
                        // the revision, as every other hydration here does.
                        return Ok(None);
                    };
                    (!digests.is_empty()).then_some(digests)
                } else {
                    None
                };
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
                if let Some(digests) = digests {
                    blob_digests
                        .entry((state.document_id.clone(), delta.branch_id.0.clone()))
                        .or_default()
                        .extend(digests);
                }
            }
        }

        Ok(Some(PreparedFacetSetProjection {
            desired,
            affected,
            branch_heads,
            removed_local,
            settled_current_heads,
            blob_digests,
        }))
    }

    /// The plaintext digests one Blob facet's value names, via the same
    /// helper the reconcile reads Blob facets with (`plaintext_blob_id`): the
    /// value's `digest` field in either ADR 003 §3 spelling, falling back to
    /// the digest a `db+blob` URL path names. `None` defers the revision the
    /// way an unresolved identity does; an absent or untypable value indexes
    /// no digest rather than failing the machine, because the value is
    /// peer-authored content the route projection must not stall on.
    async fn blob_facet_digests(
        &self,
        drawer: &DrawerRepo,
        physical_branch_id: &BranchId,
        branch_heads: &ChangeHashSet,
        facet_key: &FacetKey,
    ) -> Res<Option<Vec<BlobId>>> {
        match drawer
            .hydrate_facet_value_at_heads(physical_branch_id, branch_heads, facet_key)
            .await?
        {
            ExactFacetValueHydration::Deferred => Ok(None),
            ExactFacetValueHydration::Absent => {
                tracing::warn!(
                    branch_id = %physical_branch_id.0,
                    facet = %facet_key,
                    "facet-set value-digest projection: active Blob facet's value is absent"
                );
                Ok(Some(Vec::new()))
            }
            ExactFacetValueHydration::Present(raw) => {
                let blob = match WellKnownFacet::from_json(raw, WellKnownFacetTag::Blob) {
                    Ok(WellKnownFacet::Blob(blob)) => blob,
                    Ok(other) => {
                        tracing::warn!(
                            facet = %facet_key,
                            tag = ?other.tag(),
                            "Blob facet decoded to another variant; indexing no digest"
                        );
                        return Ok(Some(Vec::new()));
                    }
                    Err(error) => {
                        tracing::warn!(%error, facet = %facet_key, "Blob facet value undecodable");
                        return Ok(Some(Vec::new()));
                    }
                };
                Ok(Some(plaintext_blob_id(&blob).into_iter().collect()))
            }
        }
    }

    /// Apply a prepared projection inside the caller-owned walker state
    /// transaction. This method never commits; the frontier revision, typed
    /// rows, sparse DocDelta state, and upstream cursor therefore settle as
    /// one SQLite unit.
    async fn apply_projection_in_context(
        &self,
        prepared: &PreparedFacetSetProjection,
        tx: &mut Tx,
    ) -> Res<()> {
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
        // The value-digest projection rides the same settlement: a branch's
        // digest rows are rewritten exactly when its facet rows are.
        delete_blob_digest_routes(tx, &prepared.affected).await?;
        insert_blob_digest_routes(tx, &prepared.blob_digests).await?;
        self.frontier
            .apply_in_context(tx, mutations)
            .await
            .map_err(|error| ferr!("applying facet-set frontier: {error}"))?;

        Ok(())
    }

    /// Wake downstream walkers/watches after a committed frontier revision.
    /// Notification is a wakeup only: durable state and cursors determine
    /// what must be read.
    pub(crate) fn notify_changed(&self) {
        self.frontier.notify_changed();
    }
}

async fn load_route_keys(
    tx: &mut Tx,
    affected: &BTreeSet<(DocId, String)>,
) -> Res<BTreeSet<FacetRouteKey>> {
    if affected.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut query = Sql::new(
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
            .bind(document_id)
            .push(", ")
            .bind(branch_id)
            .push(")");
    }
    query.push(")");
    let mut keys = BTreeSet::new();
    for row in query.query(tx).await?.rows {
        keys.insert(FacetRouteKey {
            document_id: row.get("document_id")?,
            branch_id: BranchId(row.get("branch_id")?),
            facet_key: FacetKey {
                tag: row.get::<String>("facet_tag")?.into(),
                id: row.get("facet_id")?,
            },
        });
    }
    Ok(keys)
}

async fn delete_routes(tx: &mut Tx, affected: &BTreeSet<(DocId, String)>) -> Res<()> {
    if affected.is_empty() {
        return Ok(());
    }
    let mut query =
        Sql::new("DELETE FROM facet_set_doc_facets WHERE (document_id, branch_id) IN (");
    let mut first = true;
    for (document_id, branch_id) in affected {
        if !first {
            query.push(", ");
        }
        first = false;
        query
            .push("(")
            .bind(document_id)
            .push(", ")
            .bind(branch_id)
            .push(")");
    }
    query.push(")");
    query.execute(tx).await?;
    Ok(())
}

async fn insert_routes(tx: &mut Tx, desired: &BTreeMap<FacetRouteKey, FacetSnapshot>) -> Res<()> {
    if desired.is_empty() {
        return Ok(());
    }
    let mut query = Sql::new(
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
            .bind(&key.document_id)
            .push(", ")
            .bind(&key.branch_id.0)
            .push(", ")
            .bind(key.facet_key.tag.to_string())
            .push(", ")
            .bind(&key.facet_key.id)
            .push(", ")
            .bind(serde_json::to_string(&snapshot.facet_heads)?)
            .push(", ")
            .bind(serde_json::to_string(&snapshot.actor_id)?)
            .push(", ")
            .bind(serde_json::to_string(&snapshot.branch_heads)?)
            .push(")");
    }
    query.execute(tx).await?;
    Ok(())
}

async fn delete_blob_digest_routes(tx: &mut Tx, affected: &BTreeSet<(DocId, String)>) -> Res<()> {
    if affected.is_empty() {
        return Ok(());
    }
    let mut query =
        Sql::new("DELETE FROM facet_set_doc_blob_facets WHERE (document_id, branch_id) IN (");
    let mut first = true;
    for (document_id, branch_id) in affected {
        if !first {
            query.push(", ");
        }
        first = false;
        query
            .push("(")
            .bind(document_id)
            .push(", ")
            .bind(branch_id)
            .push(")");
    }
    query.push(")");
    query.execute(tx).await?;
    Ok(())
}

async fn insert_blob_digest_routes(
    tx: &mut Tx,
    blob_digests: &BTreeMap<(DocId, String), BTreeSet<BlobId>>,
) -> Res<()> {
    if blob_digests.is_empty() {
        return Ok(());
    }
    // The digest is stored in the bare multibase spelling (`BlobId`'s
    // `Display`): one canonical spelling per digest, whatever the facet
    // value's authored spelling was.
    let mut query = Sql::new(
        "INSERT INTO facet_set_doc_blob_facets(document_id, branch_id, blob_digest) VALUES ",
    );
    let mut first = true;
    for (row, digests) in blob_digests {
        for digest in digests {
            if !first {
                query.push(", ");
            }
            first = false;
            query
                .push("(")
                .bind(&row.0)
                .push(", ")
                .bind(&row.1)
                .push(", ")
                .bind(digest.to_string())
                .push(")");
        }
    }
    query.execute(tx).await?;
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
    sql: Database,
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
    use std::hash::Hash;
    use std::hash::Hasher;
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
    Deferred,
}

async fn run_facet_set_delta_task(
    task: FacetSetDeltaTask,
    drawer: Arc<DrawerRepo>,
    store: Arc<FacetSetRevisionStore>,
    memory: big_sync::SqliteDeltaWalkerStateRepo,
) -> Res<FacetSetTaskOutput> {
    if task.delta.epoch_only {
        begin_settlement(&memory, &task.delta)
            .await?
            .settle()
            .await?;
        return Ok(FacetSetTaskOutput::Applied);
    }
    // All hydration completes before the settlement transaction opens; the
    // projection write and the memory advance then commit as one SQLite unit.
    let Some(prepared) = store
        .prepare_projection(&drawer, vec![task.delta.clone()])
        .await?
    else {
        return Ok(FacetSetTaskOutput::Deferred);
    };
    let mut settled_delta = task.delta.clone();
    settled_delta.current_heads = prepared.settled_current_heads.clone();
    let mut settlement = begin_settlement(&memory, &settled_delta).await?;
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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(worker = "facet-set-index-machine")
    )]
    async fn machine_loop(
        &self,
        drawer: Arc<DrawerRepo>,
        part_store: big_repo::SharedPartStore,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let source = DocDeltaRevisionStore::new(AutomergeFrontierRevisionStore::new(part_store));
        // The walker cursor and the site memory share one state repo: the
        // progress table keys are disjoint from the per-branch memory keys.
        // Legacy rows without the cursor key are replayed.
        let state = self.revision_store.input_state();
        let selector = DocDeltaSelector {
            memory: state.clone(),
            source: AutomergeFrontierSelector {
                targets: vec![AutomergeFrontierTarget::All],
            },
            filter: DocDeltaBranchFilter::All,
        };
        let durable = state.progress().await?.upstream_revision;
        let reader = source.open(selector, durable).await?;
        let mut walker = ConcurrentDeltaWalker::open(reader, state.clone(), |delta: &DocDelta| {
            doc_delta_key(&delta.branch_id)
        })
        .await?;
        let mut tasks = TokioKeyedScheduler::new(FACET_SET_TASK_BUDGET);
        // One AFW-backed reader avoids one BigRepo listener per parked branch;
        // its Drawer wrapper owns the frontier cursor and per-document dedup.
        let mut materialization_wake = drawer.open_materialization_reader(None).await?;
        // The newest unacked delta per key.
        let mut pending: HashMap<DocDeltaKey, FacetSetDeltaTask> = HashMap::new();
        // Materialization wakes can race with task publication and completion.
        // Remember wakes that cannot immediately wake a task and consume them
        // after parking.
        let mut pending_wakes = BTreeSet::new();
        loop {
            let available = FACET_SET_TASK_BUDGET.saturating_sub(tasks.active_count());
            let next_deadline = tasks.next_deadline();
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                change = materialization_wake.next() => {
                    let change = change?;
                    let key = doc_delta_key(&BranchId(change.doc_id.to_string()));
                    if let Some(task) = pending.get(&key).cloned() {
                        let future = run_facet_set_delta_task(
                            task.clone(),
                            Arc::clone(&drawer),
                            Arc::clone(&self.revision_store),
                            state.clone(),
                        );
                        if !tasks.wake(key, future)? {
                            pending_wakes.insert(key);
                        }
                    } else {
                        // Retain an early wake until the walker publishes the
                        // corresponding delta and its task can be retried.
                        pending_wakes.insert(key);
                    }
                }
                completion = tasks.next_completion() => {
                    if let Some(task) = self
                        .on_task_completion(
                            &mut walker,
                            &mut tasks,
                            &mut pending,
                            &mut pending_wakes,
                            completion?,
                        )
                        .await?
                        && pending_wakes.remove(&task.key)
                    {
                        let key = task.key;
                        let future = run_facet_set_delta_task(
                            task,
                            Arc::clone(&drawer),
                            Arc::clone(&self.revision_store),
                            state.clone(),
                        );
                        tasks.wake(key, future)?;
                    }
                }
                read = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        walker
                            .next(std::num::NonZeroUsize::new(available).expect("available is non-zero"))
                            .await
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
            DocDeltaRevisionStore<
                AutomergeFrontierRevisionStore,
                big_sync::SqliteDeltaWalkerStateRepo,
            >,
            big_sync::SqliteDeltaWalkerStateRepo,
            DocDeltaKey,
        >,
        tasks: &mut TokioKeyedScheduler<DocDeltaKey, FacetSetDeltaTask, FacetSetTaskOutput>,
        pending: &mut HashMap<DocDeltaKey, FacetSetDeltaTask>,
        pending_wakes: &mut BTreeSet<DocDeltaKey>,
        completion: TokioTaskCompletion<FacetSetDeltaTask, FacetSetTaskOutput>,
    ) -> Res<Option<FacetSetDeltaTask>> {
        let task = completion.command;
        match completion.result {
            Ok(FacetSetTaskOutput::Applied) => {
                // The command's effect is durable; only now may the walker
                // cursor advance past it.
                walker.ack(task.key, task.cursor).await?;
                if pending
                    .get(&task.key)
                    .is_some_and(|existing| existing.cursor == task.cursor)
                {
                    pending_wakes.remove(&task.key);
                    pending.remove(&task.key);
                }
                Ok(None)
            }
            Ok(FacetSetTaskOutput::Deferred) => {
                // Keep the source cursor and command pending until AFW
                // publishes that this branch can be materialized.
                tasks.park(task.key, task.clone());
                Ok(Some(task))
            }
            Err(error) => panic!("facet-set task failed: {error:?}"),
        }
    }

    fn on_delta(
        &self,
        drawer: &Arc<DrawerRepo>,
        state: &big_sync::SqliteDeltaWalkerStateRepo,
        tasks: &mut TokioKeyedScheduler<DocDeltaKey, FacetSetDeltaTask, FacetSetTaskOutput>,
        pending: &mut HashMap<DocDeltaKey, FacetSetDeltaTask>,
        delta: ConcurrentDelta<DocDeltaKey, DocDelta>,
    ) -> Res<()> {
        let mut task = FacetSetDeltaTask {
            key: delta.key,
            cursor: delta.cursor,
            delta: delta.entry,
        };
        if let Some(existing) = pending.get(&delta.key)
            && existing.cursor >= task.cursor
        {
            return Ok(());
        }
        if task.delta.epoch_only
            && let Some(existing) = pending.get(&delta.key)
        {
            // An epoch-only event must not replace a parked head transition.
            // Advance that transition's settlement cursor and epoch instead. If
            // there is no projection pending, the fast path settles directly.
            let mut merged = existing.clone();
            merged.cursor = task.cursor;
            merged.delta.current_causal_epoch = task.delta.current_causal_epoch;
            task = merged;
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
    pub(crate) fn sql(&self) -> &Database {
        &self.sql
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(worker = "facet-set-index")
    )]
    pub async fn boot(
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
        drawer: Arc<DrawerRepo>,
        part_store: big_repo::SharedPartStore,
        parent_cancel_token: CancellationToken,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let sql = sqlite_local_state_repo
            .ensure_sqlite_db(FACET_SET_LOCAL_STATE_ID)
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
            let part_store = Arc::clone(&part_store);
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

    async fn init_schema(sql: &Database) -> Res<()> {
        sql.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        Ok(())
    }

    pub async fn list_tags_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        Ok(queries::tags_for_doc(&self.sql, doc_id.clone()).await?)
    }

    pub async fn list_docs_for_tag(&self, facet_tag: &str) -> Res<Vec<DocFacetTagMembership>> {
        let rows = queries::docs_for_tag(&self.sql, facet_tag.to_owned()).await?;

        rows.into_iter()
            .map(|row| {
                let head_strings: Vec<String> = serde_json::from_str(&row.branch_heads_json)?;
                Ok(DocFacetTagMembership {
                    doc_id: row.document_id,
                    branch_id: BranchId(row.branch_id),
                    facet_tag: facet_tag.to_string(),
                    origin_heads: ChangeHashSet(am_utils_rs::parse_commit_heads(&head_strings)?),
                })
            })
            .collect()
    }
    /// The association lookup the presence plane drives: documents/branches
    /// whose `Blob` facet VALUE names `blob_digest` as its plaintext digest
    /// (ADR 003 §13). This is not the facet key's id — production authors
    /// `Blob` facets via `FacetKey::from(WellKnownFacetTag::Blob)` whose id
    /// is `DEFAULT_FACET_ID` — so the digest projection built by the
    /// streaming facet-set projection is what a blob arrival resolves here,
    /// compared in its canonical bare-multibase spelling.
    pub async fn list_docs_for_blob_digest(
        &self,
        blob_digest: &BlobId,
    ) -> Res<Vec<DocFacetTagMembership>> {
        let rows = queries::docs_for_blob_digest(
            &self.sql,
            WellKnownFacetTag::Blob.as_str().to_owned(),
            blob_digest.to_string(),
        )
        .await?;

        rows.into_iter()
            .map(|row| {
                let head_strings: Vec<String> = serde_json::from_str(&row.branch_heads_json)?;
                Ok(DocFacetTagMembership {
                    doc_id: row.document_id,
                    branch_id: BranchId(row.branch_id),
                    facet_tag: WellKnownFacetTag::Blob.as_str().to_string(),
                    origin_heads: ChangeHashSet(am_utils_rs::parse_commit_heads(&head_strings)?),
                })
            })
            .collect()
    }

    /// The same table keyed the other way: the documents/branches a physical
    /// branch doc carries, for the eligibility-group membership events whose
    /// member objects are branch docs.
    pub async fn list_docs_for_branch_id(
        &self,
        branch_id: &str,
    ) -> Res<Vec<DocFacetTagMembership>> {
        let rows = queries::docs_for_branch_id(&self.sql, branch_id.to_owned()).await?;

        rows.into_iter()
            .map(|row| {
                let head_strings: Vec<String> = serde_json::from_str(&row.branch_heads_json)?;
                Ok(DocFacetTagMembership {
                    doc_id: row.document_id,
                    branch_id: BranchId(row.branch_id),
                    facet_tag: row.facet_tag,
                    origin_heads: ChangeHashSet(am_utils_rs::parse_commit_heads(&head_strings)?),
                })
            })
            .collect()
    }

    pub async fn has_tag(&self, doc_id: &DocId, facet_tag: &str) -> Res<bool> {
        let exists = queries::doc_has_tag(&self.sql, doc_id.clone(), facet_tag.to_owned()).await?;
        Ok(exists.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use big_sync_core::revisioned_store::RevisionReadLimits;
    use daybook_types::doc::AddDocArgs;
    use daybook_types::doc::BranchPathBuf;
    use daybook_types::doc::FacetKey;
    use daybook_types::doc::FacetRaw;
    use daybook_types::doc::WellKnownFacet;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn projection_persistence_is_atomic_with_walker_checkpoint() -> Res<()> {
        use crate::index::doc_delta_store::DocDeltaBranchState;
        use big_sync_core::delta_walker_sparse_state::DeltaWalkerSparseStateRepo as _;

        let runtime = sqlite_rs::SqliteRuntime::new(default());
        let sql = runtime.ephemeral(&[])?;
        DocFacetSetIndexRepo::init_schema(&sql).await?;
        let store = Arc::new(FacetSetRevisionStore::boot(sql.clone()).await?);
        let repo = DocFacetSetIndexRepo {
            sql: sql.clone(),
            revision_store: Arc::clone(&store),
        };
        let key = FacetRouteKey {
            document_id: "doc".into(),
            branch_id: BranchId("branch".into()),
            facet_key: FacetKey::from(WellKnownFacetTag::Blob),
        };
        let heads = ChangeHashSet(Vec::new().into());
        let snapshot = FacetSnapshot {
            branch_heads: heads.clone(),
            facet_heads: heads.clone(),
            actor_id: automerge::ActorId::from(b"projection-test".as_slice()),
        };
        let blob = BlobId::new([7; 32]);
        let branch = (key.document_id.clone(), key.branch_id.0.clone());
        let prepared = PreparedFacetSetProjection {
            desired: BTreeMap::from([(key.clone(), snapshot.clone())]),
            affected: BTreeSet::from([branch.clone()]),
            branch_heads: BTreeMap::from([(branch.clone(), Some(heads.clone()))]),
            removed_local: BTreeSet::new(),
            settled_current_heads: Some(heads.clone()),
            blob_digests: BTreeMap::from([(branch, BTreeSet::from([blob.clone()]))]),
        };
        let delta = DocDelta {
            branch_id: key.branch_id.clone(),
            previous_heads: None,
            current_heads: Some(heads.clone()),
            previous_causal_epoch: None,
            current_causal_epoch: Some([3; 32]),
            epoch_only: false,
        };
        let memory = store.input_state();
        {
            let mut abandoned = begin_settlement(&memory, &delta).await?;
            store
                .apply_projection_in_context(&prepared, abandoned.context_mut())
                .await?;
            let keys = load_route_keys(abandoned.context_mut(), &prepared.affected).await?;
            assert_eq!(keys, BTreeSet::from([key.clone()]));
        }
        sql.begin_write().await?.rollback().await?;
        assert_eq!(store.latest_revision().await?, 0);
        assert!(repo.list_tags_for_doc(&key.document_id).await?.is_empty());
        assert!(repo.list_docs_for_blob_digest(&blob).await?.is_empty());
        assert_eq!(memory.get(key.branch_id.0.as_bytes()).await?, None);

        let mut settlement = begin_settlement(&memory, &delta).await?;
        store
            .apply_projection_in_context(&prepared, settlement.context_mut())
            .await?;
        assert!(repo.list_tags_for_doc(&key.document_id).await?.is_empty());
        assert_eq!(memory.get(key.branch_id.0.as_bytes()).await?, None);
        settlement.settle().await?;
        store.notify_changed();
        drop(repo);
        drop(store);
        drop(memory);

        let reopened = Arc::new(FacetSetRevisionStore::boot(sql.clone()).await?);
        let repo = DocFacetSetIndexRepo {
            sql,
            revision_store: Arc::clone(&reopened),
        };
        assert_eq!(
            repo.list_tags_for_doc(&key.document_id).await?,
            vec![WellKnownFacetTag::Blob.as_str()]
        );
        let memberships = repo
            .list_docs_for_blob_digest(&blob)
            .await?
            .into_iter()
            .map(|membership| {
                (
                    membership.doc_id,
                    membership.branch_id,
                    membership.facet_tag,
                    membership.origin_heads,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            memberships,
            vec![(
                key.document_id.clone(),
                key.branch_id.clone(),
                WellKnownFacetTag::Blob.as_str().to_string(),
                heads.clone()
            )]
        );
        let bytes = reopened
            .input_state()
            .get(key.branch_id.0.as_bytes())
            .await?
            .expect("committed checkpoint");
        assert_eq!(
            serde_json::from_slice::<DocDeltaBranchState>(&bytes)?,
            DocDeltaBranchState {
                heads: Some(heads.clone()),
                causal_epoch: delta.current_causal_epoch,
            }
        );
        let revision = reopened.latest_revision().await?;
        assert!(revision > 0);
        let mut reader = reopened.open(FacetSetSelector::All, 0).await?;
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision,
                entries: vec![FacetDelta {
                    key,
                    current: Some(snapshot),
                    current_branch_heads: Some(heads),
                    removed_local: false,
                }],
            }
        );
        Ok(())
    }

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

                idempotency_key: None,
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

                idempotency_key: None,
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

                idempotency_key: None,
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
            previous_causal_epoch: None,
            current_causal_epoch: None,
            epoch_only: false,
        };
        let system_delta = DocDelta {
            branch_id: BranchId(system_doc_id.clone()),
            previous_heads: None,
            current_heads: Some(system_heads),
            previous_causal_epoch: None,
            current_causal_epoch: None,
            epoch_only: false,
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
