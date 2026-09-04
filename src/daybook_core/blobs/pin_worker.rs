use crate::interlude::*;

use daybook_types::doc::{
    BlobPin, ChangeHashSet, DocId, DocPatch, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
};
use tokio_util::sync::CancellationToken;

use crate::drawer::{DrawerRepo, MaterializationWake};
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::repos::RepoStopToken;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync::DeltaWalkerStateRepo as _;
use big_sync_core::revisioned_store::RevisionedStore as _;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker,
};
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use daybook_types::doc::BranchId;
use sqlx::{Row, Sqlite};

pub(crate) const BLOB_PIN_STATE_LOCAL_STATE_ID: &str = "@daybook/core/blob-pin-worker";

/// Walker state for the enablement machine (plugs config event rev store).
pub(crate) const BLOB_PIN_PLUG_EVENTS_STATE_ID: &str = "@daybook/core/blob-pin-plug-events";

/// Spawn the blob-pin worker and its two machines:
///
/// - the facet machine: blob facet deltas -> docs inventory (full-branch
///   state per delta);
/// - the plug-events machine: typed `PlugsEvent`s from the plugs config
///   rev store -> core inventory pin maintenance, enablement-driven.
///
/// Both share one inventory lock so their inventory writes cannot
/// interleave. The machines' stop handle rides the returned
/// [`RepoStopToken`]. No public surface: observers read the inventory
/// docs through the drawer.
pub async fn spawn_blob_pin_worker(
    drawer_repo: Arc<DrawerRepo>,
    sql: SqlCtx,
    core_inventory_doc_id: DocumentId,
    docs_inventory_doc_id: DocumentId,
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<crate::plugs::PlugsRepo>,
    parent_cancel_token: CancellationToken,
) -> Res<RepoStopToken> {
    Ctx::ensure_schema(&sql).await?;

    let core_doc_id =
        Ctx::resolve_doc_id_for_branch(&drawer_repo, core_inventory_doc_id).await?;
    let docs_doc_id =
        Ctx::resolve_doc_id_for_branch(&drawer_repo, docs_inventory_doc_id).await?;
    let ctx = Arc::new(Ctx {
        drawer_repo,
        sql,
        core_inventory_doc_id: core_doc_id,
        docs_inventory_doc_id: docs_doc_id,
        inventory_lock: Arc::new(tokio::sync::Mutex::new(())),
    });
    let event_store = Arc::new(crate::plugs::PlugsConfigEventStore::new(
        Arc::clone(&facet_set_store),
        Arc::clone(&ctx.drawer_repo),
        &plugs_repo,
    ));
    let cancel_token = parent_cancel_token.child_token();
    // One supervisor joins both machines; a panic in either takes the
    // task down per the task-panic-handler convention.
    let worker_handle = tokio::spawn({
        let facet_set_store = Arc::clone(&facet_set_store);
        let plugs_repo = Arc::clone(&plugs_repo);
        let cancel_token = cancel_token.clone();
        let mut facet_worker = Worker::new(Arc::clone(&ctx));
        let mut event_worker = Worker::new(ctx);
        async move {
            let facet = facet_worker.run_facet_machine(facet_set_store, cancel_token.clone());
            let events = event_worker.run_plug_events_machine(event_store, plugs_repo, cancel_token);
            let (facet, events) = tokio::join!(facet, events);
            facet.expect("blob-pin facet machine error");
            events.expect("blob-pin plug-events machine error");
        }
    });
    Ok(RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

/// Shared state of the blob-pin worker: the facet machine (docs inventory)
/// and the plug-events machine (core inventory) plus their inventory
/// upsert subtasks all hold this Arc. Private — the worker has no public
/// surface; observers read the inventory docs through the drawer.
struct Ctx {
    drawer_repo: Arc<DrawerRepo>,
    sql: SqlCtx,
    core_inventory_doc_id: DocId,
    docs_inventory_doc_id: DocId,
    /// One lock shared by both machines: plug-pin upserts and facet-driven
    /// inventory diffs must not interleave.
    inventory_lock: Arc<tokio::sync::Mutex<()>>,
}

/// Private machine owner. The context is shared with task futures, while each
/// machine keeps its walker, scheduler, pending work, and wake state on its
/// own stack in the machine method.
struct Worker {
    ctx: Arc<Ctx>,
}

impl Worker {
    fn new(ctx: Arc<Ctx>) -> Self {
        Self { ctx }
    }
}

impl std::ops::Deref for Worker {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

struct PreparedDocBranch {
    doc_id: DocId,
    branch_id: BranchId,
    pins: Option<HashMap<String, u64>>,
}
impl Ctx {


    async fn resolve_doc_id_for_branch(
        drawer_repo: &DrawerRepo,
        branch_doc_id: DocumentId,
    ) -> Res<DocId> {
        let (_, doc_ids) = drawer_repo.list_just_ids().await?;
        for id_str in doc_ids {
            let doc_id = DocId::from(id_str);
            if let Some(entry) = drawer_repo.get_entry(&doc_id).await?
                && entry
                    .branches
                    .values()
                    .any(|branch| branch.branch_doc_id == branch_doc_id)
            {
                return Ok(doc_id);
            }
        }
        Ok(DocId::from(branch_doc_id.to_string()))
    }

    async fn ensure_schema(sql: &SqlCtx) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blob_pin_doc_state (
                doc_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL,
                PRIMARY KEY (doc_id, branch_id, blob_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_blob_pin_doc_state_hash ON blob_pin_doc_state(blob_hash);

            CREATE TABLE IF NOT EXISTS blob_pin_plug_state (
                plug_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL,
                PRIMARY KEY (plug_id, blob_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_blob_pin_plug_state_hash ON blob_pin_plug_state(blob_hash);
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        let has_branch_id_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('blob_pin_doc_state') WHERE name = 'branch_id'",
        )
        .fetch_optional(&sql.write_pool)
        .await?;
        if has_branch_id_col.is_none() {
            let mut tx = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("ALTER TABLE blob_pin_doc_state RENAME TO blob_pin_doc_state_legacy")
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"CREATE TABLE blob_pin_doc_state (
                    doc_id TEXT NOT NULL
                  , branch_id TEXT NOT NULL
                  , blob_hash TEXT NOT NULL
                  , length_octets INTEGER NOT NULL
                  , PRIMARY KEY (doc_id, branch_id, blob_hash)
                )"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"INSERT INTO blob_pin_doc_state(doc_id, branch_id, blob_hash, length_octets)
                   SELECT doc_id
                        , CASE WHEN branch_path = 'main' THEN doc_id ELSE branch_path END
                        , blob_hash
                        , length_octets
                     FROM blob_pin_doc_state_legacy"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("DROP TABLE blob_pin_doc_state_legacy")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blob_pin_doc_state_hash ON blob_pin_doc_state(blob_hash)")
            .execute(&sql.write_pool)
            .await?;
        Ok(())
    }

    /// Blob pin candidates from one Blob facet value: the facet's digest plus
    /// any `db+blob` component URLs, keyed by representation hash.
    fn blob_pins_from_facet_value(blob: &daybook_types::doc::Blob) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        if let Some(urls) = &blob.urls {
            for url_str in urls {
                if let Ok(url) = url_str.parse::<url::Url>()
                    && (url.scheme() == crate::blobs::BLOB_SCHEME
                        || url.scheme() == "daybook-blob")
                {
                    let hash = url.path().trim_start_matches('/');
                    if hash.parse::<crate::blobs::BlobId>().is_ok() {
                        out.push((hash.to_string(), blob.length_octets));
                    }
                }
            }
        }
        if blob.digest.parse::<crate::blobs::BlobId>().is_ok() {
            out.push((blob.digest.clone(), blob.length_octets));
        }
        out
    }

    async fn hydrate_blob_pins(
        drawer: &DrawerRepo,
        physical_branch_id: &BranchId,
        document_id: &DocId,
        heads: ChangeHashSet,
    ) -> Res<Option<HashMap<String, u64>>> {
        let physical_id = physical_branch_id.0.parse::<big_repo::DocumentId>()?;
        let Some(facets) = drawer
            .hydrate_physical_doc_at_heads(physical_id, heads)
            .await?
        else {
            return Ok(None);
        };
        let branch = match WellKnownFacet::from_json(
            facets
                .get(&FacetKey::from(WellKnownFacetTag::Branch))
                .cloned()
                .ok_or_else(|| ferr!("missing mandatory Branch facet"))?,
            WellKnownFacetTag::Branch,
        )? {
            WellKnownFacet::Branch(branch) => branch,
            _ => unreachable!("Branch facet decoded to another well-known variant"),
        };
        if branch.branch_id != *physical_branch_id || branch.document_id != *document_id {
            return Err(ferr!("blob facet branch identity mismatch"));
        }
        // Plug manifests are static artifacts whose blob pins follow
        // ENABLEMENT (the core inventory, driven by the plugs config event
        // stream), not doc presence. Exclude them from the docs-inventory
        // path so a replicated manifest does not pin its blobs on every peer.
        if facets.contains_key(&FacetKey::from(WellKnownFacetTag::PlugManifest)) {
            return Ok(Some(HashMap::new()));
        }
        let dmeta = match WellKnownFacet::from_json(
            facets
                .get(&FacetKey::from(WellKnownFacetTag::Dmeta))
                .cloned()
                .ok_or_else(|| ferr!("missing mandatory Dmeta facet"))?,
            WellKnownFacetTag::Dmeta,
        )? {
            WellKnownFacet::Dmeta(dmeta) => dmeta,
            _ => unreachable!("Dmeta facet decoded to another well-known variant"),
        };
        if dmeta.id != *document_id {
            return Err(ferr!("dmeta document id does not match Branch facet"));
        }
        let mut current_pins = HashMap::new();
        for (facet_key, meta) in dmeta.facets {
            if facet_key.tag != WellKnownFacetTag::Blob.into() || !meta.deleted_at.is_empty() {
                continue;
            }
            let Some(facet_raw) = facets.get(&facet_key) else {
                return Err(ferr!("active Blob facet is missing its value"));
            };
            let WellKnownFacet::Blob(blob) =
                WellKnownFacet::from_json(facet_raw.clone(), WellKnownFacetTag::Blob)?
            else {
                unreachable!("Blob facet decoded to another well-known variant");
            };
            for (hash, length) in blob_pins_from_facet_value(&blob) {
                current_pins.insert(hash, length);
            }
        }
        Ok(Some(current_pins))
    }

    /// Hydrate the manifest doc's Blob facets at an enabled ref's heads.
    ///
    /// Returns `None` when the manifest is not locally readable at the ref
    /// (ADR 007 §6: pending). Unpinned refs resolve the branch's current
    /// heads. Pure read — the pin set is computed, never written back.
    async fn manifest_blob_pins(
        &self,
        ref_url: &url::Url,
    ) -> Res<Option<HashMap<String, u64>>> {
        let parsed = crate::plugs::PlugsRepo::parse_enabled_ref(ref_url)?;
        let branch_path =
            daybook_types::doc::BranchPath::new(parsed.branch.as_deref().unwrap_or("main"));
        let heads = if let Some(at) = &parsed.at {
            ChangeHashSet(am_utils_rs::parse_commit_heads(at)?)
        } else {
            let Some(heads) = self
                .drawer_repo
                .get_branch_heads_for_path(&parsed.doc_id, branch_path)
                .await?
            else {
                return Ok(None);
            };
            heads
        };
        // All facets: the manifest doc is small, and the Blob facet keys carry
        // per-blob ids, so a tag filter would need the full key list anyway.
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch_heads(&parsed.doc_id, branch_path, &heads, None)
            .await?
        else {
            return Ok(None);
        };
        let mut pins = HashMap::new();
        for (facet_key, raw) in &doc.facets {
            if facet_key.tag != WellKnownFacetTag::Blob.into() {
                continue;
            }
            let WellKnownFacet::Blob(blob) =
                WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::Blob)?
            else {
                unreachable!("Blob facet decoded to another well-known variant");
            };
            for (hash, length) in blob_pins_from_facet_value(&blob) {
                pins.insert(hash, length);
            }
        }
        Ok(Some(pins))
    }

    async fn desired_pins(&self) -> Res<HashMap<String, BlobPin>> {
        let rows = sqlx::query(
            "SELECT blob_hash, MAX(length_octets) AS length_octets
               FROM blob_pin_doc_state
              GROUP BY blob_hash",
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut pins = HashMap::new();
        for row in rows {
            let hash: String = row.try_get("blob_hash")?;
            let length_octets = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            pins.insert(hash, BlobPin { length_octets });
        }
        Ok(pins)
    }

    async fn apply_inventory_diff(
        &self,
        inventory_doc_id: &DocId,
        desired: &HashMap<String, BlobPin>,
    ) -> Res<()> {
        let current = self.list_pins_from_doc_id(inventory_doc_id).await?;
        let mut facets_set = HashMap::new();
        for (hash, pin) in desired {
            if current
                .get(hash)
                .is_none_or(|existing| existing.length_octets != pin.length_octets)
            {
                facets_set.insert(
                    FacetKey {
                        tag: WellKnownFacetTag::BlobPin.into(),
                        id: hash.clone(),
                    },
                    FacetRaw::from(WellKnownFacet::BlobPin(pin.clone())),
                );
            }
        }
        let mut facets_remove = current
            .keys()
            .filter(|hash| !desired.contains_key(*hash))
            .map(|hash| FacetKey {
                tag: WellKnownFacetTag::BlobPin.into(),
                id: hash.clone(),
            })
            .collect::<Vec<_>>();
        facets_remove.sort_by(|left, right| left.id.cmp(&right.id));
        if facets_set.is_empty() && facets_remove.is_empty() {
            return Ok(());
        }
        self.drawer_repo
            .update_at_heads(
                DocPatch {
                    id: inventory_doc_id.clone(),
                    user_path: None,
                    facets_set,
                    facets_remove,
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
            )
            .await?;
        Ok(())
    }

    async fn replace_doc_branch_state(&self, branches: &[PreparedDocBranch]) -> Res<()> {
        if branches.is_empty() {
            return Ok(());
        }
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        for branch in branches {
            sqlx::query("DELETE FROM blob_pin_doc_state WHERE doc_id = ? AND branch_id = ?")
                .bind(&branch.doc_id)
                .bind(&branch.branch_id.0)
                .execute(&mut *tx)
                .await?;
            let Some(pins) = &branch.pins else {
                continue;
            };
            if pins.is_empty() {
                continue;
            }
            let mut query = sqlx::QueryBuilder::<Sqlite>::new(
                "INSERT INTO blob_pin_doc_state(doc_id, branch_id, blob_hash, length_octets) ",
            );
            let rows = pins
                .iter()
                .map(|(hash, length)| Ok((hash.as_str(), i64::try_from(*length)?)))
                .collect::<Res<Vec<_>>>()?;
            query.push_values(rows.iter(), |mut row, (hash, length)| {
                row.push_bind(&branch.doc_id)
                    .push_bind(&branch.branch_id.0)
                    .push_bind(hash)
                    .push_bind(*length);
            });
            query.build().execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Upsert one enabled plug's blob pins into the core inventory.
    ///
    /// Kept core-inventory machinery (blobPin upsert + orphan eviction keyed
    /// by plug): only the pin-source changes with the plugs event rework —
    /// the caller computes pins from the manifest doc's Blob facets at the
    /// enabled heads instead of parsing manifest internals.
    async fn apply_plug_pins(&self, plug_id: &str, current_pins: HashMap<String, u64>) -> Res<()> {
        let prev_hashes: HashSet<String> =
            sqlx::query_scalar("SELECT blob_hash FROM blob_pin_plug_state WHERE plug_id = ?1")
                .bind(plug_id)
                .fetch_all(&self.sql.write_pool)
                .await?
                .into_iter()
                .collect();

        let mut pins_to_set = Vec::new();
        for (hash, length_octets) in &current_pins {
            pins_to_set.push((
                FacetKey {
                    tag: WellKnownFacetTag::BlobPin.into(),
                    id: hash.clone(),
                },
                FacetRaw::from(WellKnownFacet::BlobPin(BlobPin {
                    length_octets: *length_octets,
                })),
            ));
        }

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_plug_state WHERE plug_id = ?1")
            .bind(plug_id)
            .execute(&mut *tx)
            .await?;

        for (hash, length_octets) in &current_pins {
            sqlx::query(
                r#"
                INSERT INTO blob_pin_plug_state (plug_id, blob_hash, length_octets)
                VALUES (?1, ?2, ?3)
                "#,
            )
            .bind(plug_id)
            .bind(hash)
            .bind(*length_octets as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        let mut pins_to_remove = Vec::new();
        for prev_hash in &prev_hashes {
            if !current_pins.contains_key(prev_hash) {
                let remaining_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM blob_pin_plug_state WHERE blob_hash = ?1",
                )
                .bind(prev_hash)
                .fetch_one(&self.sql.write_pool)
                .await?;

                if remaining_count == 0 {
                    pins_to_remove.push(FacetKey {
                        tag: WellKnownFacetTag::BlobPin.into(),
                        id: prev_hash.clone(),
                    });
                }
            }
        }

        if !pins_to_set.is_empty() || !pins_to_remove.is_empty() {
            let mut facets_set = HashMap::new();
            for (key, value) in pins_to_set {
                facets_set.insert(key, FacetRaw::from(serde_json::to_value(value)?));
            }
            let patch = DocPatch {
                id: self.core_inventory_doc_id.clone(),
                user_path: None,
                facets_set,
                facets_remove: pins_to_remove,
            };
            self.drawer_repo
                .update_at_heads(patch, daybook_types::doc::BranchPath::new("main"), None)
                .await?;
        }

        Ok(())
    }

    /// Drop one plug's blob pins from the core inventory, evicting pins no
    /// other plug still references. Driven by `PlugsEvent::PlugDisabled`.
    async fn drop_plug_pins(&self, plug_id: &str) -> Res<()> {
        let prev_hashes: Vec<String> =
            sqlx::query_scalar("SELECT blob_hash FROM blob_pin_plug_state WHERE plug_id = ?1")
                .bind(plug_id)
                .fetch_all(&self.sql.write_pool)
                .await?;

        if prev_hashes.is_empty() {
            return Ok(());
        }

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_plug_state WHERE plug_id = ?1")
            .bind(plug_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut pins_to_remove = Vec::new();
        for hash in &prev_hashes {
            let remaining_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM blob_pin_plug_state WHERE blob_hash = ?1")
                    .bind(hash)
                    .fetch_one(&self.sql.write_pool)
                    .await?;

            if remaining_count == 0 {
                pins_to_remove.push(FacetKey {
                    tag: WellKnownFacetTag::BlobPin.into(),
                    id: hash.clone(),
                });
            }
        }

        if !pins_to_remove.is_empty() {
            let patch = DocPatch {
                id: self.core_inventory_doc_id.clone(),
                user_path: None,
                facets_set: HashMap::new(),
                facets_remove: pins_to_remove,
            };
            self.drawer_repo
                .update_at_heads(patch, daybook_types::doc::BranchPath::new("main"), None)
                .await?;
        }

        Ok(())
    }


    async fn list_pins_from_doc_id(&self, doc_id: &DocId) -> Res<HashMap<String, BlobPin>> {
        let Some(doc) = self
            .drawer_repo
            .get_doc_with_facets_at_branch(
                doc_id,
                &daybook_types::doc::BranchPathBuf::from("main"),
                None,
            )
            .await?
        else {
            return Ok(HashMap::new());
        };
        let mut pins = HashMap::new();
        for (key, raw) in &doc.facets {
            if key.tag == WellKnownFacetTag::BlobPin.into()
                && let Ok(WellKnownFacet::BlobPin(pin)) =
                    WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::BlobPin)
            {
                pins.insert(key.id.clone(), pin);
            }
        }
        Ok(pins)
    }
}

/// Keyed execution budget for the blob-pin machine; mirrors the frontier
/// worker's concurrent budget.
const BLOB_PIN_TASK_BUDGET: usize = 64;

/// Scheduling key: one physical branch (hash collisions only over-serialize
/// a key, never break correctness).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BlobPinKey(u64);

fn blob_pin_facet_key(branch_id: &BranchId) -> BlobPinKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    branch_id.0.hash(&mut hasher);
    BlobPinKey(hasher.finish())
}

/// The keyed command for one branch: the newest delta with the source cursor
/// it must cover.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlobPinTask {
    key: BlobPinKey,
    cursor: u64,
    /// One branch's blob facet delta from the facet-set source. Hydration is
    /// full-branch state at the delta's heads, so a newer cursor supersedes
    /// any older or sibling delta for the branch.
    delta: FacetDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlobPinTaskOutput {
    Applied,
}

async fn run_blob_pin_task(
    task: BlobPinTask,
    ctx: Arc<Ctx>,
    inventory_lock: Arc<tokio::sync::Mutex<()>>,
) -> Res<BlobPinTaskOutput> {
    let delta = task.delta;
    if delta.key.facet_key.tag != WellKnownFacetTag::Blob.into() {
        return Ok(BlobPinTaskOutput::Applied);
    }
    // All hydration completes before the inventory section opens; the
    // state write and the inventory diff are then serialized so their
    // global recomputes cannot interleave.
    let pins = match &delta.current_branch_heads {
        Some(heads) => {
            match Ctx::hydrate_blob_pins(
                &ctx.drawer_repo,
                &delta.key.branch_id,
                &delta.key.document_id,
                heads.clone(),
            )
            .await?
            {
                Some(pins) => pins,
                None => eyre::bail!(
                    "blob-pin source heads are not materialized for branch {}",
                    delta.key.branch_id.0
                ),
            }
        }
        None => {
            // Tombstone: the branch was removed; its pin rows go with it.
            let branch = PreparedDocBranch {
                doc_id: delta.key.document_id,
                branch_id: delta.key.branch_id,
                pins: None,
            };
            let _guard = inventory_lock.lock().await;
            ctx.replace_doc_branch_state(std::slice::from_ref(&branch))
                .await?;
            let docs = ctx.desired_pins().await?;
            ctx.apply_inventory_diff(&ctx.docs_inventory_doc_id, &docs)
                .await?;
            return Ok(BlobPinTaskOutput::Applied);
        }
    };
    let branch = PreparedDocBranch {
        doc_id: delta.key.document_id,
        branch_id: delta.key.branch_id,
        pins: Some(pins),
    };
    let _guard = inventory_lock.lock().await;
    ctx.replace_doc_branch_state(std::slice::from_ref(&branch))
        .await?;
    let docs = ctx.desired_pins().await?;
    ctx.apply_inventory_diff(&ctx.docs_inventory_doc_id, &docs)
        .await?;
    Ok(BlobPinTaskOutput::Applied)
}

impl Worker {
    /// The blob-pin facet machine: a `ConcurrentDeltaWalker` over the facet-set
    /// source (Blob tag), keyed by branch, with per-branch inventory tasks.
    /// Mutable machine state lives as stack locals here.
    async fn run_facet_machine(
        &mut self,
        facet_set_store: Arc<FacetSetRevisionStore>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let facet_state = SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            BLOB_PIN_STATE_LOCAL_STATE_ID,
            "facets",
        )
        .await
        .map_err(|error| ferr!("initializing blob-pin FacetSet walker state: {error}"))?;
        let durable = facet_state.progress().await?.upstream_revision;
        let reader = facet_set_store
            .open(FacetSetSelector::Tag(WellKnownFacetTag::Blob), durable)
            .await
            .map_err(|error| ferr!("opening blob-pin FacetSet reader: {error}"))?;
        let mut facet_walker = ConcurrentDeltaWalker::open(
            reader,
            facet_state,
            |entry: &FacetDelta| blob_pin_facet_key(&entry.key.branch_id),
        )
        .await
        .map_err(|error| ferr!("opening blob-pin FacetSet walker: {error}"))?;
        let mut tasks = TokioKeyedScheduler::new(BLOB_PIN_TASK_BUDGET);
        // The newest unacked delta per key.
        let mut pending: HashMap<BlobPinKey, BlobPinTask> = HashMap::new();
        loop {
            let available = BLOB_PIN_TASK_BUDGET.saturating_sub(tasks.active_count());
            let next_deadline = tasks.next_deadline();
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                completion = tasks.next_completion() => {
                    self.on_task_completion(
                        &mut facet_walker,
                        &mut tasks,
                        &mut pending,
                        completion?,
                    )
                    .await?;
                }
                facet = async {
                    if available == 0 {
                        std::future::pending().await
                    } else {
                        facet_walker.next(available).await
                    }
                } => match facet? {
                    ConcurrentDeltaRead::ReplayComplete { .. } => {}
                    ConcurrentDeltaRead::Entries { entries, .. } => {
                        for delta in entries {
                            self.on_delta(&mut tasks, &mut pending, delta)?;
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
                    tasks.tick(std::time::Instant::now())?
                }
            }
        }
    }

    async fn on_task_completion(
        &mut self,
        facet_walker: &mut ConcurrentDeltaWalker<
            '_,
            FacetSetRevisionStore,
            SqliteDeltaWalkerStateRepo,
            BlobPinKey,
        >,
        tasks: &mut TokioKeyedScheduler<BlobPinKey, BlobPinTask, BlobPinTaskOutput>,
        pending: &mut HashMap<BlobPinKey, BlobPinTask>,
        completion: TokioTaskCompletion<BlobPinTask, BlobPinTaskOutput>,
    ) -> Res<()> {
        let task = completion.command;
        match completion.result {
            BlobPinTaskOutput::Applied => {
                // The command's effect is durable; only now may the walker
                // cursor advance past it.
                facet_walker.ack(task.key, task.cursor).await?;
                if pending
                    .get(&task.key)
                    .is_some_and(|t| t.cursor == task.cursor)
                {
                    pending.remove(&task.key);
                }
            }
            Err(error) => panic!("blob-pin task failed: {error:?}"),
        }
        Ok(())
    }

    fn on_delta(
        &mut self,
        tasks: &mut TokioKeyedScheduler<BlobPinKey, BlobPinTask, BlobPinTaskOutput>,
        pending: &mut HashMap<BlobPinKey, BlobPinTask>,
        delta: ConcurrentDelta<BlobPinKey, FacetDelta>,
    ) -> Res<()> {
        let task = BlobPinTask {
            key: delta.key,
            cursor: delta.cursor,
            delta: delta.entry,
        };
        match pending.get(&task.key) {
            // Newest-wins is correct here: `hydrate_blob_pins` recomputes the
            // branch's full blob-pin state at the delta's heads, so a newer
            // cursor (or an equal-cursor sibling of the same branch) is fully
            // covered by the newest delta.
            Some(existing) if existing.cursor >= task.cursor => return Ok(()),
            _ => {}
        }
        pending.insert(task.key, task.clone());
        self.start_task(tasks, task)
    }

    fn start_task(
        &mut self,
        tasks: &mut TokioKeyedScheduler<BlobPinKey, BlobPinTask, BlobPinTaskOutput>,
        task: BlobPinTask,
    ) -> Res<()> {
        let future = run_blob_pin_task(
            task.clone(),
            Arc::clone(&self.ctx),
            Arc::clone(&self.ctx.inventory_lock),
        );
        tasks.replace(task.key, task.clone(), future)?;
        Ok(())
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PlugPinKey(u64);

fn plug_pin_key(plug_id: &str) -> PlugPinKey {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    plug_id.hash(&mut hasher);
    PlugPinKey(hasher.finish())
}

#[derive(Debug, Clone)]
struct PlugPinTask {
    key: PlugPinKey,
    event: crate::plugs::PlugsEvent,
    ref_url: Option<url::Url>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlugPinTaskOutput {
    Applied,
    Deferred,
}

async fn run_plug_pin_task(
    task: PlugPinTask,
    ctx: Arc<Ctx>,
    plugs_repo: Arc<crate::plugs::PlugsRepo>,
) -> Res<PlugPinTaskOutput> {
    match &task.event {
        crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
        | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. } => {
            let live_ref;
            let ref_url = match task.ref_url.as_ref() {
                Some(ref_url) => ref_url,
                None => {
                    let Some(ref_url) = plugs_repo.enabled_ref(plug_id).await? else {
                        return Ok(PlugPinTaskOutput::Applied);
                    };
                    live_ref = ref_url;
                    &live_ref
                }
            };
            let Some(pins) = ctx.manifest_blob_pins(ref_url).await? else {
                return Ok(PlugPinTaskOutput::Deferred);
            };
            let _guard = ctx.inventory_lock.lock().await;
            ctx.apply_plug_pins(plug_id, pins).await?;
        }
        crate::plugs::PlugsEvent::PlugDisabled { plug_id } => {
            let _guard = ctx.inventory_lock.lock().await;
            ctx.drop_plug_pins(plug_id).await?;
        }
        crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => {}
    }
    Ok(PlugPinTaskOutput::Applied)
}

/// The enablement machine: a serial walker over the plugs config event rev
/// store maintaining the core inventory's plug pins.
    ///
    /// Serial, not keyed: config revisions are rare, each event's effect is one
    /// inventory transaction, and config events must apply in order (an enable
    /// and its disable cannot reorder). Replay folds from the facet-set cursor —
    /// the rev store's diff is a pure function of the config history, so the
    /// final state converges even though intermediate replays use the revision's
    /// own config snapshot.
    ///
    /// The durable stream alone cannot resolve pending→active transitions (ADR
    /// 007 §6: a manifest arriving produces no config revision), so the machine
    /// also consumes the plugs event broadcast, which carries those transitions.
    async fn process_plug_pin_task(
        &self,
        drawer: &DrawerRepo,
        plugs_repo: Arc<crate::plugs::PlugsRepo>,
        tasks: &mut TokioKeyedScheduler<PlugPinKey, PlugPinTask, PlugPinTaskOutput>,
        subscriptions: &mut HashMap<PlugPinKey, MaterializationWake>,
        task: PlugPinTask,
    ) -> Res<()> {
        let key = task.key;
        tasks.replace(
            key,
            task.clone(),
            run_plug_pin_task(task.clone(), Arc::clone(&self.ctx), Arc::clone(&plugs_repo)),
        )?;
        let mut attempted_after_subscription = false;
        loop {
            let completion = tasks.next_completion().await?;
            match completion.result {
                Ok(PlugPinTaskOutput::Applied) => {
                    subscriptions.remove(&key);
                    return Ok(());
                }
                Ok(PlugPinTaskOutput::Deferred) => {
                    let ref_url = match task.ref_url.as_ref() {
                        Some(ref_url) => ref_url.clone(),
                        None => plugs_repo
                            .enabled_ref(match &task.event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugDisabled { plug_id } => plug_id,
                                crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => {
                                    unreachable!("config-only event cannot defer")
                                }
                            })
                            .await?
                            .ok_or_else(|| ferr!("enabled plug disappeared while materializing"))?,
                    };
                    if !subscriptions.contains_key(&key) {
                        let parsed = crate::plugs::PlugsRepo::parse_enabled_ref(&ref_url)?;
                        let branch_id = BranchId(parsed.doc_id.to_string());
                        subscriptions.insert(
                            key,
                            drawer
                                .subscribe_document_materialization(&branch_id)
                                .await?,
                        );
                    }
                    if !attempted_after_subscription {
                        attempted_after_subscription = true;
                        tasks.replace(
                            key,
                            task.clone(),
                            run_plug_pin_task(
                                task.clone(),
                                Arc::clone(&self.ctx),
                                Arc::clone(&plugs_repo),
                            ),
                        )?;
                        continue;
                    }
                    tasks.park(key, task.clone());
                    loop {
                        subscriptions
                            .get_mut(&key)
                            .expect("parked plug task has a materialization subscription")
                            .ready_changed()
                            .await?;
                        tasks.wake(
                            key,
                            run_plug_pin_task(
                                task.clone(),
                                Arc::clone(&self.ctx),
                                Arc::clone(&plugs_repo),
                            ),
                        )?;
                        let completion = tasks.next_completion().await?;
                        match completion.result {
                            Ok(PlugPinTaskOutput::Applied) => {
                                subscriptions.remove(&key);
                                return Ok(());
                            }
                            Ok(PlugPinTaskOutput::Deferred) => {
                                tasks.park(key, task.clone());
                            }
                            Err(error) => panic!("blob-pin plug task failed: {error:?}"),
                        }
                    }
                }
                Err(error) => panic!("blob-pin plug task failed: {error:?}"),
            }
        }
    }

    async fn run_plug_events_machine(
        &mut self,
        event_store: Arc<crate::plugs::PlugsConfigEventStore>,
        plugs_repo: Arc<crate::plugs::PlugsRepo>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let state = SqliteDeltaWalkerStateRepo::new(
            self.sql.read_pool.clone(),
            self.sql.write_pool.clone(),
            BLOB_PIN_PLUG_EVENTS_STATE_ID,
            "plug-events",
        )
        .await
        .map_err(|error| ferr!("initializing blob-pin plug-events walker state: {error}"))?;
        let durable = state.progress().await?.upstream_revision;
        let reader = event_store
            .open((), durable)
            .await
            .map_err(|error| ferr!("opening plugs event reader: {error}"))?;
        let mut walker = SerialDeltaWalker::open(reader, &state)
            .await
            .map_err(|error| ferr!("opening plugs event walker: {error}"))?;
        let mut events_rx = plugs_repo.subscribe_events();
        let mut tasks = TokioKeyedScheduler::new(1);
        let mut subscriptions = HashMap::new();
        loop {
            let read = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return Ok(()),
                event = events_rx.recv() => {
                    match event {
                        Ok(event) => {
                            let key = match &event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugDisabled { plug_id } => plug_pin_key(plug_id),
                                crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => continue,
                            };
                            let task = PlugPinTask { key, event, ref_url: None };
                            self.process_plug_pin_task(
                                &self.drawer_repo,
                                Arc::clone(&plugs_repo),
                                &mut tasks,
                                &mut subscriptions,
                                task,
                            )
                            .await?;
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                            tracing::warn!(missed, "plugs event broadcast lagged");
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(ferr!("plugs event broadcast closed"));
                        }
                    }
                }
                read = walker.next() => read?,
            };
            match read {
                RevisionRead::ReplayComplete { .. } => {}
                RevisionRead::Entries { revision, entries } => {
                    for entry in entries {
                        for event in &entry.events {
                            let key = match event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugDisabled { plug_id } => plug_pin_key(plug_id),
                                crate::plugs::PlugsEvent::PlugsConfigChanged { .. } => continue,
                            };
                            let ref_url = match event {
                                crate::plugs::PlugsEvent::PlugEnabled { plug_id, .. }
                                | crate::plugs::PlugsEvent::PlugUpdated { plug_id, .. } => {
                                    entry.config.enabled.get(plug_id).cloned()
                                }
                                _ => None,
                            };
                            self.process_plug_pin_task(
                                &self.drawer_repo,
                                Arc::clone(&plugs_repo),
                                &mut tasks,
                                &mut subscriptions,
                                PlugPinTask {
                                    key,
                                    event: event.clone(),
                                    ref_url,
                                },
                            )
                            .await?;
                        }
                    }
                    walker
                        .settle(revision)
                        .await
                        .map_err(|error| ferr!("settling plugs event walker: {error}"))?;
                }
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use big_sync::DeltaWalkerStateRepo;
    use daybook_types::doc::{AddDocArgs, Blob, BranchPath, DocPatch, FacetRaw, WellKnownFacet};
    use daybook_types::manifest::{PlugManifest, WflowBundleManifest};

    /// Pins recorded as BlobPin facets on an inventory doc (the actual
    /// observable: the machines apply their state into these drawer docs).
    async fn inventory_blob_pins(
        drawer: &DrawerRepo,
        inventory_doc_id: &DocId,
    ) -> Res<HashMap<String, BlobPin>> {
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch(
                inventory_doc_id,
                &daybook_types::doc::BranchPathBuf::from("main"),
                None,
            )
            .await?
        else {
            return Ok(HashMap::new());
        };
        let mut pins = HashMap::new();
        for (key, raw) in &doc.facets {
            if key.tag == WellKnownFacetTag::BlobPin.into()
                && let Ok(WellKnownFacet::BlobPin(pin)) =
                    WellKnownFacet::from_json(raw.clone(), WellKnownFacetTag::BlobPin)
            {
                pins.insert(key.id.clone(), pin);
            }
        }
        Ok(pins)
    }

    async fn wait_for_pin_presence(
        drawer: &DrawerRepo,
        inventory_doc_id: &DocId,
        hash: &str,
        should_exist: bool,
    ) -> Res<()> {
        loop {
            let pins = inventory_blob_pins(drawer, inventory_doc_id).await?;
            if pins.contains_key(hash) == should_exist {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn facet_walker_progress(sql: &SqlCtx) -> Res<u64> {
        let state = SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            BLOB_PIN_STATE_LOCAL_STATE_ID,
            "facets",
        )
        .await?;
        Ok(state.progress().await?.upstream_revision)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_doc_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let drawer = &test_context.rt.drawer_repo;
        let docs_inventory_doc_id = DocId::from(test_context.rt.rcx.docs_inventory_doc_id.clone());
        let sql = test_context.rt.rcx.sql.clone();

        let blob_id_1 = test_context
            .rt
            .blobs_repo
            .put(b"test doc blob 1 content")
            .await?;
        let blob_id_2 = test_context
            .rt
            .blobs_repo
            .put(b"test doc blob 2 content")
            .await?;
        let hash_1 = blob_id_1.to_string();
        let hash_2 = blob_id_2.to_string();

        // 1. Add doc with Blob facet having hash_1 and hash_2
        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 1234,
                            digest: "bafakedigest".to_string(),
                            inline: None,
                            urls: Some(vec![
                                format!("{}:///{hash_1}", crate::blobs::BLOB_SCHEME),
                                format!("{}:///{hash_2}", crate::blobs::BLOB_SCHEME),
                            ]),
                        })),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::Note),
                        FacetRaw::from(WellKnownFacet::Note("test note".into())),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_2, true).await?;
        let initial_progress = facet_walker_progress(&sql).await?;
        assert!(initial_progress > 0);

        let pins = inventory_blob_pins(drawer, &docs_inventory_doc_id).await?;
        assert_eq!(pins.get(&hash_1).unwrap().length_octets, 1234);
        assert_eq!(pins.get(&hash_2).unwrap().length_octets, 1234);

        // 2. Update doc to only retain hash_1
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 1234,
                            digest: "bafakedigest".to_string(),
                            inline: None,
                            urls: Some(vec![format!("{}:///{hash_1}", crate::blobs::BLOB_SCHEME)]),
                        })),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;

        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_2, false).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;
        let update_progress = facet_walker_progress(&sql).await?;
        assert!(update_progress > initial_progress);

        // Repeating the same logical value is a new source revision but must
        // leave the projected inventory unchanged.
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 1234,
                            digest: "bafakedigest".to_string(),
                            inline: None,
                            urls: Some(vec![format!("{}:///{hash_1}", crate::blobs::BLOB_SCHEME)]),
                        })),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_2, false).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;

        // A second branch owns the same pin independently. Removing it from
        // main must not unpin it until the branch is removed as well.
        let main_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, BranchPath::new("main"))
            .await?
            .ok_or_eyre("missing main branch heads")?;
        let branch_path = BranchPathBuf::from("/test/blob-pin-branch");
        test_context
            .drawer_repo
            .create_branch_at_heads_from_branch(
                &doc_id,
                &branch_path,
                BranchPath::new("main"),
                &main_heads,
                None,
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;

        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![FacetKey::from(WellKnownFacetTag::Blob)],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, true).await?;

        let branch_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch_path)
            .await?
            .ok_or_eyre("missing blob-pin branch heads")?;
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![FacetKey::from(WellKnownFacetTag::Blob)],
                    user_path: None,
                },
                BranchPath::new(branch_path.as_str()),
                Some(branch_heads),
            )
            .await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, false).await?;

        // 3. Delete doc
        test_context.drawer_repo.del(&doc_id).await?;
        wait_for_pin_presence(drawer, &docs_inventory_doc_id, &hash_1, false).await?;

        test_context.stop().await?;
        Ok(())
    }

    /// The enablement-driven plug lifecycle: authoring bakes the manifest's
    /// blob references as Blob facets on the manifest doc; enabling the plug
    /// (a config facet revision) drives the core inventory pins; disabling
    /// drops them. Manifest blob facets never flow into the docs inventory.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_plug_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let drawer = &test_context.rt.drawer_repo;
        let core_inventory_doc_id = DocId::from(test_context.rt.rcx.core_inventory_doc_id.clone());
        let docs_inventory_doc_id = DocId::from(test_context.rt.rcx.docs_inventory_doc_id.clone());
        let plugs = &test_context.rt.plugs_repo;

        let blob_id = test_context.rt.blobs_repo.put(b"test wasm bundle content").await?;
        let hash = blob_id.to_string();

        // 1. Author the plug: `add` bakes the manifest's blob references as
        //    Blob facets on the manifest doc (the static artifact carries its
        //    own blob declarations).
        let manifest = PlugManifest {
            namespace: "test".into(),
            name: "sample-plug".into(),
            version: "0.1.0".parse().unwrap(),
            title: "Sample Plug".into(),
            desc: "A test plug".into(),
            facets: default(),
            local_states: default(),
            dependencies: default(),
            routines: default(),
            wflow_bundles: [(
                "bundle1".into(),
                Arc::new(WflowBundleManifest {
                    keys: vec!["wflow1".into()],
                    component_urls: vec![format!("{}:///{hash}", crate::blobs::BLOB_SCHEME)
                        .parse()
                        .unwrap()],
                }),
            )]
            .into(),
            views: default(),
            commands: default(),
            inits: default(),
            processors: default(),
        };
        let doc_id = plugs.add(manifest).await?;

        // 2. Enable: the config revision's PlugEnabled drives the core
        //    inventory pins.
        let ref_url: url::Url = format!(
            "db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main"
        )
        .parse()?;
        plugs.enable_plug(&ref_url).await?;
        wait_for_pin_presence(drawer, &core_inventory_doc_id, &hash, true).await?;

        // 3. Manifest blobs follow enablement only: the docs inventory must
        //    not pin them (manifest-doc exclusion in the facet machine).
        let docs_pins = inventory_blob_pins(drawer, &docs_inventory_doc_id).await?;
        assert!(
            !docs_pins.contains_key(&hash),
            "manifest blob facets must not flow into the docs inventory"
        );

        // 4. Disable: the PlugDisabled event drops the plug's pins.
        plugs.disable_plug("@test/sample-plug").await?;
        wait_for_pin_presence(drawer, &core_inventory_doc_id, &hash, false).await?;

        test_context.stop().await?;
        Ok(())
    }
}
