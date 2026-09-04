use crate::interlude::*;

use daybook_types::doc::{
    BlobPin, ChangeHashSet, DocId, DocPatch, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
};
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::plugs::{PlugsRepo, PlugsRevisionSelector};
use crate::repos::RepoStopToken;
use big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use daybook_types::doc::BranchId;
use sqlx::{Row, Sqlite};

pub(crate) const BLOB_PIN_STATE_LOCAL_STATE_ID: &str = "@daybook/core/blob-pin-worker";
const BLOB_PIN_PLUG_WALKER_ID: &str = "plugs";

pub struct BlobPinWorker {
    drawer_repo: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    sql: SqlCtx,
    blobs_repo: Option<Arc<BlobsRepo>>,
    core_inventory_doc_id: DocId,
    docs_inventory_doc_id: DocId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlobPinFacetApplyOutcome {
    Applied,
    Deferred,
}

struct PreparedDocBranch {
    doc_id: DocId,
    branch_id: BranchId,
    pins: Option<HashMap<String, u64>>,
}

impl BlobPinWorker {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        plugs_repo: Arc<PlugsRepo>,
        sql: SqlCtx,
        blobs_repo: Option<Arc<BlobsRepo>>,
        core_inventory_doc_id: DocumentId,
        docs_inventory_doc_id: DocumentId,
    ) -> Res<(Arc<Self>, RepoStopToken)> {
        Self::ensure_schema(&sql).await?;

        let cancel_token = CancellationToken::new();

        let core_doc_id =
            Self::resolve_doc_id_for_branch(&drawer_repo, core_inventory_doc_id).await?;
        let docs_doc_id =
            Self::resolve_doc_id_for_branch(&drawer_repo, docs_inventory_doc_id).await?;

        let worker = Arc::new(Self {
            drawer_repo,
            plugs_repo: Arc::clone(&plugs_repo),
            sql,
            blobs_repo,
            core_inventory_doc_id: core_doc_id,
            docs_inventory_doc_id: docs_doc_id,
        });
        Ok((
            worker,
            RepoStopToken {
                cancel_token,
                worker_handle: None,
            },
        ))
    }

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
            if let Some(urls) = &blob.urls {
                for url_str in urls {
                    if let Ok(url) = url_str.parse::<url::Url>()
                        && (url.scheme() == crate::blobs::BLOB_SCHEME
                            || url.scheme() == "daybook-blob")
                    {
                        let hash = url.path().trim_start_matches('/');
                        if hash.parse::<crate::blobs::BlobId>().is_ok() {
                            current_pins.insert(hash.to_string(), blob.length_octets);
                        }
                    }
                }
            }
            if blob.digest.parse::<crate::blobs::BlobId>().is_ok() {
                current_pins.insert(blob.digest, blob.length_octets);
            }
        }
        Ok(Some(current_pins))
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

    async fn apply_facet_set_revision(
        &self,
        entries: Vec<FacetDelta>,
    ) -> Res<BlobPinFacetApplyOutcome> {
        let mut affected = BTreeMap::<(DocId, BranchId), Option<ChangeHashSet>>::new();
        for entry in entries {
            if entry.key.facet_key.tag != WellKnownFacetTag::Blob.into() {
                continue;
            }
            let key = (entry.key.document_id, entry.key.branch_id);
            let current_heads = entry.current_branch_heads;
            if let Some(previous) = affected.get(&key) {
                if previous != &current_heads {
                    return Err(ferr!("conflicting Blob branch heads in one facet revision"));
                }
            } else {
                affected.insert(key, current_heads);
            }
        }

        let mut branches = Vec::with_capacity(affected.len());
        for ((doc_id, branch_id), branch_heads) in affected {
            let pins = match branch_heads {
                Some(heads) => {
                    match Self::hydrate_blob_pins(&self.drawer_repo, &branch_id, &doc_id, heads)
                        .await?
                    {
                        Some(pins) => Some(pins),
                        None => return Ok(BlobPinFacetApplyOutcome::Deferred),
                    }
                }
                None => None,
            };
            branches.push(PreparedDocBranch {
                doc_id,
                branch_id,
                pins,
            });
        }

        self.replace_doc_branch_state(&branches).await?;
        let docs = self.desired_pins().await?;
        self.apply_inventory_diff(&self.docs_inventory_doc_id, &docs)
            .await?;
        Ok(BlobPinFacetApplyOutcome::Applied)
    }

    pub async fn reindex_plug(&self, plug_id: &str) -> Res<()> {
        let Some(manifest) = self.plugs_repo.get(plug_id).await else {
            return self.delete_plug(plug_id).await;
        };

        let mut current_pins = HashMap::<String, u64>::new();
        for bundle in manifest.wflow_bundles.values() {
            for url in &bundle.component_urls {
                if url.scheme() == crate::blobs::BLOB_SCHEME || url.scheme() == "daybook-blob" {
                    let hash = url.path().trim_start_matches('/');
                    if let Ok(blob_id) = hash.parse::<crate::blobs::BlobId>() {
                        let length_octets = if let Some(blobs) = &self.blobs_repo {
                            if let Ok(path) = blobs.get_path(blob_id).await {
                                tokio::fs::metadata(&path).await.map(|meta| meta.len()).ok()
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        if let Some(length) = length_octets {
                            current_pins.insert(hash.to_string(), length);
                        }
                    }
                }
            }
        }

        self.reconcile_plug_pins(plug_id, current_pins).await
    }

    async fn reconcile_plug_pins(
        &self,
        plug_id: &str,
        current_pins: HashMap<String, u64>,
    ) -> Res<()> {
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

    pub async fn delete_plug(&self, plug_id: &str) -> Res<()> {
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

    pub async fn list_doc_inventory_pins(&self) -> Res<HashMap<String, BlobPin>> {
        self.list_pins_from_doc_id(&self.docs_inventory_doc_id)
            .await
    }

    pub async fn list_core_inventory_pins(&self) -> Res<HashMap<String, BlobPin>> {
        self.list_pins_from_doc_id(&self.core_inventory_doc_id)
            .await
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

pub(crate) struct BlobPinConsumerStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BlobPinConsumerStopToken {
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

pub(crate) async fn spawn_blob_pin_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<PlugsRepo>,
    worker: Arc<BlobPinWorker>,
    parent_cancel_token: CancellationToken,
) -> Res<BlobPinConsumerStopToken> {
    let facet_state = SqliteDeltaWalkerStateRepo::new(
        worker.sql.read_pool.clone(),
        worker.sql.write_pool.clone(),
        BLOB_PIN_STATE_LOCAL_STATE_ID,
        "facets",
    )
    .await
    .map_err(|error| ferr!("initializing blob-pin FacetSet walker state: {error}"))?;
    let plugs_state = SqliteDeltaWalkerStateRepo::new(
        worker.sql.read_pool.clone(),
        worker.sql.write_pool.clone(),
        BLOB_PIN_STATE_LOCAL_STATE_ID,
        BLOB_PIN_PLUG_WALKER_ID,
    )
    .await
    .map_err(|error| ferr!("initializing blob-pin Plugs walker state: {error}"))?;
    let wake = worker
        .drawer_repo
        .subscribe_materialization_wake(None)
        .await?;
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let worker_handle = tokio::spawn(async move {
        run_blob_pin_consumer(
            facet_set_store,
            plugs_repo,
            worker,
            worker_cancel_token,
            facet_state,
            plugs_state,
            wake,
        )
        .await
        .unwrap();
    });
    Ok(BlobPinConsumerStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

async fn run_blob_pin_consumer(
    facet_set_store: Arc<FacetSetRevisionStore>,
    plugs_repo: Arc<PlugsRepo>,
    worker: Arc<BlobPinWorker>,
    cancel_token: CancellationToken,
    facet_state: SqliteDeltaWalkerStateRepo,
    plugs_state: SqliteDeltaWalkerStateRepo,
    mut wake: crate::drawer::MaterializationWake,
) -> Res<()> {
    let mut facet_walker = SerialDeltaWalker::open(
        facet_set_store.as_ref(),
        &facet_state,
        FacetSetSelector::Tag(WellKnownFacetTag::Blob),
    )
    .await
    .map_err(|error| ferr!("opening blob-pin FacetSet walker: {error}"))?;
    let mut plugs_walker = SerialDeltaWalker::open(
        plugs_repo.as_ref(),
        &plugs_state,
        PlugsRevisionSelector::All,
    )
    .await
    .map_err(|error| ferr!("opening blob-pin Plugs walker: {error}"))?;
    let mut deferred_facet: Option<(u64, Vec<FacetDelta>)> = None;
    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Ok(()),
            result = wake.wait(), if deferred_facet.is_some() => {
                result?;
                let (revision, entries) = deferred_facet.take().expect("deferred facet revision");
                match worker.apply_facet_set_revision(entries.clone()).await? {
                    BlobPinFacetApplyOutcome::Applied => {
                        facet_walker
                            .settle(revision)
                            .await
                            .map_err(|error| ferr!("settling blob-pin FacetSet walker: {error}"))?;
                    }
                    BlobPinFacetApplyOutcome::Deferred => {
                        deferred_facet = Some((revision, entries));
                    }
                }
            }
            read = plugs_walker.next() => {
                match read.map_err(|error| ferr!("reading blob-pin Plugs walker: {error:?}"))? {
                    RevisionRead::ReplayComplete { .. } => {}
                    RevisionRead::Entries { revision, entries } => {
                        for entry in entries {
                            if entry.value.is_some() {
                                worker.reindex_plug(&entry.key).await?;
                            } else {
                                worker.delete_plug(&entry.key).await?;
                            }
                        }
                        plugs_walker
                            .settle(revision)
                            .await
                            .map_err(|error| ferr!("settling blob-pin Plugs walker: {error}"))?;
                    }
                }
            }
            read = facet_walker.next(), if deferred_facet.is_none() => {
                match read.map_err(|error| ferr!("reading blob-pin FacetSet walker: {error:?}"))? {
                    RevisionRead::ReplayComplete { .. } => {}
                    RevisionRead::Entries { revision, entries } => {
                        match worker.apply_facet_set_revision(entries.clone()).await? {
                            BlobPinFacetApplyOutcome::Applied => {
                                facet_walker
                                    .settle(revision)
                                    .await
                                    .map_err(|error| ferr!("settling blob-pin FacetSet walker: {error}"))?;
                            }
                            BlobPinFacetApplyOutcome::Deferred => {
                                deferred_facet = Some((revision, entries));
                            }
                        }
                    }
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

    async fn wait_for_pin_presence(
        worker: &BlobPinWorker,
        is_core: bool,
        hash: &str,
        should_exist: bool,
    ) -> Res<()> {
        loop {
            let pins = if is_core {
                worker.list_core_inventory_pins().await?
            } else {
                worker.list_doc_inventory_pins().await?
            };
            if pins.contains_key(hash) == should_exist {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn facet_walker_progress(worker: &BlobPinWorker) -> Res<u64> {
        let state = SqliteDeltaWalkerStateRepo::new(
            worker.sql.read_pool.clone(),
            worker.sql.write_pool.clone(),
            BLOB_PIN_STATE_LOCAL_STATE_ID,
            "facets",
        )
        .await?;
        Ok(state.progress().await?.upstream_revision)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_doc_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let worker = Arc::clone(&test_context.rt.blob_pin_worker);

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

        wait_for_pin_presence(&worker, false, &hash_1, true).await?;
        wait_for_pin_presence(&worker, false, &hash_2, true).await?;
        let initial_progress = facet_walker_progress(&worker).await?;
        assert!(initial_progress > 0);

        let pins = worker.list_doc_inventory_pins().await?;
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

        wait_for_pin_presence(&worker, false, &hash_2, false).await?;
        wait_for_pin_presence(&worker, false, &hash_1, true).await?;
        let update_progress = facet_walker_progress(&worker).await?;
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
        wait_for_pin_presence(&worker, false, &hash_2, false).await?;
        wait_for_pin_presence(&worker, false, &hash_1, true).await?;

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
        wait_for_pin_presence(&worker, false, &hash_1, true).await?;

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
        wait_for_pin_presence(&worker, false, &hash_1, true).await?;

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
        wait_for_pin_presence(&worker, false, &hash_1, false).await?;

        // 3. Delete doc
        test_context.drawer_repo.del(&doc_id).await?;
        wait_for_pin_presence(&worker, false, &hash_1, false).await?;

        test_context.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pin_worker_plug_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let worker = Arc::clone(&test_context.rt.blob_pin_worker);
        let plugs = &test_context.rt.plugs_repo;

        let blob_id_plug = test_context
            .rt
            .blobs_repo
            .put(b"test wasm bundle content")
            .await?;
        let hash_plug = blob_id_plug.to_string();

        let mut manifest = PlugManifest {
            namespace: "test".into(),
            name: "sample-plug".into(),
            version: "0.1.0".parse().unwrap(),
            title: "Sample Plug".into(),
            desc: "A test plug".into(),
            local_states: default(),
            dependencies: default(),
            views: default(),
            routines: default(),
            wflow_bundles: [(
                "bundle1".into(),
                Arc::new(WflowBundleManifest {
                    keys: vec!["wflow1".into()],
                    component_urls: vec![
                        format!("{}:///{hash_plug}", crate::blobs::BLOB_SCHEME)
                            .parse()
                            .unwrap(),
                    ],
                }),
            )]
            .into(),
            commands: default(),
            inits: default(),
            processors: default(),
            facets: default(),
        };

        // 1. Add plug (authoring: known but not enabled — no pin yet).
        let doc_id = plugs.add(manifest.clone()).await?;

        // 2. Enable at the manifest doc (ADR §3 full ref, pinned at current
        // heads). Enablement is what drives the pin worker's reindex.
        let ref_url: url::Url =
            format!("db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main")
                .parse()?;
        plugs.enable_plug(&ref_url).await?;
        wait_for_pin_presence(&worker, true, &hash_plug, true).await?;

        // 3. Author v0.2 without the bundle — `add` writes a NEW manifest doc,
        //    so the old doc's pin stays until the plug is re-pinned to the new
        //    doc (known-but-disabled manifest changes are invisible, ADR §7).
        manifest.wflow_bundles.clear();
        manifest.version = "0.2.0".parse().unwrap();
        let doc_id_v2 = plugs.add(manifest).await?;

        // 4. Re-pin to the new doc (same plug id, ref differs → EnabledPlugUpdated)
        //    → the pin worker reindexes with the new manifest and unpins.
        let ref_url_v2: url::Url =
            format!("db+facet:///{doc_id_v2}/org.example.daybook.plugManifest/main?branch=main")
                .parse()?;
        plugs.enable_plug(&ref_url_v2).await?;
        wait_for_pin_presence(&worker, true, &hash_plug, false).await?;

        test_context.stop().await?;
        Ok(())
    }
}
