use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use daybook_types::doc::{
    BlobPin, BranchPathBuf, ChangeHashSet, DocId, DocPatch, FacetKey, FacetRaw, WellKnownFacet,
    WellKnownFacetTag,
};
use eyre::Context;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::interlude::*;
use crate::plugs::PlugsRepo;
use crate::repos::RepoStopToken;

const ERROR_ACTOR: &str = "blob pin worker actor is shut down";

#[derive(Debug)]
pub enum BlobPinWorkItem {
    DocUpsert {
        doc_id: DocId,
        branch_path: BranchPathBuf,
        heads: ChangeHashSet,
    },
    DocDelete {
        doc_id: DocId,
    },
    DocDeleteBranchesNotIn {
        doc_id: DocId,
        branch_paths: Vec<BranchPathBuf>,
    },
    PlugUpsert {
        plug_id: String,
    },
    PlugDelete {
        plug_id: String,
    },
}

pub struct BlobPinWorker {
    drawer_repo: Arc<DrawerRepo>,
    plugs_repo: Arc<PlugsRepo>,
    sql: SqlCtx,
    blobs_repo: Option<Arc<BlobsRepo>>,
    core_inventory_doc_id: DocId,
    docs_inventory_doc_id: DocId,
    work_tx: tokio::sync::mpsc::UnboundedSender<BlobPinWorkItem>,
    _cancel_token: CancellationToken,
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

        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel::<BlobPinWorkItem>();
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
            work_tx,
            _cancel_token: cancel_token.clone(),
        });

        let loop_cancel = cancel_token.clone();
        let loop_worker = Arc::clone(&worker);
        let worker_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = loop_cancel.cancelled() => break,
                    item = work_rx.recv() => {
                        let Some(item) = item else {
                            break;
                        };
                        if let Err(err) = loop_worker.handle_work_item(item).await {
                            error!(?err, "error in blob pin worker handle_work_item");
                        }
                    }
                }
            }
        });

        Ok((
            worker,
            RepoStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
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
                branch_path TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL,
                PRIMARY KEY (doc_id, branch_path, blob_hash)
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
        Ok(())
    }

    pub fn enqueue_upsert_doc(
        &self,
        doc_id: DocId,
        branch_path: BranchPathBuf,
        heads: ChangeHashSet,
    ) -> Res<()> {
        self.work_tx
            .send(BlobPinWorkItem::DocUpsert {
                doc_id,
                branch_path,
                heads,
            })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete_doc(&self, doc_id: DocId) -> Res<()> {
        self.work_tx
            .send(BlobPinWorkItem::DocDelete { doc_id })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete_doc_branches_not_in(
        &self,
        doc_id: DocId,
        branch_paths: Vec<BranchPathBuf>,
    ) -> Res<()> {
        self.work_tx
            .send(BlobPinWorkItem::DocDeleteBranchesNotIn {
                doc_id,
                branch_paths,
            })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_upsert_plug(&self, plug_id: String) -> Res<()> {
        self.work_tx
            .send(BlobPinWorkItem::PlugUpsert { plug_id })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete_plug(&self, plug_id: String) -> Res<()> {
        self.work_tx
            .send(BlobPinWorkItem::PlugDelete { plug_id })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn triage_listener(
        self: &Arc<Self>,
    ) -> Box<dyn crate::rt::switch::SwitchSink + Send + Sync> {
        Box::new(BlobPinTriageListener {
            drawer_repo: Arc::clone(&self.drawer_repo),
            worker: Arc::clone(self),
        })
    }

    async fn handle_work_item(&self, item: BlobPinWorkItem) -> Res<()> {
        match item {
            BlobPinWorkItem::DocUpsert {
                doc_id,
                branch_path,
                heads,
            } => {
                self.reindex_doc(&doc_id, &branch_path, &heads).await?;
            }
            BlobPinWorkItem::DocDelete { doc_id } => {
                self.delete_doc(&doc_id).await?;
            }
            BlobPinWorkItem::DocDeleteBranchesNotIn {
                doc_id,
                branch_paths,
            } => {
                self.delete_doc_branches_not_in(&doc_id, &branch_paths)
                    .await?;
            }
            BlobPinWorkItem::PlugUpsert { plug_id } => {
                self.reindex_plug(&plug_id).await?;
            }
            BlobPinWorkItem::PlugDelete { plug_id } => {
                self.delete_plug(&plug_id).await?;
            }
        }
        Ok(())
    }

    pub async fn reindex_doc(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
        heads: &ChangeHashSet,
    ) -> Res<()> {
        let Some(facet_keys) = self
            .drawer_repo
            .facet_keys_at_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            return self.delete_doc_branch(doc_id, branch_path).await;
        };

        let selected_blob_keys: Vec<FacetKey> = facet_keys
            .into_iter()
            .filter(|facet_key| facet_key.tag == WellKnownFacetTag::Blob.into())
            .collect();

        if selected_blob_keys.is_empty() {
            return self.delete_doc_branch(doc_id, branch_path).await;
        }

        let Some((facets, _)) = self
            .drawer_repo
            .get_at_branch_heads_with_facets_arc(
                doc_id,
                branch_path,
                heads,
                Some(selected_blob_keys),
            )
            .await?
        else {
            return Ok(());
        };

        let mut current_pins = HashMap::<String, u64>::new();
        for (facet_key, facet_raw) in facets {
            if facet_key.tag != WellKnownFacetTag::Blob.into() {
                continue;
            }
            let blob = match WellKnownFacet::from_json(
                (*facet_raw).clone(),
                WellKnownFacetTag::Blob,
            ) {
                Ok(WellKnownFacet::Blob(blob_facet)) => blob_facet,
                Err(err) => {
                    warn!(%doc_id, %branch_path, ?err, "failed to parse blob facet in BlobPinWorker");
                    continue;
                }
                _ => continue,
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
                current_pins.insert(blob.digest.clone(), blob.length_octets);
            }
        }

        self.reconcile_doc_pins(doc_id, branch_path, current_pins)
            .await
    }

    async fn reconcile_doc_pins(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
        current_pins: HashMap<String, u64>,
    ) -> Res<()> {
        let prev_hashes: HashSet<String> = sqlx::query_scalar(
            r#"
            SELECT blob_hash
            FROM blob_pin_doc_state
            WHERE doc_id = ?1 AND branch_path = ?2
            "#,
        )
        .bind(doc_id)
        .bind(branch_path.as_str())
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

        // Update local state table
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_doc_state WHERE doc_id = ?1 AND branch_path = ?2")
            .bind(doc_id)
            .bind(branch_path.as_str())
            .execute(&mut *tx)
            .await?;

        for (hash, length_octets) in &current_pins {
            sqlx::query(
                r#"
                INSERT INTO blob_pin_doc_state (doc_id, branch_path, blob_hash, length_octets)
                VALUES (?1, ?2, ?3, ?4)
                "#,
            )
            .bind(doc_id)
            .bind(branch_path.as_str())
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
                    r#"
                    SELECT COUNT(*)
                    FROM blob_pin_doc_state
                    WHERE blob_hash = ?1
                    "#,
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
                id: self.docs_inventory_doc_id.clone(),
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

    pub async fn delete_doc_branch(&self, doc_id: &DocId, branch_path: &BranchPathBuf) -> Res<()> {
        let prev_hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT blob_hash
            FROM blob_pin_doc_state
            WHERE doc_id = ?1 AND branch_path = ?2
            "#,
        )
        .bind(doc_id)
        .bind(branch_path.as_str())
        .fetch_all(&self.sql.write_pool)
        .await?;

        if prev_hashes.is_empty() {
            return Ok(());
        }

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_doc_state WHERE doc_id = ?1 AND branch_path = ?2")
            .bind(doc_id)
            .bind(branch_path.as_str())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut pins_to_remove = Vec::new();
        for hash in &prev_hashes {
            let remaining_count: i64 = sqlx::query_scalar(
                r#"
                SELECT COUNT(*)
                FROM blob_pin_doc_state
                WHERE blob_hash = ?1
                "#,
            )
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
                id: self.docs_inventory_doc_id.clone(),
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

    pub async fn delete_doc_branches_not_in(
        &self,
        doc_id: &DocId,
        branch_paths: &[BranchPathBuf],
    ) -> Res<()> {
        let existing_branches: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT branch_path FROM blob_pin_doc_state WHERE doc_id = ?1",
        )
        .bind(doc_id)
        .fetch_all(&self.sql.write_pool)
        .await?;

        let keep_set: HashSet<&str> = branch_paths.iter().map(|path| path.as_str()).collect();
        for branch in existing_branches {
            if !keep_set.contains(branch.as_str()) {
                self.delete_doc_branch(doc_id, &BranchPathBuf::from(branch))
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn delete_doc(&self, doc_id: &DocId) -> Res<()> {
        let prev_hashes: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT blob_hash FROM blob_pin_doc_state WHERE doc_id = ?1",
        )
        .bind(doc_id)
        .fetch_all(&self.sql.write_pool)
        .await?;

        if prev_hashes.is_empty() {
            return Ok(());
        }

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM blob_pin_doc_state WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut pins_to_remove = Vec::new();
        for hash in &prev_hashes {
            let remaining_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM blob_pin_doc_state WHERE blob_hash = ?1")
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
                id: self.docs_inventory_doc_id.clone(),
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

struct BlobPinTriageListener {
    drawer_repo: Arc<DrawerRepo>,
    worker: Arc<BlobPinWorker>,
}

#[async_trait]
impl crate::rt::switch::SwitchSink for BlobPinTriageListener {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_doc: true,
            consume_drawer: true,
            consume_plugs: true,
            consume_dispatch: false,
            consume_config: false,
            drawer_predicate: Some(daybook_types::manifest::DocPredicateClause::HasTag(
                WellKnownFacetTag::Blob.into(),
            )),
        }
    }

    async fn on_event(
        &mut self,
        event: &crate::rt::switch::SwitchEvent,
        _ctx: &crate::rt::switch::SwitchSinkCtx<'_>,
    ) -> Res<crate::rt::switch::SwitchSinkOutcome> {
        let outcome = crate::rt::switch::SwitchSinkOutcome::default();
        match event {
            crate::rt::switch::SwitchEvent::Doc(event) => {
                let branch_path = BranchPathBuf::from(event.branch_name.as_str());
                self.worker
                    .handle_work_item(BlobPinWorkItem::DocUpsert {
                        doc_id: event.doc_id.clone(),
                        branch_path,
                        heads: event.new_heads.clone(),
                    })
                    .await?;
            }
            crate::rt::switch::SwitchEvent::Drawer(event) => match &**event {
                crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                    self.worker
                        .handle_work_item(BlobPinWorkItem::DocDelete { doc_id: id.clone() })
                        .await?;
                }
                crate::drawer::DrawerEvent::DocAdded { id, entry, .. } => {
                    for (branch_name, heads) in &entry.branches {
                        let branch_path = BranchPathBuf::from(branch_name.as_str());
                        let Some(_keys) = self
                            .drawer_repo
                            .get_facet_keys_if_latest(id, &branch_path, heads)
                            .await?
                        else {
                            continue;
                        };
                        self.worker
                            .handle_work_item(BlobPinWorkItem::DocUpsert {
                                doc_id: id.clone(),
                                branch_path,
                                heads: heads.clone(),
                            })
                            .await?;
                    }
                }
            },
            crate::rt::switch::SwitchEvent::Plugs(event) => match &**event {
                crate::plugs::PlugsEvent::PlugEnabled { id, .. }
                | crate::plugs::PlugsEvent::EnabledPlugUpdated { id, .. } => {
                    self.worker
                        .handle_work_item(BlobPinWorkItem::PlugUpsert {
                            plug_id: id.to_string(),
                        })
                        .await?;
                }
                crate::plugs::PlugsEvent::PlugDisabled { id, .. } => {
                    self.worker
                        .handle_work_item(BlobPinWorkItem::PlugDelete {
                            plug_id: id.to_string(),
                        })
                        .await?;
                }
                crate::plugs::PlugsEvent::PlugsConfigChanged { .. }
                | crate::plugs::PlugsEvent::ManifestRejected { .. } => {}
            },
            crate::rt::switch::SwitchEvent::Dispatch(_)
            | crate::rt::switch::SwitchEvent::Config(_) => {}
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
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
        let ref_url: url::Url = format!(
            "db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main"
        )
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
        let ref_url_v2: url::Url = format!(
            "db+facet:///{doc_id_v2}/org.example.daybook.plugManifest/main?branch=main"
        )
        .parse()?;
        plugs.enable_plug(&ref_url_v2).await?;
        wait_for_pin_presence(&worker, true, &hash_plug, false).await?;

        test_context.stop().await?;
        Ok(())
    }
}
