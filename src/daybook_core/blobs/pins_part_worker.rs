use crate::drawer::DrawerRepo;
use crate::interlude::*;
use crate::repos::Repo;
use big_repo::SharedPartStore;
use daybook_types::doc::{BranchPathBuf, ChangeHashSet, DocId, FacetKey, WellKnownFacetTag};
use sqlx::QueryBuilder;
use std::collections::{HashMap, HashSet};
use tokio_util::sync::CancellationToken;

pub const DOC_BLOB_PINS_LOCAL_STATE_ID: &str = "@daybook/wip/doc-blob-pins-index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobPinsPartEvent {
    Updated { doc_id: DocId, part_id: PartId },
    Deleted { doc_id: DocId, part_id: PartId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexDocOutcome {
    Present,
    Evicted,
}

#[derive(Debug, Clone)]
enum BlobPinsPartWorkItem {
    Upsert {
        doc_id: DocId,
        branch_path: BranchPathBuf,
        heads: ChangeHashSet,
    },
    DeleteDoc {
        doc_id: DocId,
    },
    DeleteDocBranchesNotIn {
        doc_id: DocId,
        branch_paths: Vec<BranchPathBuf>,
    },
}

pub struct BlobPinsPartWorker {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub cancel_token: CancellationToken,
    drawer_repo: Arc<DrawerRepo>,
    part_store: SharedPartStore,
    work_tx: tokio::sync::mpsc::UnboundedSender<BlobPinsPartWorkItem>,
    sql: SqlCtx,
}

impl Repo for BlobPinsPartWorker {
    type Event = BlobPinsPartEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl BlobPinsPartWorker {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        part_store: SharedPartStore,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(DOC_BLOB_PINS_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&sql).await?;
        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let worker = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo,
            part_store,
            work_tx,
            sql,
        });

        let worker_handle = tokio::spawn({
            let worker = Arc::clone(&worker);
            let cancel_token = cancel_token.clone();
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => break,
                        item = work_rx.recv() => {
                            let Some(item) = item else {
                                break;
                            };
                            if let Err(err) = worker.handle_worker_item(item).await {
                                tracing::error!(?err, "error in blob pins part worker handle_worker_item");
                            }
                        }
                    }
                }
            }
        });

        Ok((
            worker,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn init_schema(sql: &SqlCtx) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_blob_pins (
                doc_id TEXT NOT NULL,
                branch_path TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL DEFAULT 0,
                origin_heads TEXT NOT NULL,
                PRIMARY KEY(doc_id, branch_path, blob_hash)
            ) STRICT
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_doc_branch ON doc_blob_pins(doc_id, branch_path)",
        )
        .execute(&sql.write_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_blob_hash ON doc_blob_pins(blob_hash)",
        )
        .execute(&sql.write_pool)
        .await?;

        Ok(())
    }

    async fn handle_worker_item(&self, item: BlobPinsPartWorkItem) -> Res<()> {
        match item {
            BlobPinsPartWorkItem::Upsert {
                doc_id,
                branch_path,
                heads,
            } => {
                let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(&doc_id);
                match self.reindex_doc(&doc_id, &branch_path, &heads).await? {
                    ReindexDocOutcome::Present => {
                        self.registry
                            .notify([BlobPinsPartEvent::Updated { doc_id, part_id }]);
                    }
                    ReindexDocOutcome::Evicted => {
                        self.registry
                            .notify([BlobPinsPartEvent::Deleted { doc_id, part_id }]);
                    }
                }
            }
            BlobPinsPartWorkItem::DeleteDoc { doc_id } => {
                let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(&doc_id);
                self.delete_doc(&doc_id).await?;
                self.registry
                    .notify([BlobPinsPartEvent::Deleted { doc_id, part_id }]);
            }
            BlobPinsPartWorkItem::DeleteDocBranchesNotIn {
                doc_id,
                branch_paths,
            } => {
                let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(&doc_id);
                match self
                    .delete_doc_branches_not_in(&doc_id, &branch_paths)
                    .await?
                {
                    ReindexDocOutcome::Present => {
                        self.registry
                            .notify([BlobPinsPartEvent::Updated { doc_id, part_id }]);
                    }
                    ReindexDocOutcome::Evicted => {
                        self.registry
                            .notify([BlobPinsPartEvent::Deleted { doc_id, part_id }]);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn reindex_doc(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
        heads: &ChangeHashSet,
    ) -> Res<ReindexDocOutcome> {
        let Some(facet_keys) = self
            .drawer_repo
            .facet_keys_at_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            return self.delete_doc_branch(doc_id, branch_path).await;
        };
        let selected_pin_keys: Vec<FacetKey> = facet_keys
            .into_iter()
            .filter(|facet_key| facet_key.tag == WellKnownFacetTag::BlobPin.into())
            .collect();
        if selected_pin_keys.is_empty() {
            return self.delete_doc_branch(doc_id, branch_path).await;
        }
        let Some((facets, _)) = self
            .drawer_repo
            .get_at_branch_heads_with_facets_arc(
                doc_id,
                branch_path,
                heads,
                Some(selected_pin_keys),
            )
            .await?
        else {
            tracing::debug!(%doc_id, %branch_path, "doc heads changed during blob pin index update");
            return self.doc_presence_outcome(doc_id).await;
        };

        let mut pins = HashMap::<Arc<str>, u64>::new();
        for (facet_key, facet_raw) in facets {
            if facet_key.tag != WellKnownFacetTag::BlobPin.into() {
                continue;
            }
            let pin: daybook_types::doc::BlobPin =
                match serde_json::from_value((*facet_raw).clone()) {
                    Ok(pin) => pin,
                    Err(err) => {
                        warn!(
                            %doc_id,
                            %branch_path,
                            ?err,
                            "failed to parse blob pin facet while indexing; evicting stale pin refs"
                        );
                        self.delete_doc_branch(doc_id, branch_path).await?;
                        return Ok(ReindexDocOutcome::Evicted);
                    }
                };
            if facet_key.id.parse::<crate::blobs::BlobId>().is_ok() {
                let hash: Arc<str> = facet_key.id.as_str().into();
                pins.insert(hash, pin.length_octets);
            } else {
                warn!(
                    %doc_id,
                    %branch_path,
                    facet_id = %facet_key.id,
                    "invalid blob pin facet id; evicting stale pin refs"
                );
                self.delete_doc_branch(doc_id, branch_path).await?;
                return Ok(ReindexDocOutcome::Evicted);
            }
        }

        self.reindex_doc_pins(doc_id, branch_path, heads, &pins)
            .await
    }

    async fn reindex_doc_pins(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
        heads: &ChangeHashSet,
        pins: &HashMap<Arc<str>, u64>,
    ) -> Res<ReindexDocOutcome> {
        let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(doc_id);
        self.part_store.ensure_part(part_id).await?;

        let prev_hashes: HashSet<Arc<str>> = self
            .list_hashes_for_doc_branch(doc_id, branch_path)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        let next_hashes: HashSet<Arc<str>> = pins.keys().cloned().collect();

        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM doc_blob_pins WHERE doc_id = ?1 AND branch_path = ?2")
            .bind(doc_id)
            .bind(branch_path.as_str())
            .execute(&mut *tx)
            .await?;

        if !pins.is_empty() {
            let serialized_heads =
                serde_json::to_string(&am_utils_rs::serialize_commit_heads(&heads.0))
                    .expect(ERROR_JSON);

            let mut rows: Vec<(&str, i64)> = Vec::with_capacity(pins.len());
            for (hash, length_octets) in pins {
                let length_octets_i64 = i64::try_from(*length_octets).map_err(|_| {
                    eyre::eyre!(
                        "blob pin length octets exceeds sqlite INTEGER range: doc_id={} branch={} length_octets={}",
                        doc_id,
                        branch_path.as_str(),
                        length_octets
                    )
                })?;
                rows.push((&hash[..], length_octets_i64));
            }

            let mut query_builder = QueryBuilder::new(
                "INSERT INTO doc_blob_pins (doc_id, branch_path, blob_hash, length_octets, origin_heads) ",
            );
            query_builder.push_values(rows.iter(), |mut row, (hash, length_octets_i64)| {
                row.push_bind(doc_id)
                    .push_bind(branch_path.as_str())
                    .push_bind(hash)
                    .push_bind(*length_octets_i64)
                    .push_bind(&serialized_heads);
            });
            query_builder.push(
                " ON CONFLICT(doc_id, branch_path, blob_hash) DO UPDATE SET origin_heads = excluded.origin_heads, length_octets = excluded.length_octets",
            );
            query_builder.build().execute(&mut *tx).await?;
        }
        tx.commit().await?;

        for (hash, length_octets) in pins {
            let obj_id = crate::blobs::blob_id_from_hash(hash);
            let payload = serde_json::json!({ "lengthOctets": length_octets });
            self.part_store.set_obj_payload(obj_id, payload).await?;
            if !prev_hashes.contains(hash) {
                self.part_store
                    .add_obj_to_parts(obj_id, vec![part_id])
                    .await?;
            }
        }

        for hash in prev_hashes.difference(&next_hashes) {
            if self
                .hash_is_unused_by_other_branches(hash, doc_id, branch_path)
                .await?
            {
                let obj_id = crate::blobs::blob_id_from_hash(hash);
                self.part_store
                    .remove_obj_from_part(obj_id, part_id)
                    .await?;
            }
        }

        self.doc_presence_outcome(doc_id).await
    }

    pub async fn delete_doc(&self, doc_id: &DocId) -> Res<()> {
        let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(doc_id);
        let prev_hashes = self.list_hashes_for_doc(doc_id).await?;
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM doc_blob_pins WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        for hash in &prev_hashes {
            let obj_id = crate::blobs::blob_id_from_hash(hash);
            self.part_store
                .remove_obj_from_part(obj_id, part_id)
                .await?;
        }
        Ok(())
    }

    async fn delete_doc_branch(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<ReindexDocOutcome> {
        let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(doc_id);
        let prev_hashes = self.list_hashes_for_doc_branch(doc_id, branch_path).await?;
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM doc_blob_pins WHERE doc_id = ?1 AND branch_path = ?2")
            .bind(doc_id)
            .bind(branch_path.as_str())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        for hash in &prev_hashes {
            if self
                .hash_is_unused_by_other_branches(hash, doc_id, branch_path)
                .await?
            {
                let obj_id = crate::blobs::blob_id_from_hash(hash);
                self.part_store
                    .remove_obj_from_part(obj_id, part_id)
                    .await?;
            }
        }
        self.doc_presence_outcome(doc_id).await
    }

    async fn delete_doc_branches_not_in(
        &self,
        doc_id: &DocId,
        branch_paths: &[BranchPathBuf],
    ) -> Res<ReindexDocOutcome> {
        let retained: HashSet<&str> = branch_paths.iter().map(|path| path.as_str()).collect();
        let current: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT branch_path FROM doc_blob_pins WHERE doc_id = ?1")
                .bind(doc_id)
                .fetch_all(&self.sql.read_pool)
                .await?;
        for branch in current {
            if !retained.contains(branch.as_str()) {
                self.delete_doc_branch(doc_id, &BranchPathBuf::from(branch.as_str()))
                    .await?;
            }
        }
        self.doc_presence_outcome(doc_id).await
    }

    async fn hash_is_unused_by_other_branches(
        &self,
        hash: &str,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<bool> {
        let exists_other: i64 = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM doc_blob_pins
                WHERE blob_hash = ?1
                  AND doc_id = ?2
                  AND branch_path != ?3
            )
            "#,
        )
        .bind(hash)
        .bind(doc_id)
        .bind(branch_path.as_str())
        .fetch_one(&self.sql.read_pool)
        .await?;
        Ok(exists_other == 0)
    }

    pub async fn list_hashes_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT blob_hash
            FROM doc_blob_pins
            WHERE doc_id = ?1
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(hashes)
    }

    pub async fn list_hashes_for_doc_branch(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT blob_hash
            FROM doc_blob_pins
            WHERE doc_id = ?1
              AND branch_path = ?2
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .bind(branch_path.as_str())
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(hashes)
    }

    async fn doc_presence_outcome(&self, doc_id: &DocId) -> Res<ReindexDocOutcome> {
        let exists: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM doc_blob_pins WHERE doc_id = ?1)")
                .bind(doc_id)
                .fetch_one(&self.sql.read_pool)
                .await?;
        if exists == 0 {
            Ok(ReindexDocOutcome::Evicted)
        } else {
            Ok(ReindexDocOutcome::Present)
        }
    }

    pub fn enqueue_upsert(
        &self,
        doc_id: DocId,
        branch_path: BranchPathBuf,
        heads: ChangeHashSet,
    ) -> Res<()> {
        self.work_tx
            .send(BlobPinsPartWorkItem::Upsert {
                doc_id,
                branch_path,
                heads,
            })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete(&self, doc_id: DocId) -> Res<()> {
        self.work_tx
            .send(BlobPinsPartWorkItem::DeleteDoc { doc_id })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete_branches_not_in(
        &self,
        doc_id: DocId,
        branch_paths: Vec<BranchPathBuf>,
    ) -> Res<()> {
        self.work_tx
            .send(BlobPinsPartWorkItem::DeleteDocBranchesNotIn {
                doc_id,
                branch_paths,
            })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn triage_listener(
        self: &Arc<Self>,
    ) -> Box<dyn crate::rt::switch::SwitchSink + Send + Sync> {
        Box::new(BlobPinsPartTriageListener {
            drawer_repo: Arc::clone(&self.drawer_repo),
            worker: Arc::clone(self),
        })
    }
}

struct BlobPinsPartTriageListener {
    drawer_repo: Arc<DrawerRepo>,
    worker: Arc<BlobPinsPartWorker>,
}

#[async_trait]
impl crate::rt::switch::SwitchSink for BlobPinsPartTriageListener {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_doc: true,
            consume_drawer: true,
            consume_plugs: false,
            consume_dispatch: false,
            consume_config: false,
            drawer_predicate: Some(daybook_types::manifest::DocPredicateClause::HasTag(
                WellKnownFacetTag::BlobPin.into(),
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
                    .handle_worker_item(BlobPinsPartWorkItem::Upsert {
                        doc_id: event.doc_id.clone(),
                        branch_path,
                        heads: event.new_heads.clone(),
                    })
                    .await?;
            }
            crate::rt::switch::SwitchEvent::Drawer(event) => match &**event {
                crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                    self.worker
                        .handle_worker_item(BlobPinsPartWorkItem::DeleteDoc { doc_id: id.clone() })
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
                            .handle_worker_item(BlobPinsPartWorkItem::Upsert {
                                doc_id: id.clone(),
                                branch_path,
                                heads: heads.clone(),
                            })
                            .await?;
                    }
                }
            },
            _ => {}
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use daybook_types::doc::{AddDocArgs, BlobPin, BranchPath, DocPatch, FacetRaw, WellKnownFacet};

    async fn wait_for_partition_member_count(
        part_store: &SharedPartStore,
        partition_id: PartId,
        expected: u64,
    ) -> Res<()> {
        loop {
            let count = part_store.member_count(partition_id).await?;
            if count == expected {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blob_pins_part_worker_lifecycle() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let worker = Arc::clone(&test_context.rt.blob_pins_part_worker);
        let blob_part_store = &test_context.rt.rcx.blob_part_store;

        let blob_id_1 = crate::blobs::BlobId::random();
        let blob_id_2 = crate::blobs::BlobId::random();
        let hash_1 = blob_id_1.to_string();
        let hash_2 = blob_id_2.to_string();

        let key_pin_1 = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_1.clone(),
        };
        let key_pin_2 = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_2.clone(),
        };

        // 1. Add document with two BlobPin facets
        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        key_pin_1.clone(),
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 150 })),
                    ),
                    (
                        key_pin_2.clone(),
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 250 })),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(&doc_id);

        wait_for_partition_member_count(blob_part_store, part_id, 2).await?;
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_1))
                .await?,
            vec![part_id]
        );
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_2))
                .await?,
            vec![part_id]
        );

        let hashes = worker.list_hashes_for_doc(&doc_id).await?;
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&hash_1));
        assert!(hashes.contains(&hash_2));

        // 2. Update document: remove pin 2
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![key_pin_2],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;

        wait_for_partition_member_count(blob_part_store, part_id, 1).await?;
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_1))
                .await?,
            vec![part_id]
        );
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_2))
                .await?,
            Vec::<PartId>::new()
        );

        let hashes_after_update = worker.list_hashes_for_doc(&doc_id).await?;
        assert_eq!(hashes_after_update, vec![hash_1.clone()]);

        // 3. Delete document: removes remaining pin 1
        test_context.drawer_repo.del(&doc_id).await?;
        wait_for_partition_member_count(blob_part_store, part_id, 0).await?;
        assert_eq!(
            blob_part_store
                .obj_parts(crate::blobs::blob_id_from_hash(&hash_1))
                .await?,
            Vec::<PartId>::new()
        );
        assert!(worker.list_hashes_for_doc(&doc_id).await?.is_empty());

        test_context.stop().await?;
        Ok(())
    }
}
