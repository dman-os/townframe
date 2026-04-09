use crate::blobs::BLOB_SCHEME;
use crate::blobs::{BlobScope, BlobsRepo};
use crate::drawer::DrawerRepo;
use crate::interlude::*;
use crate::repos::Repo;
use daybook_types::doc::{
    BranchPath, ChangeHashSet, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag,
};
use sqlx::QueryBuilder;
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;

const DOC_BLOBS_LOCAL_STATE_ID: &str = "@daybook/wip/doc-blobs-index";

#[derive(Debug, Clone)]
pub struct DocBlobMembership {
    pub doc_id: DocId,
    pub blob_hash: String,
    pub length_octets: u64,
    pub origin_heads: ChangeHashSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocBlobRef {
    pub blob_hash: String,
    pub length_octets: u64,
}

#[derive(Debug, Clone)]
pub enum DocBlobsIndexEvent {
    Updated { doc_id: DocId },
    Deleted { doc_id: DocId },
}

pub struct DocBlobsIndexRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub cancel_token: CancellationToken,
    drawer_repo: Arc<DrawerRepo>,
    blobs_repo: Arc<BlobsRepo>,
    work_tx: tokio::sync::mpsc::UnboundedSender<DocBlobsIndexWorkItem>,
    db_pool: SqlitePool,
}

impl Repo for DocBlobsIndexRepo {
    type Event = DocBlobsIndexEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

pub struct DocBlobsIndexStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DocBlobsIndexStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            utils_rs::wait_on_handle_with_timeout(handle, Duration::from_secs(2)).await?;
        }
        Ok(())
    }
}

impl DocBlobsIndexRepo {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        blobs_repo: Arc<BlobsRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, DocBlobsIndexStopToken)> {
        let (_sqlite_file_path, db_pool) = sqlite_local_state_repo
            .ensure_sqlite_pool(DOC_BLOBS_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&db_pool).await?;
        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            blobs_repo,
            work_tx,
            db_pool,
        });

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
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
                            repo.handle_worker_item(item).await.unwrap_or_log();
                        }
                    }
                }
            }
        });

        Ok((
            repo,
            DocBlobsIndexStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn init_schema(db_pool: &SqlitePool) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_blob_refs (
                doc_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL DEFAULT 0,
                origin_heads TEXT NOT NULL,
                PRIMARY KEY(doc_id, blob_hash)
            )
            "#,
        )
        .execute(db_pool)
        .await?;

        let has_length_octets_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('doc_blob_refs') WHERE name = 'length_octets'",
        )
        .fetch_optional(db_pool)
        .await?;
        if has_length_octets_col.is_none() {
            sqlx::query(
                "ALTER TABLE doc_blob_refs ADD COLUMN length_octets INTEGER NOT NULL DEFAULT 0",
            )
            .execute(db_pool)
            .await?;
        }

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_refs_blob_hash ON doc_blob_refs(blob_hash)",
        )
        .execute(db_pool)
        .await?;

        Ok(())
    }

    async fn handle_worker_item(&self, item: DocBlobsIndexWorkItem) -> Res<()> {
        match item {
            DocBlobsIndexWorkItem::Upsert {
                doc_id,
                branch_path,
                heads,
            } => match self.reindex_doc(&doc_id, &branch_path, &heads).await? {
                ReindexDocOutcome::Present => {
                    self.registry
                        .notify([DocBlobsIndexEvent::Updated { doc_id }]);
                }
                ReindexDocOutcome::Evicted => {
                    self.registry
                        .notify([DocBlobsIndexEvent::Deleted { doc_id }]);
                }
            },
            DocBlobsIndexWorkItem::DeleteDoc { doc_id } => {
                self.delete_doc(&doc_id).await?;
                self.registry
                    .notify([DocBlobsIndexEvent::Deleted { doc_id }]);
            }
        }
        Ok(())
    }

    pub async fn reindex_doc(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<ReindexDocOutcome> {
        let Some(facet_keys) = self
            .drawer_repo
            .facet_keys_at_branch_heads(doc_id, branch_path, heads)
            .await?
        else {
            self.delete_doc(doc_id).await?;
            return Ok(ReindexDocOutcome::Evicted);
        };
        let selected_blob_keys: Vec<FacetKey> = facet_keys
            .into_iter()
            .filter(|facet_key| facet_key.tag == WellKnownFacetTag::Blob.into())
            .collect();
        if selected_blob_keys.is_empty() {
            self.delete_doc(doc_id).await?;
            return Ok(ReindexDocOutcome::Evicted);
        }
        let facets = self
            .drawer_repo
            .get_at_branch_heads_with_facets_arc(
                doc_id,
                branch_path,
                heads,
                Some(selected_blob_keys),
            )
            .await?
            .map(|(facets, _)| facets)
            .ok_or_eyre("doc didn't match expectation")?;

        let mut blobs = HashMap::<String, u64>::new();
        for (_facet_key, facet_raw) in facets {
            let facet =
                match WellKnownFacet::from_json((*facet_raw).clone(), WellKnownFacetTag::Blob) {
                    Ok(facet) => facet,
                    Err(err) => {
                        warn!(
                            %doc_id,
                            ?err,
                            "failed to parse blob facet while indexing; evicting stale blob refs"
                        );
                        self.delete_doc(doc_id).await?;
                        return Ok(ReindexDocOutcome::Evicted);
                    }
                };
            let WellKnownFacet::Blob(blob) = facet else {
                continue;
            };
            if let Some(urls) = blob.urls {
                for url in urls {
                    if let Some(hash) = parse_db_blob_hash(&url) {
                        if let Some(existing_len) = blobs.insert(hash.clone(), blob.length_octets) {
                            eyre::ensure!(
                                existing_len == blob.length_octets,
                                "inconsistent blob length indexed for hash {hash}: {existing_len} != {}",
                                blob.length_octets
                            );
                        }
                    }
                }
            }
        }

        self.reindex_doc_hashes(doc_id, heads, &blobs).await
    }

    async fn reindex_doc_hashes(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        blobs: &HashMap<String, u64>,
    ) -> Res<ReindexDocOutcome> {
        let mut tx = self.db_pool.begin().await?;
        let prev_hashes: HashSet<String> = self
            .list_hashes_for_doc_tx(tx.as_mut(), doc_id)
            .await?
            .into_iter()
            .collect();
        let next_hashes: HashSet<String> = blobs.keys().cloned().collect();

        let mut hashes_to_remove = HashSet::new();
        for hash in prev_hashes.difference(&next_hashes) {
            if self
                .hash_is_unused_excluding_doc_tx(tx.as_mut(), hash, doc_id)
                .await?
            {
                hashes_to_remove.insert(hash.clone());
            }
        }
        let hashes_to_add: HashSet<String> =
            next_hashes.difference(&prev_hashes).cloned().collect();
        self.publish_hash_delta_with_retry(&hashes_to_add, &hashes_to_remove)
            .await?;

        sqlx::query("DELETE FROM doc_blob_refs WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;

        if blobs.is_empty() {
            tx.commit().await?;
            return Ok(ReindexDocOutcome::Evicted);
        }

        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&heads.0))
                .expect(ERROR_JSON);

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO doc_blob_refs (doc_id, blob_hash, length_octets, origin_heads) ",
        );
        query_builder.push_values(blobs.iter(), |mut row, (hash, length_octets)| {
            row.push_bind(doc_id)
                .push_bind(hash)
                .push_bind(
                    i64::try_from(*length_octets)
                        .expect("blob length octets exceeds sqlite INTEGER range"),
                )
                .push_bind(&serialized_heads);
        });
        query_builder.push(
            " ON CONFLICT(doc_id, blob_hash) DO UPDATE SET origin_heads = excluded.origin_heads, length_octets = excluded.length_octets",
        );
        query_builder.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ReindexDocOutcome::Present)
    }

    pub async fn delete_doc(&self, doc_id: &DocId) -> Res<()> {
        let mut tx = self.db_pool.begin().await?;
        let prev_hashes: HashSet<String> = self
            .list_hashes_for_doc_tx(tx.as_mut(), doc_id)
            .await?
            .into_iter()
            .collect();
        let mut hashes_to_remove = HashSet::new();
        for hash in &prev_hashes {
            if self
                .hash_is_unused_excluding_doc_tx(tx.as_mut(), hash, doc_id)
                .await?
            {
                hashes_to_remove.insert(hash.clone());
            }
        }
        self.publish_hash_delta_with_retry(&HashSet::new(), &hashes_to_remove)
            .await?;
        sqlx::query("DELETE FROM doc_blob_refs WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn publish_hash_delta_with_retry(
        &self,
        hashes_to_add: &HashSet<String>,
        hashes_to_remove: &HashSet<String>,
    ) -> Res<()> {
        const MAX_ATTEMPTS: usize = 3;
        for hash in hashes_to_add {
            for attempt in 1..=MAX_ATTEMPTS {
                let result = self
                    .blobs_repo
                    .add_hash_to_scope(BlobScope::Docs, hash)
                    .await;
                match result {
                    Ok(()) => break,
                    Err(err) if attempt == MAX_ATTEMPTS => return Err(err),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis((attempt * 100) as u64)).await
                    }
                }
            }
        }
        for hash in hashes_to_remove {
            for attempt in 1..=MAX_ATTEMPTS {
                let result = self
                    .blobs_repo
                    .remove_hash_from_scope(BlobScope::Docs, hash)
                    .await;
                match result {
                    Ok(()) => break,
                    Err(err) if attempt == MAX_ATTEMPTS => return Err(err),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis((attempt * 100) as u64)).await
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn list_hashes_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT blob_hash
            FROM doc_blob_refs
            WHERE doc_id = ?1
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.db_pool)
        .await?;
        Ok(hashes)
    }

    async fn list_hashes_for_doc_tx(
        &self,
        tx: &mut sqlx::SqliteConnection,
        doc_id: &DocId,
    ) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT blob_hash
            FROM doc_blob_refs
            WHERE doc_id = ?1
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(tx)
        .await?;
        Ok(hashes)
    }

    async fn hash_is_unused_excluding_doc_tx(
        &self,
        tx: &mut sqlx::SqliteConnection,
        hash: &str,
        doc_id: &DocId,
    ) -> Res<bool> {
        let exists_other: i64 = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM doc_blob_refs
                WHERE blob_hash = ?1
                  AND doc_id != ?2
            )
            "#,
        )
        .bind(hash)
        .bind(doc_id)
        .fetch_one(tx)
        .await?;
        Ok(exists_other == 0)
    }

    pub async fn list_blob_refs_for_doc(&self, doc_id: &DocId) -> Res<Vec<DocBlobRef>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT blob_hash, length_octets
            FROM doc_blob_refs
            WHERE doc_id = ?1
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.db_pool)
        .await?;
        rows.into_iter()
            .map(|(blob_hash, length_octets)| {
                let length_octets = u64::try_from(length_octets)
                    .map_err(|_| ferr!("invalid negative blob length for hash {blob_hash}"))?;
                Ok(DocBlobRef {
                    blob_hash,
                    length_octets,
                })
            })
            .collect()
    }

    pub async fn list_docs_for_hash(&self, hash: &str) -> Res<Vec<DocBlobMembership>> {
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            r#"
            SELECT doc_id, origin_heads, length_octets
            FROM doc_blob_refs
            WHERE blob_hash = ?1
            ORDER BY doc_id ASC
            "#,
        )
        .bind(hash)
        .fetch_all(&self.db_pool)
        .await?;

        rows.into_iter()
            .map(|(doc_id, origin_heads, length_octets)| {
                let head_strings: Vec<String> = serde_json::from_str(&origin_heads)?;
                let length_octets = u64::try_from(length_octets)
                    .map_err(|_| ferr!("invalid negative blob length for hash {hash}"))?;
                Ok(DocBlobMembership {
                    doc_id,
                    blob_hash: hash.to_string(),
                    length_octets,
                    origin_heads: ChangeHashSet(am_utils_rs::parse_commit_heads(&head_strings)?),
                })
            })
            .collect()
    }

    pub async fn list_all_hashes(&self) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT blob_hash
            FROM doc_blob_refs
            ORDER BY blob_hash ASC
            "#,
        )
        .fetch_all(&self.db_pool)
        .await?;
        Ok(hashes)
    }

    pub async fn list_all_memberships(&self) -> Res<Vec<(DocId, String, u64)>> {
        let rows: Vec<(DocId, String, i64)> = sqlx::query_as(
            r#"
            SELECT doc_id, blob_hash, length_octets
            FROM doc_blob_refs
            ORDER BY doc_id ASC, blob_hash ASC
            "#,
        )
        .fetch_all(&self.db_pool)
        .await?;
        rows.into_iter()
            .map(|(doc_id, blob_hash, length_octets)| {
                let length_octets = u64::try_from(length_octets)
                    .map_err(|_| ferr!("invalid negative blob length for hash {blob_hash}"))?;
                Ok((doc_id, blob_hash, length_octets))
            })
            .collect()
    }

    pub fn triage_listener(
        self: &Arc<Self>,
    ) -> Box<dyn crate::rt::switch::SwitchSink + Send + Sync> {
        Box::new(DocBlobsTriageListener {
            drawer_repo: Arc::clone(&self.drawer_repo),
            index_repo: Arc::clone(self),
        })
    }

    pub fn enqueue_upsert(
        &self,
        doc_id: DocId,
        branch_path: BranchPath,
        heads: ChangeHashSet,
    ) -> Res<()> {
        self.work_tx
            .send(DocBlobsIndexWorkItem::Upsert {
                doc_id,
                branch_path,
                heads,
            })
            .map_err(|err| ferr!("doc_blobs_index work queue closed: {err}"))?;
        Ok(())
    }

    pub fn enqueue_delete(&self, doc_id: DocId) -> Res<()> {
        self.work_tx
            .send(DocBlobsIndexWorkItem::DeleteDoc { doc_id })
            .map_err(|err| ferr!("doc_blobs_index work queue closed: {err}"))?;
        Ok(())
    }
}

fn parse_db_blob_hash(raw_url: &str) -> Option<String> {
    let parsed = url::Url::parse(raw_url).ok()?;
    if parsed.scheme() != BLOB_SCHEME {
        return None;
    }
    if parsed.host_str().is_some() {
        return None;
    }
    let hash = parsed.path().trim_start_matches('/');
    if hash.is_empty() {
        return None;
    }
    if utils_rs::hash::decode_base58_multibase(hash).is_err() {
        return None;
    }
    Some(hash.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexDocOutcome {
    Present,
    Evicted,
}

#[derive(Debug, Clone)]
enum DocBlobsIndexWorkItem {
    Upsert {
        doc_id: DocId,
        branch_path: BranchPath,
        heads: ChangeHashSet,
    },
    DeleteDoc {
        doc_id: DocId,
    },
}

struct DocBlobsTriageListener {
    drawer_repo: Arc<DrawerRepo>,
    index_repo: Arc<DocBlobsIndexRepo>,
}

#[async_trait]
impl crate::rt::switch::SwitchSink for DocBlobsTriageListener {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_drawer: true,
            consume_plugs: false,
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
        let crate::rt::switch::SwitchEvent::Drawer(event) = event else {
            return Ok(outcome);
        };
        match &**event {
            crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                self.index_repo.enqueue_delete(id.clone())?;
            }
            crate::drawer::DrawerEvent::DocAdded {
                id,
                entry,
                drawer_heads: _,
                ..
            } => {
                let Some(heads) = entry.branches.get("main") else {
                    return Ok(outcome);
                };
                let branch_path = BranchPath::from("main");
                let Some(_keys) = self
                    .drawer_repo
                    .get_facet_keys_if_latest(id, &branch_path, heads)
                    .await?
                else {
                    return Ok(outcome);
                };
                self.index_repo
                    .enqueue_upsert(id.clone(), branch_path, heads.clone())?;
            }
            crate::drawer::DrawerEvent::DocUpdated {
                id,
                entry,
                diff,
                drawer_heads: _,
                ..
            } => {
                if !diff
                    .moved_branch_names
                    .iter()
                    .any(|branch_name| branch_name == "main")
                {
                    return Ok(outcome);
                }
                let Some(heads) = entry.branches.get("main") else {
                    self.index_repo.enqueue_delete(id.clone())?;
                    return Ok(outcome);
                };
                let branch_path = BranchPath::from("main");
                let Some(_keys) = self
                    .drawer_repo
                    .get_facet_keys_if_latest(id, &branch_path, heads)
                    .await?
                else {
                    return Ok(outcome);
                };
                self.index_repo
                    .enqueue_upsert(id.clone(), branch_path, heads.clone())?;
            }
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::test_cx;
    use crate::repos::SubscribeOpts;
    use daybook_types::doc::{AddDocArgs, FacetRaw};

    async fn wait_for_hash(repo: &DocBlobsIndexRepo, doc_id: &DocId, hash: &str) -> Res<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            let hashes = repo.list_hashes_for_doc(doc_id).await?;
            if hashes.iter().any(|value| value == hash) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eyre::bail!("timeout waiting for doc blob hash")
    }

    async fn wait_for_partition_member_count(
        big_repo: &SharedBigRepo,
        partition_id: &str,
        expected: i64,
    ) -> Res<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            let count = big_repo
                .partition_member_count(&partition_id.to_string())
                .await?;
            if count == expected {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eyre::bail!(
            "timeout waiting for partition member count partition_id={partition_id} expected={expected}"
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_blobs_index_tracks_blob_hashes() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_context.rt.doc_blobs_index_repo);

        let hash_a = utils_rs::hash::encode_base58_multibase(b"fakehasha");
        let hash_b = utils_rs::hash::encode_base58_multibase(b"fakehashb");
        let doc_id = test_context
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPath::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "image/png".to_string(),
                        length_octets: 42,
                        digest: "bafakedigest".to_string(),
                        inline: None,
                        urls: Some(vec![
                            format!("{BLOB_SCHEME}:///{hash_a}"),
                            format!("{BLOB_SCHEME}:///{hash_b}"),
                        ]),
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        wait_for_hash(&repo, &doc_id, &hash_a).await?;
        let hashes = repo.list_hashes_for_doc(&doc_id).await?;
        assert!(hashes.contains(&hash_a));
        assert!(hashes.contains(&hash_b));
        let blob_refs = repo.list_blob_refs_for_doc(&doc_id).await?;
        assert_eq!(blob_refs.len(), 2);
        assert!(blob_refs
            .iter()
            .all(|blob_ref| blob_ref.length_octets == 42));

        let memberships = repo.list_docs_for_hash(&hash_a).await?;
        assert!(memberships
            .iter()
            .any(|value| value.doc_id == doc_id && value.length_octets == 42));

        test_context.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_for_missing_doc_emits_deleted_event() -> Res<()> {
        let test_context = test_cx(utils_rs::function_full!()).await?;
        let repo = Arc::clone(&test_context.rt.doc_blobs_index_repo);
        let listener = repo.subscribe(SubscribeOpts::new(16));
        let missing_doc_id = "doc-missing-for-blob-index".to_string();

        repo.enqueue_upsert(
            missing_doc_id.clone(),
            BranchPath::from("main"),
            ChangeHashSet(default()),
        )?;

        let evt = listener
            .recv_async()
            .await
            .map_err(|err| ferr!("listener recv failed: {err:?}"))?;
        assert!(
            matches!(
                &*evt,
                DocBlobsIndexEvent::Deleted { doc_id } if *doc_id == missing_doc_id
            ),
            "upsert for a missing doc should emit Deleted, got: {evt:?}"
        );

        test_context.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn doc_blobs_index_publishes_docs_scope_partition_membership() -> Res<()> {
        let local_user_path = daybook_types::doc::UserPath::from("/test-user/test-device");
        let (big_repo, big_repo_stop) = BigRepo::boot(am_utils_rs::repo::Config {
            peer_id: "test-doc-blobs-scope".into(),
            storage: am_utils_rs::repo::StorageConfig::Memory,
        })
        .await?;
        let mut drawer_doc = automerge::Automerge::new();
        {
            use automerge::transaction::Transactable;
            let mut tx = drawer_doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
        }
        let drawer_doc_id = big_repo.add_doc(drawer_doc).await?.document_id().clone();
        let temp_dir = tempfile::tempdir()?;
        let blobs_repo = crate::blobs::BlobsRepo::new(
            temp_dir.path().join("blobs"),
            "/test-user".to_string(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                big_repo.partition_store(),
            )),
        )
        .await?;
        let (drawer_repo, drawer_stop) = crate::drawer::DrawerRepo::load(
            Arc::clone(&big_repo),
            drawer_doc_id,
            local_user_path.clone(),
            crate::app::SqlCtx::new("sqlite::memory:").await?.db_pool,
            temp_dir.path().join("drawer-local-state"),
            Arc::new(std::sync::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(std::sync::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            None,
        )
        .await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            crate::local_state::SqliteLocalStateRepo::boot(temp_dir.path().join("local-state"))
                .await?;
        let (repo, repo_stop) = DocBlobsIndexRepo::boot(
            Arc::clone(&drawer_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&sqlite_local_state_repo),
        )
        .await?;

        let partition_id = crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID.to_string();
        let hash = utils_rs::hash::encode_base58_multibase(b"docs-scope-hash");
        let doc_id = drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPath::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 13,
                        digest: "ignored-digest".to_string(),
                        inline: None,
                        urls: Some(vec![format!("{BLOB_SCHEME}:///{hash}")]),
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        let heads = drawer_repo
            .get_doc_branches(&doc_id)
            .await?
            .and_then(|branches| branches.branches.get("main").cloned())
            .ok_or_eyre("expected main branch heads for test doc")?;
        repo.enqueue_upsert(doc_id.clone(), BranchPath::from("main"), heads)?;

        wait_for_partition_member_count(&big_repo, &partition_id, 1).await?;
        assert!(
            big_repo
                .is_member_present_in_partition_item_state(&partition_id, &hash)
                .await?
        );

        drawer_repo.del(&doc_id).await?;
        repo.enqueue_delete(doc_id.clone())?;
        wait_for_partition_member_count(&big_repo, &partition_id, 0).await?;
        assert!(
            !big_repo
                .is_member_present_in_partition_item_state(&partition_id, &hash)
                .await?
        );

        repo_stop.stop().await?;
        sqlite_local_state_stop.stop().await?;
        drawer_stop.stop().await?;
        big_repo_stop.stop().await?;
        Ok(())
    }
}
