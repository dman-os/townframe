use crate::blobs::BLOB_SCHEME;
use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::interlude::*;
use crate::repos::Repo;
use daybook_types::doc::{
    BranchPathBuf, ChangeHashSet, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag,
};
use sqlx::QueryBuilder;
use tokio_util::sync::CancellationToken;

const DOC_BLOBS_LOCAL_STATE_ID: &str = "@daybook/wip/doc-blobs-index";

#[derive(Debug, Clone)]
pub struct DocBlobMembership {
    pub doc_id: DocId,
    pub branch_path: BranchPathBuf,
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
    _blobs_repo: Arc<BlobsRepo>,
    work_tx: tokio::sync::mpsc::UnboundedSender<DocBlobsIndexWorkItem>,
    sql: SqlCtx,
}

fn record_blob_length(
    blobs: &mut HashMap<Arc<str>, u64>,
    hash: Arc<str>,
    length_octets: u64,
) -> bool {
    if let Some(&existing_len) = blobs.get(&hash) {
        if existing_len != length_octets {
            return false;
        }
    } else {
        blobs.insert(hash, length_octets);
    }
    true
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

impl DocBlobsIndexRepo {
    pub async fn boot(
        drawer_repo: Arc<DrawerRepo>,
        blobs_repo: Arc<BlobsRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(DOC_BLOBS_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&sql).await?;
        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel();

        let registry = crate::repos::ListenersRegistry::new();
        let cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            registry,
            cancel_token: cancel_token.child_token(),
            drawer_repo: Arc::clone(&drawer_repo),
            _blobs_repo: blobs_repo,
            work_tx,
            sql,
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
                            repo.handle_worker_item(item)
                                .await
                                .expect("doc blobs worker item handling failed");
                        }
                    }
                }
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
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_blob_refs (
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

        let has_length_octets_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('doc_blob_refs') WHERE name = 'length_octets'",
        )
        .fetch_optional(&sql.write_pool)
        .await?;
        if has_length_octets_col.is_none() {
            sqlx::query(
                "ALTER TABLE doc_blob_refs ADD COLUMN length_octets INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&sql.write_pool)
            .await?;
        }

        let has_branch_path_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('doc_blob_refs') WHERE name = 'branch_path'",
        )
        .fetch_optional(&sql.write_pool)
        .await?;
        if has_branch_path_col.is_none() {
            let mut tx = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("ALTER TABLE doc_blob_refs RENAME TO doc_blob_refs_old")
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"
                CREATE TABLE doc_blob_refs (
                    doc_id TEXT NOT NULL,
                    branch_path TEXT NOT NULL,
                    blob_hash TEXT NOT NULL,
                    length_octets INTEGER NOT NULL DEFAULT 0,
                    origin_heads TEXT NOT NULL,
                    PRIMARY KEY(doc_id, branch_path, blob_hash)
                ) STRICT
                "#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO doc_blob_refs (doc_id, branch_path, blob_hash, length_octets, origin_heads)
                SELECT doc_id, 'main', blob_hash, length_octets, origin_heads
                FROM doc_blob_refs_old
                "#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("DROP TABLE doc_blob_refs_old")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_refs_blob_hash ON doc_blob_refs(blob_hash)",
        )
        .execute(&sql.write_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_refs_doc_branch ON doc_blob_refs(doc_id, branch_path)",
        )
        .execute(&sql.write_pool)
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
            DocBlobsIndexWorkItem::DeleteDocBranchesNotIn {
                doc_id,
                branch_paths,
            } => match self
                .delete_doc_branches_not_in(&doc_id, &branch_paths)
                .await?
            {
                ReindexDocOutcome::Present => {
                    self.registry
                        .notify([DocBlobsIndexEvent::Updated { doc_id }]);
                }
                ReindexDocOutcome::Evicted => {
                    self.registry
                        .notify([DocBlobsIndexEvent::Deleted { doc_id }]);
                }
            },
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
        let selected_blob_keys: Vec<FacetKey> = facet_keys
            .into_iter()
            .filter(|facet_key| {
                facet_key.tag == WellKnownFacetTag::Blob.into()
                    || facet_key.tag == WellKnownFacetTag::BlobPin.into()
            })
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
            tracing::debug!(%doc_id, %branch_path, "doc heads changed during index update");
            return self.doc_presence_outcome(doc_id).await;
        };

        let mut blobs = HashMap::<Arc<str>, u64>::new();
        for (facet_key, facet_raw) in facets {
            if facet_key.tag == WellKnownFacetTag::BlobPin.into() {
                let pin: daybook_types::doc::BlobPin = match serde_json::from_value(
                    (*facet_raw).clone(),
                ) {
                    Ok(pin) => pin,
                    Err(err) => {
                        warn!(
                            %doc_id,
                            %branch_path,
                            ?err,
                            "failed to parse blob pin facet while indexing; evicting stale blob refs"
                        );
                        return self.delete_doc_branch(doc_id, branch_path).await;
                    }
                };
                if facet_key.id.parse::<crate::blobs::BlobId>().is_ok() {
                    let hash: Arc<str> = facet_key.id.as_str().into();
                    if !record_blob_length(&mut blobs, Arc::clone(&hash), pin.length_octets) {
                        warn!(
                            %doc_id,
                            %branch_path,
                            %hash,
                            new_length = pin.length_octets,
                            "inconsistent blob length indexed for hash; evicting stale blob refs"
                        );
                        return self.delete_doc_branch(doc_id, branch_path).await;
                    }
                } else {
                    warn!(
                        %doc_id,
                        %branch_path,
                        facet_id = %facet_key.id,
                        "invalid blob pin facet id; evicting stale blob refs"
                    );
                    return self.delete_doc_branch(doc_id, branch_path).await;
                }
            } else if facet_key.tag == WellKnownFacetTag::Blob.into() {
                let facet = match WellKnownFacet::from_json(
                    (*facet_raw).clone(),
                    WellKnownFacetTag::Blob,
                ) {
                    Ok(facet) => facet,
                    Err(err) => {
                        warn!(
                            %doc_id,
                            %branch_path,
                            ?err,
                            "failed to parse blob facet while indexing; evicting stale blob refs"
                        );
                        return self.delete_doc_branch(doc_id, branch_path).await;
                    }
                };
                let WellKnownFacet::Blob(blob) = facet else {
                    continue;
                };
                let mut found_url_hash = false;
                if let Some(urls) = blob.urls {
                    for url in urls {
                        if let Some(hash) = parse_db_blob_hash(&url) {
                            found_url_hash = true;
                            let hash: Arc<str> = hash.into();
                            if !record_blob_length(
                                &mut blobs,
                                Arc::clone(&hash),
                                blob.length_octets,
                            ) {
                                warn!(
                                    %doc_id,
                                    %branch_path,
                                    %hash,
                                    new_length = blob.length_octets,
                                    "inconsistent blob length indexed for hash in blob urls; evicting stale blob refs"
                                );
                                return self.delete_doc_branch(doc_id, branch_path).await;
                            }
                        }
                    }
                }
                if !found_url_hash
                    && !blob.digest.is_empty()
                    && blob.digest.parse::<crate::blobs::BlobId>().is_ok()
                {
                    let hash: Arc<str> = blob.digest.as_str().into();
                    if !record_blob_length(&mut blobs, Arc::clone(&hash), blob.length_octets) {
                        warn!(
                            %doc_id,
                            %branch_path,
                            %hash,
                            new_length = blob.length_octets,
                            "inconsistent blob length indexed for hash in blob digest; evicting stale blob refs"
                        );
                        return self.delete_doc_branch(doc_id, branch_path).await;
                    }
                }
            }
        }

        info!(%doc_id, %branch_path, ?blobs, "reindex_doc_hashes called");
        self.reindex_doc_hashes(doc_id, branch_path, heads, &blobs)
            .await
    }

    async fn reindex_doc_hashes(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
        heads: &ChangeHashSet,
        blobs: &HashMap<Arc<str>, u64>,
    ) -> Res<ReindexDocOutcome> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM doc_blob_refs WHERE doc_id = ?1 AND branch_path = ?2")
            .bind(doc_id)
            .bind(branch_path.as_str())
            .execute(&mut *tx)
            .await?;

        if blobs.is_empty() {
            tx.commit().await?;
            return self.doc_presence_outcome(doc_id).await;
        }

        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&heads.0))
                .expect(ERROR_JSON);

        let mut rows: Vec<(&str, i64)> = Vec::with_capacity(blobs.len());
        for (hash, length_octets) in blobs {
            let length_octets_i64 = i64::try_from(*length_octets).map_err(|_| {
                eyre::eyre!(
                    "blob length octets exceeds sqlite INTEGER range: doc_id={} branch={} length_octets={}",
                    doc_id,
                    branch_path.as_str(),
                    length_octets
                )
            })?;
            rows.push((&hash[..], length_octets_i64));
        }

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO doc_blob_refs (doc_id, branch_path, blob_hash, length_octets, origin_heads) ",
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
        tx.commit().await?;
        self.doc_presence_outcome(doc_id).await
    }

    pub async fn delete_doc(&self, doc_id: &DocId) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM doc_blob_refs WHERE doc_id = ?1")
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn delete_doc_branch(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<ReindexDocOutcome> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM doc_blob_refs WHERE doc_id = ?1 AND branch_path = ?2")
            .bind(doc_id)
            .bind(branch_path.as_str())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.doc_presence_outcome(doc_id).await
    }

    async fn doc_presence_outcome(&self, doc_id: &DocId) -> Res<ReindexDocOutcome> {
        let exists: i64 = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM doc_blob_refs
                WHERE doc_id = ?1
            )
            "#,
        )
        .bind(doc_id)
        .fetch_one(&self.sql.read_pool)
        .await?;
        if exists == 0 {
            Ok(ReindexDocOutcome::Evicted)
        } else {
            Ok(ReindexDocOutcome::Present)
        }
    }

    pub async fn list_hashes_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        Self::list_hashes_for_doc_with(&self.sql.read_pool, doc_id).await
    }

    async fn list_hashes_for_doc_with(pool: &sqlx::SqlitePool, doc_id: &DocId) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT blob_hash
            FROM doc_blob_refs
            WHERE doc_id = ?1
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(pool)
        .await?;
        Ok(hashes)
    }

    pub async fn list_hashes_for_doc_branch(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<Vec<String>> {
        Self::list_hashes_for_doc_branch_with(&self.sql.read_pool, doc_id, branch_path).await
    }

    async fn list_hashes_for_doc_branch_with(
        pool: &sqlx::SqlitePool,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<Vec<String>> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT blob_hash
            FROM doc_blob_refs
            WHERE doc_id = ?1
              AND branch_path = ?2
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .bind(branch_path.as_str())
        .fetch_all(pool)
        .await?;
        Ok(hashes)
    }

    pub async fn list_blob_refs_for_doc(&self, doc_id: &DocId) -> Res<Vec<DocBlobRef>> {
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            r#"
            SELECT blob_hash, MIN(length_octets), MAX(length_octets)
            FROM doc_blob_refs
            WHERE doc_id = ?1
            GROUP BY blob_hash
            ORDER BY blob_hash ASC
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|(blob_hash, min_length_octets, max_length_octets)| {
                eyre::ensure!(
                    min_length_octets == max_length_octets,
                    "inconsistent blob length for hash {blob_hash} on doc {doc_id}: {min_length_octets} vs {max_length_octets}"
                );
                let length_octets = u64::try_from(min_length_octets)
                    .map_err(|_| ferr!("invalid negative blob length for hash {blob_hash}"))?;
                Ok(DocBlobRef {
                    blob_hash,
                    length_octets,
                })
            })
            .collect()
    }

    pub async fn list_docs_for_hash(&self, hash: &str) -> Res<Vec<DocBlobMembership>> {
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            r#"
            SELECT doc_id, branch_path, origin_heads, length_octets
            FROM doc_blob_refs
            WHERE blob_hash = ?1
            ORDER BY doc_id ASC, branch_path ASC
            "#,
        )
        .bind(hash)
        .fetch_all(&self.sql.read_pool)
        .await?;

        rows.into_iter()
            .map(|(doc_id, branch_path, origin_heads, length_octets)| {
                let head_strings: Vec<String> = serde_json::from_str(&origin_heads)?;
                let length_octets = u64::try_from(length_octets)
                    .map_err(|_| ferr!("invalid negative blob length for hash {hash}"))?;
                Ok(DocBlobMembership {
                    doc_id,
                    branch_path: BranchPathBuf::from(branch_path),
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
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(hashes)
    }

    pub async fn list_all_memberships(&self) -> Res<Vec<(DocId, BranchPathBuf, String, u64)>> {
        let rows: Vec<(DocId, String, String, i64)> = sqlx::query_as(
            r#"
            SELECT doc_id, branch_path, blob_hash, length_octets
            FROM doc_blob_refs
            ORDER BY doc_id ASC, branch_path ASC, blob_hash ASC
            "#,
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|(doc_id, branch_path, blob_hash, length_octets)| {
                let length_octets = u64::try_from(length_octets)
                    .map_err(|_| ferr!("invalid negative blob length for hash {blob_hash}"))?;
                Ok((
                    doc_id,
                    BranchPathBuf::from(branch_path),
                    blob_hash,
                    length_octets,
                ))
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
        branch_path: BranchPathBuf,
        heads: ChangeHashSet,
    ) -> Res<()> {
        self.work_tx
            .send(DocBlobsIndexWorkItem::Upsert {
                doc_id,
                branch_path,
                heads,
            })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete(&self, doc_id: DocId) -> Res<()> {
        self.work_tx
            .send(DocBlobsIndexWorkItem::DeleteDoc { doc_id })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    pub fn enqueue_delete_branches_not_in(
        &self,
        doc_id: DocId,
        branch_paths: Vec<BranchPathBuf>,
    ) -> Res<()> {
        self.work_tx
            .send(DocBlobsIndexWorkItem::DeleteDocBranchesNotIn {
                doc_id,
                branch_paths,
            })
            .wrap_err(ERROR_ACTOR)?;
        Ok(())
    }

    async fn delete_doc_branches_not_in(
        &self,
        doc_id: &DocId,
        branch_paths: &[BranchPathBuf],
    ) -> Res<ReindexDocOutcome> {
        let retained: HashSet<&str> = branch_paths.iter().map(|path| path.as_str()).collect();
        let current: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT branch_path
            FROM doc_blob_refs
            WHERE doc_id = ?1
            "#,
        )
        .bind(doc_id)
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut outcome = self.doc_presence_outcome(doc_id).await?;
        for branch_path in current {
            if retained.contains(branch_path.as_str()) {
                continue;
            }
            outcome = self
                .delete_doc_branch(doc_id, &BranchPathBuf::from(branch_path))
                .await?;
        }
        Ok(outcome)
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
    if hash.parse::<crate::blobs::BlobId>().is_err() {
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

struct DocBlobsTriageListener {
    drawer_repo: Arc<DrawerRepo>,
    index_repo: Arc<DocBlobsIndexRepo>,
}

#[async_trait]
impl crate::rt::switch::SwitchSink for DocBlobsTriageListener {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_doc: true,
            consume_drawer: true,
            consume_plugs: false,
            consume_dispatch: false,
            consume_config: false,
            drawer_predicate: Some(daybook_types::manifest::DocPredicateClause::Or(vec![
                daybook_types::manifest::DocPredicateClause::HasTag(WellKnownFacetTag::Blob.into()),
                daybook_types::manifest::DocPredicateClause::HasTag(
                    WellKnownFacetTag::BlobPin.into(),
                ),
            ])),
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
                self.index_repo
                    .handle_worker_item(DocBlobsIndexWorkItem::Upsert {
                        doc_id: event.doc_id.clone(),
                        branch_path,
                        heads: event.new_heads.clone(),
                    })
                    .await?;
            }
            crate::rt::switch::SwitchEvent::Drawer(event) => match &**event {
                crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                    self.index_repo
                        .handle_worker_item(DocBlobsIndexWorkItem::DeleteDoc { doc_id: id.clone() })
                        .await?;
                }
                crate::drawer::DrawerEvent::DocAdded {
                    id,
                    entry,
                    ..
                } => {
                    for (branch_name, heads) in &entry.branches {
                        let branch_path = BranchPathBuf::from(branch_name.as_str());
                        let Some(_keys) = self
                            .drawer_repo
                            .get_facet_keys_if_latest(id, &branch_path, heads)
                            .await?
                        else {
                            continue;
                        };
                        self.index_repo
                            .handle_worker_item(DocBlobsIndexWorkItem::Upsert {
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
    use crate::repos::SubscribeOpts;
    use crate::test_support::test_cx;
    use daybook_types::doc::{AddDocArgs, BlobPin, FacetRaw};

    async fn wait_for_hash(repo: &DocBlobsIndexRepo, doc_id: &DocId, hash: &str) -> Res<()> {
        let deadline = tokio::time::Instant::now()
            + utils_rs::scale_timeout(std::time::Duration::from_secs(60));
        while tokio::time::Instant::now() < deadline {
            let hashes = repo.list_hashes_for_doc(doc_id).await?;
            if hashes.iter().any(|value| value == hash) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eyre::bail!("timeout waiting for doc blob hash")
    }

    struct TestIndexEnv {
        drawer_repo: Arc<DrawerRepo>,
        repo: Arc<DocBlobsIndexRepo>,
        _blobs_repo: Arc<BlobsRepo>,
        _big_sync_host: big_sync::Ctx,
        drawer_stop: crate::repos::RepoStopToken,
        sqlite_local_state_stop: crate::repos::RepoStopToken,
        repo_stop: crate::repos::RepoStopToken,
        big_repo_stop: Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
        _temp_dir: tempfile::TempDir,
    }

    impl TestIndexEnv {
        async fn stop(self) -> Res<()> {
            self.repo_stop.stop().await?;
            self.sqlite_local_state_stop.stop().await?;
            self.drawer_stop.stop().await?;
            (self.big_repo_stop)().await?;
            Ok(())
        }
    }

    async fn boot_test_index_env() -> Res<TestIndexEnv> {
        let local_user_path = daybook_types::doc::UserPathBuf::from("/test-user/test-device");
        let (big_repo, big_sync_host, big_repo_stop) = crate::test_support::boot_repo().await?;
        let mut drawer_doc = automerge::Automerge::new();
        {
            use automerge::transaction::Transactable;
            let mut tx = drawer_doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
        }
        let drawer_doc_id = big_repo.create_doc(drawer_doc).await?.document_id();
        let temp_dir = tempfile::tempdir()?;
        let blobs_repo =
            crate::blobs::BlobsRepo::new(temp_dir.path().join("blobs"), "/test-user".into())
                .await?;
        let (drawer_repo, drawer_stop) = crate::drawer::DrawerRepo::load(
            Arc::clone(&big_repo),
            Arc::clone(&big_sync_host.store),
            drawer_doc_id,
            local_user_path.clone(),
            crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?,
            temp_dir.path().join("drawer-local-state"),
            Arc::new(surelock::mutex::Mutex::new(
                utils_rs::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(surelock::mutex::Mutex::new(
                utils_rs::lru::KeyedLruPool::new(1000),
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

        Ok(TestIndexEnv {
            drawer_repo,
            repo,
            _blobs_repo: blobs_repo,
            _big_sync_host: big_sync_host,
            drawer_stop,
            sqlite_local_state_stop,
            repo_stop,
            big_repo_stop: Box::new(move || Box::pin(big_repo_stop())),
            _temp_dir: temp_dir,
        })
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
                branch_path: BranchPathBuf::from("main"),
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
        assert!(
            blob_refs
                .iter()
                .all(|blob_ref| blob_ref.length_octets == 42)
        );

        let memberships = repo.list_docs_for_hash(&hash_a).await?;
        assert!(
            memberships
                .iter()
                .any(|value| value.doc_id == doc_id && value.length_octets == 42)
        );

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
            BranchPathBuf::from("main"),
            ChangeHashSet(default()),
        )?;

        let evt = listener
            .recv_async()
            .await
            .map_err(|_| ferr!(ERROR_CHANNEL))?;
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
    async fn test_doc_blobs_index_tracks_blob_pin_facets() -> Res<()> {
        let env = boot_test_index_env().await?;

        let hash_a = crate::blobs::BlobId::random().to_string();
        let hash_b = crate::blobs::BlobId::random().to_string();

        let key_pin_a = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_a.clone(),
        };
        let key_pin_b = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_b.clone(),
        };

        let doc_id = env
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        key_pin_a,
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 100 })),
                    ),
                    (
                        key_pin_b,
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 200 })),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        let heads = env
            .drawer_repo
            .get_doc_branches(&doc_id)
            .await?
            .and_then(|branches| branches.branches.get("main").cloned())
            .ok_or_eyre("expected main branch heads for test doc")?;
        env.repo
            .enqueue_upsert(doc_id.clone(), BranchPathBuf::from("main"), heads)?;

        wait_for_hash(&env.repo, &doc_id, &hash_a).await?;
        wait_for_hash(&env.repo, &doc_id, &hash_b).await?;

        let hashes = env.repo.list_hashes_for_doc(&doc_id).await?;
        assert!(hashes.contains(&hash_a));
        assert!(hashes.contains(&hash_b));

        let blob_refs = env.repo.list_blob_refs_for_doc(&doc_id).await?;
        assert_eq!(blob_refs.len(), 2);
        assert!(
            blob_refs
                .iter()
                .any(|r| r.blob_hash == hash_a && r.length_octets == 100)
        );
        assert!(
            blob_refs
                .iter()
                .any(|r| r.blob_hash == hash_b && r.length_octets == 200)
        );

        let memberships_a = env.repo.list_docs_for_hash(&hash_a).await?;
        assert!(
            memberships_a
                .iter()
                .any(|m| m.doc_id == doc_id && m.length_octets == 100)
        );

        let memberships_b = env.repo.list_docs_for_hash(&hash_b).await?;
        assert!(
            memberships_b
                .iter()
                .any(|m| m.doc_id == doc_id && m.length_octets == 200)
        );

        env.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_blobs_index_blob_facet_digest_fallback() -> Res<()> {
        let env = boot_test_index_env().await?;

        let hash_none_urls = crate::blobs::BlobId::random().to_string();
        let hash_empty_urls = crate::blobs::BlobId::random().to_string();

        // 1. Doc with urls: None, but valid digest
        let doc_id_1 = env
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 512,
                        digest: hash_none_urls.clone(),
                        inline: None,
                        urls: None,
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let heads_1 = env
            .drawer_repo
            .get_doc_branches(&doc_id_1)
            .await?
            .and_then(|branches| branches.branches.get("main").cloned())
            .ok_or_eyre("expected main branch heads for test doc")?;
        env.repo
            .enqueue_upsert(doc_id_1.clone(), BranchPathBuf::from("main"), heads_1)?;

        wait_for_hash(&env.repo, &doc_id_1, &hash_none_urls).await?;
        let hashes_1 = env.repo.list_hashes_for_doc(&doc_id_1).await?;
        assert_eq!(hashes_1, vec![hash_none_urls.clone()]);
        let blob_refs_1 = env.repo.list_blob_refs_for_doc(&doc_id_1).await?;
        assert_eq!(blob_refs_1.len(), 1);
        assert_eq!(blob_refs_1[0].blob_hash, hash_none_urls);
        assert_eq!(blob_refs_1[0].length_octets, 512);

        // 2. Doc with urls: Some(vec![]), but valid digest
        let doc_id_2 = env
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: 1024,
                        digest: hash_empty_urls.clone(),
                        inline: None,
                        urls: Some(vec![]),
                    })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let heads_2 = env
            .drawer_repo
            .get_doc_branches(&doc_id_2)
            .await?
            .and_then(|branches| branches.branches.get("main").cloned())
            .ok_or_eyre("expected main branch heads for test doc")?;
        env.repo
            .enqueue_upsert(doc_id_2.clone(), BranchPathBuf::from("main"), heads_2)?;

        wait_for_hash(&env.repo, &doc_id_2, &hash_empty_urls).await?;
        let hashes_2 = env.repo.list_hashes_for_doc(&doc_id_2).await?;
        assert_eq!(hashes_2, vec![hash_empty_urls.clone()]);
        let blob_refs_2 = env.repo.list_blob_refs_for_doc(&doc_id_2).await?;
        assert_eq!(blob_refs_2.len(), 1);
        assert_eq!(blob_refs_2[0].blob_hash, hash_empty_urls);
        assert_eq!(blob_refs_2[0].length_octets, 1024);

        env.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_blobs_index_blob_pin_invalid_facet_id_eviction() -> Res<()> {
        let env = boot_test_index_env().await?;

        let hash_a = crate::blobs::BlobId::random().to_string();
        let key_pin_a = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: hash_a.clone(),
        };

        // 1. Initially index valid pin
        let doc_id = env
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    key_pin_a.clone(),
                    FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 100 })),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let heads = env
            .drawer_repo
            .get_doc_branches(&doc_id)
            .await?
            .and_then(|branches| branches.branches.get("main").cloned())
            .ok_or_eyre("expected main branch heads")?;
        env.repo
            .enqueue_upsert(doc_id.clone(), BranchPathBuf::from("main"), heads)?;

        wait_for_hash(&env.repo, &doc_id, &hash_a).await?;
        assert_eq!(
            env.repo.list_hashes_for_doc(&doc_id).await?,
            vec![hash_a.clone()]
        );

        // 2. Update with invalid BlobId in facet ID
        let key_pin_invalid = FacetKey {
            tag: WellKnownFacetTag::BlobPin.into(),
            id: "not_a_valid_blob_id".to_string(),
        };

        env.drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: doc_id.clone(),
                    facets_set: [(
                        key_pin_invalid,
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 100 })),
                    )]
                    .into(),
                    facets_remove: vec![key_pin_a],
                    user_path: None,
                },
                daybook_types::doc::BranchPath::new("main"),
                None,
            )
            .await?;

        let heads_invalid = env
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, daybook_types::doc::BranchPath::new("main"))
            .await?
            .ok_or_eyre("expected main branch heads")?;
        env.repo
            .enqueue_upsert(doc_id.clone(), BranchPathBuf::from("main"), heads_invalid)?;

        let start = std::time::Instant::now();
        while !env.repo.list_hashes_for_doc(&doc_id).await?.is_empty() {
            if start.elapsed() > utils_rs::scale_timeout(std::time::Duration::from_secs(5)) {
                eyre::bail!("timeout waiting for eviction on invalid facet ID");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        env.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_doc_blobs_index_conflicting_lengths_eviction() -> Res<()> {
        let env = boot_test_index_env().await?;

        let hash = crate::blobs::BlobId::random().to_string();

        // Doc with conflicting lengths between BlobPin and Blob facets for the same hash
        let doc_id = env
            .drawer_repo
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (
                        FacetKey {
                            tag: WellKnownFacetTag::BlobPin.into(),
                            id: hash.clone(),
                        },
                        FacetRaw::from(WellKnownFacet::BlobPin(BlobPin { length_octets: 100 })),
                    ),
                    (
                        FacetKey::from(WellKnownFacetTag::Blob),
                        FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                            mime: "application/octet-stream".to_string(),
                            length_octets: 200,
                            digest: hash.clone(),
                            inline: None,
                            urls: None,
                        })),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        let heads = env
            .drawer_repo
            .get_doc_branches(&doc_id)
            .await?
            .and_then(|branches| branches.branches.get("main").cloned())
            .ok_or_eyre("expected main branch heads")?;
        env.repo
            .enqueue_upsert(doc_id.clone(), BranchPathBuf::from("main"), heads)?;

        let start = std::time::Instant::now();
        while !env.repo.list_hashes_for_doc(&doc_id).await?.is_empty() {
            if start.elapsed() > utils_rs::scale_timeout(std::time::Duration::from_secs(5)) {
                eyre::bail!("timeout waiting for conflicting length eviction");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(env.repo.list_hashes_for_doc(&doc_id).await?.is_empty());

        env.stop().await?;
        Ok(())
    }
}
