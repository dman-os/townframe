use crate::drawer::DrawerRepo;
use crate::index::facet_delta::FacetDelta;
use crate::index::facet_set::{FacetSetRevisionStore, FacetSetSelector};
use crate::interlude::*;
use big_repo::SharedPartStore;
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use daybook_types::doc::{BranchId, BranchPathBuf, ChangeHashSet, DocId, WellKnownFacetTag};
use sqlx::{QueryBuilder, Row, Sqlite, Transaction};
use std::collections::{BTreeMap, BTreeSet};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use daybook_types::doc::FacetKey;

pub const DOC_BLOB_PINS_LOCAL_STATE_ID: &str = "@daybook/core/doc-blob-pins-index";

pub struct BlobPinsPartWorker {
    part_store: SharedPartStore,
    sql: SqlCtx,
}

impl BlobPinsPartWorker {
    pub async fn boot(
        part_store: SharedPartStore,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let sql = sqlite_local_state_repo
            .ensure_sqlite_ctx(DOC_BLOB_PINS_LOCAL_STATE_ID)
            .await?;
        Self::init_schema(&sql).await?;

        let cancel_token = CancellationToken::new();
        let worker = Arc::new(Self { part_store, sql });

        Ok((
            worker,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: None,
            },
        ))
    }

    async fn init_schema(sql: &SqlCtx) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_blob_pins (
                doc_id TEXT NOT NULL,
                branch_id TEXT NOT NULL,
                blob_hash TEXT NOT NULL,
                length_octets INTEGER NOT NULL DEFAULT 0,
                origin_heads TEXT NOT NULL,
                PRIMARY KEY(doc_id, branch_id, blob_hash)
            ) STRICT
            "#,
        )
        .execute(&sql.write_pool)
        .await?;

        let has_branch_id_col: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('doc_blob_pins') WHERE name = 'branch_id'",
        )
        .fetch_optional(&sql.write_pool)
        .await?;
        if has_branch_id_col.is_none() {
            let mut tx = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("ALTER TABLE doc_blob_pins RENAME TO doc_blob_pins_legacy")
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"CREATE TABLE doc_blob_pins (
                    doc_id TEXT NOT NULL
                  , branch_id TEXT NOT NULL
                  , blob_hash TEXT NOT NULL
                  , length_octets INTEGER NOT NULL DEFAULT 0
                  , origin_heads TEXT NOT NULL
                  , PRIMARY KEY(doc_id, branch_id, blob_hash)
                ) STRICT"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"INSERT INTO doc_blob_pins(
                    doc_id, branch_id, blob_hash, length_octets, origin_heads
                )
                SELECT doc_id
                     , CASE WHEN branch_path = 'main' THEN doc_id ELSE branch_path END
                     , blob_hash
                     , length_octets
                     , origin_heads
                  FROM doc_blob_pins_legacy"#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("DROP TABLE doc_blob_pins_legacy")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_doc_branch ON doc_blob_pins(doc_id, branch_id)",
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

    async fn load_branch_states_in<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        branches: &[(DocId, String)],
    ) -> Res<BTreeMap<(DocId, String), BTreeMap<String, u64>>> {
        if branches.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT doc_id, branch_id, blob_hash, length_octets FROM doc_blob_pins WHERE ",
        );
        for (index, (doc_id, branch_id)) in branches.iter().enumerate() {
            if index != 0 {
                query.push(" OR ");
            }
            query
                .push("(doc_id = ")
                .push_bind(doc_id)
                .push(" AND branch_id = ")
                .push_bind(branch_id)
                .push(")");
        }
        let mut out = BTreeMap::<(DocId, String), BTreeMap<String, u64>>::new();
        for row in query.build().fetch_all(&mut **tx).await? {
            let doc_id: DocId = row.try_get("doc_id")?;
            let branch_id: String = row.try_get("branch_id")?;
            let hash: String = row.try_get("blob_hash")?;
            let length = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            out.entry((doc_id, branch_id))
                .or_default()
                .insert(hash, length);
        }
        Ok(out)
    }

    async fn load_doc_states(
        &self,
        documents: &BTreeSet<DocId>,
    ) -> Res<BTreeMap<(DocId, String), BTreeMap<String, u64>>> {
        if documents.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut tx = self.sql.read_pool.begin().await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT doc_id, branch_id, blob_hash, length_octets FROM doc_blob_pins WHERE doc_id IN (",
        );
        let mut values = query.separated(", ");
        for document_id in documents {
            values.push_bind(document_id);
        }
        values.push_unseparated(")");
        let mut out = BTreeMap::<(DocId, String), BTreeMap<String, u64>>::new();
        for row in query.build().fetch_all(&mut *tx).await? {
            let document_id: DocId = row.try_get("doc_id")?;
            let branch_id: String = row.try_get("branch_id")?;
            let hash: String = row.try_get("blob_hash")?;
            let length = u64::try_from(row.try_get::<i64, _>("length_octets")?)?;
            out.entry((document_id, branch_id))
                .or_default()
                .insert(hash, length);
        }
        tx.rollback().await?;
        Ok(out)
    }

    async fn prepare_facet_deltas(
        &self,
        drawer: &DrawerRepo,
        entries: &[FacetDelta],
    ) -> Res<Preparation> {
        let mut grouped = BTreeMap::<(DocId, String), Vec<&FacetDelta>>::new();
        for delta in entries {
            grouped
                .entry((delta.key.document_id.clone(), delta.key.branch_id.0.clone()))
                .or_default()
                .push(delta);
        }
        if grouped.is_empty() {
            return Ok(Preparation::Ready(Vec::new()));
        }

        let branches = grouped.keys().cloned().collect::<Vec<_>>();
        let mut read_tx = self.sql.read_pool.begin().await?;
        let prior_states = self.load_branch_states_in(&mut read_tx, &branches).await?;
        read_tx.rollback().await?;

        let mut prepared = Vec::with_capacity(grouped.len());
        for ((document_id, branch_id), deltas) in grouped {
            let branch_id = BranchId(branch_id);
            let prior = prior_states
                .get(&(document_id.clone(), branch_id.0.clone()))
                .cloned()
                .unwrap_or_default();
            let mut next = prior.clone();
            let mut seen_branch_heads = None;
            for delta in deltas {
                if let Some(seen) = &seen_branch_heads {
                    if seen != &delta.current_branch_heads {
                        return Err(ferr!(
                            "BlobPin facet deltas disagree on branch heads in one revision"
                        ));
                    }
                } else {
                    seen_branch_heads = Some(delta.current_branch_heads.clone());
                }
                let Some(current) = &delta.current else {
                    next.remove(&delta.key.facet_key.id);
                    continue;
                };
                let value = match drawer
                    .hydrate_facet_value_at_heads(
                        &delta.key.branch_id,
                        &current.branch_heads,
                        &delta.key.facet_key,
                    )
                    .await?
                {
                    crate::drawer::ExactFacetValueHydration::Deferred => {
                        return Ok(Preparation::Deferred);
                    }
                    crate::drawer::ExactFacetValueHydration::Absent => {
                        next.remove(&delta.key.facet_key.id);
                        continue;
                    }
                    crate::drawer::ExactFacetValueHydration::Present(value) => value,
                };
                let pin = match daybook_types::doc::WellKnownFacet::from_json(
                    value,
                    WellKnownFacetTag::BlobPin,
                )
                .wrap_err("decode BlobPin facet")?
                {
                    daybook_types::doc::WellKnownFacet::BlobPin(pin) => pin,
                    other => eyre::bail!("expected BlobPin facet, got {:?}", other.tag()),
                };
                delta
                    .key
                    .facet_key
                    .id
                    .parse::<crate::blobs::BlobId>()
                    .map_err(|_| ferr!("BlobPin facet id is not a valid blob id"))?;
                next.insert(delta.key.facet_key.id.clone(), pin.length_octets);
            }
            prepared.push(PreparedBranchDelta {
                document_id,
                branch_id,
                branch_heads: seen_branch_heads.expect("grouped BlobPin deltas non-empty"),
                prior,
                next,
            });
        }
        Ok(Preparation::Ready(prepared))
    }

    async fn replace_branch_state_in<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        prepared: &PreparedBranchDelta,
    ) -> Res<()> {
        // FIXME: this file is noto using query macros
        sqlx::query("DELETE FROM doc_blob_pins WHERE doc_id = ? AND branch_id = ?")
            .bind(&prepared.document_id)
            .bind(&prepared.branch_id.0)
            .execute(&mut **tx)
            .await?;
        let Some(branch_heads) = &prepared.branch_heads else {
            return Ok(());
        };
        if prepared.next.is_empty() {
            return Ok(());
        }
        let serialized_heads =
            serde_json::to_string(&am_utils_rs::serialize_commit_heads(&branch_heads.0))
                .expect(ERROR_JSON);
        let rows: Vec<(&str, i64)> = prepared
            .next
            .iter()
            .map(|(hash, length)| {
                Ok((
                    hash.as_str(),
                    i64::try_from(*length)
                        .map_err(|_| ferr!("BlobPin length exceeds SQLite INTEGER range"))?,
                ))
            })
            .collect::<Res<_>>()?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO doc_blob_pins(doc_id, branch_id, blob_hash, length_octets, origin_heads) ",
        );
        query.push_values(rows.iter(), |mut row, (hash, length)| {
            row.push_bind(&prepared.document_id)
                .push_bind(&prepared.branch_id.0)
                .push_bind(hash)
                .push_bind(*length)
                .push_bind(&serialized_heads);
        });
        query.push(" ON CONFLICT(doc_id, branch_id, blob_hash) DO UPDATE SET");
        query.push(" length_octets = excluded.length_octets, origin_heads = excluded.origin_heads");
        query.build().execute(&mut **tx).await?;
        Ok(())
    }

    async fn apply_prepared_in_context<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        prepared: &[PreparedBranchDelta],
    ) -> Res<()> {
        let branches = prepared
            .iter()
            .map(|branch| (branch.document_id.clone(), branch.branch_id.0.clone()))
            .collect::<Vec<_>>();
        let authoritative = self.load_branch_states_in(tx, &branches).await?;
        for branch in prepared {
            let actual_prior = authoritative
                .get(&(branch.document_id.clone(), branch.branch_id.0.clone()))
                .cloned()
                .unwrap_or_default();
            if actual_prior != branch.prior {
                return Err(ferr!(
                    "blob-pins-part branch state changed while preparing revision"
                ));
            }
            self.replace_branch_state_in(tx, branch).await?;
        }
        Ok(())
    }

    async fn reconcile_part_store(
        &self,
        before: &BTreeMap<(DocId, String), BTreeMap<String, u64>>,
        prepared: &[PreparedBranchDelta],
    ) -> Res<()> {
        let documents = prepared
            .iter()
            .map(|branch| branch.document_id.clone())
            .collect::<BTreeSet<_>>();
        let mut after = before.clone();
        for branch in prepared {
            let key = (branch.document_id.clone(), branch.branch_id.0.clone());
            if branch.branch_heads.is_some() {
                after.insert(key, branch.next.clone());
            } else {
                after.remove(&key);
            }
        }
        for document_id in &documents {
            let part_id = crate::blobs::blob_inventory_part_id_from_doc_id(document_id);
            self.part_store.ensure_part(part_id).await?;
            let old_hashes = before
                .iter()
                .filter(|((doc_id, _), _)| doc_id == document_id)
                .flat_map(|(_, state)| state.keys().cloned())
                .collect::<BTreeSet<_>>();
            let new_hashes = after
                .iter()
                .filter(|((doc_id, _), _)| doc_id == document_id)
                .flat_map(|(_, state)| state.keys().cloned())
                .collect::<BTreeSet<_>>();
            for branch in prepared
                .iter()
                .filter(|branch| &branch.document_id == document_id)
            {
                for (hash, length) in &branch.next {
                    self.part_store
                        .set_obj_payload(
                            crate::blobs::blob_id_from_hash(hash),
                            serde_json::json!({ "lengthOctets": length }),
                        )
                        .await?;
                }
            }
            for hash in new_hashes.difference(&old_hashes) {
                self.part_store
                    .add_obj_to_parts(crate::blobs::blob_id_from_hash(hash), vec![part_id])
                    .await?;
            }
            for hash in old_hashes.difference(&new_hashes) {
                self.part_store
                    .remove_obj_from_part(crate::blobs::blob_id_from_hash(hash), part_id)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn list_hashes_for_doc(&self, doc_id: &DocId) -> Res<Vec<String>> {
        Ok(sqlx::query_scalar(
            r#"SELECT DISTINCT blob_hash
                 FROM doc_blob_pins
                WHERE doc_id = ?
                ORDER BY blob_hash ASC"#,
        )
        .bind(doc_id)
        .fetch_all(&self.sql.read_pool)
        .await?)
    }

    pub async fn list_hashes_for_doc_branch(
        &self,
        doc_id: &DocId,
        branch_path: &BranchPathBuf,
    ) -> Res<Vec<String>> {
        let branch_id = if branch_path.as_str() == "main" {
            doc_id.clone()
        } else {
            branch_path.as_str().to_owned()
        };
        Ok(sqlx::query_scalar(
            r#"SELECT DISTINCT blob_hash
                 FROM doc_blob_pins
                WHERE doc_id = ?
                  AND branch_id = ?
                ORDER BY blob_hash ASC"#,
        )
        .bind(doc_id)
        .bind(branch_id)
        .fetch_all(&self.sql.read_pool)
        .await?)
    }
}

enum Preparation {
    Ready(Vec<PreparedBranchDelta>),
    Deferred,
}

struct PreparedBranchDelta {
    document_id: DocId,
    branch_id: BranchId,
    branch_heads: Option<ChangeHashSet>,
    prior: BTreeMap<String, u64>,
    next: BTreeMap<String, u64>,
}

/// Stop handle for the FacetSet blob-pin-part consumer.
pub(crate) struct BlobPinsPartConsumerStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BlobPinsPartConsumerStopToken {
    pub(crate) async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

pub(crate) async fn spawn_facet_set_blob_pins_part_consumer(
    drawer: Arc<DrawerRepo>,
    facet_set_store: Arc<FacetSetRevisionStore>,
    worker: Arc<BlobPinsPartWorker>,
    parent_cancel_token: CancellationToken,
) -> Res<BlobPinsPartConsumerStopToken> {
    let state = big_sync::SqliteDeltaWalkerStateRepo::new(
        worker.sql.read_pool.clone(),
        worker.sql.write_pool.clone(),
        "@daybook/core/blob-pins-part-worker",
        "facets",
    )
    .await
    .map_err(|error| ferr!("initializing blob-pins-part FacetSet state: {error}"))?;
    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    let worker_handle = tokio::spawn(async move {
        worker
            .run_facet_set_machine(drawer, facet_set_store, state, worker_cancel_token)
            .await
            .unwrap();
    });
    Ok(BlobPinsPartConsumerStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

impl BlobPinsPartWorker {
    async fn run_facet_set_machine(
        self: Arc<Self>,
        drawer: Arc<DrawerRepo>,
        facet_set_store: Arc<FacetSetRevisionStore>,
        state: big_sync::SqliteDeltaWalkerStateRepo,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let mut wake = drawer.subscribe_materialization_wake(None).await?;
        let mut walker = SerialDeltaWalker::open(
            facet_set_store.as_ref(),
            &state,
            FacetSetSelector::Tag(WellKnownFacetTag::BlobPin),
            RevisionReadLimits::default(),
        )
        .await
        .map_err(|error| ferr!("opening blob-pins-part FacetSet walker: {error}"))?;
        let mut pending = None;
        loop {
            let (revision, entries) = if let Some(pending) = pending.take() {
                pending
            } else {
                match tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => return Ok(()),
                    read = walker.next() => read,
                }
                .map_err(|error| ferr!("reading blob-pins-part FacetSet walker: {error}"))?
                {
                    RevisionRead::ReplayComplete { .. } => continue,
                    RevisionRead::Entries { revision, entries } => (revision, entries),
                }
            };
            let prepared = match self.prepare_facet_deltas(&drawer, &entries).await? {
                Preparation::Deferred => {
                    pending = Some((revision, entries));
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => return Ok(()),
                        result = wake.wait() => result?,
                    }
                    continue;
                }
                Preparation::Ready(prepared) => prepared,
            };
            let documents = prepared
                .iter()
                .map(|branch| branch.document_id.clone())
                .collect::<BTreeSet<_>>();
            let before = self.load_doc_states(&documents).await?;
            self.reconcile_part_store(&before, &prepared).await?;
            let mut settlement = walker
                .begin_settlement(revision)
                .await
                .map_err(|error| ferr!("beginning blob-pins-part settlement: {error}"))?;
            self.apply_prepared_in_context(settlement.context_mut(), &prepared)
                .await?;
            settlement
                .settle()
                .await
                .map_err(|error| ferr!("settling blob-pins-part FacetSet revision: {error}"))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_cx;
    use big_sync::DeltaWalkerStateRepo;
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

    async fn facet_walker_progress(worker: &BlobPinsPartWorker) -> Res<u64> {
        let state = big_sync::SqliteDeltaWalkerStateRepo::new(
            worker.sql.read_pool.clone(),
            worker.sql.write_pool.clone(),
            "@daybook/core/blob-pins-part-worker",
            "facets",
        )
        .await?;
        Ok(state.progress().await?.upstream_revision)
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
        let initial_progress = facet_walker_progress(&worker).await?;
        assert!(initial_progress > 0);

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
        let update_progress = facet_walker_progress(&worker).await?;
        assert!(update_progress > initial_progress);

        // Keep an independent branch so removing main's pin exercises
        // branch-scoped state rather than removing shared physical membership.
        let main_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, BranchPath::new("main"))
            .await?
            .ok_or_eyre("missing main branch heads")?;
        let branch_path = BranchPathBuf::from("/test/blob-pins-part-branch");
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
        wait_for_partition_member_count(blob_part_store, part_id, 1).await?;
        assert_eq!(
            worker.list_hashes_for_doc(&doc_id).await?,
            vec![hash_1.clone()]
        );

        // Removing main's pin must not remove the branch's physical
        // membership.
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![key_pin_1.clone()],
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
            worker.list_hashes_for_doc(&doc_id).await?,
            vec![hash_1.clone()]
        );

        let branch_heads = test_context
            .drawer_repo
            .get_branch_heads_for_path(&doc_id, &branch_path)
            .await?
            .ok_or_eyre("missing blob-pins-part branch heads")?;
        test_context
            .drawer_repo
            .update_at_heads(
                DocPatch {
                    id: doc_id.clone(),
                    facets_set: default(),
                    facets_remove: vec![key_pin_1],
                    user_path: None,
                },
                BranchPath::new(branch_path.as_str()),
                Some(branch_heads),
            )
            .await?;
        wait_for_partition_member_count(blob_part_store, part_id, 0).await?;
        assert_eq!(
            worker.list_hashes_for_doc(&doc_id).await?,
            Vec::<String>::new()
        );

        // 3. Delete document: the branch-scoped tombstones are already
        // empty, so the document deletion remains an idempotent no-op for
        // physical membership.
        test_context.drawer_repo.del(&doc_id).await?;
        wait_for_partition_member_count(blob_part_store, part_id, 0).await?;
        assert!(worker.list_hashes_for_doc(&doc_id).await?.is_empty());

        test_context.stop().await?;
        Ok(())
    }
}
