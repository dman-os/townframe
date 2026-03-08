use crate::interlude::*;

use std::str::FromStr;

use samod::DocumentId;
use sqlx::Row;

use crate::repo::BigRepo;
use crate::sync::{
    FullDoc, OpaqueCursor, PartitionCursorRequest, PartitionEvent, PartitionEventKind, PartitionId,
    PartitionSubscription, PartitionSummary, PartitionSyncError, PartitionSyncProvider, PeerKey,
    SubscriptionItem, MAX_GET_DOCS_FULL_DOC_IDS,
};

const META_NEXT_TXID_KEY: &str = "next_txid";

impl BigRepo {
    pub(super) async fn ensure_schema(&self) -> Res<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_repo_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO big_repo_meta(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO NOTHING
            "#,
        )
        .bind(META_NEXT_TXID_KEY)
        .bind("1")
        .execute(&self.state_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS partition_members(
                partition_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                PRIMARY KEY(partition_id, doc_id)
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_members_doc ON partition_members(doc_id)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS partition_membership_log(
                txid INTEGER PRIMARY KEY,
                partition_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_membership_log_partition_txid ON partition_membership_log(partition_id, txid)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS partition_doc_log(
                txid INTEGER PRIMARY KEY,
                partition_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                heads_json TEXT NOT NULL,
                change_count_hint INTEGER NOT NULL,
                kind TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_doc_log_partition_txid ON partition_doc_log(partition_id, txid)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS partition_doc_state(
                partition_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                heads_json TEXT NOT NULL,
                change_count_hint INTEGER NOT NULL,
                deleted INTEGER NOT NULL,
                latest_txid INTEGER NOT NULL,
                PRIMARY KEY(partition_id, doc_id)
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS doc_version_state(
                doc_id TEXT PRIMARY KEY,
                latest_heads_json TEXT NOT NULL,
                change_count_hint INTEGER NOT NULL,
                latest_txid INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        Ok(())
    }

    pub async fn add_doc_to_partition(&self, partition_id: &PartitionId, doc_id: &str) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        sqlx::query(
            "INSERT INTO partition_members(partition_id, doc_id) VALUES(?, ?) ON CONFLICT(partition_id, doc_id) DO NOTHING",
        )
        .bind(&partition_id.0)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;
        let membership_txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            "INSERT INTO partition_membership_log(txid, partition_id, doc_id, kind, created_at_ms) VALUES(?, ?, ?, 'upsert', ?)",
        )
        .bind(membership_txid as i64)
        .bind(&partition_id.0)
        .bind(doc_id)
        .bind(Timestamp::now().as_millisecond())
        .execute(&mut *tx)
        .await?;

        let mut initial_doc_event: Option<(u64, Vec<String>, u64)> = None;
        if let Some((heads_json, change_count_hint)) = sqlx::query_as::<_, (String, i64)>(
            "SELECT latest_heads_json, change_count_hint FROM doc_version_state WHERE doc_id = ?",
        )
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let doc_txid = alloc_txid(tx.as_mut()).await?;
            sqlx::query(
                r#"
                INSERT INTO partition_doc_log(
                    txid, partition_id, doc_id, heads_json, change_count_hint, kind, created_at_ms
                ) VALUES(?, ?, ?, ?, ?, 'changed', ?)
                "#,
            )
            .bind(doc_txid as i64)
            .bind(&partition_id.0)
            .bind(doc_id)
            .bind(&heads_json)
            .bind(change_count_hint)
            .bind(Timestamp::now().as_millisecond())
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO partition_doc_state(
                    partition_id, doc_id, heads_json, change_count_hint, deleted, latest_txid
                ) VALUES(?, ?, ?, ?, 0, ?)
                ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                    heads_json = excluded.heads_json,
                    change_count_hint = excluded.change_count_hint,
                    deleted = 0,
                    latest_txid = excluded.latest_txid
                "#,
            )
            .bind(&partition_id.0)
            .bind(doc_id)
            .bind(&heads_json)
            .bind(change_count_hint)
            .bind(doc_txid as i64)
            .execute(&mut *tx)
            .await?;
            let heads: Vec<String> = serde_json::from_str(&heads_json)?;
            initial_doc_event = Some((doc_txid, heads, change_count_hint.max(0) as u64));
        }
        tx.commit().await?;

        let _ = self.partition_events_tx.send(PartitionEvent {
            cursor: OpaqueCursor::from_txid(membership_txid),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::MemberUpsert {
                doc_id: doc_id.to_owned(),
            },
        });
        if let Some((doc_txid, heads, change_count_hint)) = initial_doc_event {
            let _ = self.partition_events_tx.send(PartitionEvent {
                cursor: OpaqueCursor::from_txid(doc_txid),
                partition_id: partition_id.clone(),
                kind: PartitionEventKind::DocChanged {
                    doc_id: doc_id.to_owned(),
                    heads,
                    change_count_hint,
                },
            });
        }
        Ok(())
    }

    pub async fn remove_doc_from_partition(
        &self,
        partition_id: &PartitionId,
        doc_id: &str,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        sqlx::query("DELETE FROM partition_members WHERE partition_id = ? AND doc_id = ?")
            .bind(&partition_id.0)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        let txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            "INSERT INTO partition_membership_log(txid, partition_id, doc_id, kind, created_at_ms) VALUES(?, ?, ?, 'removed', ?)",
        )
        .bind(txid as i64)
        .bind(&partition_id.0)
        .bind(doc_id)
        .bind(Timestamp::now().as_millisecond())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let _ = self.partition_events_tx.send(PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::MemberRemoved {
                doc_id: doc_id.to_owned(),
            },
        });
        Ok(())
    }

    pub(super) async fn record_doc_heads_change(
        &self,
        doc_id: &samod::DocumentId,
        heads: Vec<automerge::ChangeHash>,
    ) -> Res<()> {
        let doc_id = doc_id.to_string();
        let serialized_heads = crate::serialize_commit_heads(&heads);
        let heads_json = serde_json::to_string(&serialized_heads)?;
        let mut tx = self.state_pool.begin().await?;

        let previous_change_count = sqlx::query_scalar::<_, i64>(
            "SELECT change_count_hint FROM doc_version_state WHERE doc_id = ?",
        )
        .bind(&doc_id)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(0);
        let change_count_hint = (previous_change_count as u64).saturating_add(1);

        let doc_txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            r#"
            INSERT INTO doc_version_state(doc_id, latest_heads_json, change_count_hint, latest_txid)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
                latest_heads_json = excluded.latest_heads_json,
                change_count_hint = excluded.change_count_hint,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(&doc_id)
        .bind(&heads_json)
        .bind(change_count_hint as i64)
        .bind(doc_txid as i64)
        .execute(&mut *tx)
        .await?;

        let partition_rows =
            sqlx::query("SELECT partition_id FROM partition_members WHERE doc_id = ?")
                .bind(&doc_id)
                .fetch_all(&mut *tx)
                .await?;

        let mut emitted = Vec::with_capacity(partition_rows.len());
        for row in partition_rows {
            let partition_id = row.try_get::<String, _>("partition_id")?;
            let txid = alloc_txid(tx.as_mut()).await?;
            sqlx::query(
                r#"
                INSERT INTO partition_doc_log(
                    txid, partition_id, doc_id, heads_json, change_count_hint, kind, created_at_ms
                ) VALUES(?, ?, ?, ?, ?, 'changed', ?)
                "#,
            )
            .bind(txid as i64)
            .bind(&partition_id)
            .bind(&doc_id)
            .bind(&heads_json)
            .bind(change_count_hint as i64)
            .bind(Timestamp::now().as_millisecond())
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO partition_doc_state(
                    partition_id, doc_id, heads_json, change_count_hint, deleted, latest_txid
                ) VALUES(?, ?, ?, ?, 0, ?)
                ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                    heads_json = excluded.heads_json,
                    change_count_hint = excluded.change_count_hint,
                    deleted = 0,
                    latest_txid = excluded.latest_txid
                "#,
            )
            .bind(&partition_id)
            .bind(&doc_id)
            .bind(&heads_json)
            .bind(change_count_hint as i64)
            .bind(txid as i64)
            .execute(&mut *tx)
            .await?;
            emitted.push((partition_id, txid));
        }

        tx.commit().await?;
        for (partition_id, txid) in emitted {
            let _ = self.partition_events_tx.send(PartitionEvent {
                cursor: OpaqueCursor::from_txid(txid),
                partition_id: PartitionId(partition_id),
                kind: PartitionEventKind::DocChanged {
                    doc_id: doc_id.clone(),
                    heads: serialized_heads.clone(),
                    change_count_hint,
                },
            });
        }
        Ok(())
    }
}

#[async_trait]
impl PartitionSyncProvider for BigRepo {
    async fn list_partitions_for_peer(&self, _peer: &PeerKey) -> Res<Vec<PartitionSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.partition_id AS partition_id,
                COALESCE(pm.member_count, 0) AS member_count,
                COALESCE(mx.latest_txid, 0) AS latest_txid
            FROM (
                SELECT DISTINCT partition_id FROM partition_members
                UNION
                SELECT DISTINCT partition_id FROM partition_membership_log
                UNION
                SELECT DISTINCT partition_id FROM partition_doc_log
            ) p
            LEFT JOIN (
                SELECT partition_id, COUNT(1) AS member_count
                FROM partition_members
                GROUP BY partition_id
            ) pm ON pm.partition_id = p.partition_id
            LEFT JOIN (
                SELECT partition_id, MAX(txid) AS latest_txid
                FROM (
                    SELECT partition_id, txid FROM partition_membership_log
                    UNION ALL
                    SELECT partition_id, txid FROM partition_doc_log
                )
                GROUP BY partition_id
            ) mx ON mx.partition_id = p.partition_id
            ORDER BY p.partition_id
            "#,
        )
        .fetch_all(&self.state_pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let partition_id: String = row.try_get("partition_id")?;
                let member_count: i64 = row.try_get("member_count")?;
                let latest_txid: i64 = row.try_get("latest_txid")?;
                eyre::Ok(PartitionSummary {
                    partition_id: PartitionId(partition_id),
                    latest_cursor: OpaqueCursor::from_txid(latest_txid.max(0) as u64),
                    member_count: member_count.max(0) as u64,
                })
            })
            .collect()
    }

    async fn get_partition_events(
        &self,
        _peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
    ) -> Res<Vec<PartitionEvent>> {
        let mut out = Vec::new();
        for req in reqs {
            ensure_partition_exists(&self.state_pool, &req.partition_id).await?;
            match &req.since {
                Some(cursor) => {
                    let txid = cursor
                        .to_txid()
                        .map_err(|_| PartitionSyncError::InvalidCursor {
                            cursor: cursor.clone(),
                        })?;
                    append_replay_events(&self.state_pool, &req.partition_id, txid, &mut out)
                        .await?;
                }
                None => {
                    append_snapshot_events(&self.state_pool, &req.partition_id, &mut out).await?
                }
            }
        }
        out.sort_by(cmp_partition_events);
        Ok(out)
    }

    async fn get_docs_full(&self, _peer: &PeerKey, doc_ids: &[String]) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(PartitionSyncError::TooManyDocIds {
                requested: doc_ids.len(),
                max: MAX_GET_DOCS_FULL_DOC_IDS,
            }
            .into_report());
        }
        let mut out = Vec::new();
        for doc_id in doc_ids {
            let parsed = DocumentId::from_str(doc_id)
                .map_err(|err| ferr!("invalid document id '{doc_id}': {err}"))?;
            let Some(handle) = self.repo.find(parsed).await? else {
                continue;
            };
            let bytes = handle.with_document(|doc| doc.save());
            out.push(FullDoc {
                doc_id: doc_id.clone(),
                automerge_save: bytes,
            });
        }
        Ok(out)
    }

    async fn subscribe(
        &self,
        peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
        capacity: usize,
    ) -> Res<PartitionSubscription> {
        let replay = self.get_partition_events(peer, reqs).await?;
        let mut live_rx = self.partition_events_tx.subscribe();
        let high_watermark = replay
            .iter()
            .filter_map(|event| event.cursor.to_txid().ok())
            .max()
            .unwrap_or(0);
        let requested: HashSet<PartitionId> =
            reqs.iter().map(|item| item.partition_id.clone()).collect();
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            for event in replay {
                if tx.send(SubscriptionItem::Event(event)).await.is_err() {
                    return;
                }
            }
            if tx.send(SubscriptionItem::SnapshotComplete).await.is_err() {
                return;
            }
            while let Ok(event) = live_rx.recv().await {
                if !requested.contains(&event.partition_id) {
                    continue;
                }
                let Ok(txid) = event.cursor.to_txid() else {
                    continue;
                };
                if txid <= high_watermark {
                    continue;
                }
                if tx.send(SubscriptionItem::Event(event)).await.is_err() {
                    return;
                }
            }
        });
        Ok(PartitionSubscription { rx })
    }
}

async fn alloc_txid(conn: &mut sqlx::SqliteConnection) -> Res<u64> {
    let current: String = sqlx::query_scalar("SELECT value FROM big_repo_meta WHERE key = ?")
        .bind(META_NEXT_TXID_KEY)
        .fetch_one(&mut *conn)
        .await?;
    let out: u64 = current.parse().wrap_err("invalid next_txid in sqlite")?;
    let next = out.saturating_add(1);
    sqlx::query("UPDATE big_repo_meta SET value = ? WHERE key = ?")
        .bind(next.to_string())
        .bind(META_NEXT_TXID_KEY)
        .execute(&mut *conn)
        .await?;
    Ok(out)
}

async fn ensure_partition_exists(pool: &sqlx::SqlitePool, partition_id: &PartitionId) -> Res<()> {
    let found: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM partition_members WHERE partition_id = ?
            UNION
            SELECT 1 FROM partition_membership_log WHERE partition_id = ?
            UNION
            SELECT 1 FROM partition_doc_log WHERE partition_id = ?
        )
        "#,
    )
    .bind(&partition_id.0)
    .bind(&partition_id.0)
    .bind(&partition_id.0)
    .fetch_one(pool)
    .await?;
    if found == 1 {
        return Ok(());
    }
    Err(PartitionSyncError::UnknownPartition {
        partition_id: partition_id.clone(),
    }
    .into_report())
}

async fn append_snapshot_events(
    pool: &sqlx::SqlitePool,
    partition_id: &PartitionId,
    out: &mut Vec<PartitionEvent>,
) -> Res<()> {
    let latest_txid: i64 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(txid), 0) FROM (
            SELECT txid FROM partition_membership_log WHERE partition_id = ?
            UNION ALL
            SELECT txid FROM partition_doc_log WHERE partition_id = ?
        )
        "#,
    )
    .bind(&partition_id.0)
    .bind(&partition_id.0)
    .fetch_one(pool)
    .await?;
    let snapshot_cursor = OpaqueCursor::from_txid(latest_txid.max(0) as u64);

    let member_rows =
        sqlx::query("SELECT doc_id FROM partition_members WHERE partition_id = ? ORDER BY doc_id")
            .bind(&partition_id.0)
            .fetch_all(pool)
            .await?;
    for row in member_rows {
        let doc_id: String = row.try_get("doc_id")?;
        out.push(PartitionEvent {
            cursor: snapshot_cursor.clone(),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::MemberUpsert { doc_id },
        });
    }

    let doc_rows = sqlx::query(
        "SELECT doc_id, heads_json, change_count_hint FROM partition_doc_state WHERE partition_id = ? AND deleted = 0 ORDER BY doc_id",
    )
    .bind(&partition_id.0)
    .fetch_all(pool)
    .await?;
    for row in doc_rows {
        let doc_id: String = row.try_get("doc_id")?;
        let heads_json: String = row.try_get("heads_json")?;
        let change_count_hint: i64 = row.try_get("change_count_hint")?;
        let heads: Vec<String> = serde_json::from_str(&heads_json)?;
        out.push(PartitionEvent {
            cursor: snapshot_cursor.clone(),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::DocChanged {
                doc_id,
                heads,
                change_count_hint: change_count_hint.max(0) as u64,
            },
        });
    }
    Ok(())
}

async fn append_replay_events(
    pool: &sqlx::SqlitePool,
    partition_id: &PartitionId,
    since_txid: u64,
    out: &mut Vec<PartitionEvent>,
) -> Res<()> {
    let membership_rows = sqlx::query(
        "SELECT txid, doc_id, kind FROM partition_membership_log WHERE partition_id = ? AND txid > ? ORDER BY txid",
    )
    .bind(&partition_id.0)
    .bind(since_txid as i64)
    .fetch_all(pool)
    .await?;
    for row in membership_rows {
        let txid: i64 = row.try_get("txid")?;
        let doc_id: String = row.try_get("doc_id")?;
        let kind: String = row.try_get("kind")?;
        let kind = match kind.as_str() {
            "upsert" => PartitionEventKind::MemberUpsert { doc_id },
            "removed" => PartitionEventKind::MemberRemoved { doc_id },
            other => eyre::bail!("invalid membership event kind '{other}'"),
        };
        out.push(PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid.max(0) as u64),
            partition_id: partition_id.clone(),
            kind,
        });
    }

    let doc_rows = sqlx::query(
        "SELECT txid, doc_id, heads_json, change_count_hint, kind FROM partition_doc_log WHERE partition_id = ? AND txid > ? ORDER BY txid",
    )
    .bind(&partition_id.0)
    .bind(since_txid as i64)
    .fetch_all(pool)
    .await?;
    for row in doc_rows {
        let txid: i64 = row.try_get("txid")?;
        let doc_id: String = row.try_get("doc_id")?;
        let heads_json: String = row.try_get("heads_json")?;
        let change_count_hint: i64 = row.try_get("change_count_hint")?;
        let kind: String = row.try_get("kind")?;
        let kind = match kind.as_str() {
            "changed" => PartitionEventKind::DocChanged {
                doc_id,
                heads: serde_json::from_str(&heads_json)?,
                change_count_hint: change_count_hint.max(0) as u64,
            },
            "deleted" => PartitionEventKind::DocDeleted {
                doc_id,
                change_count_hint: change_count_hint.max(0) as u64,
            },
            other => eyre::bail!("invalid doc event kind '{other}'"),
        };
        out.push(PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid.max(0) as u64),
            partition_id: partition_id.clone(),
            kind,
        });
    }
    Ok(())
}

fn event_kind_order(kind: &PartitionEventKind) -> u8 {
    match kind {
        PartitionEventKind::MemberUpsert { .. } => 1,
        PartitionEventKind::MemberRemoved { .. } => 2,
        PartitionEventKind::DocChanged { .. } => 3,
        PartitionEventKind::DocDeleted { .. } => 4,
    }
}

fn cmp_partition_events(left: &PartitionEvent, right: &PartitionEvent) -> std::cmp::Ordering {
    let left_txid = left.cursor.to_txid().unwrap_or(0);
    let right_txid = right.cursor.to_txid().unwrap_or(0);
    left_txid
        .cmp(&right_txid)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
        .then_with(|| event_kind_order(&left.kind).cmp(&event_kind_order(&right.kind)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::repo::{BigRepo, BigRepoConfig};
    use automerge::transaction::Transactable;

    async fn boot_big_repo() -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string("bigrepo-test-peer".to_string()))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        BigRepo::boot(repo, BigRepoConfig::new("sqlite::memory:".to_string())).await
    }

    #[tokio::test]
    async fn bigrepo_emits_partition_doc_events_on_doc_write() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = handle.document_id().to_string();
        let partition_id = PartitionId("p-main".into());

        big_repo
            .add_doc_to_partition(&partition_id, &doc_id)
            .await?;
        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "k", "v")
                    .expect("failed setting test key");
                tx.commit();
            })
            .await?;

        let events = PartitionSyncProvider::get_partition_events(
            &*big_repo,
            &PeerKey("peer-a".into()),
            &[PartitionCursorRequest {
                partition_id: partition_id.clone(),
                since: None,
            }],
        )
        .await?;

        assert!(events
            .iter()
            .any(|evt| matches!(evt.kind, PartitionEventKind::MemberUpsert { .. })));
        assert!(events
            .iter()
            .any(|evt| matches!(evt.kind, PartitionEventKind::DocChanged { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn bigrepo_full_doc_fetch_returns_saved_bytes() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = handle.document_id().to_string();
        let partition_id = PartitionId("p-fetch".into());
        big_repo
            .add_doc_to_partition(&partition_id, &doc_id)
            .await?;
        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "name", "doc-a")
                    .expect("failed setting test key");
                tx.commit();
            })
            .await?;

        let docs = PartitionSyncProvider::get_docs_full(
            &*big_repo,
            &PeerKey("peer-a".into()),
            std::slice::from_ref(&doc_id),
        )
        .await?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].doc_id, doc_id);
        assert!(!docs[0].automerge_save.is_empty());
        Ok(())
    }
}
