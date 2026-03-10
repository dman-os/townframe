use crate::interlude::*;

use sqlx::Row;

use crate::repo::BigRepo;
use crate::sync::{
    cursor, FullDoc, GetPartitionDocEventsRequest, GetPartitionDocEventsResponse,
    GetPartitionMemberEventsRequest, GetPartitionMemberEventsResponse, PartitionCursorPage,
    PartitionCursorRequest, PartitionDocEvent, PartitionDocEventDeets, PartitionEvent,
    PartitionEventDeets, PartitionId, PartitionMemberEvent, PartitionMemberEventDeets,
    PartitionSummary, PartitionSyncError, PeerKey, SubPartitionsRequest, SubscriptionItem,
    SubscriptionStreamKind, MAX_GET_DOCS_FULL_DOC_IDS,
};

const META_NEXT_TXID_KEY: &str = "next_txid";

impl BigRepo {
    pub(super) async fn ensure_schema(&self) -> Res<()> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS big_repo_meta(
                key TEXT PRIMARY KEY
                ,value TEXT NOT NULL
            )"#,
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
            CREATE TABLE IF NOT EXISTS partition_membership_state(
                partition_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                present INTEGER NOT NULL,
                added_at_txid INTEGER NULL,
                removed_at_txid INTEGER NULL,
                latest_txid INTEGER NOT NULL,
                PRIMARY KEY(partition_id, doc_id)
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_membership_state_doc ON partition_membership_state(doc_id)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_membership_state_partition_latest ON partition_membership_state(partition_id, latest_txid)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS partition_doc_state(
                partition_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                deleted INTEGER NOT NULL,
                latest_txid INTEGER NOT NULL,
                PRIMARY KEY(partition_id, doc_id)
            )
            "#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_doc_state_partition_latest ON partition_doc_state(partition_id, latest_txid)",
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
        let membership_txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            r#"
            INSERT INTO partition_membership_state(
                partition_id, doc_id, present, added_at_txid, removed_at_txid, latest_txid
            ) VALUES(?, ?, 1, ?, NULL, ?)
            ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                present = 1,
                added_at_txid = excluded.added_at_txid,
                removed_at_txid = NULL,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(partition_id)
        .bind(doc_id)
        .bind(membership_txid as i64)
        .bind(membership_txid as i64)
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
                INSERT INTO partition_doc_state(
                    partition_id, doc_id, deleted, latest_txid
                ) VALUES(?, ?, 0, ?)
                ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                    deleted = 0,
                    latest_txid = excluded.latest_txid
                "#,
            )
            .bind(partition_id)
            .bind(doc_id)
            .bind(doc_txid as i64)
            .execute(&mut *tx)
            .await?;
            let heads: Vec<String> = serde_json::from_str(&heads_json)?;
            initial_doc_event = Some((doc_txid, heads, change_count_hint.max(0) as u64));
        }
        tx.commit().await?;

        let _ = self.partition_events_tx.send(PartitionEvent {
            cursor: cursor::from_txid(membership_txid),
            partition_id: partition_id.clone(),
            deets: PartitionEventDeets::MemberUpsert {
                doc_id: doc_id.to_owned(),
            },
        });
        if let Some((doc_txid, heads, change_count_hint)) = initial_doc_event {
            let _ = self.partition_events_tx.send(PartitionEvent {
                cursor: cursor::from_txid(doc_txid),
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::DocChanged {
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
        let membership_txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            r#"
            INSERT INTO partition_membership_state(
                partition_id, doc_id, present, added_at_txid, removed_at_txid, latest_txid
            ) VALUES(?, ?, 0, NULL, ?, ?)
            ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                present = 0,
                removed_at_txid = excluded.removed_at_txid,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(partition_id)
        .bind(doc_id)
        .bind(membership_txid as i64)
        .bind(membership_txid as i64)
        .execute(&mut *tx)
        .await?;
        let mut doc_deleted_event: Option<(u64, u64)> = None;
        if let Some(change_count_hint) = sqlx::query_scalar::<_, i64>(
            "SELECT change_count_hint FROM doc_version_state WHERE doc_id = ?",
        )
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let doc_txid = alloc_txid(tx.as_mut()).await?;
            sqlx::query(
                "UPDATE partition_doc_state SET deleted = 1, latest_txid = ? WHERE partition_id = ? AND doc_id = ?",
            )
            .bind(doc_txid as i64)
            .bind(partition_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
            doc_deleted_event = Some((doc_txid, change_count_hint.max(0) as u64));
        }
        tx.commit().await?;

        let _ = self.partition_events_tx.send(PartitionEvent {
            cursor: cursor::from_txid(membership_txid),
            partition_id: partition_id.clone(),
            deets: PartitionEventDeets::MemberRemoved {
                doc_id: doc_id.to_owned(),
            },
        });
        if let Some((doc_txid, change_count_hint)) = doc_deleted_event {
            let _ = self.partition_events_tx.send(PartitionEvent {
                cursor: cursor::from_txid(doc_txid),
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::DocDeleted {
                    doc_id: doc_id.to_owned(),
                    change_count_hint,
                },
            });
        }
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

        let partition_rows = sqlx::query(
            "SELECT partition_id FROM partition_membership_state WHERE doc_id = ? AND present = 1",
        )
        .bind(&doc_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut emitted = Vec::with_capacity(partition_rows.len());
        for row in partition_rows {
            let partition_id = row.try_get::<String, _>("partition_id")?;
            let txid = alloc_txid(tx.as_mut()).await?;
            sqlx::query(
                r#"
                INSERT INTO partition_doc_state(
                    partition_id, doc_id, deleted, latest_txid
                ) VALUES(?, ?, 0, ?)
                ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                    deleted = 0,
                    latest_txid = excluded.latest_txid
                "#,
            )
            .bind(&partition_id)
            .bind(&doc_id)
            .bind(txid as i64)
            .execute(&mut *tx)
            .await?;
            emitted.push((partition_id, txid));
        }

        tx.commit().await?;
        for (partition_id, txid) in emitted {
            let _ = self.partition_events_tx.send(PartitionEvent {
                cursor: cursor::from_txid(txid),
                partition_id,
                deets: PartitionEventDeets::DocChanged {
                    doc_id: doc_id.clone(),
                    heads: serialized_heads.clone(),
                    change_count_hint,
                },
            });
        }
        Ok(())
    }

    pub async fn partition_member_count(&self, part_id: &PartitionId) -> Res<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM partition_membership_state WHERE partition_id = ? AND present = 1",
        )
        .bind(part_id)
        .fetch_one(&self.state_pool)
        .await?;
        Ok(count)
    }

    pub async fn is_doc_present_in_partition_state(
        &self,
        partition_id: &PartitionId,
        doc_id: &str,
    ) -> Res<bool> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM partition_doc_state WHERE partition_id = ? AND doc_id = ? AND deleted = 0)",
        )
        .bind(partition_id)
        .bind(doc_id)
        .fetch_one(&self.state_pool)
        .await?;
        Ok(exists == 1)
    }

    pub async fn list_partitions_for_peer(&self, _peer: &PeerKey) -> Res<Vec<PartitionSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.partition_id AS partition_id,
                COALESCE(pm.member_count, 0) AS member_count,
                COALESCE(mx.latest_txid, 0) AS latest_txid
            FROM (
                SELECT DISTINCT partition_id FROM partition_membership_state
                UNION
                SELECT DISTINCT partition_id FROM partition_doc_state
            ) p
            LEFT JOIN (
                SELECT partition_id, COUNT(1) AS member_count
                FROM partition_membership_state
                WHERE present = 1
                GROUP BY partition_id
            ) pm ON pm.partition_id = p.partition_id
            LEFT JOIN (
                SELECT partition_id, MAX(txid) AS latest_txid
                FROM (
                    SELECT partition_id, latest_txid AS txid FROM partition_membership_state
                    UNION ALL
                    SELECT partition_id, latest_txid AS txid FROM partition_doc_state
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
                    partition_id,
                    latest_cursor: cursor::from_txid(latest_txid.max(0) as u64),
                    member_count: member_count.max(0) as u64,
                })
            })
            .collect()
    }

    pub async fn get_partition_member_events_for_peer(
        &self,
        _peer: &PeerKey,
        req: &GetPartitionMemberEventsRequest,
    ) -> Res<GetPartitionMemberEventsResponse> {
        let limit = req.limit.max(1) as usize;
        let mut events = Vec::new();
        let mut cursors = Vec::with_capacity(req.partitions.len());
        for part in &req.partitions {
            ensure_partition_exists(&self.state_pool, &part.partition_id).await?;
            let (mut part_events, next_cursor, has_more) =
                load_member_partition_page(&self.state_pool, part, limit).await?;
            events.append(&mut part_events);
            cursors.push(PartitionCursorPage {
                partition_id: part.partition_id.clone(),
                next_cursor,
                has_more,
            });
        }
        events.sort_by(cmp_member_events);
        Ok(GetPartitionMemberEventsResponse { events, cursors })
    }

    pub async fn get_partition_doc_events_for_peer(
        &self,
        _peer: &PeerKey,
        req: &GetPartitionDocEventsRequest,
    ) -> Res<GetPartitionDocEventsResponse> {
        let limit = req.limit.max(1) as usize;
        let mut events = Vec::new();
        let mut cursors = Vec::with_capacity(req.partitions.len());
        for part in &req.partitions {
            ensure_partition_exists(&self.state_pool, &part.partition_id).await?;
            let (mut part_events, next_cursor, has_more) =
                load_doc_partition_page(&self.state_pool, part, limit).await?;
            events.append(&mut part_events);
            cursors.push(PartitionCursorPage {
                partition_id: part.partition_id.clone(),
                next_cursor,
                has_more,
            });
        }
        events.sort_by(cmp_doc_events);
        Ok(GetPartitionDocEventsResponse { events, cursors })
    }

    pub async fn get_docs_full_for_peer(
        &self,
        _peer: &PeerKey,
        doc_ids: &[String],
    ) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(PartitionSyncError::TooManyDocIds {
                requested: doc_ids.len(),
                max: MAX_GET_DOCS_FULL_DOC_IDS,
            }
            .into_report());
        }
        let mut out = Vec::new();
        for doc_id in doc_ids {
            let is_member: i64 = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM partition_membership_state WHERE doc_id = ? AND present = 1)",
            )
            .bind(doc_id)
            .fetch_one(&self.state_pool)
            .await?;
            if is_member != 1 {
                continue;
            }
            let Some(handle) = self.handle_cache.get(doc_id) else {
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

    pub async fn is_doc_accessible_for_peer(&self, _peer: &PeerKey, doc_id: &str) -> Res<bool> {
        let exists: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM partition_membership_state WHERE doc_id = ? AND present = 1)")
                .bind(doc_id)
                .fetch_one(&self.state_pool)
                .await?;
        Ok(exists == 1)
    }

    pub async fn subscribe_partition_events_for_peer(
        &self,
        peer: &PeerKey,
        reqs: &SubPartitionsRequest,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<SubscriptionItem>> {
        let mut live_rx = self.partition_events_tx.subscribe();
        let member_req = GetPartitionMemberEventsRequest {
            partitions: reqs
                .partitions
                .iter()
                .map(|item| PartitionCursorRequest {
                    partition_id: item.partition_id.clone(),
                    since: item.since_member.clone(),
                })
                .collect(),
            limit: u32::MAX,
        };
        let doc_req = GetPartitionDocEventsRequest {
            partitions: reqs
                .partitions
                .iter()
                .map(|item| PartitionCursorRequest {
                    partition_id: item.partition_id.clone(),
                    since: item.since_doc.clone(),
                })
                .collect(),
            limit: u32::MAX,
        };
        let replay_members = self
            .get_partition_member_events_for_peer(peer, &member_req)
            .await?;
        let replay_docs = self
            .get_partition_doc_events_for_peer(peer, &doc_req)
            .await?;
        let member_high_watermark = replay_members
            .events
            .iter()
            .filter_map(|event| cursor::to_txid(&event.cursor).ok())
            .max()
            .unwrap_or(0);
        let doc_high_watermark = replay_docs
            .events
            .iter()
            .filter_map(|event| cursor::to_txid(&event.cursor).ok())
            .max()
            .unwrap_or(0);
        let requested: HashSet<PartitionId> = reqs
            .partitions
            .iter()
            .map(|item| item.partition_id.clone())
            .collect();
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            for event in replay_members.events {
                if tx.send(SubscriptionItem::MemberEvent(event)).await.is_err() {
                    return;
                }
            }
            if tx
                .send(SubscriptionItem::SnapshotComplete {
                    stream: SubscriptionStreamKind::Member,
                })
                .await
                .is_err()
            {
                return;
            }
            for event in replay_docs.events {
                if tx.send(SubscriptionItem::DocEvent(event)).await.is_err() {
                    return;
                }
            }
            if tx
                .send(SubscriptionItem::SnapshotComplete {
                    stream: SubscriptionStreamKind::Doc,
                })
                .await
                .is_err()
            {
                return;
            }
            // FIXME: this will error on lagged, not just on closed
            loop {
                let event = match live_rx.recv().await {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                        let _ = tx.send(SubscriptionItem::Lagged { dropped }).await;
                        return;
                    }
                };
                if !requested.contains(&event.partition_id) {
                    continue;
                }
                let Ok(txid) = cursor::to_txid(&event.cursor) else {
                    continue;
                };
                match event.deets {
                    PartitionEventDeets::MemberUpsert { doc_id } => {
                        if txid <= member_high_watermark {
                            continue;
                        }
                        if tx
                            .send(SubscriptionItem::MemberEvent(PartitionMemberEvent {
                                cursor: event.cursor,
                                partition_id: event.partition_id,
                                deets: PartitionMemberEventDeets::MemberUpsert { doc_id },
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    PartitionEventDeets::MemberRemoved { doc_id } => {
                        if txid <= member_high_watermark {
                            continue;
                        }
                        if tx
                            .send(SubscriptionItem::MemberEvent(PartitionMemberEvent {
                                cursor: event.cursor,
                                partition_id: event.partition_id,
                                deets: PartitionMemberEventDeets::MemberRemoved { doc_id },
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    PartitionEventDeets::DocChanged {
                        doc_id,
                        heads,
                        change_count_hint,
                    } => {
                        if txid <= doc_high_watermark {
                            continue;
                        }
                        if tx
                            .send(SubscriptionItem::DocEvent(PartitionDocEvent {
                                cursor: event.cursor,
                                partition_id: event.partition_id,
                                deets: PartitionDocEventDeets::DocChanged {
                                    doc_id,
                                    heads,
                                    change_count_hint,
                                },
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    PartitionEventDeets::DocDeleted {
                        doc_id,
                        change_count_hint,
                    } => {
                        if txid <= doc_high_watermark {
                            continue;
                        }
                        if tx
                            .send(SubscriptionItem::DocEvent(PartitionDocEvent {
                                cursor: event.cursor,
                                partition_id: event.partition_id,
                                deets: PartitionDocEventDeets::DocDeleted {
                                    doc_id,
                                    change_count_hint,
                                },
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        });
        Ok(rx)
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
            SELECT 1 FROM partition_membership_state WHERE partition_id = ?
            UNION
            SELECT 1 FROM partition_doc_state WHERE partition_id = ?
        )
        "#,
    )
    .bind(partition_id)
    .bind(partition_id)
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

async fn load_member_partition_page(
    pool: &sqlx::SqlitePool,
    req: &PartitionCursorRequest,
    limit: usize,
) -> Res<(Vec<PartitionMemberEvent>, Option<String>, bool)> {
    match &req.since {
        Some(cursor) => {
            let since_txid =
                cursor::to_txid(cursor).map_err(|_| PartitionSyncError::InvalidCursor {
                    cursor: cursor.clone(),
                })?;
            let rows = sqlx::query(
                "SELECT latest_txid AS txid, doc_id, present FROM partition_membership_state WHERE partition_id = ? AND latest_txid > ? ORDER BY latest_txid LIMIT ?",
            )
            .bind(&req.partition_id)
            .bind(since_txid as i64)
            .bind((limit + 1) as i64)
            .fetch_all(pool)
            .await?;
            let has_more = rows.len() > limit;
            let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
            let events = rows
                .iter()
                .map(|row| -> Res<PartitionMemberEvent> {
                    let txid: i64 = row.try_get("txid")?;
                    let doc_id: String = row.try_get("doc_id")?;
                    let present: i64 = row.try_get("present")?;
                    let deets = match present {
                        1 => PartitionMemberEventDeets::MemberUpsert { doc_id },
                        0 => PartitionMemberEventDeets::MemberRemoved { doc_id },
                        other => eyre::bail!("invalid membership present flag '{other}'"),
                    };
                    Ok(PartitionMemberEvent {
                        cursor: cursor::from_txid(txid.max(0) as u64),
                        partition_id: req.partition_id.clone(),
                        deets,
                    })
                })
                .collect::<Res<Vec<_>>>()?;
            let next_cursor = events.last().map(|item| item.cursor.clone());
            Ok((events, next_cursor, has_more))
        }
        None => {
            let latest_txid: i64 = sqlx::query_scalar(
                r#"
                SELECT COALESCE(MAX(txid), 0) FROM (
                    SELECT latest_txid AS txid FROM partition_membership_state WHERE partition_id = ?
                    UNION ALL
                    SELECT latest_txid AS txid FROM partition_doc_state WHERE partition_id = ?
                )
                "#,
            )
            .bind(&req.partition_id)
            .bind(&req.partition_id)
            .fetch_one(pool)
            .await?;
            let snapshot_cursor = cursor::from_txid(latest_txid.max(0) as u64);
            let rows = sqlx::query(
                "SELECT doc_id FROM partition_membership_state WHERE partition_id = ? AND present = 1 ORDER BY doc_id LIMIT ?",
            )
            .bind(&req.partition_id)
            .bind((limit + 1) as i64)
            .fetch_all(pool)
            .await?;
            let has_more = rows.len() > limit;
            let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
            let events = rows
                .into_iter()
                .map(|row| -> Res<PartitionMemberEvent> {
                    let doc_id: String = row.try_get("doc_id")?;
                    Ok(PartitionMemberEvent {
                        cursor: snapshot_cursor.clone(),
                        partition_id: req.partition_id.clone(),
                        deets: PartitionMemberEventDeets::MemberUpsert { doc_id },
                    })
                })
                .collect::<Res<Vec<_>>>()?;
            let next_cursor = if events.is_empty() {
                None
            } else {
                Some(snapshot_cursor)
            };
            Ok((events, next_cursor, has_more))
        }
    }
}

async fn load_doc_partition_page(
    pool: &sqlx::SqlitePool,
    req: &PartitionCursorRequest,
    limit: usize,
) -> Res<(Vec<PartitionDocEvent>, Option<String>, bool)> {
    match &req.since {
        Some(cursor) => {
            let since_txid =
                cursor::to_txid(cursor).map_err(|_| PartitionSyncError::InvalidCursor {
                    cursor: cursor.clone(),
                })?;
            let rows = sqlx::query(
                r#"
                SELECT
                    ds.latest_txid AS txid,
                    ds.doc_id AS doc_id,
                    ds.deleted AS deleted,
                    dv.latest_heads_json AS heads_json,
                    COALESCE(dv.change_count_hint, 0) AS change_count_hint
                FROM partition_doc_state ds
                LEFT JOIN doc_version_state dv ON dv.doc_id = ds.doc_id
                WHERE ds.partition_id = ? AND ds.latest_txid > ?
                ORDER BY ds.latest_txid
                LIMIT ?
                "#,
            )
            .bind(&req.partition_id)
            .bind(since_txid as i64)
            .bind((limit + 1) as i64)
            .fetch_all(pool)
            .await?;
            let has_more = rows.len() > limit;
            let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
            let events = rows
                .iter()
                .map(|row| -> Res<PartitionDocEvent> {
                    let txid: i64 = row.try_get("txid")?;
                    let doc_id: String = row.try_get("doc_id")?;
                    let heads_json: Option<String> = row.try_get("heads_json")?;
                    let change_count_hint: i64 = row.try_get("change_count_hint")?;
                    let deleted: i64 = row.try_get("deleted")?;
                    let deets = match deleted {
                        0 => PartitionDocEventDeets::DocChanged {
                            doc_id,
                            heads: serde_json::from_str(heads_json.as_deref().unwrap_or("[]"))?,
                            change_count_hint: change_count_hint.max(0) as u64,
                        },
                        1 => PartitionDocEventDeets::DocDeleted {
                            doc_id,
                            change_count_hint: change_count_hint.max(0) as u64,
                        },
                        other => eyre::bail!("invalid deleted flag '{other}'"),
                    };
                    Ok(PartitionDocEvent {
                        cursor: cursor::from_txid(txid.max(0) as u64),
                        partition_id: req.partition_id.clone(),
                        deets,
                    })
                })
                .collect::<Res<Vec<_>>>()?;
            let next_cursor = events.last().map(|item| item.cursor.clone());
            Ok((events, next_cursor, has_more))
        }
        None => {
            let latest_txid: i64 = sqlx::query_scalar(
                r#"
                SELECT COALESCE(MAX(txid), 0) FROM (
                    SELECT latest_txid AS txid FROM partition_membership_state WHERE partition_id = ?
                    UNION ALL
                    SELECT latest_txid AS txid FROM partition_doc_state WHERE partition_id = ?
                )
                "#,
            )
            .bind(&req.partition_id)
            .bind(&req.partition_id)
            .fetch_one(pool)
            .await?;
            let snapshot_cursor = cursor::from_txid(latest_txid.max(0) as u64);
            let rows = sqlx::query(
                r#"
                SELECT
                    ds.doc_id AS doc_id,
                    dv.latest_heads_json AS heads_json,
                    COALESCE(dv.change_count_hint, 0) AS change_count_hint
                FROM partition_doc_state ds
                JOIN doc_version_state dv ON dv.doc_id = ds.doc_id
                WHERE ds.partition_id = ? AND ds.deleted = 0
                ORDER BY ds.doc_id
                LIMIT ?
                "#,
            )
            .bind(&req.partition_id)
            .bind((limit + 1) as i64)
            .fetch_all(pool)
            .await?;
            let has_more = rows.len() > limit;
            let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
            let events = rows
                .into_iter()
                .map(|row| -> Res<PartitionDocEvent> {
                    let doc_id: String = row.try_get("doc_id")?;
                    let heads_json: String = row.try_get("heads_json")?;
                    let change_count_hint: i64 = row.try_get("change_count_hint")?;
                    Ok(PartitionDocEvent {
                        cursor: snapshot_cursor.clone(),
                        partition_id: req.partition_id.clone(),
                        deets: PartitionDocEventDeets::DocChanged {
                            doc_id,
                            heads: serde_json::from_str(&heads_json)?,
                            change_count_hint: change_count_hint.max(0) as u64,
                        },
                    })
                })
                .collect::<Res<Vec<_>>>()?;
            let next_cursor = if events.is_empty() {
                None
            } else {
                Some(snapshot_cursor)
            };
            Ok((events, next_cursor, has_more))
        }
    }
}

fn cmp_member_events(
    left: &PartitionMemberEvent,
    right: &PartitionMemberEvent,
) -> std::cmp::Ordering {
    let left_txid = cursor::to_txid(&left.cursor).unwrap_or(0);
    let right_txid = cursor::to_txid(&right.cursor).unwrap_or(0);
    left_txid
        .cmp(&right_txid)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
}

fn cmp_doc_events(left: &PartitionDocEvent, right: &PartitionDocEvent) -> std::cmp::Ordering {
    let left_txid = cursor::to_txid(&left.cursor).unwrap_or(0);
    let right_txid = cursor::to_txid(&right.cursor).unwrap_or(0);
    left_txid
        .cmp(&right_txid)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::repo::{BigRepo, BigRepoConfig};
    use crate::sync::{
        GetPartitionDocEventsRequest, GetPartitionMemberEventsRequest, PartitionCursorRequest,
    };
    use automerge::transaction::Transactable;

    async fn boot_big_repo() -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string("bigrepo-test-peer".to_string()))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        BigRepo::boot_with_repo(repo, BigRepoConfig::new("sqlite::memory:".to_string())).await
    }

    #[tokio::test]
    async fn bigrepo_emits_partition_doc_events_on_doc_write() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = handle.document_id().to_string();
        let partition_id = "p-main".into();

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

        let events = big_repo
            .get_partition_doc_events_for_peer(
                &"peer-a".into(),
                &GetPartitionDocEventsRequest {
                    partitions: vec![PartitionCursorRequest {
                        partition_id: partition_id.clone(),
                        since: None,
                    }],
                    limit: 1024,
                },
            )
            .await?;

        assert!(events
            .events
            .iter()
            .any(|evt| matches!(evt.deets, PartitionDocEventDeets::DocChanged { .. })));
        assert!(events
            .cursors
            .iter()
            .any(|page| page.partition_id == partition_id));
        Ok(())
    }

    #[tokio::test]
    async fn bigrepo_member_snapshot_excludes_removed_docs() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
        let target_doc_id = handle.document_id().to_string();
        let partition_id = "p-remove".into();
        big_repo
            .add_doc_to_partition(&partition_id, &target_doc_id)
            .await?;
        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "v", 1_i64)
                    .expect("failed setting test key");
                tx.commit();
            })
            .await?;
        big_repo
            .remove_doc_from_partition(&partition_id, &target_doc_id)
            .await?;

        let snapshot = big_repo
            .get_partition_member_events_for_peer(
                &"peer-a".into(),
                &GetPartitionMemberEventsRequest {
                    partitions: vec![PartitionCursorRequest {
                        partition_id,
                        since: None,
                    }],
                    limit: 1024,
                },
            )
            .await?;
        assert!(
            !snapshot.events.iter().any(|event| {
                matches!(
                    event.deets,
                    PartitionMemberEventDeets::MemberUpsert { ref doc_id } if doc_id == &target_doc_id
                )
            }),
            "removed doc should not remain in snapshot membership"
        );
        Ok(())
    }
}
