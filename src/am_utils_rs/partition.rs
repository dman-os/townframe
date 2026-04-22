use crate::interlude::*;

use sqlx::Row;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::sync::protocol::{
    GetPartitionDocEventsRequest, GetPartitionDocEventsResponse, GetPartitionMemberEventsRequest,
    GetPartitionMemberEventsResponse, ListPartitionsResponse, PartitionCursorPage,
    PartitionCursorRequest, PartitionDocEvent, PartitionDocEventDeets, PartitionEvent,
    PartitionEventDeets, PartitionId, PartitionMemberEvent, PartitionMemberEventDeets,
    PartitionSummary, PeerKey, SubPartitionsRequest, SubscriptionItem, SubscriptionStreamKind,
    DEFAULT_EVENT_PAGE_LIMIT,
};

const META_NEXT_TXID_KEY: &str = "next_txid";
const MAX_PAGE_LIMIT: u32 = 1_024;

#[derive(Clone, Debug)]
pub struct PartitionStore {
    state_pool: sqlx::SqlitePool,
    partition_events_tx: broadcast::Sender<PartitionEvent>,
    partition_forwarder_cancel: CancellationToken,
    partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
}

impl PartitionStore {
    pub fn new(
        state_pool: sqlx::SqlitePool,
        partition_events_tx: broadcast::Sender<PartitionEvent>,
        partition_forwarder_cancel: CancellationToken,
        partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
    ) -> Self {
        Self {
            state_pool,
            partition_events_tx,
            partition_forwarder_cancel,
            partition_forwarders,
        }
    }

    pub fn state_pool(&self) -> &sqlx::SqlitePool {
        &self.state_pool
    }

    pub fn subscribe_partition_events(&self) -> broadcast::Receiver<PartitionEvent> {
        self.partition_events_tx.subscribe()
    }

    pub async fn ensure_schema(&self) -> Res<()> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS meta_kvstore(
                key TEXT PRIMARY KEY
                ,value TEXT NOT NULL
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO meta_kvstore(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO NOTHING
            "#,
        )
        .bind(META_NEXT_TXID_KEY)
        .bind("1")
        .execute(&self.state_pool)
        .await?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS partition_membership_state(
                partition_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                present INTEGER NOT NULL,
                member_payload_json TEXT NOT NULL DEFAULT '{}',
                added_at_txid INTEGER NULL,
                removed_at_txid INTEGER NULL,
                latest_txid INTEGER NOT NULL,
                PRIMARY KEY(partition_id, item_id)
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_membership_state_item ON partition_membership_state(item_id)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_membership_state_partition_latest ON partition_membership_state(partition_id, latest_txid)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS partition_item_state(
                partition_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                deleted INTEGER NOT NULL,
                item_payload_json TEXT NOT NULL DEFAULT '{}',
                latest_txid INTEGER NOT NULL,
                PRIMARY KEY(partition_id, item_id)
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS partition_state(
                partition_id TEXT PRIMARY KEY,
                latest_txid INTEGER NOT NULL DEFAULT 0
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_item_state_partition_latest ON partition_item_state(partition_id, latest_txid)",
        )
        .execute(&self.state_pool)
        .await?;
        Ok(())
    }

    pub async fn ensure_partition(&self, partition_id: &PartitionId) -> Res<()> {
        sqlx::query(
            r#"
            INSERT INTO partition_state(partition_id, latest_txid)
            VALUES(?, 0)
            ON CONFLICT(partition_id) DO NOTHING
            "#,
        )
        .bind(partition_id)
        .execute(&self.state_pool)
        .await?;
        Ok(())
    }

    pub async fn add_member(
        &self,
        partition_id: &PartitionId,
        item_id: &str,
        payload: &serde_json::Value,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO partition_state(partition_id, latest_txid)
            VALUES(?, 0)
            ON CONFLICT(partition_id) DO NOTHING
            "#,
        )
        .bind(partition_id)
        .execute(&mut *tx)
        .await?;
        let membership_txid = alloc_txid(tx.as_mut()).await?;
        let membership_write = sqlx::query(
            r#"
            INSERT INTO partition_membership_state(
                partition_id, item_id, present, member_payload_json, added_at_txid, removed_at_txid, latest_txid
            ) VALUES(?, ?, 1, ?, ?, NULL, ?)
            ON CONFLICT(partition_id, item_id) DO UPDATE SET
                present = 1,
                member_payload_json = excluded.member_payload_json,
                added_at_txid = excluded.added_at_txid,
                removed_at_txid = NULL,
                latest_txid = excluded.latest_txid
            WHERE partition_membership_state.present != 1
            "#,
        )
        .bind(partition_id)
        .bind(item_id)
        .bind(serde_json::to_string(payload)?)
        .bind(membership_txid as i64)
        .bind(membership_txid as i64)
        .execute(&mut *tx)
        .await?;
        if membership_write.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(());
        }
        let item_payload_json = sqlx::query_scalar::<_, String>(
            "SELECT item_payload_json FROM partition_item_state WHERE partition_id = ? AND item_id = ?",
        )
        .bind(partition_id)
        .bind(item_id)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or_else(|| "{}".to_string());
        let item_payload = serde_json::from_str::<serde_json::Value>(&item_payload_json)?;
        let item_txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            r#"
            INSERT INTO partition_item_state(
                partition_id, item_id, deleted, item_payload_json, latest_txid
            ) VALUES(?, ?, 0, ?, ?)
            ON CONFLICT(partition_id, item_id) DO UPDATE SET
                deleted = 0,
                item_payload_json = excluded.item_payload_json,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(partition_id)
        .bind(item_id)
        .bind(&item_payload_json)
        .bind(item_txid as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        self.partition_events_tx
            .send(PartitionEvent {
                cursor: membership_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::MemberUpsert {
                    item_id: item_id.to_owned(),
                    payload: payload.clone(),
                },
            })
            .ok();
        self.partition_events_tx
            .send(PartitionEvent {
                cursor: item_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::ItemChanged {
                    item_id: item_id.to_owned(),
                    payload: item_payload,
                },
            })
            .ok();
        Ok(())
    }

    pub async fn remove_member(
        &self,
        partition_id: &PartitionId,
        item_id: &str,
        payload: &serde_json::Value,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO partition_state(partition_id, latest_txid)
            VALUES(?, 0)
            ON CONFLICT(partition_id) DO NOTHING
            "#,
        )
        .bind(partition_id)
        .execute(&mut *tx)
        .await?;
        let membership_txid = alloc_txid(tx.as_mut()).await?;
        let membership_write = sqlx::query(
            r#"
            UPDATE partition_membership_state
            SET present = 0,
                member_payload_json = ?,
                removed_at_txid = ?,
                latest_txid = ?
            WHERE partition_id = ?
              AND item_id = ?
              AND present = 1
            "#,
        )
        .bind(serde_json::to_string(payload)?)
        .bind(membership_txid as i64)
        .bind(membership_txid as i64)
        .bind(partition_id)
        .bind(item_id)
        .execute(&mut *tx)
        .await?;
        if membership_write.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(());
        }
        let item_payload_json = sqlx::query_scalar::<_, String>(
            "SELECT item_payload_json FROM partition_item_state WHERE partition_id = ? AND item_id = ?",
        )
        .bind(partition_id)
        .bind(item_id)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or_else(|| "{}".to_string());
        let item_payload = serde_json::from_str::<serde_json::Value>(&item_payload_json)?;
        let item_txid = alloc_txid(tx.as_mut()).await?;
        sqlx::query(
            r#"
            INSERT INTO partition_item_state(
                partition_id, item_id, deleted, item_payload_json, latest_txid
            ) VALUES(?, ?, 1, ?, ?)
            ON CONFLICT(partition_id, item_id) DO UPDATE SET
                deleted = 1,
                item_payload_json = excluded.item_payload_json,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(partition_id)
        .bind(item_id)
        .bind(&item_payload_json)
        .bind(item_txid as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        self.partition_events_tx
            .send(PartitionEvent {
                cursor: membership_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::MemberRemoved {
                    item_id: item_id.to_owned(),
                    payload: payload.clone(),
                },
            })
            .ok();
        self.partition_events_tx
            .send(PartitionEvent {
                cursor: item_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::ItemDeleted {
                    item_id: item_id.to_owned(),
                    payload: item_payload,
                },
            })
            .ok();
        Ok(())
    }

    pub async fn record_item_change(
        &self,
        partition_id: &PartitionId,
        item_id: &str,
        item_payload: &serde_json::Value,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let txid = record_item_change_tx(tx.as_mut(), partition_id, item_id, item_payload).await?;
        tx.commit().await?;
        self.partition_events_tx
            .send(PartitionEvent {
                cursor: txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::ItemChanged {
                    item_id: item_id.to_owned(),
                    payload: item_payload.clone(),
                },
            })
            .ok();
        Ok(())
    }

    pub async fn record_item_deleted(
        &self,
        partition_id: &PartitionId,
        item_id: &str,
        item_payload: &serde_json::Value,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let txid = record_item_deleted_tx(tx.as_mut(), partition_id, item_id, item_payload).await?;
        tx.commit().await?;
        self.partition_events_tx
            .send(PartitionEvent {
                cursor: txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::ItemDeleted {
                    item_id: item_id.to_owned(),
                    payload: item_payload.clone(),
                },
            })
            .ok();
        Ok(())
    }

    pub async fn record_member_item_change(
        &self,
        item_id: &str,
        item_payload: &serde_json::Value,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let partition_rows = sqlx::query(
            "SELECT partition_id FROM partition_membership_state WHERE item_id = ? AND present = 1",
        )
        .bind(item_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut events = Vec::with_capacity(partition_rows.len());
        for row in partition_rows {
            let partition_id: String = row.try_get("partition_id")?;
            let txid =
                record_item_change_tx(tx.as_mut(), &partition_id, item_id, item_payload).await?;
            events.push(PartitionEvent {
                cursor: txid,
                partition_id,
                deets: PartitionEventDeets::ItemChanged {
                    item_id: item_id.to_owned(),
                    payload: item_payload.clone(),
                },
            });
        }
        tx.commit().await?;
        for event in events {
            self.partition_events_tx.send(event).ok();
        }
        Ok(())
    }

    pub async fn record_member_item_deleted(
        &self,
        item_id: &str,
        item_payload: &serde_json::Value,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let partition_rows =
            sqlx::query("SELECT partition_id FROM partition_membership_state WHERE item_id = ?")
                .bind(item_id)
                .fetch_all(&mut *tx)
                .await?;
        let mut events = Vec::with_capacity(partition_rows.len());
        for row in partition_rows {
            let partition_id: String = row.try_get("partition_id")?;
            let txid =
                record_item_deleted_tx(tx.as_mut(), &partition_id, item_id, item_payload).await?;
            events.push(PartitionEvent {
                cursor: txid,
                partition_id,
                deets: PartitionEventDeets::ItemDeleted {
                    item_id: item_id.to_owned(),
                    payload: item_payload.clone(),
                },
            });
        }
        tx.commit().await?;
        for event in events {
            self.partition_events_tx.send(event).ok();
        }
        Ok(())
    }

    pub async fn member_count(&self, part_id: &PartitionId) -> Res<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM partition_membership_state WHERE partition_id = ? AND present = 1",
        )
        .bind(part_id)
        .fetch_one(&self.state_pool)
        .await?;
        Ok(count)
    }

    pub async fn is_member_present_in_item_state(
        &self,
        partition_id: &PartitionId,
        member_id: &str,
    ) -> Res<bool> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM partition_item_state WHERE partition_id = ? AND item_id = ? AND deleted = 0)",
        )
        .bind(partition_id)
        .bind(member_id)
        .fetch_one(&self.state_pool)
        .await?;
        Ok(exists == 1)
    }

    pub async fn item_payload(
        &self,
        partition_id: &PartitionId,
        item_id: &str,
    ) -> Res<Option<serde_json::Value>> {
        let payload_json = sqlx::query_scalar::<_, String>(
            "SELECT item_payload_json FROM partition_item_state WHERE partition_id = ? AND item_id = ? AND deleted = 0",
        )
        .bind(partition_id)
        .bind(item_id)
        .fetch_optional(&self.state_pool)
        .await?;
        payload_json
            .map(|json| serde_json::from_str::<serde_json::Value>(&json))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn item_row_count(&self, partition_id: &PartitionId, item_id: &str) -> Res<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM partition_item_state WHERE partition_id = ? AND item_id = ?",
        )
        .bind(partition_id)
        .bind(item_id)
        .fetch_one(&self.state_pool)
        .await?;
        Ok(count)
    }

    pub async fn list_known_item_ids(&self) -> Res<Vec<String>> {
        let ids = sqlx::query_scalar(
            r#"
            SELECT DISTINCT item_id AS doc_id FROM partition_membership_state
            UNION
            SELECT DISTINCT item_id AS doc_id FROM partition_item_state
            "#,
        )
        .fetch_all(&self.state_pool)
        .await?;
        Ok(ids)
    }

    pub async fn is_item_present_in_membership_partitions(
        &self,
        item_id: &str,
        allowed_partitions: &[PartitionId],
    ) -> Res<bool> {
        if allowed_partitions.is_empty() {
            return Ok(false);
        }
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT EXISTS(SELECT 1 FROM partition_membership_state WHERE item_id = ",
        );
        query.push_bind(item_id);
        query.push(" AND present = 1 AND partition_id IN (");
        let mut separated = query.separated(", ");
        for partition_id in allowed_partitions {
            separated.push_bind(partition_id);
        }
        separated.push_unseparated("))");
        let exists: i64 = query
            .build_query_scalar()
            .fetch_one(&self.state_pool)
            .await?;
        Ok(exists == 1)
    }

    pub async fn find_first_item_missing_membership_in_partitions(
        &self,
        item_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Option<String>> {
        if item_ids.is_empty() || allowed_partitions.is_empty() {
            return Ok(item_ids.first().cloned());
        }
        let mut query =
            sqlx::QueryBuilder::<sqlx::Sqlite>::new("WITH requested(item_id, ordinal) AS (SELECT ");
        for (idx, item_id) in item_ids.iter().enumerate() {
            if idx > 0 {
                query.push(" UNION ALL SELECT ");
            }
            query.push_bind(item_id);
            query.push(", ");
            query.push_bind(i64::try_from(idx).expect("item index exceeds sqlite INTEGER range"));
        }
        query.push(") SELECT requested.item_id FROM requested WHERE NOT EXISTS (");
        query
            .push("SELECT 1 FROM partition_membership_state m WHERE m.item_id = requested.item_id");
        query.push(" AND m.present = 1 AND m.partition_id IN (");
        let mut partitions = query.separated(", ");
        for partition_id in allowed_partitions {
            partitions.push_bind(partition_id);
        }
        partitions.push_unseparated(")) ORDER BY requested.ordinal LIMIT 1");
        let denied = query
            .build_query_scalar::<String>()
            .fetch_optional(&self.state_pool)
            .await?;
        Ok(denied)
    }

    pub async fn list_partitions_for_peer(&self, _peer: &PeerKey) -> Res<ListPartitionsResponse> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.partition_id AS partition_id,
                COALESCE(pm.member_count, 0) AS member_count,
                COALESCE(mx.latest_txid, 0) AS latest_txid
            FROM (
                SELECT partition_id FROM partition_state
                UNION
                SELECT DISTINCT partition_id FROM partition_membership_state
                UNION
                SELECT DISTINCT partition_id FROM partition_item_state
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
                    SELECT partition_id, latest_txid AS txid FROM partition_state
                    UNION ALL
                    SELECT partition_id, latest_txid AS txid FROM partition_membership_state
                    UNION ALL
                    SELECT partition_id, latest_txid AS txid FROM partition_item_state
                )
                GROUP BY partition_id
            ) mx ON mx.partition_id = p.partition_id
            ORDER BY p.partition_id
            "#,
        )
        .fetch_all(&self.state_pool)
        .await?;

        let partitions = rows
            .into_iter()
            .map(|row| {
                let partition_id: String = row.try_get("partition_id")?;
                let member_count: i64 = row.try_get("member_count")?;
                let latest_txid: i64 = row.try_get("latest_txid")?;
                eyre::Ok(PartitionSummary {
                    partition_id,
                    latest_cursor: latest_txid.max(0) as u64,
                    member_count: member_count.max(0) as u64,
                })
            })
            .collect::<Res<Vec<_>>>()?;
        Ok(ListPartitionsResponse { partitions })
    }

    pub async fn get_partition_member_events_for_peer(
        &self,
        _peer: &PeerKey,
        req: &GetPartitionMemberEventsRequest,
    ) -> Res<GetPartitionMemberEventsResponse> {
        let limit = req.limit.clamp(1, MAX_PAGE_LIMIT) as usize;
        let mut events = Vec::with_capacity(req.partitions.len().saturating_mul(limit));
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
        let limit = req.limit.clamp(1, MAX_PAGE_LIMIT) as usize;
        let mut events = Vec::with_capacity(req.partitions.len().saturating_mul(limit));
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

    pub async fn subscribe_partition_events_for_peer(
        &self,
        peer: &PeerKey,
        reqs: &SubPartitionsRequest,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<SubscriptionItem>> {
        let mut live_rx = self.partition_events_tx.subscribe();
        let mut member_parts: Vec<PartitionCursorRequest> = reqs
            .partitions
            .iter()
            .map(|item| PartitionCursorRequest {
                partition_id: item.partition_id.clone(),
                since: item.since_member,
            })
            .collect();
        let mut doc_parts: Vec<PartitionCursorRequest> = reqs
            .partitions
            .iter()
            .map(|item| PartitionCursorRequest {
                partition_id: item.partition_id.clone(),
                since: item.since_doc,
            })
            .collect();
        let requested: HashSet<PartitionId> = reqs
            .partitions
            .iter()
            .map(|item| item.partition_id.clone())
            .collect();
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        let mut member_high_watermark: HashMap<PartitionId, u64> = reqs
            .partitions
            .iter()
            .map(|item| {
                (
                    item.partition_id.clone(),
                    item.since_member.unwrap_or_default(),
                )
            })
            .collect();
        let mut doc_high_watermark: HashMap<PartitionId, u64> = reqs
            .partitions
            .iter()
            .map(|item| {
                (
                    item.partition_id.clone(),
                    item.since_doc.unwrap_or_default(),
                )
            })
            .collect();
        let store = self.clone();
        let peer = peer.clone();
        let cancel_token = self.partition_forwarder_cancel.clone();
        let fut = {
            let span = tracing::info_span!("partition live forwarder");
            async move {
                loop {
                    let replay_members = store
                        .get_partition_member_events_for_peer(
                            &peer,
                            &GetPartitionMemberEventsRequest {
                                partitions: member_parts.clone(),
                                limit: DEFAULT_EVENT_PAGE_LIMIT,
                            },
                        )
                        .await?;
                    for event in replay_members.events {
                        let entry = member_high_watermark
                            .entry(event.partition_id.clone())
                            .or_default();
                        *entry = (*entry).max(event.cursor);
                        if tx.send(SubscriptionItem::MemberEvent(event)).await.is_err() {
                            return Ok(());
                        }
                    }
                    let mut any_more = false;
                    for cursor_page in replay_members.cursors {
                        let Some(part) = member_parts
                            .iter_mut()
                            .find(|part| part.partition_id == cursor_page.partition_id)
                        else {
                            continue;
                        };
                        part.since = cursor_page.next_cursor.or(part.since);
                        any_more |= cursor_page.has_more;
                    }
                    if !any_more {
                        break;
                    }
                }
                if tx
                    .send(SubscriptionItem::ReplayComplete {
                        stream: SubscriptionStreamKind::Member,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }

                loop {
                    let replay_docs = store
                        .get_partition_doc_events_for_peer(
                            &peer,
                            &GetPartitionDocEventsRequest {
                                partitions: doc_parts.clone(),
                                limit: DEFAULT_EVENT_PAGE_LIMIT,
                            },
                        )
                        .await?;
                    for event in replay_docs.events {
                        let entry = doc_high_watermark
                            .entry(event.partition_id.clone())
                            .or_default();
                        *entry = (*entry).max(event.cursor);
                        if tx.send(SubscriptionItem::DocEvent(event)).await.is_err() {
                            return Ok(());
                        }
                    }
                    let mut any_more = false;
                    for cursor_page in replay_docs.cursors {
                        let Some(part) = doc_parts
                            .iter_mut()
                            .find(|part| part.partition_id == cursor_page.partition_id)
                        else {
                            continue;
                        };
                        part.since = cursor_page.next_cursor.or(part.since);
                        any_more |= cursor_page.has_more;
                    }
                    if !any_more {
                        break;
                    }
                }
                if tx
                    .send(SubscriptionItem::ReplayComplete {
                        stream: SubscriptionStreamKind::Doc,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }

                loop {
                    let recv = cancel_token.run_until_cancelled(live_rx.recv()).await;
                    let event = match recv {
                        None => break,
                        Some(Ok(event)) => event,
                        Some(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                        Some(Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped))) => {
                            let _ = tx.send(SubscriptionItem::Lagged { dropped }).await;
                            break;
                        }
                    };
                    if !requested.contains(&event.partition_id) {
                        continue;
                    }
                    let txid = event.cursor;
                    let partition_id = event.partition_id.clone();
                    match event.deets {
                        PartitionEventDeets::MemberUpsert { item_id, payload } => {
                            let high_watermark =
                                *member_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            let payload = serde_json::to_string(&payload)
                                .expect("member upsert payload should serialize to json");
                            if tx
                                .send(SubscriptionItem::MemberEvent(PartitionMemberEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionMemberEventDeets::MemberUpsert {
                                        item_id,
                                        payload,
                                    },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            member_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::MemberRemoved { item_id, payload } => {
                            let high_watermark =
                                *member_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            let payload = serde_json::to_string(&payload)
                                .expect("member removed payload should serialize to json");
                            if tx
                                .send(SubscriptionItem::MemberEvent(PartitionMemberEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionMemberEventDeets::MemberRemoved {
                                        item_id,
                                        payload,
                                    },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            member_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::ItemChanged { item_id, payload } => {
                            let high_watermark =
                                *doc_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            let payload = serde_json::to_string(&payload)
                                .expect("item changed payload should serialize to json");
                            if tx
                                .send(SubscriptionItem::DocEvent(PartitionDocEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionDocEventDeets::ItemChanged { item_id, payload },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            doc_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::ItemDeleted { item_id, payload } => {
                            let high_watermark =
                                *doc_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            let payload = serde_json::to_string(&payload)
                                .expect("item deleted payload should serialize to json");
                            if tx
                                .send(SubscriptionItem::DocEvent(PartitionDocEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionDocEventDeets::ItemDeleted { item_id, payload },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            doc_high_watermark.insert(partition_id, txid);
                        }
                    }
                }
                eyre::Ok(())
            }
            .instrument(span)
        };
        self.partition_forwarders
            .spawn(async move { fut.await.unwrap() })?;
        Ok(rx)
    }

    pub async fn subscribe_partition_doc_events_local(
        &self,
        partition_id: &PartitionId,
        since: Option<u64>,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<PartitionDocEvent>> {
        ensure_partition_exists(&self.state_pool, partition_id).await?;
        let partition_id = partition_id.clone();
        let store = self.clone();
        let cancel_token = self.partition_forwarder_cancel.clone();
        let mut live_rx = self.partition_events_tx.subscribe();
        let mut high_watermark = since.unwrap_or_default();
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        let fut = {
            let span =
                tracing::info_span!("partition local doc forwarder", partition_id = %partition_id);
            async move {
                loop {
                    // Replay from cursor.
                    loop {
                        let (events, _next_cursor, has_more) = load_doc_partition_page(
                            &store.state_pool,
                            &PartitionCursorRequest {
                                partition_id: partition_id.clone(),
                                since: Some(high_watermark),
                            },
                            DEFAULT_EVENT_PAGE_LIMIT as usize,
                        )
                        .await?;
                        if events.is_empty() {
                            break;
                        }
                        for event in events {
                            high_watermark = high_watermark.max(event.cursor);
                            if tx.send(event).await.is_err() {
                                return eyre::Ok(());
                            }
                        }
                        if !has_more {
                            break;
                        }
                    }

                    // Tail live events until lag/close/cancel, then replay again from watermark.
                    loop {
                        let recv = cancel_token.run_until_cancelled(live_rx.recv()).await;
                        let event = match recv {
                            None => return eyre::Ok(()),
                            Some(Ok(event)) => event,
                            Some(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                                return eyre::Ok(())
                            }
                            Some(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => break,
                        };
                        if event.partition_id != partition_id {
                            continue;
                        }
                        let txid = event.cursor;
                        if txid <= high_watermark {
                            continue;
                        }
                        let deets = match event.deets {
                            PartitionEventDeets::ItemChanged { item_id, payload } => {
                                PartitionDocEventDeets::ItemChanged {
                                    item_id,
                                    payload: serde_json::to_string(&payload).expect(ERROR_JSON),
                                }
                            }
                            PartitionEventDeets::ItemDeleted { item_id, payload } => {
                                PartitionDocEventDeets::ItemDeleted {
                                    item_id,
                                    payload: serde_json::to_string(&payload).expect(ERROR_JSON),
                                }
                            }
                            _ => continue,
                        };
                        let doc_event = PartitionDocEvent {
                            cursor: txid,
                            partition_id: partition_id.clone(),
                            deets,
                        };
                        high_watermark = txid;
                        if tx.send(doc_event).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
            .instrument(span)
        };
        self.partition_forwarders
            .spawn(async move { fut.await.unwrap() })?;
        Ok(rx)
    }
}

async fn alloc_txid(conn: &mut sqlx::SqliteConnection) -> Res<u64> {
    let next_value: i64 = sqlx::query_scalar(
        "UPDATE meta_kvstore SET value = CAST(value AS INTEGER) + 1 WHERE key = ? RETURNING CAST(value AS INTEGER)",
    )
    .bind(META_NEXT_TXID_KEY)
    .fetch_one(&mut *conn)
    .await?;
    let out = next_value.saturating_sub(1);
    if out < 0 {
        eyre::bail!("invalid next_txid in sqlite: {next_value}");
    }
    let out = out as u64;
    Ok(out)
}

async fn record_item_change_tx(
    conn: &mut sqlx::SqliteConnection,
    partition_id: &PartitionId,
    item_id: &str,
    item_payload: &serde_json::Value,
) -> Res<u64> {
    let item_payload_json = serde_json::to_string(item_payload)?;
    let txid = alloc_txid(conn).await?;
    sqlx::query(
        r#"
        INSERT INTO partition_item_state(
            partition_id, item_id, deleted, item_payload_json, latest_txid
        ) VALUES(?, ?, 0, ?, ?)
        ON CONFLICT(partition_id, item_id) DO UPDATE SET
            deleted = 0,
            item_payload_json = excluded.item_payload_json,
            latest_txid = excluded.latest_txid
        "#,
    )
    .bind(partition_id)
    .bind(item_id)
    .bind(item_payload_json)
    .bind(txid as i64)
    .execute(&mut *conn)
    .await?;
    Ok(txid)
}

async fn record_item_deleted_tx(
    conn: &mut sqlx::SqliteConnection,
    partition_id: &PartitionId,
    item_id: &str,
    item_payload: &serde_json::Value,
) -> Res<u64> {
    let item_payload_json = serde_json::to_string(item_payload)?;
    let txid = alloc_txid(conn).await?;
    sqlx::query(
        r#"
        INSERT INTO partition_item_state(
            partition_id, item_id, deleted, item_payload_json, latest_txid
        ) VALUES(?, ?, 1, ?, ?)
        ON CONFLICT(partition_id, item_id) DO UPDATE SET
            deleted = 1,
            item_payload_json = excluded.item_payload_json,
            latest_txid = excluded.latest_txid
        "#,
    )
    .bind(partition_id)
    .bind(item_id)
    .bind(item_payload_json)
    .bind(txid as i64)
    .execute(&mut *conn)
    .await?;
    Ok(txid)
}

async fn ensure_partition_exists(pool: &sqlx::SqlitePool, partition_id: &PartitionId) -> Res<()> {
    let found: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM partition_state WHERE partition_id = ?
            UNION
            SELECT 1 FROM partition_membership_state WHERE partition_id = ?
            UNION
            SELECT 1 FROM partition_item_state WHERE partition_id = ?
        )
        "#,
    )
    .bind(partition_id)
    .bind(partition_id)
    .bind(partition_id)
    .fetch_one(pool)
    .await?;
    if found == 1 {
        return Ok(());
    }
    Err(
        crate::sync::protocol::PartitionSyncError::UnknownPartition {
            partition_id: partition_id.clone(),
        }
        .into(),
    )
}

async fn load_member_partition_page(
    pool: &sqlx::SqlitePool,
    req: &PartitionCursorRequest,
    limit: usize,
) -> Res<(Vec<PartitionMemberEvent>, Option<u64>, bool)> {
    let rows = if let Some(since) = req.since {
        sqlx::query(
            r#"
            SELECT txid, item_id, member_payload_json, kind FROM (
                SELECT added_at_txid AS txid, item_id, member_payload_json, 1 AS kind
                FROM partition_membership_state
                WHERE partition_id = ? AND added_at_txid IS NOT NULL AND added_at_txid > ?
                UNION ALL
                SELECT removed_at_txid AS txid, item_id, member_payload_json, 0 AS kind
                FROM partition_membership_state
                WHERE partition_id = ? AND removed_at_txid IS NOT NULL AND removed_at_txid > ?
            )
            ORDER BY txid, item_id, kind
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id)
        .bind(since as i64)
        .bind(&req.partition_id)
        .bind(since as i64)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT COALESCE(added_at_txid, latest_txid) AS txid, item_id, member_payload_json, 1 AS kind
            FROM partition_membership_state
            WHERE partition_id = ? AND present = 1
            ORDER BY txid, item_id
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    };

    let has_more = rows.len() > limit;
    let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
    let events = rows
        .into_iter()
        .map(|row| -> Res<PartitionMemberEvent> {
            let txid: i64 = row.try_get("txid")?;
            let item_id: String = row.try_get("item_id")?;
            let payload_json: String = row.try_get("member_payload_json")?;
            let kind: i64 = row.try_get("kind")?;
            let deets = match kind {
                1 => PartitionMemberEventDeets::MemberUpsert {
                    item_id,
                    payload: payload_json,
                },
                0 => PartitionMemberEventDeets::MemberRemoved {
                    item_id,
                    payload: payload_json,
                },
                other => eyre::bail!("invalid membership kind '{other}'"),
            };
            Ok(PartitionMemberEvent {
                cursor: txid.max(0) as u64,
                partition_id: req.partition_id.clone(),
                deets,
            })
        })
        .collect::<Res<Vec<_>>>()?;
    let next_cursor = events.last().map(|item| item.cursor);
    Ok((events, next_cursor, has_more))
}

async fn load_doc_partition_page(
    pool: &sqlx::SqlitePool,
    req: &PartitionCursorRequest,
    limit: usize,
) -> Res<(Vec<PartitionDocEvent>, Option<u64>, bool)> {
    let rows = if let Some(since) = req.since {
        sqlx::query(
            r#"
            SELECT
                latest_txid AS event_txid,
                item_id AS item_id,
                deleted AS deleted,
                item_payload_json AS payload_json
            FROM partition_item_state
            WHERE partition_id = ? AND latest_txid > ?
            ORDER BY event_txid, item_id
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id)
        .bind(since as i64)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT
                item_id AS item_id,
                0 AS deleted,
                latest_txid AS event_txid,
                item_payload_json AS payload_json
            FROM partition_item_state
            WHERE partition_id = ? AND deleted = 0
            ORDER BY event_txid, item_id
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    };

    let has_more = rows.len() > limit;
    let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
    let events = rows
        .into_iter()
        .map(|row| -> Res<PartitionDocEvent> {
            let event_txid: i64 = row.try_get("event_txid")?;
            let item_id: String = row.try_get("item_id")?;
            let payload_json: String = row.try_get("payload_json")?;
            let deleted: i64 = row.try_get("deleted")?;
            let deets = match deleted {
                0 => PartitionDocEventDeets::ItemChanged {
                    item_id,
                    payload: payload_json,
                },
                1 => PartitionDocEventDeets::ItemDeleted {
                    item_id,
                    payload: payload_json,
                },
                other => eyre::bail!("invalid deleted flag '{other}'"),
            };
            Ok(PartitionDocEvent {
                cursor: event_txid.max(0) as u64,
                partition_id: req.partition_id.clone(),
                deets,
            })
        })
        .collect::<Res<Vec<_>>>()?;
    let next_cursor = events.last().map(|item| item.cursor);
    Ok((events, next_cursor, has_more))
}

fn cmp_member_events(
    left: &PartitionMemberEvent,
    right: &PartitionMemberEvent,
) -> std::cmp::Ordering {
    left.cursor
        .cmp(&right.cursor)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
}

fn cmp_doc_events(left: &PartitionDocEvent, right: &PartitionDocEvent) -> std::cmp::Ordering {
    left.cursor
        .cmp(&right.cursor)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::{BigRepo, Config, PeerId, StorageConfig};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashSet;
    use tokio::time::{timeout, Duration};

    async fn make_store() -> PartitionStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite pool should initialize");
        let (events_tx, _events_rx) = broadcast::channel(1024);
        let forwarders = Arc::new(utils_rs::AbortableJoinSet::new());
        let store = PartitionStore::new(
            pool,
            events_tx,
            CancellationToken::new(),
            Arc::clone(&forwarders),
        );
        store
            .ensure_schema()
            .await
            .expect("schema should initialize");
        store
    }

    async fn boot_repo() -> (Arc<BigRepo>, crate::repo::BigRepoStopToken) {
        BigRepo::boot(Config {
            peer_id: PeerId::new([9_u8; 32]),
            storage: StorageConfig::Memory,
        })
        .await
        .expect("big repo should boot")
    }

    #[tokio::test]
    async fn add_member_restores_tombstoned_item_payload() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-docs".into();
        let item_id = "item-1";
        let member_payload = serde_json::json!({});
        let expected_payload = serde_json::json!({ "k": "v" });

        store
            .add_member(&partition_id, item_id, &member_payload)
            .await
            .expect("membership add should succeed");
        store
            .record_item_change(&partition_id, item_id, &expected_payload)
            .await
            .expect("item change should succeed");
        store
            .remove_member(&partition_id, item_id, &member_payload)
            .await
            .expect("membership remove should succeed");
        store
            .add_member(&partition_id, item_id, &member_payload)
            .await
            .expect("membership re-add should succeed");

        let payload_json: String = sqlx::query_scalar(
            "SELECT item_payload_json FROM partition_item_state WHERE partition_id = ? AND item_id = ?",
        )
        .bind(&partition_id)
        .bind(item_id)
        .fetch_one(store.state_pool())
        .await
        .expect("item payload row should exist");
        let payload = serde_json::from_str::<serde_json::Value>(&payload_json)
            .expect("payload JSON should parse");
        assert_eq!(payload, expected_payload);
    }

    #[tokio::test]
    async fn subscribe_partition_doc_events_local_replays_then_tails() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-docs-local-sub".into();
        let item_id = "item-1";
        store
            .add_member(&partition_id, item_id, &serde_json::json!({}))
            .await
            .expect("membership add should succeed");
        store
            .record_item_change(&partition_id, item_id, &serde_json::json!({"a": 1}))
            .await
            .expect("item change should succeed");

        let mut rx = store
            .subscribe_partition_doc_events_local(&partition_id, Some(0), 8)
            .await
            .expect("local doc subscription should start");

        let replay_evt = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting replay event")
            .expect("channel closed while waiting replay event");
        assert_eq!(replay_evt.partition_id, partition_id);
        assert!(matches!(
            replay_evt.deets,
            PartitionDocEventDeets::ItemChanged { .. }
        ));

        store
            .record_item_deleted(&partition_id, item_id, &serde_json::json!({"a": 1}))
            .await
            .expect("item delete should succeed");
        let live_evt = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting live event")
            .expect("channel closed while waiting live event");
        assert!(matches!(
            live_evt.deets,
            PartitionDocEventDeets::ItemDeleted { .. }
        ));
    }

    #[tokio::test]
    async fn subscribe_partition_events_returns_even_when_replay_exceeds_capacity() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-replay".into();
        for idx in 0..8 {
            store
                .add_member(
                    &partition_id,
                    &format!("item-{idx}"),
                    &serde_json::json!({}),
                )
                .await
                .expect("membership add should succeed");
        }

        let req = SubPartitionsRequest {
            partitions: vec![crate::sync::protocol::PartitionStreamCursorRequest {
                partition_id: partition_id.clone(),
                since_member: None,
                since_doc: None,
            }],
        };
        let mut rx = timeout(
            Duration::from_millis(250),
            store.subscribe_partition_events_for_peer(&"peer-x".into(), &req, 1),
        )
        .await
        .expect("subscription call should not block on replay")
        .expect("subscription should initialize");

        let first_item = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("replay should produce at least one item");
        assert!(first_item.is_some());
    }

    #[tokio::test]
    async fn get_docs_full_respects_allowed_partitions() {
        let (repo, stop) = boot_repo().await;
        let partition_a: PartitionId = "p-a".into();
        let partition_b: PartitionId = "p-b".into();
        let doc_a = repo
            .put_doc(
                crate::repo::DocumentId::random(),
                automerge::Automerge::new(),
            )
            .await
            .unwrap();
        let doc_b = repo
            .put_doc(
                crate::repo::DocumentId::random(),
                automerge::Automerge::new(),
            )
            .await
            .unwrap();
        repo.partition_store()
            .add_member(
                &partition_a,
                &doc_a.document_id().to_string(),
                &serde_json::json!({}),
            )
            .await
            .unwrap();
        repo.partition_store()
            .add_member(
                &partition_b,
                &doc_b.document_id().to_string(),
                &serde_json::json!({}),
            )
            .await
            .unwrap();

        let allowed = repo
            .get_docs_full_in_partitions(
                &[doc_a.document_id().to_string()],
                std::slice::from_ref(&partition_a),
            )
            .await
            .expect("allowed doc should be readable");
        assert_eq!(allowed.len(), 1);
        assert_eq!(allowed[0].doc_id, doc_a.document_id().to_string());

        let denied = repo
            .get_docs_full_in_partitions(
                &[
                    doc_a.document_id().to_string(),
                    doc_b.document_id().to_string(),
                ],
                std::slice::from_ref(&partition_a),
            )
            .await;
        assert!(
            denied.is_err(),
            "access should be denied for partition-b doc"
        );

        stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn bigrepo_member_snapshot_paginates_all_docs() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-member-page".into();
        let mut expected = HashSet::new();
        for idx in 0..3 {
            let item_id = format!("item-{idx}");
            expected.insert(item_id.clone());
            store
                .add_member(&partition_id, &item_id, &serde_json::json!({}))
                .await
                .expect("membership add should succeed");
        }

        let mut since = None;
        let mut seen = Vec::new();
        loop {
            let response = store
                .get_partition_member_events_for_peer(
                    &"peer-x".into(),
                    &GetPartitionMemberEventsRequest {
                        partitions: vec![PartitionCursorRequest {
                            partition_id: partition_id.clone(),
                            since,
                        }],
                        limit: 1,
                    },
                )
                .await
                .expect("member snapshot page should succeed");
            seen.extend(response.events);
            let page = response
                .cursors
                .into_iter()
                .find(|page| page.partition_id == partition_id)
                .expect("page for partition must be returned");
            if !page.has_more {
                break;
            }
            since = page.next_cursor;
        }

        let item_ids = seen
            .into_iter()
            .map(|event| match event.deets {
                PartitionMemberEventDeets::MemberUpsert { item_id, .. }
                | PartitionMemberEventDeets::MemberRemoved { item_id, .. } => item_id,
            })
            .collect::<HashSet<_>>();
        assert_eq!(item_ids, expected);
    }

    #[tokio::test]
    async fn bigrepo_doc_snapshot_paginates_all_docs() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-doc-page".into();
        let mut expected = HashSet::new();
        for idx in 0..3 {
            let item_id = format!("item-{idx}");
            expected.insert(item_id.clone());
            store
                .add_member(&partition_id, &item_id, &serde_json::json!({}))
                .await
                .expect("membership add should succeed");
            store
                .record_item_change(&partition_id, &item_id, &serde_json::json!({"idx": idx}))
                .await
                .expect("doc event should succeed");
        }

        let mut since = None;
        let mut seen = Vec::new();
        loop {
            let response = store
                .get_partition_doc_events_for_peer(
                    &"peer-x".into(),
                    &GetPartitionDocEventsRequest {
                        partitions: vec![PartitionCursorRequest {
                            partition_id: partition_id.clone(),
                            since,
                        }],
                        limit: 1,
                    },
                )
                .await
                .expect("doc snapshot page should succeed");
            seen.extend(response.events);
            let page = response
                .cursors
                .into_iter()
                .find(|page| page.partition_id == partition_id)
                .expect("page for partition must be returned");
            if !page.has_more {
                break;
            }
            since = page.next_cursor;
        }

        let item_ids = seen
            .into_iter()
            .map(|event| match event.deets {
                PartitionDocEventDeets::ItemChanged { item_id, .. }
                | PartitionDocEventDeets::ItemDeleted { item_id, .. } => item_id,
            })
            .collect::<HashSet<_>>();
        assert_eq!(item_ids, expected);
    }
}
