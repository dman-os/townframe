use crate::interlude::*;

use sqlx::Row;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::sync::protocol::{
    GetPartitionItemEventsRequest, GetPartitionItemEventsResponse, GetPartitionMemberEventsRequest,
    GetPartitionMemberEventsResponse, ListPartitionsResponse, PartitionCursorPage,
    PartitionCursorRequest, PartitionEvent, PartitionEventDeets, PartitionId, PartitionItemEvent,
    PartitionItemEventDeets, PartitionMemberEvent, PartitionMemberEventDeets, PartitionSummary,
    SubPartitionsRequest, SubscriptionEvent, SubscriptionStreamKind, DEFAULT_EVENT_PAGE_LIMIT,
};
use std::time::Duration;

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
    pub async fn boot(state_pool: sqlx::SqlitePool) -> Res<(Arc<Self>, PartitionStoreStopToken)> {
        let (partition_events_tx, _) =
            broadcast::channel(crate::sync::protocol::DEFAULT_SUBSCRIPTION_CAPACITY);
        let partition_forwarder_cancel = CancellationToken::new();
        let partition_forwarders = Arc::new(utils_rs::AbortableJoinSet::new());
        let store = Arc::new(Self {
            state_pool,
            partition_events_tx,
            partition_forwarder_cancel: partition_forwarder_cancel.clone(),
            partition_forwarders: Arc::clone(&partition_forwarders),
        });
        store.ensure_schema().await?;
        Ok((
            store,
            PartitionStoreStopToken {
                partition_forwarder_cancel,
                partition_forwarders,
            },
        ))
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
            r#"CREATE TABLE IF NOT EXISTS partitions(
                partition_id TEXT PRIMARY KEY,
                latest_txid INTEGER NOT NULL DEFAULT 0,
                change_count INTEGER NOT NULL DEFAULT 0
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS items(
                item_id TEXT PRIMARY KEY,
                item_payload_json TEXT NOT NULL DEFAULT '{}',
                deleted INTEGER NOT NULL DEFAULT 0,
                latest_txid INTEGER NOT NULL DEFAULT 0,
                change_count INTEGER NOT NULL DEFAULT 0
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS partition_items(
                partition_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                present INTEGER NOT NULL,
                added_at_txid INTEGER NULL,
                removed_at_txid INTEGER NULL,
                latest_txid INTEGER NOT NULL,
                PRIMARY KEY(partition_id, item_id)
            )"#,
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_items_item
                ON partition_items(item_id)",
        )
        .execute(&self.state_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_partition_items_partition_latest
                ON partition_items(partition_id, latest_txid)",
        )
        .execute(&self.state_pool)
        .await?;
        Ok(())
    }
}

// queries
impl PartitionStore {
    pub async fn member_count(&self, part_id: &PartitionId) -> Res<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM partition_items WHERE partition_id = ? AND present = 1",
        )
        .bind(&part_id[..])
        .fetch_one(&self.state_pool)
        .await?;
        Ok(count)
    }

    pub async fn item_payload(&self, item_id: &str) -> Res<Option<serde_json::Value>> {
        let payload_json = sqlx::query_scalar::<_, String>(
            "SELECT item_payload_json FROM items WHERE item_id = ? AND deleted = 0",
        )
        .bind(item_id)
        .fetch_optional(&self.state_pool)
        .await?;
        payload_json
            .map(|json| serde_json::from_str::<serde_json::Value>(&json))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn item_partition_count(&self, item_id: &str) -> Res<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM partition_items WHERE item_id = ? AND present = 1",
        )
        .bind(item_id)
        .fetch_one(&self.state_pool)
        .await?;
        Ok(count)
    }

    pub async fn list_known_item_ids(&self) -> Res<Vec<String>> {
        let ids = sqlx::query_scalar(
            r#"
            SELECT DISTINCT item_id AS doc_id FROM items
            UNION
            SELECT DISTINCT item_id AS doc_id FROM partition_items
            "#,
        )
        .fetch_all(&self.state_pool)
        .await?;
        Ok(ids)
    }

    pub async fn is_item_member_of_partitions(
        &self,
        item_id: &str,
        allowed_partitions: &[PartitionId],
    ) -> Res<bool> {
        if allowed_partitions.is_empty() {
            return Ok(false);
        }
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT EXISTS(SELECT 1 FROM partition_items WHERE item_id = ",
        );
        query.push_bind(item_id);
        query.push(" AND present = 1 AND partition_id IN (");
        let mut separated = query.separated(", ");
        for partition_id in allowed_partitions {
            separated.push_bind(&partition_id[..]);
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
        query.push("SELECT 1 FROM partition_items m WHERE m.item_id = requested.item_id");
        query.push(" AND m.present = 1 AND m.partition_id IN (");
        let mut partitions = query.separated(", ");
        for partition_id in allowed_partitions {
            partitions.push_bind(&partition_id[..]);
        }
        partitions.push_unseparated(")) ORDER BY requested.ordinal LIMIT 1");
        let denied = query
            .build_query_scalar::<String>()
            .fetch_optional(&self.state_pool)
            .await?;
        Ok(denied)
    }

    // FIXME: it ought to be possible to dry this up with subscribe
    pub async fn subscribe_partition_item_events_local(
        &self,
        partition_id: &PartitionId,
        since: Option<u64>,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<PartitionItemEvent>> {
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
                        let (events, _next_cursor, has_more) = load_item_partition_page(
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
                                PartitionItemEventDeets::ItemChanged {
                                    item_id,
                                    payload: serde_json::to_string(&payload).expect(ERROR_JSON),
                                }
                            }
                            PartitionEventDeets::ItemDeleted { item_id, payload } => {
                                PartitionItemEventDeets::ItemDeleted {
                                    item_id,
                                    payload: serde_json::to_string(&payload).expect(ERROR_JSON),
                                }
                            }
                            _ => continue,
                        };
                        let doc_event = PartitionItemEvent {
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

// protocol
impl PartitionStore {
    pub async fn list_partitions(&self) -> Res<ListPartitionsResponse> {
        let rows = sqlx::query(
            r#"
            SELECT
                p.partition_id AS partition_id,
                COALESCE(pm.member_count, 0) AS member_count,
                COALESCE(mx.latest_txid, 0) AS latest_txid
            FROM (
                SELECT partition_id FROM partitions
                UNION
                SELECT DISTINCT partition_id FROM partition_items
            ) p
            LEFT JOIN (
                SELECT partition_id, COUNT(1) AS member_count
                FROM partition_items
                WHERE present = 1
                GROUP BY partition_id
            ) pm ON pm.partition_id = p.partition_id
            LEFT JOIN (
                SELECT partition_id, MAX(txid) AS latest_txid
                FROM (
                    SELECT partition_id, latest_txid AS txid FROM partitions
                    UNION ALL
                    SELECT partition_id, latest_txid AS txid FROM partition_items
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
                let partition_id = partition_id.into();
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

    pub async fn get_partition_member_events(
        &self,
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

    pub async fn get_partition_item_events(
        &self,
        req: &GetPartitionItemEventsRequest,
    ) -> Res<GetPartitionItemEventsResponse> {
        let limit = req.limit.clamp(1, MAX_PAGE_LIMIT) as usize;
        let mut events = Vec::with_capacity(req.partitions.len().saturating_mul(limit));
        let mut cursors = Vec::with_capacity(req.partitions.len());
        for part in &req.partitions {
            ensure_partition_exists(&self.state_pool, &part.partition_id).await?;
            let (mut part_events, next_cursor, has_more) =
                load_item_partition_page(&self.state_pool, part, limit).await?;
            events.append(&mut part_events);
            cursors.push(PartitionCursorPage {
                partition_id: part.partition_id.clone(),
                next_cursor,
                has_more,
            });
        }
        events.sort_by(cmp_item_events);
        Ok(GetPartitionItemEventsResponse { events, cursors })
    }

    pub async fn subscribe(
        &self,
        reqs: &SubPartitionsRequest,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<SubscriptionEvent>> {
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
                since: item.since_event,
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
                    item.since_event.unwrap_or_default(),
                )
            })
            .collect();
        let store = self.clone();
        let cancel_token = self.partition_forwarder_cancel.clone();
        let fut = {
            let span = tracing::info_span!("partition live forwarder");
            async move {
                loop {
                    let replay_members = store
                        .get_partition_member_events(&GetPartitionMemberEventsRequest {
                            partitions: member_parts.clone(),
                            limit: DEFAULT_EVENT_PAGE_LIMIT,
                        })
                        .await?;
                    for event in replay_members.events {
                        let entry = member_high_watermark
                            .entry(event.partition_id.clone())
                            .or_default();
                        *entry = (*entry).max(event.cursor);
                        if tx
                            .send(SubscriptionEvent::MemberEvent(event))
                            .await
                            .is_err()
                        {
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
                    .send(SubscriptionEvent::ReplayComplete {
                        stream: SubscriptionStreamKind::Member,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }

                loop {
                    let replay_docs = store
                        .get_partition_item_events(&GetPartitionItemEventsRequest {
                            partitions: doc_parts.clone(),
                            limit: DEFAULT_EVENT_PAGE_LIMIT,
                        })
                        .await?;
                    for event in replay_docs.events {
                        let entry = doc_high_watermark
                            .entry(event.partition_id.clone())
                            .or_default();
                        *entry = (*entry).max(event.cursor);
                        if tx.send(SubscriptionEvent::ItemEvent(event)).await.is_err() {
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
                    .send(SubscriptionEvent::ReplayComplete {
                        stream: SubscriptionStreamKind::Item,
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
                            let _ = tx.send(SubscriptionEvent::Lagged { dropped }).await;
                            break;
                        }
                    };
                    if !requested.contains(&event.partition_id) {
                        continue;
                    }
                    let txid = event.cursor;
                    let partition_id = event.partition_id.clone();
                    match event.deets {
                        PartitionEventDeets::MemberUpsert { item_id } => {
                            let high_watermark =
                                *member_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            if tx
                                .send(SubscriptionEvent::MemberEvent(PartitionMemberEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionMemberEventDeets::MemberUpsert { item_id },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            member_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::MemberRemoved { item_id } => {
                            let high_watermark =
                                *member_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            if tx
                                .send(SubscriptionEvent::MemberEvent(PartitionMemberEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionMemberEventDeets::MemberRemoved { item_id },
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
                                .send(SubscriptionEvent::ItemEvent(PartitionItemEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionItemEventDeets::ItemChanged {
                                        item_id,
                                        payload,
                                    },
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
                                .send(SubscriptionEvent::ItemEvent(PartitionItemEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionItemEventDeets::ItemDeleted {
                                        item_id,
                                        payload,
                                    },
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
}

// mutations
impl PartitionStore {
    pub async fn ensure_partition(&self, partition_id: &PartitionId) -> Res<()> {
        sqlx::query(
            r#"
        INSERT INTO partitions(partition_id, latest_txid, change_count)
        VALUES(?, 0, 0)
        ON CONFLICT(partition_id) DO NOTHING
        "#,
        )
        .bind(&partition_id[..])
        .execute(&self.state_pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_item(
        &self,
        item_id: Arc<str>,
        item_payload: &serde_json::Value,
        partition_ids: &[PartitionId],
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let txid = record_item_upsert_tx(tx.as_mut(), &item_id, item_payload).await?;
        let mut partition_ids_to_add = partition_ids.to_vec();
        partition_ids_to_add.sort();
        partition_ids_to_add.dedup();
        for partition_id in &partition_ids_to_add {
            attach_item_to_partition_tx(tx.as_mut(), partition_id, &item_id, txid).await?;
        }
        let current_partition_ids = current_item_partition_ids(tx.as_mut(), &item_id).await?;
        tx.commit().await?;

        for partition_id in &partition_ids_to_add {
            self.partition_events_tx
                .send(PartitionEvent {
                    cursor: txid,
                    partition_id: partition_id.clone(),
                    deets: PartitionEventDeets::MemberUpsert {
                        item_id: Arc::clone(&item_id),
                    },
                })
                .ok();
        }
        for partition_id in current_partition_ids {
            self.partition_events_tx
                .send(PartitionEvent {
                    cursor: txid,
                    partition_id,
                    deets: PartitionEventDeets::ItemChanged {
                        item_id: Arc::clone(&item_id),
                        payload: item_payload.clone(),
                    },
                })
                .ok();
        }
        Ok(())
    }

    pub async fn remove_item(&self, item_id: Arc<str>) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let Some(item_payload) = current_item_payload_value(tx.as_mut(), &item_id).await? else {
            tx.commit().await?;
            return Ok(());
        };
        let txid = record_item_delete_tx(tx.as_mut(), &item_id).await?;
        let partition_ids = current_item_partition_ids(tx.as_mut(), &item_id).await?;
        tx.commit().await?;

        for partition_id in partition_ids {
            self.partition_events_tx
                .send(PartitionEvent {
                    cursor: txid,
                    partition_id,
                    deets: PartitionEventDeets::ItemDeleted {
                        item_id: Arc::clone(&item_id),
                        payload: item_payload.clone(),
                    },
                })
                .ok();
        }
        Ok(())
    }

    pub async fn add_item_to_partition(
        &self,
        partition_id: &PartitionId,
        item_id: Arc<str>,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        let txid = alloc_txid(tx.as_mut()).await?;
        attach_item_to_partition_tx(tx.as_mut(), partition_id, &item_id, txid).await?;
        let item_payload = current_item_payload_value(tx.as_mut(), &item_id).await?;
        tx.commit().await?;

        self.partition_events_tx
            .send(PartitionEvent {
                cursor: txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::MemberUpsert {
                    item_id: Arc::clone(&item_id),
                },
            })
            .ok();
        if let Some(payload) = item_payload {
            self.partition_events_tx
                .send(PartitionEvent {
                    cursor: txid,
                    partition_id: partition_id.clone(),
                    deets: PartitionEventDeets::ItemChanged { item_id, payload },
                })
                .ok();
        }
        Ok(())
    }

    pub async fn remove_item_from_partition(
        &self,
        partition_id: PartitionId,
        item_id: Arc<str>,
    ) -> Res<()> {
        let mut tx = self.state_pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO partitions(partition_id, latest_txid, change_count)
            VALUES(?, 0, 0)
            ON CONFLICT(partition_id) DO NOTHING
            "#,
        )
        .bind(&partition_id[..])
        .execute(&mut *tx)
        .await?;
        let already_absent: i64 = sqlx::query_scalar(
            "SELECT NOT EXISTS(SELECT 1 FROM partition_items WHERE partition_id = ? AND item_id = ? AND present = 1)",
        )
        .bind(&partition_id[..])
        .bind(&item_id[..])
        .fetch_one(&mut *tx)
        .await?;
        if already_absent == 1 {
            tx.commit().await?;
            return Ok(());
        }
        let txid = alloc_txid(tx.as_mut()).await?;
        let rows_affected = sqlx::query(
            r#"
            UPDATE partition_items
            SET present = 0,
                removed_at_txid = ?,
                latest_txid = ?
            WHERE partition_id = ?
              AND item_id = ?
              AND present = 1
            "#,
        )
        .bind(txid as i64)
        .bind(txid as i64)
        .bind(&partition_id[..])
        .bind(&item_id[..])
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows_affected == 0 {
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query(
            r#"
            UPDATE partitions
            SET latest_txid = ?, change_count = change_count + 1
            WHERE partition_id = ?
            "#,
        )
        .bind(txid as i64)
        .bind(&partition_id[..])
        .execute(&mut *tx)
        .await?;
        let item_payload = current_item_payload_value(tx.as_mut(), &item_id)
            .await?
            .unwrap_or_else(|| serde_json::json!({}));
        tx.commit().await?;

        self.partition_events_tx
            .send(PartitionEvent {
                cursor: txid,
                partition_id: Arc::clone(&partition_id),
                deets: PartitionEventDeets::MemberRemoved {
                    item_id: Arc::clone(&item_id),
                },
            })
            .ok();
        self.partition_events_tx
            .send(PartitionEvent {
                cursor: txid,
                partition_id: partition_id,
                deets: PartitionEventDeets::ItemDeleted {
                    item_id,
                    payload: item_payload,
                },
            })
            .ok();
        Ok(())
    }
}

pub struct PartitionStoreStopToken {
    partition_forwarder_cancel: CancellationToken,
    partition_forwarders: Arc<utils_rs::AbortableJoinSet>,
}

impl PartitionStoreStopToken {
    pub async fn stop(self) -> Res<()> {
        self.partition_forwarder_cancel.cancel();
        match self.partition_forwarders.stop(Duration::from_secs(5)).await {
            Ok(()) => Ok(()),
            Err(utils_rs::AbortableJoinSetStopError::Timeout(_))
            | Err(utils_rs::AbortableJoinSetStopError::Aborted) => Ok(()),
            Err(err) => Err(err.into()),
        }
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
    // NOTE: txid starts from 1 to make 0 a good
    // zero state for cursors
    if out < 1 {
        eyre::bail!("invalid next_txid in sqlite: {next_value}");
    }
    let out = out as u64;
    Ok(out)
}

async fn record_item_upsert_tx(
    conn: &mut sqlx::SqliteConnection,
    item_id: &str,
    item_payload: &serde_json::Value,
) -> Res<u64> {
    let item_payload_json = serde_json::to_string(item_payload)?;
    let txid = alloc_txid(conn).await?;
    sqlx::query(
        r#"
        INSERT INTO items(item_id, item_payload_json, deleted, latest_txid, change_count)
        VALUES(?, ?, 0, ?, 1)
        ON CONFLICT(item_id) DO UPDATE SET
            item_payload_json = excluded.item_payload_json,
            deleted = 0,
            latest_txid = excluded.latest_txid,
            change_count = items.change_count + 1
        "#,
    )
    .bind(item_id)
    .bind(item_payload_json)
    .bind(txid as i64)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        r#"
        UPDATE partition_items
        SET latest_txid = ?
        WHERE item_id = ? AND present = 1
        "#,
    )
    .bind(txid as i64)
    .bind(item_id)
    .execute(&mut *conn)
    .await?;
    Ok(txid)
}

async fn attach_item_to_partition_tx(
    conn: &mut sqlx::SqliteConnection,
    partition_id: &PartitionId,
    item_id: &str,
    txid: u64,
) -> Res<()> {
    sqlx::query(
        r#"
        INSERT INTO partitions(partition_id, latest_txid, change_count)
        VALUES(?, 0, 0)
        ON CONFLICT(partition_id) DO NOTHING
        "#,
    )
    .bind(&partition_id[..])
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO partition_items(
            partition_id, item_id, present, added_at_txid, removed_at_txid, latest_txid
        ) VALUES(?, ?, 1, ?, NULL, ?)
        ON CONFLICT(partition_id, item_id) DO UPDATE SET
            present = 1,
            added_at_txid = excluded.added_at_txid,
            removed_at_txid = NULL,
            latest_txid = excluded.latest_txid
        "#,
    )
    .bind(&partition_id[..])
    .bind(item_id)
    .bind(txid as i64)
    .bind(txid as i64)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        r#"
        UPDATE partitions
        SET latest_txid = ?, change_count = change_count + 1
        WHERE partition_id = ?
        "#,
    )
    .bind(txid as i64)
    .bind(&partition_id[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn record_item_delete_tx(conn: &mut sqlx::SqliteConnection, item_id: &str) -> Res<u64> {
    let txid = alloc_txid(conn).await?;
    sqlx::query(
        r#"
        INSERT INTO items(item_id, item_payload_json, deleted, latest_txid, change_count)
        VALUES(?, '{}', 1, ?, 1)
        ON CONFLICT(item_id) DO UPDATE SET
            deleted = 1,
            latest_txid = excluded.latest_txid,
            change_count = items.change_count + 1
        "#,
    )
    .bind(item_id)
    .bind(txid as i64)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        r#"
        UPDATE partition_items
        SET latest_txid = ?
        WHERE item_id = ? AND present = 1
        "#,
    )
    .bind(txid as i64)
    .bind(item_id)
    .execute(&mut *conn)
    .await?;
    Ok(txid)
}

async fn current_item_payload_value(
    conn: &mut sqlx::SqliteConnection,
    item_id: &str,
) -> Res<Option<serde_json::Value>> {
    let row = sqlx::query("SELECT item_payload_json, deleted FROM items WHERE item_id = ?")
        .bind(item_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let deleted: i64 = row.try_get("deleted")?;
    if deleted != 0 {
        return Ok(None);
    }
    let payload_json: String = row.try_get("item_payload_json")?;
    Ok(Some(
        serde_json::from_str::<serde_json::Value>(&payload_json).wrap_err(ERROR_JSON)?,
    ))
}

async fn current_item_partition_ids(
    conn: &mut sqlx::SqliteConnection,
    item_id: &str,
) -> Res<Vec<PartitionId>> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT partition_id FROM partition_items WHERE item_id = ? AND present = 1",
    )
    .bind(item_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

async fn ensure_partition_exists(pool: &sqlx::SqlitePool, partition_id: &PartitionId) -> Res<()> {
    let found: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM partitions WHERE partition_id = ?
            UNION
            SELECT 1 FROM partition_items WHERE partition_id = ?
        )
        "#,
    )
    .bind(&partition_id[..])
    .bind(&partition_id[..])
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
            SELECT txid, item_id, kind FROM (
                SELECT added_at_txid AS txid, item_id, 1 AS kind
                FROM partition_items
                WHERE partition_id = ? AND added_at_txid IS NOT NULL AND added_at_txid > ?
                UNION ALL
                SELECT removed_at_txid AS txid, item_id, 0 AS kind
                FROM partition_items
                WHERE partition_id = ? AND removed_at_txid IS NOT NULL AND removed_at_txid > ?
            )
            ORDER BY txid, item_id, kind
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id[..])
        .bind(since as i64)
        .bind(&req.partition_id[..])
        .bind(since as i64)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT COALESCE(added_at_txid, latest_txid) AS txid, item_id, 1 AS kind
            FROM partition_items
            WHERE partition_id = ? AND present = 1
            ORDER BY txid, item_id
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id[..])
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
            let item_id = item_id.into();
            let kind: i64 = row.try_get("kind")?;
            let deets = match kind {
                1 => PartitionMemberEventDeets::MemberUpsert { item_id },
                0 => PartitionMemberEventDeets::MemberRemoved { item_id },
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

async fn load_item_partition_page(
    pool: &sqlx::SqlitePool,
    req: &PartitionCursorRequest,
    limit: usize,
) -> Res<(Vec<PartitionItemEvent>, Option<u64>, bool)> {
    let rows = if let Some(since) = req.since {
        sqlx::query(
            r#"
            SELECT
                pi.latest_txid AS event_txid,
                pi.item_id AS item_id,
                pi.present AS present,
                COALESCE(i.deleted, 0) AS deleted,
                COALESCE(i.item_payload_json, '{}') AS payload_json
            FROM partition_items pi
            LEFT JOIN items i ON i.item_id = pi.item_id
            WHERE pi.partition_id = ? AND pi.latest_txid > ?
            ORDER BY event_txid, item_id
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id[..])
        .bind(since as i64)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT
                pi.item_id AS item_id,
                0 AS deleted,
                pi.latest_txid AS event_txid,
                COALESCE(i.item_payload_json, '{}') AS payload_json,
                pi.present AS present
            FROM partition_items pi
            LEFT JOIN items i ON i.item_id = pi.item_id
            WHERE pi.partition_id = ? AND pi.present = 1 AND COALESCE(i.deleted, 0) = 0
            ORDER BY event_txid, item_id
            LIMIT ?
            "#,
        )
        .bind(&req.partition_id[..])
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    };

    let has_more = rows.len() > limit;
    let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
    let events = rows
        .into_iter()
        .map(|row| -> Res<PartitionItemEvent> {
            let event_txid: i64 = row.try_get("event_txid")?;
            let item_id: String = row.try_get("item_id")?;
            let item_id = item_id.into();
            let payload_json: String = row.try_get("payload_json")?;
            let present: i64 = row.try_get("present")?;
            let deleted: i64 = row.try_get("deleted")?;
            let deets = match (present, deleted) {
                (1, 0) => PartitionItemEventDeets::ItemChanged {
                    item_id,
                    payload: payload_json,
                },
                _ => PartitionItemEventDeets::ItemDeleted {
                    item_id,
                    payload: payload_json,
                },
            };
            Ok(PartitionItemEvent {
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

fn cmp_item_events(left: &PartitionItemEvent, right: &PartitionItemEvent) -> std::cmp::Ordering {
    left.cursor
        .cmp(&right.cursor)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tokio::time::{timeout, Duration};

    async fn make_store() -> Arc<PartitionStore> {
        let (store, _stop) = crate::repo::tests::boot_part_store(&"sqlite::memory:")
            .await
            .expect("error booting in memory part store");
        store
    }

    #[tokio::test]
    async fn upsert_item_restores_tombstoned_item_payload() {
        let store = make_store().await;
        let item_id = "item-1";
        let expected_payload = serde_json::json!({ "k": "v" });

        store
            .upsert_item(item_id.into(), &expected_payload, &[])
            .await
            .expect("item upsert should succeed");
        store
            .remove_item(item_id.into())
            .await
            .expect("item remove should succeed");
        store
            .upsert_item(item_id.into(), &expected_payload, &[])
            .await
            .expect("item re-upsert should succeed");

        let payload_json: String = sqlx::query_scalar(
            "SELECT item_payload_json FROM items WHERE item_id = ? AND deleted = 0",
        )
        .bind(item_id)
        .fetch_one(store.state_pool())
        .await
        .expect("item payload row should exist");
        let payload = serde_json::from_str::<serde_json::Value>(&payload_json)
            .expect("payload JSON should parse");
        assert_eq!(payload, expected_payload);
    }

    #[tokio::test]
    async fn upsert_item_can_attach_partitions_in_one_tx() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-one-tx".into();
        let item_id = Arc::<str>::from("item-1");
        let expected_payload = serde_json::json!({ "k": "v" });

        store
            .upsert_item(
                Arc::clone(&item_id),
                &expected_payload,
                std::slice::from_ref(&partition_id),
            )
            .await
            .expect("item upsert should succeed");

        assert_eq!(
            store
                .item_payload(&item_id)
                .await
                .expect("item payload should read"),
            Some(expected_payload)
        );
        assert_eq!(
            store
                .member_count(&partition_id)
                .await
                .expect("partition should exist"),
            1
        );
        assert_eq!(
            store
                .item_partition_count(&item_id)
                .await
                .expect("item membership count should read"),
            1
        );
    }

    #[tokio::test]
    async fn subscribe_partition_item_events_local_replays_then_tails() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-docs-local-sub".into();
        let item_id = "item-1";
        store
            .upsert_item(item_id.into(), &serde_json::json!({}), &[])
            .await
            .expect("item upsert should succeed");
        store
            .add_item_to_partition(&partition_id, item_id.into())
            .await
            .expect("item membership should succeed");
        store
            .upsert_item(item_id.into(), &serde_json::json!({"a": 1}), &[])
            .await
            .expect("item change should succeed");

        let mut rx = store
            .subscribe_partition_item_events_local(&partition_id, Some(0), 8)
            .await
            .expect("local doc subscription should start");

        let replay_evt = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting replay event")
            .expect("channel closed while waiting replay event");
        assert_eq!(replay_evt.partition_id, partition_id);
        assert!(matches!(
            replay_evt.deets,
            PartitionItemEventDeets::ItemChanged { .. }
        ));

        store
            .remove_item_from_partition(Arc::clone(&partition_id), item_id.into())
            .await
            .expect("item delete should succeed");
        let live_evt = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting live event")
            .expect("channel closed while waiting live event");
        assert!(matches!(
            live_evt.deets,
            PartitionItemEventDeets::ItemDeleted { .. }
        ));
    }

    #[tokio::test]
    async fn subscribe_partition_events_returns_even_when_replay_exceeds_capacity() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-replay".into();
        for idx in 0..8 {
            store
                .upsert_item(format!("item-{idx}").into(), &serde_json::json!({}), &[])
                .await
                .expect("item upsert should succeed");
            store
                .add_item_to_partition(&partition_id, format!("item-{idx}").into())
                .await
                .expect("item membership should succeed");
        }

        let req = SubPartitionsRequest {
            partitions: vec![crate::sync::protocol::PartitionStreamCursorRequest {
                partition_id: partition_id.clone(),
                since_member: None,
                since_event: None,
            }],
        };
        let mut rx = timeout(Duration::from_millis(250), store.subscribe(&req, 1))
            .await
            .expect("subscription call should not block on replay")
            .expect("subscription should initialize");

        let first_item = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("replay should produce at least one item");
        assert!(first_item.is_some());
    }

    #[tokio::test]
    async fn get_docs_full_respects_allowed_partitions() -> Res<()> {
        let (repo, partition_store, stop) = crate::repo::tests::boot_repo().await?;
        let partition_a: PartitionId = "p-a".into();
        let partition_b: PartitionId = "p-b".into();
        let doc_a = repo
            .put_doc(
                crate::repo::DocumentId::random(),
                automerge::Automerge::new(),
            )
            .await?;
        let doc_b = repo
            .put_doc(
                crate::repo::DocumentId::random(),
                automerge::Automerge::new(),
            )
            .await?;
        partition_store
            .upsert_item(
                doc_a.document_id().to_string().into(),
                &serde_json::json!({}),
                &[],
            )
            .await?;
        partition_store
            .add_item_to_partition(&partition_a, doc_a.document_id().to_string().into())
            .await?;
        partition_store
            .upsert_item(
                doc_b.document_id().to_string().into(),
                &serde_json::json!({}),
                &[],
            )
            .await?;
        partition_store
            .add_item_to_partition(&partition_b, doc_b.document_id().to_string().into())
            .await?;

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

        stop().await
    }

    #[tokio::test]
    async fn bigrepo_member_snapshot_paginates_all_docs() {
        let store = make_store().await;
        let partition_id: PartitionId = "p-member-page".into();
        let mut expected = HashSet::new();
        for idx in 0..3 {
            let item_id = format!("item-{idx}").into();
            expected.insert(Arc::clone(&item_id));
            store
                .upsert_item(Arc::clone(&item_id), &serde_json::json!({}), &[])
                .await
                .expect("membership add should succeed");
            store
                .add_item_to_partition(&partition_id, item_id)
                .await
                .expect("membership add should succeed");
        }

        let mut since = None;
        let mut seen = Vec::new();
        loop {
            let response = store
                .get_partition_member_events(&GetPartitionMemberEventsRequest {
                    partitions: vec![PartitionCursorRequest {
                        partition_id: partition_id.clone(),
                        since,
                    }],
                    limit: 1,
                })
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
            let item_id = format!("item-{idx}").into();
            expected.insert(Arc::clone(&item_id));
            store
                .upsert_item(Arc::clone(&item_id), &serde_json::json!({"idx": idx}), &[])
                .await
                .expect("membership add should succeed");
            store
                .add_item_to_partition(&partition_id, Arc::clone(&item_id))
                .await
                .expect("membership add should succeed");
        }

        let mut since = None;
        let mut seen = Vec::new();
        loop {
            let response = store
                .get_partition_item_events(&GetPartitionItemEventsRequest {
                    partitions: vec![PartitionCursorRequest {
                        partition_id: partition_id.clone(),
                        since,
                    }],
                    limit: 1,
                })
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
                PartitionItemEventDeets::ItemChanged { item_id, .. }
                | PartitionItemEventDeets::ItemDeleted { item_id, .. } => item_id,
            })
            .collect::<HashSet<_>>();
        assert_eq!(item_ids, expected);
    }
}
