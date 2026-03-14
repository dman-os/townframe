use crate::interlude::*;

use sqlx::{QueryBuilder, Row};
use std::collections::HashMap;
use std::str::FromStr;

use crate::repo::BigRepo;
use crate::sync::protocol::*;

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
        let existing_present: Option<i64> = sqlx::query_scalar(
            "SELECT present FROM partition_membership_state WHERE partition_id = ? AND doc_id = ?",
        )
        .bind(partition_id)
        .bind(doc_id)
        .fetch_optional(&self.state_pool)
        .await?;
        if existing_present == Some(1) {
            return Ok(());
        }
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

        let doc_ver_row = sqlx::query_as::<_, (String, i64)>(
            "SELECT latest_heads_json, change_count_hint FROM doc_version_state WHERE doc_id = ?",
        )
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;
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
        let (heads, change_count_hint) = if let Some((heads_json, change_count_hint)) = doc_ver_row
        {
            (
                serde_json::from_str::<Vec<String>>(&heads_json)?,
                change_count_hint.max(0) as u64,
            )
        } else {
            (Vec::new(), 0)
        };
        tx.commit().await?;

        self.partition_events_tx
            .send(PartitionEvent {
                cursor: membership_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::MemberUpsert {
                    doc_id: doc_id.to_owned(),
                },
            })
            .ok();
        self.partition_events_tx
            .send(PartitionEvent {
                cursor: doc_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::DocChanged {
                    doc_id: doc_id.to_owned(),
                    heads,
                    change_count_hint,
                },
            })
            .ok();
        Ok(())
    }

    pub async fn remove_doc_from_partition(
        &self,
        partition_id: &PartitionId,
        doc_id: &str,
    ) -> Res<()> {
        let existing_present: Option<i64> = sqlx::query_scalar(
            "SELECT present FROM partition_membership_state WHERE partition_id = ? AND doc_id = ?",
        )
        .bind(partition_id)
        .bind(doc_id)
        .fetch_optional(&self.state_pool)
        .await?;
        if existing_present != Some(1) {
            return Ok(());
        }
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

        self.partition_events_tx
            .send(PartitionEvent {
                cursor: membership_txid,
                partition_id: partition_id.clone(),
                deets: PartitionEventDeets::MemberRemoved {
                    doc_id: doc_id.to_owned(),
                },
            })
            .ok();
        if let Some((doc_txid, change_count_hint)) = doc_deleted_event {
            self.partition_events_tx
                .send(PartitionEvent {
                    cursor: doc_txid,
                    partition_id: partition_id.clone(),
                    deets: PartitionEventDeets::DocDeleted {
                        doc_id: doc_id.to_owned(),
                        change_count_hint,
                    },
                })
                .ok();
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
            debug!(
                partition_id,
                doc_id,
                cursor = txid,
                head_count = serialized_heads.len(),
                change_count_hint,
                "emitting partition doc changed event"
            );
            self.partition_events_tx
                .send(PartitionEvent {
                    cursor: txid,
                    partition_id,
                    deets: PartitionEventDeets::DocChanged {
                        doc_id: doc_id.clone(),
                        heads: serialized_heads.clone(),
                        change_count_hint,
                    },
                })
                .ok();
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

    // TODO: proper peer based permissioning
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
                    latest_cursor: latest_txid.max(0) as u64,
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

    pub async fn get_docs_full_in_partitions(
        &self,
        doc_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(PartitionSyncError::TooManyDocIds {
                requested: doc_ids.len(),
                max: MAX_GET_DOCS_FULL_DOC_IDS,
            }
            .into());
        }

        let mut dedup = HashSet::new();
        let requested_doc_ids: Vec<String> = doc_ids
            .iter()
            .filter(|doc_id| dedup.insert((*doc_id).clone()))
            .cloned()
            .collect();
        let denied_doc_id = self
            .find_first_inaccessible_doc_in_partitions(&requested_doc_ids, allowed_partitions)
            .await?;
        if let Some(denied) = denied_doc_id {
            return Err(PartitionSyncError::DocAccessDenied { doc_id: denied }.into());
        }

        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;
        let rows = futures::stream::iter(requested_doc_ids.into_iter().map(|doc_id| async move {
            let parsed = match samod::DocumentId::from_str(&doc_id) {
                Ok(val) => val,
                Err(_) => return Ok(None),
            };
            let doc = match self.repo.local_export(parsed).await {
                Ok(doc) => doc,
                Err(samod::LocalExportError::NotFound { .. }) => return Ok(None),
                Err(err) => {
                    return Err(eyre::Report::from(err).wrap_err("failed local-exporting doc"));
                }
            };
            Ok(Some(FullDoc {
                doc_id,
                automerge_save: doc.save(),
            }))
        }))
        .buffered_unordered(16)
        .collect::<Vec<Res<Option<FullDoc>>>>()
        .await;

        let mut out = Vec::new();
        for row in rows {
            if let Some(doc) = row? {
                out.push(doc);
            }
        }
        Ok(out)
    }

    pub async fn is_doc_accessible_in_partitions(
        &self,
        doc_id: &str,
        allowed_partitions: &[PartitionId],
    ) -> Res<bool> {
        if allowed_partitions.is_empty() {
            return Ok(false);
        }
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT EXISTS(SELECT 1 FROM partition_membership_state WHERE doc_id = ",
        );
        query.push_bind(doc_id);
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

    async fn find_first_inaccessible_doc_in_partitions(
        &self,
        doc_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Option<String>> {
        if doc_ids.is_empty() || allowed_partitions.is_empty() {
            return Ok(doc_ids.first().cloned());
        }
        let mut query = QueryBuilder::<sqlx::Sqlite>::new("WITH requested(doc_id) AS (SELECT ");
        for (idx, doc_id) in doc_ids.iter().enumerate() {
            if idx > 0 {
                query.push(" UNION ALL SELECT ");
            }
            query.push_bind(doc_id);
        }
        query.push(") SELECT requested.doc_id FROM requested WHERE NOT EXISTS (");
        query.push("SELECT 1 FROM partition_membership_state m WHERE m.doc_id = requested.doc_id");
        query.push(" AND m.present = 1 AND m.partition_id IN (");
        let mut partitions = query.separated(", ");
        for partition_id in allowed_partitions {
            partitions.push_bind(partition_id);
        }
        partitions.push_unseparated(")) LIMIT 1");
        let denied = query
            .build_query_scalar::<String>()
            .fetch_optional(&self.state_pool)
            .await?;
        Ok(denied)
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
        let mut rx_opt = Some(rx);
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
        'member_replay: loop {
            let replay_members = self
                .get_partition_member_events_for_peer(
                    peer,
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
                    break 'member_replay;
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
        if tx.is_closed() {
            return Ok(rx_opt
                .take()
                .expect("partition subscription response channel should exist"));
        }
        if tx
            .send(SubscriptionItem::ReplayComplete {
                stream: SubscriptionStreamKind::Member,
            })
            .await
            .is_err()
        {
            return Ok(rx_opt
                .take()
                .expect("partition subscription response channel should exist"));
        }
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
        'doc_replay: loop {
            let replay_docs = self
                .get_partition_doc_events_for_peer(
                    peer,
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
                    break 'doc_replay;
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
        if tx.is_closed() {
            return Ok(rx_opt
                .take()
                .expect("partition subscription response channel should exist"));
        }
        if tx
            .send(SubscriptionItem::ReplayComplete {
                stream: SubscriptionStreamKind::Doc,
            })
            .await
            .is_err()
        {
            return Ok(rx_opt
                .take()
                .expect("partition subscription response channel should exist"));
        }

        let cancel_token = self.partition_forwarder_cancel.clone();
        let fut = {
            let span = tracing::info_span!("partition live forwarder");
            async move {
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
                        PartitionEventDeets::MemberUpsert { doc_id } => {
                            let high_watermark =
                                *member_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            if tx
                                .send(SubscriptionItem::MemberEvent(PartitionMemberEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionMemberEventDeets::MemberUpsert { doc_id },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            member_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::MemberRemoved { doc_id } => {
                            let high_watermark =
                                *member_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            if tx
                                .send(SubscriptionItem::MemberEvent(PartitionMemberEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionMemberEventDeets::MemberRemoved { doc_id },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            member_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::DocChanged {
                            doc_id,
                            heads,
                            change_count_hint,
                        } => {
                            let high_watermark =
                                *doc_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            if tx
                                .send(SubscriptionItem::DocEvent(PartitionDocEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionDocEventDeets::DocChanged {
                                        doc_id,
                                        heads,
                                        change_count_hint,
                                    },
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            doc_high_watermark.insert(partition_id, txid);
                        }
                        PartitionEventDeets::DocDeleted {
                            doc_id,
                            change_count_hint,
                        } => {
                            let high_watermark =
                                *doc_high_watermark.get(&partition_id).unwrap_or(&0);
                            if txid <= high_watermark {
                                continue;
                            }
                            if tx
                                .send(SubscriptionItem::DocEvent(PartitionDocEvent {
                                    cursor: event.cursor,
                                    partition_id: partition_id.clone(),
                                    deets: PartitionDocEventDeets::DocDeleted {
                                        doc_id,
                                        change_count_hint,
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
        Ok(rx_opt
            .take()
            .expect("partition subscription response channel should exist"))
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
    .into())
}

async fn load_member_partition_page(
    pool: &sqlx::SqlitePool,
    req: &PartitionCursorRequest,
    limit: usize,
) -> Res<(Vec<PartitionMemberEvent>, Option<u64>, bool)> {
    let rows = if let Some(since) = req.since {
        sqlx::query(
            r#"
            SELECT txid, doc_id, kind FROM (
                SELECT added_at_txid AS txid, doc_id, 1 AS kind
                FROM partition_membership_state
                WHERE partition_id = ? AND added_at_txid IS NOT NULL AND added_at_txid > ?
                UNION ALL
                SELECT removed_at_txid AS txid, doc_id, 0 AS kind
                FROM partition_membership_state
                WHERE partition_id = ? AND removed_at_txid IS NOT NULL AND removed_at_txid > ?
            )
            ORDER BY txid, doc_id, kind
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
            SELECT COALESCE(added_at_txid, latest_txid) AS txid, doc_id, 1 AS kind
            FROM partition_membership_state
            WHERE partition_id = ? AND present = 1
            ORDER BY txid, doc_id
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
            let doc_id: String = row.try_get("doc_id")?;
            let kind: i64 = row.try_get("kind")?;
            let deets = match kind {
                1 => PartitionMemberEventDeets::MemberUpsert { doc_id },
                0 => PartitionMemberEventDeets::MemberRemoved { doc_id },
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
                ds.doc_id AS doc_id,
                ds.deleted AS deleted,
                ds.latest_txid AS state_txid,
                COALESCE(dv.latest_txid, 0) AS doc_txid,
                COALESCE(dv.latest_heads_json, '[]') AS heads_json,
                COALESCE(dv.change_count_hint, 0) AS change_count_hint,
                CASE
                    WHEN ds.deleted = 1 THEN ds.latest_txid
                    WHEN ds.latest_txid > ? THEN ds.latest_txid
                    ELSE COALESCE(dv.latest_txid, 0)
                END AS event_txid
            FROM partition_doc_state ds
            LEFT JOIN doc_version_state dv ON dv.doc_id = ds.doc_id
            WHERE ds.partition_id = ?
            AND (
                CASE
                    WHEN ds.deleted = 1 THEN ds.latest_txid
                    WHEN ds.latest_txid > ? THEN ds.latest_txid
                    ELSE COALESCE(dv.latest_txid, 0)
                END
            ) > ?
            ORDER BY event_txid, ds.doc_id
            LIMIT ?
            "#,
        )
        .bind(since as i64)
        .bind(&req.partition_id)
        .bind(since as i64)
        .bind(since as i64)
        .bind((limit + 1) as i64)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT
                ds.doc_id AS doc_id,
                0 AS deleted,
                ds.latest_txid AS state_txid,
                COALESCE(dv.latest_txid, ds.latest_txid) AS doc_txid,
                COALESCE(dv.latest_heads_json, '[]') AS heads_json,
                COALESCE(dv.change_count_hint, 0) AS change_count_hint,
                COALESCE(dv.latest_txid, ds.latest_txid) AS event_txid
            FROM partition_doc_state ds
            LEFT JOIN doc_version_state dv ON dv.doc_id = ds.doc_id
            WHERE ds.partition_id = ? AND ds.deleted = 0
            ORDER BY event_txid, ds.doc_id
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
            let doc_id: String = row.try_get("doc_id")?;
            let heads_json: String = row.try_get("heads_json")?;
            let change_count_hint: i64 = row.try_get("change_count_hint")?;
            let deleted: i64 = row.try_get("deleted")?;
            let deets = match deleted {
                0 => PartitionDocEventDeets::DocChanged {
                    doc_id,
                    heads: serde_json::from_str(&heads_json)?,
                    change_count_hint: change_count_hint.max(0) as u64,
                },
                1 => PartitionDocEventDeets::DocDeleted {
                    doc_id,
                    change_count_hint: change_count_hint.max(0) as u64,
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

    use crate::repo::{BigRepo, BigRepoConfig};
    use crate::sync::protocol::{
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
        let _partition_events_rx = big_repo.subscribe_partition_events();
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
        let _partition_events_rx = big_repo.subscribe_partition_events();
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

    #[tokio::test]
    async fn bigrepo_member_snapshot_paginates_all_docs() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let partition_id: PartitionId = "p-snapshot-members".into();
        let mut expected = std::collections::HashSet::new();
        for _ in 0..7 {
            let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
            let doc_id = handle.document_id().to_string();
            big_repo
                .add_doc_to_partition(&partition_id, &doc_id)
                .await?;
            expected.insert(doc_id);
        }

        let mut since = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = big_repo
                .get_partition_member_events_for_peer(
                    &"peer-a".into(),
                    &GetPartitionMemberEventsRequest {
                        partitions: vec![PartitionCursorRequest {
                            partition_id: partition_id.clone(),
                            since,
                        }],
                        limit: 3,
                    },
                )
                .await?;
            for evt in &page.events {
                if let PartitionMemberEventDeets::MemberUpsert { doc_id } = &evt.deets {
                    seen.insert(doc_id.clone());
                }
            }
            let cursor = page
                .cursors
                .iter()
                .find(|item| item.partition_id == partition_id)
                .expect(ERROR_IMPOSSIBLE);
            since = cursor.next_cursor;
            if !cursor.has_more {
                break;
            }
        }

        assert_eq!(seen, expected, "snapshot paging dropped member docs");
        Ok(())
    }

    #[tokio::test]
    async fn bigrepo_doc_snapshot_paginates_all_docs() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let partition_id: PartitionId = "p-snapshot-docs".into();
        let mut expected = std::collections::HashSet::new();
        for i in 0..7_u64 {
            let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
            handle
                .with_document(move |doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "idx", i)
                        .expect("failed setting test key");
                    tx.commit();
                })
                .await?;
            let doc_id = handle.document_id().to_string();
            big_repo
                .add_doc_to_partition(&partition_id, &doc_id)
                .await?;
            expected.insert(doc_id);
        }

        let mut since = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = big_repo
                .get_partition_doc_events_for_peer(
                    &"peer-a".into(),
                    &GetPartitionDocEventsRequest {
                        partitions: vec![PartitionCursorRequest {
                            partition_id: partition_id.clone(),
                            since,
                        }],
                        limit: 3,
                    },
                )
                .await?;
            for evt in &page.events {
                if let PartitionDocEventDeets::DocChanged { doc_id, .. } = &evt.deets {
                    seen.insert(doc_id.clone());
                }
            }
            let cursor = page
                .cursors
                .iter()
                .find(|item| item.partition_id == partition_id)
                .expect(ERROR_IMPOSSIBLE);
            since = cursor.next_cursor;
            if !cursor.has_more {
                break;
            }
        }

        assert_eq!(seen, expected, "snapshot paging dropped doc events");
        Ok(())
    }

    #[tokio::test]
    async fn get_docs_full_respects_allowed_partitions() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let p1: PartitionId = "p-allowed".into();
        let p2: PartitionId = "p-denied".into();

        let d1 = big_repo.create_doc(automerge::Automerge::new()).await?;
        let d1_id = d1.document_id().to_string();
        big_repo.add_doc_to_partition(&p1, &d1_id).await?;

        let d2 = big_repo.create_doc(automerge::Automerge::new()).await?;
        let d2_id = d2.document_id().to_string();
        big_repo.add_doc_to_partition(&p2, &d2_id).await?;

        assert!(
            big_repo
                .is_doc_accessible_in_partitions(&d1_id, std::slice::from_ref(&p1))
                .await?
        );
        assert!(
            !big_repo
                .is_doc_accessible_in_partitions(&d2_id, std::slice::from_ref(&p1))
                .await?
        );

        let err = big_repo
            .get_docs_full_in_partitions(&[d1_id.clone(), d2_id.clone()], &[p1])
            .await
            .expect_err("request should be denied when one requested doc is inaccessible");
        assert!(
            err.to_string().contains("access denied for doc"),
            "expected access denied error, got: {err}"
        );
        Ok(())
    }
}
