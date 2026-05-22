use super::{HostPartitionStore, ObjStoreLease, StoreMutationOutcome};
use crate::interlude::*;
#[cfg(test)]
use crate::test_support::{
    ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot, TestStoreSetup,
};

use big_sync_core::part_store::{CursorIndex, ObjPayload, PartStoreReadOnly};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest, BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED,
};
use big_sync_core::{mpsc, BuckId, Byte32Id, Fingerprint, ObjId, PartId, PeerId};
use future_form::{FutureForm, Sendable};
use futures::future::BoxFuture;
use sqlx::Row;
#[cfg(test)]
use uuid::Uuid;

#[derive(Clone)]
pub struct SqlitePartStore {
    read_pool: sqlx::SqlitePool,
    write_pool: sqlx::SqlitePool,
    scope_id: i64,
    _scope_key: Arc<str>,
    bus: Arc<std::sync::Mutex<HashMap<PartId, Vec<mpsc::Sender<SubEvent>>>>>,
}

impl SqlitePartStore {
    pub async fn new(
        read_pool: sqlx::SqlitePool,
        write_pool: sqlx::SqlitePool,
        scope_key: impl Into<Arc<str>>,
    ) -> Res<Self> {
        init_schema(&write_pool).await?;
        let scope_key = scope_key.into();
        let scope_id = Self::ensure_scope_id(&write_pool, &scope_key).await?;
        Ok(Self {
            read_pool,
            write_pool,
            scope_id,
            _scope_key: scope_key,
            bus: default(),
        })
    }

    async fn next_id(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, key: &str) -> Res<u64> {
        let value: i64 = sqlx::query_scalar(
            "UPDATE big_sync_meta SET value = value + 1 WHERE key = ?1 RETURNING value",
        )
        .bind(key)
        .fetch_one(&mut **tx)
        .await?;
        Ok(u64::try_from(value).expect(ERROR_IMPOSSIBLE))
    }

    async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        Self::next_id(tx, "global_cursor").await
    }

    fn id_blob(id: Byte32Id) -> Vec<u8> {
        id.into_bytes().to_vec()
    }

    fn part_blob(id: PartId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    fn obj_blob(id: ObjId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    fn peer_blob(id: PeerId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    fn part_from_blob(blob: Vec<u8>) -> PartId {
        PartId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        ObjId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    fn peer_from_blob(blob: Vec<u8>) -> PeerId {
        PeerId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    async fn ensure_scope_id(pool: &sqlx::SqlitePool, scope_key: &Arc<str>) -> Res<i64> {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        let scope_id = Self::ensure_scope_id_in_tx(&mut tx, scope_key).await?;
        tx.commit().await?;
        Ok(scope_id)
    }

    async fn ensure_scope_id_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        scope_key: &Arc<str>,
    ) -> Res<i64> {
        if let Some(scope_id) = sqlx::query_scalar::<_, i64>(
            "SELECT scope_id FROM big_sync_scopes WHERE scope_key = ?1",
        )
        .bind(scope_key.as_ref())
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(scope_id);
        }

        sqlx::query("INSERT INTO big_sync_scopes(scope_key) VALUES (?1)")
            .bind(scope_key.as_ref())
            .execute(&mut **tx)
            .await?;
        let scope_id: i64 =
            sqlx::query_scalar("SELECT scope_id FROM big_sync_scopes WHERE scope_key = ?1")
                .bind(scope_key.as_ref())
                .fetch_one(&mut **tx)
                .await?;
        Ok(scope_id)
    }

    async fn queue_transition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartId,
    ) -> Res<CursorIndex> {
        let cursor = Self::next_cursor(tx).await?;
        let cursor_i64 = i64::try_from(cursor).expect(ERROR_IMPOSSIBLE);
        sqlx::query(
            "UPDATE big_sync_parts
             SET latest_cursor = ?1
             WHERE scope_id = ?2 AND part_id = ?3",
        )
        .bind(cursor_i64)
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&mut **tx)
        .await?;
        Ok(cursor)
    }

    fn publish(&self, events: Vec<SubEvent>) {
        let mut bus = self.bus.lock().expect(ERROR_MUTEX);
        for event in events {
            let part_id = match &event {
                SubEvent::Upserted(transition) => transition.part_id,
                SubEvent::Deleted(transition) => transition.part_id,
                SubEvent::ReplayComplete => continue,
            };
            let Some(subs) = bus.get_mut(&part_id) else {
                continue;
            };
            subs.retain(|sub| sub.try_send(event.clone()).is_ok());
        }
    }

    async fn bucket_items_for_path(
        &self,
        part_id: PartId,
        path: BuckId,
    ) -> Res<Vec<(ObjId, CursorIndex, bool, Option<ObjPayload>)>> {
        let (lower, upper) = super::obj_id_bounds_for_bucket(path);
        let mut query = String::from(
            "SELECT members.obj_id, members.changed_at, members.removed_at, members.latest_cursor, objs.payload_json
             FROM big_sync_members members
             LEFT JOIN big_sync_objs objs
               ON objs.scope_id = members.scope_id AND objs.obj_id = members.obj_id
             WHERE members.scope_id = ?1 AND members.part_id = ?2 AND members.obj_id >= ?3",
        );
        if upper.is_some() {
            query.push_str(" AND members.obj_id < ?4");
        }
        query.push_str(" ORDER BY members.obj_id ASC");
        let mut q = sqlx::query(&query)
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(lower));
        if let Some(upper) = upper {
            q = q.bind(Self::obj_blob(upper));
        }
        let rows = q.fetch_all(&self.read_pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let removed_at: Option<i64> = row.try_get("removed_at")?;
            let latest_cursor: i64 = row.try_get("latest_cursor")?;
            let payload = if removed_at.is_some() {
                None
            } else {
                let payload_json: Option<String> = row.try_get("payload_json")?;
                Some(
                    serde_json::from_str(&payload_json.expect(ERROR_IMPOSSIBLE))
                        .wrap_err(ERROR_JSON)?,
                )
            };
            out.push((
                obj_id,
                u64::try_from(latest_cursor).expect(ERROR_IMPOSSIBLE),
                removed_at.is_some(),
                payload,
            ));
        }
        Ok(out)
    }

    async fn bucket_summary_for_path(&self, part_id: PartId, path: BuckId) -> Res<BucketSummary> {
        let items = self.bucket_items_for_path(part_id, path).await?;
        let mut live_fp = 0u64;
        let mut dead_fp = 0u64;
        let mut live_count = 0u32;
        let mut dead_count = 0u32;
        let mut changed_at = 0u64;

        for (obj_id, cursor, dead, payload) in items {
            changed_at = changed_at.max(cursor);
            if dead {
                dead_fp = dead_fp.wrapping_add(
                    Fingerprint::new(
                        &BUCKET_DEAD_FP_SEED,
                        &("big-sync-bucket-dead-v1", path, obj_id),
                    )
                    .as_u64(),
                );
                dead_count = dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
            } else {
                let payload = payload.expect(ERROR_IMPOSSIBLE);
                live_fp = live_fp.wrapping_add(
                    Fingerprint::new(
                        &BUCKET_LIVE_FP_SEED,
                        &("big-sync-bucket-live-v1", path, obj_id, payload),
                    )
                    .as_u64(),
                );
                live_count = live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
            }
        }

        Ok(BucketSummary {
            id: path,
            len: live_count + dead_count,
            live_count,
            fp: (live_fp, dead_fp),
            changed_at,
        })
    }
}

async fn init_schema(pool: &sqlx::SqlitePool) -> Res<()> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value INTEGER NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;
    for key in ["global_cursor", "lease_counter"] {
        sqlx::query("INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES (?1, 0)")
            .bind(key)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_scopes (
            scope_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_key TEXT NOT NULL UNIQUE
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_parts (
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            part_id BLOB NOT NULL,
            latest_cursor INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(scope_id, part_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_objs (
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            obj_id BLOB NOT NULL,
            payload_json TEXT,
            PRIMARY KEY(scope_id, obj_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_members (
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            part_id BLOB NOT NULL,
            obj_id BLOB NOT NULL,
            changed_at INTEGER NOT NULL,
            removed_at INTEGER,
            latest_cursor INTEGER NOT NULL,
            PRIMARY KEY(scope_id, part_id, obj_id),
            FOREIGN KEY(scope_id, part_id) REFERENCES big_sync_parts(scope_id, part_id),
            FOREIGN KEY(scope_id, obj_id) REFERENCES big_sync_objs(scope_id, obj_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS big_sync_members_part_latest_idx
         ON big_sync_members(scope_id, part_id, latest_cursor)",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS big_sync_members_obj_idx
         ON big_sync_members(scope_id, obj_id)",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_peer_cursors (
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            peer_id BLOB NOT NULL,
            part_id BLOB NOT NULL,
            cursor INTEGER NOT NULL,
            PRIMARY KEY(scope_id, peer_id, part_id),
            FOREIGN KEY(scope_id, part_id) REFERENCES big_sync_parts(scope_id, part_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

#[async_trait]
impl HostPartitionStore for SqlitePartStore {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>> {
        let mut out = HashMap::new();
        for part_id in parts {
            let row = sqlx::query(
                "SELECT latest_cursor
                 FROM big_sync_parts
                 WHERE scope_id = ?1 AND part_id = ?2",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .fetch_optional(&self.read_pool)
            .await?;
            let Some(row) = row else {
                return Ok(Err(ListPartsError::UnkownParts {
                    unkown_parts: vec![part_id],
                }));
            };
            let latest_cursor: i64 = row.try_get("latest_cursor")?;
            let member_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM big_sync_members
                 WHERE scope_id = ?1 AND part_id = ?2 AND removed_at IS NULL",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .fetch_one(&self.read_pool)
            .await?;
            out.insert(
                part_id,
                PartSummary {
                    latest_cursor: u64::try_from(latest_cursor).expect(ERROR_IMPOSSIBLE),
                    member_count: u64::try_from(member_count).expect(ERROR_IMPOSSIBLE),
                },
            );
        }
        Ok(Ok(out))
    }

    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        let member_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE scope_id = ?1 AND part_id = ?2 AND removed_at IS NULL",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .fetch_one(&self.read_pool)
        .await?;
        Ok(u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload_json
                 FROM big_sync_objs
                 WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&self.read_pool)
        .await?
        .flatten();
        payload
            .map(|payload| serde_json::from_str(&payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn upsert_obj(
        &self,
        obj_id: ObjId,
        payload: ObjPayload,
        parts: Vec<PartId>,
        _lease: Option<ObjStoreLease>,
    ) -> Res<StoreMutationOutcome> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO UPDATE SET payload_json = excluded.payload_json",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(&payload_json)
        .execute(&mut *tx)
        .await?;

        let mut events = Vec::new();
        for part_id in parts {
            sqlx::query(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .execute(&mut *tx)
            .await?;
            let cursor = self.queue_transition(&mut tx, part_id).await?;
            sqlx::query(
                "INSERT INTO big_sync_members(scope_id, part_id, obj_id, changed_at, removed_at, latest_cursor)
                 VALUES (?1, ?2, ?3, ?4, NULL, ?4)
                 ON CONFLICT(scope_id, part_id, obj_id) DO UPDATE SET
                    changed_at = excluded.changed_at,
                    removed_at = NULL,
                    latest_cursor = excluded.latest_cursor",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        tx.commit().await?;
        self.publish(events);
        Ok(StoreMutationOutcome::Applied)
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query(
            "SELECT part_id FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&self.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
            .collect())
    }

    async fn get_obj_lease(&self, _obj_id: ObjId) -> Res<ObjStoreLease> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let lease = Self::next_id(&mut tx, "lease_counter").await?;
        tx.commit().await?;
        Ok(lease)
    }

    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        self.bucket_summary_for_path(part_id, id).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        let summaries = self.summarize_parts(HashSet::from([req.part_id])).await?;
        if let Err(err) = summaries {
            return Ok(Err(err));
        }
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let summary = self
            .bucket_summary_for_path(req.part_id, req.offset)
            .await?;
        if summary.changed_at <= req.since {
            return Ok(Ok(Vec::new()));
        }
        Ok(Ok(vec![summary]))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        let summaries = self.summarize_parts(HashSet::from([req.part_id])).await?;
        if summaries.is_err() {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }

        let mut bucks = HashMap::new();
        for buck_req in req.buckets {
            let items = self
                .bucket_items_for_path(req.part_id, buck_req.buck_id)
                .await?;
            let start = match buck_req.after {
                Some(after) => items
                    .iter()
                    .position(|(obj_id, _, _, _)| *obj_id > after)
                    .unwrap_or(items.len()),
                None => 0,
            };
            let take = req.limit_hint.max(1) as usize;
            let end = (start + take).min(items.len());
            let done = end == items.len();
            let next_after = if done || start >= end {
                None
            } else {
                Some(items[end - 1].0)
            };
            let entries = items
                .into_iter()
                .skip(start)
                .take(take)
                .map(|(obj_id, _cursor, dead, payload)| {
                    let fp = if dead {
                        Fingerprint::new(
                            &req.seed,
                            &("big-sync-obj-fp-v1", obj_id, serde_json::Value::Null),
                        )
                    } else {
                        let payload = payload.expect(ERROR_IMPOSSIBLE);
                        Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
                    };
                    BucketObjPageEntry { obj_id, dead, fp }
                })
                .collect();
            bucks.insert(
                buck_req.buck_id,
                LeafBucketPage {
                    entries,
                    next_after,
                    done,
                },
            );
        }
        Ok(Ok(LeafBucketResult {
            seed: req.seed,
            bucks,
        }))
    }

    async fn add_obj_to_parts(
        &self,
        obj_id: ObjId,
        parts: Vec<PartId>,
        _lease: Option<ObjStoreLease>,
    ) -> Res<StoreMutationOutcome> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(scope_id, obj_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .execute(&mut *tx)
        .await?;
        let payload_json: Option<String> = sqlx::query_scalar(
            "SELECT payload_json
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let payload: ObjPayload = serde_json::from_str(
            &payload_json.expect("add_obj_to_parts requires an existing payload"),
        )
        .wrap_err(ERROR_JSON)?;
        let mut events = Vec::new();
        for part_id in parts {
            sqlx::query(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .execute(&mut *tx)
            .await?;
            let current = sqlx::query(
                "SELECT removed_at
                 FROM big_sync_members
                 WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .fetch_optional(&mut *tx)
            .await?;
            if current
                .as_ref()
                .and_then(|row| {
                    row.try_get::<Option<i64>, _>("removed_at")
                        .expect(ERROR_JSON)
                })
                .is_none()
                && current.is_some()
            {
                continue;
            }
            let cursor = self.queue_transition(&mut tx, part_id).await?;
            sqlx::query(
                "INSERT INTO big_sync_members(scope_id, part_id, obj_id, changed_at, removed_at, latest_cursor)
                 VALUES (?1, ?2, ?3, ?4, NULL, ?4)
                 ON CONFLICT(scope_id, part_id, obj_id) DO UPDATE SET
                    changed_at = excluded.changed_at,
                    removed_at = NULL,
                    latest_cursor = excluded.latest_cursor",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        tx.commit().await?;
        self.publish(events);
        Ok(StoreMutationOutcome::Applied)
    }

    async fn remove_obj_from_part(
        &self,
        obj_id: ObjId,
        part_id: PartId,
        _lease: Option<ObjStoreLease>,
    ) -> Res<StoreMutationOutcome> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(scope_id, obj_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&mut *tx)
        .await?;
        let current = sqlx::query(
            "SELECT removed_at
             FROM big_sync_members
             WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = current else {
            tx.commit().await?;
            return Ok(StoreMutationOutcome::Applied);
        };
        let removed_at: Option<i64> = row.try_get("removed_at")?;
        if removed_at.is_some() {
            tx.commit().await?;
            return Ok(StoreMutationOutcome::Applied);
        }

        let cursor = self.queue_transition(&mut tx, part_id).await?;
        sqlx::query(
            "UPDATE big_sync_members
             SET removed_at = ?1, latest_cursor = ?1
             WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
        )
        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .bind(Self::obj_blob(obj_id))
        .execute(&mut *tx)
        .await?;

        let live_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_one(&mut *tx)
        .await?;
        if live_count == 0 {
            sqlx::query(
                "UPDATE big_sync_objs
                 SET payload_json = NULL
                 WHERE scope_id = ?1 AND obj_id = ?2",
            )
            .bind(self.scope_id)
            .bind(Self::obj_blob(obj_id))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        self.publish(vec![SubEvent::Deleted(big_sync_core::rpc::ObjRemoved {
            cursor,
            part_id,
            obj_id,
        })]);
        Ok(StoreMutationOutcome::Applied)
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        let cursor: Option<i64> = sqlx::query_scalar(
            "SELECT cursor
             FROM big_sync_peer_cursors
             WHERE scope_id = ?1 AND peer_id = ?2 AND part_id = ?3",
        )
        .bind(self.scope_id)
        .bind(Self::peer_blob(peer_id))
        .bind(Self::part_blob(part_id))
        .fetch_optional(&self.read_pool)
        .await?;
        Ok(cursor
            .map(|cursor| u64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()> {
        sqlx::query(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&self.write_pool)
        .await?;
        sqlx::query(
            "INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_id, cursor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, peer_id, part_id) DO UPDATE SET cursor = excluded.cursor",
        )
        .bind(self.scope_id)
        .bind(Self::peer_blob(peer_id))
        .bind(Self::part_blob(part_id))
        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        let summaries = self.summarize_parts(parts.clone()).await?;
        if let Err(err) = summaries {
            return Ok(Err(err));
        }

        let mut out = HashMap::new();
        for part_id in parts {
            let rows = sqlx::query(
                "SELECT members.obj_id, members.changed_at, members.removed_at, members.latest_cursor, objs.payload_json
                 FROM big_sync_members members
                 LEFT JOIN big_sync_objs objs
                   ON objs.scope_id = members.scope_id AND objs.obj_id = members.obj_id
                 WHERE members.scope_id = ?1 AND members.part_id = ?2 AND members.latest_cursor > ?3
                 ORDER BY latest_cursor ASC
                 LIMIT ?4",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.read_pool)
            .await?;
            let mut next_cursor = None;
            let mut events = Vec::new();
            for row in rows {
                let row_cursor: i64 = row.try_get("latest_cursor")?;
                if events.len() >= usize::try_from(limit).expect(ERROR_IMPOSSIBLE) {
                    next_cursor = Some(u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE));
                    break;
                }
                let changed_at: i64 = row.try_get("changed_at")?;
                let removed_at: Option<i64> = row.try_get("removed_at")?;
                let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
                let removed_after_change = removed_at
                    .is_some_and(|removed_at| removed_at >= changed_at && removed_at == row_cursor);
                events.push(if removed_after_change {
                    PartEvent::Deleted(big_sync_core::rpc::ObjRemoved {
                        cursor: u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE),
                        part_id,
                        obj_id,
                    })
                } else {
                    PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                        cursor: u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE),
                        part_id,
                        obj_id,
                        payload: serde_json::from_str(
                            &row.try_get::<Option<String>, _>("payload_json")?
                                .expect(ERROR_IMPOSSIBLE),
                        )
                        .wrap_err(ERROR_JSON)?,
                    })
                });
            }
            out.insert(
                part_id,
                PartPage {
                    events,
                    next_cursor,
                },
            );
        }
        Ok(Ok(out))
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        let parts: HashSet<_> = reqs.parts.iter().map(|req| req.part_id).collect();
        let summaries = self.summarize_parts(parts.clone()).await?;
        if let Err(err) = summaries {
            return Ok(Err(err));
        }

        let cursor = reqs
            .parts
            .iter()
            .map(|req| req.cursor)
            .min()
            .unwrap_or_default();
        let (tx, rx) = mpsc::unbounded("SqlitePartStore".into(), "caller".into());
        let page = self
            .list_events(parts.clone(), cursor, u32::MAX)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        for (_, part_page) in page {
            for event in part_page.events {
                let sub_event = match event {
                    PartEvent::Upserted(transition) => SubEvent::Upserted(transition),
                    PartEvent::Deleted(transition) => SubEvent::Deleted(transition),
                };
                if tx.send(sub_event).await.is_err() {
                    return Ok(Ok(rx));
                }
            }
        }
        if tx.send(SubEvent::ReplayComplete).await.is_err() {
            return Ok(Ok(rx));
        }

        let mut bus = self.bus.lock().expect(ERROR_MUTEX);
        for part_id in parts {
            bus.entry(part_id).or_default().push(tx.clone());
        }
        Ok(Ok(rx))
    }
}

impl PartStoreReadOnly<Sendable> for SqlitePartStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        Sendable::from_future(async move {
            HostPartitionStore::member_count(self, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        Sendable::from_future(async move {
            HostPartitionStore::obj_payload(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn get_bucket_summary<'a>(
        &'a self,
        part_id: PartId,
        id: BuckId,
    ) -> BoxFuture<'a, BucketSummary> {
        Sendable::from_future(async move {
            HostPartitionStore::get_bucket_summary(self, part_id, id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        Sendable::from_future(async move {
            HostPartitionStore::obj_parts(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> BoxFuture<'a, CursorIndex> {
        Sendable::from_future(async move {
            HostPartitionStore::get_peer_part_cursor(self, peer_id, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }
}

impl big_sync_core::part_store::PartStore<Sendable> for SqlitePartStore {
    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> BoxFuture<'a, ()> {
        let payload = payload.clone();
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartitionStore::upsert_obj(self, obj_id, payload, parts, None)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartitionStore::add_obj_to_parts(self, obj_id, parts, None)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartitionStore::remove_obj_from_part(self, obj_id, part_id, None)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartitionStore::set_peer_part_cursor(self, peer_id, part_id, cursor)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }
}

#[cfg(test)]
#[async_trait]
impl ObservedStore for SqlitePartStore {
    async fn observed_snapshot(&self) -> Res<ObservedStoreSnapshot> {
        let rows = sqlx::query(
            "SELECT members.obj_id, members.part_id, objs.payload_json
             FROM big_sync_members members
             LEFT JOIN big_sync_objs objs
               ON objs.scope_id = members.scope_id AND objs.obj_id = members.obj_id
             WHERE members.scope_id = ?1 AND members.removed_at IS NULL
             ORDER BY members.obj_id ASC, members.part_id ASC",
        )
        .bind(self.scope_id)
        .fetch_all(&self.read_pool)
        .await?;
        let mut objs = std::collections::BTreeMap::new();
        for row in rows {
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let part_id = Self::part_from_blob(row.try_get("part_id")?);
            let payload_json: Option<String> = row.try_get("payload_json")?;
            let payload = payload_json
                .map(|payload| serde_json::from_str(&payload).wrap_err(ERROR_JSON))
                .transpose()?;
            let entry = objs.entry(obj_id).or_insert_with(|| ObservedObjSnapshot {
                payload: payload.clone(),
                parts: std::collections::BTreeSet::new(),
            });
            if entry.payload.is_none() {
                entry.payload = payload;
            }
            entry.parts.insert(part_id);
        }
        let cursors = sqlx::query(
            "SELECT peer_id, part_id, cursor
             FROM big_sync_peer_cursors
             WHERE scope_id = ?1",
        )
        .bind(self.scope_id)
        .fetch_all(&self.read_pool)
        .await?;
        let mut peer_part_cursors = std::collections::BTreeMap::new();
        for row in cursors {
            peer_part_cursors.insert(
                (
                    Self::peer_from_blob(row.try_get("peer_id")?),
                    Self::part_from_blob(row.try_get("part_id")?),
                ),
                u64::try_from(row.try_get::<i64, _>("cursor")?).expect(ERROR_IMPOSSIBLE),
            );
        }
        Ok(ObservedStoreSnapshot {
            objs,
            peer_part_cursors,
        })
    }
}

#[cfg(test)]
#[async_trait]
impl TestStoreSetup for SqlitePartStore {
    async fn ensure_test_part(&self, part_id: PartId) -> Res<()> {
        sqlx::query(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::part_store::contract;
    use std::str::FromStr;

    async fn test_pool() -> Res<sqlx::SqlitePool> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let db_url = format!("sqlite://{}", db_path.display());
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .locking_mode(sqlx::sqlite::SqliteLockingMode::Exclusive)
            .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await?;
        Ok(pool)
    }

    async fn test_store(scope_key: &str) -> Res<SqlitePartStore> {
        let pool = test_pool().await?;
        SqlitePartStore::new(pool.clone(), pool, scope_key).await
    }

    fn test_part_id(seed: u8) -> PartId {
        PartId(Byte32Id::new([seed; 32]))
    }

    fn test_obj_id(seed: u8) -> ObjId {
        ObjId(Byte32Id::new([seed; 32]))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_membership_semantics() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(1);
        let obj_id = test_obj_id(2);
        contract::assert_membership_semantics(&store, part_id, obj_id).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_add_obj_to_parts_is_idempotent() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(3);
        let obj_id = test_obj_id(4);
        contract::assert_add_obj_to_parts_is_idempotent(&store, part_id, obj_id).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_peer_cursor_roundtrip() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(5);
        contract::assert_peer_cursor_roundtrip(&store, PeerId(Byte32Id::new([42; 32])), part_id)
            .await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_root_bucket_contract() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(6);
        let seed = big_sync_core::FingerprintSeed::new(1, 2);

        let mut obj_ids = Vec::new();
        for ii in 0..5u8 {
            let obj_id = test_obj_id(10 + ii);
            HostPartitionStore::upsert_obj(
                &store,
                obj_id,
                serde_json::json!({"phase": "present", "ii": ii}),
                vec![part_id],
                None,
            )
            .await?;
            obj_ids.push(obj_id);
        }

        crate::part_store::contract::assert_root_bucket_contract(
            &store,
            part_id,
            seed,
            &obj_ids,
            &[],
            2,
        )
        .await?;

        let removed_obj_id = obj_ids[1];
        HostPartitionStore::remove_obj_from_part(&store, removed_obj_id, part_id, None).await?;
        let live_ids: Vec<_> = obj_ids
            .iter()
            .copied()
            .filter(|obj_id| *obj_id != removed_obj_id)
            .collect();
        crate::part_store::contract::assert_root_bucket_contract(
            &store,
            part_id,
            seed,
            &live_ids,
            &[removed_obj_id],
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_list_events_derives_latest_effective_transition() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(7);
        let obj_id = test_obj_id(8);

        HostPartitionStore::upsert_obj(
            &store,
            obj_id,
            serde_json::json!({"phase": "created"}),
            vec![part_id],
            None,
        )
        .await?;
        HostPartitionStore::remove_obj_from_part(&store, obj_id, part_id, None).await?;

        let deleted_page = HostPartitionStore::list_events(&store, HashSet::from([part_id]), 0, 10)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        let deleted_events = &deleted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(deleted_events.len(), 1);
        let PartEvent::Deleted(transition) = &deleted_events[0] else {
            panic!("expected deleted event");
        };
        assert_eq!(transition.cursor, 2);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);

        HostPartitionStore::upsert_obj(
            &store,
            obj_id,
            serde_json::json!({"phase": "recreated"}),
            vec![part_id],
            None,
        )
        .await?;

        let upserted_page =
            HostPartitionStore::list_events(&store, HashSet::from([part_id]), 0, 10)
                .await?
                .expect(ERROR_IMPOSSIBLE);
        let upserted_events = &upserted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(upserted_events.len(), 1);
        let PartEvent::Upserted(transition) = &upserted_events[0] else {
            panic!("expected upserted event");
        };
        assert_eq!(transition.cursor, 3);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_scopes_are_isolated_by_scope_key() -> Res<()> {
        let store_a = test_store("big-sync-sqlite-test://repo").await?;
        let store_b = SqlitePartStore::new(
            store_a.read_pool.clone(),
            store_a.write_pool.clone(),
            "big-sync-sqlite-test://other-repo",
        )
        .await?;

        let part_id = test_part_id(9);
        let obj_id = test_obj_id(10);

        HostPartitionStore::upsert_obj(
            &store_a,
            obj_id,
            serde_json::json!({"scope": "a"}),
            vec![part_id],
            None,
        )
        .await?;
        HostPartitionStore::upsert_obj(
            &store_b,
            obj_id,
            serde_json::json!({"scope": "b"}),
            vec![part_id],
            None,
        )
        .await?;

        assert_eq!(
            HostPartitionStore::obj_payload(&store_a, obj_id).await?,
            Some(serde_json::json!({"scope": "a"}))
        );
        assert_eq!(
            HostPartitionStore::obj_payload(&store_b, obj_id).await?,
            Some(serde_json::json!({"scope": "b"}))
        );
        assert_eq!(
            HostPartitionStore::member_count(&store_a, part_id).await?,
            1
        );
        assert_eq!(
            HostPartitionStore::member_count(&store_b, part_id).await?,
            1
        );
        Ok(())
    }
}
