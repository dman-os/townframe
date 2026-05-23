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
use sqlx::{QueryBuilder, Row};
#[cfg(test)]
use uuid::Uuid;

#[derive(Clone)]
pub struct SqlitePartStore {
    read_pool: sqlx::SqlitePool,
    write_pool: sqlx::SqlitePool,
    scope_id: i64,
    bucket_depth: u8,
    _scope_key: Arc<str>,
    bus: Arc<std::sync::Mutex<HashMap<PartId, Vec<mpsc::Sender<SubEvent>>>>>,
}

#[derive(Default, Clone, Copy)]
struct BucketSummaryRow {
    changed_at: u64,
    live_count: u64,
    dead_count: u64,
    live_fp: u64,
    dead_fp: u64,
}

enum MemberState {
    Absent,
    Live(ObjPayload),
    Dead,
}

impl BucketSummaryRow {
    fn apply_transition(
        &mut self,
        buck_id: BuckId,
        obj_id: ObjId,
        cursor: CursorIndex,
        old: &MemberState,
        new: &MemberState,
    ) {
        self.changed_at = cursor;
        match old {
            MemberState::Absent => {}
            MemberState::Live(payload) => {
                self.live_count = self.live_count.checked_sub(1).expect(ERROR_IMPOSSIBLE);
                self.live_fp = self.live_fp.wrapping_sub(
                    Fingerprint::new(
                        &BUCKET_LIVE_FP_SEED,
                        &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
                    )
                    .as_u64(),
                );
            }
            MemberState::Dead => {
                self.dead_count = self.dead_count.checked_sub(1).expect(ERROR_IMPOSSIBLE);
                self.dead_fp = self.dead_fp.wrapping_sub(
                    Fingerprint::new(
                        &BUCKET_DEAD_FP_SEED,
                        &("big-sync-bucket-dead-v1", buck_id, obj_id),
                    )
                    .as_u64(),
                );
            }
        }
        match new {
            MemberState::Absent => {}
            MemberState::Live(payload) => {
                self.live_count = self.live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
                self.live_fp = self.live_fp.wrapping_add(
                    Fingerprint::new(
                        &BUCKET_LIVE_FP_SEED,
                        &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
                    )
                    .as_u64(),
                );
            }
            MemberState::Dead => {
                self.dead_count = self.dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
                self.dead_fp = self.dead_fp.wrapping_add(
                    Fingerprint::new(
                        &BUCKET_DEAD_FP_SEED,
                        &("big-sync-bucket-dead-v1", buck_id, obj_id),
                    )
                    .as_u64(),
                );
            }
        }
    }
}

impl SqlitePartStore {
    pub async fn new(
        read_pool: sqlx::SqlitePool,
        write_pool: sqlx::SqlitePool,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
    ) -> Res<Self> {
        init_schema(&write_pool, bucket_depth).await?;
        let scope_key = scope_key.into();
        let scope_id = Self::ensure_scope_id(&write_pool, &scope_key).await?;
        Ok(Self {
            read_pool,
            write_pool,
            scope_id,
            bucket_depth,
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

    async fn current_obj_lease(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjId,
    ) -> Res<Option<ObjStoreLease>> {
        let lease: Option<i64> = sqlx::query_scalar(
            "SELECT lease
             FROM big_sync_obj_leases
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut **tx)
        .await?;
        Ok(lease.map(|lease| u64::try_from(lease).expect(ERROR_IMPOSSIBLE)))
    }

    async fn set_obj_lease(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjId,
        lease: ObjStoreLease,
    ) -> Res<()> {
        sqlx::query(
            "INSERT INTO big_sync_obj_leases(scope_id, obj_id, lease)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO UPDATE SET lease = excluded.lease",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(i64::try_from(lease).expect(ERROR_IMPOSSIBLE))
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn clear_obj_lease(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjId,
    ) -> Res<()> {
        sqlx::query(
            "DELETE FROM big_sync_obj_leases
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .execute(&mut **tx)
        .await?;
        Ok(())
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

    fn buck_i64(id: BuckId) -> i64 {
        (i64::from(id.level()) << 16) | i64::from(id.index())
    }

    fn buck_id(value: i64) -> BuckId {
        BuckId::new((value >> 16) as u8, value as u16)
    }

    fn db_from_u64(value: u64) -> i64 {
        i64::from_ne_bytes(value.to_ne_bytes())
    }

    fn u64_from_db(value: i64) -> u64 {
        u64::from_ne_bytes(value.to_ne_bytes())
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

    async fn load_member_state(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartId,
        obj_id: ObjId,
    ) -> Res<MemberState> {
        let row = sqlx::query(
            "SELECT members.removed_at, objs.payload_json
             FROM big_sync_members members
             LEFT JOIN big_sync_objs objs
               ON objs.scope_id = members.scope_id AND objs.obj_id = members.obj_id
             WHERE members.scope_id = ?1 AND members.part_id = ?2 AND members.obj_id = ?3",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Ok(MemberState::Absent);
        };
        let removed_at: Option<i64> = row.try_get("removed_at")?;
        if removed_at.is_some() {
            return Ok(MemberState::Dead);
        }
        let payload_json: Option<String> = row.try_get("payload_json")?;
        let payload =
            serde_json::from_str(&payload_json.expect("live member must still have payload"))
                .wrap_err(ERROR_JSON)?;
        Ok(MemberState::Live(payload))
    }

    async fn apply_bucket_transition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartId,
        obj_id: ObjId,
        cursor: CursorIndex,
        old: &MemberState,
        new: &MemberState,
    ) -> Res<()> {
        let bucket_ids: Vec<_> = (0..=self.bucket_depth)
            .map(|level| BuckId::from_obj_id(level, &obj_id))
            .collect();
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT buck_id, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.push_bind(self.scope_id);
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(part_id));
        query.push(" AND buck_id IN (");
        let mut separated = query.separated(", ");
        for buck_id in &bucket_ids {
            separated.push_bind(Self::buck_i64(*buck_id));
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&mut **tx).await?;
        let mut current = HashMap::with_capacity(rows.len());
        for row in rows {
            let buck_id = Self::buck_id(row.try_get::<i64, _>("buck_id")?);
            current.insert(
                buck_id,
                BucketSummaryRow {
                    changed_at: u64::try_from(row.try_get::<i64, _>("changed_at")?)
                        .expect(ERROR_IMPOSSIBLE),
                    live_count: u64::try_from(row.try_get::<i64, _>("live_count")?)
                        .expect(ERROR_IMPOSSIBLE),
                    dead_count: u64::try_from(row.try_get::<i64, _>("dead_count")?)
                        .expect(ERROR_IMPOSSIBLE),
                    live_fp: Self::u64_from_db(row.try_get::<i64, _>("live_fp")?),
                    dead_fp: Self::u64_from_db(row.try_get::<i64, _>("dead_fp")?),
                },
            );
        }
        for buck_id in bucket_ids {
            let mut summary = current.remove(&buck_id).unwrap_or_default();
            summary.apply_transition(buck_id, obj_id, cursor, old, new);
            sqlx::query(
                "INSERT INTO big_sync_buckets(
                    scope_id, part_id, buck_id, level, changed_at,
                    live_count, dead_count, live_fp, dead_fp
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(scope_id, part_id, buck_id) DO UPDATE SET
                    level = excluded.level,
                    changed_at = excluded.changed_at,
                    live_count = excluded.live_count,
                    dead_count = excluded.dead_count,
                    live_fp = excluded.live_fp,
                    dead_fp = excluded.dead_fp",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::buck_i64(buck_id))
            .bind(i64::from(buck_id.level()))
            .bind(i64::try_from(summary.changed_at).expect(ERROR_IMPOSSIBLE))
            .bind(i64::try_from(summary.live_count).expect(ERROR_IMPOSSIBLE))
            .bind(i64::try_from(summary.dead_count).expect(ERROR_IMPOSSIBLE))
            .bind(Self::db_from_u64(summary.live_fp))
            .bind(Self::db_from_u64(summary.dead_fp))
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
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

    async fn bucket_summary_for_path(&self, part_id: PartId, path: BuckId) -> Res<BucketSummary> {
        let row = sqlx::query(
            "SELECT changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_id = ?2 AND level = ?3 AND buck_id = ?4",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .bind(i64::from(path.level()))
        .bind(Self::buck_i64(path))
        .fetch_optional(&self.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(BucketSummary {
                id: path,
                len: 0,
                live_count: 0,
                fp: (0, 0),
                changed_at: 0,
            });
        };
        let changed_at: i64 = row.try_get("changed_at")?;
        let live_count: i64 = row.try_get("live_count")?;
        let dead_count: i64 = row.try_get("dead_count")?;
        let live_fp: i64 = row.try_get("live_fp")?;
        let dead_fp: i64 = row.try_get("dead_fp")?;
        Ok(BucketSummary {
            id: path,
            len: u32::try_from(
                u64::try_from(live_count).expect(ERROR_IMPOSSIBLE)
                    + u64::try_from(dead_count).expect(ERROR_IMPOSSIBLE),
            )
            .expect(ERROR_IMPOSSIBLE),
            live_count: u32::try_from(live_count).expect(ERROR_IMPOSSIBLE),
            fp: (Self::u64_from_db(live_fp), Self::u64_from_db(dead_fp)),
            changed_at: u64::try_from(changed_at).expect(ERROR_IMPOSSIBLE),
        })
    }
}

async fn init_schema(pool: &sqlx::SqlitePool, bucket_depth: u8) -> Res<()> {
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
    sqlx::query("INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES ('bucket_depth', ?1)")
        .bind(i64::from(bucket_depth))
        .execute(&mut *tx)
        .await?;
    let existing_bucket_depth: i64 = sqlx::query_scalar(
        "SELECT value
         FROM big_sync_meta
         WHERE key = 'bucket_depth'",
    )
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(
        u8::try_from(existing_bucket_depth).expect(ERROR_IMPOSSIBLE),
        bucket_depth,
        "bucket depth is fixed for the database"
    );
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
        "CREATE TABLE IF NOT EXISTS big_sync_buckets (
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            part_id BLOB NOT NULL,
            buck_id INTEGER NOT NULL,
            level INTEGER NOT NULL,
            changed_at INTEGER NOT NULL DEFAULT 0,
            live_count INTEGER NOT NULL DEFAULT 0,
            dead_count INTEGER NOT NULL DEFAULT 0,
            live_fp INTEGER NOT NULL DEFAULT 0,
            dead_fp INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(scope_id, part_id, buck_id),
            FOREIGN KEY(scope_id, part_id) REFERENCES big_sync_parts(scope_id, part_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS big_sync_buckets_level_changed_idx
         ON big_sync_buckets(scope_id, part_id, level, changed_at, buck_id)",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_obj_leases (
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            obj_id BLOB NOT NULL,
            lease INTEGER NOT NULL,
            PRIMARY KEY(scope_id, obj_id),
            FOREIGN KEY(scope_id, obj_id) REFERENCES big_sync_objs(scope_id, obj_id)
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
        if parts.is_empty() {
            return Ok(Ok(HashMap::new()));
        }

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT p.part_id, p.latest_cursor, COALESCE(b.live_count, 0) AS member_count
             FROM big_sync_parts p
             LEFT JOIN big_sync_buckets b
               ON b.scope_id = p.scope_id
              AND b.part_id = p.part_id
              AND b.level = 0
              AND b.buck_id = 0
             WHERE p.scope_id = ",
        );
        query.push_bind(self.scope_id);
        query.push(" AND p.part_id IN (");
        let mut separated = query.separated(", ");
        for part_id in &parts {
            separated.push_bind(Self::part_blob(*part_id));
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.read_pool).await?;

        if rows.len() != parts.len() {
            let found: HashSet<PartId> = rows
                .iter()
                .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut missing: Vec<_> = parts.difference(&found).copied().collect();
            missing.sort();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: missing,
            }));
        }

        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let part_id = Self::part_from_blob(row.try_get("part_id")?);
            let latest_cursor: i64 = row.try_get("latest_cursor")?;
            let member_count: i64 = row.try_get("member_count")?;
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
        let member_count: Option<i64> = sqlx::query_scalar(
            "SELECT live_count
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_id = ?2 AND level = 0 AND buck_id = 0",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .fetch_optional(&self.read_pool)
        .await?;
        Ok(member_count
            .map(|member_count| u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let row = sqlx::query(
            "SELECT payload_json
                 FROM big_sync_objs
                 WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&self.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Option<String> = row.try_get("payload_json")?;
        payload
            .map(|payload| serde_json::from_str(&payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn upsert_obj(
        &self,
        obj_id: ObjId,
        payload: ObjPayload,
        parts: Vec<PartId>,
        lease: Option<ObjStoreLease>,
    ) -> Res<StoreMutationOutcome> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(lease) = lease {
            if self.current_obj_lease(&mut tx, obj_id).await? != Some(lease) {
                return Ok(StoreMutationOutcome::Stale);
            }
        }
        let mut parts = parts;
        parts.sort();
        parts.dedup();
        let mut part_states = Vec::with_capacity(parts.len());
        for part_id in &parts {
            part_states.push((
                *part_id,
                self.load_member_state(&mut tx, *part_id, obj_id).await?,
            ));
        }
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
        self.clear_obj_lease(&mut tx, obj_id).await?;

        let mut events = Vec::new();
        for (part_id, old_state) in part_states {
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
            self.apply_bucket_transition(
                &mut tx,
                part_id,
                obj_id,
                cursor,
                &old_state,
                &MemberState::Live(payload.clone()),
            )
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
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL
             ORDER BY part_id ASC",
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

    async fn get_obj_lease(&self, obj_id: ObjId) -> Res<ObjStoreLease> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let lease = Self::next_id(&mut tx, "lease_counter").await?;
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(scope_id, obj_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .execute(&mut *tx)
        .await?;
        self.set_obj_lease(&mut tx, obj_id, lease).await?;
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
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let part_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(req.part_id))
        .fetch_optional(&self.read_pool)
        .await?;
        let Some(_) = part_exists else {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        };

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT buck_id, level, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.push_bind(self.scope_id);
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(" AND level = ");
        query.push_bind(i64::from(req.offset.level()));
        query.push(" AND buck_id >= ");
        query.push_bind(Self::buck_i64(req.offset));
        query.push(" AND changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" ORDER BY buck_id ASC LIMIT ");
        query.push_bind(i64::from(req.limit_hint) + i64::from(BuckId::ARITY));
        let rows = query.build().fetch_all(&self.read_pool).await?;

        if rows.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        let mut out = Vec::new();
        let mut last_parent = None;
        for row in rows {
            let bucket = BucketSummary {
                id: Self::buck_id(row.try_get::<i64, _>("buck_id")?),
                len: u32::try_from(
                    u64::try_from(row.try_get::<i64, _>("live_count")?).expect(ERROR_IMPOSSIBLE)
                        + u64::try_from(row.try_get::<i64, _>("dead_count")?)
                            .expect(ERROR_IMPOSSIBLE),
                )
                .expect(ERROR_IMPOSSIBLE),
                live_count: u32::try_from(row.try_get::<i64, _>("live_count")?)
                    .expect(ERROR_IMPOSSIBLE),
                fp: (
                    Self::u64_from_db(row.try_get::<i64, _>("live_fp")?),
                    Self::u64_from_db(row.try_get::<i64, _>("dead_fp")?),
                ),
                changed_at: u64::try_from(row.try_get::<i64, _>("changed_at")?)
                    .expect(ERROR_IMPOSSIBLE),
            };
            if out.len() < usize::try_from(req.limit_hint).expect(ERROR_IMPOSSIBLE) {
                out.push(bucket);
                continue;
            }
            let parent = bucket.id.parent();
            if last_parent.is_none() {
                last_parent = Some(out.last().expect(ERROR_IMPOSSIBLE).id.parent());
            }
            if Some(parent) != last_parent {
                break;
            }
            out.push(bucket);
        }
        Ok(Ok(out))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        let part_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(req.part_id))
        .fetch_optional(&self.read_pool)
        .await?;
        if part_exists.is_none() {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }

        if req.buckets.is_empty() {
            return Ok(Ok(LeafBucketResult {
                seed: req.seed,
                bucks: HashMap::new(),
            }));
        }

        struct LeafBucketPageBuilder {
            buck_id: BuckId,
            entries: Vec<BucketObjPageEntry>,
            total_count: u32,
        }

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "WITH requested(req_ord, buck_id, lower_id, upper_id, after_id) AS (",
        );
        for (req_ord, buck_req) in req.buckets.iter().enumerate() {
            let (lower_id, upper_id) = super::obj_id_bounds_for_bucket(buck_req.buck_id);
            if req_ord > 0 {
                query.push(" UNION ALL ");
            }
            query.push("SELECT ");
            query.push_bind(i64::try_from(req_ord).expect(ERROR_IMPOSSIBLE));
            query.push(" AS req_ord, ");
            query.push_bind(Self::buck_i64(buck_req.buck_id));
            query.push(" AS buck_id, ");
            query.push_bind(Self::obj_blob(lower_id));
            query.push(" AS lower_id, ");
            if let Some(upper_id) = upper_id {
                query.push_bind(Self::obj_blob(upper_id));
            } else {
                query.push("NULL");
            }
            query.push(" AS upper_id, ");
            if let Some(after) = buck_req.after {
                query.push_bind(Self::obj_blob(after));
            } else {
                query.push("NULL");
            }
            query.push(" AS after_id");
        }
        query.push(
            "), ranked AS (
                SELECT
                    r.req_ord,
                    r.buck_id,
                    m.obj_id,
                    m.removed_at,
                    o.payload_json,
                    COUNT(*) OVER (PARTITION BY r.req_ord) AS total_count,
                    ROW_NUMBER() OVER (PARTITION BY r.req_ord ORDER BY m.obj_id ASC) AS row_num
                FROM requested r
                JOIN big_sync_members m
                  ON m.scope_id = ",
        );
        query.push_bind(self.scope_id);
        query.push(" AND m.part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(" AND m.obj_id >= r.lower_id");
        query.push(" AND (r.upper_id IS NULL OR m.obj_id < r.upper_id)");
        query.push(" AND (r.after_id IS NULL OR m.obj_id > r.after_id)");
        query.push(
            "
                LEFT JOIN big_sync_objs o
                  ON o.scope_id = m.scope_id AND o.obj_id = m.obj_id
            )
            SELECT req_ord, buck_id, obj_id, removed_at, payload_json, total_count
            FROM ranked
            WHERE row_num <= ",
        );
        query.push_bind(i64::from(req.limit_hint.max(1)));
        query.push(" ORDER BY req_ord, obj_id ASC");

        let rows = query.build().fetch_all(&self.read_pool).await?;
        let mut pages: Vec<_> = req
            .buckets
            .iter()
            .map(|buck_req| LeafBucketPageBuilder {
                buck_id: buck_req.buck_id,
                entries: Vec::new(),
                total_count: 0,
            })
            .collect();
        for row in rows {
            let req_ord =
                usize::try_from(row.try_get::<i64, _>("req_ord")?).expect(ERROR_IMPOSSIBLE);
            let page = pages.get_mut(req_ord).expect(ERROR_IMPOSSIBLE);
            page.total_count =
                u32::try_from(row.try_get::<i64, _>("total_count")?).expect(ERROR_IMPOSSIBLE);
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let dead = row.try_get::<Option<i64>, _>("removed_at")?.is_some();
            let fp = if dead {
                Fingerprint::new(
                    &req.seed,
                    &("big-sync-obj-fp-v1", obj_id, serde_json::Value::Null),
                )
            } else {
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = serde_json::from_str(&payload_json.expect(ERROR_IMPOSSIBLE))
                    .wrap_err(ERROR_JSON)?;
                Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
            };
            page.entries.push(BucketObjPageEntry { obj_id, dead, fp });
        }
        let mut bucks = HashMap::with_capacity(pages.len());
        for page in pages {
            let done =
                u32::try_from(page.entries.len()).expect(ERROR_IMPOSSIBLE) == page.total_count;
            let next_after = if done || page.entries.is_empty() {
                None
            } else {
                Some(page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id)
            };
            bucks.insert(
                page.buck_id,
                LeafBucketPage {
                    entries: page.entries,
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
        lease: Option<ObjStoreLease>,
    ) -> Res<StoreMutationOutcome> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(lease) = lease {
            if self.current_obj_lease(&mut tx, obj_id).await? != Some(lease) {
                return Ok(StoreMutationOutcome::Stale);
            }
        }
        let mut parts = parts;
        parts.sort();
        parts.dedup();
        let mut part_states = Vec::with_capacity(parts.len());
        for part_id in &parts {
            part_states.push((
                *part_id,
                self.load_member_state(&mut tx, *part_id, obj_id).await?,
            ));
        }
        let payload_json: Option<String> = sqlx::query_scalar(
            "SELECT payload_json
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?;
        let payload: ObjPayload = serde_json::from_str(
            &payload_json.expect("add_obj_to_parts requires an existing payload"),
        )
        .wrap_err(ERROR_JSON)?;
        self.clear_obj_lease(&mut tx, obj_id).await?;
        let mut events = Vec::new();
        for (part_id, old_state) in part_states {
            sqlx::query(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .execute(&mut *tx)
            .await?;
            if matches!(old_state, MemberState::Live(_)) {
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
            self.apply_bucket_transition(
                &mut tx,
                part_id,
                obj_id,
                cursor,
                &old_state,
                &MemberState::Live(payload.clone()),
            )
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
        lease: Option<ObjStoreLease>,
    ) -> Res<StoreMutationOutcome> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let obj_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(_) = obj_exists else {
            tx.commit().await?;
            return Ok(StoreMutationOutcome::Applied);
        };
        if let Some(lease) = lease {
            if self.current_obj_lease(&mut tx, obj_id).await? != Some(lease) {
                return Ok(StoreMutationOutcome::Stale);
            }
        }
        self.clear_obj_lease(&mut tx, obj_id).await?;
        sqlx::query(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&mut *tx)
        .await?;
        let current_state = self.load_member_state(&mut tx, part_id, obj_id).await?;
        let MemberState::Live(old_payload) = current_state else {
            tx.commit().await?;
            return Ok(StoreMutationOutcome::Applied);
        };

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
        self.apply_bucket_transition(
            &mut tx,
            part_id,
            obj_id,
            cursor,
            &MemberState::Live(old_payload),
            &MemberState::Dead,
        )
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

    async fn test_pools() -> Res<(sqlx::SqlitePool, sqlx::SqlitePool)> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let db_url = format!("sqlite://{}", db_path.display());
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .create_if_missing(true);
        let read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await?;
        let write_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok((read_pool, write_pool))
    }

    async fn test_store(scope_key: &str) -> Res<SqlitePartStore> {
        let (read_pool, write_pool) = test_pools().await?;
        SqlitePartStore::new(read_pool, write_pool, scope_key, BuckId::MAX_LEVEL).await
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
            BuckId::MAX_LEVEL,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_obj_leases_are_sparse_and_cleared_on_success() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://leases").await?;
        let obj_id = test_obj_id(11);
        let part_id = test_part_id(12);

        let lease_one = HostPartitionStore::get_obj_lease(&store, obj_id).await?;
        let lease_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_obj_leases
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(store.scope_id)
        .bind(SqlitePartStore::obj_blob(obj_id))
        .fetch_one(&store.read_pool)
        .await?;
        assert_eq!(lease_count, 1);

        let lease_two = HostPartitionStore::get_obj_lease(&store, obj_id).await?;
        assert_ne!(lease_one, lease_two);
        let stored_lease: i64 = sqlx::query_scalar(
            "SELECT lease FROM big_sync_obj_leases
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(store.scope_id)
        .bind(SqlitePartStore::obj_blob(obj_id))
        .fetch_one(&store.read_pool)
        .await?;
        assert_eq!(
            stored_lease,
            i64::try_from(lease_two).expect(ERROR_IMPOSSIBLE)
        );

        let stale = HostPartitionStore::upsert_obj(
            &store,
            obj_id,
            serde_json::json!({"lease": "stale"}),
            vec![part_id],
            Some(lease_one),
        )
        .await?;
        assert_eq!(stale, StoreMutationOutcome::Stale);
        let still_active: i64 = sqlx::query_scalar(
            "SELECT lease FROM big_sync_obj_leases
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(store.scope_id)
        .bind(SqlitePartStore::obj_blob(obj_id))
        .fetch_one(&store.read_pool)
        .await?;
        assert_eq!(
            still_active,
            i64::try_from(lease_two).expect(ERROR_IMPOSSIBLE)
        );

        let applied = HostPartitionStore::upsert_obj(
            &store,
            obj_id,
            serde_json::json!({"lease": "applied"}),
            vec![part_id],
            Some(lease_two),
        )
        .await?;
        assert_eq!(applied, StoreMutationOutcome::Applied);
        let lease_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_obj_leases
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(store.scope_id)
        .bind(SqlitePartStore::obj_blob(obj_id))
        .fetch_one(&store.read_pool)
        .await?;
        assert_eq!(lease_count, 0);
        Ok(())
    }
}
