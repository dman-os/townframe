use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{BucketSummary, BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED};
use big_sync_core::{BuckId, Byte32Id, Fingerprint, ObjId, PartId, PeerId};

use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;

// ---------------------------------------------------------------------------
// PendingSubscription — shared atomics-based state machine used by the
// subscription replay → live handoff in both memory and SQLite stores.
// ---------------------------------------------------------------------------

pub const SUB_REPLAYING_CLEAN: u8 = 0;
pub const SUB_REPLAYING_DIRTY: u8 = 1;
pub const SUB_FINALIZING: u8 = 2;
pub const SUB_REPLAY_DONE: u8 = 3;

pub struct PendingSubscription {
    pub state: std::sync::atomic::AtomicU8,
}

impl PendingSubscription {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::atomic::AtomicU8::new(SUB_REPLAYING_CLEAN),
        })
    }

    pub fn mark_dirty(&self) -> bool {
        loop {
            let state = self.state.load(std::sync::atomic::Ordering::Acquire);
            match state {
                SUB_REPLAYING_CLEAN | SUB_FINALIZING => {
                    if self
                        .state
                        .compare_exchange(
                            state,
                            SUB_REPLAYING_DIRTY,
                            std::sync::atomic::Ordering::AcqRel,
                            std::sync::atomic::Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return false;
                    }
                }
                SUB_REPLAYING_DIRTY => return false,
                SUB_REPLAY_DONE => return true,
                _ => panic!("invalid subscription state {state}"),
            }
        }
    }

    pub fn begin_finalization(&self) -> bool {
        self.state
            .compare_exchange(
                SUB_REPLAYING_CLEAN,
                SUB_FINALIZING,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn become_ready(&self) -> bool {
        self.state
            .compare_exchange(
                SUB_FINALIZING,
                SUB_REPLAY_DONE,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }
}

// ---------------------------------------------------------------------------
// Stable Access encode / decode
//
// Use explicit integer constants instead of enum discriminants so that the
// persisted encoding is stable across enum reorderings or repr changes.
// ---------------------------------------------------------------------------

pub const ACCESS_RELAY: u8 = 0;
pub const ACCESS_READ: u8 = 1;
pub const ACCESS_EDIT: u8 = 2;
pub const ACCESS_ADMIN: u8 = 3;

pub fn encode_access(access: &keyhive_core::access::Access) -> i64 {
    i64::from(match access {
        keyhive_core::access::Access::Relay => ACCESS_RELAY,
        keyhive_core::access::Access::Read => ACCESS_READ,
        keyhive_core::access::Access::Edit => ACCESS_EDIT,
        keyhive_core::access::Access::Admin => ACCESS_ADMIN,
    })
}

pub fn decode_access(level: u8) -> keyhive_core::access::Access {
    match level {
        ACCESS_RELAY => keyhive_core::access::Access::Relay,
        ACCESS_READ => keyhive_core::access::Access::Read,
        ACCESS_EDIT => keyhive_core::access::Access::Edit,
        ACCESS_ADMIN => keyhive_core::access::Access::Admin,
        other => panic!("invalid persisted access_level {other}"),
    }
}

// ---------------------------------------------------------------------------
// BucketSummaryRow  —  in-memory accumulator for bucket fingerprint state.
// ---------------------------------------------------------------------------

#[derive(Default, Clone, Copy)]
pub struct BucketSummaryRow {
    pub changed_at: u64,
    pub live_count: u64,
    pub dead_count: u64,
    pub live_fp: u64,
    pub dead_fp: u64,
}

impl BucketSummaryRow {
    pub fn apply_transition(
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

// ---------------------------------------------------------------------------
// MemberState
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum MemberState {
    Absent,
    Live(ObjPayload),
    Dead,
}

// ---------------------------------------------------------------------------
// SqliteCore — shared SQLite database handle and helper methods used by both
// SqlitePartStore and SqliteBigRepoStore.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SqliteCore {
    pub sql: SqlCtx,
    pub scope_id: i64,
    pub bucket_depth: u8,
    pub _scope_key: Arc<str>,
}

impl SqliteCore {
    // -----------------------------------------------------------------------
    // Construction + schema
    // -----------------------------------------------------------------------

    pub async fn new(sql: SqlCtx, scope_key: impl Into<Arc<str>>, bucket_depth: u8) -> Res<Self> {
        let scope_key = scope_key.into();
        let scope_id = Self::ensure_scope_id(&sql.write_pool, &scope_key).await?;
        Ok(Self {
            sql,
            scope_id,
            bucket_depth,
            _scope_key: scope_key,
        })
    }

    pub async fn init_schema(pool: &sqlx::SqlitePool, bucket_depth: u8) -> Res<()> {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_sync_meta (
                key TEXT PRIMARY KEY NOT NULL,
                value INTEGER NOT NULL
            ) STRICT",
        )
        .execute(&mut *tx)
        .await?;
        for key in ["global_cursor"] {
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
            ) STRICT",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_sync_parts (
                scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
                part_id BLOB NOT NULL,
                latest_cursor INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(scope_id, part_id)
            ) STRICT",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_sync_objs (
                scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
                obj_id BLOB NOT NULL,
                payload_json TEXT,
                PRIMARY KEY(scope_id, obj_id),
                CHECK(payload_json IS NULL OR json_valid(payload_json))
            ) STRICT",
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
            ) STRICT",
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
            "CREATE TABLE IF NOT EXISTS big_sync_members (
                scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
                part_id BLOB NOT NULL,
                obj_id BLOB NOT NULL,
                added_at INTEGER NOT NULL,
                added_payload_json TEXT,
                changed_at INTEGER NOT NULL,
                removed_at INTEGER,
                latest_cursor INTEGER NOT NULL,
                PRIMARY KEY(scope_id, part_id, obj_id),
                FOREIGN KEY(scope_id, part_id) REFERENCES big_sync_parts(scope_id, part_id),
                FOREIGN KEY(scope_id, obj_id) REFERENCES big_sync_objs(scope_id, obj_id)
            ) STRICT",
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
            "CREATE TABLE IF NOT EXISTS big_sync_pending_members (
                scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
                part_id BLOB NOT NULL,
                obj_id BLOB NOT NULL,
                PRIMARY KEY(scope_id, part_id, obj_id),
                FOREIGN KEY(scope_id, obj_id) REFERENCES big_sync_objs(scope_id, obj_id)
            ) STRICT",
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
            ) STRICT",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_sync_syncable (
                scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
                obj_id BLOB NOT NULL,
                principal_id BLOB NOT NULL,
                access_level INTEGER NOT NULL,
                PRIMARY KEY(scope_id, obj_id, principal_id)
            ) STRICT",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ID codecs (static helpers)
    // -----------------------------------------------------------------------

    pub fn id_blob(id: Byte32Id) -> Vec<u8> {
        id.into_bytes().to_vec()
    }

    pub fn part_blob(id: PartId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    pub fn obj_blob(id: ObjId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    pub fn peer_blob(id: PeerId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    pub fn buck_i64(id: BuckId) -> i64 {
        (i64::from(id.level()) << 16) | i64::from(id.index())
    }

    pub fn buck_id(value: i64) -> BuckId {
        BuckId::new((value >> 16) as u8, value as u16)
    }

    pub fn db_from_u64(value: u64) -> i64 {
        i64::from_ne_bytes(value.to_ne_bytes())
    }

    pub fn u64_from_db(value: i64) -> u64 {
        u64::from_ne_bytes(value.to_ne_bytes())
    }

    pub fn part_from_blob(blob: Vec<u8>) -> PartId {
        PartId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    pub fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        ObjId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    pub fn peer_from_blob(blob: Vec<u8>) -> PeerId {
        PeerId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    // -----------------------------------------------------------------------
    // Sequence helpers
    // -----------------------------------------------------------------------

    pub async fn next_id(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, key: &str) -> Res<u64> {
        let value: i64 = sqlx::query_scalar(
            "UPDATE big_sync_meta SET value = value + 1 WHERE key = ?1 RETURNING value",
        )
        .bind(key)
        .fetch_one(&mut **tx)
        .await?;
        Ok(u64::try_from(value).expect(ERROR_IMPOSSIBLE))
    }

    pub async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        Self::next_id(tx, "global_cursor").await
    }

    // -----------------------------------------------------------------------
    // Scope helpers
    // -----------------------------------------------------------------------

    pub async fn ensure_scope_id(pool: &sqlx::SqlitePool, scope_key: &Arc<str>) -> Res<i64> {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        let scope_id = Self::ensure_scope_id_in_tx(&mut tx, scope_key).await?;
        tx.commit().await?;
        Ok(scope_id)
    }

    pub async fn ensure_scope_id_in_tx(
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

    // -----------------------------------------------------------------------
    // Bucket helpers
    // -----------------------------------------------------------------------

    pub async fn bucket_summary_for_path(
        &self,
        part_id: PartId,
        path: BuckId,
    ) -> Res<BucketSummary> {
        let row = sqlx::query(
            "SELECT changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_id = ?2 AND level = ?3 AND buck_id = ?4",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .bind(i64::from(path.level()))
        .bind(Self::buck_i64(path))
        .fetch_optional(&self.sql.read_pool)
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

    // -----------------------------------------------------------------------
    // Member-state query
    // -----------------------------------------------------------------------

    pub async fn load_member_state(
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
        let payload = payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        Ok(MemberState::Live(payload))
    }

    // -----------------------------------------------------------------------
    // Bucket transition — update fingerprint aggregates across all bucket
    // levels for a given (part, obj) transition.
    // -----------------------------------------------------------------------

    pub async fn apply_bucket_transition(
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

    // -----------------------------------------------------------------------
    // Rehydrate doc_members_cache from persisted syncable rows.
    // -----------------------------------------------------------------------

    pub async fn load_doc_members(
        &self,
    ) -> Res<HashMap<ObjId, HashMap<PeerId, keyhive_core::access::Access>>> {
        let mut doc_members: HashMap<ObjId, HashMap<PeerId, keyhive_core::access::Access>> =
            HashMap::new();
        let rows = sqlx::query(
            "SELECT obj_id, principal_id, access_level
             FROM big_sync_syncable
             WHERE scope_id = ?1",
        )
        .bind(self.scope_id)
        .fetch_all(&self.sql.read_pool)
        .await?;
        for row in rows {
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let principal = Self::peer_from_blob(row.try_get("principal_id")?);
            let access: u8 = row
                .try_get::<i64, _>("access_level")?
                .try_into()
                .expect(ERROR_IMPOSSIBLE);
            let access = decode_access(access);
            doc_members
                .entry(obj_id)
                .or_default()
                .insert(principal, access);
        }
        Ok(doc_members)
    }
}

// Re-export for convenience
pub use std::collections::HashMap;
