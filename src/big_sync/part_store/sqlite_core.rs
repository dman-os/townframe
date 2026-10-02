use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED, BucketSummary};
use big_sync_core::{BuckId, ByteKey, Fingerprint, ObjKey, PartKey, PeerKey};

use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;

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
        obj_id: ObjKey,
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
                        &("big-sync-bucket-live-v1", buck_id, obj_id.clone(), payload),
                    )
                    .as_u64(),
                );
            }
            MemberState::Dead => {
                self.dead_count = self.dead_count.checked_sub(1).expect(ERROR_IMPOSSIBLE);
                self.dead_fp = self.dead_fp.wrapping_sub(
                    Fingerprint::new(
                        &BUCKET_DEAD_FP_SEED,
                        &("big-sync-bucket-dead-v1", buck_id, obj_id.clone()),
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

pub const EVENT_CHANGED: i64 = 1;
pub const EVENT_REMOVED: i64 = 2;

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

static MIGRATOR: std::sync::LazyLock<sqlx::migrate::Migrator> = std::sync::LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
});
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
        MIGRATOR.run(pool).await?;
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        for key in ["global_cursor"] {
            sqlx::query!(
                "INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES (?1, 0)",
                key
            )
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query!(
            "INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES ('bucket_depth', ?1)",
            i64::from(bucket_depth)
        )
        .execute(&mut *tx)
        .await?;
        let existing_bucket_depth: i64 = sqlx::query_scalar!(
            "SELECT value
             FROM big_sync_meta
             WHERE key = 'bucket_depth'"
        )
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(
            u8::try_from(existing_bucket_depth).expect(ERROR_IMPOSSIBLE),
            bucket_depth,
            "bucket depth is fixed for the database"
        );
        tx.commit().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ID codecs (static helpers)
    // -----------------------------------------------------------------------

    pub fn id_blob(id: ByteKey) -> Vec<u8> {
        id.into_bytes().to_vec()
    }

    pub fn part_blob(id: PartKey) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    pub fn obj_blob(id: ObjKey) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    pub fn peer_blob(id: PeerKey) -> Vec<u8> {
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

    pub fn part_from_blob(blob: Vec<u8>) -> PartKey {
        PartKey::new(blob)
    }

    pub fn obj_from_blob(blob: Vec<u8>) -> ObjKey {
        ObjKey::new(blob)
    }

    pub fn peer_from_blob(blob: Vec<u8>) -> PeerKey {
        PeerKey::new(blob)
    }

    // -----------------------------------------------------------------------
    // Sequence helpers
    // -----------------------------------------------------------------------

    pub async fn ensure_part_ref(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartKey,
    ) -> Res<i64> {
        let row = sqlx::query!(
            "INSERT INTO big_sync_parts(scope_id, part_id)
             VALUES (?1, ?2)
             ON CONFLICT(scope_id, part_id) DO UPDATE SET part_id = excluded.part_id
             RETURNING part_ref",
            self.scope_id,
            Self::part_blob(part_id)
        )
        .fetch_one(&mut **tx)
        .await?;
        Ok(row.part_ref)
    }

    pub async fn ensure_obj_ref(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjKey,
    ) -> Res<i64> {
        let buck_index = i64::from(BuckId::deepest_from_obj_key(&obj_id).index());
        let row = sqlx::query!(
            "INSERT INTO big_sync_objs(scope_id, obj_id, buck_index)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO UPDATE SET obj_id = excluded.obj_id,
                                                         buck_index = excluded.buck_index
             RETURNING obj_ref",
            self.scope_id,
            Self::obj_blob(obj_id),
            buck_index
        )
        .fetch_one(&mut **tx)
        .await?;
        Ok(row.obj_ref)
    }

    pub async fn find_part_ref(&self, part_id: PartKey) -> Res<Option<i64>> {
        Ok(sqlx::query_scalar!(
            "SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?2",
            self.scope_id,
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?)
    }

    pub async fn find_obj_ref(&self, obj_id: ObjKey) -> Res<Option<i64>> {
        Ok(sqlx::query_scalar!(
            "SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope_id,
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?)
    }

    pub async fn next_id(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, key: &str) -> Res<u64> {
        let value: i64 = sqlx::query_scalar!(
            "UPDATE big_sync_meta SET value = value + 1 WHERE key = ?1 RETURNING value",
            key
        )
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
        if let Some(scope_id) = sqlx::query_scalar!(
            "SELECT scope_id FROM big_sync_scopes WHERE scope_key = ?1",
            scope_key.as_ref()
        )
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(scope_id);
        }

        sqlx::query!(
            "INSERT INTO big_sync_scopes(scope_key) VALUES (?1)",
            scope_key.as_ref()
        )
        .execute(&mut **tx)
        .await?;
        let scope_id: i64 = sqlx::query_scalar!(
            "SELECT scope_id FROM big_sync_scopes WHERE scope_key = ?1",
            scope_key.as_ref()
        )
        .fetch_one(&mut **tx)
        .await?;
        Ok(scope_id)
    }

    // -----------------------------------------------------------------------
    // Bucket helpers
    // -----------------------------------------------------------------------

    pub async fn bucket_summary_for_path(
        &self,
        part_id: PartKey,
        path: BuckId,
    ) -> Res<BucketSummary> {
        let row = sqlx::query!(
            "SELECT changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts
                  WHERE scope_id = ?1 AND part_id = ?2
             ) AND level = ?3 AND buck_id = ?4",
            self.scope_id,
            Self::part_blob(part_id),
            i64::from(path.level()),
            Self::buck_i64(path)
        )
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
        let changed_at = row.changed_at;
        let live_count = row.live_count;
        let dead_count = row.dead_count;
        let live_fp = row.live_fp;
        let dead_fp = row.dead_fp;
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
        part_id: PartKey,
        obj_id: ObjKey,
    ) -> Res<MemberState> {
        let row = sqlx::query!(
            "SELECT members.event_type, objs.payload_json
             FROM big_sync_members members
             LEFT JOIN big_sync_objs objs ON objs.obj_ref = members.obj_ref
             WHERE members.scope_id = ?1
               AND members.maybe_part_ref = (
                   SELECT part_ref FROM big_sync_parts
                    WHERE scope_id = ?1 AND part_id = ?2
               )
               AND members.obj_ref = (
                   SELECT obj_ref FROM big_sync_objs
                    WHERE scope_id = ?1 AND obj_id = ?3
               )",
            self.scope_id,
            Self::part_blob(part_id),
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Ok(MemberState::Absent);
        };
        if row.event_type == EVENT_REMOVED {
            return Ok(MemberState::Dead);
        }
        let payload_json = row.payload_json;
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
        part_id: PartKey,
        obj_id: ObjKey,
        cursor: CursorIndex,
        old: &MemberState,
        new: &MemberState,
    ) -> Res<()> {
        let part_ref = self.ensure_part_ref(tx, part_id).await?;
        let deepest = BuckId::deepest_from_obj_key(&obj_id);
        let bucket_ids: Vec<_> = (0..=self.bucket_depth)
            .map(|level| deepest.to_level(level))
            .collect();
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT buck_id, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.push_bind(self.scope_id);
        query.push(" AND part_ref = ");
        query.push_bind(part_ref);
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
            summary.apply_transition(buck_id, obj_id.clone(), cursor, old, new);
            sqlx::query!(
                "INSERT INTO big_sync_buckets(
                    scope_id, part_ref, buck_id, level, changed_at,
                    live_count, dead_count, live_fp, dead_fp
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(scope_id, part_ref, buck_id) DO UPDATE SET
                    level = excluded.level,
                    changed_at = excluded.changed_at,
                    live_count = excluded.live_count,
                    dead_count = excluded.dead_count,
                    live_fp = excluded.live_fp,
                    dead_fp = excluded.dead_fp",
                self.scope_id,
                part_ref,
                Self::buck_i64(buck_id),
                i64::from(buck_id.level()),
                i64::try_from(summary.changed_at).expect(ERROR_IMPOSSIBLE),
                i64::try_from(summary.live_count).expect(ERROR_IMPOSSIBLE),
                i64::try_from(summary.dead_count).expect(ERROR_IMPOSSIBLE),
                Self::db_from_u64(summary.live_fp),
                Self::db_from_u64(summary.dead_fp)
            )
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}
