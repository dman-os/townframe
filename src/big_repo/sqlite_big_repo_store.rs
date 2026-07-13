use crate::interlude::*;
use big_sync::HostPartStore;
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest, BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED,
};
use big_sync_core::{mpsc, BuckId, Byte32Id, Fingerprint, ObjId, PartId, PeerId};
use future_form::{FutureForm, Sendable};
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob,
    collections::Set,
    crypto::digest::Digest,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::Sedimentree,
};
use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

#[derive(Clone)]
pub struct SqliteBigRepoStore {
    sql: SqlCtx,
    scope_id: i64,
    bucket_depth: u8,
    _scope_key: Arc<str>,
    bus: Arc<std::sync::Mutex<HashMap<PartId, Vec<(mpsc::Sender<SubEvent>, PeerId)>>>>,
    /// In-memory doc-members cache, written alongside the SQL table.
    doc_members_cache:
        Arc<std::sync::RwLock<HashMap<ObjId, HashMap<PeerId, keyhive_core::access::Access>>>>,
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

impl SqliteBigRepoStore {
    pub async fn new(sql: SqlCtx, scope_key: impl Into<Arc<str>>, bucket_depth: u8) -> Res<Self> {
        init_schema(&sql.write_pool, bucket_depth).await?;
        let scope_key = scope_key.into();
        let scope_id = Self::ensure_scope_id(&sql.write_pool, &scope_key).await?;
        let store = Self {
            sql,
            scope_id,
            bucket_depth,
            _scope_key: scope_key,
            bus: default(),
            doc_members_cache: default(),
        };
        store.init_subduction_schema().await?;
        Ok(store)
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

    #[cfg(test)]
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
        let payload = payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
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

    async fn publish(&self, events: Vec<SubEvent>) {
        // Extract references before locking bus to avoid self-borrow conflict.
        let cache = self.doc_members_cache.read().expect(ERROR_MUTEX);
        let mut bus = self.bus.lock().expect(ERROR_MUTEX);
        for event in &events {
            let (part_ids, evt_obj_id): (Vec<PartId>, Option<ObjId>) = match event {
                SubEvent::Changed(transition) => {
                    (transition.part_ids.clone(), Some(transition.obj_id))
                }
                SubEvent::Added(transition) => (vec![transition.part_id], Some(transition.obj_id)),
                SubEvent::Removed(transition) => {
                    (vec![transition.part_id], Some(transition.obj_id))
                }
                SubEvent::ReplayComplete => continue,
            };
            let evt = event.clone();
            for part_id in part_ids {
                let Some(subs) = bus.get_mut(&part_id) else {
                    continue;
                };
                subs.retain(|(sub, principal)| {
                    if let Some(obj_id) = evt_obj_id {
                        // Only filter when doc_members is explicitly configured.
                        // No entry → allow all (backward compatible).
                        if let Some(members) = cache.get(&obj_id) {
                            if !members
                                .get(principal)
                                .map(|a| a.is_reader())
                                .unwrap_or(false)
                            {
                                return true;
                            }
                        }
                    }
                    sub.try_send(evt.clone()).is_ok()
                });
            }
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
}

async fn init_schema(pool: &sqlx::SqlitePool, bucket_depth: u8) -> Res<()> {
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

#[async_trait]
impl HostPartStore for SqliteBigRepoStore {
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
        let rows = query.build().fetch_all(&self.sql.read_pool).await?;

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
        .fetch_optional(&self.sql.read_pool)
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
        .fetch_optional(&self.sql.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Option<String> = row.try_get("payload_json")?;
        payload
            .as_deref()
            .filter(|payload| !payload.is_empty())
            .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn set_obj_payload(&self, obj_id: ObjId, payload: ObjPayload) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let event = self.set_obj_payload_in_tx(&mut tx, obj_id, payload).await?;
        tx.commit().await?;
        if let Some(event) = event {
            self.publish(vec![event]).await;
        }
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query(
            "SELECT part_id FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL
             ORDER BY part_id ASC",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
            .collect())
    }

    async fn obj_exists(&self, obj_id: ObjId) -> Res<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(exists.is_some())
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
        .fetch_optional(&self.sql.read_pool)
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
        let rows = query.build().fetch_all(&self.sql.read_pool).await?;

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
        .fetch_optional(&self.sql.read_pool)
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
            let (lower_id, upper_id) = obj_id_bounds_for_bucket(buck_req.buck_id);
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
        query.push(" JOIN big_sync_buckets s ON s.scope_id = m.scope_id AND s.part_id = m.part_id AND s.buck_id = r.buck_id AND s.changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
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

        let rows = query.build().fetch_all(&self.sql.read_pool).await?;
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
                let payload = payload_json
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(&payload_json).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
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

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
        let payload: ObjPayload = payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let event_payload: Option<ObjPayload> = payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?;
        let payload_json = payload_json.filter(|payload_json| !payload_json.is_empty());
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(payload_json.as_deref())
        .execute(&mut *tx)
        .await?;
        let added_payload_json = payload_json.clone();
        let changed_parts: Vec<_> = part_states
            .into_iter()
            .filter(|(_, old_state)| !matches!(old_state, MemberState::Live(_)))
            .collect();
        if changed_parts.is_empty() {
            tx.commit().await?;
            return Ok(());
        }
        let cursor = Self::next_cursor(&mut tx).await?;
        let mut events = Vec::with_capacity(changed_parts.len());
        for (part_id, old_state) in changed_parts {
            sqlx::query(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO big_sync_members(scope_id, part_id, obj_id, added_at, added_payload_json, changed_at, removed_at, latest_cursor)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?4, NULL, ?4)
                 ON CONFLICT(scope_id, part_id, obj_id) DO UPDATE SET
                    added_at = excluded.added_at,
                    added_payload_json = excluded.added_payload_json,
                    changed_at = excluded.changed_at,
                    removed_at = NULL,
                    latest_cursor = excluded.latest_cursor",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(added_payload_json.as_deref())
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
            sqlx::query(
                "UPDATE big_sync_parts
                 SET latest_cursor = ?1
                 WHERE scope_id = ?2 AND part_id = ?3",
            )
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(self.scope_id)
            .bind(Self::part_blob(part_id))
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: event_payload.clone(),
            }));
        }
        tx.commit().await?;
        self.publish(events).await;
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            return Ok(());
        };
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
            return Ok(());
        };

        let cursor = Self::next_cursor(&mut tx).await?;
        sqlx::query(
            "UPDATE big_sync_members
             SET removed_at = ?1, changed_at = ?1, latest_cursor = ?1
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
        self.publish(vec![SubEvent::Removed(
            big_sync_core::rpc::ObjRemovedFromPart {
                cursor,
                part_id,
                obj_id,
            },
        )])
        .await;
        Ok(())
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
        .fetch_optional(&self.sql.read_pool)
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
        .execute(&self.sql.write_pool)
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
        .execute(&self.sql.write_pool)
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
                "SELECT members.obj_id, members.added_at, members.added_payload_json, members.changed_at, members.removed_at, members.latest_cursor, objs.payload_json
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
            .fetch_all(&self.sql.read_pool)
            .await?;
            let mut events = Vec::new();
            for row in rows {
                let row_cursor: i64 = row.try_get("latest_cursor")?;
                let added_at: i64 = row.try_get("added_at")?;
                let removed_at: Option<i64> = row.try_get("removed_at")?;
                let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
                let added_payload_json: Option<String> = row.try_get("added_payload_json")?;
                let added_payload = added_payload_json
                    .as_deref()
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
                    .transpose()?;
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = payload_json
                    .as_deref()
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
                    .transpose()?;
                if let Some(removed_at) = removed_at {
                    if removed_at > i64::try_from(cursor).expect(ERROR_IMPOSSIBLE) {
                        events.push((
                            removed_at,
                            PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                                cursor: u64::try_from(removed_at).expect(ERROR_IMPOSSIBLE),
                                part_id,
                                obj_id,
                            }),
                        ));
                    }
                    continue;
                }
                if added_at > i64::try_from(cursor).expect(ERROR_IMPOSSIBLE) {
                    events.push((
                        added_at,
                        PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                            cursor: u64::try_from(added_at).expect(ERROR_IMPOSSIBLE),
                            part_id,
                            obj_id,
                            payload: added_payload.clone(),
                        }),
                    ));
                }
                if row_cursor > added_at
                    && row_cursor > i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
                {
                    events.push((
                        row_cursor,
                        PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                            cursor: u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE),
                            part_ids: vec![part_id],
                            obj_id,
                            payload: payload.expect(ERROR_IMPOSSIBLE),
                        }),
                    ));
                }
            }
            events.sort_by_key(|(cursor, _)| *cursor);
            let mut next_cursor = None;
            if events.len() > usize::try_from(limit).expect(ERROR_IMPOSSIBLE) {
                let next = events[usize::try_from(limit).expect(ERROR_IMPOSSIBLE)].0;
                next_cursor = Some(u64::try_from(next).expect(ERROR_IMPOSSIBLE));
            }
            let events = events
                .into_iter()
                .take(usize::try_from(limit).expect(ERROR_IMPOSSIBLE))
                .map(|(_, event)| event)
                .collect();
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
        subscriber: PeerId,
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
                    PartEvent::Changed(transition) => SubEvent::Changed(transition),
                    PartEvent::Added(transition) => SubEvent::Added(transition),
                    PartEvent::Removed(transition) => SubEvent::Removed(transition),
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
            bus.entry(part_id)
                .or_default()
                .push((tx.clone(), subscriber));
        }
        Ok(Ok(rx))
    }

    async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        sqlx::query(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    async fn set_doc_members(
        &self,
        doc: ObjId,
        agents: HashMap<PeerId, keyhive_core::access::Access>,
    ) {
        let doc_blob = Self::obj_blob(doc);
        let mut tx = self
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2")
            .bind(self.scope_id)
            .bind(&doc_blob)
            .execute(&mut *tx)
            .await
            .unwrap();
        for (principal, access) in &agents {
            sqlx::query(
                "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(self.scope_id)
            .bind(&doc_blob)
            .bind(Self::peer_blob(*principal))
            .bind(i64::from(*access as u8))
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        self.doc_members_cache
            .write()
            .expect(ERROR_MUTEX)
            .insert(doc, agents);
    }

    async fn add_doc_member(
        &self,
        doc: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) {
        let doc_blob = Self::obj_blob(doc);
        let mut tx = self
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2")
            .bind(self.scope_id)
            .bind(&doc_blob)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(self.scope_id)
        .bind(&doc_blob)
        .bind(Self::peer_blob(member))
        .bind(i64::from(access as u8))
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        self.doc_members_cache
            .write()
            .expect(ERROR_MUTEX)
            .entry(doc)
            .or_default()
            .insert(member, access);
    }

    async fn remove_doc_member(&self, doc: ObjId, member: PeerId) {
        let doc_blob = Self::obj_blob(doc);
        let mut tx = self
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2 AND principal_id = ?3")
            .bind(self.scope_id)
            .bind(&doc_blob)
            .bind(Self::peer_blob(member))
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        self.doc_members_cache
            .write()
            .expect(ERROR_MUTEX)
            .entry(doc)
            .or_default()
            .remove(&member);
    }
}

fn obj_id_bounds_for_bucket(bucket_id: BuckId) -> (ObjId, Option<ObjId>) {
    let prefix_bits = u32::from(bucket_id.level()) * u32::from(BuckId::BITS_PER_LEVEL);
    debug_assert!(prefix_bits <= u16::BITS);
    if prefix_bits == 0 {
        return (ObjId(Byte32Id::new([0; 32])), None);
    }
    let shift = u16::BITS - prefix_bits;
    let start_prefix = u32::from(bucket_id.index()) << shift;
    let start = {
        let mut bytes = [0; 32];
        bytes[..2].copy_from_slice(&(start_prefix as u16).to_be_bytes());
        ObjId(Byte32Id::new(bytes))
    };
    if prefix_bits == u16::BITS || bucket_id.index() == u16::MAX {
        return (start, None);
    }
    let next_prefix = (u32::from(bucket_id.index()) + 1) << shift;
    if next_prefix > u32::from(u16::MAX) {
        return (start, None);
    }
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&(next_prefix as u16).to_be_bytes());
    (start, Some(ObjId(Byte32Id::new(bytes))))
}

#[derive(Debug, thiserror::Error)]
pub enum SqliteBigRepoStoreError {
    #[error("sqlite big repo store error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("invalid sqlite big repo store record")]
    InvalidRecord,
    #[error("failed decoding sqlite big repo store record: {0}")]
    Decode(#[from] sedimentree_core::codec::error::DecodeError),
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

impl SqliteBigRepoStore {
    async fn set_obj_payload_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjId,
        payload: ObjPayload,
    ) -> Res<Option<SubEvent>> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let old_payload_json: Option<String> = sqlx::query_scalar(
            "SELECT payload_json
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut **tx)
        .await?;
        let live_part_ids: Vec<PartId> = sqlx::query_scalar(
            "SELECT part_id
             FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(Self::part_from_blob)
        .collect();
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO UPDATE SET payload_json = excluded.payload_json",
        )
        .bind(self.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(&payload_json)
        .execute(&mut **tx)
        .await?;
        if live_part_ids.is_empty() {
            return Ok(None);
        }
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let cursor = Self::next_cursor(tx).await?;
        for part_id in &live_part_ids {
            sqlx::query(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
            )
            .bind(self.scope_id)
            .bind(Self::part_blob(*part_id))
            .execute(&mut **tx)
            .await?;
            sqlx::query(
                "UPDATE big_sync_members
                 SET changed_at = ?1, latest_cursor = ?1
                 WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
            )
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(self.scope_id)
            .bind(Self::part_blob(*part_id))
            .bind(Self::obj_blob(obj_id))
            .execute(&mut **tx)
            .await?;
            self.apply_bucket_transition(
                tx,
                *part_id,
                obj_id,
                cursor,
                &MemberState::Live(old_payload.clone()),
                &MemberState::Live(payload.clone()),
            )
            .await?;
            sqlx::query(
                "UPDATE big_sync_parts
                 SET latest_cursor = ?1
                 WHERE scope_id = ?2 AND part_id = ?3",
            )
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(self.scope_id)
            .bind(Self::part_blob(*part_id))
            .execute(&mut **tx)
            .await?;
        }
        Ok(Some(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: live_part_ids,
            obj_id,
            payload,
        })))
    }

    async fn init_subduction_schema(&self) -> Result<(), SqliteBigRepoStoreError> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        for statement in [
            "CREATE TABLE IF NOT EXISTS big_repo_subduction_trees (
                scope_id INTEGER NOT NULL,
                sedimentree_id BLOB NOT NULL,
                PRIMARY KEY(scope_id, sedimentree_id)
            ) STRICT",
            "CREATE TABLE IF NOT EXISTS big_repo_subduction_commits (
                scope_id INTEGER NOT NULL,
                sedimentree_id BLOB NOT NULL,
                commit_id BLOB NOT NULL,
                digest BLOB NOT NULL,
                signed BLOB NOT NULL,
                blob BLOB NOT NULL,
                PRIMARY KEY(scope_id, sedimentree_id, commit_id, digest),
                FOREIGN KEY(scope_id, sedimentree_id)
                    REFERENCES big_repo_subduction_trees(scope_id, sedimentree_id)
            ) STRICT",
            "CREATE TABLE IF NOT EXISTS big_repo_subduction_fragments (
                scope_id INTEGER NOT NULL,
                sedimentree_id BLOB NOT NULL,
                head_id BLOB NOT NULL,
                digest BLOB NOT NULL,
                signed BLOB NOT NULL,
                blob BLOB NOT NULL,
                PRIMARY KEY(scope_id, sedimentree_id, head_id, digest),
                FOREIGN KEY(scope_id, sedimentree_id)
                    REFERENCES big_repo_subduction_trees(scope_id, sedimentree_id)
            ) STRICT",
            "CREATE INDEX IF NOT EXISTS big_repo_subduction_commits_tree_idx
             ON big_repo_subduction_commits(scope_id, sedimentree_id, commit_id, digest)",
            "CREATE INDEX IF NOT EXISTS big_repo_subduction_fragments_tree_idx
             ON big_repo_subduction_fragments(scope_id, sedimentree_id, head_id, digest)",
        ] {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    fn tree_blob(id: SedimentreeId) -> Vec<u8> {
        id.as_bytes().to_vec()
    }

    fn obj_id(id: SedimentreeId) -> ObjId {
        ObjId(Byte32Id::new(*id.as_bytes()))
    }

    fn commit_blob(id: CommitId) -> Vec<u8> {
        id.as_bytes().to_vec()
    }

    fn digest_blob<T>(payload: &T) -> Vec<u8>
    where
        T: sedimentree_core::codec::schema::Schema + sedimentree_core::codec::encode::EncodeFields,
    {
        Digest::hash(payload).as_bytes().to_vec()
    }

    fn decode_id(bytes: Vec<u8>) -> Result<[u8; 32], SqliteBigRepoStoreError> {
        bytes
            .try_into()
            .map_err(|_| SqliteBigRepoStoreError::InvalidRecord)
    }

    async fn save_tree(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query(
            "INSERT OR IGNORE INTO big_repo_subduction_trees(scope_id, sedimentree_id)
             VALUES (?1, ?2)",
        )
        .bind(self.scope_id)
        .bind(Self::tree_blob(id))
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn commit_rows(
        &self,
        id: SedimentreeId,
        commit_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<LooseCommit>, Blob)>, SqliteBigRepoStoreError> {
        let query = if commit_id.is_some() {
            "SELECT signed, blob FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 AND commit_id = ?3
             ORDER BY digest"
        } else {
            "SELECT signed, blob FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest"
        };
        let mut request = sqlx::query(query)
            .bind(self.scope_id)
            .bind(Self::tree_blob(id));
        if let Some(commit_id) = commit_id {
            request = request.bind(Self::commit_blob(commit_id));
        }
        let rows = request.fetch_all(&self.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                let blob: Vec<u8> = row.try_get("blob")?;
                Ok((Signed::try_decode(&signed)?, Blob::new(blob)))
            })
            .collect()
    }

    async fn fragment_rows(
        &self,
        id: SedimentreeId,
        head_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<Fragment>, Blob)>, SqliteBigRepoStoreError> {
        let query = if head_id.is_some() {
            "SELECT signed, blob FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 AND head_id = ?3
             ORDER BY digest"
        } else {
            "SELECT signed, blob FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest"
        };
        let mut request = sqlx::query(query)
            .bind(self.scope_id)
            .bind(Self::tree_blob(id));
        if let Some(head_id) = head_id {
            request = request.bind(Self::commit_blob(head_id));
        }
        let rows = request.fetch_all(&self.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                let blob: Vec<u8> = row.try_get("blob")?;
                Ok((Signed::try_decode(&signed)?, Blob::new(blob)))
            })
            .collect()
    }

    async fn insert_commit(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let (signed, payload, blob) = verified.into_full_parts();
        sqlx::query(
            "INSERT OR IGNORE INTO big_repo_subduction_commits
             (scope_id, sedimentree_id, commit_id, digest, signed, blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(self.scope_id)
        .bind(Self::tree_blob(id))
        .bind(Self::commit_blob(payload.head()))
        .bind(Self::digest_blob(&payload))
        .bind(signed.as_bytes())
        .bind(blob.into_contents())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn insert_fragment(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let (signed, payload, blob) = verified.into_full_parts();
        sqlx::query(
            "INSERT OR IGNORE INTO big_repo_subduction_fragments
             (scope_id, sedimentree_id, head_id, digest, signed, blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(self.scope_id)
        .bind(Self::tree_blob(id))
        .bind(Self::commit_blob(payload.head()))
        .bind(Self::digest_blob(&payload))
        .bind(signed.as_bytes())
        .bind(blob.into_contents())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

impl SqliteBigRepoStore {
    async fn sedimentree_payload_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<ObjPayload, SqliteBigRepoStoreError> {
        let commit_rows = sqlx::query(
            "SELECT signed FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest",
        )
        .bind(self.scope_id)
        .bind(Self::tree_blob(id))
        .fetch_all(&mut **tx)
        .await?;
        let commits = commit_rows
            .into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                Ok(Signed::<LooseCommit>::try_decode(&signed)?.try_decode_trusted_payload()?)
            })
            .collect::<Result<Vec<_>, SqliteBigRepoStoreError>>()?;

        let fragment_rows = sqlx::query(
            "SELECT signed FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest",
        )
        .bind(self.scope_id)
        .bind(Self::tree_blob(id))
        .fetch_all(&mut **tx)
        .await?;
        let fragments = fragment_rows
            .into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                Ok(Signed::<Fragment>::try_decode(&signed)?.try_decode_trusted_payload()?)
            })
            .collect::<Result<Vec<_>, SqliteBigRepoStoreError>>()?;

        let tree = Sedimentree::new(fragments, commits);
        let heads: Arc<[automerge::ChangeHash]> = Arc::from(
            tree.heads(&CountLeadingZeroBytes)
                .into_iter()
                .map(|head| automerge::ChangeHash(*head.as_bytes()))
                .collect::<Vec<_>>(),
        );
        Ok(serde_json::json!({
            "heads": am_utils_rs::serialize_commit_heads(&heads),
        }))
    }
}

impl Storage<Sendable> for SqliteBigRepoStore {
    type Error = SqliteBigRepoStoreError;

    fn save_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.save_tree(&mut tx, id).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn delete_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            let tree = Self::tree_blob(id);
            sqlx::query(
                "DELETE FROM big_repo_subduction_commits
                 WHERE scope_id = ?1 AND sedimentree_id = ?2",
            )
            .bind(self.scope_id)
            .bind(&tree)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM big_repo_subduction_fragments
                 WHERE scope_id = ?1 AND sedimentree_id = ?2",
            )
            .bind(self.scope_id)
            .bind(&tree)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM big_repo_subduction_trees
                 WHERE scope_id = ?1 AND sedimentree_id = ?2",
            )
            .bind(self.scope_id)
            .bind(tree)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(&self) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows = sqlx::query(
                "SELECT sedimentree_id FROM big_repo_subduction_trees
                 WHERE scope_id = ?1 ORDER BY sedimentree_id",
            )
            .bind(self.scope_id)
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(SedimentreeId::new(Self::decode_id(
                        row.try_get("sedimentree_id")?,
                    )?))
                })
                .collect()
        })
    }

    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        Sendable::from_future(async move {
            let found: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM big_repo_subduction_trees
                 WHERE scope_id = ?1 AND sedimentree_id = ?2",
            )
            .bind(self.scope_id)
            .bind(Self::tree_blob(id))
            .fetch_optional(&self.sql.read_pool)
            .await?;
            Ok(found.is_some())
        })
    }

    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.save_tree(&mut tx, id).await?;
            self.insert_commit(&mut tx, id, verified).await?;
            let payload = self.sedimentree_payload_in_tx(&mut tx, id).await?;
            let event = self
                .set_obj_payload_in_tx(&mut tx, Self::obj_id(id), payload)
                .await?;
            tx.commit().await?;
            if let Some(event) = event {
                self.publish(vec![event]).await;
            }
            Ok(())
        })
    }

    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows = sqlx::query(
                "SELECT DISTINCT commit_id FROM big_repo_subduction_commits
                 WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id",
            )
            .bind(self.scope_id)
            .bind(Self::tree_blob(id))
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row.try_get("commit_id")?)?)))
                .collect()
        })
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Sendable::from_future(async move {
            self.commit_rows(id, None)
                .await?
                .into_iter()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .collect()
        })
    }

    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Sendable::from_future(async move {
            self.commit_rows(id, None)
                .await?
                .into_iter()
                .map(|(signed, _)| Ok(signed.try_decode_trusted_payload()?))
                .collect()
        })
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Sendable::from_future(async move {
            self.commit_rows(id, Some(commit_id))
                .await?
                .into_iter()
                .next()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .transpose()
        })
    }

    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("DELETE FROM big_repo_subduction_commits WHERE scope_id = ?1 AND sedimentree_id = ?2 AND commit_id = ?3")
                .bind(self.scope_id).bind(Self::tree_blob(id)).bind(Self::commit_blob(commit_id))
                .execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn delete_loose_commits(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("DELETE FROM big_repo_subduction_commits WHERE scope_id = ?1 AND sedimentree_id = ?2")
                .bind(self.scope_id).bind(Self::tree_blob(id)).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.save_tree(&mut tx, id).await?;
            self.insert_fragment(&mut tx, id, verified).await?;
            let payload = self.sedimentree_payload_in_tx(&mut tx, id).await?;
            let event = self
                .set_obj_payload_in_tx(&mut tx, Self::obj_id(id), payload)
                .await?;
            tx.commit().await?;
            if let Some(event) = event {
                self.publish(vec![event]).await;
            }
            Ok(())
        })
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        head_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Sendable::from_future(async move {
            self.fragment_rows(id, Some(head_id))
                .await?
                .into_iter()
                .next()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .transpose()
        })
    }

    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows = sqlx::query("SELECT DISTINCT head_id FROM big_repo_subduction_fragments WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id")
                .bind(self.scope_id).bind(Self::tree_blob(id)).fetch_all(&self.sql.read_pool).await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row.try_get("head_id")?)?)))
                .collect()
        })
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Sendable::from_future(async move {
            self.fragment_rows(id, None)
                .await?
                .into_iter()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .collect()
        })
    }

    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        Sendable::from_future(async move {
            self.fragment_rows(id, None)
                .await?
                .into_iter()
                .map(|(signed, _)| Ok(signed.try_decode_trusted_payload()?))
                .collect()
        })
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        head_id: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("DELETE FROM big_repo_subduction_fragments WHERE scope_id = ?1 AND sedimentree_id = ?2 AND head_id = ?3")
                .bind(self.scope_id).bind(Self::tree_blob(id)).bind(Self::commit_blob(head_id))
                .execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn delete_fragments(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("DELETE FROM big_repo_subduction_fragments WHERE scope_id = ?1 AND sedimentree_id = ?2")
                .bind(self.scope_id).bind(Self::tree_blob(id)).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> BoxFuture<'_, Result<usize, Self::Error>> {
        Sendable::from_future(async move {
            let count = commits.len() + fragments.len();
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.save_tree(&mut tx, id).await?;
            for commit in commits {
                self.insert_commit(&mut tx, id, commit).await?;
            }
            for fragment in fragments {
                self.insert_fragment(&mut tx, id, fragment).await?;
            }
            let event = if count == 0 {
                None
            } else {
                let payload = self.sedimentree_payload_in_tx(&mut tx, id).await?;
                self.set_obj_payload_in_tx(&mut tx, Self::obj_id(id), payload)
                    .await?
            };
            tx.commit().await?;
            if let Some(event) = event {
                self.publish(vec![event]).await;
            }
            Ok(count)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync::{host_part_store_contract, HostPartStoreContractHarness};
    use sedimentree_core::blob::BlobMeta;
    use subduction_crypto::signer::memory::MemorySigner;

    struct SqliteBigRepoHarness {
        store: SqliteBigRepoStore,
    }

    impl HostPartStoreContractHarness for SqliteBigRepoHarness {
        fn store(&self) -> &dyn HostPartStore {
            &self.store
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_big_repo_host_part_store_contract() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "big-repo-sqlite-host-contract", BuckId::MAX_LEVEL)
                .await?;
        host_part_store_contract::assert_host_part_store_contract(&SqliteBigRepoHarness { store })
            .await
    }

    async fn make_commit(
        signer: &MemorySigner,
        tree: SedimentreeId,
        head_byte: u8,
    ) -> VerifiedMeta<LooseCommit> {
        let blob = Blob::new(vec![head_byte; 3]);
        let mut head = [0; 32];
        head[0] = head_byte;
        let payload = LooseCommit::new(
            tree,
            CommitId::new(head),
            std::collections::BTreeSet::new(),
            BlobMeta::new(&blob),
        );
        let signed = Signed::seal::<Sendable, _>(signer, payload).await;
        VerifiedMeta::new(
            signed.into_signed().try_verify().expect("fresh signature"),
            blob,
        )
        .expect("fresh blob metadata")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_big_repo_subduction_roundtrip() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "big-repo-sqlite-subduction", BuckId::MAX_LEVEL).await?;
        let signer = MemorySigner::from_bytes(&[9; 32]);
        let tree = SedimentreeId::new([4; 32]);
        let verified = make_commit(&signer, tree, 1).await;

        Storage::<Sendable>::save_loose_commit(&store, tree, verified.clone()).await?;
        let loaded = Storage::<Sendable>::load_loose_commits(&store, tree).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].signed().as_bytes(), verified.signed().as_bytes());
        assert_eq!(loaded[0].blob().as_slice(), verified.blob().as_slice());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_big_repo_commit_updates_payload_atomically() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "big-repo-sqlite-atomic", BuckId::MAX_LEVEL).await?;
        let signer = MemorySigner::from_bytes(&[10; 32]);
        let tree = SedimentreeId::new([11; 32]);
        let obj_id = SqliteBigRepoStore::obj_id(tree);
        let part_id = PartId(Byte32Id::new([12; 32]));
        HostPartStore::set_obj_payload(&store, obj_id, serde_json::json!({"old": true})).await?;
        HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;

        let commit = make_commit(&signer, tree, 1).await;
        Storage::<Sendable>::save_loose_commit(&store, tree, commit).await?;

        let mut head = [0; 32];
        head[0] = 1;
        let heads: Arc<[automerge::ChangeHash]> = Arc::from(vec![automerge::ChangeHash(head)]);
        let expected = serde_json::json!({
            "heads": am_utils_rs::serialize_commit_heads(&heads),
        });
        assert_eq!(
            HostPartStore::obj_payload(&store, obj_id).await?,
            Some(expected)
        );
        assert_eq!(HostPartStore::member_count(&store, part_id).await?, 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_big_repo_commit_rolls_back_when_payload_update_fails() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "big-repo-sqlite-atomic-failure", BuckId::MAX_LEVEL)
                .await?;
        let signer = MemorySigner::from_bytes(&[13; 32]);
        let tree = SedimentreeId::new([14; 32]);
        let obj_id = SqliteBigRepoStore::obj_id(tree);
        let part_id = PartId(Byte32Id::new([15; 32]));
        let old_payload = serde_json::json!({"old": true});
        HostPartStore::set_obj_payload(&store, obj_id, old_payload.clone()).await?;
        HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;

        sqlx::query(
            "CREATE TRIGGER fail_big_repo_payload_update
             BEFORE UPDATE OF payload_json ON big_sync_objs
             WHEN hex(NEW.obj_id) = '0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E'
             BEGIN SELECT RAISE(ABORT, 'injected payload failure'); END",
        )
        .execute(&store.sql.write_pool)
        .await?;

        let commit = make_commit(&signer, tree, 1).await;
        assert!(Storage::<Sendable>::save_loose_commit(&store, tree, commit)
            .await
            .is_err());
        assert!(Storage::<Sendable>::load_loose_commits(&store, tree)
            .await?
            .is_empty());
        assert_eq!(
            HostPartStore::obj_payload(&store, obj_id).await?,
            Some(old_payload)
        );
        assert_eq!(HostPartStore::member_count(&store, part_id).await?, 1);
        Ok(())
    }
}
