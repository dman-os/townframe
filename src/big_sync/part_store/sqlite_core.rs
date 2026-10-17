use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED, BucketSummary};
use big_sync_core::{BuckId, ByteKey, Fingerprint, ObjKey, PartKey, PeerKey};

use crate::sqlite::queries;
use sqlite_rs::{Database, Sql, Tx};

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
    pub sql: Database,
    pub scope_id: i64,
    pub bucket_depth: u8,
    pub _scope_key: Arc<str>,
}

impl SqliteCore {
    // -----------------------------------------------------------------------
    // Construction + schema
    // -----------------------------------------------------------------------

    pub async fn new(sql: Database, scope_key: impl Into<Arc<str>>, bucket_depth: u8) -> Res<Self> {
        let scope_key = scope_key.into();
        let scope_id = Self::ensure_scope_id(&sql, &scope_key).await?;
        Ok(Self {
            sql,
            scope_id,
            bucket_depth,
            _scope_key: scope_key,
        })
    }

    pub async fn init_schema(database: &Database, bucket_depth: u8) -> Res<()> {
        database
            .apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        let tx = database.begin_write().await?;
        for key in ["global_cursor"] {
            queries::init_meta(&tx, key.to_owned()).await?;
        }
        queries::init_bucket_depth(&tx, i64::from(bucket_depth)).await?;
        let existing_bucket_depth = queries::bucket_depth(&tx).await?;
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

    pub async fn ensure_part_ref(&self, tx: &mut Tx, part_id: PartKey) -> Res<i64> {
        Ok(queries::ensure_part_ref(tx, self.scope_id, Self::part_blob(part_id)).await?)
    }

    pub async fn ensure_obj_ref(&self, tx: &mut Tx, obj_id: ObjKey) -> Res<i64> {
        let buck_index = i64::from(BuckId::deepest_from_obj_key(&obj_id).index());
        Ok(queries::ensure_obj_ref(tx, self.scope_id, Self::obj_blob(obj_id), buck_index).await?)
    }

    pub async fn find_part_ref(&self, part_id: PartKey) -> Res<Option<i64>> {
        Ok(queries::find_part_ref(&self.sql, self.scope_id, Self::part_blob(part_id)).await?)
    }

    pub async fn find_obj_ref(&self, obj_id: ObjKey) -> Res<Option<i64>> {
        Ok(queries::find_obj_ref(&self.sql, self.scope_id, Self::obj_blob(obj_id)).await?)
    }

    pub async fn next_id(tx: &mut Tx, key: &str) -> Res<u64> {
        let value = queries::next_id(tx, key.to_owned()).await?;
        Ok(u64::try_from(value).expect(ERROR_IMPOSSIBLE))
    }

    pub async fn next_cursor(tx: &mut Tx) -> Res<CursorIndex> {
        Self::next_id(tx, "global_cursor").await
    }

    // -----------------------------------------------------------------------
    // Scope helpers
    // -----------------------------------------------------------------------

    pub async fn ensure_scope_id(database: &Database, scope_key: &Arc<str>) -> Res<i64> {
        let mut tx = database.begin_write().await?;
        let scope_id = Self::ensure_scope_id_in_tx(&mut tx, scope_key).await?;
        tx.commit().await?;
        Ok(scope_id)
    }

    pub async fn ensure_scope_id_in_tx(tx: &mut Tx, scope_key: &Arc<str>) -> Res<i64> {
        if let Some(scope_id) = queries::find_scope_id(tx, scope_key.to_string()).await? {
            return Ok(scope_id);
        }
        queries::insert_scope(tx, scope_key.to_string()).await?;
        Ok(queries::find_scope_id(tx, scope_key.to_string())
            .await?
            .expect(ERROR_IMPOSSIBLE))
    }

    // -----------------------------------------------------------------------
    // Bucket helpers
    // -----------------------------------------------------------------------

    pub async fn bucket_summary_for_path(
        &self,
        part_id: PartKey,
        path: BuckId,
    ) -> Res<BucketSummary> {
        let row = queries::bucket_summary_for_path(
            &self.sql,
            self.scope_id,
            Self::part_blob(part_id),
            i64::from(path.level()),
            Self::buck_i64(path),
        )
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
        tx: &mut Tx,
        part_id: PartKey,
        obj_id: ObjKey,
    ) -> Res<MemberState> {
        let row = queries::load_member_state(
            tx,
            self.scope_id,
            Self::part_blob(part_id),
            Self::obj_blob(obj_id),
        )
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
        tx: &mut Tx,
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
        let mut query = Sql::new(
            "SELECT buck_id, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.bind(self.scope_id);
        query.push(" AND part_ref = ");
        query.bind(part_ref);
        query.push(" AND buck_id IN ");
        query.bind_list(bucket_ids.iter().map(|buck_id| Self::buck_i64(*buck_id)));
        let rows = query.query(tx).await?.rows;
        let mut current = HashMap::with_capacity(rows.len());
        for row in rows {
            let buck_id = Self::buck_id(row.get::<i64>("buck_id")?);
            current.insert(
                buck_id,
                BucketSummaryRow {
                    changed_at: u64::try_from(row.get::<i64>("changed_at")?)
                        .expect(ERROR_IMPOSSIBLE),
                    live_count: u64::try_from(row.get::<i64>("live_count")?)
                        .expect(ERROR_IMPOSSIBLE),
                    dead_count: u64::try_from(row.get::<i64>("dead_count")?)
                        .expect(ERROR_IMPOSSIBLE),
                    live_fp: Self::u64_from_db(row.get::<i64>("live_fp")?),
                    dead_fp: Self::u64_from_db(row.get::<i64>("dead_fp")?),
                },
            );
        }
        for buck_id in bucket_ids {
            let mut summary = current.remove(&buck_id).unwrap_or_default();
            summary.apply_transition(buck_id, obj_id.clone(), cursor, old, new);
            queries::upsert_bucket(
                tx,
                self.scope_id,
                part_ref,
                Self::buck_i64(buck_id),
                i64::from(buck_id.level()),
                i64::try_from(summary.changed_at).expect(ERROR_IMPOSSIBLE),
                i64::try_from(summary.live_count).expect(ERROR_IMPOSSIBLE),
                i64::try_from(summary.dead_count).expect(ERROR_IMPOSSIBLE),
                Self::db_from_u64(summary.live_fp),
                Self::db_from_u64(summary.dead_fp),
            )
            .await?;
        }
        Ok(())
    }
}
