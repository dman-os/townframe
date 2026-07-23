use crate::interlude::*;
use big_sync::HostPartStore;
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest, SubscriptionTarget, BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED,
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

const SUB_REPLAYING_CLEAN: u8 = 0;
const SUB_REPLAYING_DIRTY: u8 = 1;
const SUB_FINALIZING: u8 = 2;
const SUB_REPLAY_DONE: u8 = 3;

const KEYHIVE_EVENT_LOG_MAX_ENTRIES: i64 = 200_000;
struct PendingSubscription {
    state: std::sync::atomic::AtomicU8,
}

impl PendingSubscription {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::atomic::AtomicU8::new(SUB_REPLAYING_CLEAN),
        })
    }

    fn mark_dirty(&self) -> bool {
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

    fn begin_finalization(&self) -> bool {
        self.state
            .compare_exchange(
                SUB_REPLAYING_CLEAN,
                SUB_FINALIZING,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    fn become_ready(&self) -> bool {
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

struct BigRepoSubscription {
    sender: mpsc::Sender<SubEvent>,
    principal: Option<PeerId>,
    pending: Arc<PendingSubscription>,
}

#[derive(Default)]
struct BigRepoSubscriptions {
    by_part: HashMap<PartId, HashSet<Uuid>>,
    parts_by_sub: HashMap<Uuid, HashSet<PartId>>,
    by_obj: HashMap<ObjId, HashSet<Uuid>>,
    obj_by_sub: HashMap<Uuid, ObjId>,
    pending: HashSet<Uuid>,
    live: HashSet<Uuid>,
    subs: HashMap<Uuid, Arc<BigRepoSubscription>>,
}

impl BigRepoSubscriptions {
    fn remove(&mut self, sub_id: Uuid) {
        self.pending.remove(&sub_id);
        self.live.remove(&sub_id);
        self.subs.remove(&sub_id);
        if let Some(parts) = self.parts_by_sub.remove(&sub_id) {
            for part_id in parts {
                if let Some(subs) = self.by_part.get_mut(&part_id) {
                    subs.remove(&sub_id);
                }
            }
        }
        if let Some(obj_id) = self.obj_by_sub.remove(&sub_id) {
            if let Some(subs) = self.by_obj.get_mut(&obj_id) {
                subs.remove(&sub_id);
            }
        }
    }
}

#[derive(Clone)]
pub struct SqliteBigRepoStore {
    sql: SqlCtx,
    scope_id: i64,
    bucket_depth: u8,
    _scope_key: Arc<str>,
    bus: Arc<std::sync::RwLock<BigRepoSubscriptions>>,
    hidden_parts: Arc<HashSet<PartId>>,
    /// In-memory doc-members cache, written alongside the SQL table.
    doc_members_cache:
        Arc<std::sync::RwLock<HashMap<ObjId, HashMap<PeerId, keyhive_core::access::Access>>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupPartReconciliation {
    pub(crate) doc: ObjId,
    pub(crate) agents: HashMap<PeerId, keyhive_core::access::Access>,
    pub(crate) managed_group_parts: HashSet<PartId>,
    pub(crate) desired_group_parts: HashSet<PartId>,
    pub(crate) desired_global: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct KeyhiveEventRow {
    pub(crate) seq: u64,
    pub(crate) bytes: Vec<u8>,
}

impl std::fmt::Debug for SqliteBigRepoStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteBigRepoStore")
            .finish_non_exhaustive()
    }
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
        Self::new_with_config(sql, scope_key, bucket_depth, Default::default()).await
    }

    pub async fn new_with_config(
        sql: SqlCtx,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
        config: big_sync::HostPartStoreConfig,
    ) -> Res<Self> {
        init_schema(&sql.write_pool, bucket_depth).await?;
        let scope_key = scope_key.into();
        let scope_id = Self::ensure_scope_id(&sql.write_pool, &scope_key).await?;
        // Rehydrate doc_members_cache from persisted syncable rows.
        let mut doc_members: HashMap<ObjId, HashMap<PeerId, keyhive_core::access::Access>> =
            HashMap::new();
        let rows = sqlx::query(
            "SELECT obj_id, principal_id, access_level
             FROM big_sync_syncable
             WHERE scope_id = ?1",
        )
        .bind(scope_id)
        .fetch_all(&sql.read_pool)
        .await?;
        for row in rows {
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let principal = Self::peer_from_blob(row.try_get("principal_id")?);
            let access: u8 = row
                .try_get::<i64, _>("access_level")?
                .try_into()
                .expect(ERROR_IMPOSSIBLE);
            let access = match access {
                0 => keyhive_core::access::Access::Relay,
                1 => keyhive_core::access::Access::Read,
                2 => keyhive_core::access::Access::Edit,
                3 => keyhive_core::access::Access::Admin,
                other => panic!("invalid persisted access_level {other}"),
            };
            doc_members
                .entry(obj_id)
                .or_default()
                .insert(principal, access);
        }

        let store = Self {
            sql,
            scope_id,
            bucket_depth,
            _scope_key: scope_key,
            bus: default(),
            hidden_parts: Arc::new(config.hidden_parts),
            doc_members_cache: Arc::new(std::sync::RwLock::new(doc_members)),
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
        let cache = self.doc_members_cache.read().expect(ERROR_MUTEX).clone();
        let mut promote = Vec::new();
        let mut drop_subs = HashSet::new();
        {
            let bus = self.bus.read().expect(ERROR_MUTEX);
            for event in events {
                let (obj_id, object_event) = match &event {
                    SubEvent::Changed(inner) => (
                        inner.obj_id,
                        Some(SubEvent::ObjectChanged(
                            big_sync_core::rpc::ObjChangedWithoutPart {
                                obj_id: inner.obj_id,
                                payload: inner.payload.clone(),
                            },
                        )),
                    ),
                    SubEvent::Added(inner) => (
                        inner.obj_id,
                        inner.payload.clone().map(|payload| {
                            SubEvent::ObjectChanged(big_sync_core::rpc::ObjChangedWithoutPart {
                                obj_id: inner.obj_id,
                                payload,
                            })
                        }),
                    ),
                    SubEvent::Removed(inner) => (inner.obj_id, None),
                    SubEvent::ObjectChanged(inner) => (inner.obj_id, Some(event.clone())),
                    SubEvent::ReplayComplete => continue,
                };
                let mut recipients = HashMap::new();
                match &event {
                    SubEvent::Changed(inner) => {
                        for part_id in &inner.part_ids {
                            if let Some(subs) = bus.by_part.get(part_id) {
                                for &sub_id in subs {
                                    let mut projected = event.clone();
                                    if let SubEvent::Changed(inner) = &mut projected {
                                        inner.part_ids = vec![*part_id];
                                    }
                                    recipients.insert(sub_id, projected);
                                }
                            }
                        }
                    }
                    SubEvent::Added(inner) => {
                        if let Some(subs) = bus.by_part.get(&inner.part_id) {
                            for &sub_id in subs {
                                recipients.insert(sub_id, event.clone());
                            }
                        }
                    }
                    SubEvent::Removed(inner) => {
                        if let Some(subs) = bus.by_part.get(&inner.part_id) {
                            for &sub_id in subs {
                                recipients.insert(sub_id, event.clone());
                            }
                        }
                    }
                    SubEvent::ObjectChanged(_) => {}
                    SubEvent::ReplayComplete => unreachable!(),
                }
                if let Some(object_event) = object_event {
                    if let Some(subs) = bus.by_obj.get(&obj_id) {
                        for &sub_id in subs {
                            recipients.insert(sub_id, object_event.clone());
                        }
                    }
                }
                for (sub_id, event) in recipients {
                    let Some(sub) = bus.subs.get(&sub_id) else {
                        continue;
                    };
                    if bus.pending.contains(&sub_id) {
                        if sub.pending.mark_dirty() {
                            promote.push((sub_id, event.clone(), obj_id));
                        }
                        continue;
                    }
                    if !bus.live.contains(&sub_id) {
                        continue;
                    }
                    let permitted = match sub.principal {
                        None => true,
                        Some(principal) => cache
                            .get(&obj_id)
                            .map(|members| {
                                members
                                    .get(&principal)
                                    .map(|access| access.is_reader())
                                    .unwrap_or(false)
                            })
                            .unwrap_or(true),
                    };
                    if permitted && sub.sender.try_send(event.clone()).is_err() {
                        drop_subs.insert(sub_id);
                    }
                }
            }
        }

        for (sub_id, event, obj_id) in promote {
            let mut bus = self.bus.write().expect(ERROR_MUTEX);
            let Some(sub) = bus.subs.get(&sub_id).cloned() else {
                continue;
            };
            if bus.pending.remove(&sub_id) {
                if sub.pending.state.load(std::sync::atomic::Ordering::Acquire) != SUB_REPLAY_DONE {
                    bus.pending.insert(sub_id);
                    continue;
                }
                bus.live.insert(sub_id);
            }
            let permitted = match sub.principal {
                None => true,
                Some(principal) => cache
                    .get(&obj_id)
                    .map(|members| {
                        members
                            .get(&principal)
                            .map(|access| access.is_reader())
                            .unwrap_or(false)
                    })
                    .unwrap_or(true),
            };
            if permitted && sub.sender.try_send(event).is_err() {
                bus.remove(sub_id);
            }
        }

        if !drop_subs.is_empty() {
            let mut bus = self.bus.write().expect(ERROR_MUTEX);
            for sub_id in drop_subs {
                bus.remove(sub_id);
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
        let mut hidden: Vec<_> = parts.intersection(&self.hidden_parts).copied().collect();
        if !hidden.is_empty() {
            hidden.sort_unstable();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: hidden,
            }));
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
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
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
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(LeafBucketsError::UnkownPart));
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
             ON CONFLICT(scope_id, peer_id, part_id) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)",
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
        self.list_events_with_policy(parts, cursor, limit, true)
            .await
    }

    async fn list_events_with_policy(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
        enforce_policy: bool,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        if enforce_policy {
            let summaries = self.summarize_parts(parts.clone()).await?;
            if let Err(err) = summaries {
                return Ok(Err(err));
            }
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
            let limit_usize = usize::try_from(limit).expect(ERROR_IMPOSSIBLE);
            if limit_usize != 0 && events.len() > limit_usize {
                let next = events[limit_usize - 1].0;
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
        self.subscribe_with_policy(reqs, Some(subscriber)).await
    }

    async fn subscribe_local(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        self.subscribe_with_policy(reqs, None).await
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
    async fn subscribe_with_policy(
        &self,
        reqs: SubPartsRequest,
        subscriber: Option<PeerId>,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        let target = reqs.target;
        let part_cursor = match target {
            SubscriptionTarget::Part { part_id, cursor } => {
                if subscriber.is_some() {
                    let summaries = self.summarize_parts(HashSet::from([part_id])).await?;
                    if let Err(err) = summaries {
                        return Ok(Err(err));
                    }
                }
                Some((part_id, cursor))
            }
            SubscriptionTarget::Object { .. } => None,
        };
        let (tx, rx) = mpsc::unbounded("SqliteBigRepoStore".into(), "caller".into());
        let sub_id = uuid::Uuid::new_v4();
        let sub = Arc::new(BigRepoSubscription {
            sender: tx.clone(),
            principal: subscriber,
            pending: PendingSubscription::new(),
        });
        {
            let mut bus = self.bus.write().expect(ERROR_IMPOSSIBLE);
            bus.pending.insert(sub_id);
            bus.subs.insert(sub_id, Arc::clone(&sub));
            match target {
                SubscriptionTarget::Part { part_id, .. } => {
                    bus.parts_by_sub.insert(sub_id, HashSet::from([part_id]));
                    bus.by_part.entry(part_id).or_default().insert(sub_id);
                }
                SubscriptionTarget::Object { obj_id } => {
                    bus.obj_by_sub.insert(sub_id, obj_id);
                    bus.by_obj.entry(obj_id).or_default().insert(sub_id);
                }
            }
        }

        let store = self.clone();
        tokio::spawn(async move {
            let mut cursor = part_cursor.map(|(_, cursor)| cursor).unwrap_or_default();
            let mut marker_sent = false;
            loop {
                sub.pending
                    .state
                    .store(SUB_REPLAYING_CLEAN, std::sync::atomic::Ordering::Release);
                let mut output = Vec::new();
                let mut raw_event_count = 0;
                let mut max_cursor = cursor;
                if let Some((part_id, requested_cursor)) = part_cursor {
                    let page = store
                        .list_events_with_policy(
                            HashSet::from([part_id]),
                            cursor,
                            u32::MAX,
                            subscriber.is_some(),
                        )
                        .await
                        .expect(ERROR_IMPOSSIBLE)
                        .expect(ERROR_IMPOSSIBLE);
                    let cache = store
                        .doc_members_cache
                        .read()
                        .expect(ERROR_IMPOSSIBLE)
                        .clone();
                    for (_, part_page) in page {
                        for event in part_page.events {
                            raw_event_count += 1;
                            let event_cursor = match &event {
                                PartEvent::Changed(inner) => inner.cursor,
                                PartEvent::Added(inner) => inner.cursor,
                                PartEvent::Removed(inner) => inner.cursor,
                            };
                            max_cursor = max_cursor.max(event_cursor);
                            let obj_id = match &event {
                                PartEvent::Changed(inner) => inner.obj_id,
                                PartEvent::Added(inner) => inner.obj_id,
                                PartEvent::Removed(inner) => inner.obj_id,
                            };
                            let permitted = match subscriber {
                                None => true,
                                Some(principal) => cache
                                    .get(&obj_id)
                                    .map(|members| {
                                        members
                                            .get(&principal)
                                            .map(|access| access.is_reader())
                                            .unwrap_or(false)
                                    })
                                    .unwrap_or(true),
                            };
                            if !permitted || event_cursor <= requested_cursor {
                                continue;
                            }
                            output.push(match event {
                                PartEvent::Changed(inner) => {
                                    let mut projected = inner;
                                    projected.part_ids = vec![part_id];
                                    SubEvent::Changed(projected)
                                }
                                PartEvent::Added(inner) => SubEvent::Added(inner),
                                PartEvent::Removed(inner) => SubEvent::Removed(inner),
                            });
                        }
                    }
                } else if !marker_sent {
                    let SubscriptionTarget::Object { obj_id } = target else {
                        unreachable!("missing subscription target");
                    };
                    let cache = store
                        .doc_members_cache
                        .read()
                        .expect(ERROR_IMPOSSIBLE)
                        .clone();
                    let permitted = match subscriber {
                        None => true,
                        Some(principal) => cache
                            .get(&obj_id)
                            .map(|members| {
                                members
                                    .get(&principal)
                                    .map(|access| access.is_reader())
                                    .unwrap_or(false)
                            })
                            .unwrap_or(true),
                    };
                    if permitted {
                        if let Some(payload) =
                            store.obj_payload(obj_id).await.expect(ERROR_IMPOSSIBLE)
                        {
                            output.push(SubEvent::ObjectChanged(
                                big_sync_core::rpc::ObjChangedWithoutPart { obj_id, payload },
                            ));
                        }
                    }
                }
                for event in output {
                    if tx.send(event).await.is_err() {
                        store.bus.write().expect(ERROR_IMPOSSIBLE).remove(sub_id);
                        return;
                    }
                }
                cursor = max_cursor;
                if raw_event_count != 0 {
                    continue;
                }
                if !marker_sent {
                    if !sub.pending.begin_finalization() {
                        continue;
                    }
                    if tx.send(SubEvent::ReplayComplete).await.is_err() {
                        store.bus.write().expect(ERROR_IMPOSSIBLE).remove(sub_id);
                        return;
                    }
                    marker_sent = true;
                    if sub.pending.become_ready() {
                        return;
                    }
                } else if sub.pending.become_ready() {
                    return;
                }
            }
        });
        Ok(Ok(rx))
    }

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
            "CREATE TABLE IF NOT EXISTS big_repo_keyhive_event_log (
                scope_id INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                event_hash BLOB NOT NULL,
                event_bytes BLOB NOT NULL,
                event_kind INTEGER,
                source_id BLOB,
                PRIMARY KEY(scope_id, seq),
                UNIQUE(scope_id, event_hash)
            ) STRICT",
            "CREATE TABLE IF NOT EXISTS big_repo_keyhive_replay_tail (
                scope_id INTEGER NOT NULL,
                event_hash BLOB NOT NULL,
                PRIMARY KEY(scope_id, event_hash),
                FOREIGN KEY(scope_id, event_hash)
                    REFERENCES big_repo_keyhive_event_log(scope_id, event_hash)
            ) STRICT",
            "CREATE TABLE IF NOT EXISTS big_repo_group_part_cursor (
                scope_id INTEGER PRIMARY KEY,
                cursor INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
            ) STRICT",
            "CREATE INDEX IF NOT EXISTS big_repo_keyhive_event_log_hash_idx
             ON big_repo_keyhive_event_log(scope_id, event_hash)",
            "CREATE INDEX IF NOT EXISTS big_repo_keyhive_replay_tail_seq_idx
             ON big_repo_keyhive_replay_tail(scope_id, event_hash)",
        ] {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        sqlx::query(
            "INSERT OR IGNORE INTO big_repo_group_part_cursor(scope_id, cursor)
             VALUES (?1, 0)",
        )
        .bind(self.scope_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
    /// Reconcile one bounded document batch transactionally.
    ///
    /// Non-final worker batches leave the durable event cursor untouched so a
    /// crash replays all derived updates safely before the cursor advances.
    pub(crate) async fn reconcile_group_part_batch(
        &self,
        mutations: &[GroupPartReconciliation],
        event_cursor: u64,
        advance_cursor: bool,
    ) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut transitions = Vec::new();
        let mut transition_event_payloads = HashMap::new();
        let mut reconciled_docs = HashMap::new();

        for mutation in mutations {
            let doc_blob = Self::obj_blob(mutation.doc);
            let payload_json: Option<String> = sqlx::query_scalar(
                "SELECT payload_json FROM big_sync_objs
                 WHERE scope_id = ?1 AND obj_id = ?2",
            )
            .bind(self.scope_id)
            .bind(&doc_blob)
            .fetch_optional(&mut *tx)
            .await?;
            let payload_json = payload_json.filter(|value| !value.is_empty());
            let payload: ObjPayload = payload_json
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| serde_json::from_str(value).wrap_err(ERROR_JSON))
                .transpose()?
                .unwrap_or(serde_json::Value::Null);
            let event_payload = payload_json
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| serde_json::from_str(value).wrap_err(ERROR_JSON))
                .transpose()?;
            sqlx::query(
                "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(scope_id, obj_id) DO NOTHING",
            )
            .bind(self.scope_id)
            .bind(&doc_blob)
            .bind(payload_json.as_deref())
            .execute(&mut *tx)
            .await?;

            sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2")
                .bind(self.scope_id)
                .bind(&doc_blob)
                .execute(&mut *tx)
                .await?;
            for (principal, access) in &mutation.agents {
                sqlx::query(
                    "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .bind(self.scope_id)
                .bind(&doc_blob)
                .bind(Self::peer_blob(*principal))
                .bind(i64::from(*access as u8))
                .execute(&mut *tx)
                .await?;
            }
            reconciled_docs.insert(mutation.doc, mutation.agents.clone());

            let current_rows = sqlx::query(
                "SELECT part_id FROM big_sync_members
                 WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
            )
            .bind(self.scope_id)
            .bind(&doc_blob)
            .fetch_all(&mut *tx)
            .await?;
            let current_parts: HashSet<PartId> = current_rows
                .into_iter()
                .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut desired_parts = mutation.desired_group_parts.clone();
            if mutation.desired_global {
                desired_parts.insert(crate::GLOBAL_PART_ID);
            }
            let stale = current_parts
                .intersection(&mutation.managed_group_parts)
                .filter(|part| !desired_parts.contains(part))
                .copied()
                .chain(
                    (!mutation.desired_global && current_parts.contains(&crate::GLOBAL_PART_ID))
                        .then_some(crate::GLOBAL_PART_ID),
                )
                .collect::<HashSet<_>>();
            let additions = desired_parts.difference(&current_parts).copied();

            for part_id in stale.into_iter().chain(additions) {
                let old = self
                    .load_member_state(&mut tx, part_id, mutation.doc)
                    .await?;
                let new = if desired_parts.contains(&part_id) {
                    MemberState::Live(payload.clone())
                } else {
                    MemberState::Dead
                };
                if matches!((&old, &new), (MemberState::Live(_), MemberState::Live(_))) {
                    continue;
                }
                transition_event_payloads.insert((part_id, mutation.doc), event_payload.clone());
                transitions.push((part_id, mutation.doc, old, new));
            }
        }

        let cursor = if transitions.is_empty() {
            None
        } else {
            Some(Self::next_cursor(&mut tx).await?)
        };
        let mut events = Vec::with_capacity(transitions.len());
        if let Some(cursor) = cursor {
            for (part_id, doc, old, new) in transitions {
                sqlx::query(
                    "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                     VALUES (?1, ?2, 0)
                     ON CONFLICT(scope_id, part_id) DO NOTHING",
                )
                .bind(self.scope_id)
                .bind(Self::part_blob(part_id))
                .execute(&mut *tx)
                .await?;
                match &new {
                    MemberState::Live(_) => {
                        sqlx::query(
                            "INSERT INTO big_sync_members(
                             scope_id, part_id, obj_id, added_at, added_payload_json,
                             changed_at, removed_at, latest_cursor
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?4, NULL, ?4)
                             ON CONFLICT(scope_id, part_id, obj_id) DO UPDATE SET
                             added_at = excluded.added_at,
                             added_payload_json = excluded.added_payload_json,
                             changed_at = excluded.changed_at, removed_at = NULL,
                             latest_cursor = excluded.latest_cursor",
                        )
                        .bind(self.scope_id)
                        .bind(Self::part_blob(part_id))
                        .bind(Self::obj_blob(doc))
                        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
                        .bind(
                            transition_event_payloads
                                .get(&(part_id, doc))
                                .and_then(|payload| payload.as_ref())
                                .map(|payload| serde_json::to_string(payload).expect(ERROR_JSON)),
                        )
                        .execute(&mut *tx)
                        .await?;
                        self.apply_bucket_transition(&mut tx, part_id, doc, cursor, &old, &new)
                            .await?;
                        events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                            cursor,
                            part_id,
                            obj_id: doc,
                            payload: transition_event_payloads
                                .get(&(part_id, doc))
                                .cloned()
                                .flatten(),
                        }));
                    }
                    MemberState::Dead => {
                        sqlx::query(
                            "UPDATE big_sync_members
                             SET removed_at = ?1, changed_at = ?1, latest_cursor = ?1
                             WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
                        )
                        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
                        .bind(self.scope_id)
                        .bind(Self::part_blob(part_id))
                        .bind(Self::obj_blob(doc))
                        .execute(&mut *tx)
                        .await?;
                        self.apply_bucket_transition(&mut tx, part_id, doc, cursor, &old, &new)
                            .await?;
                        events.push(SubEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                            cursor,
                            part_id,
                            obj_id: doc,
                        }));
                    }
                    MemberState::Absent => {
                        unreachable!("reconciliation cannot target absent state")
                    }
                }
                sqlx::query(
                    "UPDATE big_sync_parts SET latest_cursor = ?1
                     WHERE scope_id = ?2 AND part_id = ?3",
                )
                .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
                .bind(self.scope_id)
                .bind(Self::part_blob(part_id))
                .execute(&mut *tx)
                .await?;
            }
        }
        if advance_cursor {
            sqlx::query("UPDATE big_repo_group_part_cursor SET cursor = ?1 WHERE scope_id = ?2")
                .bind(i64::try_from(event_cursor).expect(ERROR_IMPOSSIBLE))
                .bind(self.scope_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;

        self.doc_members_cache
            .write()
            .expect(ERROR_MUTEX)
            .extend(reconciled_docs);
        if !events.is_empty() {
            self.publish(events).await;
        }
        Ok(())
    }

    pub(crate) async fn keyhive_group_part_cursor(&self) -> Res<u64> {
        let cursor: i64 =
            sqlx::query_scalar("SELECT cursor FROM big_repo_group_part_cursor WHERE scope_id = ?1")
                .bind(self.scope_id)
                .fetch_one(&self.sql.read_pool)
                .await?;
        Ok(Self::u64_from_db(cursor))
    }

    pub(crate) async fn keyhive_event_log_cursor(&self) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(seq) FROM big_repo_keyhive_event_log WHERE scope_id = ?1",
        )
        .bind(self.scope_id)
        .fetch_one(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn keyhive_events_after(
        &self,
        cursor: u64,
        limit: u32,
    ) -> Res<Vec<KeyhiveEventRow>> {
        let rows = sqlx::query(
            "SELECT seq, event_bytes
             FROM big_repo_keyhive_event_log
             WHERE scope_id = ?1 AND seq > ?2
             ORDER BY seq
             LIMIT ?3",
        )
        .bind(self.scope_id)
        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
        .bind(i64::from(limit))
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(KeyhiveEventRow {
                    seq: Self::u64_from_db(row.try_get("seq")?),
                    bytes: row.try_get("event_bytes")?,
                })
            })
            .collect()
    }

    pub(crate) async fn save_keyhive_event(
        &self,
        hash: subduction_keyhive::storage::StorageHash,
        data: Vec<u8>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let next_seq: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(seq), 0) + 1
             FROM big_repo_keyhive_event_log
             WHERE scope_id = ?1",
        )
        .bind(self.scope_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO big_repo_keyhive_event_log(
                scope_id, seq, event_hash, event_bytes
             )
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, event_hash) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(next_seq)
        .bind(hash.as_bytes().as_slice())
        .bind(data)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO big_repo_keyhive_replay_tail(scope_id, event_hash)
             VALUES (?1, ?2)
             ON CONFLICT(scope_id, event_hash) DO NOTHING",
        )
        .bind(self.scope_id)
        .bind(hash.as_bytes().as_slice())
        .execute(&mut *tx)
        .await?;
        // This log is a durable dirty-hint history, not an archive. Bound its
        // rows while keeping sequence numbers monotonic. A worker that starts
        // before the retained window detects the gap and reconciles current state.
        let prune_before: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(seq) - ?1
             FROM big_repo_keyhive_event_log
             WHERE scope_id = ?2",
        )
        .bind(KEYHIVE_EVENT_LOG_MAX_ENTRIES)
        .bind(self.scope_id)
        .fetch_one(&mut *tx)
        .await?;
        if let Some(prune_before) = prune_before.filter(|cursor| *cursor > 0) {
            sqlx::query(
                "DELETE FROM big_repo_keyhive_replay_tail
                 WHERE scope_id = ?1
                   AND event_hash IN (
                       SELECT event_hash
                       FROM big_repo_keyhive_event_log
                       WHERE scope_id = ?1 AND seq <= ?2
                   )",
            )
            .bind(self.scope_id)
            .bind(prune_before)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM big_repo_keyhive_event_log
                 WHERE scope_id = ?1 AND seq <= ?2",
            )
            .bind(self.scope_id)
            .bind(prune_before)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn load_keyhive_events(
        &self,
    ) -> Result<Vec<(subduction_keyhive::storage::StorageHash, Vec<u8>)>, SqliteBigRepoStoreError>
    {
        let rows = sqlx::query(
            "SELECT event_hash, event_bytes
             FROM big_repo_keyhive_event_log
             WHERE scope_id = ?1
               AND event_hash IN (
                   SELECT event_hash
                   FROM big_repo_keyhive_replay_tail
                   WHERE scope_id = ?1
               )
             ORDER BY seq",
        )
        .bind(self.scope_id)
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let hash_bytes: Vec<u8> = row.try_get("event_hash")?;
                let hash = Self::decode_id(hash_bytes)?;
                let event_bytes: Vec<u8> = row.try_get("event_bytes")?;
                Ok((
                    subduction_keyhive::storage::StorageHash::new(hash),
                    event_bytes,
                ))
            })
            .collect()
    }

    pub(crate) async fn delete_keyhive_event(
        &self,
        hash: subduction_keyhive::storage::StorageHash,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query(
            "DELETE FROM big_repo_keyhive_replay_tail
             WHERE scope_id = ?1 AND event_hash = ?2",
        )
        .bind(self.scope_id)
        .bind(hash.as_bytes().as_slice())
        .execute(&self.sql.write_pool)
        .await?;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_big_repo_local_subscription_bypasses_remote_policy_and_hidden_parts() -> Res<()>
    {
        let sql = SqlCtx::memory().await?;
        let part = PartId(Byte32Id::new([221; 32]));
        let obj = ObjId(Byte32Id::new([222; 32]));
        let store = SqliteBigRepoStore::new_with_config(
            sql,
            "big-repo-sqlite-local-subscription",
            BuckId::MAX_LEVEL,
            big_sync::HostPartStoreConfig {
                hidden_parts: HashSet::from([part]),
            },
        )
        .await?;
        HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 1})).await?;
        HostPartStore::ensure_part(&store, part).await?;
        HostPartStore::add_obj_to_parts(&store, obj, vec![part]).await?;

        let mut rx = HostPartStore::subscribe_local(
            &store,
            SubPartsRequest {
                target: SubscriptionTarget::Part {
                    part_id: part,
                    cursor: 0,
                },
            },
        )
        .await??;
        let mut saw_added = false;
        loop {
            match rx.recv().await? {
                SubEvent::Added(event) if event.obj_id == obj && event.part_id == part => {
                    saw_added = true;
                }
                SubEvent::ReplayComplete => break,
                _ => {}
            }
        }
        assert!(saw_added);

        HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 2})).await?;
        assert!(matches!(rx.recv().await?, SubEvent::Changed(event) if event.obj_id == obj));
        Ok(())
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
    #[tokio::test]
    async fn sqlite_big_repo_keyhive_events_are_ordered_and_deduplicated() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "keyhive-event-order", BuckId::MAX_LEVEL).await?;
        let first = subduction_keyhive::storage::StorageHash::new([1; 32]);
        let second = subduction_keyhive::storage::StorageHash::new([2; 32]);
        store.save_keyhive_event(first, b"first".to_vec()).await?;
        store.save_keyhive_event(second, b"second".to_vec()).await?;
        store
            .save_keyhive_event(first, b"replacement".to_vec())
            .await?;

        assert_eq!(
            store.load_keyhive_events().await?,
            vec![(first, b"first".to_vec()), (second, b"second".to_vec())]
        );
        assert_eq!(store.keyhive_event_log_cursor().await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_big_repo_keyhive_event_log_is_bounded() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "keyhive-event-retention", BuckId::MAX_LEVEL).await?;
        sqlx::query(
            "WITH RECURSIVE numbers(n) AS (
                 SELECT 1
                 UNION ALL
                 SELECT n + 1 FROM numbers WHERE n < 200000
             )
             INSERT INTO big_repo_keyhive_event_log(
                 scope_id, seq, event_hash, event_bytes
             )
             SELECT ?1, n, CAST(printf('%064x', n) AS BLOB), zeroblob(1)
             FROM numbers",
        )
        .bind(store.scope_id)
        .execute(&store.sql.write_pool)
        .await?;
        let hash = subduction_keyhive::storage::StorageHash::new([9; 32]);
        store.save_keyhive_event(hash, b"new".to_vec()).await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = ?1",
        )
        .bind(store.scope_id)
        .fetch_one(&store.sql.read_pool)
        .await?;
        assert_eq!(count, 200_000);
        assert_eq!(store.keyhive_event_log_cursor().await?, 200_001);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_big_repo_keyhive_event_tail_deletion_keeps_history() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "keyhive-event-tail", BuckId::MAX_LEVEL).await?;
        let hash = subduction_keyhive::storage::StorageHash::new([3; 32]);
        store.save_keyhive_event(hash, b"event".to_vec()).await?;
        store.delete_keyhive_event(hash).await?;

        assert!(store.load_keyhive_events().await?.is_empty());
        let immutable_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = ?1",
        )
        .bind(store.scope_id)
        .fetch_one(&store.sql.read_pool)
        .await?;
        assert_eq!(immutable_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_big_repo_keyhive_events_are_scope_isolated() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let first_store =
            SqliteBigRepoStore::new(sql.clone(), "keyhive-scope-a", BuckId::MAX_LEVEL).await?;
        let second_store =
            SqliteBigRepoStore::new(sql, "keyhive-scope-b", BuckId::MAX_LEVEL).await?;
        let hash = subduction_keyhive::storage::StorageHash::new([4; 32]);
        first_store.save_keyhive_event(hash, b"a".to_vec()).await?;
        second_store.save_keyhive_event(hash, b"b".to_vec()).await?;

        assert_eq!(
            first_store.load_keyhive_events().await?,
            vec![(hash, b"a".to_vec())]
        );
        assert_eq!(
            second_store.load_keyhive_events().await?,
            vec![(hash, b"b".to_vec())]
        );
        Ok(())
    }
    #[tokio::test]
    async fn sqlite_big_repo_keyhive_events_survive_restart() -> Res<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("events.sqlite");
        let url = format!("sqlite://{}", db_path.display());
        let first = SqlCtx::url(&url).await?;
        let store = SqliteBigRepoStore::new(first, "keyhive-restart", BuckId::MAX_LEVEL).await?;
        let hash = subduction_keyhive::storage::StorageHash::new([5; 32]);
        store
            .save_keyhive_event(hash, b"persistent".to_vec())
            .await?;
        drop(store);

        let reopened = SqlCtx::url(&url).await?;
        let store = SqliteBigRepoStore::new(reopened, "keyhive-restart", BuckId::MAX_LEVEL).await?;
        assert_eq!(
            store.load_keyhive_events().await?,
            vec![(hash, b"persistent".to_vec())]
        );
        Ok(())
    }
    #[tokio::test]
    async fn sqlite_big_repo_keyhive_duplicate_saves_are_safe_concurrently() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "keyhive-concurrent", BuckId::MAX_LEVEL).await?;
        let hash = subduction_keyhive::storage::StorageHash::new([6; 32]);
        let left = store.clone();
        let right = store.clone();
        let (left, right) = tokio::join!(
            left.save_keyhive_event(hash, b"left".to_vec()),
            right.save_keyhive_event(hash, b"right".to_vec()),
        );
        left?;
        right?;
        let events = store.load_keyhive_events().await?;
        assert_eq!(events.len(), 1);
        assert!(events[0].1 == b"left" || events[0].1 == b"right");
        Ok(())
    }
    #[tokio::test]
    async fn sqlite_big_repo_keyhive_archive_changes_do_not_prune_event_log() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "keyhive-archive", BuckId::MAX_LEVEL).await?;
        let archive_dir = tempfile::tempdir()?;
        let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::fs(
            store.clone(),
            archive_dir.path().to_path_buf(),
        )?;
        let event_hash = subduction_keyhive::storage::StorageHash::new([7; 32]);
        let archive_hash = subduction_keyhive::storage::StorageHash::new([8; 32]);
        subduction_keyhive::storage::KeyhiveStorage::<Sendable>::save_event(
            &storage,
            event_hash,
            b"event".to_vec(),
        )
        .await?;
        subduction_keyhive::storage::KeyhiveStorage::<Sendable>::save_archive(
            &storage,
            archive_hash,
            b"archive".to_vec(),
        )
        .await?;
        subduction_keyhive::storage::KeyhiveStorage::<Sendable>::delete_archive(
            &storage,
            archive_hash,
        )
        .await?;

        assert!(
            subduction_keyhive::storage::KeyhiveStorage::<Sendable>::load_archives(&storage,)
                .await?
                .is_empty()
        );
        assert_eq!(
            subduction_keyhive::storage::KeyhiveStorage::<Sendable>::load_events(&storage).await?,
            vec![(event_hash, b"event".to_vec())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_adds_managed_parts() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-add", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([1; 32]));
        let group_part = PartId(Byte32Id::new([2; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(group_part).await?;
        store.ensure_part(crate::GLOBAL_PART_ID).await?;

        let mutations = vec![GroupPartReconciliation {
            doc,
            agents: HashMap::new(),
            managed_group_parts: HashSet::from([group_part]),
            desired_group_parts: HashSet::from([group_part]),
            desired_global: true,
        }];
        store
            .reconcile_group_part_batch(&mutations, 42, true)
            .await?;

        let parts = HostPartStore::obj_parts(&store, doc).await?;
        assert!(
            parts.contains(&group_part),
            "doc should be in the managed group part"
        );
        assert!(
            parts.contains(&crate::GLOBAL_PART_ID),
            "doc should be in the global part when desired_global=true"
        );
        assert_eq!(store.keyhive_group_part_cursor().await?, 42);
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_removes_stale_managed_membership() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-stale", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([10; 32]));
        let part_a = PartId(Byte32Id::new([11; 32]));
        let part_b = PartId(Byte32Id::new([12; 32]));
        let part_c = PartId(Byte32Id::new([13; 32]));
        let peer = PeerId(Byte32Id::new([14; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part_a).await?;
        store.ensure_part(part_b).await?;
        store.ensure_part(part_c).await?;
        HostPartStore::add_obj_to_parts(&store, doc, vec![part_a, part_b, part_c]).await?;

        let parts_before = HostPartStore::obj_parts(&store, doc).await?;
        assert_eq!(parts_before.len(), 3);

        let mutations = vec![GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part_a, part_b]),
            desired_group_parts: HashSet::from([part_a]),
            desired_global: false,
        }];
        store
            .reconcile_group_part_batch(&mutations, 100, true)
            .await?;

        let parts = HostPartStore::obj_parts(&store, doc).await?;
        assert!(
            parts.contains(&part_a),
            "desired managed part should remain"
        );
        assert!(
            !parts.contains(&part_b),
            "stale managed part should be removed"
        );
        assert!(
            parts.contains(&part_c),
            "unrelated (non-managed) part should be preserved"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_cursor_advances() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-cursor", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([20; 32]));
        let part = PartId(Byte32Id::new([21; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;

        let m = |_cursor| GroupPartReconciliation {
            doc,
            agents: HashMap::new(),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        };
        store
            .reconcile_group_part_batch(&[m(0)], 200, false)
            .await?;
        assert_eq!(store.keyhive_group_part_cursor().await?, 0);

        store.reconcile_group_part_batch(&[m(0)], 300, true).await?;
        assert_eq!(store.keyhive_group_part_cursor().await?, 300);
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_rolls_back_on_cursor_update_failure() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-rollback", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([30; 32]));
        let part = PartId(Byte32Id::new([31; 32]));
        let peer = PeerId(Byte32Id::new([32; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;

        sqlx::query(
            "CREATE TRIGGER fail_cursor_update
             BEFORE UPDATE OF cursor ON big_repo_group_part_cursor
             BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END",
        )
        .execute(&store.sql.write_pool)
        .await?;

        let mutations = vec![GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        }];
        assert!(store
            .reconcile_group_part_batch(&mutations, 42, true)
            .await
            .is_err());

        assert!(
            HostPartStore::obj_parts(&store, doc).await?.is_empty(),
            "no part membership should survive a failed transaction"
        );
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            0,
            "group-part cursor should remain at the initial value after rollback"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_removes_global_when_desired_global_drops() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "reconcile-global-drop", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([40; 32]));
        let group_part = PartId(Byte32Id::new([41; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(group_part).await?;
        store.ensure_part(crate::GLOBAL_PART_ID).await?;
        // Put the doc into both the managed group part AND the global part.
        HostPartStore::add_obj_to_parts(&store, doc, vec![group_part, crate::GLOBAL_PART_ID])
            .await?;

        let parts_before = HostPartStore::obj_parts(&store, doc).await?;
        assert!(parts_before.contains(&crate::GLOBAL_PART_ID));
        assert!(parts_before.contains(&group_part));

        // Reconcile: same managed part desired, but desired_global dropped to false.
        // This exercises the code path at lines ~2012-2015 where GLOBAL_PART_ID is
        // added to stale outside of the managed_group_parts intersection.
        let mutations = vec![GroupPartReconciliation {
            doc,
            agents: HashMap::new(),
            managed_group_parts: HashSet::from([group_part]),
            desired_group_parts: HashSet::from([group_part]),
            desired_global: false,
        }];
        store
            .reconcile_group_part_batch(&mutations, 110, true)
            .await?;

        let parts = HostPartStore::obj_parts(&store, doc).await?;
        assert!(parts.contains(&group_part), "managed part should remain");
        assert!(
            !parts.contains(&crate::GLOBAL_PART_ID),
            "global part should be removed when desired_global drops to false"
        );
        assert_eq!(store.keyhive_group_part_cursor().await?, 110);
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_noop_still_advances_cursor() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-noop", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([50; 32]));
        let part = PartId(Byte32Id::new([51; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;
        // Put the doc into the part first so the reconciliation is a no-op.
        HostPartStore::add_obj_to_parts(&store, doc, vec![part]).await?;

        let m = GroupPartReconciliation {
            doc,
            agents: HashMap::new(),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        };
        store.reconcile_group_part_batch(&[m], 500, true).await?;
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            500,
            "cursor should advance even when no transitions occur"
        );

        // Verify state is unchanged: doc is still in the part, and no spurious
        // part removals occurred.
        let parts = HostPartStore::obj_parts(&store, doc).await?;
        assert!(parts.contains(&part), "doc should still be in the part");
        assert_eq!(parts.len(), 1, "no extra parts should appear");
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_empty_mutations_advances_cursor() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-empty", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([60; 32]));
        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;

        // Empty mutations slice: no documents affected by events.
        // This mirrors the case where GroupPartWorker sees PrekeysExpanded
        // or PrekeyRotated events that produce zero affected_documents.
        store.reconcile_group_part_batch(&[], 600, true).await?;

        // Cursor should advance even with zero mutations.
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            600,
            "cursor must advance when no documents are affected"
        );

        // No state should be modified: no part memberships created.
        assert!(
            HostPartStore::obj_parts(&store, doc).await?.is_empty(),
            "empty mutations must not create part memberships"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_idempotent_duplicate_delivery() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-idempotent", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([70; 32]));
        let part = PartId(Byte32Id::new([71; 32]));
        let peer = PeerId(Byte32Id::new([72; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;

        let m = GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        };

        // First delivery: reconcile once.
        store
            .reconcile_group_part_batch(&[m.clone()], 700, true)
            .await?;
        let parts_after_first = HostPartStore::obj_parts(&store, doc).await?;
        assert!(
            parts_after_first.contains(&part),
            "doc should be in the part after first reconciliation"
        );
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            700,
            "cursor should advance after first delivery"
        );

        // Second delivery: same reconciliation, same event cursor.
        // This mirrors replaying a Keyhive event whose reconciliation
        // is identical to the already-applied state.
        store.reconcile_group_part_batch(&[m], 700, true).await?;
        let parts_after_second = HostPartStore::obj_parts(&store, doc).await?;
        assert_eq!(
            parts_after_first, parts_after_second,
            "duplicate reconciliation must produce identical membership"
        );
        assert!(
            parts_after_second.contains(&part),
            "doc should remain in the part after duplicate delivery"
        );
        assert_eq!(
            parts_after_second.len(),
            1,
            "no extra parts should appear after duplicate delivery"
        );
        // Cursor must advance monotonically (higher wins).
        store
            .reconcile_group_part_batch(
                &[GroupPartReconciliation {
                    doc,
                    agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
                    managed_group_parts: HashSet::from([part]),
                    desired_group_parts: HashSet::from([part]),
                    desired_global: false,
                }],
                800,
                true,
            )
            .await?;
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            800,
            "cursor must advance monotonically across repeated reconciliations"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_cursor_survives_store_restart() -> Res<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("reconcile-restart.sqlite");
        let url = format!("sqlite://{}", db_path.display());

        let first = SqlCtx::url(&url).await?;
        let store = SqliteBigRepoStore::new(first, "reconcile-restart", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([80; 32]));
        let part = PartId(Byte32Id::new([81; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;

        let m = GroupPartReconciliation {
            doc,
            agents: HashMap::new(),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        };
        store.reconcile_group_part_batch(&[m], 900, true).await?;
        assert_eq!(store.keyhive_group_part_cursor().await?, 900);
        assert!(
            HostPartStore::obj_parts(&store, doc).await?.contains(&part),
            "doc should be in part before restart"
        );
        drop(store);

        // Reopen the same database.
        let reopened = SqlCtx::url(&url).await?;
        let store =
            SqliteBigRepoStore::new(reopened, "reconcile-restart", BuckId::MAX_LEVEL).await?;

        // Cursor must survive restart.
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            900,
            "cursor must persist across store restart"
        );
        // Part membership must survive restart.
        assert!(
            HostPartStore::obj_parts(&store, doc).await?.contains(&part),
            "part membership must persist across store restart"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_rolls_back_on_syncable_write_failure() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "reconcile-syncable-fail", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([90; 32]));
        let part = PartId(Byte32Id::new([91; 32]));
        let peer = PeerId(Byte32Id::new([92; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;
        // Put doc in part so a later removal has stale parts to process.
        HostPartStore::add_obj_to_parts(&store, doc, vec![part]).await?;
        store
            .set_doc_members(
                doc,
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await;

        // Inject failure on the syncable DELETE (which runs before member UPDATE).
        sqlx::query(
            "CREATE TRIGGER fail_syncable_delete
             BEFORE DELETE ON big_sync_syncable
             BEGIN SELECT RAISE(ABORT, 'injected syncable failure'); END",
        )
        .execute(&store.sql.write_pool)
        .await?;

        let mutations = vec![GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::new(),
            desired_global: false,
        }];
        assert!(
            store
                .reconcile_group_part_batch(&mutations, 42, true)
                .await
                .is_err(),
            "reconciliation must fail when syncable DELETE fails"
        );

        // Verify no state leaked: cursor unchanged.
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            0,
            "cursor must remain at initial value after syncable-write rollback"
        );
        // Doc should still be in the part (no removal applied).
        assert!(
            HostPartStore::obj_parts(&store, doc).await?.contains(&part),
            "part membership must survive syncable-write rollback"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_rolls_back_on_member_insert_failure() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "reconcile-member-insert-fail", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([100; 32]));
        let part = PartId(Byte32Id::new([101; 32]));
        let peer = PeerId(Byte32Id::new([102; 32]));

        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;

        // Inject failure on member INSERT (runs during Live transition).
        sqlx::query(
            "CREATE TRIGGER fail_member_insert
             BEFORE INSERT ON big_sync_members
             BEGIN SELECT RAISE(ABORT, 'injected member insert failure'); END",
        )
        .execute(&store.sql.write_pool)
        .await?;

        let mutations = vec![GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        }];
        assert!(
            store
                .reconcile_group_part_batch(&mutations, 42, true)
                .await
                .is_err(),
            "reconciliation must fail when member INSERT fails"
        );

        // Verify no state leaked: cursor unchanged.
        assert_eq!(
            store.keyhive_group_part_cursor().await?,
            0,
            "cursor must remain at initial value after member-insert rollback"
        );
        // No membership should be created (INSERT was aborted).
        assert!(
            HostPartStore::obj_parts(&store, doc).await?.is_empty(),
            "no part membership should survive member-insert rollback"
        );
        // The syncable write rolled back too — no agents persisted for this doc.
        let agents_in_syncable: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(store.scope_id)
        .bind(SqliteBigRepoStore::obj_blob(doc))
        .fetch_one(&store.sql.read_pool)
        .await?;
        assert_eq!(
            agents_in_syncable, 0,
            "syncable write must be rolled back when member INSERT fails"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_rolls_back_on_bucket_write_failure() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store =
            SqliteBigRepoStore::new(sql, "reconcile-bucket-fail", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([110; 32]));
        let part = PartId(Byte32Id::new([111; 32]));
        let peer = PeerId(Byte32Id::new([112; 32]));
        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;
        sqlx::query(
            "CREATE TRIGGER fail_bucket_insert
             BEFORE INSERT ON big_sync_buckets
             BEGIN SELECT RAISE(ABORT, 'injected bucket failure'); END",
        )
        .execute(&store.sql.write_pool)
        .await?;
        let mutation = GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        };
        assert!(store
            .reconcile_group_part_batch(&[mutation], 42, true)
            .await
            .is_err());
        assert_eq!(store.keyhive_group_part_cursor().await?, 0);
        assert!(HostPartStore::obj_parts(&store, doc).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_group_part_batch_rolls_back_on_part_cursor_write_failure() -> Res<()> {
        let sql = SqlCtx::memory().await?;
        let store = SqliteBigRepoStore::new(sql, "reconcile-part-fail", BuckId::MAX_LEVEL).await?;
        let doc = ObjId(Byte32Id::new([120; 32]));
        let part = PartId(Byte32Id::new([121; 32]));
        let peer = PeerId(Byte32Id::new([122; 32]));
        HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
        store.ensure_part(part).await?;
        sqlx::query(
            "CREATE TRIGGER fail_part_cursor_update
             BEFORE UPDATE OF latest_cursor ON big_sync_parts
             BEGIN SELECT RAISE(ABORT, 'injected part cursor failure'); END",
        )
        .execute(&store.sql.write_pool)
        .await?;
        let mutation = GroupPartReconciliation {
            doc,
            agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            managed_group_parts: HashSet::from([part]),
            desired_group_parts: HashSet::from([part]),
            desired_global: false,
        };
        assert!(store
            .reconcile_group_part_batch(&[mutation], 42, true)
            .await
            .is_err());
        assert_eq!(store.keyhive_group_part_cursor().await?, 0);
        assert!(HostPartStore::obj_parts(&store, doc).await?.is_empty());
        Ok(())
    }
}
