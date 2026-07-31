use super::HostPartStore;
use super::policy::ObjAccessPolicy;
use crate::interlude::*;
#[cfg(test)]
use crate::test_support::{ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot};

#[cfg(test)]
use big_sync_core::part_store::PartStoreReadOnly;
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest,
};
#[cfg(test)]
use big_sync_core::Byte32Id;
use big_sync_core::{mpsc, BuckId, Fingerprint, ObjId, PartId, PeerId};
#[cfg(test)]
use future_form::{FutureForm, Sendable};
#[cfg(test)]
use futures::future::BoxFuture;
use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;
#[cfg(test)]
use uuid::Uuid;

use super::sqlite_core::{
    encode_access, PendingSubscription, SUB_REPLAYING_CLEAN, SUB_REPLAY_DONE,
};

struct SqliteSubscription {
    sender: mpsc::Sender<SubEvent>,
    principal: PeerId,
    pending: Arc<PendingSubscription>,
}

#[derive(Default)]
struct SqliteSubscriptions {
    by_part: HashMap<PartId, HashSet<Uuid>>,
    parts_by_sub: HashMap<Uuid, HashSet<PartId>>,
    by_obj: HashMap<ObjId, HashSet<Uuid>>,
    objs_by_sub: HashMap<Uuid, HashSet<ObjId>>,
    pending: HashSet<Uuid>,
    live: HashSet<Uuid>,
    subs: HashMap<Uuid, Arc<SqliteSubscription>>,
}

impl SqliteSubscriptions {
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
        if let Some(obj_ids) = self.objs_by_sub.remove(&sub_id) {
            for obj_id in obj_ids {
                if let Some(subs) = self.by_obj.get_mut(&obj_id) {
                    subs.remove(&sub_id);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SqlitePartStore {
    pub(crate) core: SqliteCore,
    bus: Arc<std::sync::RwLock<SqliteSubscriptions>>,
    hidden_parts: Arc<HashSet<PartId>>,
    /// Access-control policy consulted at event-forward time.
    policy: Arc<dyn ObjAccessPolicy>,
}

use super::sqlite_core::MemberState;
use super::sqlite_core::SqliteCore;

/// Thin forwarding helpers so call sites inside SqlitePartStore's
/// HostPartStore impl continue to compile without changes.
impl SqlitePartStore {
    fn part_blob(id: PartId) -> Vec<u8> {
        SqliteCore::part_blob(id)
    }
    fn obj_blob(id: ObjId) -> Vec<u8> {
        SqliteCore::obj_blob(id)
    }
    fn peer_blob(id: PeerId) -> Vec<u8> {
        SqliteCore::peer_blob(id)
    }
    fn event_part_id(event: &SubEvent) -> Option<PartId> {
        match event {
            SubEvent::Changed(_) | SubEvent::ObjectChanged(_) => None,
            SubEvent::Added(inner) => Some(inner.part_id),
            SubEvent::Removed(inner) => Some(inner.part_id),
            SubEvent::ReplayComplete => None,
        }
    }
    fn buck_i64(id: BuckId) -> i64 {
        SqliteCore::buck_i64(id)
    }
    fn buck_id(value: i64) -> BuckId {
        SqliteCore::buck_id(value)
    }
    fn u64_from_db(value: i64) -> u64 {
        SqliteCore::u64_from_db(value)
    }
    fn part_from_blob(blob: Vec<u8>) -> PartId {
        SqliteCore::part_from_blob(blob)
    }
    fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        SqliteCore::obj_from_blob(blob)
    }
    #[cfg(test)]
    fn peer_from_blob(blob: Vec<u8>) -> PeerId {
        SqliteCore::peer_from_blob(blob)
    }
    async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        SqliteCore::next_cursor(tx).await
    }

    async fn load_member_state(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartId,
        obj_id: ObjId,
    ) -> Res<MemberState> {
        self.core.load_member_state(tx, part_id, obj_id).await
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
        self.core
            .apply_bucket_transition(tx, part_id, obj_id, cursor, old, new)
            .await
    }

    async fn bucket_summary_for_path(&self, part_id: PartId, path: BuckId) -> Res<BucketSummary> {
        self.core.bucket_summary_for_path(part_id, path).await
    }
}

impl SqlitePartStore {
    pub async fn new(
        sql: SqlCtx,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
        policy: Arc<dyn ObjAccessPolicy>,
    ) -> Res<Self> {
        Self::new_with_config(sql, scope_key, bucket_depth, Default::default(), policy).await
    }

    pub async fn new_with_config(
        sql: SqlCtx,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
        config: super::HostPartStoreConfig,
        policy: Arc<dyn ObjAccessPolicy>,
    ) -> Res<Self> {
        SqliteCore::init_schema(&sql.write_pool, bucket_depth).await?;
        let core = SqliteCore::new(sql, scope_key, bucket_depth).await?;
        // Rehydrate the policy's member map from persisted syncable rows.
        for (obj, agents) in core.load_doc_members().await? {
            policy.set_obj_members(obj, agents);
        }

        Ok(Self {
            core,
            bus: default(),
            hidden_parts: Arc::new(config.hidden_parts),
            policy,
        })
    }
    async fn publish(&self, events: Vec<SubEvent>) {
        let mut promote = Vec::new();
        let mut drop_subs = HashSet::new();
        {
            let bus = self.bus.read().expect(ERROR_MUTEX);
            for event in events {
                let (part_ids, obj_id, object_event) = match &event {
                    SubEvent::Changed(inner) => (
                        inner.part_ids.clone(),
                        inner.obj_id,
                        SubEvent::ObjectChanged(big_sync_core::rpc::ObjChangedWithoutPart {
                            obj_id: inner.obj_id,
                            payload: inner.payload.clone(),
                        }),
                    ),
                    SubEvent::Added(inner) => (
                        vec![inner.part_id],
                        inner.obj_id,
                        SubEvent::ObjectChanged(big_sync_core::rpc::ObjChangedWithoutPart {
                            obj_id: inner.obj_id,
                            payload: inner.payload.clone(),
                        }),
                    ),
                    SubEvent::Removed(inner) => {
                        (vec![inner.part_id], inner.obj_id, SubEvent::ReplayComplete)
                    }
                    SubEvent::ObjectChanged(inner) => (Vec::new(), inner.obj_id, event.clone()),
                    SubEvent::ReplayComplete => continue,
                };
                let mut recipients = HashMap::new();
                for part_id in part_ids {
                    if let Some(subs) = bus.by_part.get(&part_id) {
                        for &sub_id in subs {
                            match recipients.get_mut(&sub_id) {
                                Some(SubEvent::Changed(existing))
                                    if matches!(event, SubEvent::Changed(_)) =>
                                {
                                    existing.part_ids.push(part_id);
                                }
                                _ => {
                                    let mut projected = event.clone();
                                    if let SubEvent::Changed(inner) = &mut projected {
                                        inner.part_ids = vec![part_id];
                                    }
                                    recipients.insert(sub_id, projected);
                                }
                            }
                        }
                    }
                }
                if !matches!(object_event, SubEvent::ReplayComplete) {
                    if let Some(subs) = bus.by_obj.get(&obj_id) {
                        for &sub_id in subs {
                            recipients
                                .entry(sub_id)
                                .or_insert_with(|| object_event.clone());
                        }
                    }
                }
                for (sub_id, event) in recipients {
                    let Some(sub) = bus.subs.get(&sub_id) else {
                        continue;
                    };
                    if bus.pending.contains(&sub_id) {
                        if sub.pending.mark_dirty() {
                            promote.push((sub_id, event, obj_id));
                        }
                        continue;
                    }
                    if !bus.live.contains(&sub_id) {
                        continue;
                    }
                    let permitted = self
                        .policy
.is_event_permitted(Self::event_part_id(&event), obj_id, Some(sub.principal));
                    if permitted && sub.sender.try_send(event).is_err() {
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
            let permitted = self
                .policy
.is_event_permitted(Self::event_part_id(&event), obj_id, Some(sub.principal));
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
}

#[async_trait]
impl HostPartStore for SqlitePartStore {
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
        query.push_bind(self.core.scope_id);
        query.push(" AND p.part_id IN (");
        let mut separated = query.separated(", ");
        for part_id in &parts {
            separated.push_bind(Self::part_blob(*part_id));
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;

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
        .bind(self.core.scope_id)
        .bind(Self::part_blob(part_id))
        .fetch_optional(&self.core.sql.read_pool)
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
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&self.core.sql.read_pool)
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
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let old_payload_json: Option<String> = sqlx::query_scalar(
            "SELECT payload_json
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?;
        let live_part_ids: Vec<PartId> = sqlx::query_scalar(
            "SELECT part_id
             FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(Self::part_from_blob)
        .collect();
        let pending_part_ids: Vec<PartId> = sqlx::query_scalar(
            "SELECT part_id
             FROM big_sync_pending_members
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(Self::part_from_blob)
        .collect();
        sqlx::query(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO UPDATE SET payload_json = excluded.payload_json",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(&payload_json)
        .execute(&mut *tx)
        .await?;
        if live_part_ids.is_empty() {
            tx.commit().await?;
            if pending_part_ids.is_empty() {
                self.publish(vec![SubEvent::ObjectChanged(
                    big_sync_core::rpc::ObjChangedWithoutPart { obj_id, payload },
                )])
                .await;
            } else {
                self.add_obj_to_parts(obj_id, pending_part_ids).await?;
            }
            return Ok(());
        }
        assert!(
            pending_part_ids.is_empty(),
            "readable object cannot retain latent part memberships"
        );
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let cursor = Self::next_cursor(&mut tx).await?;
        for part_id in &live_part_ids {
            sqlx::query(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
            )
            .bind(self.core.scope_id)
            .bind(Self::part_blob(*part_id))
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE big_sync_members
                 SET changed_at = ?1, latest_cursor = ?1
                 WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
            )
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(self.core.scope_id)
            .bind(Self::part_blob(*part_id))
            .bind(Self::obj_blob(obj_id))
            .execute(&mut *tx)
            .await?;
            self.apply_bucket_transition(
                &mut tx,
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
            .bind(self.core.scope_id)
            .bind(Self::part_blob(*part_id))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.publish(vec![SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: live_part_ids,
            obj_id,
            payload,
        })])
        .await;
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query(
            "SELECT part_id FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL
             UNION
             SELECT part_id FROM big_sync_pending_members
             WHERE scope_id = ?1 AND obj_id = ?2
             ORDER BY part_id ASC",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&self.core.sql.read_pool)
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
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&self.core.sql.read_pool)
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
        .bind(self.core.scope_id)
        .bind(Self::part_blob(req.part_id))
        .fetch_optional(&self.core.sql.read_pool)
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
        query.push_bind(self.core.scope_id);
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
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;

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
        .bind(self.core.scope_id)
        .bind(Self::part_blob(req.part_id))
        .fetch_optional(&self.core.sql.read_pool)
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
        query.push_bind(self.core.scope_id);
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

        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;
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
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
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
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?;
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
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(payload_json.as_deref())
        .execute(&mut *tx)
        .await?;
        let Some(payload) = event_payload else {
            for part_id in parts {
                sqlx::query(
                    "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, part_id, obj_id)
                     VALUES (?1, ?2, ?3)",
                )
                .bind(self.core.scope_id)
                .bind(Self::part_blob(part_id))
                .bind(Self::obj_blob(obj_id))
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            return Ok(());
        };
        let added_payload_json = Some(serde_json::to_string(&payload).wrap_err(ERROR_JSON)?);
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
            .bind(self.core.scope_id)
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
            .bind(self.core.scope_id)
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
            .bind(self.core.scope_id)
            .bind(Self::part_blob(part_id))
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM big_sync_pending_members
                 WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
            )
            .bind(self.core.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        tx.commit().await?;
        self.publish(events).await;
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.core.scope_id)
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
        .bind(self.core.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM big_sync_pending_members
             WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
        )
        .bind(self.core.scope_id)
        .bind(Self::part_blob(part_id))
        .bind(Self::obj_blob(obj_id))
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
        .bind(self.core.scope_id)
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
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .fetch_one(&mut *tx)
        .await?;
        if live_count == 0 {
            sqlx::query(
                "UPDATE big_sync_objs
                 SET payload_json = NULL
                 WHERE scope_id = ?1 AND obj_id = ?2",
            )
            .bind(self.core.scope_id)
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
        .bind(self.core.scope_id)
        .bind(Self::peer_blob(peer_id))
        .bind(Self::part_blob(part_id))
        .fetch_optional(&self.core.sql.read_pool)
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
        .bind(self.core.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&self.core.sql.write_pool)
        .await?;
        sqlx::query(
            "INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_id, cursor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, peer_id, part_id) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)",
        )
        .bind(self.core.scope_id)
        .bind(Self::peer_blob(peer_id))
        .bind(Self::part_blob(part_id))
        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
        .execute(&self.core.sql.write_pool)
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
            .bind(self.core.scope_id)
            .bind(Self::part_blob(part_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.core.sql.read_pool)
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
                            payload: added_payload
                                .clone()
                                .expect("visible membership requires added payload"),
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
                // next_cursor is the LAST returned event, not the first
                // excluded one.  The input cursor means "return events after
                // this cursor", so the caller passes next_cursor verbatim.
                let next = events[limit_usize.saturating_sub(1)].0;
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
        use big_sync_core::rpc::SubscriptionTarget;

        let part_cursors: HashMap<PartId, CursorIndex> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, cursor } => Some((*part_id, *cursor)),
                SubscriptionTarget::Object { .. } => None,
            })
            .collect();
        let objects: HashSet<ObjId> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id } => Some(*obj_id),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect();
        let parts: HashSet<_> = part_cursors.keys().copied().collect();
        if let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }

        let (tx, rx) = mpsc::unbounded("SqlitePartStore".into(), "caller".into());
        let sub_id = Uuid::new_v4();
        let sub = Arc::new(SqliteSubscription {
            sender: tx.clone(),
            principal: subscriber,
            pending: PendingSubscription::new(),
        });
        {
            let mut bus = self.bus.write().expect(ERROR_MUTEX);
            bus.pending.insert(sub_id);
            bus.subs.insert(sub_id, Arc::clone(&sub));
            bus.parts_by_sub.insert(sub_id, parts.clone());
            for part_id in &parts {
                bus.by_part.entry(*part_id).or_default().insert(sub_id);
            }
            bus.objs_by_sub.insert(sub_id, objects.clone());
            for obj_id in &objects {
                bus.by_obj.entry(*obj_id).or_default().insert(sub_id);
            }
        }

        let store = self.clone();
        tokio::spawn(async move {
            let mut cursor = part_cursors.values().copied().min().unwrap_or_default();
            let mut marker_sent = false;
            let mut object_replay_pending = true;
            loop {
                sub.pending
                    .state
                    .store(SUB_REPLAYING_CLEAN, std::sync::atomic::Ordering::Release);
                let page = store
                    .list_events(parts.clone(), cursor, u32::MAX)
                    .await
                    .expect(ERROR_IMPOSSIBLE)
                    .expect(ERROR_IMPOSSIBLE);
                let mut output: Vec<SubEvent> = Vec::new();
                let mut max_cursor = cursor;
                let mut raw_event_count = 0;
                for (part_id, part_page) in page {
                    for event in part_page.events {
                        raw_event_count += 1;
                        let event_cursor = match &event {
                            PartEvent::Changed(inner) => inner.cursor,
                            PartEvent::Added(inner) => inner.cursor,
                            PartEvent::Removed(inner) => inner.cursor,
                        };
                        max_cursor = max_cursor.max(event_cursor);
                        if event_cursor <= part_cursors.get(&part_id).copied().unwrap_or_default() {
                            continue;
                        }
                        let obj_id = match &event {
                            PartEvent::Changed(inner) => inner.obj_id,
                            PartEvent::Added(inner) => inner.obj_id,
                            PartEvent::Removed(inner) => inner.obj_id,
                        };
                        let permitted = store
                            .policy
                            .is_event_permitted(Some(part_id), obj_id, Some(subscriber));
                        if !permitted {
                            continue;
                        }
                        match event {
                            PartEvent::Changed(inner) => {
                                if let Some(SubEvent::Changed(existing)) =
                                    output.iter_mut().find(|candidate| {
                                        matches!(
                                            candidate,
                                            SubEvent::Changed(candidate)
                                                if candidate.cursor == inner.cursor
                                                    && candidate.obj_id == inner.obj_id
                                        )
                                    })
                                {
                                    if !existing.part_ids.contains(&part_id) {
                                        existing.part_ids.push(part_id);
                                    }
                                } else {
                                    let mut inner = inner;
                                    inner.part_ids = vec![part_id];
                                    output.push(SubEvent::Changed(inner));
                                }
                            }
                            PartEvent::Added(inner) => output.push(SubEvent::Added(inner)),
                            PartEvent::Removed(inner) => output.push(SubEvent::Removed(inner)),
                        }
                    }
                }
                if object_replay_pending {
                    for obj_id in &objects {
                        let permitted = store
                            .policy
                            .is_event_permitted(None, *obj_id, Some(subscriber));
                        if permitted {
                            if let Some(payload) = HostPartStore::obj_payload(&store, *obj_id)
                                .await
                                .expect(ERROR_IMPOSSIBLE)
                            {
                                output.push(SubEvent::ObjectChanged(
                                    big_sync_core::rpc::ObjChangedWithoutPart {
                                        obj_id: *obj_id,
                                        payload,
                                    },
                                ));
                            }
                        }
                    }
                    object_replay_pending = false;
                }
                for event in output {
                    if tx.send(event).await.is_err() {
                        store.bus.write().expect(ERROR_MUTEX).remove(sub_id);
                        return;
                    }
                }
                cursor = max_cursor;
                if raw_event_count != 0 {
                    continue;
                }
                if !marker_sent {
                    if !sub.pending.begin_finalization() {
                        object_replay_pending = true;
                        continue;
                    }
                    if tx.send(SubEvent::ReplayComplete).await.is_err() {
                        store.bus.write().expect(ERROR_MUTEX).remove(sub_id);
                        return;
                    }
                    marker_sent = true;
                    if sub.pending.become_ready() {
                        return;
                    }
                    object_replay_pending = true;
                } else if sub.pending.become_ready() {
                    return;
                } else {
                    object_replay_pending = true;
                }
            }
        });
        Ok(Ok(rx))
    }

    async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        sqlx::query(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
        )
        .bind(self.core.scope_id)
        .bind(Self::part_blob(part_id))
        .execute(&self.core.sql.write_pool)
        .await?;
        Ok(())
    }

    async fn set_obj_members(
        &self,
        obj: ObjId,
        agents: HashMap<PeerId, keyhive_core::access::Access>,
    ) {
        let obj_blob = Self::obj_blob(obj);
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2")
            .bind(self.core.scope_id)
.bind(&obj_blob)
            .execute(&mut *tx)
            .await
            .unwrap();
        for (principal, access) in &agents {
            sqlx::query(
                "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(self.core.scope_id)
.bind(&obj_blob)
            .bind(Self::peer_blob(*principal))
            .bind(encode_access(access))
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        self.policy.set_obj_members(obj, agents);
    }

    async fn add_obj_member(
        &self,
        obj: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) {
        let obj_blob = Self::obj_blob(obj);
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2")
            .bind(self.core.scope_id)
.bind(&obj_blob)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(self.core.scope_id)
.bind(&obj_blob)
        .bind(Self::peer_blob(member))
        .bind(encode_access(&access))
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        self.policy.add_obj_member(obj, member, access);
    }

    async fn remove_obj_member(&self, obj: ObjId, member: PeerId) {
        let obj_blob = Self::obj_blob(obj);
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2 AND principal_id = ?3")
            .bind(self.core.scope_id)
.bind(&obj_blob)
            .bind(Self::peer_blob(member))
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        self.policy.remove_obj_member(obj, member);
    }
}

#[cfg(test)]
impl PartStoreReadOnly<Sendable> for SqlitePartStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        Sendable::from_future(async move {
            HostPartStore::member_count(self, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        Sendable::from_future(async move {
            HostPartStore::obj_payload(self, obj_id)
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
            HostPartStore::get_bucket_summary(self, part_id, id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        Sendable::from_future(async move {
            HostPartStore::obj_parts(self, obj_id)
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
            HostPartStore::get_peer_part_cursor(self, peer_id, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }
}

#[cfg(test)]
impl big_sync_core::part_store::PartStore<Sendable> for SqlitePartStore {
    fn upsert_obj<'a>(&'a self, obj_id: ObjId, payload: &ObjPayload) -> BoxFuture<'a, ()> {
        let payload = payload.clone();
        Sendable::from_future(async move {
            HostPartStore::set_obj_payload(self, obj_id, payload)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartStore::add_obj_to_parts(self, obj_id, parts)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartStore::remove_obj_from_part(self, obj_id, part_id)
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
            HostPartStore::set_peer_part_cursor(self, peer_id, part_id, cursor)
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
        .bind(self.core.scope_id)
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        let mut objs = std::collections::BTreeMap::new();
        for row in rows {
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let part_id = Self::part_from_blob(row.try_get("part_id")?);
            let payload_json: Option<String> = row.try_get("payload_json")?;
            let payload = payload_json
                .as_deref()
                .filter(|payload| !payload.is_empty())
                .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
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
        .bind(self.core.scope_id)
        .fetch_all(&self.core.sql.read_pool)
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
mod tests {
    use super::*;
    use crate::part_store::host_contract::{self, HostPartStoreContractHarness};
    use big_sync_core::part_store::contract;

    async fn test_sql() -> Res<SqlCtx> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let sqlite_url = format!("sqlite://{}", db_path.display());
        SqlCtx::url(&sqlite_url).await
    }

    async fn test_store(scope_key: &str) -> Res<SqlitePartStore> {
        let sql = test_sql().await?;
        SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL, Arc::new(crate::AllowAllPolicy)).await
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
            HostPartStore::set_obj_payload(
                &store,
                obj_id,
                serde_json::json!({"phase": "present", "ii": ii}),
            )
            .await?;
            HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;
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
        HostPartStore::remove_obj_from_part(&store, removed_obj_id, part_id).await?;
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

        HostPartStore::set_obj_payload(&store, obj_id, serde_json::json!({"phase": "created"}))
            .await?;
        HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;
        HostPartStore::remove_obj_from_part(&store, obj_id, part_id).await?;

        let deleted_page = HostPartStore::list_events(&store, HashSet::from([part_id]), 0, 10)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        let deleted_events = &deleted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(deleted_events.len(), 1);
        let PartEvent::Removed(transition) = &deleted_events[0] else {
            panic!("expected removed event");
        };
        assert_eq!(transition.cursor, 2);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);

        HostPartStore::set_obj_payload(&store, obj_id, serde_json::json!({"phase": "recreated"}))
            .await?;
        HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;

        let upserted_page = HostPartStore::list_events(&store, HashSet::from([part_id]), 0, 10)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        let upserted_events = &upserted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(upserted_events.len(), 1);
        let PartEvent::Added(second_added) = &upserted_events[0] else {
            panic!("expected second added event");
        };
        assert_eq!(second_added.cursor, 3);
        assert_eq!(second_added.part_id, part_id);
        assert_eq!(second_added.obj_id, obj_id);
        assert_eq!(
            second_added.payload,
            serde_json::json!({"phase": "recreated"})
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_scopes_are_isolated_by_scope_key() -> Res<()> {
        let store_a = test_store("big-sync-sqlite-test://repo").await?;
        let store_b = SqlitePartStore::new(
            store_a.core.sql.clone(),
            "big-sync-sqlite-test://other-repo",
            BuckId::MAX_LEVEL,
            Arc::new(crate::AllowAllPolicy),
        )
        .await?;
        let part_id = test_part_id(9);
        let obj_id = test_obj_id(10);

        HostPartStore::set_obj_payload(&store_a, obj_id, serde_json::json!({"scope": "a"})).await?;
        HostPartStore::add_obj_to_parts(&store_a, obj_id, vec![part_id]).await?;
        HostPartStore::set_obj_payload(&store_b, obj_id, serde_json::json!({"scope": "b"})).await?;
        HostPartStore::add_obj_to_parts(&store_b, obj_id, vec![part_id]).await?;

        assert_eq!(
            HostPartStore::obj_payload(&store_a, obj_id).await?,
            Some(serde_json::json!({"scope": "a"}))
        );
        assert_eq!(
            HostPartStore::obj_payload(&store_b, obj_id).await?,
            Some(serde_json::json!({"scope": "b"}))
        );
        assert_eq!(HostPartStore::member_count(&store_a, part_id).await?, 1);
        assert_eq!(HostPartStore::member_count(&store_b, part_id).await?, 1);
        Ok(())
    }

    struct SqliteHostHarness {
        store: SqlitePartStore,
    }

    #[async_trait]
    impl HostPartStoreContractHarness for SqliteHostHarness {
        fn store(&self) -> &dyn HostPartStore {
            &self.store
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_host_part_store_contract() -> Res<()> {
        let harness = SqliteHostHarness {
            store: test_store("big-sync-sqlite-test://host-contract").await?,
        };
        host_contract::assert_host_part_store_contract(&harness).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_membership_cache_rehydrates_after_restart() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{timeout, Duration};

        let sql = test_sql().await?;
        let scope_key = "big-sync-sqlite-test://membership-restart";

        // ---- first session ----
        let store1 = SqlitePartStore::new(sql.clone(), scope_key, BuckId::MAX_LEVEL, Arc::new(crate::AllowAllPolicy)).await?;
        let part = PartId(Byte32Id::new([201u8; 32]));
        let obj = ObjId(Byte32Id::new([202u8; 32]));
        let auth = PeerId::new([203u8; 32]);
        let denied = PeerId::new([204u8; 32]);

        store1.ensure_part(part).await?;
        store1
            .set_obj_payload(obj, serde_json::json!("first"))
            .await?;
        store1.add_obj_to_parts(obj, vec![part]).await?;

        // Persist membership.
        store1
            .set_obj_members(obj, std::collections::HashMap::from([(auth, Access::Read)]))
            .await;

        // Helper to drain through ReplayComplete.
        async fn drain_through_replay(rx: &mpsc::Receiver<SubEvent>) -> Res<()> {
            loop {
                match timeout(Duration::from_secs(5), rx.recv()).await? {
                    Ok(SubEvent::ReplayComplete) => return Ok(()),
                    Ok(_) => continue,
                    Err(_) => eyre::bail!("sub channel closed during replay"),
                }
            }
        }

        let sub = |peer: PeerId| {
            let store = &store1;
            let part = &part;
            async move {
                store
                    .subscribe(
                        SubPartsRequest {
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: *part,
                                    cursor: 0,
                                },
                            ]),
                        },
                        peer,
                    )
                    .await?
                    .map_err(eyre::Report::from)
            }
        };
        let auth_rx1 = sub(auth).await?;
        let denied_rx1 = sub(denied).await?;
        drain_through_replay(&auth_rx1).await?;
        drain_through_replay(&denied_rx1).await?;

        // Live mutation: authorized should receive, denied should not.
        store1
            .set_obj_payload(obj, serde_json::json!("second"))
            .await?;
        let _ = timeout(Duration::from_secs(2), auth_rx1.recv())
            .await
            .expect("authorized must receive live event in first session")
            .expect("channel must not close for authorized");
        match timeout(Duration::from_millis(500), denied_rx1.recv()).await {
            Err(_elapsed) => {} /* expected */
            Ok(Ok(evt)) => {
                panic!("denied must not receive live event in first session; got {evt:?}");
            }
            Ok(Err(_)) => {
                panic!("denied channel closed unexpectedly in first session");
            }
        }

        // Drop store1 to simulate restart.
        drop(store1);

        // ---- second session on the same database and scope ----
        let store2 = SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL, Arc::new(crate::AllowAllPolicy)).await?;

        let sub2 = |peer: PeerId| {
            let store = &store2;
            let part = &part;
            async move {
                store
                    .subscribe(
                        SubPartsRequest {
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: *part,
                                    cursor: 0,
                                },
                            ]),
                        },
                        peer,
                    )
                    .await?
                    .map_err(eyre::Report::from)
            }
        };
        let auth_rx2 = sub2(auth).await?;
        let denied_rx2 = sub2(denied).await?;
        drain_through_replay(&auth_rx2).await?;
        drain_through_replay(&denied_rx2).await?;
        // Live mutation after restart.
        store2
            .set_obj_payload(obj, serde_json::json!("third"))
            .await?;
        let _ = timeout(Duration::from_secs(2), auth_rx2.recv())
            .await
            .expect("authorized must receive live event after restart")
            .expect("channel must not close for authorized after restart");
        // Denied must still be denied after restart (cache must be rehydrated).
        match timeout(Duration::from_millis(500), denied_rx2.recv()).await {
            Err(_elapsed) => {} /* expected: cache correctly rehydrated */
            Ok(Ok(evt)) => {
                panic!(
                    "denied subscriber received live event after restart; cache was NOT rehydrated, got {evt:?}"
                );
            }
            Ok(Err(_)) => {
                panic!("denied channel closed unexpectedly after restart");
            }
        }

        Ok(())
    }
}
