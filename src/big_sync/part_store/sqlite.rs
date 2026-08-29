use super::super::keyed_frontier::{
    PartFrontierKey, SqlitePartFrontier, SqlitePartSelector, open_sqlite_reader,
};
use super::HostPartStore;
use super::LocalPartRevisionReader;
use super::sqlite_core::{EVENT_ADDED, EVENT_REMOVED};
use crate::interlude::*;
#[cfg(test)]
use crate::test_support::{ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot};

#[cfg(test)]
use big_sync_core::Byte32Id;
use big_sync_core::keyed_frontier::{
    FrontierRead, FrontierReadLimits, KeyedFrontier, KeyedFrontierTransaction,
};
#[cfg(test)]
use big_sync_core::part_store::PartStoreReadOnly;
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, ObjAddedToPart, ObjChanged,
    ObjRemovedFromPart, PartEvent, PartPage, PartSummary, SubEvent, SubPartsRequest,
};
use big_sync_core::{BuckId, Fingerprint, ObjId, PartId, PeerId, mpsc};
#[cfg(test)]
use future_form::{FutureForm, Sendable};
#[cfg(test)]
use futures::future::BoxFuture;
use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;
use tokio::sync::Notify;
#[cfg(test)]
use uuid::Uuid;

use super::sqlite_core::encode_access;

struct ReplayCandidate {
    txid: CursorIndex,
    obj_id: ObjId,
    event_type: i64,
    payload: ObjPayload,
}

#[derive(Clone)]
pub struct SqlitePartStore {
    pub(crate) core: SqliteCore,
    pub(crate) frontier: SqlitePartFrontier,
    hidden_parts: Arc<HashSet<PartId>>,
}

/// Open a local revision reader over an existing BigSync SQLite schema.
/// Callers that own a different store facade can supply its read pool, scope,
/// and commit wakeup without going through the legacy subscription channel.
pub async fn open_sqlite_local_revision_reader(
    read_pool: sqlx::SqlitePool,
    scope_id: i64,
    changed: Arc<Notify>,
    reqs: SubPartsRequest,
    limits: big_sync_core::revisioned_store::RevisionReadLimits,
) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
    use big_sync_core::rpc::SubscriptionTarget;

    let objects = reqs
        .targets
        .iter()
        .filter_map(|target| match target {
            SubscriptionTarget::Object { obj_id } => Some(*obj_id),
            SubscriptionTarget::Part { .. } => None,
        })
        .collect::<HashSet<_>>();
    let parts = reqs
        .targets
        .iter()
        .filter_map(|target| match target {
            SubscriptionTarget::Part { part_id, .. } => Some(*part_id),
            SubscriptionTarget::Object { .. } => None,
        })
        .collect::<HashSet<_>>();
    let mut selector = SqlitePartSelector::default();
    for target in reqs.targets {
        match target {
            SubscriptionTarget::Part { part_id, cursor } => {
                selector
                    .parts
                    .entry(part_id)
                    .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound.max(cursor)))
                    .or_insert(reqs.lower_bound.max(cursor));
            }
            SubscriptionTarget::Object { obj_id } => {
                selector
                    .objects
                    .entry(obj_id)
                    .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound))
                    .or_insert(reqs.lower_bound);
            }
        }
    }
    let frontier = SqlitePartFrontier::new(read_pool.clone(), read_pool, scope_id, changed);
    let reader = open_sqlite_reader(
        frontier,
        selector,
        big_sync_core::keyed_frontier::FrontierReadLimits {
            max_entries: limits.max_entries,
        },
    )
    .await?;
    Ok(Ok(Box::new(super::PartRevisionReader::new(
        reader, objects, parts,
    ))))
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
}

impl SqlitePartStore {
    pub async fn new(sql: SqlCtx, scope_key: impl Into<Arc<str>>, bucket_depth: u8) -> Res<Self> {
        Self::new_with_config(sql, scope_key, bucket_depth, Default::default()).await
    }

    pub async fn new_with_config(
        sql: SqlCtx,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
        config: super::HostPartStoreConfig,
    ) -> Res<Self> {
        SqliteCore::init_schema(&sql.write_pool, bucket_depth).await?;
        let core = SqliteCore::new(sql, scope_key, bucket_depth).await?;
        let changed = Arc::new(tokio::sync::Notify::new());
        let frontier = SqlitePartFrontier::new(
            core.sql.read_pool.clone(),
            core.sql.write_pool.clone(),
            core.scope_id,
            changed,
        );

        Ok(Self {
            core,
            frontier,
            hidden_parts: Arc::new(config.hidden_parts),
        })
    }
}

impl SqlitePartStore {
    async fn replay_candidates(
        &self,
        parts: &HashSet<PartId>,
        objects: &HashSet<ObjId>,
        lower_bound: CursorIndex,
        exact_txid: Option<CursorIndex>,
        limit: Option<u32>,
    ) -> Res<Vec<ReplayCandidate>> {
        if parts.is_empty() && objects.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT m.txid, o.obj_id, m.event_type, o.payload_json
             FROM big_sync_members m
             JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
             LEFT JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        if let Some(txid) = exact_txid {
            query.push(" AND m.txid = ");
            query.push_bind(i64::try_from(txid).expect(ERROR_IMPOSSIBLE));
        } else {
            query.push(" AND m.txid > ");
            query.push_bind(i64::try_from(lower_bound).expect(ERROR_IMPOSSIBLE));
        }
        query.push(" AND (");
        if parts.is_empty() {
            query.push("0");
        } else {
            query
                .push("m.maybe_part_ref IN (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
            query.push_bind(self.core.scope_id);
            query.push(" AND part_id IN (");
            let mut separated = query.separated(", ");
            for part in parts {
                separated.push_bind(Self::part_blob(*part));
            }
            separated.push_unseparated("))");
        }
        query.push(" OR ");
        if objects.is_empty() {
            query.push("0");
        } else {
            query.push("m.obj_ref IN (SELECT obj_ref FROM big_sync_objs WHERE scope_id = ");
            query.push_bind(self.core.scope_id);
            query.push(" AND obj_id IN (");
            let mut separated = query.separated(", ");
            for obj in objects {
                separated.push_bind(Self::obj_blob(*obj));
            }
            separated.push_unseparated("))");
        }
        query.push(") ORDER BY m.txid, m.obj_ref, m.maybe_part_ref");
        if let Some(limit) = limit {
            query.push(" LIMIT ");
            query.push_bind(i64::from(limit));
        }
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let payload = row
                    .try_get::<Option<String>, _>("payload_json")?
                    .as_deref()
                    .filter(|str| !str.is_empty())
                    .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                Ok(ReplayCandidate {
                    txid: u64::try_from(row.try_get::<i64, _>("txid")?).expect(ERROR_IMPOSSIBLE),
                    obj_id: Self::obj_from_blob(row.try_get("obj_id")?),
                    event_type: row.try_get("event_type")?,
                    payload,
                })
            })
            .collect()
    }
}

#[async_trait]
impl HostPartStore for SqlitePartStore {
    async fn latest_revision(&self) -> Res<CursorIndex> {
        let revision: i64 =
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&self.core.sql.read_pool)
                .await?;
        Ok(u64::try_from(revision)?)
    }

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
              AND b.part_ref = p.part_ref
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
                    deepest_bucket_level: self.core.bucket_depth,
                },
            );
        }
        Ok(Ok(out))
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        if let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let mut out = HashMap::new();
        for part_id in parts {
            let requested_part = HashSet::from([part_id]);
            let candidates = self
                .replay_candidates(&requested_part, &HashSet::new(), cursor, None, Some(limit))
                .await?;
            let limit_usize = usize::try_from(limit).expect(ERROR_IMPOSSIBLE);
            let (candidates, next_cursor) = if limit_usize == 0 || candidates.len() < limit_usize {
                (candidates, None)
            } else {
                let cutoff = candidates.last().expect(ERROR_IMPOSSIBLE).txid;
                let mut page = candidates
                    .into_iter()
                    .filter(|candidate| candidate.txid < cutoff)
                    .collect::<Vec<_>>();
                let boundary = self
                    .replay_candidates(&requested_part, &HashSet::new(), cursor, Some(cutoff), None)
                    .await?;
                page.extend(boundary);
                let has_more = !self
                    .replay_candidates(&requested_part, &HashSet::new(), cutoff, None, Some(1))
                    .await?
                    .is_empty();
                (page, has_more.then_some(cutoff))
            };
            let events = candidates
                .iter()
                .map(|candidate| match candidate.event_type {
                    EVENT_ADDED => PartEvent::Added(ObjAddedToPart {
                        cursor: candidate.txid,
                        part_id,
                        obj_id: candidate.obj_id,
                        payload: candidate.payload.clone(),
                    }),
                    EVENT_REMOVED => PartEvent::Removed(ObjRemovedFromPart {
                        cursor: candidate.txid,
                        part_id,
                        obj_id: candidate.obj_id,
                    }),
                    _ => PartEvent::Changed(ObjChanged {
                        cursor: candidate.txid,
                        part_ids: vec![part_id],
                        obj_id: candidate.obj_id,
                        payload: candidate.payload.clone(),
                    }),
                })
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

    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        let member_count: Option<i64> = sqlx::query_scalar!(
            "SELECT live_count
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?2
             ) AND level = 0 AND buck_id = 0",
            self.core.scope_id,
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        Ok(member_count
            .map(|member_count| u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let row = sqlx::query!(
            "SELECT payload_json
                 FROM big_sync_objs
                 WHERE scope_id = ?1 AND obj_id = ?2",
            self.core.scope_id,
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Option<String> = row.payload_json;
        payload
            .as_deref()
            .filter(|payload| !payload.is_empty())
            .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn set_obj_payload(&self, obj_id: ObjId, payload: ObjPayload) -> Res<()> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut frontier_tx = self.frontier.begin().await?;
        let cursor = frontier_tx.revision().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id).await?;
        let old_payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        sqlx::query!(
            "UPDATE big_sync_objs SET payload_json = ?1 WHERE obj_ref = ?2",
            payload_json,
            obj_ref
        )
        .execute(&mut **tx)
        .await?;
        let live_parts = sqlx::query!(
            "SELECT m.maybe_part_ref, p.part_id
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2
               AND m.maybe_part_ref > 0 AND m.event_type != ?3",
            self.core.scope_id,
            obj_ref,
            EVENT_REMOVED
        )
        .fetch_all(&mut **tx)
        .await?;
        let pending_parts = sqlx::query!(
            "SELECT p.part_ref, p.part_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2",
            self.core.scope_id,
            obj_ref
        )
        .fetch_all(&mut **tx)
        .await?;
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|str| !str.is_empty())
            .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        for part in &live_parts {
            let old_state = MemberState::Live(old_payload.clone());
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    Self::part_from_blob(part.part_id.clone()),
                    obj_id,
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
        }
        let mut events = vec![SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: live_parts
                .iter()
                .map(|row| Self::part_from_blob(row.part_id.clone()))
                .collect(),
            obj_id,
            payload: payload.clone(),
        })];
        for part in &pending_parts {
            let part_id = Self::part_from_blob(part.part_id.clone());
            let part_ref = part.part_ref;
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    part_id,
                    obj_id,
                    cursor,
                    &MemberState::Absent,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members
                 WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.core.scope_id,
                obj_ref,
                part_ref
            )
            .execute(&mut **tx)
            .await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        if live_parts.is_empty() {
            frontier_tx
                .put(
                    PartFrontierKey::Object(obj_id),
                    PartEvent::Changed(ObjChanged {
                        cursor,
                        part_ids: Vec::new(),
                        obj_id,
                        payload: payload.clone(),
                    }),
                )
                .await?;
        }
        for part in &live_parts {
            frontier_tx
                .put(
                    PartFrontierKey::Part {
                        obj_id,
                        part_id: Self::part_from_blob(part.part_id.clone()),
                    },
                    PartEvent::Changed(ObjChanged {
                        cursor,
                        part_ids: vec![Self::part_from_blob(part.part_id.clone())],
                        obj_id,
                        payload: payload.clone(),
                    }),
                )
                .await?;
        }
        for part in &pending_parts {
            let part_id = Self::part_from_blob(part.part_id.clone());
            frontier_tx
                .put(
                    PartFrontierKey::Part { obj_id, part_id },
                    PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                        cursor,
                        part_id,
                        obj_id,
                        payload: payload.clone(),
                    }),
                )
                .await?;
        }
        frontier_tx.commit().await?;
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query!(
            "SELECT p.part_id
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             ) AND m.maybe_part_ref > 0 AND m.event_type != ?3
             UNION
             SELECT p.part_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             )
             ORDER BY part_id ASC",
            self.core.scope_id,
            Self::obj_blob(obj_id),
            EVENT_REMOVED
        )
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.part_id))
            .collect())
    }

    async fn obj_exists(&self, obj_id: ObjId) -> Res<bool> {
        let exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.core.scope_id,
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        Ok(exists.is_some())
    }

    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        self.core.bucket_summary_for_path(part_id, id).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.core.scope_id,
            Self::part_blob(req.part_id)
        )
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
        query.push(" AND part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(self.core.scope_id);
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(")");
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
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.core.scope_id,
            Self::part_blob(req.part_id)
        )
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
                    o.obj_id,
                    m.event_type,
                    o.payload_json,
                    COUNT(*) OVER (PARTITION BY r.req_ord) AS total_count,
                    ROW_NUMBER() OVER (PARTITION BY r.req_ord ORDER BY o.obj_id ASC) AS row_num
                FROM requested r
                JOIN big_sync_members m
                  ON m.scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        query
            .push(" AND m.maybe_part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(self.core.scope_id);
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(") JOIN big_sync_buckets s ON s.scope_id = m.scope_id AND s.part_ref = m.maybe_part_ref AND s.buck_id = r.buck_id AND s.changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" AND o.obj_id >= r.lower_id");
        query.push(" AND (r.upper_id IS NULL OR o.obj_id < r.upper_id)");
        query.push(" AND (r.after_id IS NULL OR o.obj_id > r.after_id)");
        query.push(
            "
                JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
            )
            SELECT req_ord, buck_id, obj_id, event_type, payload_json, total_count
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
            let dead = row.try_get::<i64, _>("event_type")? == EVENT_REMOVED;
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
        let mut frontier_tx = self.frontier.begin().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id).await?;
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        let Some(payload_json) = payload_json.filter(|str| !str.is_empty()) else {
            for part_id in parts {
                let part_ref = self.core.ensure_part_ref(&mut *tx, part_id).await?;
                sqlx::query!(
                    "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, obj_ref, part_ref)
                     VALUES (?1, ?2, ?3)",
                    self.core.scope_id,
                    obj_ref,
                    part_ref
                )
                .execute(&mut **tx)
                .await?;
            }
            frontier_tx.commit().await?;
            return Ok(());
        };
        let payload: ObjPayload = serde_json::from_str(&payload_json).wrap_err(ERROR_JSON)?;
        let cursor = frontier_tx.revision().await?;
        let tx = frontier_tx.context_mut();
        let mut events = Vec::new();
        for part_id in parts {
            let part_ref = self.core.ensure_part_ref(&mut *tx, part_id).await?;
            let old_state = self
                .core
                .load_member_state(&mut *tx, part_id, obj_id)
                .await?;
            if matches!(old_state, MemberState::Live(_)) {
                continue;
            }
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    part_id,
                    obj_id,
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.core.scope_id, obj_ref, part_ref
            ).execute(&mut **tx).await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        for event in &events {
            if let SubEvent::Added(added) = event {
                frontier_tx
                    .put(
                        PartFrontierKey::Part {
                            obj_id,
                            part_id: added.part_id,
                        },
                        PartEvent::Added(added.clone()),
                    )
                    .await?;
            }
        }
        frontier_tx.commit().await?;
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut frontier_tx = self.frontier.begin().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref: Option<i64> = sqlx::query_scalar!(
            "SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2",
            self.core.scope_id,
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&mut **tx)
        .await?;
        let Some(obj_ref) = obj_ref else {
            frontier_tx.commit().await?;
            return Ok(());
        };
        let part_ref = self.core.ensure_part_ref(&mut *tx, part_id).await?;
        sqlx::query!(
            "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
            self.core.scope_id, obj_ref, part_ref
        ).execute(&mut **tx).await?;
        let old_state = self
            .core
            .load_member_state(&mut *tx, part_id, obj_id)
            .await?;
        let MemberState::Live(old_payload) = old_state else {
            frontier_tx.commit().await?;
            return Ok(());
        };
        let cursor = frontier_tx.revision().await?;
        let tx = frontier_tx.context_mut();
        self.core
            .apply_bucket_transition(
                &mut *tx,
                part_id,
                obj_id,
                cursor,
                &MemberState::Live(old_payload),
                &MemberState::Dead,
            )
            .await?;
        let live_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE scope_id = ? AND obj_ref = ? AND maybe_part_ref > 0
               AND maybe_part_ref != ? AND event_type != ?",
        )
        .bind(self.core.scope_id)
        .bind(obj_ref)
        .bind(part_ref)
        .bind(EVENT_REMOVED)
        .fetch_one(&mut **tx)
        .await?;
        if live_count == 0 {
            sqlx::query!(
                "UPDATE big_sync_objs SET payload_json = NULL WHERE obj_ref = ?1",
                obj_ref
            )
            .execute(&mut **tx)
            .await?;
        }
        frontier_tx
            .delete(PartFrontierKey::Part { obj_id, part_id })
            .await?;
        frontier_tx.commit().await?;
        Ok(())
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT cursor
             FROM big_sync_peer_cursors
             WHERE scope_id = ?1 AND peer_id = ?2 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?3
             )",
            self.core.scope_id,
            Self::peer_blob(peer_id),
            Self::part_blob(part_id)
        )
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
        let mut tx = self.core.sql.write_pool.begin().await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
        sqlx::query!(
            "INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_ref, cursor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, peer_id, part_ref) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)",
            self.core.scope_id,
            Self::peer_blob(peer_id),
            part_ref,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
        subscriber: PeerId,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;
        let mut selector = SqlitePartSelector::default();
        let mut parts = HashSet::new();
        let mut objects = HashSet::new();
        for target in &reqs.targets {
            match target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    parts.insert(*part_id);
                    selector
                        .parts
                        .insert(*part_id, reqs.lower_bound.max(*cursor));
                }
                SubscriptionTarget::Object { obj_id } => {
                    objects.insert(*obj_id);
                    selector.objects.insert(*obj_id, reqs.lower_bound);
                }
            }
        }
        if let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let (tx, rx) = mpsc::unbounded("SqlitePartStore".into(), "caller".into());
        let store = self.clone();
        tokio::spawn(async move {
            let mut reader = store
                .frontier
                .open(selector, FrontierReadLimits { max_entries: 256 })
                .await
                .expect(ERROR_IMPOSSIBLE);
            let mut replay_complete_sent = false;
            loop {
                let read = reader.next().await.expect(ERROR_IMPOSSIBLE);
                let FrontierRead::Entries { entries, .. } = read else {
                    assert!(
                        !replay_complete_sent,
                        "frontier emitted ReplayComplete twice"
                    );
                    replay_complete_sent = true;
                    if tx.send(SubEvent::ReplayComplete).await.is_err() {
                        return;
                    }
                    continue;
                };
                let mut output: Vec<SubEvent> = Vec::new();
                for entry in entries {
                    let key_obj_id = match entry.key {
                        PartFrontierKey::Object(obj_id) | PartFrontierKey::Part { obj_id, .. } => {
                            obj_id
                        }
                    };
                    let (part_id, event) = match (entry.key, entry.value) {
                        (PartFrontierKey::Object(_), None) => continue,
                        (PartFrontierKey::Object(_), Some(PartEvent::Changed(changed))) => {
                            (None, SubEvent::Changed(changed))
                        }
                        (PartFrontierKey::Object(_), Some(PartEvent::Added(_)))
                        | (PartFrontierKey::Object(_), Some(PartEvent::Removed(_))) => {
                            unreachable!("object frontier rows are changed or tombstones")
                        }
                        (
                            PartFrontierKey::Part { obj_id, part_id },
                            None | Some(PartEvent::Removed(_)),
                        ) if objects.contains(&obj_id) && !parts.contains(&part_id) => (
                            None,
                            SubEvent::Changed(ObjChanged {
                                cursor: entry.revision,
                                part_ids: Vec::new(),
                                obj_id,
                                payload: serde_json::Value::Null,
                            }),
                        ),
                        (
                            PartFrontierKey::Part { obj_id, part_id },
                            Some(PartEvent::Added(added)),
                        ) if objects.contains(&obj_id) && !parts.contains(&part_id) => (
                            None,
                            SubEvent::Changed(ObjChanged {
                                cursor: entry.revision,
                                part_ids: Vec::new(),
                                obj_id,
                                payload: added.payload,
                            }),
                        ),
                        (
                            PartFrontierKey::Part { obj_id, part_id },
                            Some(PartEvent::Changed(changed)),
                        ) if objects.contains(&obj_id) && !parts.contains(&part_id) => (
                            None,
                            SubEvent::Changed(ObjChanged {
                                cursor: entry.revision,
                                part_ids: Vec::new(),
                                obj_id,
                                payload: changed.payload,
                            }),
                        ),
                        (
                            PartFrontierKey::Part { obj_id, part_id },
                            None | Some(PartEvent::Removed(_)),
                        ) => (
                            Some(part_id),
                            SubEvent::Removed(ObjRemovedFromPart {
                                cursor: entry.revision,
                                part_id,
                                obj_id,
                            }),
                        ),
                        (
                            PartFrontierKey::Part { obj_id, part_id },
                            Some(PartEvent::Added(mut added)),
                        ) => {
                            added.cursor = entry.revision;
                            added.part_id = part_id;
                            added.obj_id = obj_id;
                            (Some(part_id), SubEvent::Added(added))
                        }
                        (
                            PartFrontierKey::Part { obj_id, part_id },
                            Some(PartEvent::Changed(mut changed)),
                        ) => {
                            changed.cursor = entry.revision;
                            changed.part_ids = vec![part_id];
                            changed.obj_id = obj_id;
                            (Some(part_id), SubEvent::Changed(changed))
                        }
                    };
                    if part_id.is_none() && !objects.contains(&key_obj_id) {
                        continue;
                    }
                    if let Some(part_id) = part_id
                        && !parts.contains(&part_id)
                    {
                        continue;
                    }
                    let permitted =
                        event_permitted(&store.core, part_id, key_obj_id, Some(subscriber))
                            .await
                            .expect(ERROR_IMPOSSIBLE);
                    if !permitted {
                        continue;
                    }
                    match event {
                        SubEvent::Changed(changed) => {
                            let entry = output.iter_mut().find_map(|event| match event {
                                SubEvent::Changed(existing)
                                    if existing.cursor == changed.cursor
                                        && existing.obj_id == changed.obj_id =>
                                {
                                    Some(existing)
                                }
                                _ => None,
                            });
                            if let Some(existing) = entry {
                                for part_id in changed.part_ids {
                                    if !existing.part_ids.contains(&part_id) {
                                        existing.part_ids.push(part_id);
                                    }
                                }
                                existing.part_ids.sort_unstable();
                            } else {
                                output.push(SubEvent::Changed(changed));
                            }
                        }
                        event => output.push(event),
                    }
                }
                for event in output {
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Ok(rx))
    }

    async fn open_local_revision_reader(
        &self,
        reqs: SubPartsRequest,
        limits: big_sync_core::revisioned_store::RevisionReadLimits,
    ) -> Res<Result<Box<dyn super::LocalPartRevisionReader>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;

        let objects = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id } => Some(*obj_id),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect::<HashSet<_>>();
        let parts = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, .. } => Some(*part_id),
                SubscriptionTarget::Object { .. } => None,
            })
            .collect::<HashSet<_>>();
        let mut selector = SqlitePartSelector::default();
        for target in reqs.targets {
            match target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    selector
                        .parts
                        .entry(part_id)
                        .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound.max(cursor)))
                        .or_insert(reqs.lower_bound.max(cursor));
                }
                SubscriptionTarget::Object { obj_id } => {
                    selector
                        .objects
                        .entry(obj_id)
                        .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound))
                        .or_insert(reqs.lower_bound);
                }
            }
        }
        let reader = open_sqlite_reader(
            self.frontier.clone(),
            selector,
            big_sync_core::keyed_frontier::FrontierReadLimits {
                max_entries: limits.max_entries,
            },
        )
        .await?;
        Ok(Ok(Box::new(super::PartRevisionReader::new(
            reader, objects, parts,
        ))))
    }

    async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        let mut tx = self.core.sql.write_pool.begin().await?;
        self.core.ensure_part_ref(&mut tx, part_id).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_obj_members(
        &self,
        obj: ObjId,
        agents: HashMap<PeerId, keyhive_core::access::Access>,
    ) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj).await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2",
            self.core.scope_id,
            obj_ref
        )
        .execute(&mut *tx)
        .await?;
        for (principal, access) in &agents {
            sqlx::query!(
                "INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level)
                 VALUES (?1, ?2, ?3, ?4)",
                self.core.scope_id,
                obj_ref,
                Self::peer_blob(*principal),
                encode_access(access)
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn add_obj_member(
        &self,
        obj: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj).await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2 AND principal_id = ?3",
            self.core.scope_id, obj_ref, Self::peer_blob(member)
        ).execute(&mut *tx).await?;
        sqlx::query!(
            "INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level)
             VALUES (?1, ?2, ?3, ?4)",
            self.core.scope_id,
            obj_ref,
            Self::peer_blob(member),
            encode_access(&access)
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_obj_member(&self, obj: ObjId, member: PeerId) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let Some(obj_ref) = self.core.find_obj_ref(obj).await? else {
            tx.commit().await?;
            return Ok(());
        };
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2 AND principal_id = ?3",
            self.core.scope_id, obj_ref, Self::peer_blob(member)
        ).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn is_event_permitted(
        &self,
        part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> Res<bool> {
        event_permitted(&self.core, part_id, obj_id, principal).await
    }
}

/// Policy check for delivering an event to a remote subscriber.
async fn event_permitted(
    core: &SqliteCore,
    part_id: Option<PartId>,
    obj_id: ObjId,
    principal: Option<PeerId>,
) -> Res<bool> {
    let Some(peer) = principal else {
        return Ok(true);
    };
    let peer_blob = SqliteCore::peer_blob(peer);
    let access_level: Option<i64> = sqlx::query_scalar!(
        "SELECT access_level
         FROM big_sync_syncable
         WHERE scope_id = ?1 AND obj_ref = (
             SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
         ) AND principal_id = ?3",
        core.scope_id,
        SqliteCore::obj_blob(obj_id),
        &peer_blob
    )
    .fetch_optional(&core.sql.read_pool)
    .await?;
    let permitted = access_level
        .map(|lvl| u8::try_from(lvl).expect(ERROR_IMPOSSIBLE))
        .map(super::sqlite_core::decode_access)
        .is_some_and(|access| access.is_fetcher());
    tracing::trace!(
        ?part_id,
        ?obj_id,
        ?principal,
        permitted,
        "policy event permission",
    );
    Ok(permitted)
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
        let rows = sqlx::query!(
            "SELECT objs.obj_id, parts.part_id, objs.payload_json
             FROM big_sync_members members
             JOIN big_sync_objs objs ON objs.obj_ref = members.obj_ref
             JOIN big_sync_parts parts ON parts.part_ref = members.maybe_part_ref
             WHERE members.scope_id = ?1 AND members.maybe_part_ref > 0
               AND members.event_type != ?2
             ORDER BY objs.obj_id ASC, parts.part_id ASC",
            self.core.scope_id,
            EVENT_REMOVED
        )
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        let mut objs = std::collections::BTreeMap::new();
        for row in rows {
            let obj_id = Self::obj_from_blob(row.obj_id);
            let part_id = Self::part_from_blob(row.part_id);
            let payload_json: Option<String> = row.payload_json;
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
        let cursors = sqlx::query!(
            "SELECT peer_id, parts.part_id, cursor
             FROM big_sync_peer_cursors
             JOIN big_sync_parts parts ON parts.part_ref = big_sync_peer_cursors.part_ref
             WHERE big_sync_peer_cursors.scope_id = ?1",
            self.core.scope_id
        )
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        let mut peer_part_cursors = std::collections::BTreeMap::new();
        for row in cursors {
            peer_part_cursors.insert(
                (
                    Self::peer_from_blob(row.peer_id),
                    Self::part_from_blob(row.part_id),
                ),
                u64::try_from(row.cursor).expect(ERROR_IMPOSSIBLE),
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
    use big_sync_core::keyed_frontier::KeyedFrontierTransaction;
    use big_sync_core::part_store::contract;

    async fn test_sql() -> Res<SqlCtx> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let sqlite_url = format!("sqlite://{}", db_path.display());
        SqlCtx::url(&sqlite_url).await
    }

    async fn test_store(scope_key: &str) -> Res<SqlitePartStore> {
        let sql = test_sql().await?;
        SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL).await
    }

    fn test_part_id(seed: u8) -> PartId {
        PartId(Byte32Id::new([seed; 32]))
    }

    fn test_obj_id(seed: u8) -> ObjId {
        ObjId(Byte32Id::new([seed; 32]))
    }

    async fn put_frontier_event(
        store: &SqlitePartStore,
        key: PartFrontierKey,
        event: PartEvent,
    ) -> Res<CursorIndex> {
        let mut tx = store.frontier.begin().await?;
        tx.put(key, event).await?;
        Ok(tx.commit().await?)
    }

    async fn delete_frontier_key(
        store: &SqlitePartStore,
        key: PartFrontierKey,
    ) -> Res<CursorIndex> {
        let mut tx = store.frontier.begin().await?;
        tx.delete(key).await?;
        Ok(tx.commit().await?)
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
        assert_eq!(transition.cursor, 3);
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
        assert_eq!(second_added.cursor, 5);
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
        use tokio::time::{Duration, timeout};

        let sql = test_sql().await?;
        let scope_key = "big-sync-sqlite-test://membership-restart";

        // ---- first session ----
        let store1 = SqlitePartStore::new(sql.clone(), scope_key, BuckId::MAX_LEVEL).await?;
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
            .await?;

        // Helper to drain through ReplayComplete.
        async fn drain_through_replay(rx: &mpsc::Receiver<SubEvent>) -> Res<()> {
            loop {
                match timeout(Duration::from_secs(5), rx.recv()).await? {
                    Ok(SubEvent::ReplayComplete) => return Ok(()),
                    Ok(_) => continue,
                    Err(_) => {
                        eyre::bail!("sub channel closed during replay");
                    }
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
                            lower_bound: 0,
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
        auth_rx1
            .recv()
            .await
            .expect("authorized must receive live event in first session");
        match timeout(
            utils_rs::scale_timeout(Duration::from_millis(500)),
            denied_rx1.recv(),
        )
        .await
        {
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
        let store2 = SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL).await?;

        let sub2 = |peer: PeerId| {
            let store = &store2;
            let part = &part;
            async move {
                store
                    .subscribe(
                        SubPartsRequest {
                            lower_bound: 0,
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
        auth_rx2
            .recv()
            .await
            .expect("authorized must receive live event after restart");
        // Denied must still be denied after restart (cache must be rehydrated).
        match timeout(
            utils_rs::scale_timeout(Duration::from_millis(500)),
            denied_rx2.recv(),
        )
        .await
        {
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

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_exact_object_receives_changed() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-object").await?;
        let obj_id = test_obj_id(210);
        let peer = PeerId::new([211; 32]);
        store
            .set_obj_members(obj_id, HashMap::from([(peer, Access::Read)]))
            .await?;
        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id,
                payload: serde_json::json!({"value": 1}),
            }),
        )
        .await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                        obj_id,
                    }]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        match rx.recv().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.cursor, 1);
                assert_eq!(changed.obj_id, obj_id);
                assert!(changed.part_ids.is_empty());
                assert_eq!(changed.payload, serde_json::json!({"value": 1}));
            }
            event => panic!("expected object Changed, got {event:?}"),
        }
        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_part_targets_have_independent_lower_bounds() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-part-cursors").await?;
        let obj_id = test_obj_id(212);
        let first_part = test_part_id(213);
        let second_part = test_part_id(214);
        let peer = PeerId::new([215; 32]);
        store.ensure_part(first_part).await?;
        store.ensure_part(second_part).await?;
        store
            .set_obj_members(obj_id, HashMap::from([(peer, Access::Read)]))
            .await?;
        assert_eq!(
            put_frontier_event(
                &store,
                PartFrontierKey::Part {
                    obj_id,
                    part_id: first_part,
                },
                PartEvent::Added(ObjAddedToPart {
                    cursor: 0,
                    part_id: first_part,
                    obj_id,
                    payload: serde_json::json!({"part": 1}),
                }),
            )
            .await?,
            1
        );
        assert_eq!(
            put_frontier_event(
                &store,
                PartFrontierKey::Part {
                    obj_id,
                    part_id: second_part,
                },
                PartEvent::Added(ObjAddedToPart {
                    cursor: 0,
                    part_id: second_part,
                    obj_id,
                    payload: serde_json::json!({"part": 2}),
                }),
            )
            .await?,
            2
        );

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: first_part,
                            cursor: 1,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: second_part,
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        match rx.recv().await.expect("subscription channel stays open") {
            SubEvent::Added(added) => {
                assert_eq!(added.cursor, 2);
                assert_eq!(added.part_id, second_part);
                assert_eq!(added.obj_id, obj_id);
            }
            event => panic!("expected only newer second-part Added, got {event:?}"),
        }
        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_coalesces_one_revision_across_parts() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-coalesce").await?;
        let obj_id = test_obj_id(216);
        let first_part = test_part_id(217);
        let second_part = test_part_id(218);
        let peer = PeerId::new([219; 32]);
        store.ensure_part(first_part).await?;
        store.ensure_part(second_part).await?;
        store
            .set_obj_members(obj_id, HashMap::from([(peer, Access::Read)]))
            .await?;
        let mut tx = store.frontier.begin().await?;
        tx.put(
            PartFrontierKey::Part {
                obj_id,
                part_id: first_part,
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![first_part],
                obj_id,
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;
        tx.put(
            PartFrontierKey::Part {
                obj_id,
                part_id: second_part,
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![second_part],
                obj_id,
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;
        assert_eq!(tx.commit().await?, 1);

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: first_part,
                            cursor: 0,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: second_part,
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        match rx.recv().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.cursor, 1);
                assert_eq!(changed.obj_id, obj_id);
                assert_eq!(
                    changed.part_ids.iter().copied().collect::<HashSet<_>>(),
                    HashSet::from([first_part, second_part])
                );
            }
            event => panic!("expected coalesced Changed, got {event:?}"),
        }
        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_part_tombstone_is_removed() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-tombstone").await?;
        let obj_id = test_obj_id(220);
        let part_id = test_part_id(221);
        let peer = PeerId::new([222; 32]);
        store.ensure_part(part_id).await?;
        store
            .set_obj_members(obj_id, HashMap::from([(peer, Access::Read)]))
            .await?;
        delete_frontier_key(&store, PartFrontierKey::Part { obj_id, part_id }).await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id,
                        cursor: 0,
                    }]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::Removed(ObjRemovedFromPart {
                cursor: 1,
                part_id,
                obj_id,
            })
        );
        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_replays_once_then_reads_after_boundary() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-boundary").await?;
        let obj_id = test_obj_id(223);
        let peer = PeerId::new([224; 32]);
        store
            .set_obj_members(obj_id, HashMap::from([(peer, Access::Read)]))
            .await?;
        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id,
                payload: serde_json::json!({"value": 1}),
            }),
        )
        .await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                        obj_id,
                    }]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        assert!(matches!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::Changed(ObjChanged { cursor: 1, .. })
        ));

        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id,
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;

        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        match rx.recv().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.cursor, 2);
                assert_eq!(changed.obj_id, obj_id);
                assert_eq!(changed.payload, serde_json::json!({"value": 2}));
            }
            event => panic!("expected post-boundary Changed, got {event:?}"),
        }
        Ok(())
    }
}
