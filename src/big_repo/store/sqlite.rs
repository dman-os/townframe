use crate::interlude::*;

use big_sync::HostPartStore;
use big_sync::open_sqlite_local_revision_reader;
use big_sync::sqlite_core::{
    EVENT_ADDED, EVENT_CHANGED, EVENT_REMOVED, MemberState, PendingSubscription, SUB_REPLAY_DONE,
    SUB_REPLAYING_CLEAN, SqliteCore, encode_access,
};
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest, SubscriptionTarget,
};
use big_sync_core::{BuckId, Byte32Id, Fingerprint, mpsc};
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob,
    collections::Set,
    crypto::digest::Digest,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use sqlx_utils_rs::SqlCtx;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use tokio::sync::Notify;
mod checkpoints;
mod events;
pub(crate) use events::{
    KEYHIVE_ADMISSION_READER_AUTOMERGE_FRONTIER, KEYHIVE_ADMISSION_READER_CAUSAL_CHECKPOINT,
    KEYHIVE_ADMISSION_READER_GROUP_PART, KEYHIVE_ADMISSION_READER_PREKEY_JANITOR,
};
mod ids;
mod parts_cursors;
mod sedimentree;
mod tree_cache;
use tree_cache::{TREE_CACHE_METADATA_CAPACITY, TreeCache, TreeCacheGuard};

#[cfg(test)]
static FAIL_NEXT_ADMISSION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static MIGRATOR: std::sync::LazyLock<sqlx::migrate::Migrator> = std::sync::LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.dangerous_set_table_name("_big_repo_migrations");
    migrator
});
struct KeyhiveEventQueryRow {
    event_hash: Vec<u8>,
    event_bytes: Vec<u8>,
    source_id: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests;

pub(crate) enum TreeStorageMutation {
    InsertCommit(VerifiedMeta<LooseCommit>),
    InsertFragment(VerifiedMeta<Fragment>),
    InsertBatch {
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    },
    DeleteCommit(CommitId),
    DeleteFragment(CommitId),
    DeleteAllCommits,
    DeleteAllFragments,
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
    objs_by_sub: HashMap<Uuid, HashSet<ObjId>>,
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
pub struct SqliteBigRepoStore {
    core: SqliteCore,
    bus: Arc<std::sync::RwLock<BigRepoSubscriptions>>,
    hidden_parts: Arc<HashSet<PartId>>,
    /// Transaction-scoped sedimentree projection cache (see [`TreeCache`]).
    tree_cache: Arc<std::sync::Mutex<TreeCache>>,
    local_revision_wakeups: Arc<Notify>,
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigSyncStoreSnapshot {
    pub objects: Vec<(ObjId, Option<serde_json::Value>)>,
    pub memberships: Vec<(PartId, ObjId, i64, i64)>,
    pub pending_memberships: Vec<(PartId, ObjId)>,
    pub part_cursors: Vec<(PartId, i64)>,
    pub peer_part_cursors: Vec<(PeerId, PartId, i64)>,
    pub keyhive_event_count: i64,
    pub keyhive_event_bytes: i64,
    pub local_cgka_secret_count: usize,
    pub local_prekey_secret_count: usize,
    pub sedimentree_item_count: i64,
    pub sedimentree_blob_bytes: i64,
}

impl std::ops::Deref for SqliteBigRepoStore {
    type Target = SqliteCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

#[cfg(feature = "test-support")]
impl SqliteBigRepoStore {
    /// Capture every convergence-relevant BigSync row plus protocol-volume
    /// counters. Intended for deterministic cross-node test diagnostics.
    pub async fn big_sync_store_snapshot(&self) -> Res<BigSyncStoreSnapshot> {
        let object_rows = sqlx::query!(
            "SELECT obj_id, payload_json FROM big_sync_objs
             WHERE scope_id = ?1 ORDER BY obj_id",
            self.scope_id
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let objects = object_rows
            .into_iter()
            .map(|row| {
                let mut payload = row
                    .payload_json
                    .map(|json| serde_json::from_str::<serde_json::Value>(&json))
                    .transpose()?;
                if let Some(heads) = payload
                    .as_mut()
                    .and_then(|value| value.get_mut("heads"))
                    .and_then(serde_json::Value::as_array_mut)
                {
                    heads.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
                }
                Ok((Self::obj_from_blob(row.obj_id), payload))
            })
            .collect::<Res<Vec<_>>>()?;

        let membership_rows = sqlx::query!(
            "SELECT p.part_id, o.obj_id, m.event_type, m.txid
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
             WHERE m.scope_id = ?1 AND m.maybe_part_ref > 0
             ORDER BY p.part_id, o.obj_id",
            self.scope_id
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let memberships = membership_rows
            .into_iter()
            .map(|row| {
                Ok((
                    Self::part_from_blob(row.part_id),
                    Self::obj_from_blob(row.obj_id),
                    row.event_type,
                    row.txid,
                ))
            })
            .collect::<Res<Vec<_>>>()?;

        let pending_rows = sqlx::query!(
            "SELECT p.part_id, o.obj_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
             WHERE m.scope_id = ?1 ORDER BY p.part_id, o.obj_id",
            self.scope_id
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let pending_memberships = pending_rows
            .into_iter()
            .map(|row| {
                Ok((
                    Self::part_from_blob(row.part_id),
                    Self::obj_from_blob(row.obj_id),
                ))
            })
            .collect::<Res<Vec<_>>>()?;

        let part_rows = sqlx::query!(
            "SELECT part_id, latest_cursor FROM big_sync_parts
             WHERE scope_id = ?1 ORDER BY part_id",
            self.scope_id
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let part_cursors = part_rows
            .into_iter()
            .map(|row| Ok((Self::part_from_blob(row.part_id), row.latest_cursor)))
            .collect::<Res<Vec<_>>>()?;

        let peer_rows = sqlx::query!(
            "SELECT peer_id, p.part_id, cursor
             FROM big_sync_peer_cursors c
             JOIN big_sync_parts p ON p.part_ref = c.part_ref
             WHERE c.scope_id = ?1 ORDER BY peer_id, p.part_id",
            self.scope_id
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let peer_part_cursors = peer_rows
            .into_iter()
            .map(|row| {
                Ok((
                    SqliteCore::peer_from_blob(row.peer_id),
                    Self::part_from_blob(row.part_id),
                    row.cursor,
                ))
            })
            .collect::<Res<Vec<_>>>()?;

        let volume = sqlx::query!(
            "SELECT
               (SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = ?1) AS kh_count,
               (SELECT COALESCE(SUM(length(event_bytes)), 0) FROM big_repo_keyhive_event_log WHERE scope_id = ?1) AS kh_bytes,
               ((SELECT COUNT(*) FROM big_repo_subduction_commits WHERE scope_id = ?1) +
                (SELECT COUNT(*) FROM big_repo_subduction_fragments WHERE scope_id = ?1)) AS sediment_count,
               ((SELECT COALESCE(SUM(length(blob)), 0) FROM big_repo_subduction_commits WHERE scope_id = ?1) +
                (SELECT COALESCE(SUM(length(blob)), 0) FROM big_repo_subduction_fragments WHERE scope_id = ?1)) AS sediment_bytes",
            self.scope_id
        )
        .fetch_one(&self.sql.read_pool)
        .await?;

        Ok(BigSyncStoreSnapshot {
            objects,
            memberships,
            pending_memberships,
            part_cursors,
            peer_part_cursors,
            keyhive_event_count: volume.kh_count,
            keyhive_event_bytes: volume.kh_bytes,
            local_cgka_secret_count: 0,
            local_prekey_secret_count: 0,
            sedimentree_item_count: volume.sediment_count,
            sedimentree_blob_bytes: volume.sediment_bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GroupPartReconciliation {
    pub(crate) doc: ObjId,
    pub(crate) agents: HashMap<PeerId, keyhive_core::access::Access>,
    pub(crate) managed_group_parts: HashSet<PartId>,
    pub(crate) desired_group_parts: HashSet<PartId>,
    pub(crate) desired_global: bool,
}

/// Durable record of keyhive event *incorporation*.
///
/// Unlike a raw event-log row (some of whose effects may
/// still be pending), a row here means the event's effects are applied to
/// the keyhive projection. Fed exclusively by the durable incorporation hook;
/// workers tail this instead of the raw arrival log.
#[derive(Debug, Clone)]
pub(crate) struct AdmissionEventRow {
    pub(crate) seq: u64,
    pub(crate) bytes: Vec<u8>,
    /// Hash of the admitted event, for classification without rehashing.
    pub(crate) event_hash: [u8; 32],
    /// Verifying key of the peer the events were learned from (`None` for
    /// locally created events).
    pub(crate) source_id: Option<Vec<u8>>,
}

/// Inline sink for durable Keyhive incorporation records.
#[derive(Clone)]
pub(crate) struct KeyhiveIncorporationSink {
    store: SqliteBigRepoStore,
    runtime_events: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    /// Dispatcher wake-up signal. The durable admission log is the source
    /// of truth; this signal only wakes the dispatcher to tail it.
    dispatcher_notify: std::sync::Weak<tokio::sync::Notify>,
}

impl KeyhiveIncorporationSink {
    pub(crate) fn new(
        store: SqliteBigRepoStore,
        runtime_events: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        dispatcher_notify: std::sync::Weak<tokio::sync::Notify>,
    ) -> Self {
        Self {
            store,
            runtime_events,
            dispatcher_notify,
        }
    }

    pub(crate) async fn append(
        &self,
        hashes: Vec<[u8; 32]>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> Res<()> {
        if hashes.is_empty() {
            return Ok(());
        }
        let hashes = hashes
            .into_iter()
            .map(subduction_keyhive::storage::StorageHash::new)
            .collect();
        let seq = self.store.append_admitted_events(hashes, source).await?;
        if let Err(error) = self
            .runtime_events
            .send(crate::runtime2::Runtime2Evt::KeyhiveAdmissionAdvanced { seq })
            .await
        {
            // The durable incorporation is already committed. Runtime events
            // are only wake-up hints, and a closed channel is expected during
            // shutdown; never turn that into a failed protocol exchange.
            tracing::debug!(
                ?error,
                seq,
                "runtime event channel closed after incorporation commit"
            );
        }
        Ok(())
    }
}

impl subduction_keyhive::DurableIncorporationSink<Sendable> for KeyhiveIncorporationSink {
    fn admit(
        self: Arc<Self>,
        hashes: Vec<[u8; 32]>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> <Sendable as future_form::FutureForm>::Future<
        'static,
        Result<(), subduction_keyhive::StorageError>,
    > {
        Sendable::from_future(async move {
            self.append(hashes.clone(), source.clone())
                .await
                .map_err(|error| subduction_keyhive::StorageError::Save(error.to_string()))?;
            if let Some(notify) = self.dispatcher_notify.upgrade() {
                notify.notify_one();
            }
            Ok(())
        })
    }

    fn unadmitted_wal_events(
        self: Arc<Self>,
    ) -> <Sendable as future_form::FutureForm>::Future<
        'static,
        Result<
            Vec<([u8; 32], Vec<u8>, Option<subduction_keyhive::KeyhivePeerId>)>,
            subduction_keyhive::StorageError,
        >,
    > {
        Sendable::from_future(async move {
            self.store
                .unadmitted_keyhive_events()
                .await
                .map_err(|error| subduction_keyhive::StorageError::Load(error.to_string()))
        })
    }
}

impl std::fmt::Debug for SqliteBigRepoStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteBigRepoStore")
            .finish_non_exhaustive()
    }
}
pub(crate) struct ScopeHandle<'a> {
    store: &'a SqliteBigRepoStore,
}

impl ScopeHandle<'_> {
    #[inline]
    pub(crate) fn id(&self) -> i64 {
        self.store.scope_id
    }
}

impl SqliteBigRepoStore {
    /// Return the scope-bound handle used by every store query.
    /// Keeping the scope token behind this handle makes omission visible in review
    /// and provides one place to evolve scoped query binding.
    pub(crate) fn scope(&self) -> ScopeHandle<'_> {
        ScopeHandle { store: self }
    }
    pub async fn new(sql: SqlCtx, scope_key: impl Into<Arc<str>>, bucket_depth: u8) -> Res<Self> {
        Self::new_with_config(sql, scope_key, bucket_depth, Default::default()).await
    }

    pub async fn new_with_config(
        sql: SqlCtx,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
        config: big_sync::HostPartStoreConfig,
    ) -> Res<Self> {
        SqliteCore::init_schema(&sql.write_pool, bucket_depth).await?;
        MIGRATOR.run(&sql.write_pool).await?;
        let core = SqliteCore::new(sql, scope_key, bucket_depth).await?;

        let store = Self {
            core,
            bus: default(),
            hidden_parts: Arc::new(config.hidden_parts),
            tree_cache: Arc::new(std::sync::Mutex::new(TreeCache::new(
                TREE_CACHE_METADATA_CAPACITY,
            ))),
            local_revision_wakeups: Arc::new(Notify::new()),
        };
        store.init_subduction_schema().await?;
        Ok(store)
    }

    async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        SqliteCore::next_cursor(tx).await
    }

    fn event_part_id(event: &SubEvent) -> Option<PartId> {
        match event {
            SubEvent::Changed(_) => None,
            SubEvent::Added(inner) => Some(inner.part_id),
            SubEvent::Removed(inner) => Some(inner.part_id),
            SubEvent::ReplayComplete => None,
        }
    }

    async fn publish(&self, events: Vec<SubEvent>) -> Res<()> {
        let mut promote = Vec::new();
        let mut dispatch = Vec::new();
        let mut recipients: HashMap<(Uuid, ObjId, CursorIndex, Option<PartId>), SubEvent> =
            HashMap::new();
        let mut push_recipient = |sub_id: Uuid, event: SubEvent| {
            let (obj_id, cursor, part_id) = match &event {
                SubEvent::Changed(inner) => (inner.obj_id, inner.cursor, None),
                SubEvent::Added(inner) => (inner.obj_id, inner.cursor, Some(inner.part_id)),
                SubEvent::Removed(inner) => (inner.obj_id, inner.cursor, Some(inner.part_id)),
                SubEvent::ReplayComplete => unreachable!(),
            };
            recipients
                .entry((sub_id, obj_id, cursor, part_id))
                .and_modify(|existing| {
                    if let (SubEvent::Changed(existing), SubEvent::Changed(new)) =
                        (existing, &event)
                    {
                        existing.part_ids.extend(new.part_ids.iter().copied());
                        existing.part_ids.sort_unstable();
                        existing.part_ids.dedup();
                        existing.payload = new.payload.clone();
                    }
                })
                .or_insert(event);
        };
        {
            let bus = self.bus.read().expect(ERROR_MUTEX);
            for event in events {
                let obj_id = match &event {
                    SubEvent::Changed(inner) => inner.obj_id,
                    SubEvent::Added(inner) => inner.obj_id,
                    SubEvent::Removed(inner) => inner.obj_id,
                    SubEvent::ReplayComplete => continue,
                };
                let object_event = match &event {
                    SubEvent::Changed(inner) => {
                        Some(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                            cursor: inner.cursor,
                            part_ids: Vec::new(),
                            obj_id: inner.obj_id,
                            payload: inner.payload.clone(),
                        }))
                    }
                    SubEvent::Added(inner) => {
                        Some(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                            cursor: inner.cursor,
                            part_ids: Vec::new(),
                            obj_id: inner.obj_id,
                            payload: inner.payload.clone(),
                        }))
                    }
                    SubEvent::Removed(_) | SubEvent::ReplayComplete => None,
                };
                match &event {
                    SubEvent::Changed(inner) => {
                        for part_id in &inner.part_ids {
                            if let Some(subs) = bus.by_part.get(part_id) {
                                for &sub_id in subs {
                                    let mut projected = event.clone();
                                    if let SubEvent::Changed(inner) = &mut projected {
                                        inner.part_ids = vec![*part_id];
                                    }
                                    push_recipient(sub_id, projected);
                                }
                            }
                        }
                    }
                    SubEvent::Added(inner) => {
                        if let Some(subs) = bus.by_part.get(&inner.part_id) {
                            for &sub_id in subs {
                                push_recipient(sub_id, event.clone());
                            }
                        }
                    }
                    SubEvent::Removed(inner) => {
                        if let Some(subs) = bus.by_part.get(&inner.part_id) {
                            for &sub_id in subs {
                                push_recipient(sub_id, event.clone());
                            }
                        }
                    }
                    SubEvent::ReplayComplete => unreachable!(),
                }
                if let Some(object_event) = object_event
                    && let Some(subs) = bus.by_obj.get(&obj_id)
                {
                    for &sub_id in subs {
                        if matches!(&event, SubEvent::Added(inner)
                            if bus.parts_by_sub
                                .get(&sub_id)
                                .is_some_and(|parts| parts.contains(&inner.part_id)))
                        {
                            continue;
                        }
                        push_recipient(sub_id, object_event.clone());
                    }
                }
            }
            for ((sub_id, obj_id, _, _), event) in recipients {
                let Some(sub) = bus.subs.get(&sub_id) else {
                    continue;
                };
                if bus.pending.contains(&sub_id) {
                    if sub.pending.mark_dirty() {
                        promote.push((sub_id, event, obj_id, sub.principal, sub.sender.clone()));
                    }
                    continue;
                }
                if !bus.live.contains(&sub_id) {
                    continue;
                }
                dispatch.push((sub_id, event, obj_id, sub.principal, sub.sender.clone()));
            }
        }

        let mut drop_subs = HashSet::new();
        for (sub_id, event, obj_id, principal, sender) in dispatch {
            // A policy-check failure must not masquerade as a denial: that
            // would silently drop a deliverable event from a live subscriber.
            let permitted = matches!(event, SubEvent::Removed(_))
                || self
                    .is_event_permitted(Self::event_part_id(&event), obj_id, principal)
                    .await?;
            if permitted && sender.try_send(event).is_err() {
                drop_subs.insert(sub_id);
            }
        }

        for (sub_id, event, obj_id, principal, sender) in promote {
            let permitted = matches!(event, SubEvent::Removed(_))
                || self
                    .is_event_permitted(Self::event_part_id(&event), obj_id, principal)
                    .await?;
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
            if permitted && sender.try_send(event).is_err() {
                bus.remove(sub_id);
            }
        }

        if !drop_subs.is_empty() {
            let mut bus = self.bus.write().expect(ERROR_MUTEX);
            for sub_id in drop_subs {
                bus.remove(sub_id);
            }
        }
        self.local_revision_wakeups.notify_waiters();
        Ok(())
    }

    pub(crate) async fn is_event_permitted(
        &self,
        part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> Res<bool> {
        let Some(peer) = principal else {
            return Ok(true);
        };
        let peer_blob = Self::peer_blob(peer);
        let access_level: Option<i64> = sqlx::query_scalar!(
            "SELECT access_level
             FROM big_sync_syncable
             WHERE scope_id = ?1 AND obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             ) AND principal_id = ?3",
            self.scope_id,
            Self::obj_blob(obj_id),
            &peer_blob
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        let permitted = access_level
            .map(|lvl| u8::try_from(lvl).expect(ERROR_IMPOSSIBLE))
            .map(big_sync::sqlite_core::decode_access)
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
