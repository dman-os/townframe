use crate::interlude::*;

use big_sync::open_sqlite_revision_reader;
use big_sync::sqlite_core::{EVENT_CHANGED, EVENT_REMOVED, MemberState, SqliteCore, encode_access};
use big_sync::{HostPartStore, PartScope, PartStoreStats, ReadTarget};
use big_sync_core::part_store::{CursorIndex, ObjPayload, PartDirtyCount};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary,
    SubEvent, SubPartsRequest, SubscriptionTarget,
};
use big_sync_core::{BuckId, ByteKey, Fingerprint, mpsc};
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
mod secret_blobs;
pub(crate) use secret_blobs::SecretBlobKind;
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


/// One node's Keyhive ingestion ledger.
///
/// `logged` counts events that reached the raw event log, `admitted` those
/// Keyhive applied into the graph. `unapplied_by_source` keys the
/// logged-but-unapplied remainder by the peer that delivered it (`None` for
/// locally authored events).
#[derive(Debug, Default, Clone)]
pub(crate) struct KeyhiveEventLedger {
    pub(crate) logged: u64,
    pub(crate) admitted: u64,
    pub(crate) admission_head: u64,
    pub(crate) unapplied_by_source: std::collections::BTreeMap<Option<Vec<u8>>, u64>,
}

#[derive(Clone)]
pub struct SqliteBigRepoStore {
    core: SqliteCore,
    hidden_parts: Arc<HashSet<PartKey>>,
    /// Transaction-scoped sedimentree projection cache (see [`TreeCache`]).
    tree_cache: Arc<std::sync::Mutex<TreeCache>>,
    local_revision_wakeups: Arc<Notify>,
    /// Last admission-log seq committed to this scope's store, bumped inside
    /// [`append_admitted_events`](Self::append_admitted_events) — the single
    /// durable admission choke point. A keyhive sync completion stamps this
    /// into its event so the hub can enforce, without a DB round-trip, that the
    /// round's admissions are reflected in its own `admitted_head` before it
    /// resolves waiters.
    ///
    /// Zero until the first append in this process, matching the hub's
    /// `admitted_head` initialization, so a completion that admitted nothing
    /// new still resolves immediately.
    admission_watermark: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigSyncStoreSnapshot {
    pub objects: Vec<(ObjKey, Option<serde_json::Value>)>,
    pub memberships: Vec<(PartKey, ObjKey, i64, i64)>,
    pub pending_memberships: Vec<(PartKey, ObjKey)>,
    pub part_cursors: Vec<(PartKey, i64)>,
    pub peer_part_cursors: Vec<(PeerKey, PartKey, i64)>,
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
    pub(crate) doc: ObjKey,
    /// The doc-level union of [`Self::part_agents`]: used by the grant re-emit. Never
    /// written to a part row.
    pub(crate) agents: HashMap<PeerKey, keyhive_core::access::Access>,
    /// The agent set of each part the doc resides in — these are the access rows that
    /// get written, so a principal of one group never receives another group's part.
    pub(crate) part_agents: HashMap<PartKey, Arc<HashMap<PeerKey, keyhive_core::access::Access>>>,
    /// The parts this reconciliation manages: a part the doc is currently in that is
    /// managed but not desired is removed. `/seds` is in here like any other part
    /// (decision 9).
    pub(crate) managed_group_parts: HashSet<PartKey>,
    pub(crate) desired_group_parts: HashSet<PartKey>,
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
            hidden_parts: Arc::new(config.hidden_parts),
            tree_cache: Arc::new(std::sync::Mutex::new(TreeCache::new(
                TREE_CACHE_METADATA_CAPACITY,
            ))),
            local_revision_wakeups: Arc::new(Notify::new()),
            admission_watermark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        store.init_subduction_schema().await?;
        Ok(store)
    }

    async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        SqliteCore::next_cursor(tx).await
    }

    fn event_scope(event: &SubEvent) -> PartScope {
        match event {
            SubEvent::Changed(inner) => match inner.part_ids.as_slice() {
                [] => PartScope::FromObject,
                [part] => PartScope::Part(part.clone()),
                parts => PartScope::AnyOf(parts.to_vec()),
            },
            SubEvent::Removed(inner) => PartScope::Part(inner.part_id.clone()),
            SubEvent::ReplayComplete => PartScope::FromObject,
        }
    }

    fn event_kind(event: &SubEvent) -> &'static str {
        match event {
            SubEvent::Changed(_) => "changed",
            SubEvent::Removed(_) => "removed",
            SubEvent::ReplayComplete => "replay_complete",
        }
    }

    /// Payload-free description of an event, for diagnostics that must not
    /// spill object content into logs.
    fn event_diagnostic(event: &SubEvent) -> (CursorIndex, Vec<PartKey>) {
        match event {
            SubEvent::Changed(inner) => (inner.cursor, inner.part_ids.clone()),
            SubEvent::Removed(inner) => (inner.cursor, vec![inner.part_id.clone()]),
            SubEvent::ReplayComplete => (CursorIndex::default(), Vec::new()),
        }
    }

    async fn publish(&self, events: Vec<SubEvent>) -> Res<()> {
        // Events reach readers through the durable frontier: the write that produced
        // them bumped the scope's revision, and this wake is what tells a waiting
        // reader to look again. Nothing is routed and nobody is registered — a reader
        // holds no subscription — so the only thing a publish owes anyone is the
        // signal itself.
        for event in events {
            if !matches!(event, SubEvent::ReplayComplete) {
                let (cursor, part_ids) = Self::event_diagnostic(&event);
                tracing::debug!(
                    ?cursor,
                    ?part_ids,
                    event_kind = Self::event_kind(&event),
                    "part-store published event",
                );
            }
        }
        self.local_revision_wakeups.notify_waiters();
        Ok(())
    }

    /// Filter an outbound event's candidate parts down to the parts `principal` may read;
    /// `None` when unfiltered (trusted local subscriber). See
    /// [`big_sync::PartScope`] — authorization and non-exposure are the same operation.
    pub(crate) async fn permitted_parts(
        &self,
        scope: PartScope,
        obj_id: ObjKey,
        principal: Option<PeerKey>,
    ) -> Res<Option<Vec<PartKey>>> {
        let Some(ref peer) = principal else {
            return Ok(None);
        };
        let peer_blob = Self::peer_blob(peer.clone());
        let candidates: Vec<PartKey> = match scope {
            // Resolve and filter in one query: the object's live parts that grant this
            // principal access. An event that named nothing usable still delivers when
            // the principal can read some part of it.
            PartScope::FromObject => {
                let readable = self.readable_parts_of_object(&obj_id, &peer_blob).await?;
                tracing::trace!(
                    ?obj_id,
                    ?principal,
                    part_count = readable.len(),
                    "policy event permission",
                );
                return Ok(Some(readable));
            }
            PartScope::Part(part_id) => vec![part_id],
            PartScope::AnyOf(part_ids) => part_ids,
        };
        let mut readable = Vec::with_capacity(candidates.len());
        for part_id in candidates {
            let access_level: Option<i64> = match self.core.find_part_ref(part_id.clone()).await? {
                Some(part_ref) => {
                    sqlx::query_scalar!(
                        "SELECT access_level
                         FROM big_sync_syncable
                         WHERE scope_id = ?1 AND part_ref = ?2 AND principal_id = ?3",
                        self.scope_id,
                        part_ref,
                        &peer_blob
                    )
                    .fetch_optional(&self.sql.read_pool)
                    .await?
                }
                None => None,
            };
            if access_level.is_some_and(is_fetch_access) {
                readable.push(part_id);
            }
        }
        tracing::trace!(
            ?obj_id,
            ?principal,
            part_count = readable.len(),
            "policy event permission",
        );
        Ok(Some(readable))
    }

    /// The parts containing `obj_id` that `peer_blob` may fetch-read, in key order.
    ///
    /// This is the `FromObject` resolution at the heart of decision 2: an event or a
    /// subscription that names the object rather than a part resolves through the object's
    /// real parts, which are the only ones an access row can name.
    async fn readable_parts_of_object(
        &self,
        obj_id: &ObjKey,
        peer_blob: &[u8],
    ) -> Res<Vec<PartKey>> {
        let rows = sqlx::query!(
            "SELECT p.part_id AS 'part_id: Vec<u8>', s.access_level
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             JOIN big_sync_syncable s ON s.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1
               AND m.obj_ref = (
                   SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
               )
               AND m.maybe_part_ref > 0
               AND m.event_type != 2
               AND s.principal_id = ?3
             ORDER BY p.part_id",
            self.scope_id,
            Self::obj_blob(obj_id.clone()),
            peer_blob
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter(|row| is_fetch_access(row.access_level))
            .map(|row| Self::part_from_blob(row.part_id))
            .collect())
    }
}

fn is_fetch_access(access_level: i64) -> bool {
    u8::try_from(access_level)
        .ok()
        .map(big_sync::sqlite_core::decode_access)
        .is_some_and(|access| access.is_fetcher())
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
