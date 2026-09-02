use crate::interlude::*;

use big_sync_core::keyed_frontier::{FrontierRead, FrontierRevision, KeyedFrontierReader};
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits};
use big_sync_core::rpc::{
    BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, ObjChanged, ObjRemovedFromPart, PartEvent, PartPage,
    PartSummary, SubEvent, SubPartsRequest,
};
use big_sync_core::{BuckId, Byte32Id, ObjId, PartId, PeerId, mpsc};

/// The logical object and part routes represented by the part-store frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PartFrontierKey {
    Object(ObjId),
    Part { obj_id: ObjId, part_id: PartId },
}

pub(crate) use sqlite_frontier::SqlitePartFrontier;
pub(crate) use sqlite_read::SqlitePartSelector;

mod sqlite_frontier;
mod sqlite_read;
mod sqlite_write;

pub mod memory;
pub mod sqlite;
pub mod sqlite_core;

/// Local, already-authorized revision stream for part-store consumers.
#[async_trait]
pub trait LocalPartRevisionReader: Send {
    async fn next(&mut self) -> Res<RevisionRead<FrontierRevision, SubEvent>>;
}

pub(crate) struct PartRevisionReader {
    inner: Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent>>,
    objects: HashSet<ObjId>,
    parts: HashSet<PartId>,
    pending: std::collections::VecDeque<RevisionRead<FrontierRevision, SubEvent>>,
    pending_replay_complete: Option<FrontierRevision>,
    last_revision: FrontierRevision,
    replay_complete_seen: bool,
}

impl PartRevisionReader {
    pub(crate) fn new(
        inner: Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent>>,
        objects: HashSet<ObjId>,
        parts: HashSet<PartId>,
    ) -> Self {
        Self {
            inner,
            objects,
            parts,
            pending: std::collections::VecDeque::new(),
            pending_replay_complete: None,
            last_revision: 0,
            replay_complete_seen: false,
        }
    }

    fn project(
        &self,
        key: PartFrontierKey,
        value: Option<PartEvent>,
        revision: FrontierRevision,
    ) -> Option<SubEvent> {
        match (key, value) {
            (PartFrontierKey::Object(_), None) => None,
            (PartFrontierKey::Object(_), Some(PartEvent::Changed(mut event))) => {
                event.cursor = revision;
                Some(SubEvent::Changed(event))
            }
            (PartFrontierKey::Part { obj_id, part_id }, value)
                if self.objects.contains(&obj_id) && !self.parts.contains(&part_id) =>
            {
                let payload = match value {
                    Some(PartEvent::Added(event)) => event.payload,
                    Some(PartEvent::Changed(event)) => event.payload,
                    Some(PartEvent::Removed(_)) | None => serde_json::Value::Null,
                };
                Some(SubEvent::Changed(ObjChanged {
                    cursor: revision,
                    part_ids: Vec::new(),
                    obj_id,
                    payload,
                }))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Added(mut event)))
                if self.parts.contains(&part_id) =>
            {
                event.cursor = revision;
                event.obj_id = obj_id;
                event.part_id = part_id;
                Some(SubEvent::Added(event))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Changed(mut event)))
                if self.parts.contains(&part_id) =>
            {
                event.cursor = revision;
                event.obj_id = obj_id;
                event.part_ids = vec![part_id];
                Some(SubEvent::Changed(event))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Removed(_)) | None)
                if self.parts.contains(&part_id) =>
            {
                Some(SubEvent::Removed(ObjRemovedFromPart {
                    cursor: revision,
                    part_id,
                    obj_id,
                }))
            }
            _ => None,
        }
    }

    fn merge_changed(events: &mut Vec<SubEvent>, event: SubEvent) {
        if let SubEvent::Changed(changed) = &event
            && let Some(SubEvent::Changed(existing)) = events.iter_mut().find(|candidate| {
                matches!(candidate, SubEvent::Changed(candidate)
                    if candidate.cursor == changed.cursor && candidate.obj_id == changed.obj_id)
            })
        {
            for part_id in &changed.part_ids {
                if !existing.part_ids.contains(part_id) {
                    existing.part_ids.push(*part_id);
                }
            }
            existing.part_ids.sort_unstable();
            existing.payload = changed.payload.clone();
        } else {
            events.push(event);
        }
    }
}

#[async_trait]
impl LocalPartRevisionReader for PartRevisionReader {
    async fn next(&mut self) -> Res<RevisionRead<FrontierRevision, SubEvent>> {
        if let Some(read) = self.pending.pop_front() {
            return Ok(read);
        }
        if let Some(through) = self.pending_replay_complete.take() {
            return Ok(RevisionRead::ReplayComplete { through });
        }
        match self.inner.next().await.map_err(|error| ferr!("{error}"))? {
            FrontierRead::ReplayComplete { through } => {
                if self.replay_complete_seen {
                    return Err(ferr!("frontier emitted ReplayComplete twice"));
                }
                self.replay_complete_seen = true;
                if through > self.last_revision {
                    self.last_revision = through;
                    self.pending_replay_complete = Some(through);
                    return Ok(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                Ok(RevisionRead::ReplayComplete { through })
            }
            FrontierRead::Entries { entries, through } => {
                self.last_revision = self.last_revision.max(through);
                let mut grouped = BTreeMap::<FrontierRevision, Vec<SubEvent>>::new();
                for entry in entries {
                    if let Some(event) = self.project(entry.key, entry.value, entry.revision) {
                        Self::merge_changed(grouped.entry(entry.revision).or_default(), event);
                    }
                }
                if grouped.is_empty() {
                    return Ok(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                let last = *grouped.keys().next_back().expect(ERROR_IMPOSSIBLE);
                self.pending.extend(
                    grouped
                        .into_iter()
                        .map(|(revision, entries)| RevisionRead::Entries { revision, entries }),
                );
                if through > last {
                    self.pending.push_back(RevisionRead::Entries {
                        revision: through,
                        entries: Vec::new(),
                    });
                }
                Ok(self.pending.pop_front().expect(ERROR_IMPOSSIBLE))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostPartStoreConfig {
    /// Parts that remain physically present but are invisible to remote part access.
    pub hidden_parts: HashSet<PartId>,
    pub debounce_quiet_window: std::time::Duration,
    pub debounce_max_latency: std::time::Duration,
}

impl Default for HostPartStoreConfig {
    fn default() -> Self {
        Self {
            hidden_parts: HashSet::new(),
            debounce_quiet_window: std::time::Duration::from_millis(50),
            debounce_max_latency: std::time::Duration::from_millis(500),
        }
    }
}

// pub type ObjStoreLease = u64;

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum StoreMutationOutcome {
//     Applied,
//     Stale,
// }

#[async_trait]
pub trait HostPartStore: Send + Sync {
    async fn latest_revision(&self) -> Res<CursorIndex>;
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>>;
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>>;
    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>>;
    async fn member_count(&self, part_id: PartId) -> Res<u64>;
    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary>;

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>>;
    async fn obj_exists(&self, obj_id: ObjId) -> Res<bool>;

    // NOTE: upsert_obj doesn't take/invalidate leases since
    // it doesn't affect part membership
    async fn set_obj_payload(&self, obj_id: ObjId, payload: ObjPayload) -> Res<()>;

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>>;

    // async fn get_obj_lease(&self, obj_id: ObjId) -> Res<ObjStoreLease>;

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()>;

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()>;

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()>;

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex>;

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>>;
    async fn list_events_with_policy(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
        enforce_policy: bool,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        if enforce_policy {
            return Err(ferr!("policy enforcement not supported on this store"));
        }
        self.list_events(parts, cursor, limit).await
    }

    /// Subscribe to events for the given parts, filtering events for
    /// the given `subscriber` (ed25519 verifying key bytes).
    /// Events for documents the subscriber cannot fetch are silently dropped.
    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
        subscriber: PeerId,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>>;
    /// Subscribe a trusted local consumer without remote authorization or
    /// hidden-part filtering. This method is intentionally not exposed by RPC.
    /// Stores that do not provide a local mirror return an error.
    async fn subscribe_local(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        let mut reader = self
            .open_local_revision_reader(reqs, RevisionReadLimits::default())
            .await??;
        let (tx, rx) = mpsc::unbounded("HostPartStore".into(), "local-revision-reader".into());
        tokio::spawn(async move {
            loop {
                match reader.next().await.expect(ERROR_IMPOSSIBLE) {
                    RevisionRead::Entries { entries, .. } => {
                        for event in entries {
                            if tx.send(event).await.is_err() {
                                return;
                            }
                        }
                    }
                    RevisionRead::ReplayComplete { .. } => {
                        if tx.send(SubEvent::ReplayComplete).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Ok(rx))
    }

    /// Open a trusted local revision reader. This boundary intentionally has
    /// no remote authorization or hidden-part filtering.
    async fn open_local_revision_reader(
        &self,
        _reqs: SubPartsRequest,
        _limits: RevisionReadLimits,
    ) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
        Err(ferr!("local revision reader is not available"))
    }
    async fn ensure_part(&self, part_id: PartId) -> Res<()>;

    /// Set the agents who have access to `obj` and their [`Access`] level.
    /// The store's [`ObjAccessPolicy`] uses this to determine fetchability.
    async fn set_obj_members(
        &self,
        obj: ObjId,
        agents: HashMap<PeerId, keyhive_core::access::Access>,
    ) -> Res<()>;

    /// Add a single member to `obj` with the given [`Access`] level.
    async fn add_obj_member(
        &self,
        obj: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) -> Res<()>;

    /// Remove a single member from `obj`.
    async fn remove_obj_member(&self, obj: ObjId, member: PeerId) -> Res<()>;

    /// Whether `principal` may receive events for `obj_id`.
    ///
    /// - `principal == None` (trusted local subscriber): always permitted.
    /// - `part_id` is the part the event belongs to when known.
    async fn is_event_permitted(
        &self,
        _part_id: Option<PartId>,
        _obj_id: ObjId,
        _principal: Option<PeerId>,
    ) -> Res<bool> {
        Ok(true)
    }
}

pub(crate) fn obj_id_bounds_for_bucket(bucket_id: BuckId) -> (ObjId, Option<ObjId>) {
    let level = bucket_id.level();
    let prefix_bits = u32::from(level) * u32::from(BuckId::BITS_PER_LEVEL);
    debug_assert!(prefix_bits <= u16::BITS);

    if prefix_bits == 0 {
        return (ObjId(Byte32Id::new([0; 32])), None);
    }

    let shift = u16::BITS - prefix_bits;
    let start_prefix = (u32::from(bucket_id.index())) << shift;
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
    let end = Some({
        let mut bytes = [0; 32];
        bytes[..2].copy_from_slice(&(next_prefix as u16).to_be_bytes());
        ObjId(Byte32Id::new(bytes))
    });
    (start, end)
}

#[cfg(any(test, feature = "test-support"))]
#[cfg_attr(not(test), allow(dead_code))]
pub mod contract {
    use super::*;
    use big_sync_core::rpc::{
        BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED, BucketSummary, GetChangedBucketsRequest,
        LeafBucketRequest, LeafBucketsRequest,
    };
    use big_sync_core::{Fingerprint, FingerprintSeed};
    use std::collections::BTreeSet;

    // pub async fn assert_scoped_obj_id_distribution<R>(
    //     resolver: &R,
    //     objs: &[ScopedObjRef],
    // ) -> Res<()>
    // where
    //     R: ScopedIdResolver + Sync,
    // {
    //     assert!(
    //         objs.len() >= 32,
    //         "need enough objects to exercise object-id distribution"
    //     );
    //
    //     let mut obj_ids = Vec::with_capacity(objs.len());
    //     for obj in objs {
    //         let first = resolver.resolve_obj(obj).await?;
    //         let second = resolver.resolve_obj(obj).await?;
    //         assert_eq!(first, second, "resolve_obj must be stable for {obj:?}");
    //         obj_ids.push(first);
    //     }
    //
    //     let unique_ids: BTreeSet<_> = obj_ids.iter().copied().collect();
    //     assert_eq!(
    //         unique_ids.len(),
    //         obj_ids.len(),
    //         "resolve_obj must not collapse distinct scoped objects onto the same obj id"
    //     );
    //
    //     let unique_leaf_buckets: BTreeSet<_> = obj_ids
    //         .iter()
    //         .map(|obj_id| BuckId::from_obj_id(BuckId::MAX_LEVEL, obj_id))
    //         .collect();
    //     assert!(
    //         unique_leaf_buckets.len() >= 8,
    //         "object ids are too clustered across leaf buckets"
    //     );
    //     Ok(())
    // }

    async fn expected_bucket_summary<S>(
        store: &S,
        live_ids: &BTreeSet<ObjId>,
        dead_ids: &BTreeSet<ObjId>,
    ) -> Res<BucketSummary>
    where
        S: HostPartStore + Sync,
    {
        let mut live_fp = 0u64;
        let mut dead_fp = 0u64;
        let mut live_count = 0u32;
        let mut dead_count = 0u32;

        let root = BuckId::ROOT;
        for obj_id in live_ids {
            let payload = store
                .obj_payload(*obj_id)
                .await?
                .expect("live object must have payload");
            live_fp = live_fp.wrapping_add(
                Fingerprint::new(
                    &BUCKET_LIVE_FP_SEED,
                    &("big-sync-bucket-live-v1", root, *obj_id, payload),
                )
                .as_u64(),
            );
            live_count = live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        }
        for obj_id in dead_ids {
            assert!(
                !live_ids.contains(obj_id),
                "live and dead object sets must be disjoint"
            );
            assert!(
                store.obj_payload(*obj_id).await?.is_none(),
                "dead object must not have payload"
            );
            dead_fp = dead_fp.wrapping_add(
                Fingerprint::new(
                    &BUCKET_DEAD_FP_SEED,
                    &("big-sync-bucket-dead-v1", root, *obj_id),
                )
                .as_u64(),
            );
            dead_count = dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        }

        Ok(BucketSummary {
            id: root,
            len: live_count + dead_count,
            live_count,
            fp: (live_fp, dead_fp),
            changed_at: 0,
        })
    }

    pub async fn assert_root_bucket_summary<S>(
        store: &S,

        part_id: PartId,
        live_ids: &[ObjId],
        dead_ids: &[ObjId],
    ) -> Res<()>
    where
        S: HostPartStore + Sync,
    {
        assert_eq!(
            live_ids.len(),
            live_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "live object set contains duplicates"
        );
        assert_eq!(
            dead_ids.len(),
            dead_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "dead object set contains duplicates"
        );
        let live_ids: BTreeSet<_> = live_ids.iter().copied().collect();
        let dead_ids: BTreeSet<_> = dead_ids.iter().copied().collect();
        let expected = expected_bucket_summary(store, &live_ids, &dead_ids).await?;

        assert_eq!(
            store.member_count(part_id).await?,
            u64::from(expected.live_count)
        );

        let direct = store.get_bucket_summary(part_id, BuckId::ROOT).await?;
        assert_eq!(direct.id, BuckId::ROOT);
        assert_eq!(direct.len, expected.len);
        assert_eq!(direct.live_count, expected.live_count);
        assert_eq!(direct.fp, expected.fp);

        let changed = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id,
                offset: BuckId::ROOT,
                since: 0,
                limit_hint: 1,
            })
            .await?;
        let changed = changed.expect(ERROR_IMPOSSIBLE);
        if expected.len == 0 {
            assert!(changed.is_empty());
        } else {
            assert_eq!(changed.len(), 1);
            assert_eq!(changed[0].id, BuckId::ROOT);
            assert_eq!(changed[0].len, expected.len);
            assert_eq!(changed[0].live_count, expected.live_count);
            assert_eq!(changed[0].fp, expected.fp);
            assert_eq!(changed[0].changed_at, direct.changed_at);
        }

        Ok(())
    }

    pub async fn assert_root_leaf_pagination<S>(
        store: &S,

        part_id: PartId,
        seed: FingerprintSeed,
        live_ids: &[ObjId],
        dead_ids: &[ObjId],
        limit_hint: u32,
    ) -> Res<()>
    where
        S: HostPartStore + Sync,
    {
        assert_eq!(
            live_ids.len(),
            live_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "live object set contains duplicates"
        );
        assert_eq!(
            dead_ids.len(),
            dead_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "dead object set contains duplicates"
        );
        let live_ids: BTreeSet<_> = live_ids.iter().copied().collect();
        let dead_ids: BTreeSet<_> = dead_ids.iter().copied().collect();
        assert!(
            live_ids.is_disjoint(&dead_ids),
            "live and dead object sets must be disjoint"
        );

        let expected: Vec<_> = live_ids.union(&dead_ids).copied().collect();
        let limit_hint = limit_hint.max(1);
        let mut seen = BTreeSet::new();
        let mut after = None;

        loop {
            let result = store
                .leaf_buckets(LeafBucketsRequest {
                    part_id,
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after,
                    }],
                    seed,
                    limit_hint,
                })
                .await?;
            let result = result.expect(ERROR_IMPOSSIBLE);
            assert_eq!(result.seed, seed);
            assert_eq!(result.bucks.len(), 1);

            let page = result.bucks.get(&BuckId::ROOT).expect(ERROR_IMPOSSIBLE);
            assert!(
                page.entries
                    .windows(2)
                    .all(|pair| pair[0].obj_id < pair[1].obj_id)
            );
            assert!(page.entries.len() <= limit_hint as usize);

            if page.entries.is_empty() {
                assert!(page.done);
                assert!(page.next_after.is_none());
                break;
            }

            let last_obj_id = page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id;
            if page.done {
                assert!(page.next_after.is_none());
            } else {
                assert_eq!(page.next_after, Some(last_obj_id));
                assert_eq!(page.entries.len(), limit_hint as usize);
            }

            for entry in &page.entries {
                assert!(
                    seen.insert(entry.obj_id),
                    "duplicate leaf entry {}",
                    entry.obj_id
                );
                assert_eq!(entry.dead, dead_ids.contains(&entry.obj_id));
                let expected_fp = if entry.dead {
                    Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", entry.obj_id, serde_json::Value::Null),
                    )
                } else {
                    let payload = store
                        .obj_payload(entry.obj_id)
                        .await?
                        .expect("live object must have payload");
                    Fingerprint::new(&seed, &("big-sync-obj-fp-v1", entry.obj_id, payload))
                };
                assert_eq!(entry.fp, expected_fp);
            }

            if page.done {
                break;
            }
            after = page.next_after;
        }

        assert_eq!(seen, expected.into_iter().collect());
        Ok(())
    }

    pub async fn assert_root_bucket_contract<S>(
        store: &S,

        part_id: PartId,
        seed: FingerprintSeed,
        live_ids: &[ObjId],
        dead_ids: &[ObjId],
        limit_hint: u32,
    ) -> Res<()>
    where
        S: HostPartStore + Sync,
    {
        assert_root_bucket_summary(store, part_id, live_ids, dead_ids).await?;
        assert_root_leaf_pagination(store, part_id, seed, live_ids, dead_ids, limit_hint).await?;
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod host_contract {
    use super::*;
    use big_sync_core::rpc::{
        BUCKET_LIVE_FP_SEED, BucketObjPageEntry, BucketSummary, LeafBucketPage, LeafBucketRequest,
        LeafBucketsRequest, ListPartsError, PartEvent, PartPage, SubEvent, SubPartsRequest,
    };
    use big_sync_core::{Fingerprint, FingerprintSeed};
    use keyhive_core::access::Access;
    use tokio::time::{Duration, timeout};

    #[async_trait]
    pub trait HostPartStoreContractHarness {
        fn store(&self) -> &dyn HostPartStore;
    }

    fn test_part(seed: u8) -> PartId {
        PartId(Byte32Id::new([seed; 32]))
    }

    fn test_obj(seed: u8) -> ObjId {
        let mut bytes = [0; 32];
        bytes[0] = seed;
        ObjId(Byte32Id::new(bytes))
    }

    fn payload(tag: &'static str, idx: u64) -> ObjPayload {
        serde_json::json!({
            "tag": tag,
            "idx": idx,
        })
    }

    fn obj_in_bucket(bucket_id: BuckId, salt: u8) -> ObjId {
        let (start, _) = super::obj_id_bounds_for_bucket(bucket_id);
        let mut bytes = start.0.into_bytes();
        bytes[31] = salt;
        ObjId(Byte32Id::new(bytes))
    }

    async fn seed_live_obj<S>(
        store: &S,
        obj_id: ObjId,
        payload: ObjPayload,
        parts: &[PartId],
    ) -> Res<()>
    where
        S: HostPartStore + Sync + ?Sized,
    {
        store.set_obj_payload(obj_id, payload.clone()).await?;
        assert_eq!(store.obj_payload(obj_id).await?, Some(payload.clone()));
        if !parts.is_empty() {
            store.add_obj_to_parts(obj_id, parts.to_vec()).await?;
        }
        Ok(())
    }

    fn assert_added(
        event: &PartEvent,
        cursor: CursorIndex,
        part_id: PartId,
        obj_id: ObjId,
        payload: ObjPayload,
    ) {
        let PartEvent::Added(transition) = event else {
            panic!("expected added event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
        assert_eq!(transition.payload, payload);
    }

    async fn recv_sub_event(rx: &big_sync_core::mpsc::Receiver<SubEvent>) -> Res<SubEvent> {
        Ok(timeout(Duration::from_secs(5), rx.recv()).await??)
    }

    async fn collect_sub_events(
        rx: &big_sync_core::mpsc::Receiver<SubEvent>,
    ) -> Res<Vec<SubEvent>> {
        let mut out = Vec::new();
        loop {
            let evt = recv_sub_event(rx).await?;
            let done = matches!(evt, SubEvent::ReplayComplete);
            out.push(evt);
            if done {
                break;
            }
        }
        Ok(out)
    }

    pub async fn assert_host_part_store_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        assert_summarize_parts_contract(harness).await?;
        assert_payload_can_trail_membership_contract(harness).await?;
        assert_changed_buckets_contract(harness).await?;
        assert_leaf_buckets_contract(harness).await?;
        assert_list_events_contract(harness).await?;
        assert_subscribe_contract(harness).await?;
        assert_readable_subscribe_contract(harness).await?;
        assert_subscribe_replay_filtering_contract(harness).await?;
        assert_subscription_semantics_contract(harness).await?;
        assert_subscribe_live_filtering_contract(harness).await?;
        assert_subscribe_per_part_cursor_contract(harness).await?;
        assert_list_events_pagination_contract(harness).await?;
        assert_peer_cursor_monotonicity_contract(harness).await?;
        assert_obj_occupancy_contract(harness).await?;
        assert_remove_obj_advances_latest_cursor_contract(harness).await?;
        assert_list_events_next_cursor_exactness_contract(harness).await?;
        assert_local_revision_reader_contract(harness).await?;
        Ok(())
    }

    pub async fn assert_local_revision_reader_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part_a = test_part(201);
        let part_b = test_part(202);
        let obj = test_obj(203);
        store.ensure_part(part_a).await?;
        store.ensure_part(part_b).await?;
        seed_live_obj(store, obj, payload("revision-1", 1), &[]).await?;
        store.add_obj_to_parts(obj, vec![part_a, part_b]).await?;
        store.set_obj_payload(obj, payload("revision-2", 2)).await?;

        let mut reader = store
            .open_local_revision_reader(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Object { obj_id: obj },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part_a,
                            cursor: 0,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part_b,
                            cursor: 0,
                        },
                    ]),
                },
                RevisionReadLimits { max_entries: 1 },
            )
            .await??;
        let mut last_revision = 0;
        let mut grouped_revision = None;
        let replay_through = loop {
            match reader.next().await? {
                RevisionRead::Entries { revision, entries } => {
                    assert!(
                        revision > last_revision,
                        "local revisions must strictly increase: previous={last_revision}, next={revision}"
                    );
                    last_revision = revision;
                    if let Some(changed) = entries.iter().find_map(|entry| match entry {
                        SubEvent::Changed(changed)
                            if changed.obj_id == obj
                                && changed.part_ids == vec![part_a, part_b] =>
                        {
                            Some(changed)
                        }
                        _ => None,
                    }) {
                        assert_eq!(entries.len(), 1, "same-revision changes must be grouped");
                        grouped_revision = Some(revision);
                        assert_eq!(changed.obj_id, obj);
                        assert_eq!(changed.part_ids, vec![part_a, part_b]);
                    }
                }
                RevisionRead::ReplayComplete { through } => {
                    assert!(through >= last_revision);
                    break through;
                }
            }
        };
        let grouped_revision = grouped_revision.expect("replay must contain grouped change");

        store.remove_obj_from_part(obj, part_a).await?;
        let removed_revision = match reader.next().await? {
            RevisionRead::Entries { revision, entries } => {
                assert!(revision > replay_through);
                assert!(
                    matches!(entries.as_slice(), [SubEvent::Removed(removed)] if removed.obj_id == obj && removed.part_id == part_a)
                );
                revision
            }
            other => panic!("expected tombstone revision, got {other:?}"),
        };

        let missing_obj = test_obj(204);
        let mut filtered = store
            .open_local_revision_reader(
                SubPartsRequest {
                    lower_bound: replay_through,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                        obj_id: missing_obj,
                    }]),
                },
                RevisionReadLimits { max_entries: 1 },
            )
            .await??;
        let filtered_through = match filtered.next().await? {
            RevisionRead::Entries { revision, entries } => {
                assert!(revision >= replay_through);
                assert!(entries.is_empty());
                revision
            }
            other => panic!("expected empty filtered progress, got {other:?}"),
        };
        assert!(matches!(
            filtered.next().await?,
            RevisionRead::ReplayComplete { through } if through == filtered_through
        ));

        let mut bounded = store
            .open_local_revision_reader(
                SubPartsRequest {
                    lower_bound: replay_through,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part_b,
                        cursor: grouped_revision,
                    }]),
                },
                RevisionReadLimits { max_entries: 1 },
            )
            .await??;
        let bounded_through = match bounded.next().await? {
            RevisionRead::Entries { revision, entries } => {
                assert!(revision >= replay_through);
                assert!(entries.is_empty());
                revision
            }
            other => panic!("expected empty bounded progress, got {other:?}"),
        };
        assert!(matches!(
            bounded.next().await?,
            RevisionRead::ReplayComplete { through } if through == bounded_through
        ));

        let unrelated_obj = test_obj(205);
        store
            .set_obj_payload(unrelated_obj, payload("revision-filtered", 5))
            .await?;
        assert!(matches!(
            filtered.next().await?,
            RevisionRead::Entries {
                revision,
                entries
            } if entries.is_empty() && revision > filtered_through
        ));

        store
            .set_obj_payload(obj, payload("revision-live", 6))
            .await?;
        match reader.next().await? {
            RevisionRead::Entries { revision, entries } => {
                assert!(revision > removed_revision);
                assert!(
                    matches!(entries.as_slice(), [SubEvent::Changed(changed)] if changed.obj_id == obj)
                );
            }
            other => panic!("expected live revision, got {other:?}"),
        }
        Ok(())
    }

    pub async fn assert_obj_occupancy_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part_id = test_part(14);
        let obj_id = test_obj(15);
        assert!(!store.obj_exists(obj_id).await?);

        let payload = payload("occupancy", 1);
        store.set_obj_payload(obj_id, payload).await?;
        assert!(store.obj_exists(obj_id).await?);

        store.add_obj_to_parts(obj_id, vec![part_id]).await?;
        assert!(store.obj_exists(obj_id).await?);

        store.remove_obj_from_part(obj_id, part_id).await?;
        assert!(store.obj_exists(obj_id).await?);
        Ok(())
    }

    pub async fn assert_summarize_parts_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part_a = test_part(11);
        let part_b = test_part(12);
        let unknown = test_part(13);
        let obj_a = test_obj(1);
        let obj_b = test_obj(2);

        store.ensure_part(part_a).await?;
        store.ensure_part(part_b).await?;

        assert_eq!(
            store.summarize_parts(HashSet::new()).await??,
            HashMap::new()
        );
        match store.summarize_parts(HashSet::from([unknown])).await? {
            Err(ListPartsError::UnkownParts { unkown_parts }) => {
                assert_eq!(unkown_parts, vec![unknown]);
            }
            other => panic!("unexpected summarize_parts result: {other:?}"),
        }

        seed_live_obj(store, obj_a, payload("summarize-a", 1), &[part_a]).await?;
        seed_live_obj(store, obj_b, payload("summarize-b", 2), &[part_b]).await?;

        let summary = store
            .summarize_parts(HashSet::from([part_a, part_b]))
            .await??;
        assert_eq!(summary.len(), 2);
        assert_eq!(summary[&part_a].member_count, 1);
        assert_eq!(summary[&part_a].latest_cursor, 2);
        assert_eq!(summary[&part_b].member_count, 1);
        assert_eq!(summary[&part_b].latest_cursor, 4);

        match store
            .summarize_parts(HashSet::from([part_a, unknown]))
            .await?
        {
            Err(ListPartsError::UnkownParts { unkown_parts }) => {
                assert_eq!(unkown_parts, vec![unknown]);
            }
            other => panic!("unexpected summarize_parts result: {other:?}"),
        }
        Ok(())
    }

    pub async fn assert_changed_buckets_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(21);
        let unknown = test_part(22);
        let bucket_a = BuckId::new(1, 0);
        let bucket_b = BuckId::new(1, 1);
        let bucket_c = BuckId::new(1, 2);
        let obj_a = obj_in_bucket(bucket_a, 1);
        let obj_b = obj_in_bucket(bucket_b, 2);
        let obj_c = obj_in_bucket(bucket_c, 3);

        store.ensure_part(part).await?;
        seed_live_obj(store, obj_a, payload("changed-a", 1), &[part]).await?;
        seed_live_obj(store, obj_b, payload("changed-b", 2), &[part]).await?;
        seed_live_obj(store, obj_c, payload("changed-c", 3), &[part]).await?;

        match store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: unknown,
                offset: bucket_a,
                since: 0,
                limit_hint: 16,
            })
            .await?
        {
            Err(ListPartsError::UnkownParts { unkown_parts }) => {
                assert_eq!(unkown_parts, vec![unknown]);
            }
            other => panic!("unexpected get_changed_buckets result: {other:?}"),
        }

        let changed = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: part,
                offset: bucket_a,
                since: 0,
                limit_hint: 16,
            })
            .await??
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(changed.len(), 3);
        assert!(changed.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert!(
            changed
                .iter()
                .all(|buck| buck.id.level() == bucket_a.level())
        );
        for buck in &changed {
            assert_eq!(store.get_bucket_summary(part, buck.id).await?, *buck);
        }

        let changed_from_b = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: part,
                offset: bucket_b,
                since: 0,
                limit_hint: 16,
            })
            .await??
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(
            changed_from_b
                .iter()
                .map(|buck| buck.id)
                .collect::<Vec<_>>(),
            vec![bucket_b, bucket_c]
        );

        let cutoff = changed
            .iter()
            .map(|buck| buck.changed_at)
            .max()
            .expect(ERROR_IMPOSSIBLE);
        let nothing = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: part,
                offset: bucket_a,
                since: cutoff,
                limit_hint: 16,
            })
            .await??;
        assert!(nothing.is_empty());

        seed_live_obj(store, obj_a, payload("changed-a-2", 4), &[part]).await?;
        let changed_after = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: part,
                offset: bucket_a,
                since: cutoff,
                limit_hint: 16,
            })
            .await??
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(changed_after.len(), 1);
        assert_eq!(changed_after[0].id, bucket_a);
        assert!(changed_after[0].changed_at > cutoff);
        assert_eq!(
            store.get_bucket_summary(part, bucket_a).await?,
            changed_after[0]
        );
        Ok(())
    }

    pub async fn assert_payload_can_trail_membership_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(24);
        let bucket = BuckId::new(1, 6);
        let obj = obj_in_bucket(bucket, 9);
        let seed = FingerprintSeed::new(0x4444_5555, 0x6666_7777);

        store.ensure_part(part).await?;
        store.add_obj_to_parts(obj, vec![part]).await?;

        assert_eq!(store.obj_payload(obj).await?, None);
        assert_eq!(store.obj_parts(obj).await?, vec![part]);
        assert_eq!(store.member_count(part).await?, 0);

        let bucket_before = store.get_bucket_summary(part, bucket).await?;
        assert_eq!(
            bucket_before,
            BucketSummary {
                id: bucket,
                len: 0,
                live_count: 0,
                fp: (0, 0),
                changed_at: 0,
            }
        );

        let leaf_before = store
            .leaf_buckets(LeafBucketsRequest {
                part_id: part,
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket,
                    after: None,
                }],
                seed,
                limit_hint: 8,
            })
            .await??
            .bucks
            .remove(&bucket)
            .expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            leaf_before,
            LeafBucketPage {
                entries: Vec::new(),
                next_after: None,
                done: true,
            }
        );

        let events_before = store.list_events(HashSet::from([part]), 0, 8).await??;
        assert_eq!(
            events_before.get(&part).expect(ERROR_IMPOSSIBLE),
            &PartPage {
                events: Vec::new(),
                next_cursor: None,
            }
        );
        store
            .set_obj_payload(obj, payload("late-payload", 99))
            .await?;

        let live_fp_after = Fingerprint::new(
            &BUCKET_LIVE_FP_SEED,
            &(
                "big-sync-bucket-live-v1",
                bucket,
                obj,
                payload("late-payload", 99),
            ),
        )
        .as_u64();
        let bucket_after = store.get_bucket_summary(part, bucket).await?;
        assert_eq!(bucket_after.id, bucket);
        assert_eq!(bucket_after.len, 1);
        assert_eq!(bucket_after.live_count, 1);
        assert_eq!(bucket_after.fp, (live_fp_after, 0));
        assert!(bucket_after.changed_at > bucket_before.changed_at);

        let changed = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: part,
                offset: bucket,
                since: bucket_before.changed_at,
                limit_hint: 16,
            })
            .await??
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(changed, vec![bucket_after]);

        let leaf_after = store
            .leaf_buckets(LeafBucketsRequest {
                part_id: part,
                since: bucket_before.changed_at,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket,
                    after: None,
                }],
                seed,
                limit_hint: 8,
            })
            .await??
            .bucks
            .remove(&bucket)
            .expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            leaf_after,
            LeafBucketPage {
                entries: vec![BucketObjPageEntry {
                    obj_id: obj,
                    dead: false,
                    fp: Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", obj, payload("late-payload", 99)),
                    ),
                }],
                next_after: None,
                done: true,
            }
        );

        let events_after = store.list_events(HashSet::from([part]), 0, 8).await??;
        let page_after = events_after.get(&part).expect(ERROR_IMPOSSIBLE);
        assert_eq!(page_after.events.len(), 1);
        let added_cursor = match &page_after.events[0] {
            PartEvent::Added(event) => event.cursor,
            other => panic!("expected added event, got {other:?}"),
        };
        assert_added(
            &page_after.events[0],
            added_cursor,
            part,
            obj,
            payload("late-payload", 99),
        );
        assert_eq!(page_after.next_cursor, None);
        Ok(())
    }

    pub async fn assert_leaf_buckets_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(31);
        let unknown = test_part(32);
        let bucket_a = BuckId::new(1, 3);
        let bucket_b = BuckId::new(1, 4);
        let a1 = obj_in_bucket(bucket_a, 1);
        let a2 = obj_in_bucket(bucket_a, 2);
        let a3 = obj_in_bucket(bucket_a, 3);
        let b1 = obj_in_bucket(bucket_b, 1);
        let seed = FingerprintSeed::new(0xaaaa_bbbb, 0xcccc_dddd);

        store.ensure_part(part).await?;
        seed_live_obj(store, a2, payload("leaf-a2", 2), &[part]).await?;
        seed_live_obj(store, a1, payload("leaf-a1", 1), &[part]).await?;
        seed_live_obj(store, a3, payload("leaf-a3", 3), &[part]).await?;
        store.remove_obj_from_part(a3, part).await?;
        seed_live_obj(store, b1, payload("leaf-b1", 4), &[part]).await?;

        match store
            .leaf_buckets(LeafBucketsRequest {
                part_id: unknown,
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket_a,
                    after: None,
                }],
                seed,
                limit_hint: 2,
            })
            .await?
        {
            Err(LeafBucketsError::UnkownPart) => {}
            other => panic!("unexpected leaf_buckets result: {other:?}"),
        }

        let page = store
            .leaf_buckets(LeafBucketsRequest {
                part_id: part,
                since: 0,
                buckets: vec![
                    LeafBucketRequest {
                        buck_id: bucket_a,
                        after: None,
                    },
                    LeafBucketRequest {
                        buck_id: bucket_b,
                        after: None,
                    },
                ],
                seed,
                limit_hint: 2,
            })
            .await??;
        assert_eq!(page.seed, seed);
        let page_a = page.bucks.get(&bucket_a).expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            page_a,
            &LeafBucketPage {
                entries: vec![
                    BucketObjPageEntry {
                        obj_id: a1,
                        dead: false,
                        fp: Fingerprint::new(
                            &seed,
                            &("big-sync-obj-fp-v1", a1, payload("leaf-a1", 1))
                        ),
                    },
                    BucketObjPageEntry {
                        obj_id: a2,
                        dead: false,
                        fp: Fingerprint::new(
                            &seed,
                            &("big-sync-obj-fp-v1", a2, payload("leaf-a2", 2))
                        ),
                    },
                ],
                next_after: Some(a2),
                done: false,
            }
        );
        let page_b = page.bucks.get(&bucket_b).expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            page_b,
            &LeafBucketPage {
                entries: vec![BucketObjPageEntry {
                    obj_id: b1,
                    dead: false,
                    fp: Fingerprint::new(&seed, &("big-sync-obj-fp-v1", b1, payload("leaf-b1", 4))),
                }],
                next_after: None,
                done: true,
            }
        );

        let since = store.get_bucket_summary(part, bucket_b).await?.changed_at;
        store.set_obj_payload(b1, payload("leaf-b1-2", 5)).await?;

        let since_page = store
            .leaf_buckets(LeafBucketsRequest {
                part_id: part,
                since,
                buckets: vec![
                    LeafBucketRequest {
                        buck_id: bucket_a,
                        after: None,
                    },
                    LeafBucketRequest {
                        buck_id: bucket_b,
                        after: None,
                    },
                ],
                seed,
                limit_hint: 2,
            })
            .await??;
        assert_eq!(
            since_page.bucks.get(&bucket_a).expect(ERROR_IMPOSSIBLE),
            &LeafBucketPage {
                entries: vec![],
                next_after: None,
                done: true,
            }
        );
        assert_eq!(
            since_page.bucks.get(&bucket_b).expect(ERROR_IMPOSSIBLE),
            &LeafBucketPage {
                entries: vec![BucketObjPageEntry {
                    obj_id: b1,
                    dead: false,
                    fp: Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", b1, payload("leaf-b1-2", 5)),
                    ),
                }],
                next_after: None,
                done: true,
            }
        );

        let page_a_tail = store
            .leaf_buckets(LeafBucketsRequest {
                part_id: part,
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: bucket_a,
                    after: Some(a2),
                }],
                seed,
                limit_hint: 2,
            })
            .await??
            .bucks
            .remove(&bucket_a)
            .expect(ERROR_IMPOSSIBLE);
        assert_eq!(
            page_a_tail,
            LeafBucketPage {
                entries: vec![BucketObjPageEntry {
                    obj_id: a3,
                    dead: true,
                    fp: Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", a3, serde_json::Value::Null),
                    ),
                }],
                next_after: None,
                done: true,
            }
        );
        Ok(())
    }

    pub async fn assert_list_events_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part_a = test_part(41);
        let part_b = test_part(42);
        let unknown = test_part(43);
        let obj = test_obj(44);

        store.ensure_part(part_a).await?;
        store.ensure_part(part_b).await?;

        seed_live_obj(store, obj, payload("events-1", 1), &[part_a]).await?;
        store.add_obj_to_parts(obj, vec![part_b]).await?;
        store.set_obj_payload(obj, payload("events-2", 2)).await?;
        store.remove_obj_from_part(obj, part_a).await?;
        store.set_obj_payload(obj, payload("events-3", 3)).await?;

        match store.list_events(HashSet::from([unknown]), 0, 10).await? {
            Err(ListPartsError::UnkownParts { unkown_parts }) => {
                assert_eq!(unkown_parts, vec![unknown]);
            }
            other => panic!("unexpected list_events result: {other:?}"),
        }

        let page1 = store
            .list_events(HashSet::from([part_a]), 0, 10)
            .await??
            .remove(&part_a)
            .expect(ERROR_IMPOSSIBLE);
        match &page1.events[..] {
            [PartEvent::Removed(removed)] => {
                assert_eq!(removed.part_id, part_a);
                assert_eq!(removed.obj_id, obj);
            }
            other => panic!("unexpected part_a page1: {other:?}"),
        }

        let page_b = store
            .list_events(HashSet::from([part_b]), 0, 10)
            .await??
            .remove(&part_b)
            .expect(ERROR_IMPOSSIBLE);
        match &page_b.events[..] {
            [PartEvent::Changed(changed)] => {
                assert_eq!(changed.part_ids, vec![part_b]);
                assert_eq!(changed.obj_id, obj);
                assert_eq!(changed.payload, payload("events-3", 3));
            }
            other => panic!("unexpected latest part_b page: {other:?}"),
        }
        Ok(())
    }

    pub async fn assert_readable_subscribe_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(54);
        let obj = test_obj(55);
        let reader = big_sync_core::PeerId::new([56u8; 32]);

        store.ensure_part(part).await?;
        store
            .set_obj_members(
                obj,
                std::collections::HashMap::from([(reader, Access::Read)]),
            )
            .await?;
        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                reader,
            )
            .await??;
        loop {
            if matches!(recv_sub_event(&rx).await?, SubEvent::ReplayComplete) {
                break;
            }
        }

        store
            .set_obj_payload(obj, payload("readable-subscribe", 1))
            .await?;
        store.add_obj_to_parts(obj, vec![part]).await?;

        loop {
            match recv_sub_event(&rx).await? {
                SubEvent::Added(event) => {
                    assert_eq!(event.obj_id, obj);
                    assert_eq!(event.part_id, part);
                    break;
                }
                SubEvent::ReplayComplete | SubEvent::Changed(_) | SubEvent::Removed(_) => {}
            }
        }
        Ok(())
    }

    pub async fn assert_subscribe_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part_a = test_part(51);
        let part_b = test_part(52);
        let obj = test_obj(53);

        store.ensure_part(part_a).await?;
        store.ensure_part(part_b).await?;

        // Grant the subscriber Read access so the filter passes events.
        let sub_peer = big_sync_core::PeerId::new([0u8; 32]);
        store
            .set_obj_members(
                obj,
                std::collections::HashMap::from([(sub_peer, Access::Read)]),
            )
            .await?;

        seed_live_obj(store, obj, payload("sub-1", 1), &[part_a]).await?;
        store.add_obj_to_parts(obj, vec![part_b]).await?;
        store.set_obj_payload(obj, payload("sub-2", 2)).await?;
        store.remove_obj_from_part(obj, part_a).await?;
        store.set_obj_payload(obj, payload("sub-3", 3)).await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part_b,
                        cursor: 0,
                    }]),
                },
                sub_peer,
            )
            .await??;
        let events = collect_sub_events(&rx).await?;
        let replay_cursor = match &events[..] {
            [SubEvent::Changed(changed), SubEvent::ReplayComplete] => {
                assert_eq!(changed.part_ids, vec![part_b]);
                assert_eq!(changed.obj_id, obj);
                assert_eq!(changed.payload, payload("sub-3", 3));
                changed.cursor
            }
            other => panic!("unexpected replay events: {other:?}"),
        };

        store.set_obj_payload(obj, payload("sub-4", 4)).await?;
        let live_evt = recv_sub_event(&rx).await?;
        match live_evt {
            SubEvent::Changed(transition) => {
                assert_eq!(transition.part_ids, vec![part_b]);
                assert_eq!(transition.obj_id, obj);
                assert_eq!(transition.payload, payload("sub-4", 4));
                assert!(transition.cursor > replay_cursor);
            }
            other => panic!("unexpected live sub event: {other:?}"),
        }
        Ok(())
    }

    pub async fn assert_subscribe_replay_filtering_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(61);
        let obj = test_obj(62);
        let auth_peer = big_sync_core::PeerId::new([63u8; 32]);
        let denied_peer = big_sync_core::PeerId::new([64u8; 32]);

        store.ensure_part(part).await?;

        // Seed the doc before any subscriptions.
        store
            .set_obj_payload(obj, payload("replay-filter", 1))
            .await?;
        store.add_obj_to_parts(obj, vec![part]).await?;

        // Set explicit membership: auth_peer has Read; denied_peer gets
        // an empty membership map (explicitly denied).
        store
            .set_obj_members(
                obj,
                std::collections::HashMap::from([(auth_peer, Access::Read)]),
            )
            .await?;

        // Subscribe the authorized peer.
        let auth_rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                auth_peer,
            )
            .await??;
        let auth_events = collect_sub_events(&auth_rx).await?;
        assert!(
            auth_events.iter().any(|evt| match evt {
                SubEvent::Added(added) => added.obj_id == obj && added.part_id == part,
                SubEvent::Changed(changed) => {
                    changed.obj_id == obj && changed.part_ids == vec![part]
                }
                _ => false,
            }),
            "authorized subscriber must receive the document event during replay; got {auth_events:?}"
        );
        assert!(
            auth_events
                .iter()
                .any(|evt| matches!(evt, SubEvent::ReplayComplete)),
            "authorized subscriber must receive ReplayComplete"
        );

        // Subscribe the denied peer (empty membership => no fetcher access).
        let denied_rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                denied_peer,
            )
            .await??;
        let denied_events = collect_sub_events(&denied_rx).await?;
        assert!(
            denied_events
                .iter()
                .any(|evt| matches!(evt, SubEvent::ReplayComplete)),
            "denied subscriber must receive ReplayComplete"
        );
        // The denied subscriber must NOT receive any document events during replay.
        for evt in &denied_events {
            match evt {
                SubEvent::Added(transition) => {
                    panic!(
                        "denied subscriber must not receive Added event during replay; got {transition:?}"
                    );
                }
                SubEvent::Changed(transition) => {
                    panic!(
                        "denied subscriber must not receive Changed event during replay; got {transition:?}"
                    );
                }
                SubEvent::Removed(transition) => {
                    panic!(
                        "denied subscriber must not receive Removed event during replay; got {transition:?}"
                    );
                }
                SubEvent::ReplayComplete => {}
            }
        }
        Ok(())
    }

    pub async fn assert_subscribe_live_filtering_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(71);
        let overlapping_part = test_part(76);
        let obj = test_obj(72);
        let auth_peer = big_sync_core::PeerId::new([73u8; 32]);
        let relay_peer = big_sync_core::PeerId::new([74u8; 32]);
        let denied_peer = big_sync_core::PeerId::new([75u8; 32]);

        store.ensure_part(part).await?;
        store.ensure_part(overlapping_part).await?;

        // Seed the doc before any subscriptions.
        store
            .set_obj_payload(obj, payload("live-filter", 1))
            .await?;
        store
            .add_obj_to_parts(obj, vec![part, overlapping_part])
            .await?;

        // Set membership: auth_peer has Read, relay_peer has Relay,
        // denied_peer has no entry (explicitly denied via empty map).
        store
            .set_obj_members(
                obj,
                std::collections::HashMap::from([
                    (auth_peer, Access::Read),
                    (relay_peer, Access::Relay),
                ]),
            )
            .await?;

        // Subscribe all three and drain through ReplayComplete so each
        // is registered for live events.
        let sub = |peer| async move {
            store
                .subscribe(
                    SubPartsRequest {
                        lower_bound: 0,
                        targets: HashSet::from([
                            big_sync_core::rpc::SubscriptionTarget::Part {
                                part_id: part,
                                cursor: 0,
                            },
                            big_sync_core::rpc::SubscriptionTarget::Part {
                                part_id: overlapping_part,
                                cursor: 0,
                            },
                        ]),
                    },
                    peer,
                )
                .await?
                .map_err(eyre::Report::from)
        };
        let auth_rx = sub(auth_peer).await?;
        let relay_rx = sub(relay_peer).await?;
        let denied_rx = sub(denied_peer).await?;

        collect_sub_events(&auth_rx).await?;
        collect_sub_events(&relay_rx).await?;
        collect_sub_events(&denied_rx).await?;

        // Now all three are subscribed for live events.  Mutate the doc.
        store
            .set_obj_payload(obj, payload("live-filter", 2))
            .await?;

        // Authorized (Read) must receive the live Changed event for each subscribed partition.
        // A change to one object is one logical event, even when it has
        // multiple subscribed part tags.
        let auth_live = recv_sub_event(&auth_rx).await?;
        let SubEvent::Changed(auth_changed) = auth_live else {
            panic!("authorized subscriber expected Changed, got {auth_live:?}");
        };
        assert_eq!(auth_changed.obj_id, obj);
        assert_eq!(auth_changed.payload, payload("live-filter", 2));
        assert_eq!(
            auth_changed.part_ids.into_iter().collect::<HashSet<_>>(),
            HashSet::from([part, overlapping_part]),
            "one live event must cover every subscribed part",
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), auth_rx.recv())
                .await
                .is_err(),
            "multi-part change must not emit duplicate logical events",
        );

        let relay_live = recv_sub_event(&relay_rx).await?;
        let SubEvent::Changed(relay_changed) = relay_live else {
            panic!("relay subscriber expected Changed, got {relay_live:?}");
        };
        assert_eq!(relay_changed.obj_id, obj);
        assert_eq!(relay_changed.payload, payload("live-filter", 2));
        assert_eq!(
            relay_changed.part_ids.into_iter().collect::<HashSet<_>>(),
            HashSet::from([part, overlapping_part]),
            "one relay event must cover every subscribed part",
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), relay_rx.recv())
                .await
                .is_err(),
            "relay multi-part change must not be duplicated",
        );

        // Denied subscriber must NOT receive any live document event.
        match tokio::time::timeout(Duration::from_millis(500), denied_rx.recv()).await {
            Err(_elapsed) => { /* expected: no event within timeout */ }
            Ok(Ok(evt)) => {
                panic!("denied subscriber must not receive live event; got {evt:?}");
            }
            Ok(Err(_)) => {
                panic!("denied subscriber channel closed unexpectedly");
            }
        }

        Ok(())
    }

    pub async fn assert_subscription_semantics_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        #[expect(clippy::too_many_arguments)]
        async fn run_case(
            store: &dyn HostPartStore,
            mode: u8,
            order: u8,
            obj: ObjId,
            part_a: PartId,
            part_b: PartId,
            peer: PeerId,
            live: bool,
        ) -> Res<Vec<SubEvent>> {
            store.ensure_part(part_a).await?;
            store.ensure_part(part_b).await?;
            store
                .set_obj_members(obj, HashMap::from([(peer, Access::Read)]))
                .await?;
            store.set_obj_payload(obj, payload("matrix", 0)).await?;
            store.add_obj_to_parts(obj, vec![part_a, part_b]).await?;
            let baseline_cursor = store
                .list_events(HashSet::from([part_a, part_b]), 0, u32::MAX)
                .await??
                .values()
                .flat_map(|page| page.events.iter())
                .map(|event| match event {
                    PartEvent::Changed(inner) => inner.cursor,
                    PartEvent::Added(inner) => inner.cursor,
                    PartEvent::Removed(inner) => inner.cursor,
                })
                .max()
                .unwrap_or_default();
            let mut targets = HashSet::new();
            if mode != 1 {
                targets.insert(big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_a,
                    cursor: baseline_cursor,
                });
                targets.insert(big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_b,
                    cursor: baseline_cursor,
                });
            }
            if mode != 0 {
                targets.insert(big_sync_core::rpc::SubscriptionTarget::Object { obj_id: obj });
            }
            let request = SubPartsRequest {
                lower_bound: baseline_cursor,
                targets,
            };
            let operations: &[u8] = match order {
                0 => &[0],
                1 => &[1, 2],
                2 => &[0, 1],
                _ => unreachable!("unknown subscription mutation order"),
            };
            if !live {
                for operation in operations {
                    match operation {
                        0 => store.set_obj_payload(obj, payload("matrix", 1)).await?,
                        1 => store.remove_obj_from_part(obj, part_a).await?,
                        2 => store.set_obj_payload(obj, payload("matrix", 2)).await?,
                        _ => unreachable!("unknown subscription mutation"),
                    }
                }
            }
            let rx = store.subscribe(request, peer).await??;
            if !live {
                return collect_sub_events(&rx).await;
            }
            let mut events = collect_sub_events(&rx).await?;
            for operation in operations {
                match operation {
                    0 => store.set_obj_payload(obj, payload("matrix", 1)).await?,
                    1 => store.remove_obj_from_part(obj, part_a).await?,
                    2 => store.set_obj_payload(obj, payload("matrix", 2)).await?,
                    _ => unreachable!("unknown subscription mutation"),
                }
                events.push(recv_sub_event(&rx).await?);
            }
            while let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
            {
                events.push(event);
            }
            Ok(events)
        }
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct CanonicalState {
            payload: Option<ObjPayload>,
            live_parts: BTreeSet<PartId>,
        }

        struct EventLedger {
            mode: u8,
            obj: ObjId,
            requested_parts: BTreeSet<PartId>,
            state: CanonicalState,
            last_cursor: Option<CursorIndex>,
            replay_complete_count: u8,
            changed_groups: HashMap<(CursorIndex, ObjId), BTreeSet<PartId>>,
            violations: Vec<String>,
        }

        impl EventLedger {
            fn new(mode: u8, obj: ObjId, part_a: PartId, part_b: PartId) -> Self {
                Self {
                    mode,
                    obj,
                    requested_parts: BTreeSet::from([part_a, part_b]),
                    state: CanonicalState {
                        payload: None,
                        live_parts: BTreeSet::new(),
                    },
                    last_cursor: None,
                    replay_complete_count: 0,
                    changed_groups: HashMap::new(),
                    violations: Vec::new(),
                }
            }

            fn cursor(&mut self, cursor: CursorIndex) {
                if let Some(last) = self.last_cursor
                    && cursor < last
                {
                    self.violations
                        .push(format!("cursor regressed from {last} to {cursor}"));
                }
                self.last_cursor = Some(self.last_cursor.map_or(cursor, |last| last.max(cursor)));
            }

            fn check_part(&mut self, part_id: PartId, event: &str) {
                if self.mode == 1 || !self.requested_parts.contains(&part_id) {
                    self.violations.push(format!(
                        "{event} projected invalid part {part_id:?} for subscription mode {}",
                        self.mode
                    ));
                }
            }

            fn check_changed_projection(&mut self, part_ids: &[PartId]) {
                if self.mode == 1 && !part_ids.is_empty() {
                    self.violations.push(format!(
                        "object-target Changed contained real parts: {part_ids:?}"
                    ));
                }
                if part_ids
                    .iter()
                    .any(|part| !self.requested_parts.contains(part))
                {
                    self.violations.push(format!(
                        "Changed contained an unsubscribed part: {part_ids:?}"
                    ));
                }
            }

            fn observe(&mut self, event: SubEvent) {
                match event {
                    SubEvent::ReplayComplete => {
                        self.replay_complete_count = self.replay_complete_count.saturating_add(1);
                    }
                    SubEvent::Changed(inner) => {
                        self.cursor(inner.cursor);
                        if inner.obj_id != self.obj {
                            self.violations.push(format!(
                                "Changed targeted {:?}, expected {:?}",
                                inner.obj_id, self.obj
                            ));
                        }
                        self.check_changed_projection(&inner.part_ids);
                        self.state.payload = Some(inner.payload);
                        self.state.live_parts.extend(inner.part_ids.iter().copied());
                        self.changed_groups
                            .entry((inner.cursor, inner.obj_id))
                            .or_default()
                            .extend(inner.part_ids);
                    }
                    SubEvent::Added(inner) => {
                        self.cursor(inner.cursor);
                        if inner.obj_id != self.obj {
                            self.violations.push(format!(
                                "Added targeted {:?}, expected {:?}",
                                inner.obj_id, self.obj
                            ));
                        }
                        self.check_part(inner.part_id, "Added");
                        self.state.payload = Some(inner.payload);
                        self.state.live_parts.insert(inner.part_id);
                    }
                    SubEvent::Removed(inner) => {
                        self.cursor(inner.cursor);
                        if inner.obj_id != self.obj {
                            self.violations.push(format!(
                                "Removed targeted {:?}, expected {:?}",
                                inner.obj_id, self.obj
                            ));
                        }
                        self.check_part(inner.part_id, "Removed");
                        self.state.live_parts.remove(&inner.part_id);
                    }
                }
            }

            fn finish(self, expected: CanonicalState) -> CanonicalState {
                let mut violations = self.violations;
                if self.replay_complete_count != 1 {
                    violations.push(format!(
                        "expected exactly one ReplayComplete, got {}",
                        self.replay_complete_count
                    ));
                }
                if self.state != expected {
                    violations.push(format!(
                        "canonical state mismatch: observed {:?}, expected {:?}",
                        self.state, expected
                    ));
                }
                assert!(
                    violations.is_empty(),
                    "unresolved subscription violations: {violations:?}"
                );
                self.state
            }
        }
        fn canonical_state(
            mode: u8,
            obj: ObjId,
            part_a: PartId,
            part_b: PartId,
            events: Vec<SubEvent>,
            expected: CanonicalState,
        ) -> CanonicalState {
            let mut ledger = EventLedger::new(mode, obj, part_a, part_b);
            for event in events {
                ledger.observe(event);
            }
            ledger.finish(expected)
        }

        let store = harness.store();
        let peer = PeerId::new([90u8; 32]);
        for (mode, order, seed) in [
            (0u8, 0u8, 91u8),
            (0, 1, 94),
            (0, 2, 97),
            (1, 0, 100),
            (2, 0, 103),
        ] {
            let part_a = test_part(seed);
            let part_b = test_part(seed + 1);
            let replay = run_case(
                store,
                mode,
                order,
                test_obj(seed + 2),
                part_a,
                part_b,
                peer,
                false,
            )
            .await?;
            let live = run_case(
                store,
                mode,
                order,
                test_obj(seed + 3),
                part_a,
                part_b,
                peer,
                true,
            )
            .await?;
            let expected = CanonicalState {
                payload: Some(payload(
                    "matrix",
                    match order {
                        0 | 2 => 1,
                        1 => 2,
                        _ => unreachable!("unknown mutation order"),
                    },
                )),
                live_parts: if mode == 1 {
                    BTreeSet::new()
                } else if order == 0 {
                    BTreeSet::from([part_a, part_b])
                } else {
                    BTreeSet::from([part_b])
                },
            };
            let replay_state = canonical_state(
                mode,
                test_obj(seed + 2),
                part_a,
                part_b,
                replay,
                expected.clone(),
            );
            let live_state = canonical_state(
                mode,
                test_obj(seed + 3),
                part_a,
                part_b,
                live,
                expected.clone(),
            );
            assert_eq!(
                replay_state, live_state,
                "replay and live canonical states diverged for subscription mode {mode}, order {order}",
            );
        }

        async fn run_zero_part_case(
            store: &dyn HostPartStore,
            obj: ObjId,
            peer: PeerId,
            live: bool,
        ) -> Res<Vec<SubEvent>> {
            store
                .set_obj_members(obj, HashMap::from([(peer, Access::Read)]))
                .await?;
            if !live {
                store.set_obj_payload(obj, payload("zero-part", 1)).await?;
            }
            let rx = store
                .subscribe(
                    SubPartsRequest {
                        lower_bound: 0,
                        targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                            obj_id: obj,
                        }]),
                    },
                    peer,
                )
                .await??;
            if live {
                let mut events = collect_sub_events(&rx).await?;
                store.set_obj_payload(obj, payload("zero-part", 1)).await?;
                events.push(recv_sub_event(&rx).await?);
                Ok(events)
            } else {
                collect_sub_events(&rx).await
            }
        }

        let zero_replay =
            run_zero_part_case(store, test_obj(180), PeerId::new([181; 32]), false).await?;
        let zero_live = {
            let obj = test_obj(182);
            let peer = PeerId::new([183; 32]);
            store
                .set_obj_members(obj, HashMap::from([(peer, Access::Read)]))
                .await?;
            let rx = store
                .subscribe(
                    SubPartsRequest {
                        lower_bound: 0,
                        targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                            obj_id: obj,
                        }]),
                    },
                    peer,
                )
                .await??;
            let mut events = collect_sub_events(&rx).await?;
            store.set_obj_payload(obj, payload("zero-part", 1)).await?;
            events.push(recv_sub_event(&rx).await?);
            events
        };
        let zero_expected = CanonicalState {
            payload: Some(payload("zero-part", 1)),
            live_parts: BTreeSet::new(),
        };
        let zero_replay_state = canonical_state(
            1,
            test_obj(180),
            test_part(0),
            test_part(1),
            zero_replay,
            zero_expected.clone(),
        );
        let zero_live_state = canonical_state(
            1,
            test_obj(182),
            test_part(0),
            test_part(1),
            zero_live,
            zero_expected,
        );
        assert_eq!(
            zero_replay_state, zero_live_state,
            "zero-real-part object replay and live object subscriptions must converge",
        );

        async fn run_zero_mixed_case(
            store: &dyn HostPartStore,
            obj: ObjId,
            part: PartId,
            peer: PeerId,
            live: bool,
        ) -> Res<Vec<SubEvent>> {
            store.ensure_part(part).await?;
            store
                .set_obj_members(obj, HashMap::from([(peer, Access::Read)]))
                .await?;
            store.set_obj_payload(obj, payload("mixed", 0)).await?;
            store.add_obj_to_parts(obj, vec![part]).await?;
            let baseline = store
                .list_events(HashSet::from([part]), 0, u32::MAX)
                .await??
                .values()
                .flat_map(|page| page.events.iter())
                .map(|event| match event {
                    PartEvent::Changed(inner) => inner.cursor,
                    PartEvent::Added(inner) => inner.cursor,
                    PartEvent::Removed(inner) => inner.cursor,
                })
                .max()
                .unwrap_or_default();
            if !live {
                store.set_obj_payload(obj, payload("mixed", 1)).await?;
            }
            let rx = store
                .subscribe(
                    SubPartsRequest {
                        lower_bound: baseline,
                        targets: HashSet::from([
                            big_sync_core::rpc::SubscriptionTarget::Part {
                                part_id: part,
                                cursor: baseline,
                            },
                            big_sync_core::rpc::SubscriptionTarget::Object { obj_id: obj },
                        ]),
                    },
                    peer,
                )
                .await??;
            if live {
                let mut events = collect_sub_events(&rx).await?;
                store.set_obj_payload(obj, payload("mixed", 1)).await?;
                events.push(recv_sub_event(&rx).await?);
                while let Ok(Ok(event)) =
                    tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
                {
                    events.push(event);
                }
                Ok(events)
            } else {
                collect_sub_events(&rx).await
            }
        }

        async fn run_populated_object_case(
            store: &dyn HostPartStore,
            obj: ObjId,
            part: PartId,
            peer: PeerId,
            live: bool,
        ) -> Res<Vec<SubEvent>> {
            store.ensure_part(part).await?;
            store
                .set_obj_members(obj, HashMap::from([(peer, Access::Read)]))
                .await?;
            store
                .set_obj_payload(obj, payload("object-only", 0))
                .await?;
            store.add_obj_to_parts(obj, vec![part]).await?;
            let baseline = store
                .list_events(HashSet::from([part]), 0, u32::MAX)
                .await??
                .values()
                .flat_map(|page| page.events.iter())
                .map(|event| match event {
                    PartEvent::Changed(inner) => inner.cursor,
                    PartEvent::Added(inner) => inner.cursor,
                    PartEvent::Removed(inner) => inner.cursor,
                })
                .max()
                .unwrap_or_default();

            if !live {
                store
                    .set_obj_payload(obj, payload("object-only", 1))
                    .await?;
            }
            let rx = store
                .subscribe(
                    SubPartsRequest {
                        lower_bound: baseline,
                        targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                            obj_id: obj,
                        }]),
                    },
                    peer,
                )
                .await??;
            if live {
                let mut events = collect_sub_events(&rx).await?;
                store
                    .set_obj_payload(obj, payload("object-only", 1))
                    .await?;
                events.push(recv_sub_event(&rx).await?);
                Ok(events)
            } else {
                collect_sub_events(&rx).await
            }
        }

        let populated_object_replay = run_populated_object_case(
            store,
            test_obj(190),
            test_part(191),
            PeerId::new([192; 32]),
            false,
        )
        .await?;
        let populated_object_live = run_populated_object_case(
            store,
            test_obj(193),
            test_part(194),
            PeerId::new([195; 32]),
            true,
        )
        .await?;
        let object_expected = CanonicalState {
            payload: Some(payload("object-only", 1)),
            live_parts: BTreeSet::new(),
        };
        let populated_object_replay_state = canonical_state(
            1,
            test_obj(190),
            test_part(191),
            test_part(191),
            populated_object_replay,
            object_expected.clone(),
        );
        let populated_object_live_state = canonical_state(
            1,
            test_obj(193),
            test_part(194),
            test_part(194),
            populated_object_live,
            object_expected,
        );
        assert_eq!(
            populated_object_replay_state, populated_object_live_state,
            "populated object replay and live subscriptions must converge",
        );

        let mixed_replay_part = test_part(185);
        let mixed_replay = run_zero_mixed_case(
            store,
            test_obj(184),
            mixed_replay_part,
            PeerId::new([186; 32]),
            false,
        )
        .await?;
        let mixed_live_part = test_part(188);
        let mixed_live = run_zero_mixed_case(
            store,
            test_obj(187),
            mixed_live_part,
            PeerId::new([189; 32]),
            true,
        )
        .await?;
        let mixed_replay_expected = CanonicalState {
            payload: Some(payload("mixed", 1)),
            live_parts: BTreeSet::from([mixed_replay_part]),
        };
        let mixed_replay_state = canonical_state(
            2,
            test_obj(184),
            mixed_replay_part,
            mixed_replay_part,
            mixed_replay,
            mixed_replay_expected,
        );
        let mixed_live_expected = CanonicalState {
            payload: Some(payload("mixed", 1)),
            live_parts: BTreeSet::from([mixed_live_part]),
        };
        let mixed_live_state = canonical_state(
            2,
            test_obj(187),
            mixed_live_part,
            mixed_live_part,
            mixed_live,
            mixed_live_expected,
        );
        assert_eq!(
            (
                &mixed_replay_state.payload,
                mixed_replay_state.live_parts.is_empty(),
                mixed_replay_state.live_parts.len(),
            ),
            (
                &mixed_live_state.payload,
                mixed_live_state.live_parts.is_empty(),
                mixed_live_state.live_parts.len(),
            ),
            "mixed object/part replay and live subscriptions must converge semantically",
        );

        Ok(())
    }

    pub async fn assert_subscribe_per_part_cursor_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part_a = test_part(81);
        let part_b = test_part(82);
        let obj = test_obj(83);
        let peer = big_sync_core::PeerId::new([84u8; 32]);

        store.ensure_part(part_a).await?;
        store.ensure_part(part_b).await?;
        store
            .set_obj_members(
                obj,
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await?;
        // Build events: cursor 1-4 only for part_a, 5-6 involve part_b.
        // First set payload while obj has no parts (no event recorded).
        store.set_obj_payload(obj, payload("per-cursor", 1)).await?;
        store.add_obj_to_parts(obj, vec![part_a]).await?;
        // cursor=1: Added obj, part_a
        store.set_obj_payload(obj, payload("per-cursor", 2)).await?;
        // cursor=2: Changed [part_a]
        store.set_obj_payload(obj, payload("per-cursor", 3)).await?;
        // cursor=3: Changed [part_a]
        store.set_obj_payload(obj, payload("per-cursor", 4)).await?;
        // cursor=4: Changed [part_a]

        store.add_obj_to_parts(obj, vec![part_b]).await?;
        // cursor=5: Added obj, part_b
        store.set_obj_payload(obj, payload("per-cursor", 5)).await?;
        // cursor=6: Changed [part_a, part_b]

        // The request lower bound is shared by all targets. The latest-state
        // replay returns one Changed event for the payload mutation spanning
        // both parts, rather than replaying stale Added events.
        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part_a,
                            cursor: 0,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part_b,
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await??;
        let events = collect_sub_events(&rx).await?;

        let changes: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                SubEvent::Changed(changed) if changed.obj_id == obj => Some(changed),
                _ => None,
            })
            .collect();
        assert_eq!(
            changes.len(),
            1,
            "one logical multi-part change must produce one replay message: {events:?}",
        );
        assert_eq!(
            changes[0].part_ids.iter().copied().collect::<HashSet<_>>(),
            HashSet::from([part_a, part_b]),
        );
        Ok(())
    }

    pub async fn assert_list_events_pagination_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(151);
        let objs = [test_obj(152), test_obj(153), test_obj(154)];

        store.ensure_part(part).await?;

        // Create three distinct events on the same part, each at a distinct cursor.
        for (ii, &obj_id) in objs.iter().enumerate() {
            store
                .set_obj_payload(obj_id, payload("pagination", ii as u64))
                .await?;
            store.add_obj_to_parts(obj_id, vec![part]).await?;
        }

        // Paginate with limit=1, following next_cursor until exhaustion.
        let mut cursor = 0;
        let mut collected: Vec<(CursorIndex, ObjId)> = Vec::new();
        loop {
            let page = store
                .list_events(HashSet::from([part]), cursor, 1)
                .await??
                .remove(&part)
                .expect(ERROR_IMPOSSIBLE);
            for evt in &page.events {
                match evt {
                    PartEvent::Added(added) => {
                        collected.push((added.cursor, added.obj_id));
                    }
                    PartEvent::Changed(_) | PartEvent::Removed(_) => {
                        panic!("unexpected event type for single-object part");
                    }
                }
            }
            match page.next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }

        // Every event must be returned exactly once, in order.
        assert_eq!(
            collected.len(),
            objs.len(),
            "expected {} events via pagination, got {collected:?}",
            objs.len(),
        );
        for (ii, &expected_obj_id) in objs.iter().enumerate() {
            let (retrieved_cursor, retrieved_obj_id) = collected[ii];
            assert_eq!(
                retrieved_obj_id, expected_obj_id,
                "event {ii}: expected obj {expected_obj_id}, got {retrieved_obj_id}"
            );
            if ii > 0 {
                assert!(
                    retrieved_cursor > collected[ii - 1].0,
                    "event {ii} cursor {} not after previous cursor {}",
                    retrieved_cursor,
                    collected[ii - 1].0,
                );
            }
        }
        Ok(())
    }

    pub async fn assert_peer_cursor_monotonicity_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(101);
        let peer = big_sync_core::PeerId::new([102u8; 32]);

        store.ensure_part(part).await?;

        // Set to a higher cursor, then attempt regression.
        store.set_peer_part_cursor(peer, part, 42).await?;
        assert_eq!(
            store.get_peer_part_cursor(peer, part).await?,
            42,
            "initial cursor should be 42"
        );

        // Attempt to regress: setting to 5 must be a no-op.
        store.set_peer_part_cursor(peer, part, 5).await?;
        let cursor = store.get_peer_part_cursor(peer, part).await?;
        assert!(
            cursor >= 42,
            "peer part cursor regressed from 42 to {cursor}"
        );
        Ok(())
    }

    pub async fn assert_remove_obj_advances_latest_cursor_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(111);
        let obj = test_obj(112);

        store.ensure_part(part).await?;
        store.set_obj_payload(obj, payload("cursor-adv", 1)).await?;
        store.add_obj_to_parts(obj, vec![part]).await?;

        let summaries_after_add = store.summarize_parts(HashSet::from([part])).await??;
        let cursor_after_add = summaries_after_add
            .get(&part)
            .expect("summary must contain part")
            .latest_cursor;

        store.remove_obj_from_part(obj, part).await?;

        let summaries_after_remove = store.summarize_parts(HashSet::from([part])).await??;
        let cursor_after_remove = summaries_after_remove
            .get(&part)
            .expect("summary must contain part")
            .latest_cursor;

        assert!(
            cursor_after_remove > cursor_after_add,
            "removing an object from a part must advance latest_cursor: initial={cursor_after_add}, post_remove={cursor_after_remove}"
        );

        let page = store
            .list_events(HashSet::from([part]), cursor_after_add, u32::MAX)
            .await??
            .remove(&part)
            .expect(ERROR_IMPOSSIBLE);

        assert!(
            page.events.iter().any(|evt| matches!(evt, PartEvent::Removed(rem) if rem.obj_id == obj && rem.cursor == cursor_after_remove)),
            "list_events must contain Removed event with advanced cursor {cursor_after_remove}: {:?}",
            page.events
        );
        Ok(())
    }

    pub async fn assert_list_events_next_cursor_exactness_contract<H>(harness: &H) -> Res<()>
    where
        H: HostPartStoreContractHarness + Sync,
    {
        let store = harness.store();
        let part = test_part(121);
        let obj1 = test_obj(122);
        let obj2 = test_obj(123);
        let obj3 = test_obj(124);

        store.ensure_part(part).await?;

        // Seed 2 objects
        store.set_obj_payload(obj1, payload("exactness", 1)).await?;
        store.add_obj_to_parts(obj1, vec![part]).await?;
        store.set_obj_payload(obj2, payload("exactness", 2)).await?;
        store.add_obj_to_parts(obj2, vec![part]).await?;

        // Query exactly limit=2 matching 2 events: next_cursor MUST be None
        let page_exact = store
            .list_events(HashSet::from([part]), 0, 2)
            .await??
            .remove(&part)
            .expect(ERROR_IMPOSSIBLE);

        assert_eq!(page_exact.events.len(), 2);
        assert_eq!(
            page_exact.next_cursor, None,
            "next_cursor must be None when no further events remain beyond limit page"
        );

        // Seed 3rd object
        store.set_obj_payload(obj3, payload("exactness", 3)).await?;
        store.add_obj_to_parts(obj3, vec![part]).await?;

        // Query limit=2 when 3 events exist: next_cursor MUST be Some
        let page_more = store
            .list_events(HashSet::from([part]), 0, 2)
            .await??
            .remove(&part)
            .expect(ERROR_IMPOSSIBLE);

        assert_eq!(page_more.events.len(), 2);
        let next = page_more
            .next_cursor
            .expect("next_cursor must be Some when matching events remain beyond limit page");

        // Fetching page starting from next_cursor gets the 3rd event with next_cursor == None
        let page_tail = store
            .list_events(HashSet::from([part]), next, 2)
            .await??
            .remove(&part)
            .expect(ERROR_IMPOSSIBLE);

        assert_eq!(page_tail.events.len(), 1);
        assert_eq!(
            page_tail.next_cursor, None,
            "next_cursor must be None on tail page"
        );
        Ok(())
    }
}
