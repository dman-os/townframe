use crate::interlude::*;

use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PartPage, PartSummary, SubEvent, SubPartsRequest,
};
use big_sync_core::{mpsc, BuckId, Byte32Id, ObjId, PartId, PeerId};

pub mod memory;
pub mod sqlite;

// pub type ObjStoreLease = u64;

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum StoreMutationOutcome {
//     Applied,
//     Stale,
// }

#[async_trait]
pub trait HostPartStore: Send + Sync {
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

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>>;
}

pub(crate) fn obj_id_bounds_for_bucket(bucket_id: BuckId) -> (ObjId, Option<ObjId>) {
    let level = bucket_id.level();
    let prefix_bits = u32::from(level) * u32::from(BuckId::BITS_PER_LEVEL);
    debug_assert!(prefix_bits <= u32::from(u16::BITS));

    if prefix_bits == 0 {
        return (ObjId(Byte32Id::new([0; 32])), None);
    }

    let shift = u32::from(u16::BITS) - prefix_bits;
    let start_prefix = (u32::from(bucket_id.index())) << shift;
    let start = {
        let mut bytes = [0; 32];
        bytes[..2].copy_from_slice(&(start_prefix as u16).to_be_bytes());
        ObjId(Byte32Id::new(bytes))
    };
    if prefix_bits == u32::from(u16::BITS) || bucket_id.index() == u16::MAX {
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
pub mod contract {
    use super::*;
    use big_sync_core::rpc::{
        BucketSummary, GetChangedBucketsRequest, LeafBucketRequest, LeafBucketsRequest,
        BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED,
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
            assert!(page
                .entries
                .windows(2)
                .all(|pair| pair[0].obj_id < pair[1].obj_id));
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
        BucketObjPageEntry, BucketSummary, LeafBucketPage, LeafBucketRequest, LeafBucketsRequest,
        ListPartsError, PartEvent, PartPage, SubEvent, SubPartsRequest, BUCKET_LIVE_FP_SEED,
    };
    use big_sync_core::{Fingerprint, FingerprintSeed};
    use tokio::time::{timeout, Duration};

    #[async_trait]
    pub trait HostPartStoreContractHarness {
        fn store(&self) -> &dyn HostPartStore;

        async fn ensure_part(&self, part_id: PartId) -> Res<()>;
    }

    fn test_peer(seed: u8) -> PeerId {
        PeerId(Byte32Id::new([seed; 32]))
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

    fn part_ids(parts: &[PartId]) -> HashSet<PartId> {
        parts.iter().copied().collect()
    }

    fn assert_added(
        event: &PartEvent,
        cursor: CursorIndex,
        part_id: PartId,
        obj_id: ObjId,
        payload: Option<ObjPayload>,
    ) {
        let PartEvent::Added(transition) = event else {
            panic!("expected added event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
        assert_eq!(transition.payload, payload);
    }

    fn assert_removed(event: &PartEvent, cursor: CursorIndex, part_id: PartId, obj_id: ObjId) {
        let PartEvent::Removed(transition) = event else {
            panic!("expected removed event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
    }

    fn assert_changed(
        event: &PartEvent,
        cursor: CursorIndex,
        part_ids: &[PartId],
        obj_id: ObjId,
        payload: ObjPayload,
    ) {
        let PartEvent::Changed(transition) = event else {
            panic!("expected changed event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_ids, part_ids);
        assert_eq!(transition.obj_id, obj_id);
        assert_eq!(transition.payload, payload);
    }

    fn assert_sub_added(
        event: &SubEvent,
        cursor: CursorIndex,
        part_id: PartId,
        obj_id: ObjId,
        payload: Option<ObjPayload>,
    ) {
        let SubEvent::Added(transition) = event else {
            panic!("expected added sub event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
        assert_eq!(transition.payload, payload);
    }

    fn assert_sub_removed(event: &SubEvent, cursor: CursorIndex, part_id: PartId, obj_id: ObjId) {
        let SubEvent::Removed(transition) = event else {
            panic!("expected removed sub event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
    }

    fn assert_sub_changed(
        event: &SubEvent,
        cursor: CursorIndex,
        part_ids: &[PartId],
        obj_id: ObjId,
        payload: ObjPayload,
    ) {
        let SubEvent::Changed(transition) = event else {
            panic!("expected changed sub event");
        };
        assert_eq!(transition.cursor, cursor);
        assert_eq!(transition.part_ids, part_ids);
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

        harness.ensure_part(part_a).await?;
        harness.ensure_part(part_b).await?;

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
        assert_eq!(summary[&part_a].latest_cursor, 1);
        assert_eq!(summary[&part_b].member_count, 1);
        assert_eq!(summary[&part_b].latest_cursor, 2);

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

        harness.ensure_part(part).await?;
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
        assert!(changed
            .iter()
            .all(|buck| buck.id.level() == bucket_a.level()));
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

        harness.ensure_part(part).await?;
        store.add_obj_to_parts(obj, vec![part]).await?;

        assert_eq!(store.obj_payload(obj).await?, None);
        assert_eq!(store.obj_parts(obj).await?, vec![part]);
        assert_eq!(store.member_count(part).await?, 1);

        let live_fp_before = Fingerprint::new(
            &BUCKET_LIVE_FP_SEED,
            &(
                "big-sync-bucket-live-v1",
                bucket,
                obj,
                serde_json::Value::Null,
            ),
        )
        .as_u64();
        let bucket_before = store.get_bucket_summary(part, bucket).await?;
        assert_eq!(
            bucket_before,
            BucketSummary {
                id: bucket,
                len: 1,
                live_count: 1,
                fp: (live_fp_before, 0),
                changed_at: 3,
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
                entries: vec![BucketObjPageEntry {
                    obj_id: obj,
                    dead: false,
                    fp: Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", obj, serde_json::Value::Null),
                    ),
                }],
                next_after: None,
                done: true,
            }
        );

        let events_before = store.list_events(HashSet::from([part]), 0, 8).await??;
        assert_eq!(
            events_before.get(&part).expect(ERROR_IMPOSSIBLE),
            &PartPage {
                events: vec![PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                    cursor: 3,
                    part_id: part,
                    obj_id: obj,
                    payload: None,
                })],
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
        assert_eq!(page_after.events.len(), 2);
        assert_added(&page_after.events[0], 3, part, obj, None);
        assert_changed(
            &page_after.events[1],
            4,
            &[part],
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

        harness.ensure_part(part).await?;
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

        harness.ensure_part(part_a).await?;
        harness.ensure_part(part_b).await?;

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
            [PartEvent::Added(added), PartEvent::Changed(changed)] => {
                assert_eq!(added.part_id, part_b);
                assert_eq!(added.obj_id, obj);
                assert_eq!(added.payload, Some(payload("events-1", 1)));
                assert_eq!(changed.part_ids, vec![part_b]);
                assert_eq!(changed.obj_id, obj);
                assert_eq!(changed.payload, payload("events-3", 3));
                assert!(added.cursor < changed.cursor);
            }
            other => panic!("unexpected part_b page: {other:?}"),
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

        harness.ensure_part(part_a).await?;
        harness.ensure_part(part_b).await?;

        seed_live_obj(store, obj, payload("sub-1", 1), &[part_a]).await?;
        store.add_obj_to_parts(obj, vec![part_b]).await?;
        store.set_obj_payload(obj, payload("sub-2", 2)).await?;
        store.remove_obj_from_part(obj, part_a).await?;
        store.set_obj_payload(obj, payload("sub-3", 3)).await?;

        let rx = store
            .subscribe(SubPartsRequest {
                parts: vec![big_sync_core::rpc::PartStreamCursorRequest {
                    part_id: part_b,
                    cursor: 3,
                }],
            })
            .await??;
        let events = collect_sub_events(&rx).await?;
        let replay_cursor = match &events[..] {
            [SubEvent::Added(added), SubEvent::Changed(changed), SubEvent::ReplayComplete] => {
                assert_eq!(added.part_id, part_b);
                assert_eq!(added.obj_id, obj);
                assert_eq!(added.payload, Some(payload("sub-1", 1)));
                assert_eq!(changed.part_ids, vec![part_b]);
                assert_eq!(changed.obj_id, obj);
                assert_eq!(changed.payload, payload("sub-3", 3));
                assert!(added.cursor < changed.cursor);
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
}
