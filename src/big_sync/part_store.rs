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
