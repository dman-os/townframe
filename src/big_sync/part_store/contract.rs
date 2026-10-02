use super::*;
use big_sync_core::rpc::{
    BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED, BucketSummary, GetChangedBucketsRequest,
    LeafBucketRequest, LeafBucketsRequest,
};
use big_sync_core::{Fingerprint, FingerprintSeed};
use std::collections::BTreeSet;

/// The peer every bucket read in the contract modules is asked as.
///
/// A bucket walk is a peer-facing read, so it refuses a part its subscriber may
/// not read exactly as it refuses one the scope does not have. The helpers below
/// grant this principal Read on the parts they walk, which is what a permitted
/// peer holds; the refusal side is pinned by the responder's own test.
pub(crate) const CONTRACT_BUCKET_SUBSCRIBER: u8 = 0xf0;

pub(crate) async fn grant_bucket_read<S>(
    store: &S,
    parts: impl IntoIterator<Item = PartKey>,
) -> Res<PeerKey>
where
    S: HostPartStore + Sync + ?Sized,
{
    use keyhive_core::access::Access;

    let subscriber = PeerKey::new([CONTRACT_BUCKET_SUBSCRIBER; 32]);
    let agents = HashMap::from([(subscriber.clone(), Access::Read)]);
    for part in parts {
        store.set_part_members(part, agents.clone()).await?;
    }
    Ok(subscriber)
}

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
//         .map(|obj_id| BuckId::from_obj_key(BuckId::MAX_LEVEL, obj_id))
//         .collect();
//     assert!(
//         unique_leaf_buckets.len() >= 8,
//         "object ids are too clustered across leaf buckets"
//     );
//     Ok(())
// }

async fn expected_bucket_summary<S>(
    store: &S,
    live_ids: &BTreeSet<ObjKey>,
    dead_ids: &BTreeSet<ObjKey>,
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
            .obj_payload(obj_id.clone())
            .await?
            .expect("live object must have payload");
        live_fp = live_fp.wrapping_add(
            Fingerprint::new(
                &BUCKET_LIVE_FP_SEED,
                &("big-sync-bucket-live-v1", root, obj_id.clone(), payload),
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
        dead_fp = dead_fp.wrapping_add(
            Fingerprint::new(
                &BUCKET_DEAD_FP_SEED,
                &("big-sync-bucket-dead-v1", root, obj_id.clone()),
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

    part_id: PartKey,
    live_ids: &[ObjKey],
    dead_ids: &[ObjKey],
) -> Res<()>
where
    S: HostPartStore + Sync,
{
    assert_eq!(
        live_ids.len(),
        live_ids.iter().cloned().collect::<BTreeSet<_>>().len(),
        "live object set contains duplicates"
    );
    assert_eq!(
        dead_ids.len(),
        dead_ids.iter().cloned().collect::<BTreeSet<_>>().len(),
        "dead object set contains duplicates"
    );
    let live_ids: BTreeSet<_> = live_ids.iter().cloned().collect();
    let dead_ids: BTreeSet<_> = dead_ids.iter().cloned().collect();
    let expected = expected_bucket_summary(store, &live_ids, &dead_ids).await?;
    let subscriber = grant_bucket_read(store, [part_id.clone()]).await?;

    assert_eq!(
        store.member_count(part_id.clone()).await?,
        u64::from(expected.live_count)
    );

    let direct = store
        .get_bucket_summary(part_id.clone(), BuckId::ROOT)
        .await?;
    assert_eq!(direct.id, BuckId::ROOT);
    assert_eq!(direct.len, expected.len);
    assert_eq!(direct.live_count, expected.live_count);
    assert_eq!(direct.fp, expected.fp);

    let changed = store
        .get_changed_buckets(
            GetChangedBucketsRequest {
                part_id,
                offset: BuckId::ROOT,
                to_level: BuckId::ROOT.level(),
                since: 0,
                limit_hint: 1,
            },
            subscriber,
        )
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

    part_id: PartKey,
    seed: FingerprintSeed,
    live_ids: &[ObjKey],
    dead_ids: &[ObjKey],
    limit_hint: u32,
) -> Res<()>
where
    S: HostPartStore + Sync,
{
    assert_eq!(
        live_ids.len(),
        live_ids.iter().cloned().collect::<BTreeSet<_>>().len(),
        "live object set contains duplicates"
    );
    assert_eq!(
        dead_ids.len(),
        dead_ids.iter().cloned().collect::<BTreeSet<_>>().len(),
        "dead object set contains duplicates"
    );
    let live_ids: BTreeSet<_> = live_ids.iter().cloned().collect();
    let dead_ids: BTreeSet<_> = dead_ids.iter().cloned().collect();
    assert!(
        live_ids.is_disjoint(&dead_ids),
        "live and dead object sets must be disjoint"
    );

    let expected: Vec<_> = live_ids.union(&dead_ids).cloned().collect();
    let limit_hint = limit_hint.max(1);
    let subscriber = grant_bucket_read(store, [part_id.clone()]).await?;
    let mut seen = BTreeSet::new();
    let mut after = None;

    loop {
        let result = store
            .leaf_buckets(
                LeafBucketsRequest {
                    part_id: part_id.clone(),
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after,
                    }],
                    seed,
                    limit_hint,
                },
                subscriber.clone(),
            )
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

        let last_obj_id = page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id.clone();
        if page.done {
            assert!(page.next_after.is_none());
        } else {
            assert_eq!(page.next_after, Some(last_obj_id));
            assert_eq!(page.entries.len(), limit_hint as usize);
        }

        for entry in &page.entries {
            assert!(
                seen.insert(entry.obj_id.clone()),
                "duplicate leaf entry {}",
                entry.obj_id
            );
            assert_eq!(entry.dead, dead_ids.contains(&entry.obj_id));
            let expected_fp = if entry.dead {
                Fingerprint::new(
                    &seed,
                    &(
                        "big-sync-obj-fp-v1",
                        entry.obj_id.clone(),
                        serde_json::Value::Null,
                    ),
                )
            } else {
                let payload = store
                    .obj_payload(entry.obj_id.clone())
                    .await?
                    .expect("live object must have payload");
                Fingerprint::new(
                    &seed,
                    &("big-sync-obj-fp-v1", entry.obj_id.clone(), payload),
                )
            };
            assert_eq!(entry.fp, expected_fp);
        }

        if page.done {
            break;
        }
        after = page.next_after.clone();
    }

    assert_eq!(seen, expected.into_iter().collect());
    Ok(())
}

pub async fn assert_root_bucket_contract<S>(
    store: &S,

    part_id: PartKey,
    seed: FingerprintSeed,
    live_ids: &[ObjKey],
    dead_ids: &[ObjKey],
    limit_hint: u32,
) -> Res<()>
where
    S: HostPartStore + Sync,
{
    assert_root_bucket_summary(store, part_id.clone(), live_ids, dead_ids).await?;
    assert_root_leaf_pagination(store, part_id, seed, live_ids, dead_ids, limit_hint).await?;
    Ok(())
}
