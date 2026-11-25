//! The bucket laws under load: many keys sharing deep buckets.
//!
//! The base universe is four one-byte keys, whose deepest buckets are all different, so a bucket
//! below level 1 never holds two members there and the incremental summary is never asked to
//! keep two members of one deep bucket straight. The crowd is eight keys found by search (a key
//! cannot be built to hash into a chosen bucket): three share one deepest bucket, two sit in its
//! sibling (same parent, different deepest bucket), and the rest leave the first key's path one
//! level at a time. Every level of every key's path is therefore shared by some other key.

use std::sync::OnceLock;

use super::*;
use crate::part_store::bucket_summary::BucketFingerprint;

/// The keys of the crowd.
pub const CROWD: u8 = 8;

/// Candidate keys in search order. They are all distinct and share nothing but a prefix, so the
/// hash spreads them over the deepest buckets evenly.
fn candidates() -> impl Iterator<Item = ObjKey> {
    (0..=u32::MAX).map(|counter| {
        let mut bytes = b"crowd:".to_vec();
        bytes.extend_from_slice(&counter.to_be_bytes());
        ObjKey::new(&bytes)
    })
}

/// The `nth` candidate that lands in `bucket` at its own level, so a test can put a member in a
/// chosen bucket (`nth` 0 is the first, and a larger one is a key sorting elsewhere in it). The
/// search costs about `ARITY^level` hashes per match, ~65k at the deepest level.
pub fn obj_in_bucket(bucket: BuckId, nth: usize) -> ObjKey {
    candidates()
        .filter(|key| BuckId::from_obj_key(bucket.level(), key) == bucket)
        .nth(nth)
        .expect("some key hashes into every bucket")
}

/// The deepest level at which two deepest buckets share a path (0: only the root).
fn shared_level(a: BuckId, b: BuckId) -> BuckLevel {
    (0..=BuckId::MAX_LEVEL)
        .rev()
        .find(|level| a.to_level(*level) == b.to_level(*level))
        .expect("every pair shares the root")
}

/// The crowd, grouped by how deep it shares a path with its first key: indices `0..3` share the
/// first key's deepest bucket, `3..5` its level 3 bucket but not its deepest, `5` its level 2
/// bucket but not level 3, `6` its level 1 bucket but not level 2, `7` only the root.
pub fn crowd() -> &'static [ObjKey] {
    static KEYS: OnceLock<Vec<ObjKey>> = OnceLock::new();
    KEYS.get_or_init(|| {
        // How many keys share exactly this many levels with the first (itself among those at
        // the deepest level), from the root down.
        const WANT: [usize; 5] = [1, 1, 1, 2, 3];
        let mut groups: [Vec<ObjKey>; 5] = default();
        let mut keys = candidates();
        let first = keys.next().expect(ERROR_IMPOSSIBLE);
        let first_deepest = BuckId::deepest_from_obj_key(&first);
        groups[usize::from(BuckId::MAX_LEVEL)].push(first);
        // The sibling's deepest bucket, once its first member is found.
        let mut sibling = None;
        for key in keys {
            // One hash a candidate: the search is ~200k of them, in a debug build.
            let deepest = BuckId::deepest_from_obj_key(&key);
            let level = usize::from(shared_level(first_deepest, deepest));
            if level == 3 && sibling.is_some_and(|sibling| sibling != deepest) {
                continue;
            }
            if groups[level].len() < WANT[level] {
                if level == 3 {
                    sibling = Some(deepest);
                }
                groups[level].push(key);
            }
            if groups
                .iter()
                .zip(WANT)
                .all(|(group, want)| group.len() == want)
            {
                break;
            }
        }
        groups.into_iter().rev().flatten().collect()
    })
}

/// Differential op sequences over the crowd, for [`crowded_differential`].
pub fn crowd_ops() -> impl Strategy<Value = Vec<PartOp>> {
    ops_over(
        (0..CROWD)
            .prop_map(|at| crowd()[usize::from(at)].clone())
            .boxed(),
    )
}

/// [`differential`] over the crowd: the same checks, whose every bucket path is shared.
pub async fn crowded_differential<B: Backend>(backend: &B, ops: Vec<PartOp>) -> Res<()> {
    differential_over(backend, crowd().to_vec(), ops).await
}

/// A harness over the crowd with part 0 granted to principal 0.
async fn open<B: Backend>(backend: &B) -> Res<Harness<B>> {
    let mut harness = Harness::open_over(backend, crowd().to_vec()).await?;
    harness.commit(vec![grant(&[(0, 0)])]).await?;
    Ok(harness)
}

/// Every bucket on a crowd key's path, root included.
fn crowd_buckets() -> Vec<BuckId> {
    crowd()
        .iter()
        .flat_map(|key| {
            let deepest = BuckId::deepest_from_obj_key(key);
            (0..=BuckId::MAX_LEVEL).map(move |level| deepest.to_level(level))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The payload and the membership of part 0 for `keys`, as two batches of one transaction.
fn live(keys: &[ObjKey], value: u8) -> Vec<PartWrite> {
    vec![
        PartWrite::SetPayloads(
            keys.iter()
                .map(|key| (key.clone(), payload(value)))
                .collect(),
        ),
        PartWrite::AddMembers(keys.iter().map(|key| (key.clone(), part(0))).collect()),
    ]
}

fn unlive(keys: &[ObjKey]) -> PartWrite {
    PartWrite::RemoveMembers(keys.iter().map(|key| (key.clone(), part(0))).collect())
}

impl<B: Backend> Harness<B> {
    /// Part 0's summaries over the crowd's buckets.
    async fn crowd_summaries(&self) -> Res<Vec<BucketSummary>> {
        self.store
            .bucket_summaries(&part(0), &crowd_buckets())
            .await
    }

    /// One bucket of part 0.
    async fn summary_of(&self, id: BuckId) -> Res<BucketSummary> {
        Ok(self
            .store
            .bucket_summaries(&part(0), &[id])
            .await?
            .remove(0))
    }
}

/// A summary without its `changed_at`, which is the order the writes were stamped in and so is
/// the one thing two orders of the same writes may differ in.
fn content(mut summaries: Vec<BucketSummary>) -> Vec<BucketSummary> {
    for summary in &mut summaries {
        summary.changed_at = 0;
    }
    summaries
}

/// Law: a bucket counts every member under it at every level, and its leaves list them in
/// (deepest bucket, key) order, whole or paged one at a time.
pub async fn crowded_buckets_count_and_list_every_member<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = open(backend).await?;
    harness.commit(live(crowd(), 1)).await?;
    let first = BuckId::deepest_from_obj_key(&crowd()[0]);
    // 3 keys in the deepest bucket, then 5, 6, 7 and 8 as the path widens to the root.
    for (level, expected) in [(4, 3), (3, 5), (2, 6), (1, 7), (0, 8)] {
        let summary = harness.summary_of(first.to_level(level)).await?;
        assert_eq!(
            (summary.len, summary.live_count),
            (expected, expected),
            "level {level}"
        );
    }
    let sibling = BuckId::deepest_from_obj_key(&crowd()[3]);
    assert_ne!(sibling, first);
    assert_eq!(harness.summary_of(sibling).await?.len, 2);
    let alone = BuckId::deepest_from_obj_key(&crowd()[7]);
    assert_eq!(harness.summary_of(alone).await?.len, 1);

    // The deepest bucket lists its members in key order.
    let principal = principal(0);
    let in_first = harness
        .store
        .bucket_leaves(&principal, &part(0), &[(first, None)], 0, usize::MAX)
        .await?
        .expect("the part is readable")
        .remove(0);
    let mut keys = crowd()[..3].to_vec();
    keys.sort();
    assert_eq!(
        in_first.iter().map(|entry| &entry.obj).collect::<Vec<_>>(),
        keys.iter().collect::<Vec<_>>()
    );
    // Every bucket of the path, paged one entry at a time, reaches each member once, in the
    // order of the whole listing.
    for id in crowd_buckets() {
        let whole = harness
            .store
            .bucket_leaves(&principal, &part(0), &[(id, None)], 0, usize::MAX)
            .await?
            .expect("the part is readable")
            .remove(0);
        let mut paged = Vec::new();
        let mut after = None;
        loop {
            let page = harness
                .store
                .bucket_leaves(&principal, &part(0), &[(id, after.clone())], 0, 1)
                .await?
                .expect("the part is readable")
                .remove(0);
            let Some(entry) = page.into_iter().next() else {
                break;
            };
            after = Some(entry.obj.clone());
            paged.push(entry);
        }
        assert_eq!(paged, whole, "{id:?} paged by one");
        assert_eq!(whole.len(), harness.summary_of(id).await?.len as usize);
    }
    Ok(())
}

/// Law: a bucket's summary is the same whatever order its members were written in, whether the
/// payload or the membership arrived first, and whether a member left before or after another
/// changed.
pub async fn bucket_summaries_ignore_write_order<B: Backend>(backend: &B) -> Res<()> {
    let keys = crowd();
    let orders: [Vec<usize>; 4] = [
        (0..keys.len()).collect(),
        (0..keys.len()).rev().collect(),
        // a stride coprime to 8 visits each once, and interleaves the groups
        (0..keys.len()).map(|at| at * 3 % keys.len()).collect(),
        // the deep bucket's members last, apart from each other
        vec![7, 6, 5, 3, 4, 0, 2, 1],
    ];
    let mut reference: Option<(Vec<BucketSummary>, Vec<BucketSummary>)> = None;
    for (at, order) in orders.iter().enumerate() {
        let mut harness = open(backend).await?;
        for &key in order {
            let key = std::slice::from_ref(&keys[key]);
            if at % 2 == 0 {
                harness.commit(live(key, 1)).await?;
            } else {
                // the membership waits pending (in no bucket) until the payload arrives
                let [payload, member] = <[PartWrite; 2]>::try_from(live(key, 1)).expect("two");
                harness.commit(vec![member]).await?;
                harness.commit(vec![payload]).await?;
            }
        }
        let all_live = content(harness.crowd_summaries().await?);
        // The same members leave in this order, and the others change their payload.
        for &key in order {
            if key % 2 == 0 {
                harness
                    .commit(vec![unlive(std::slice::from_ref(&keys[key]))])
                    .await?;
            } else {
                harness
                    .commit(live(std::slice::from_ref(&keys[key]), 2))
                    .await?;
            }
        }
        let churned = content(harness.crowd_summaries().await?);
        match &reference {
            None => reference = Some((all_live, churned)),
            Some((first_live, first_churned)) => {
                assert_eq!(&all_live, first_live, "order {order:?} after the writes");
                assert_eq!(&churned, first_churned, "order {order:?} after the churn");
            }
        }
    }
    Ok(())
}

/// Law: removals and payload changes inside one deep bucket maintain its summary incrementally:
/// only the buckets on the path change, the sibling does not, and a change undone restores
/// the fingerprints exactly.
pub async fn bucket_summaries_follow_changes_inside_one_bucket<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = open(backend).await?;
    harness.commit(live(crowd(), 1)).await?;
    let deep = BuckId::deepest_from_obj_key(&crowd()[0]);
    let sibling = BuckId::deepest_from_obj_key(&crowd()[3]);
    let path = (0..=BuckId::MAX_LEVEL)
        .map(|level| deep.to_level(level))
        .collect::<Vec<_>>();
    let start = harness.crowd_summaries().await?;
    let of = |summaries: &[BucketSummary], id: BuckId| {
        summaries
            .iter()
            .find(|summary| summary.id == id)
            .expect("the crowd's buckets are summarized")
            .clone()
    };
    let moved = of(&start, deep);
    let keep = of(&start, sibling);

    // A payload change inside the bucket: counts stay, the live fingerprint moves, and the
    // buckets off the path are not touched.
    harness.commit(live(&crowd()[1..2], 2)).await?;
    let changed = harness.crowd_summaries().await?;
    let now = of(&changed, deep);
    assert_eq!((now.len, now.live_count), (moved.len, moved.live_count));
    assert_ne!(
        now.fp.0, moved.fp.0,
        "the live fingerprint follows the payload"
    );
    assert_eq!(now.fp.1, moved.fp.1, "no member died");
    assert!(now.changed_at > moved.changed_at);
    assert_eq!(of(&changed, sibling), keep, "the sibling is not touched");
    for id in &path {
        assert!(of(&changed, *id).changed_at > of(&start, *id).changed_at);
    }

    // Changed back: the fingerprint is the one it was, whatever came between.
    harness.commit(live(&crowd()[1..2], 1)).await?;
    let back = harness.summary_of(deep).await?;
    assert_eq!(
        back.fp, moved.fp,
        "a change undone restores the fingerprints"
    );
    assert_eq!(back.len, moved.len);

    // A removal: the member is dead, not gone, so the bucket's length holds.
    harness.commit(vec![unlive(&crowd()[1..2])]).await?;
    let died = harness.summary_of(deep).await?;
    assert_eq!((died.len, died.live_count), (3, 2));
    assert_ne!(died.fp.0, moved.fp.0);
    assert_ne!(
        died.fp.1, back.fp.1,
        "the dead fingerprint holds the member now"
    );
    assert_eq!(harness.summary_of(sibling).await?.fp, keep.fp);
    // A second member dies, and then comes back: the first one's death is still counted.
    harness.commit(vec![unlive(&crowd()[2..3])]).await?;
    assert_eq!(harness.summary_of(deep).await?.live_count, 1);
    harness.commit(live(&crowd()[2..3], 1)).await?;
    let revived = harness.summary_of(deep).await?;
    assert_eq!((revived.len, revived.live_count), (3, 2));
    assert_eq!(
        revived.fp.1, died.fp.1,
        "the revived member left the dead fingerprint"
    );
    assert_eq!(revived.fp.0, died.fp.0);

    // Everyone leaves, and the bucket still names them dead.
    harness.commit(vec![unlive(&crowd()[..3])]).await?;
    let gone = harness.summary_of(deep).await?;
    assert_eq!((gone.len, gone.live_count), (3, 0));
    assert_eq!(
        gone.fp.0,
        BucketFingerprint::default(),
        "no live member is left in the sum"
    );
    Ok(())
}
