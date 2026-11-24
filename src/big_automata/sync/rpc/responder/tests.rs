//! The responder's laws (see [`crate::sync::rpc`]) against a `MemoryPartStore`, with the part
//! oracle as the reference where a law is a count or an order. The differential proptest is in
//! [`differential`].

use futures::executor::block_on;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::*;
use crate::fingerprint::FingerprintSeed;
use crate::part_store::AccessLevel;
use crate::part_store::AuthorizedPage;
use crate::part_store::ObjState;
use crate::part_store::PartSummary;
use crate::part_store::laws::grant;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::laws::revoke;
use crate::part_store::laws::stage;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartOracle;
use crate::part_store::oracle::PartWrite;
use crate::rev::Limits;
use crate::rev::Rev;
use crate::rev::RevWatch;

mod differential;

/// The one principal the laws ask as.
fn asker() -> PeerKey {
    principal(0)
}

/// An object whose bucket the hash picks; plenty of them spread over the levels.
pub(super) fn okey(index: u16) -> ObjKey {
    ObjKey::new(&index.to_be_bytes())
}

/// A store and the oracle that mirrors every commit into it.
pub(super) struct Rig {
    pub store: CountingStore,
    pub oracle: PartOracle,
}

impl Rig {
    pub fn new() -> Self {
        let store = CountingStore::default();
        let generation = futures::FutureExt::now_or_never(store.inner.generation())
            .expect("the memory store answers at once")
            .unwrap();
        Self {
            store,
            oracle: PartOracle::with_generation(generation),
        }
    }

    pub fn responder(&self) -> RpcResponder<CountingStore> {
        RpcResponder::new(self.store.clone())
    }

    pub async fn commit(&mut self, writes: Vec<PartWrite>) -> Option<Rev> {
        let mut tx = self.store.inner.begin();
        for write in writes.clone() {
            stage(&self.store.inner, &mut tx, write).await.unwrap();
        }
        let rev = tx.commit();
        assert_eq!(self.oracle.commit(writes), rev);
        rev
    }

    /// Objects `0..count` (payload their index), all members of part 1, which `asker` reads.
    pub async fn populate(&mut self, count: u16) -> Rev {
        self.commit(vec![
            PartWrite::SetPayloads(
                (0..count)
                    .map(|index| (okey(index), serde_json::json!(index)))
                    .collect(),
            ),
            PartWrite::AddMembers((0..count).map(|index| (okey(index), part(1))).collect()),
            grant(&[(1, 0)]),
        ])
        .await
        .expect("the writes change a row")
    }
}

/// A store that counts how many calls the responder makes of it (law 2).
#[derive(Clone, Default)]
pub(super) struct CountingStore {
    pub inner: MemoryPartStore,
    calls: Arc<AtomicUsize>,
}

impl CountingStore {
    fn count<T>(&self, value: T) -> T {
        self.calls.fetch_add(1, Ordering::SeqCst);
        value
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl PartStoreServe for CountingStore {
    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send {
        self.count(self.inner.latest_rev())
    }
    fn watch(&self) -> RevWatch {
        self.count(self.inner.watch())
    }
    fn generation(&self) -> impl Future<Output = Res<crate::part_store::Generation>> + Send {
        self.count(self.inner.generation())
    }
    fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &crate::part_store::LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut AuthorizedPage,
    ) -> impl Future<Output = Res<()>> + Send {
        self.count(
            self.inner
                .read_authorized(principal, routes, after, until, limits, page),
        )
    }
    fn part_summaries(
        &self,
        principal: &PeerKey,
        parts: &[(PartKey, Rev)],
    ) -> impl Future<Output = Res<Vec<Option<PartSummary>>>> + Send {
        self.count(self.inner.part_summaries(principal, parts))
    }
    fn readable_obj_states(
        &self,
        principal: &PeerKey,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send {
        self.count(self.inner.readable_obj_states(principal, objs))
    }
    fn bucket_list(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<BucketSummary>>>> + Send {
        self.count(
            self.inner
                .bucket_list(principal, part, from, to_level, since, limit),
        )
    }
    fn bucket_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        bucks: &[(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<Vec<LeafEntry>>>>> + Send {
        self.count(
            self.inner
                .bucket_leaves(principal, part, bucks, since, limit),
        )
    }
}

fn list_request(
    part_key: PartKey,
    to_level: BuckLevel,
    since: Rev,
    limit_hint: u32,
) -> GetChangedBucketsRequest {
    GetChangedBucketsRequest {
        part_key,
        offset: BuckId::ROOT,
        to_level,
        since,
        limit_hint,
    }
}

fn leaf_request(
    part_key: PartKey,
    buckets: Vec<(BuckId, Option<ObjKey>)>,
    since: Rev,
    limit_hint: Option<u32>,
) -> LeafBucketsRequest {
    LeafBucketsRequest {
        part_key,
        since,
        buckets: buckets
            .into_iter()
            .map(|(buck_id, after)| LeafBucketRequest { buck_id, after })
            .collect(),
        seed: FingerprintSeed::new(7, 9),
        limit_hint,
    }
}

fn summaries_of(parts: &[(u8, Rev)]) -> PartSummariesRequest {
    PartSummariesRequest {
        parts: parts
            .iter()
            .map(|&(index, since)| (part(index), since))
            .collect(),
    }
}

/// Part 1 readable by `asker`, part 2 populated but granted to someone else only, part 9 not in
/// the store at all.
async fn denied_and_unknown() -> Rig {
    let mut rig = Rig::new();
    rig.populate(6).await;
    rig.commit(vec![
        PartWrite::AddMembers((0..6).map(|index| (okey(index), part(2))).collect()),
        grant(&[(2, 1)]),
    ])
    .await;
    rig
}

#[test]
fn law_1_a_part_the_asker_may_not_read_answers_as_one_the_store_does_not_know() {
    block_on(async {
        let rig = denied_and_unknown().await;
        let responder = rig.responder();
        let asker = asker();
        let (denied, unknown) = (part(2), part(9));

        let summaries = responder
            .part_summaries(&asker, summaries_of(&[(1, 0), (2, 0), (9, 0)]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summaries.keys().collect::<Vec<_>>(), [&part(1)]);

        // At every limit, since and level: the refusals of the two are the same value.
        for (to_level, since, limit) in [(0, 0, 0), (2, 0, 100), (4, 5, 1)] {
            let listed = |part_key: PartKey| {
                responder.changed_buckets(&asker, list_request(part_key, to_level, since, limit))
            };
            assert_eq!(
                listed(denied.clone()).await.unwrap(),
                Err(BucketsError::UnknownPart)
            );
            assert_eq!(
                listed(denied.clone()).await.unwrap(),
                listed(unknown.clone()).await.unwrap()
            );
        }
        for hint in [None, Some(0), Some(1000)] {
            let leaves = |part_key: PartKey| {
                responder.leaf_buckets(
                    &asker,
                    leaf_request(part_key, vec![(BuckId::ROOT, None)], 0, hint),
                )
            };
            assert_eq!(
                leaves(denied.clone()).await.unwrap(),
                Err(BucketsError::UnknownPart)
            );
            assert_eq!(
                leaves(denied.clone()).await.unwrap(),
                leaves(unknown.clone()).await.unwrap()
            );
        }

        // The readable part's empty answer is not a refusal: a quiet listing is `Ok`.
        let quiet = responder
            .changed_buckets(&asker, list_request(part(1), 4, u64::MAX, 100))
            .await
            .unwrap();
        assert_eq!(quiet, Ok(vec![]));
    })
}

#[test]
fn law_1_a_revoked_part_answers_as_unknown_at_once() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(6).await;
        let responder = rig.responder();
        let asker = asker();
        let answers = || async {
            (
                responder
                    .part_summaries(&asker, summaries_of(&[(1, 0)]))
                    .await
                    .unwrap()
                    .unwrap(),
                responder
                    .changed_buckets(&asker, list_request(part(1), 1, 0, 100))
                    .await
                    .unwrap(),
                responder
                    .leaf_buckets(
                        &asker,
                        leaf_request(part(1), vec![(BuckId::ROOT, None)], 0, Some(100)),
                    )
                    .await
                    .unwrap(),
            )
        };
        let (summaries, listing, leaves) = answers().await;
        assert!(summaries.contains_key(&part(1)));
        assert!(listing.is_ok_and(|listed| !listed.is_empty()));
        assert!(leaves.is_ok_and(|pages| !pages[&BuckId::ROOT].entries.is_empty()));

        rig.commit(vec![revoke(&[(1, 0)])]).await;
        let (summaries, listing, leaves) = answers().await;
        assert!(summaries.is_empty());
        assert_eq!(listing, Err(BucketsError::UnknownPart));
        assert_eq!(leaves, Err(BucketsError::UnknownPart));
    })
}

#[test]
fn law_2_each_answer_is_one_store_call() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(40).await;
        let responder = rig.responder();
        let asker = asker();
        let calls = || rig.store.calls();

        responder
            .part_summaries(&asker, summaries_of(&[(1, 0), (2, 0), (3, 0)]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls(), 1, "a summary of three parts is one store call");

        responder
            .changed_buckets(&asker, list_request(part(1), 2, 0, 100))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls(), 2);

        let buckets = (0..5)
            .map(|index| (BuckId::from_obj_key(1, &okey(index)), None))
            .collect();
        responder
            .leaf_buckets(&asker, leaf_request(part(1), buckets, 0, Some(10)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            calls(),
            3,
            "five buckets of one request come from one snapshot"
        );

        // A refusal is one call too.
        responder
            .leaf_buckets(
                &asker,
                leaf_request(
                    part(9),
                    vec![(BuckId::ROOT, None), (BuckId::ROOT, None)],
                    0,
                    None,
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(calls(), 4);
    })
}

#[test]
fn law_3_a_summary_is_per_readable_part_and_from_the_asked_cursor() {
    block_on(async {
        let mut rig = Rig::new();
        let first = rig.populate(6).await;
        // Part 2: readable, its own rows written later; part 3: not granted.
        let second = rig
            .commit(vec![
                PartWrite::AddMembers((0..3).map(|index| (okey(index), part(2))).collect()),
                grant(&[(2, 0)]),
            ])
            .await
            .unwrap();
        rig.commit(vec![
            PartWrite::AddMembers(vec![(okey(0), part(3))]),
            grant(&[(3, 1)]),
        ])
        .await;
        let responder = rig.responder();
        let asker = asker();

        for (since_one, since_two) in [(0, 0), (first, first), (second, second), (first, 0)] {
            let asked = summaries_of(&[(1, since_one), (2, since_two), (3, 0), (9, 0)]);
            let answer = responder
                .part_summaries(&asker, asked.clone())
                .await
                .unwrap()
                .unwrap();
            let expected = asked
                .parts
                .iter()
                .filter_map(|(part, since)| {
                    Some((part.clone(), rig.oracle.part_summary(&asker, part, *since)?))
                })
                .collect::<Map<_, _>>();
            assert_eq!(answer, expected, "{since_one} {since_two}");
            assert_eq!(answer.len(), 2);
        }
        // The cursor is per part: a part asked from its own latest is clean, the other is not.
        let answer = responder
            .part_summaries(&asker, summaries_of(&[(1, first), (2, 0)]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer[&part(1)].dirty.member_changes, 0);
        assert_eq!(answer[&part(2)].dirty.member_changes, 3);
        assert_eq!(answer[&part(1)].member_count, 6);
    })
}

#[test]
fn law_3_a_request_over_the_cap_is_refused_whole_and_one_at_the_cap_is_answered() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(3).await;
        let responder = rig.responder();
        let asker = asker();
        let naming = |count: usize| PartSummariesRequest {
            parts: (0..count)
                .map(|index| (PartKey::new(&[u8::try_from(index).unwrap()]), 0))
                .collect(),
        };

        let at_cap = responder
            .part_summaries(&asker, naming(MAX_SUMMARY_PARTS))
            .await
            .unwrap();
        assert_eq!(
            at_cap,
            Ok(Map::new()),
            "none of these parts exists, and that is no error"
        );
        let calls = rig.store.calls();

        let over = responder
            .part_summaries(&asker, naming(MAX_SUMMARY_PARTS + 1))
            .await
            .unwrap();
        assert_eq!(
            over,
            Err(PartSummariesError::TooManyParts {
                cap: MAX_SUMMARY_PARTS
            })
        );
        assert_eq!(
            rig.store.calls(),
            calls,
            "the refusal precedes every store read"
        );

        // Over the cap with a readable part among them: still the whole request that is refused,
        // never a partial answer.
        let mut parts = naming(MAX_SUMMARY_PARTS + 1).parts;
        parts.insert(part(1), 0);
        assert!(
            responder
                .part_summaries(&asker, PartSummariesRequest { parts })
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(MAX_SUMMARY_PARTS, 64, "the cap is part of the protocol");
    })
}

#[test]
fn law_4_a_listing_is_in_bucket_order_within_the_level_and_changed_after_since() {
    block_on(async {
        let mut rig = Rig::new();
        let first = rig.populate(120).await;
        // A later write touches a few buckets only.
        let second = rig
            .commit(vec![PartWrite::SetPayloads(
                (0..3)
                    .map(|index| (okey(index), serde_json::json!(1000 + index)))
                    .collect(),
            )])
            .await
            .unwrap();
        let responder = rig.responder();
        let asker = asker();

        for to_level in 0..=3 {
            for since in [0, first, second] {
                let listed = responder
                    .changed_buckets(&asker, list_request(part(1), to_level, since, 10_000))
                    .await
                    .unwrap()
                    .unwrap();
                let expected = rig
                    .oracle
                    .bucket_list(&asker, &part(1), BuckId::ROOT, to_level, since, usize::MAX)
                    .unwrap();
                assert_eq!(listed, expected, "level {to_level} since {since}");
                assert!(listed.windows(2).all(|pair| pair[0].id < pair[1].id));
                assert!(listed.iter().all(|buck| buck.id.level() <= to_level));
                assert!(listed.iter().all(|buck| buck.changed_at > since));
            }
        }
        // The later write is a handful of buckets per level; the quiet since is empty.
        let recent = responder
            .changed_buckets(&asker, list_request(part(1), 1, first, 100))
            .await
            .unwrap()
            .unwrap();
        assert!((1..=4).contains(&recent.len()), "{}", recent.len());
        let quiet = responder
            .changed_buckets(&asker, list_request(part(1), 3, second, 100))
            .await
            .unwrap()
            .unwrap();
        assert!(quiet.is_empty());

        // From an offset, the earlier buckets are not listed.
        let all = responder
            .changed_buckets(&asker, list_request(part(1), 1, 0, 100))
            .await
            .unwrap()
            .unwrap();
        let mut from_third = list_request(part(1), 1, 0, 100);
        from_third.offset = all[2].id;
        assert_eq!(
            responder
                .changed_buckets(&asker, from_third)
                .await
                .unwrap()
                .unwrap(),
            all[2..]
        );
    })
}

#[test]
fn law_4_the_limit_keeps_the_siblings_of_the_last_bucket_and_is_capped() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(1300).await;
        let responder = rig.responder();
        let asker = asker();
        let listing = |limit| {
            let (responder, asker) = (responder.clone(), asker.clone());
            async move {
                responder
                    .changed_buckets(&asker, list_request(part(1), 4, 0, limit))
                    .await
                    .unwrap()
                    .unwrap()
            }
        };

        assert!(
            listing(0).await.is_empty(),
            "a zero limit is a zero-bucket page"
        );
        let all = rig
            .oracle
            .bucket_list(&asker, &part(1), BuckId::ROOT, 4, 0, usize::MAX)
            .unwrap();
        for limit in [1, 2, 17, 40] {
            let listed = listing(limit).await;
            let expected_len = {
                // The first `limit`, then the rest of the last one's siblings.
                let last = &all[limit as usize - 1];
                all.iter()
                    .enumerate()
                    .take_while(|(at, buck)| {
                        *at < limit as usize
                            || (buck.id.level() == last.id.level()
                                && buck.id.parent() == last.id.parent())
                    })
                    .count()
            };
            assert_eq!(listed, all[..expected_len], "limit {limit}");
            assert!(listed.len() < limit as usize + usize::from(BuckId::ARITY));
        }
        let capped = listing(u32::MAX).await;
        let cap = MAX_LIST_BUCKETS as usize;
        assert!(
            all.len() > cap + usize::from(BuckId::ARITY),
            "the store has more than the cap"
        );
        assert!(
            (cap..cap + usize::from(BuckId::ARITY)).contains(&capped.len()),
            "a limit above the cap is capped, not refused: {}",
            capped.len()
        );
        assert_eq!(capped, all[..capped.len()]);
    })
}

/// The members of `bucket` the oracle holds, all of them.
fn members(rig: &Rig, bucket: BuckId) -> Vec<LeafEntry> {
    rig.oracle.members_of(&part(1), bucket, None, usize::MAX)
}

#[test]
fn law_5_leaf_pages_chain_through_next_after_and_echo_their_after() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(30).await;
        // One member leaves: its entry is dead, with no fingerprint.
        rig.commit(vec![PartWrite::RemoveMembers(vec![(okey(4), part(1))])])
            .await;
        let responder = rig.responder();
        let asker = asker();
        let all = members(&rig, BuckId::ROOT);
        assert_eq!(all.len(), 30);

        for limit in [1u32, 4, 7, 29, 30, 31] {
            let request = leaf_request(part(1), vec![], 0, Some(limit));
            let mut collected = Vec::new();
            let mut after = None;
            loop {
                let mut request = request.clone();
                request.buckets = vec![LeafBucketRequest {
                    buck_id: BuckId::ROOT,
                    after: after.clone(),
                }];
                let mut answer = responder
                    .leaf_buckets(&asker, request.clone())
                    .await
                    .unwrap()
                    .unwrap();
                let page = answer
                    .remove(&BuckId::ROOT)
                    .expect("a page for the asked bucket");
                assert!(answer.is_empty());
                assert_eq!(page.after, after, "the page echoes the position it answers");
                assert!(page.entries.len() <= limit as usize);
                assert_eq!(
                    page.next_after.is_some(),
                    collected.len() + page.entries.len() < all.len(),
                    "next_after is set exactly when the bucket has more"
                );
                collected.extend(page.entries.iter().cloned());
                match page.next_after {
                    Some(next) => {
                        assert_eq!(&next, &page.entries.last().unwrap().obj_key);
                        after = Some(next);
                    }
                    None => break,
                }
            }
            let expected = all
                .iter()
                .map(|entry| BucketObjPageEntry {
                    obj_key: entry.obj.clone(),
                    fp: entry.live.map(|digest| {
                        BucketObjPageEntry::fingerprint(&request.seed, &entry.obj, digest)
                    }),
                })
                .collect::<Vec<_>>();
            assert_eq!(collected, expected, "limit {limit}");
        }
        let dead = all.iter().filter(|entry| entry.live.is_none()).count();
        assert_eq!(dead, 1);
    })
}

#[test]
fn law_5_a_limit_above_the_cap_is_capped_and_no_hint_is_one_entry() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(1100).await;
        let asker = asker();
        let page_of = |responder: RpcResponder<CountingStore>, hint| {
            let asker = asker.clone();
            async move {
                responder
                    .leaf_buckets(
                        &asker,
                        leaf_request(part(1), vec![(BuckId::ROOT, None)], 0, hint),
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .remove(&BuckId::ROOT)
                    .unwrap()
            }
        };

        let page = page_of(rig.responder(), Some(u32::MAX)).await;
        assert_eq!(page.entries.len(), MAX_LEAF_PAGE as usize);
        assert!(page.next_after.is_some());

        let small = rig.responder().with_leaf_cap(3);
        assert_eq!(page_of(small.clone(), Some(100)).await.entries.len(), 3);
        assert_eq!(page_of(small.clone(), Some(2)).await.entries.len(), 2);
        assert_eq!(page_of(small.clone(), None).await.entries.len(), 1);
        assert_eq!(
            page_of(small, Some(0)).await.entries.len(),
            1,
            "a zero hint is the smallest page"
        );
    })
}

#[test]
fn law_5_each_asked_bucket_answers_from_its_own_after_and_an_unchanged_one_is_empty() {
    block_on(async {
        let mut rig = Rig::new();
        let first = rig.populate(60).await;
        let second = rig
            .commit(vec![PartWrite::SetPayloads(vec![(
                okey(0),
                serde_json::json!("moved"),
            )])])
            .await
            .unwrap();
        let responder = rig.responder();
        let asker = asker();

        let level = 1;
        let mut buckets = (0..60)
            .map(|index| BuckId::from_obj_key(level, &okey(index)))
            .collect::<Vec<_>>();
        buckets.sort();
        buckets.dedup();
        assert!(buckets.len() >= 3, "the objects spread over the level");
        let (wanted, rest) = (buckets[0], buckets[1]);
        let after = members(&rig, rest).first().unwrap().obj.clone();

        let answer = responder
            .leaf_buckets(
                &asker,
                leaf_request(
                    part(1),
                    vec![(wanted, None), (rest, Some(after.clone()))],
                    0,
                    Some(1000),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer.len(), 2);
        assert_eq!(answer[&wanted].after, None);
        assert_eq!(answer[&rest].after, Some(after.clone()));
        assert_eq!(answer[&wanted].entries.len(), members(&rig, wanted).len());
        assert_eq!(
            answer[&rest].entries.len(),
            rig.oracle
                .members_of(&part(1), rest, Some(&after), usize::MAX)
                .len()
        );
        assert!(
            answer[&rest]
                .entries
                .iter()
                .all(|entry| entry.obj_key != after),
            "the entry at `after` is not repeated"
        );

        // From `since` = the latest rev, every bucket is unchanged: an empty page, still echoing.
        let quiet = responder
            .leaf_buckets(
                &asker,
                leaf_request(
                    part(1),
                    vec![(rest, Some(after.clone()))],
                    second,
                    Some(1000),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            quiet[&rest],
            LeafBucketPage {
                after: Some(after),
                entries: vec![],
                next_after: None
            }
        );
        // Only the bucket the later write touched answers past the earlier rev.
        let touched = BuckId::from_obj_key(level, &okey(0));
        for bucket in buckets {
            let page = responder
                .leaf_buckets(
                    &asker,
                    leaf_request(part(1), vec![(bucket, None)], first, Some(1000)),
                )
                .await
                .unwrap()
                .unwrap()
                .remove(&bucket)
                .unwrap();
            assert_eq!(!page.entries.is_empty(), bucket == touched, "{bucket:?}");
        }
    })
}

/// Granting and revoking between calls moves every answer with it (the access is the store's
/// current one, law 1), whichever order the writes arrive in.
#[test]
fn law_1_access_is_the_current_grant_at_every_call() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(6).await;
        let responder = rig.responder();
        let other = principal(1);
        let readable = |who: PeerKey| {
            let responder = responder.clone();
            async move {
                responder
                    .changed_buckets(&who, list_request(part(1), 0, 0, 10))
                    .await
                    .unwrap()
                    .is_ok()
            }
        };
        assert!(!readable(other.clone()).await);
        rig.commit(vec![grant(&[(1, 1)])]).await;
        assert!(readable(other.clone()).await);
        rig.commit(vec![revoke(&[(1, 1)])]).await;
        assert!(!readable(other.clone()).await);
        rig.commit(vec![PartWrite::GrantAccess(vec![(
            part(1),
            other.clone(),
            AccessLevel::Admin,
        )])])
        .await;
        assert!(readable(other).await);
    })
}

#[test]
fn law_5_a_request_malformed_whatever_the_store_holds_is_refused_before_it_is_read() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(20).await;
        let responder = rig.responder();
        let asker = asker();
        let calls = rig.store.calls();

        // An `after` that is not in its bucket: the store would take it as a caller bug.
        let level_one = BuckId::from_obj_key(1, &okey(0));
        let stranger = (1..20)
            .map(okey)
            .find(|obj| BuckId::from_obj_key(1, obj) != level_one)
            .expect("twenty objects do not all share a bucket");
        // Ids no store builds: beyond the deepest level, and an index past its level's range.
        let beyond: BuckId = serde_json::from_value(serde_json::json!(200u32 << 16)).unwrap();
        let past_range: BuckId =
            serde_json::from_value(serde_json::json!((1u32 << 16) | 16)).unwrap();
        for buckets in [
            vec![(level_one, Some(stranger))],
            vec![(BuckId::ROOT, None), (beyond, None)],
            vec![(past_range, None)],
        ] {
            // Whatever the part: malformed is nothing about a part.
            for part_index in [1, 2, 9] {
                let answer = responder
                    .leaf_buckets(
                        &asker,
                        leaf_request(part(part_index), buckets.clone(), 0, Some(5)),
                    )
                    .await
                    .unwrap();
                assert!(
                    matches!(answer, Err(BucketsError::Invalid(_))),
                    "{answer:?}"
                );
            }
        }
        let mut list = list_request(part(1), 2, 0, 5);
        list.offset = beyond;
        assert!(matches!(
            responder.changed_buckets(&asker, list).await.unwrap(),
            Err(BucketsError::Invalid(_))
        ));
        assert_eq!(rig.store.calls(), calls, "none of them reached the store");

        // The offset past the end of a level is the walk's own relist cursor: not malformed.
        let mut relist = list_request(part(1), 2, 0, 5);
        relist.offset = past_range;
        assert!(
            responder
                .changed_buckets(&asker, relist)
                .await
                .unwrap()
                .is_ok()
        );
    })
}

/// The XRPC wire is JSON, which has no byte strings and no non-string map keys of its own: every
/// request, answer and refusal survives it (the seed's bytes did not, before the wire existed).
#[test]
fn the_wire_types_round_trip_through_json() {
    fn round_trip<
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    >(
        value: &T,
    ) {
        let json = serde_json::to_string(value).unwrap();
        assert_eq!(&serde_json::from_str::<T>(&json).unwrap(), value, "{json}");
    }
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(30).await;
        rig.commit(vec![PartWrite::RemoveMembers(vec![(okey(3), part(1))])])
            .await;
        let responder = rig.responder();
        let asker = asker();

        let summaries = summaries_of(&[(1, 0), (2, 5)]);
        round_trip(&summaries);
        round_trip(
            &responder
                .part_summaries(&asker, summaries)
                .await
                .unwrap()
                .unwrap(),
        );
        round_trip(&PartSummariesError::TooManyParts { cap: 64 });

        let list = list_request(part(1), 1, 0, 100);
        round_trip(&list);
        round_trip(
            &responder
                .changed_buckets(&asker, list)
                .await
                .unwrap()
                .unwrap(),
        );

        let leaves = leaf_request(part(1), vec![(BuckId::ROOT, Some(okey(2)))], 0, Some(5));
        round_trip(&leaves);
        round_trip(
            &responder
                .leaf_buckets(&asker, leaves)
                .await
                .unwrap()
                .unwrap(),
        );
        round_trip(&BucketsError::UnknownPart);
        round_trip(&BucketsError::TooManyBuckets { cap: 64 });
        round_trip(&BucketsError::Invalid("x".into()));
    })
}

#[test]
fn law_5_a_request_naming_too_many_buckets_is_refused_whole() {
    block_on(async {
        let mut rig = Rig::new();
        rig.populate(20).await;
        let responder = rig.responder();
        let asker = asker();
        let naming = |count: usize| {
            // Real, readable buckets: only the count may be what trips the cap.
            let buckets = (0..count)
                .map(|at| (BuckId::new(2, u16::try_from(at).unwrap()), None))
                .collect();
            leaf_request(part(1), buckets, 0, Some(5))
        };
        assert!(
            responder
                .leaf_buckets(&asker, naming(MAX_LEAF_BUCKETS))
                .await
                .unwrap()
                .is_ok(),
            "a request at the cap is answered"
        );
        let calls = rig.store.calls();
        for part_index in [1, 2, 9] {
            let mut request = naming(MAX_LEAF_BUCKETS + 1);
            request.part_key = part(part_index);
            assert_eq!(
                responder.leaf_buckets(&asker, request).await.unwrap(),
                Err(BucketsError::TooManyBuckets {
                    cap: MAX_LEAF_BUCKETS
                }),
                "refused alike whatever the part"
            );
        }
        assert_eq!(rig.store.calls(), calls, "before the store is read");
    })
}
