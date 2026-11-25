//! The bucket walk's pieces in isolation, then end to end through [`harness`]: a requester's
//! machines pulling from a responder's memory part store.

mod harness;
mod last_bucket;
mod scaling;
mod walk;

use super::*;

use crate::part_store::ObjState;
use crate::part_store::PartStoreServe;
use crate::part_store::bucket_summary::BucketFingerprint;
use crate::part_store::memory::MemoryPartStore;
use crate::rev::Rev;
use std::collections::HashMap;

fn part() -> PartKey {
    PartKey(ByteKey::new(&[1; 32]))
}

/// `calc_working_level` must never return a level exceeding `remote_depth`, even at
/// extreme `remote_size`.
#[test]
fn calc_working_level_never_exceeds_remote_depth() {
    assert_eq!(calc_working_level(20_000, 0), 0);
    assert_eq!(calc_working_level(1_000_000, 0), 0);
    assert!(calc_working_level(10_000, 5) <= 5);
    assert_eq!(calc_working_level(0, 0), 0);
    assert_eq!(calc_working_level(0, 10), 0);
}

/// The working level is the shallowest whose child buckets each hold at most
/// `BUFFER_TARGET` members, so each level is reached in turn as the part grows by
/// `ARITY`, at the boundaries on both sides.
///
/// The port as copied compared the child width only on its first step and the working level's
/// own width after (`ARITY.pow(working_level)`), so leaving level 0 and stopping at level 1
/// needed opposite answers to one comparison: level 1 was never returned, and
/// `calc_working_level(16_400, 4)` was 2.
#[test]
fn calc_working_level_reaches_every_level() {
    let target = u64::from(BucketMachine::BUFFER_TARGET);
    let arity = u64::from(BuckId::ARITY);
    let depth = BuckId::MAX_LEVEL;
    assert_eq!(calc_working_level(target * arity, depth), 0);
    assert_eq!(calc_working_level(target * arity + arity, depth), 1);
    assert_eq!(calc_working_level(target * arity.pow(2), depth), 1);
    assert_eq!(
        calc_working_level(target * arity.pow(2) + arity.pow(2), depth),
        2
    );
}

/// A part store that answers bucket summaries from a table. `filter_buckets` reads
/// nothing else, so the remaining methods are unreachable rather than stubbed.
struct SummaryStore {
    summaries: HashMap<BuckId, BucketSummary>,
}

impl PartStoreRead for SummaryStore {
    async fn obj_states(&self, _objs: &[ObjKey]) -> Res<Vec<Option<ObjState>>> {
        unreachable!("filter_buckets does not read objects")
    }

    async fn bucket_summaries(&self, _part: &PartKey, ids: &[BuckId]) -> Res<Vec<BucketSummary>> {
        Ok(ids
            .iter()
            .map(|id| {
                self.summaries
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| panic!("no local summary for {id:?}"))
            })
            .collect())
    }

    async fn member_count(&self, _part: &PartKey) -> Res<u64> {
        unreachable!("filter_buckets does not count members")
    }

    async fn partless_objects(&self, _limit: usize, _after: Option<&ObjKey>) -> Res<Vec<ObjKey>> {
        unreachable!("filter_buckets does not list part-less objects")
    }
}

fn summary(id: BuckId, len: u32, live_count: u32, changed_at: Rev) -> BucketSummary {
    BucketSummary {
        id,
        // only equality matters here, and both sides carry the same pair
        fp: (BucketFingerprint::default(), BucketFingerprint::default()),
        len,
        live_count,
        changed_at,
    }
}

/// Pruning must not rest on fingerprint equality alone. Both buckets below agree on their
/// fingerprint pair, but the second disagrees on the materialized counts, so it is dirty
/// and must be handed off. Fingerprint-only pruning called it clean, left `out` empty and
/// returned `Relist`, silently dropping the difference.
#[test]
fn filter_buckets_equal_fp_different_counts_is_dirty() {
    let clean = BuckId::new(1, 0);
    let dirty = BuckId::new(1, 1);
    let store = SummaryStore {
        summaries: HashMap::from([
            // Counts agree; the watermark disagrees and must not matter.
            (clean, summary(clean, 5, 3, 99)),
            (dirty, summary(dirty, 4, 3, 7)),
        ]),
    };
    let remote = vec![summary(clean, 5, 3, 7), summary(dirty, 5, 3, 7)];

    match futures::executor::block_on(filter_buckets(part(), 1, remote, &store))
        .expect("the summary store reads infallibly")
    {
        FilteredBuckets::Handoff(out) => {
            assert_eq!(out.len(), 1, "only the count-mismatched bucket is dirty");
            assert_eq!(out[0].id, dirty);
        }
        FilteredBuckets::Done => panic!("count-mismatched bucket was pruned as clean"),
        FilteredBuckets::Relist(id) => {
            panic!("count-mismatched bucket was pruned as clean, relisting {id:?}")
        }
    }
}

/// The guard for the test above: equal counts still prune, even when the two sides
/// disagree on `changed_at`.
#[test]
fn filter_buckets_equal_counts_different_watermark_is_clean() {
    let buck = BuckId::ROOT;
    let store = SummaryStore {
        summaries: HashMap::from([(buck, summary(buck, 5, 3, 99))]),
    };
    let remote = vec![summary(buck, 5, 3, 7)];

    match futures::executor::block_on(filter_buckets(part(), 0, remote, &store))
        .expect("the summary store reads infallibly")
    {
        FilteredBuckets::Done => {}
        FilteredBuckets::Handoff(out) => {
            panic!("identical bucket was not pruned, handed off {out:?}")
        }
        FilteredBuckets::Relist(id) => {
            panic!("identical bucket was not pruned, relisted {id:?}")
        }
    }
}

/// Leaf entries compare by the stored payload digest: a local object whose payload matches a
/// remote entry's content is not fetched again, and a different digest is a change. Runs the
/// memory part store.
#[test]
fn filter_objects_compares_payloads_by_stored_digest() {
    use crate::part_store::PartStoreWrite;
    use crate::part_store::PayloadDigest;
    use crate::part_store::memory::MemoryPartStore;
    use crate::sync::rpc::bucket::BucketObjPageEntry;

    let store = MemoryPartStore::default();
    let obj = ObjKey::new(&[7]);
    let payload = serde_json::json!({"held": "here"});
    let digest = PayloadDigest::of(&payload).expect(ERROR_IMPOSSIBLE);
    let mut tx = store.begin();
    futures::executor::block_on(PartStoreWrite::set_payloads(
        &store,
        &mut tx,
        vec![(obj.clone(), payload)],
    ))
    .expect("staging is infallible");
    futures::executor::block_on(PartStoreWrite::add_members(
        &store,
        &mut tx,
        &[(obj.clone(), part())],
    ))
    .expect("staging is infallible");
    tx.commit();

    let seed = FingerprintSeed::new(1, 2);
    let leaf = |digest| {
        Map::from([(
            BuckId::ROOT,
            RawLeafBucketPage {
                after: None,
                entries: vec![BucketObjPageEntry {
                    obj_key: obj.clone(),
                    fp: Some(BucketObjPageEntry::fingerprint(&seed, &obj, digest)),
                }],
                next_after: None,
            },
        )])
    };
    let deltas = |bucks| {
        futures::executor::block_on(filter_objects(part(), bucks, seed, &store))
            .expect("the memory store reads infallibly")
            .remove(&BuckId::ROOT)
            .expect("the bucket was asked for")
            .entries
            .into_iter()
            .map(|entry| (entry.obj_key, entry.delta))
            .collect::<Vec<_>>()
    };

    assert_eq!(deltas(leaf(digest)), vec![], "same content");
    let other = PayloadDigest::of(&serde_json::json!({"held": "changed"})).expect(ERROR_IMPOSSIBLE);
    assert_eq!(
        deltas(leaf(other)),
        vec![(obj.clone(), PartObjDelta::Change)]
    );
}

/// Run a scenario end to end and check what every part came to, against where the part stood
/// before the walk ([`harness::divergence`]).
///
/// - A part the requester may read converges ([`harness::assert_converged`]). The scenario's
///   parts are small, so the working level is 0 and the decision's single listing of the root
///   is the only list request; agreeing roots end there, with no leaf request and no write.
///   Otherwise the leaf requests stay within one per page of the responder's root, the syncs
///   within the live members that differed, the removals within the responder's dead
///   members.
/// - A part it may not read is refused before any listing: no bucket, leaf entry or object of
///   it leaves the responder, the requester's membership in it is untouched, and so is the
///   payload of every object live at the responder only in such parts.
fn walk_converges(scenario: harness::Scenario) -> Res<harness::Driver<MemoryPartStore>> {
    use harness::*;
    futures::executor::block_on(async {
        let responder = MemoryPartStore::default();
        build(&responder, &scenario.responder, &scenario.readable_parts()).await?;
        let requester = MemoryPartStore::default();
        build(&requester, &scenario.requester, &[]).await?;

        let parts = scenario.parts();
        let readable = scenario.readable_parts();
        let mut before = BTreeMap::new();
        for part in &parts {
            before.insert(
                part.clone(),
                (
                    divergence(&requester, &responder, part).await?,
                    membership(&requester, part).await?,
                ),
            );
        }
        // Objects live at the responder only in parts the requester may not read.
        let objs = (0..scenario.responder.len())
            .map(|at| obj(u16::try_from(at).expect(ERROR_IMPOSSIBLE)))
            .collect::<Vec<_>>();
        let hidden = {
            let states = PartStoreRead::obj_states(&responder, &objs).await?;
            objs.iter()
                .zip(states)
                .filter(|(_, state)| {
                    state.as_ref().is_none_or(|state| {
                        state.content.is_none()
                            || state.parts.iter().all(|part| !readable.contains(part))
                    })
                })
                .map(|(obj, _)| obj.clone())
                .collect::<Vec<_>>()
        };
        // Their payloads, that is: the requester may still remove them from a readable part,
        // pending memberships included.
        let payloads = |states: Vec<Option<ObjState>>| {
            states
                .into_iter()
                .map(|state| {
                    state
                        .and_then(|state| state.content)
                        .map(|(payload, _)| payload)
                })
                .collect::<Vec<_>>()
        };
        let hidden_before = payloads(PartStoreRead::obj_states(&requester, &hidden).await?);

        let mut driver = Driver::new(
            requester,
            Responder::new(responder, scenario.leaf_cap).with_leaf_byte_budget(scenario.leaf_bytes),
        );
        driver.run(&parts).await?;

        for part in &parts {
            let (divergence, members_before) = &before[part];
            let traffic = driver
                .responder
                .traffic
                .get(part)
                .copied()
                .unwrap_or_default();
            let applied = driver.applied.get(part).copied().unwrap_or_default();
            let walk = driver.walks[part];
            if readable.contains(part) {
                assert_converged(&driver.requester, driver.responder.store(), part).await?;
                assert_eq!(traffic.list_requests, 1, "{part}: {walk:?}");
                if divergence.root_agrees {
                    assert_eq!(walk, Walk::Clean, "{part}: agreeing roots need no walk");
                    assert_eq!(traffic.leaf_requests, 0);
                    assert_eq!(applied, Applied::default());
                } else {
                    assert!(matches!(
                        walk,
                        Walk::Clean | Walk::Walked { working_level: 0 }
                    ));
                    let page = u64::try_from(scenario.page_entries())
                        .expect(ERROR_IMPOSSIBLE)
                        .min(u64::from(BucketMachine::LEAF_BUCKET_LIMIT_HINT));
                    assert!(
                        traffic.leaf_requests <= divergence.responder_len.div_ceil(page),
                        "{part}: {traffic:?} for {divergence:?}"
                    );
                    assert!(
                        applied.syncs <= divergence.live_differing,
                        "{part}: {applied:?} for {divergence:?}"
                    );
                    assert!(
                        applied.removes <= divergence.dead,
                        "{part}: {applied:?} for {divergence:?}"
                    );
                }
            } else {
                assert_eq!(walk, Walk::Refused);
                assert_eq!(
                    (
                        traffic.list_requests,
                        traffic.buckets_out,
                        traffic.leaf_requests
                    ),
                    (0, 0, 0),
                    "{part} leaked to a requester that may not read it"
                );
                assert_eq!(traffic.fetches, 0);
                assert_eq!(applied, Applied::default());
                // The requester's own pending memberships in the part go live once a payload
                // arrives through a readable part: its intent, not the responder's data.
                let pending = scenario
                    .requester
                    .iter()
                    .enumerate()
                    .filter(|(_, spec)| {
                        spec.payload.is_none()
                            && spec.parts[usize::from(part_index(part))] == Member::Live
                    })
                    .map(|(at, _)| obj(u16::try_from(at).expect(ERROR_IMPOSSIBLE)))
                    .collect::<Vec<_>>();
                let promoted = PartStoreRead::obj_states(&driver.requester, &pending).await?;
                let mut expected = members_before.clone();
                expected.extend(
                    pending
                        .into_iter()
                        .zip(promoted)
                        .filter(|(_, state)| {
                            state.as_ref().is_some_and(|state| state.content.is_some())
                        })
                        .map(|(obj, _)| (obj, true)),
                );
                assert_eq!(membership(&driver.requester, part).await?, expected);
            }
        }
        assert_eq!(
            payloads(PartStoreRead::obj_states(&driver.requester, &hidden).await?),
            hidden_before,
            "an object only in unreadable parts reached the requester"
        );
        Ok(driver)
    })
}

proptest::proptest! {
    #[test]
    fn walks_converge(scenario in harness::scenarios()) {
        walk_converges(scenario)
            .map(drop)
            .map_err(|err| proptest::test_runner::TestCaseError::fail(format!("{err:?}")))?;
    }
}

/// Every kind of member on both sides, identical: each part is settled by the decision's one
/// listing of its root.
#[test]
fn identical_stores_compare_only_the_root() -> Res<()> {
    use harness::*;
    let objs = vec![
        ObjSpec {
            payload: Some(payload(1)),
            parts: vec![Member::Live, Member::Dead, Member::Absent],
        },
        ObjSpec {
            payload: Some(payload(2)),
            parts: vec![Member::Live, Member::Live, Member::Live],
        },
        ObjSpec {
            payload: None,
            parts: vec![Member::Live, Member::Absent, Member::Dead],
        },
        ObjSpec {
            payload: Some(payload(0)),
            parts: vec![Member::Dead, Member::Dead, Member::Live],
        },
    ];
    let scenario = Scenario {
        responder: objs.clone(),
        requester: objs,
        readable: vec![true; usize::from(PARTS)],
        leaf_cap: BucketMachine::LEAF_BUCKET_LIMIT_HINT,
        leaf_bytes: crate::part_store::LEAF_PAGE_BYTE_BUDGET,
    };
    let driver = walk_converges(scenario.clone())?;
    for part in scenario.parts() {
        assert_eq!(driver.walks[&part], Walk::Clean);
        assert_eq!(
            driver.responder.traffic[&part],
            Traffic {
                summary_requests: 1,
                list_requests: 1,
                buckets_out: 1,
                ..default()
            }
        );
    }
    assert!(driver.applied.is_empty());
    Ok(())
}

/// A requester that may read one part, may not read a second and asks for a third the
/// responder does not know: the first converges, the other two are refused alike, before any
/// listing, and nothing of them reaches the requester.
#[test]
fn unauthorized_parts_converge_to_nothing() -> Res<()> {
    use harness::*;
    let live_everywhere = vec![Member::Live, Member::Live, Member::Absent];
    walk_converges(Scenario {
        responder: vec![
            ObjSpec {
                payload: Some(payload(1)),
                parts: live_everywhere.clone(),
            },
            ObjSpec {
                payload: Some(payload(2)),
                parts: vec![Member::Absent, Member::Live, Member::Absent],
            },
            ObjSpec {
                payload: Some(payload(1)),
                parts: vec![Member::Absent, Member::Dead, Member::Absent],
            },
        ],
        requester: vec![
            ObjSpec {
                payload: Some(payload(0)),
                parts: vec![Member::Absent, Member::Absent, Member::Live],
            },
            ObjSpec {
                payload: None,
                parts: vec![Member::Absent; 3],
            },
            ObjSpec {
                payload: Some(payload(1)),
                parts: vec![Member::Absent, Member::Live, Member::Absent],
            },
        ],
        readable: vec![true, false, false],
        leaf_cap: 1,
        leaf_bytes: crate::part_store::LEAF_PAGE_BYTE_BUDGET,
    })
    .map(drop)
}

/// A responder removed object 0 from part 2 and keeps it live in part 1; the requester holds a
/// *pending* membership of object 0 in part 2 (no payload yet). Both parts are readable.
///
/// A pending membership is a membership; only its payload is pending. Part 2's walk reaches
/// object 0's dead leaf entry while the requester's part 2 bucket is empty (a pending
/// membership has no bucket entry), sees the pending membership through `obj_states` and
/// removes it. Part 1's sync then brings the payload with nothing left in part 2 to promote,
/// whichever order the walks run in.
///
/// This was `walks_converge`'s counterexample while pending memberships were invisible to
/// `obj_states`: the dead entry was dropped as one for an object the requester did not hold,
/// and the payload later promoted the membership against the responder's removal.
#[test]
fn a_pending_membership_misses_a_removal_its_payload_overtakes() -> Res<()> {
    use harness::*;
    walk_converges(Scenario {
        responder: vec![ObjSpec {
            payload: Some(payload(0)),
            parts: vec![Member::Absent, Member::Live, Member::Dead],
        }],
        requester: vec![ObjSpec {
            payload: None,
            parts: vec![Member::Absent, Member::Absent, Member::Live],
        }],
        readable: vec![false, true, true],
        leaf_cap: 1,
        leaf_bytes: crate::part_store::LEAF_PAGE_BYTE_BUDGET,
    })
    .map(drop)
}

/// A part past `BUFFER_TARGET * ARITY` members is walked below the root, and the
/// requester's agreeing buckets are pruned by their summaries: only the members of the dirty
/// buckets at the working level are leafed, and the listing pages through the levels once
/// plus a relisting per handoff.
///
/// At this size the walk is at level 1: 16 buckets of about 1_060 members, so the five changes
/// dirty at most five of them, and the members of the clean ones are never leafed.
///
/// The changes: a new payload (object 1), a member the requester lacks (3), a removal (4),
/// another new payload (5) and a membership the requester lacks while holding the payload (6).
#[test]
fn a_large_part_walks_only_its_dirty_buckets() -> Res<()> {
    use harness::*;
    const MEMBERS: u16 = 17_000;
    let working_level = calc_working_level(u64::from(MEMBERS), BuckId::MAX_LEVEL);
    assert_eq!(working_level, 1);
    let spec = |payload, member| ObjSpec {
        payload,
        parts: vec![member],
    };
    let responder = (0..MEMBERS)
        .map(|at| match at {
            4 => spec(Some(payload(0)), Member::Dead),
            _ => spec(Some(payload(0)), Member::Live),
        })
        .collect::<Vec<_>>();
    let requester = (0..MEMBERS)
        .map(|at| match at {
            1 | 5 => spec(Some(payload(1)), Member::Live),
            3 => spec(None, Member::Absent),
            6 => spec(Some(payload(0)), Member::Absent),
            _ => spec(Some(payload(0)), Member::Live),
        })
        .collect::<Vec<_>>();
    futures::executor::block_on(async {
        let part = part(0);
        let responder_store = MemoryPartStore::default();
        build(&responder_store, &responder, std::slice::from_ref(&part)).await?;
        let requester_store = MemoryPartStore::default();
        build(&requester_store, &requester, &[]).await?;

        // The buckets at the working level whose summaries differ, and their members.
        let at_level = PartStoreServe::bucket_list(
            &responder_store,
            &requester_principal(),
            &part,
            BuckId::new(working_level, 0),
            working_level,
            0,
            usize::MAX,
        )
        .await?
        .expect("the part is readable");
        let ids = at_level.iter().map(|bucket| bucket.id).collect::<Vec<_>>();
        let ours = PartStoreRead::bucket_summaries(&requester_store, &part, &ids).await?;
        let dirty = at_level
            .iter()
            .zip(&ours)
            .filter(|(theirs, ours)| !summaries_agree(ours, theirs))
            .map(|(theirs, _)| u64::from(theirs.len))
            .collect::<Vec<_>>();
        let dirty_len = dirty.iter().sum::<u64>();
        assert!(!dirty.is_empty() && dirty.len() < at_level.len());
        // Every bucket from the root through the working level.
        let listable = (0..=u32::from(working_level))
            .map(|level| u64::from(BuckId::ARITY).pow(level))
            .sum::<u64>();

        let mut driver = Driver::new(
            requester_store,
            Responder::new(responder_store, BucketMachine::LEAF_BUCKET_LIMIT_HINT),
        );
        driver.run(std::slice::from_ref(&part)).await?;
        assert_converged(&driver.requester, driver.responder.store(), &part).await?;
        assert_eq!(driver.walks[&part], Walk::Walked { working_level });
        assert_eq!(
            driver.applied[&part],
            Applied {
                syncs: 4,
                removes: 1
            }
        );
        let traffic = driver.responder.traffic[&part];
        assert!(
            traffic.leaf_entries_out <= dirty_len,
            "{traffic:?} leafed past the {dirty_len} members of the dirty buckets"
        );
        let pages = listable.div_ceil(u64::from(BucketMachine::GET_BUCKET_LIMIT_HINT));
        assert!(
            traffic.list_requests <= pages + dirty.len() as u64 + 2,
            "{traffic:?} for {pages} pages of buckets and {dirty} dirty",
            dirty = dirty.len()
        );
        Ok(())
    })
}
