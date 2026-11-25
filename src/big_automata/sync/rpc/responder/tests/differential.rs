//! The responder against the part oracle over random stores, grants and requests.
//!
//! The expectation is the dumbest reading of the laws over the oracle: every answer is computed
//! from the oracle's unlimited reads and cut by the law's own words, with no store call shaped
//! like the responder's (no one-past probe, no headroom read).

use proptest::prelude::*;

use super::*;
use crate::part_store::oracle::PartWrite;

const OBJS: u16 = 6;
const PARTS: u8 = 3;
const PRINCIPALS: u8 = 3;
/// Small, so a handful of members already pages.
const LEAF_CAP: u32 = 3;
/// Between one wide entry and two: a page of wide keys stops at the budget, one of narrow keys
/// at the cap, and the widest key of all is over the budget by itself.
const LEAF_BYTES: usize = 700;

/// An object key whose width follows its index (2, 402 or 902 bytes: entries of 14, 415 and 915
/// bytes), so the byte budget meets entries of different cost in one bucket.
fn dkey(index: u16) -> ObjKey {
    let mut bytes = index.to_be_bytes().to_vec();
    bytes.resize([2, 402, 902][usize::from(index % 3)], 0);
    ObjKey::new(&bytes)
}

#[derive(Debug, Clone)]
enum Op {
    Commit(Vec<PartWrite>),
    Summaries {
        principal: u8,
        parts: Vec<(u8, u8)>,
    },
    List {
        principal: u8,
        part: u8,
        from: (u8, u8),
        to_level: u8,
        since: u8,
        limit: u32,
    },
    Leaves {
        principal: u8,
        part: u8,
        buckets: Vec<(u8, u16, u8, u16)>,
        since: u8,
        hint: Option<u32>,
    },
}

fn writes() -> impl Strategy<Value = Vec<PartWrite>> {
    use proptest::collection::vec;
    let obj = || (0..OBJS).prop_map(dkey);
    let part = || (0..PARTS).prop_map(crate::part_store::laws::part);
    let who = || (0..PRINCIPALS).prop_map(crate::part_store::laws::principal);
    let write = prop_oneof![
        3 => vec((obj(), 0u8..3), 1..4)
            .prop_map(|set| PartWrite::SetPayloads(
                set.into_iter().map(|(obj, value)| (obj, serde_json::json!(value))).collect()
            )),
        4 => vec((obj(), part()), 1..4).prop_map(PartWrite::AddMembers),
        2 => vec((obj(), part()), 1..3).prop_map(PartWrite::RemoveMembers),
        3 => vec((part(), who()), 1..3).prop_map(|grants| PartWrite::GrantAccess(
            grants.into_iter().map(|(part, who)| (part, who, AccessLevel::Read)).collect()
        )),
        1 => vec((part(), who()), 1..2).prop_map(PartWrite::RevokeAccess),
    ];
    vec(write, 1..4)
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    use proptest::collection::vec;
    let op = prop_oneof![
        4 => writes().prop_map(Op::Commit),
        2 => (0..PRINCIPALS, vec((0..PARTS + 1, 0u8..8), 0..5))
            .prop_map(|(principal, parts)| Op::Summaries { principal, parts }),
        3 => (0..PRINCIPALS, 0..PARTS + 1, (0u8..3, 0u8..4), 0u8..4, 0u8..8, 0u32..6)
            .prop_map(|(principal, part, from, to_level, since, limit)| Op::List {
                principal, part, from, to_level, since, limit
            }),
        4 => (
            0..PRINCIPALS,
            0..PARTS + 1,
            vec((0u8..3, 0..OBJS, 0u8..4, 0..OBJS), 1..4),
            0u8..8,
            proptest::option::of(0u32..6),
        )
            .prop_map(|(principal, part, buckets, since, hint)| Op::Leaves {
                principal, part, buckets, since, hint
            }),
    ];
    vec(op, 1..30)
}

/// The bucket of object `index` at `level`.
fn bucket(level: u8, index: u16) -> BuckId {
    BuckId::from_obj_key(level, &dkey(index))
}

async fn run(ops: Vec<Op>) {
    let mut rig = Rig::new();
    let responder = rig
        .responder()
        .with_leaf_cap(LEAF_CAP)
        .with_leaf_byte_budget(LEAF_BYTES);
    for op in ops {
        match op {
            Op::Commit(writes) => {
                rig.commit(writes).await;
            }
            Op::Summaries { principal, parts } => {
                let principal = principal_of(principal);
                let request = PartSummariesRequest {
                    parts: parts
                        .iter()
                        .map(|&(p, since)| (part(p), u64::from(since)))
                        .collect(),
                };
                let expected = request
                    .parts
                    .iter()
                    .filter_map(|(part, since)| {
                        Some((
                            part.clone(),
                            rig.oracle.part_summary(&principal, part, *since)?,
                        ))
                    })
                    .collect::<Map<_, _>>();
                let answer = responder.part_summaries(&principal, request).await.unwrap();
                assert_eq!(answer, Ok(expected));
            }
            Op::List {
                principal,
                part: p,
                from,
                to_level,
                since,
                limit,
            } => {
                let principal = principal_of(principal);
                let from = bucket(from.0, u16::from(from.1));
                let mut request = list_request(part(p), to_level, u64::from(since), limit);
                request.offset = from;
                let expected = rig
                    .oracle
                    .bucket_list(
                        &principal,
                        &part(p),
                        from,
                        to_level,
                        u64::from(since),
                        usize::MAX,
                    )
                    .map(|all| {
                        let limit = (limit.min(MAX_LIST_BUCKETS)) as usize;
                        // `limit` buckets, then the last one's siblings: the same parent, which
                        // bucket order keeps adjacent.
                        let mut cut = all.iter().take(limit).cloned().collect::<Vec<_>>();
                        if let Some(last) = cut.last().map(|last| last.id) {
                            cut.extend(
                                all.iter()
                                    .skip(limit)
                                    .take_while(|buck| {
                                        buck.id.level() == last.level()
                                            && buck.id.parent() == last.parent()
                                    })
                                    .cloned(),
                            );
                        }
                        cut
                    })
                    .ok_or(BucketsError::UnknownPart);
                assert_eq!(
                    responder
                        .changed_buckets(&principal, request)
                        .await
                        .unwrap(),
                    expected
                );
            }
            Op::Leaves {
                principal,
                part: p,
                buckets,
                since,
                hint,
            } => {
                let principal = principal_of(principal);
                let asked = buckets
                    .iter()
                    .map(|&(level, bucket_of, kind, other)| {
                        let after = match kind {
                            0 => None,
                            // in the bucket by construction
                            1 | 2 => Some(dkey(bucket_of)),
                            _ => Some(dkey(other)),
                        };
                        (bucket(level, bucket_of), after)
                    })
                    .collect::<Map<_, _>>();
                let request = leaf_request(
                    part(p),
                    asked.clone().into_iter().collect(),
                    u64::from(since),
                    hint,
                );
                let seed = request.seed;
                let malformed = asked.iter().any(|(buck, after)| {
                    after
                        .as_ref()
                        .is_some_and(|after| BuckId::from_obj_key(buck.level(), after) != *buck)
                });
                let limit = hint.map_or(1, |hint| hint.max(1)).min(LEAF_CAP) as usize;
                let expected = rig
                    .oracle
                    .bucket_leaves(
                        &principal,
                        &part(p),
                        &asked
                            .iter()
                            .map(|(b, a)| (*b, a.clone()))
                            .collect::<Vec<_>>(),
                        u64::from(since),
                        usize::MAX,
                    )
                    .map(|all| {
                        asked
                            .iter()
                            .zip(all)
                            .map(|((buck, after), entries)| {
                                // The entries up to the limit, stopping before the one that
                                // would carry the page past the budget; the first is always in.
                                let mut bytes = 0;
                                let mut page = Vec::new();
                                for entry in entries.iter().take(limit) {
                                    let cost =
                                        BucketObjPageEntry::wire_bytes(entry.obj.as_bytes().len());
                                    if !page.is_empty() && bytes + cost > LEAF_BYTES {
                                        break;
                                    }
                                    bytes += cost;
                                    page.push(entry.clone());
                                }
                                let next_after = (page.len() < entries.len())
                                    .then(|| page.last().expect("a non-empty page").obj.clone());
                                let page = LeafBucketPage {
                                    after: after.clone(),
                                    entries: page
                                        .into_iter()
                                        .map(|entry| BucketObjPageEntry {
                                            fp: entry.live.map(|digest| {
                                                BucketObjPageEntry::fingerprint(
                                                    &seed, &entry.obj, digest,
                                                )
                                            }),
                                            obj_key: entry.obj,
                                        })
                                        .collect(),
                                    next_after,
                                };
                                (*buck, page)
                            })
                            .collect::<Map<_, _>>()
                    })
                    .ok_or(BucketsError::UnknownPart);
                let calls = rig.store.calls();
                let answer = responder.leaf_buckets(&principal, request).await.unwrap();
                if malformed {
                    assert!(
                        matches!(answer, Err(BucketsError::Invalid(_))),
                        "{answer:?}"
                    );
                    assert_eq!(rig.store.calls(), calls, "refused before the store");
                } else {
                    assert_eq!(answer, expected);
                }
            }
        }
    }
}

fn principal_of(index: u8) -> PeerKey {
    principal(index)
}

proptest! {
    #[test]
    fn the_responder_answers_as_the_oracle_does(ops in ops()) {
        futures::executor::block_on(run(ops));
    }
}
