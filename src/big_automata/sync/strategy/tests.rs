use super::*;

use crate::part_store::PartDirtyCount;
use proptest::prelude::*;

fn part(seed: u8) -> PartKey {
    PartKey::new(&[seed; 32])
}

fn summary(latest_cursor: Rev, dirty_total: u64) -> PartSummary {
    PartSummary {
        generation: Generation::default(),
        latest_cursor,
        member_count: 1,
        deepest_bucket_level: BuckId::MAX_LEVEL,
        dirty: PartDirtyCount {
            member_changes: dirty_total,
            access_changes: 0,
        },
    }
}

/// Decide one part, default mode `CursorAndBucket` unless `mode` names one.
fn decide_one(summary: PartSummary, mode: Option<AllowedStrat>) -> PeerPartStrat {
    let key = part(1);
    let modes = mode.map(|mode| (key.clone(), mode)).into_iter().collect();
    decide_peer_strat(
        &Set::from([key.clone()]),
        &Map::from([(key.clone(), summary)]),
        &modes,
        AllowedStrat::CursorAndBucket,
    )
    .remove(&key)
    .unwrap()
}

/// The band comes from the dirty count the peer published, not the cursor gap: a stored
/// cursor trailing by 100_000 with one relevant change stays on cursor replay.
#[test]
fn large_cursor_gap_with_small_dirty_count_stays_on_cursor() {
    assert_eq!(
        decide_one(summary(100_000, 1), None),
        PeerPartStrat::Cursor {
            generation: Generation::default(),
            latest: 100_000
        }
    );
}

/// The converse: a tiny gap with a lot of relevant change walks buckets.
#[test]
fn small_cursor_gap_with_large_dirty_count_walks_buckets() {
    let mut sum = summary(12, 0);
    sum.dirty = PartDirtyCount {
        member_changes: 200,
        access_changes: 100,
    };
    assert_eq!(
        decide_one(sum, None),
        PeerPartStrat::BucketWalk {
            generation: Generation::default(),
            working_level: 0,
            remote_depth: BuckId::MAX_LEVEL,
            remote_len: 1,
            latest: 12
        }
    );
}

/// The threshold is inclusive on the cursor side, and access changes count toward it.
#[test]
fn dirty_threshold_is_inclusive_and_counts_access_changes() {
    let at = BucketMachine::BUCKET_DIRTY_THRESHOLD;
    assert!(matches!(
        decide_one(summary(5, at), None),
        PeerPartStrat::Cursor { .. }
    ));
    let mut over = summary(5, at);
    over.dirty.access_changes = 1;
    assert!(matches!(
        decide_one(over, None),
        PeerPartStrat::BucketWalk { .. }
    ));
}

/// The walk's working level follows the part's size and depth.
#[test]
fn walk_carries_the_working_level_of_the_summary() {
    let mut sum = summary(3, 1_000);
    sum.member_count = 10_000_000;
    sum.deepest_bucket_level = 3;
    let PeerPartStrat::BucketWalk {
        working_level,
        remote_depth,
        remote_len,
        ..
    } = decide_one(sum, None)
    else {
        panic!("expected a walk");
    };
    assert_eq!(
        working_level,
        calc_working_level(10_000_000, 3),
        "the level comes from calc_working_level"
    );
    assert!(working_level > 0);
    assert_eq!((remote_depth, remote_len), (3, 10_000_000));
}

/// The per-part mode overrides the default in both directions.
#[test]
fn per_part_mode_overrides_the_default() {
    let key = part(1);
    let other = part(2);
    let sums = Map::from([
        (key.clone(), summary(5, 1_000)),
        (other.clone(), summary(5, 1_000)),
    ]);
    let decided = decide_peer_strat(
        &Set::from([key.clone(), other.clone()]),
        &sums,
        &Map::from([(key.clone(), AllowedStrat::Cursor)]),
        AllowedStrat::CursorAndBucket,
    );
    assert!(matches!(decided[&key], PeerPartStrat::Cursor { .. }));
    assert!(matches!(decided[&other], PeerPartStrat::BucketWalk { .. }));
}

/// Per-part refusal: a part absent from the summaries is `Unknown` and leaves the readable
/// one decided.
#[test]
fn a_refused_part_of_a_batch_leaves_the_readable_part_decided() {
    let readable = part(1);
    let refused = part(2);
    let decided = decide_peer_strat(
        &Set::from([readable.clone(), refused.clone()]),
        &Map::from([(readable.clone(), summary(100, 1))]),
        &Map::new(),
        AllowedStrat::Cursor,
    );
    assert_eq!(
        decided[&readable],
        PeerPartStrat::Cursor {
            generation: Generation::default(),
            latest: 100
        }
    );
    assert_eq!(decided[&refused], PeerPartStrat::Unknown);
}

fn any_summary() -> impl Strategy<Value = PartSummary> {
    (
        0..3u128,
        any::<Rev>(),
        any::<u64>(),
        any::<u8>(),
        any::<u64>(),
        0..=1u64,
    )
        .prop_map(
            |(
                generation,
                latest_cursor,
                member_count,
                deepest_bucket_level,
                member_changes,
                access_changes,
            )| {
                PartSummary {
                    generation: Generation(Uuid::from_u128(generation)),
                    latest_cursor,
                    member_count,
                    deepest_bucket_level,
                    dirty: PartDirtyCount {
                        member_changes,
                        access_changes,
                    },
                }
            },
        )
}

proptest! {
    #[test]
    fn decisions_hold_their_invariants(
        // Seeds 0..8 are asked of the peer, and the peer answers a subset of seeds 0..12.
        asked in proptest::collection::btree_set(0u8..8, 0..8),
        answered in proptest::collection::btree_map(0u8..12, any_summary(), 0..12),
        cursor_only in proptest::collection::btree_set(0u8..12, 0..12),
        default_cursor_only in any::<bool>(),
    ) {
        let parts: Set<PartKey> = asked.iter().map(|seed| part(*seed)).collect();
        let summaries: Map<PartKey, PartSummary> =
            answered.iter().map(|(seed, sum)| (part(*seed), *sum)).collect();
        let modes: Map<PartKey, AllowedStrat> = cursor_only
            .iter()
            .map(|seed| (part(*seed), AllowedStrat::Cursor))
            .collect();
        let default_mode = if default_cursor_only {
            AllowedStrat::Cursor
        } else {
            AllowedStrat::CursorAndBucket
        };
        let decided = decide_peer_strat(&parts, &summaries, &modes, default_mode);
        prop_assert_eq!(decided.len(), parts.len());
        for part_key in &parts {
            let strat = decided[part_key];
            let Some(sum) = summaries.get(part_key) else {
                // (2) absent from the summaries
                prop_assert_eq!(strat, PeerPartStrat::Unknown);
                continue;
            };
            let forced = match modes.get(part_key) {
                Some(mode) => matches!(mode, AllowedStrat::Cursor),
                None => default_cursor_only,
            };
            match strat {
                PeerPartStrat::Unknown => prop_assert!(false, "answered part decided Unknown"),
                PeerPartStrat::Cursor { generation, latest } => {
                    prop_assert_eq!(generation, sum.generation);
                    prop_assert_eq!(latest, sum.latest_cursor);
                }
                PeerPartStrat::BucketWalk { generation, latest, .. } => {
                    prop_assert_eq!(generation, sum.generation);
                    // (1) CursorOnly never walks
                    prop_assert!(!forced);
                    // (3) the handover point is the summary's latest cursor
                    prop_assert_eq!(latest, sum.latest_cursor);
                }
            }
        }
    }
}

/// A peer with no bucket levels offers no walk, so even a large dirty count stays on the cursor.
#[test]
fn a_peer_without_buckets_stays_on_the_cursor() {
    let part = PartKey::new(&[1]);
    let summary = PartSummary {
        generation: Generation::default(),
        latest_cursor: 9,
        member_count: 100_000,
        deepest_bucket_level: 0,
        dirty: crate::part_store::PartDirtyCount {
            member_changes: BucketMachine::BUCKET_DIRTY_THRESHOLD + 1,
            access_changes: 0,
        },
    };
    let decided = decide_peer_strat(
        &[part.clone()].into(),
        &[(part.clone(), summary)].into(),
        &default(),
        AllowedStrat::CursorAndBucket,
    );
    assert!(matches!(
        decided[&part],
        PeerPartStrat::Cursor { latest: 9, .. }
    ));
}
