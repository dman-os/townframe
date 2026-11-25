use super::*;

use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Job {
    Sync(u8),
    RemoveFromPart(u8),
}

type Machine = WatermarkMachine<Job, &'static str, u64>;

fn set(sids: &[&'static str]) -> Set<&'static str> {
    sids.iter().copied().collect()
}

/// A stream never seen, or retired, owes nothing.
#[test]
fn an_unknown_or_retired_stream_is_settled() {
    let mut m = Machine::default();
    assert!(m.is_settled(&"p"));
    m.add_job(Job::Sync(1), 5, ["p"]);
    m.retire_stream(&"p");
    assert!(m.is_settled(&"p"));
}

#[test]
fn add_job_opens_a_slot_per_new_stream() {
    let mut m = Machine::default();
    assert_eq!(m.add_job(Job::Sync(1), 5, ["p", "q"]), set(&["p", "q"]));
    assert!(m.owes(&Job::Sync(1), &5));
    assert!(!m.is_settled(&"p"));
    assert_eq!(m.watermark(&"p"), None, "an open slot is not ready");
}

#[test]
fn add_job_skips_re_deliveries_and_emitted_cursors() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 5, ["p"]);
    assert_eq!(
        m.add_job(Job::Sync(1), 5, ["p", "q"]),
        set(&["q"]),
        "p already holds a slot at 5"
    );
    m.settle_on_cursor(&Job::Sync(1), &5);
    assert_eq!(
        m.add_job(Job::Sync(2), 5, ["p"]),
        set(&[]),
        "at the watermark"
    );
    assert_eq!(m.add_job(Job::Sync(2), 3, ["p"]), set(&[]), "below it");
    assert_eq!(m.add_job(Job::Sync(2), 6, ["p"]), set(&["p"]));
}

/// The admission guard compares against the emitted watermark as an `Option`: on a fresh
/// stream the default cursor is a real cursor, not a replay.
#[test]
fn add_job_admits_the_default_cursor_on_a_fresh_stream() {
    let mut m = Machine::default();
    assert_eq!(m.add_job(Job::Sync(1), 0, ["p"]), set(&["p"]));
}

#[test]
fn settle_on_cursor_advances_every_stream_the_waiter_holds() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 7, ["p", "q"]);
    assert_eq!(
        m.settle_on_cursor(&Job::Sync(1), &7),
        vec![("p", Some(7)), ("q", Some(7))]
    );
    assert!(m.is_settled(&"p"));
    assert_eq!(m.pending_jobs_len(), 0);
}

#[test]
fn settle_on_cursor_advances_only_over_the_contiguous_prefix() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 10, ["p"]);
    m.add_job(Job::Sync(2), 11, ["p"]);
    assert_eq!(
        m.settle_on_cursor(&Job::Sync(2), &11),
        vec![("p", None)],
        "10 is still owed"
    );
    assert_eq!(
        m.settle_on_cursor(&Job::Sync(1), &10),
        vec![("p", Some(11))]
    );
    assert!(m.is_settled(&"p"));
}

#[test]
fn settle_on_cursor_is_a_no_op_for_a_stale_or_unknown_job() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 5, ["p"]);
    assert_eq!(m.settle_on_cursor(&Job::Sync(1), &4), vec![]);
    assert_eq!(m.settle_on_cursor(&Job::Sync(9), &5), vec![]);
    assert_eq!(
        m.settle_on_cursor(&Job::RemoveFromPart(1), &5),
        vec![],
        "a different kind of work on the same object is a different job"
    );
    assert!(m.owes(&Job::Sync(1), &5));
}

/// Two kinds of work on one object hold their own slots: settling one never readies the
/// other's.
#[test]
fn distinct_jobs_on_one_object_settle_independently() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(9), 5, ["q"]);
    m.add_job(Job::RemoveFromPart(9), 5, ["p"]);
    assert_eq!(m.settle_on_cursor(&Job::Sync(9), &5), vec![("q", Some(5))]);
    assert!(m.owes(&Job::RemoveFromPart(9), &5));
    assert_eq!(
        m.settle_on_cursor(&Job::RemoveFromPart(9), &5),
        vec![("p", Some(5))]
    );
}

#[test]
fn release_stream_frees_only_that_stream_before_the_cursor() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 30, ["alpha", "beta"]);
    m.add_job(Job::Sync(1), 32, ["alpha"]);

    assert_eq!(m.release_stream(&Job::Sync(1), &"alpha", &31), Some(30));
    assert!(!m.is_settled(&"alpha"), "32 is not before the cursor");
    assert!(
        !m.is_settled(&"beta"),
        "beta still owes the job's work at 30"
    );
    assert_eq!(
        m.settle_on_cursor(&Job::Sync(1), &30),
        vec![("beta", Some(30))]
    );
}

#[test]
fn release_stream_drops_waiters_it_empties() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 2, ["p"]);
    assert_eq!(m.release_stream(&Job::Sync(1), &"p", &3), Some(2));
    assert!(!m.owes(&Job::Sync(1), &2));
    assert_eq!(m.pending_jobs_len(), 0);
    assert_eq!(m.settle_on_cursor(&Job::Sync(1), &2), vec![], "now stale");
}

#[test]
fn release_stream_leaves_other_jobs_alone() {
    let mut m = Machine::default();
    m.add_job(Job::RemoveFromPart(1), 4, ["p"]);
    assert_eq!(m.release_stream(&Job::Sync(1), &"p", &9), None);
    assert!(m.owes(&Job::RemoveFromPart(1), &4));
}

/// Shrunk by the oracle test: releasing a job from a stream the machine has never seen is a
/// no-op, not an invariant break.
#[test]
fn release_stream_on_an_unknown_stream_is_a_no_op() {
    let mut m = Machine::default();
    m.add_job(Job::RemoveFromPart(2), 0, ["b"]);
    assert_eq!(m.release_stream(&Job::RemoveFromPart(2), &"c", &0), None);
    assert!(m.owes(&Job::RemoveFromPart(2), &0));
}

#[test]
fn retire_stream_drops_only_that_stream() {
    let mut m = Machine::default();
    m.add_job(Job::Sync(1), 1, ["gone", "kept"]);
    m.add_job(Job::Sync(2), 2, ["gone"]);
    m.retire_stream(&"gone");
    assert_eq!(m.watermark(&"gone"), None);
    assert!(
        !m.owes(&Job::Sync(2), &2),
        "a waiter that only held the stream is gone"
    );
    assert_eq!(
        m.settle_on_cursor(&Job::Sync(1), &1),
        vec![("kept", Some(1))]
    );
}

// ── model test ────────────────────────────────────────────────────────────────────────────

static STREAMS: [&str; 3] = ["a", "b", "c"];

#[derive(Debug, Clone)]
enum Op {
    AddJob {
        key: Job,
        cursor: u64,
        sids: Vec<&'static str>,
    },
    Settle {
        key: Job,
        cursor: u64,
    },
    Release {
        key: Job,
        sid: &'static str,
        before: u64,
    },
    Retire {
        sid: &'static str,
    },
}

fn any_job() -> impl Strategy<Value = Job> {
    prop_oneof![
        (0u8..3).prop_map(Job::Sync),
        (0u8..3).prop_map(Job::RemoveFromPart)
    ]
}

fn any_sid() -> impl Strategy<Value = &'static str> {
    prop::sample::select(STREAMS.as_slice())
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (any_job(), 0u64..8, prop::sample::subsequence(STREAMS.as_slice(), 1..=3))
            .prop_map(|(key, cursor, sids)| Op::AddJob { key, cursor, sids }),
        3 => (any_job(), 0u64..8).prop_map(|(key, cursor)| Op::Settle { key, cursor }),
        2 => (any_job(), any_sid(), 0u64..9)
            .prop_map(|(key, sid, before)| Op::Release { key, sid, before }),
        1 => any_sid().prop_map(|sid| Op::Retire { sid }),
    ]
}

/// The naive reading of the contract, with no waiter structure: each slot remembers the job
/// it still owes (`None` once ready), and a stream's watermark is its contiguous ready prefix.
#[derive(Default)]
struct Oracle {
    slots: BTreeMap<(&'static str, u64), Option<Job>>,
    emitted: BTreeMap<&'static str, u64>,
}

impl Oracle {
    fn drain(&mut self, sid: &'static str) -> Option<u64> {
        let mut reached = None;
        while let Some((&(_, cursor), None)) = self.slots.range((sid, 0)..=(sid, u64::MAX)).next() {
            self.slots.remove(&(sid, cursor));
            reached = Some(cursor);
        }
        if let Some(cursor) = reached {
            self.emitted.insert(sid, cursor);
        }
        reached
    }

    fn owes(&self, key: Job, cursor: u64) -> bool {
        self.slots
            .iter()
            .any(|(&(_, slot_cursor), owed)| slot_cursor == cursor && *owed == Some(key))
    }

    fn pending(&self) -> usize {
        self.slots
            .iter()
            .filter_map(|(&(_, cursor), owed)| owed.map(|key| (key, cursor)))
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }
}

proptest! {
    #[test]
    fn matches_the_naive_oracle(ops in prop::collection::vec(any_op(), 1..40)) {
        let mut m = Machine::default();
        let mut oracle = Oracle::default();
        for op in ops {
            match op {
                Op::AddJob { key, cursor, sids } => {
                    let mut expected = Set::new();
                    for sid in &sids {
                        let stale = oracle.emitted.get(sid).is_some_and(|&emitted| cursor <= emitted);
                        if stale || oracle.slots.contains_key(&(*sid, cursor)) {
                            continue;
                        }
                        oracle.slots.insert((*sid, cursor), Some(key));
                        expected.insert(*sid);
                    }
                    prop_assert_eq!(m.add_job(key, cursor, sids), expected);
                }
                Op::Settle { key, cursor } => {
                    let owed: Vec<_> = oracle
                        .slots
                        .iter()
                        .filter(|(slot, owed)| slot.1 == cursor && **owed == Some(key))
                        .map(|(slot, _)| slot.0)
                        .collect();
                    let mut expected = vec![];
                    for sid in owed {
                        oracle.slots.insert((sid, cursor), None);
                        expected.push((sid, oracle.drain(sid)));
                    }
                    prop_assert_eq!(m.settle_on_cursor(&key, &cursor), expected);
                }
                Op::Release { key, sid, before } => {
                    let owed: Vec<_> = oracle
                        .slots
                        .range((sid, 0)..(sid, before))
                        .filter(|(_, owed)| **owed == Some(key))
                        .map(|(&slot, _)| slot)
                        .collect();
                    for slot in &owed {
                        oracle.slots.insert(*slot, None);
                    }
                    let expected = if owed.is_empty() { None } else { oracle.drain(sid) };
                    prop_assert_eq!(m.release_stream(&key, &sid, &before), expected);
                }
                Op::Retire { sid } => {
                    oracle.slots.retain(|&(slot_sid, _), _| slot_sid != sid);
                    oracle.emitted.remove(sid);
                    m.retire_stream(&sid);
                }
            }
            for sid in STREAMS {
                prop_assert_eq!(m.watermark(&sid), oracle.emitted.get(sid).copied());
                let oracle_settled = !oracle.slots.keys().any(|&(slot_sid, _)| slot_sid == sid);
                prop_assert_eq!(m.is_settled(&sid), oracle_settled);
            }
            for key in [Job::Sync(0), Job::Sync(1), Job::Sync(2), Job::RemoveFromPart(0), Job::RemoveFromPart(1), Job::RemoveFromPart(2)] {
                for cursor in 0..8 {
                    prop_assert_eq!(m.owes(&key, &cursor), oracle.owes(key, cursor));
                }
            }
            prop_assert_eq!(m.pending_jobs_len(), oracle.pending());
        }
    }
}
