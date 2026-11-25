//! Law tests and the differential proptest of [`PartLifecycle`].

use super::*;

mod sim;

fn part(n: u8) -> PartKey {
    PartKey::new(&[100 + n])
}

pub(super) fn config() -> Config {
    Config {
        max_parts: 2,
        min_backoff: Duration::from_secs(2),
        max_backoff: Duration::from_secs(8),
    }
}

/// The peer's generation number `n`; tests that do not care about generations use 0.
fn generation(n: u128) -> Generation {
    Generation(Uuid::from_u128(n))
}

/// The cursor a part holds after consuming `rev` of generation 0; `None` for 0, a part never
/// seen.
fn held(rev: Rev) -> Option<PeerCursor> {
    (rev > 0).then(|| PeerCursor {
        generation: generation(0),
        rev,
    })
}

fn cursor_strat(latest: Rev) -> PeerPartStrat {
    cursor_in(0, latest)
}

fn walk_strat(latest: Rev) -> PeerPartStrat {
    walk_in(0, latest)
}

fn walk_in(n: u128, latest: Rev) -> PeerPartStrat {
    PeerPartStrat::BucketWalk {
        generation: generation(n),
        working_level: 1,
        remote_depth: 2,
        remote_len: 100,
        latest,
    }
}

fn cursor_in(n: u128, latest: Rev) -> PeerPartStrat {
    PeerPartStrat::Cursor {
        generation: generation(n),
        latest,
    }
}

fn answered(entries: &[(u8, PeerPartStrat)]) -> DecideOutcome {
    DecideOutcome::Answered(entries.iter().map(|(p, s)| (part(*p), *s)).collect())
}

struct Rig {
    lc: PartLifecycle,
    now: Instant,
}

impl Rig {
    fn new() -> Self {
        Self::with(config())
    }

    fn with(config: Config) -> Self {
        Self {
            lc: PartLifecycle::new(config),
            now: Instant::now(),
        }
    }

    /// Want part `p`, holding `cursor` of generation 0 (0: never seen).
    fn add(&mut self, p: u8, cursor: Rev) {
        self.lc.add(part(p), held(cursor));
    }

    fn remove(&mut self, p: u8) -> Vec<LifecycleCommand> {
        let mut out = vec![];
        self.lc.remove(&part(p), &mut out);
        out
    }

    fn flush(&mut self) -> Vec<LifecycleCommand> {
        let mut out = vec![];
        self.lc.flush(self.now, &mut out);
        out
    }

    fn wait(&mut self, secs: u64) {
        self.now += Duration::from_secs(secs);
    }

    fn decided(&mut self, call: CallId, outcome: DecideOutcome) -> Vec<LifecycleCommand> {
        let mut out = vec![];
        self.lc.decided(call, outcome, &mut out);
        out
    }

    fn applied(&mut self, p: u8, cursor: Rev) -> Vec<LifecycleCommand> {
        let mut out = vec![];
        self.lc.applied(&part(p), cursor, &mut out);
        out
    }

    fn upgrade(&mut self, p: u8) -> Vec<LifecycleCommand> {
        let mut out = vec![];
        self.lc.upgrade(&part(p), &mut out);
        out
    }

    fn phase(&self, p: u8) -> Option<Phase> {
        self.lc.phase(&part(p))
    }

    /// Flush, expecting exactly one `Decide`, and return its call and the parts it names.
    fn decide(&mut self) -> (CallId, Vec<u8>) {
        let mut out = self.flush();
        assert_eq!(out.len(), 1, "one decide expected, got {out:?}");
        let LifecycleCommand::Decide { call, parts } = out.remove(0) else {
            panic!("not a decide")
        };
        let mut named: Vec<u8> = parts.keys().map(|k| k.as_bytes()[0] - 100).collect();
        named.sort();
        (call, named)
    }

    /// One part decided as a cursor part, through a call of its own.
    fn cursor_part(&mut self, p: u8, held: Rev) {
        self.add(p, held);
        let (call, _) = self.decide();
        self.decided(call, answered(&[(p, cursor_strat(held))]));
        assert_eq!(self.phase(p), Some(Phase::Cursor));
    }

    /// One part decided as a walking part, through a call of its own.
    fn walking_part(&mut self, p: u8, held: Rev, latest: Rev) {
        self.add(p, held);
        let (call, _) = self.decide();
        self.decided(call, answered(&[(p, walk_strat(latest))]));
        assert_eq!(self.phase(p), Some(Phase::Walking));
    }
}

fn persists(cmds: &[LifecycleCommand]) -> Vec<(u8, Rev)> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            LifecycleCommand::PersistCursor { part, cursor } => {
                Some((part.as_bytes()[0] - 100, cursor.rev))
            }
            _ => None,
        })
        .collect()
}

fn retires(cmds: &[LifecycleCommand]) -> Vec<u8> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            LifecycleCommand::Retire { part } => Some(part.as_bytes()[0] - 100),
            _ => None,
        })
        .collect()
}

// Law 1

#[test]
fn law_1_every_wanted_part_has_one_phase_and_an_unwanted_one_has_none() {
    let mut rig = Rig::new();
    assert_eq!(rig.phase(1), None);
    rig.add(1, 0);
    assert_eq!(rig.phase(1), Some(Phase::Deciding));
    let (call, _) = rig.decide();
    assert_eq!(
        rig.phase(1),
        Some(Phase::Deciding),
        "in a call is still deciding"
    );
    rig.decided(call, answered(&[]));
    assert_eq!(rig.phase(1), Some(Phase::Unanswered));
    rig.remove(1);
    assert_eq!(rig.phase(1), None);
}

#[test]
fn law_1_add_on_a_wanted_part_and_remove_on_an_unwanted_one_are_no_ops() {
    let mut rig = Rig::new();
    assert!(rig.remove(1).is_empty());
    rig.add(1, 3);
    rig.add(1, 9);
    let mut out = rig.flush();
    let LifecycleCommand::Decide { call, parts } = out.remove(0) else {
        panic!()
    };
    assert_eq!(
        parts,
        Map::from([(part(1), 3)]),
        "the second add changed nothing"
    );
    rig.decided(call, answered(&[(1, cursor_strat(5))]));
    rig.add(1, 9);
    assert_eq!(rig.phase(1), Some(Phase::Cursor));
    assert_eq!(rig.lc.cursor(&part(1)), Some(3));
    assert!(rig.remove(2).is_empty());
}

#[test]
fn law_1_remove_retires_a_part_that_left_deciding_exactly_once() {
    let mut rig = Rig::new();
    rig.cursor_part(1, 4);
    rig.walking_part(2, 0, 6);
    assert_eq!(retires(&rig.remove(1)), [1]);
    assert_eq!(retires(&rig.remove(1)), [] as [u8; 0]);
    assert_eq!(retires(&rig.remove(2)), [2]);
    assert_eq!(retires(&rig.remove(2)), [] as [u8; 0]);
}

#[test]
fn law_1_remove_while_deciding_or_unanswered_retires_nothing() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    assert!(rig.remove(1).is_empty(), "queued");
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, _) = rig.decide();
    assert!(rig.remove(1).is_empty(), "in a call");
    rig.decided(call, answered(&[]));
    assert_eq!(rig.phase(2), Some(Phase::Unanswered));
    assert!(rig.remove(2).is_empty(), "unanswered");
}

#[test]
fn law_1_a_removed_part_can_be_added_again_and_retires_again() {
    let mut rig = Rig::new();
    rig.cursor_part(1, 0);
    assert_eq!(retires(&rig.remove(1)), [1]);
    rig.cursor_part(1, 0);
    assert_eq!(retires(&rig.remove(1)), [1]);
}

// Law 2

#[test]
fn law_2_a_call_names_at_most_max_parts_oldest_queued_first() {
    let mut rig = Rig::new();
    for p in [3, 1, 2, 0] {
        rig.add(p, 0);
    }
    let (call, named) = rig.decide();
    assert_eq!(named, [1, 3], "parts 3 and 1 were queued first");
    assert!(rig.flush().is_empty(), "law 2: one call in flight");
    rig.decided(
        call,
        answered(&[(3, cursor_strat(1)), (1, cursor_strat(1))]),
    );
    let (_, named) = rig.decide();
    assert_eq!(named, [0, 2]);
}

#[test]
fn law_2_parts_added_while_a_call_is_in_flight_wait_for_the_next_call() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let (call, named) = rig.decide();
    assert_eq!(named, [1]);
    rig.add(2, 0);
    rig.wait(60);
    assert!(
        rig.flush().is_empty(),
        "nothing restarts or doubles the call in flight"
    );
    rig.decided(call, answered(&[(1, cursor_strat(1))]));
    assert_eq!(rig.decide().1, [2]);
}

#[test]
fn law_2_removing_and_adding_parts_does_not_restart_the_call_in_flight() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let (call, _) = rig.decide();
    rig.remove(1);
    rig.add(2, 0);
    rig.add(1, 0);
    assert!(rig.flush().is_empty());
    assert_eq!(rig.lc.flight.as_ref().map(|f| f.call), Some(call));
}

#[test]
fn law_2_a_part_the_cap_leaves_out_goes_in_a_later_call_not_a_latch() {
    let mut rig = Rig::new();
    for p in 0..4 {
        rig.add(p, 0);
    }
    let (call, named) = rig.decide();
    assert_eq!(named.len(), 2);
    rig.decided(
        call,
        answered(&[(named[0], cursor_strat(1)), (named[1], cursor_strat(1))]),
    );
    let (call, named) = rig.decide();
    assert_eq!(named, [2, 3]);
    rig.decided(
        call,
        answered(&[(2, cursor_strat(1)), (3, cursor_strat(1))]),
    );
    assert!((0..4).all(|p| rig.phase(p) == Some(Phase::Cursor)));
}

#[test]
fn law_2_oldest_queued_first_holds_across_new_and_retried_parts() {
    let mut rig = Rig::with(Config {
        max_parts: 1,
        ..config()
    });
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, named) = rig.decide();
    assert_eq!(named, [1]);
    rig.decided(call, answered(&[]));
    // part 1 is backed off; the older queued part 2 goes first and its decision opens the lane
    let (call, named) = rig.decide();
    assert_eq!(named, [2]);
    rig.decided(call, answered(&[(2, cursor_strat(1))]));
    rig.add(3, 0);
    assert_eq!(
        rig.decide().1,
        [1],
        "part 1 has waited longer than the new part 3"
    );
}

// Law 3

#[test]
fn law_3_a_removed_part_ignores_its_entry_and_the_rest_of_the_answer_applies() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, _) = rig.decide();
    rig.remove(1);
    let out = rig.decided(
        call,
        answered(&[(1, cursor_strat(5)), (2, cursor_strat(5))]),
    );
    assert_eq!(
        out,
        [LifecycleCommand::StartCursor {
            part: part(2),
            from: 0
        }]
    );
    assert_eq!(rig.phase(1), None);
    assert_eq!(rig.phase(2), Some(Phase::Cursor));
}

#[test]
fn law_3_a_removed_and_readded_part_ignores_the_old_entry() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, _) = rig.decide();
    rig.remove(1);
    rig.add(1, 7);
    let out = rig.decided(call, answered(&[(1, walk_strat(5)), (2, cursor_strat(5))]));
    assert_eq!(
        out,
        [LifecycleCommand::StartCursor {
            part: part(2),
            from: 0
        }]
    );
    assert_eq!(
        rig.phase(1),
        Some(Phase::Deciding),
        "the new incarnation still waits"
    );
    assert_eq!(rig.decide().1, [1]);
}

#[test]
fn law_3_a_readded_part_is_not_decided_by_an_answer_to_a_later_call_of_its_old_life() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let (old, _) = rig.decide();
    rig.remove(1);
    rig.add(1, 0);
    rig.decided(old, answered(&[]));
    let (new, _) = rig.decide();
    assert!(
        rig.decided(old, answered(&[(1, cursor_strat(1))]))
            .is_empty(),
        "old call is over"
    );
    assert_eq!(rig.phase(1), Some(Phase::Deciding));
    assert_eq!(rig.decided(new, answered(&[(1, cursor_strat(1))])).len(), 1);
}

#[test]
fn law_3_an_event_for_another_call_changes_nothing() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let (call, _) = rig.decide();
    for outcome in [
        answered(&[(1, cursor_strat(1))]),
        DecideOutcome::Failed,
        DecideOutcome::TooManyParts { cap: 1 },
    ] {
        assert!(rig.decided(call + 1, outcome.clone()).is_empty());
        assert!(rig.decided(call - 1, outcome).is_empty());
    }
    assert_eq!(rig.lc.max_parts(), 2);
    assert!(rig.flush().is_empty(), "the call is still in flight");
    rig.decided(call, answered(&[(1, cursor_strat(1))]));
    assert_eq!(rig.phase(1), Some(Phase::Cursor));
}

// Law 4

#[test]
fn law_4_a_missing_or_unknown_part_is_retried_after_backoff() {
    let mut rig = Rig::with(Config {
        max_parts: 3,
        ..config()
    });
    for p in 1..=3 {
        rig.add(p, 0);
    }
    let (call, _) = rig.decide();
    rig.decided(
        call,
        answered(&[(1, PeerPartStrat::Unknown), (3, cursor_strat(1))]),
    );
    assert_eq!(rig.phase(1), Some(Phase::Unanswered));
    assert_eq!(rig.phase(2), Some(Phase::Unanswered));
    assert_eq!(rig.phase(3), Some(Phase::Cursor));
}

#[test]
fn law_4_a_failed_call_returns_all_its_deciding_parts_to_unanswered() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, _) = rig.decide();
    rig.remove(2);
    assert!(rig.decided(call, DecideOutcome::Failed).is_empty());
    assert_eq!(rig.phase(1), Some(Phase::Unanswered));
    assert_eq!(rig.phase(2), None);
    assert!(rig.flush().is_empty(), "backed off");
    rig.wait(2);
    assert_eq!(rig.decide().1, [1]);
}

#[test]
fn law_4_too_many_parts_lowers_the_cap_and_requeues_at_once_without_backoff() {
    let mut rig = Rig::with(Config {
        max_parts: 3,
        ..config()
    });
    for p in 0..3 {
        rig.add(p, 0);
    }
    let (call, named) = rig.decide();
    assert_eq!(named, [0, 1, 2]);
    assert!(
        rig.decided(call, DecideOutcome::TooManyParts { cap: 1 })
            .is_empty()
    );
    assert_eq!(rig.lc.max_parts(), 1);
    assert!((0..3).all(|p| rig.phase(p) == Some(Phase::Deciding)));
    assert_eq!(
        rig.decide().1,
        [0],
        "no wait, oldest first, within the new cap"
    );
}

#[test]
fn law_4_the_lowered_cap_holds_for_the_session_and_cannot_be_raised() {
    let mut rig = Rig::with(Config {
        max_parts: 3,
        ..config()
    });
    for p in 0..3 {
        rig.add(p, 0);
    }
    let (call, _) = rig.decide();
    rig.decided(call, DecideOutcome::TooManyParts { cap: 2 });
    let (call, named) = rig.decide();
    assert_eq!(named, [0, 1]);
    rig.decided(call, DecideOutcome::TooManyParts { cap: 9 });
    assert_eq!(rig.lc.max_parts(), 2, "a larger cap does not raise it");
}

#[test]
fn law_4_a_refusal_that_cannot_lower_the_cap_backs_off_instead_of_spinning() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let (call, _) = rig.decide();
    rig.decided(call, DecideOutcome::TooManyParts { cap: 5 });
    assert_eq!(rig.phase(1), Some(Phase::Unanswered));
    assert!(rig.flush().is_empty());
}

#[test]
fn law_4_a_too_many_parts_refusal_leaves_the_lanes_backoff_alone() {
    let mut rig = Rig::new();
    rig.add(0, 0);
    rig.add(1, 0);
    let (call, _) = rig.decide();
    rig.decided(call, DecideOutcome::TooManyParts { cap: 1 });
    // two empty answers climb the ladder to 2s then 4s; the refusal before them added no rung
    for expected in [[0], [1]] {
        let (call, named) = rig.decide();
        assert_eq!(named, expected);
        rig.decided(call, answered(&[]));
    }
    rig.flush();
    let due = rig.lc.next_due().expect("both parts back off");
    assert_eq!(
        (due - rig.now).as_secs(),
        4,
        "the refusal did not climb the ladder"
    );
}

// Law 5

#[test]
fn law_5_backoff_starts_at_the_floor_doubles_and_is_capped() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let mut waits = vec![];
    for _ in 0..5 {
        let (call, _) = rig.decide();
        rig.decided(call, DecideOutcome::Failed);
        assert!(rig.flush().is_empty());
        let due = rig.lc.next_due().expect("armed");
        waits.push((due - rig.now).as_secs());
        rig.now = due;
    }
    assert_eq!(waits, [2, 4, 8, 8, 8]);
}

#[test]
fn law_5_an_answer_that_decides_a_part_resets_the_backoff() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    for _ in 0..3 {
        let (call, _) = rig.decide();
        rig.decided(call, DecideOutcome::Failed);
        rig.flush();
        rig.now = rig.lc.next_due().unwrap();
    }
    rig.add(2, 0);
    let (call, named) = rig.decide();
    assert_eq!(named, [1, 2]);
    rig.decided(call, answered(&[(2, cursor_strat(1))]));
    assert_eq!(rig.phase(1), Some(Phase::Unanswered));
    assert_eq!(rig.decide().1, [1], "the reset opened the gate at once");
}

#[test]
fn law_5_an_answer_that_decides_nothing_does_not_reset_the_backoff() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    let (call, _) = rig.decide();
    rig.decided(call, DecideOutcome::Failed);
    rig.flush();
    rig.now = rig.lc.next_due().unwrap();
    let (call, _) = rig.decide();
    rig.decided(call, answered(&[(1, PeerPartStrat::Unknown)]));
    rig.flush();
    let due = rig.lc.next_due().expect("armed");
    assert_eq!((due - rig.now).as_secs(), 4, "the ladder went on: 2 then 4");
}

#[test]
fn law_5_one_call_carries_every_due_unanswered_part_beside_new_ones() {
    let mut rig = Rig::with(Config {
        max_parts: 4,
        ..config()
    });
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, _) = rig.decide();
    rig.decided(call, answered(&[]));
    rig.add(3, 0);
    let (call, named) = rig.decide();
    assert_eq!(named, [3], "a new part is not held by the lane's backoff");
    rig.decided(call, DecideOutcome::Failed);
    rig.flush();
    rig.now = rig.lc.next_due().unwrap();
    assert_eq!(rig.decide().1, [1, 2, 3], "every due part rides one call");
}

#[test]
fn law_5_a_refused_part_delays_a_decidable_neighbour_by_one_call_at_most() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    rig.add(2, 0);
    let (call, _) = rig.decide();
    rig.decided(call, answered(&[(2, cursor_strat(1))]));
    assert_eq!(rig.phase(2), Some(Phase::Cursor));
    assert_eq!(rig.phase(1), Some(Phase::Unanswered));
}

// Law 6

#[test]
fn law_6_a_cursor_decision_starts_from_the_cursor_we_advertised() {
    let mut rig = Rig::new();
    rig.add(1, 4);
    let mut out = rig.flush();
    let LifecycleCommand::Decide { call, parts } = out.remove(0) else {
        panic!()
    };
    assert_eq!(parts[&part(1)], 4);
    // the peer's `latest` does not move where the replay starts
    let out = rig.decided(call, answered(&[(1, cursor_strat(99))]));
    assert_eq!(
        out,
        [LifecycleCommand::StartCursor {
            part: part(1),
            from: 4
        }]
    );
}

#[test]
fn law_6_a_walk_decision_starts_the_walk_and_holds_the_summary_latest() {
    let mut rig = Rig::new();
    rig.add(1, 2);
    let (call, _) = rig.decide();
    let out = rig.decided(call, answered(&[(1, walk_strat(7))]));
    assert_eq!(
        out,
        [LifecycleCommand::StartWalk {
            part: part(1),
            strat: walk_strat(7),
            since: 2
        }]
    );
    assert_eq!(rig.lc.cursor(&part(1)), Some(7));
    assert_eq!(rig.phase(1), Some(Phase::Walking));
}

#[test]
fn law_6_a_walk_never_holds_a_cursor_below_the_one_we_already_persisted() {
    let mut rig = Rig::new();
    rig.add(1, 6);
    let (call, _) = rig.decide();
    rig.decided(call, answered(&[(1, walk_strat(6))]));
    assert_eq!(rig.lc.cursor(&part(1)), Some(6));
    assert_eq!(persists(&rig.upgrade(1)), [(1, 6)]);
}

// Law 10

/// A part holding `rev` of generation 0, decided from a summary `strat`: the commands.
fn decide_held(rev: Rev, strat: PeerPartStrat) -> (Rig, Vec<LifecycleCommand>) {
    let mut rig = Rig::new();
    rig.add(1, rev);
    let (call, _) = rig.decide();
    let out = rig.decided(call, answered(&[(1, strat)]));
    (rig, out)
}

#[test]
fn law_10_a_matching_generation_resumes_from_the_cursor() {
    // latest above, and equal to, the held cursor
    for latest in [9, 5] {
        let (rig, out) = decide_held(5, cursor_in(0, latest));
        assert_eq!(
            out,
            [LifecycleCommand::StartCursor {
                part: part(1),
                from: 5
            }]
        );
        assert_eq!(rig.lc.cursor(&part(1)), Some(5));
    }
    let (rig, out) = decide_held(5, walk_in(0, 9));
    assert_eq!(
        out,
        [LifecycleCommand::StartWalk {
            part: part(1),
            strat: walk_in(0, 9),
            since: 5
        }]
    );
    assert_eq!(rig.lc.cursor(&part(1)), Some(9));
}

#[test]
fn law_10_another_generation_restarts_from_zero() {
    let (rig, out) = decide_held(5, cursor_in(1, 9));
    assert_eq!(
        out,
        [
            LifecycleCommand::Restart {
                part: part(1),
                held: PeerCursor {
                    generation: generation(0),
                    rev: 5
                },
                peer_generation: generation(1),
                peer_latest: 9
            },
            LifecycleCommand::PersistCursor {
                part: part(1),
                cursor: PeerCursor {
                    generation: generation(1),
                    rev: 0
                }
            },
            LifecycleCommand::StartCursor {
                part: part(1),
                from: 0
            }
        ]
    );
    assert_eq!(rig.lc.cursor(&part(1)), Some(0));
    // a walk lists everything, and hands over at the summary's latest
    let (rig, out) = decide_held(5, walk_in(1, 9));
    assert_eq!(
        out[2],
        LifecycleCommand::StartWalk {
            part: part(1),
            strat: walk_in(1, 9),
            since: 0
        }
    );
    assert!(matches!(out[0], LifecycleCommand::Restart { .. }));
    assert_eq!(rig.lc.cursor(&part(1)), Some(9));
}

#[test]
fn law_10_a_latest_below_the_cursor_restarts_from_zero() {
    let (rig, out) = decide_held(5, cursor_in(0, 4));
    assert_eq!(
        out,
        [
            LifecycleCommand::Restart {
                part: part(1),
                held: PeerCursor {
                    generation: generation(0),
                    rev: 5
                },
                peer_generation: generation(0),
                peer_latest: 4
            },
            LifecycleCommand::PersistCursor {
                part: part(1),
                cursor: PeerCursor {
                    generation: generation(0),
                    rev: 0
                }
            },
            LifecycleCommand::StartCursor {
                part: part(1),
                from: 0
            }
        ]
    );
    assert_eq!(rig.lc.cursor(&part(1)), Some(0));
    let (_, out) = decide_held(5, walk_in(0, 4));
    assert!(matches!(
        out[..],
        [
            LifecycleCommand::Restart { .. },
            LifecycleCommand::PersistCursor { .. },
            LifecycleCommand::StartWalk { since: 0, .. }
        ]
    ));
}

/// The restart writes `(peer generation, 0)` at once, also for a part that walks: a stale cursor
/// of an unminted restore must not outlive a crash.
#[test]
fn law_10_a_restart_persists_zero_under_the_peers_generation_at_once() {
    for (strat, peer_generation, phase) in [
        (cursor_in(0, 4), 0, Phase::Cursor),
        (walk_in(0, 4), 0, Phase::Walking),
        (cursor_in(1, 9), 1, Phase::Cursor),
        (walk_in(1, 9), 1, Phase::Walking),
    ] {
        let (rig, out) = decide_held(5, strat);
        assert!(
            matches!(out[0], LifecycleCommand::Restart { .. }),
            "{out:?}"
        );
        assert_eq!(
            out[1],
            LifecycleCommand::PersistCursor {
                part: part(1),
                cursor: PeerCursor {
                    generation: generation(peer_generation),
                    rev: 0
                }
            },
            "the write comes before the start"
        );
        assert_eq!(persists(&out), [(1, 0)]);
        assert_eq!(rig.phase(1), Some(phase));
    }
    // no restart, no write
    let (_, out) = decide_held(5, cursor_in(0, 9));
    assert!(persists(&out).is_empty());
}

#[test]
fn law_10_the_persisted_cursor_after_a_restart_carries_the_new_generation() {
    let new = |rev| PeerCursor {
        generation: generation(1),
        rev,
    };
    let (mut rig, _) = decide_held(5, cursor_in(1, 9));
    let out = rig.applied(1, 3);
    assert_eq!(
        out,
        [LifecycleCommand::PersistCursor {
            part: part(1),
            cursor: new(3)
        }],
        "a cursor below the old one persists: it is a position in another log"
    );
    // through a walk, at the upgrade
    let (mut rig, _) = decide_held(5, walk_in(1, 9));
    assert!(rig.applied(1, 11).is_empty());
    assert_eq!(
        rig.upgrade(1),
        [LifecycleCommand::PersistCursor {
            part: part(1),
            cursor: new(11)
        }]
    );
    // a part that resumed keeps persisting its generation
    let (mut rig, _) = decide_held(5, cursor_in(0, 9));
    assert_eq!(
        rig.applied(1, 6),
        [LifecycleCommand::PersistCursor {
            part: part(1),
            cursor: PeerCursor {
                generation: generation(0),
                rev: 6
            }
        }]
    );
}

#[test]
fn law_10_a_part_never_seen_adopts_the_peers_generation_without_a_restart() {
    for strat in [cursor_in(7, 3), walk_in(7, 3)] {
        let (mut rig, out) = decide_held(0, strat);
        assert!(
            !out.iter()
                .any(|cmd| matches!(cmd, LifecycleCommand::Restart { .. })),
            "{out:?}"
        );
        let out = if matches!(strat, PeerPartStrat::Cursor { .. }) {
            rig.applied(1, 5)
        } else {
            rig.upgrade(1)
        };
        assert!(matches!(
            out[..],
            [LifecycleCommand::PersistCursor { cursor, .. }] if cursor.generation == generation(7)
        ));
    }
}

// Law 7

#[test]
fn law_7_applied_while_walking_raises_the_held_cursor_and_persists_nothing() {
    let mut rig = Rig::new();
    rig.walking_part(1, 0, 3);
    assert!(rig.applied(1, 5).is_empty());
    assert_eq!(rig.lc.cursor(&part(1)), Some(5));
    assert!(rig.applied(1, 9).is_empty());
    assert_eq!(rig.lc.cursor(&part(1)), Some(9));
}

#[test]
fn law_7_applied_below_the_held_cursor_does_not_lower_it() {
    let mut rig = Rig::new();
    rig.walking_part(1, 0, 6);
    rig.applied(1, 2);
    assert_eq!(rig.lc.cursor(&part(1)), Some(6));
}

#[test]
fn law_7_applied_for_a_part_that_has_not_started_or_is_gone_changes_nothing() {
    let mut rig = Rig::new();
    assert!(rig.applied(1, 5).is_empty());
    rig.add(1, 2);
    assert!(rig.applied(1, 5).is_empty());
    assert_eq!(rig.lc.cursor(&part(1)), Some(2));
    let (call, _) = rig.decide();
    rig.decided(call, answered(&[]));
    assert!(rig.applied(1, 5).is_empty());
    assert_eq!(rig.lc.cursor(&part(1)), Some(2));
}

// Law 8

#[test]
fn law_8_upgrade_persists_the_held_cursor_and_moves_the_part_to_cursor() {
    let mut rig = Rig::new();
    rig.walking_part(1, 0, 3);
    rig.applied(1, 8);
    assert_eq!(persists(&rig.upgrade(1)), [(1, 8)]);
    assert_eq!(rig.phase(1), Some(Phase::Cursor));
}

#[test]
fn law_8_upgrade_persists_the_held_cursor_not_the_summary_latest() {
    let mut rig = Rig::new();
    rig.walking_part(1, 0, 3);
    rig.applied(1, 5);
    let cmds = rig.upgrade(1);
    assert_eq!(persists(&cmds), [(1, 5)]);
}

#[test]
#[should_panic(expected = "walking part")]
fn law_8_upgrade_happens_once() {
    let mut rig = Rig::new();
    rig.walking_part(1, 0, 3);
    rig.upgrade(1);
    rig.upgrade(1);
}

#[test]
#[should_panic(expected = "walking part")]
fn law_8_upgrade_of_a_deciding_part_is_a_parent_bug() {
    let mut rig = Rig::new();
    rig.add(1, 0);
    rig.upgrade(1);
}

#[test]
#[should_panic(expected = "unwanted part")]
fn law_8_upgrade_of_an_unwanted_part_is_a_parent_bug() {
    let mut rig = Rig::new();
    rig.upgrade(1);
}

#[test]
fn law_8_in_cursor_applied_persists_each_raise() {
    let mut rig = Rig::new();
    rig.cursor_part(1, 4);
    assert_eq!(persists(&rig.applied(1, 6)), [(1, 6)]);
    assert_eq!(persists(&rig.applied(1, 9)), [(1, 9)]);
}

#[test]
fn law_8_in_cursor_the_cursor_never_goes_back() {
    let mut rig = Rig::new();
    rig.cursor_part(1, 4);
    rig.applied(1, 6);
    assert!(rig.applied(1, 6).is_empty(), "equal raises nothing");
    assert!(rig.applied(1, 2).is_empty(), "lower is ignored");
    assert_eq!(rig.lc.cursor(&part(1)), Some(6));
}

#[test]
fn law_8_after_the_upgrade_applied_continues_from_the_handed_over_cursor() {
    let mut rig = Rig::new();
    rig.walking_part(1, 0, 3);
    rig.applied(1, 5);
    rig.upgrade(1);
    assert!(rig.applied(1, 5).is_empty());
    assert_eq!(persists(&rig.applied(1, 6)), [(1, 6)]);
}

// Law 9

#[test]
fn law_9_a_new_connection_starts_empty_and_resumes_from_the_persisted_cursor() {
    let mut first = Rig::new();
    first.cursor_part(1, 0);
    first.applied(1, 5);
    drop(first);
    let mut second = Rig::new();
    assert_eq!(second.phase(1), None);
    assert!(second.flush().is_empty());
    second.add(1, 5);
    let (call, _) = second.decide();
    let out = second.decided(call, answered(&[(1, cursor_strat(8))]));
    assert_eq!(
        out,
        [LifecycleCommand::StartCursor {
            part: part(1),
            from: 5
        }]
    );
}

mod prop {
    use super::*;
    use proptest::prelude::*;
    use sim::Op;
    use sim::Sim;

    proptest! {
        #[test]
        fn differential_against_the_oracle(
            max_parts in 1usize..=3,
            ops in proptest::collection::vec(sim::op(), 1..120),
        ) {
            let mut sim = Sim::new(max_parts);
            for op in ops {
                sim.step(op);
            }
        }
    }

    /// Smoke check that the generator reaches the interesting states at all.
    #[test]
    fn a_scripted_walk_through_every_phase_agrees_with_the_oracle() {
        let mut sim = Sim::new(2);
        for op in [
            Op::Add { part: 0 },
            Op::Add { part: 1 },
            Op::Add { part: 2 },
            Op::Flush { secs: 0 },
            Op::Answer {
                call: sim::CallPick::Current,
                reply: sim::Reply::Parts(vec![
                    Some(sim::Spec::Walk(0, 5)),
                    Some(sim::Spec::Cursor(1, 2)),
                    None,
                    None,
                ]),
            },
            Op::Applied { part: 0, cursor: 7 },
            Op::Upgrade { pick: 0 },
            Op::Applied { part: 1, cursor: 3 },
            Op::Flush { secs: 3 },
            Op::Remove { part: 0 },
            Op::Add { part: 0 },
        ] {
            sim.step(op);
        }
    }
}
