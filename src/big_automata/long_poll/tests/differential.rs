//! The machine against its naive oracle: the same inputs, every output compared after every
//! step. Time is drawn at random, so it goes back as often as forward, and revs are drawn so
//! commits arrive late, repeated and out of order; interests and ids are few so waiters
//! collide on them constantly.

use proptest::prelude::*;

use super::*;
use crate::long_poll::oracle::Oracle;

#[derive(Debug, Clone)]
enum Op {
    Park {
        interests: Vec<u8>,
        after_rev: Rev,
        /// Past the op's `now`; 0 is a deadline already due.
        hold_ms: u64,
        window_ms: u64,
        min_amount: u64,
    },
    Commit {
        rev: Rev,
        changes: Vec<(u8, u64)>,
    },
    Tick,
    /// Cancel the `n`th waiter ever parked, live or not.
    Cancel(usize),
}

fn any_op() -> impl Strategy<Value = (u64, Op)> {
    let op = prop_oneof![
        4 => (
            prop::collection::vec(0u8..4, 0..4),
            0u64..8,
            0u64..200,
            prop_oneof![Just(0u64), 1u64..120],
            0u64..6,
        )
            .prop_map(|(interests, after_rev, hold_ms, window_ms, min_amount)| Op::Park {
                interests,
                after_rev,
                hold_ms,
                window_ms,
                min_amount,
            }),
        6 => (1u64..12, prop::collection::btree_map(0u8..4, 0u64..4, 0..4))
            .prop_map(|(rev, changes)| Op::Commit { rev, changes: changes.into_iter().collect() }),
        3 => Just(Op::Tick),
        2 => any::<usize>().prop_map(Op::Cancel),
    ];
    (0u64..600, op)
}

fn run(ops: Vec<(u64, Op)>) {
    let mut real = LongPoll::<u8>::new();
    let mut naive = Oracle::<u8>::new();
    let mut issued = 0u64;
    for (now_ms, op) in ops {
        let (mut got, mut want) = (vec![], vec![]);
        match op {
            Op::Park {
                interests,
                after_rev,
                hold_ms,
                window_ms,
                min_amount,
            } => {
                let request = Park {
                    interests: &interests,
                    after_rev,
                    deadline: at(now_ms + hold_ms),
                    linger: linger(window_ms, min_amount),
                };
                real.park(at(now_ms), w(issued), request, &mut got);
                naive.park(at(now_ms), w(issued), request, &mut want);
                issued += 1;
            }
            Op::Commit { rev, changes } => {
                real.committed(at(now_ms), rev, changes.iter().copied(), &mut got);
                naive.committed(at(now_ms), rev, &changes, &mut want);
            }
            Op::Tick => {
                real.tick(at(now_ms), &mut got);
                naive.tick(at(now_ms), &mut want);
            }
            Op::Cancel(n) => {
                if issued > 0 {
                    let id = w(n as u64 % issued);
                    real.cancelled(id);
                    naive.cancelled(id);
                }
            }
        }
        got.sort();
        want.sort();
        assert_eq!(got, want, "outputs");
        real.audit();
        assert_eq!(
            (0..issued)
                .filter(|id| real.is_parked(w(*id)))
                .map(w)
                .collect::<Vec<_>>(),
            naive.parked(),
            "parked"
        );
        assert_eq!(real.parked(), naive.parked().len());
    }
}

proptest! {
    #[test]
    fn the_machine_matches_the_naive_oracle(
        ops in prop::collection::vec(any_op(), 1..80),
    ) {
        run(ops);
    }
}

/// Commits on interests no one watches cost nothing, however many waiters are parked, and a
/// commit costs its watchers and no more.
#[test]
fn the_work_of_a_commit_is_its_watchers_not_the_parked() {
    for parked in [10u64, 1_000, 10_000] {
        let mut poll = LongPoll::<u64>::new();
        let mut out = vec![];
        // One waiter per interest, and `watchers` more on interest 0.
        for id in 0..parked {
            poll.park(
                at(0),
                w(id),
                Park {
                    interests: &[id + 1],
                    after_rev: 0,
                    deadline: at(1_000_000),
                    linger: linger(0, 0),
                },
                &mut out,
            );
        }
        let before = poll.touched();
        poll.committed(at(1), 1, [(0, 1), (parked + 5, 1)], &mut out);
        assert_eq!(poll.touched() - before, 0, "{parked} parked, none watching");
        // Three interests woken: three waiters touched, whatever else is parked.
        poll.committed(at(2), 2, [(1, 1), (2, 1), (3, 1)], &mut out);
        assert_eq!(poll.touched() - before, 3, "{parked} parked");
        assert_eq!(out.len(), 3);
        // An idle tick touches nobody.
        poll.tick(at(3), &mut out);
        assert_eq!(poll.touched() - before, 3);
    }
}
