//! Named scenarios on the simulation's harness: the crash that must not lose a walk's
//! removal, a fetch a contradiction must cancel, and the lanes' latency and throughput.
//! Regressions the proptest shrinks go here too, named for what they pin.

use super::*;

use crate::sync::object_jobs::Settle;

impl Sim {
    /// The embedder wants these parts.
    fn want(&mut self, parts: &[u8]) {
        let add_parts = parts.iter().fold(0, |mask, part| mask | 1 << part);
        self.step(Op::Routes {
            add_parts,
            rem_parts: 0,
            add_objs: 0,
            rem_objs: 0,
        });
    }

    /// The peer makes `obj` a member of `part` with a payload.
    fn member(&mut self, obj: u8, part: u8) {
        self.remote.commit(laws::add(&[(obj, part)]));
        self.remote.commit(laws::set(&[(obj, 1)]));
    }

    fn local_members(&self, part: u8) -> BTreeSet<ObjKey> {
        self.local.members(&laws::part(part))
    }

    fn live_rounds(&self) -> usize {
        self.rounds
            .iter()
            .filter(|(lane, _)| *lane == Lane::Live)
            .count()
    }
}

/// A walk finds that the peer removed an object while we were away; its record is issued, and
/// the process dies before it settles. The part's cursor must not have been persisted past the
/// removal, so the restart replays it.
#[test]
fn a_removal_found_by_a_walk_survives_a_crash_before_it_settles() {
    let mut sim = Sim::new(Shape::small(), 7);
    sim.remote.commit(laws::grant(&[(0, 0)]));
    sim.member(0, 0);
    sim.member(1, 0);
    sim.want(&[0]);
    sim.quiesce();
    let held = sim.local.persisted[&part(0)];
    assert_eq!(sim.local_members(0), [obj(0), obj(1)].into());

    // the peer removes object 0, and we restart into a summary that says "far behind"
    sim.remote.commit(laws::remove(&[(0, 0)]));
    sim.remote.walk_bias = true;
    sim.step(Op::Crash {
        committed: 0,
        close: true,
    });
    let removal_in_flight = |sim: &Sim| {
        sim.local.calls.values().any(|flight| {
            matches!(flight, Flight::Record { job, .. } if job.obj == obj(0) && job.remove == [part(0)])
        })
    };
    for _ in 0..40 {
        if removal_in_flight(&sim) {
            break;
        }
        sim.deliver_all();
        sim.pass_time();
    }
    assert!(removal_in_flight(&sim), "the walk found the removal");
    assert!(sim.sync.walking(&part(0)));
    assert_eq!(
        sim.local.persisted[&part(0)],
        held,
        "nothing persists mid-walk"
    );

    // the process dies with the removal's record not applied
    sim.remote.walk_bias = false;
    sim.step(Op::Crash {
        committed: 0,
        close: true,
    });
    sim.quiesce();
    assert_eq!(
        sim.local_members(0),
        [obj(1)].into(),
        "the removal was not lost"
    );
}

/// A fetch of an object into a part is cancelled once the machine learns the peer removed it
/// from that part, within a bounded number of exchanges, however long the fetch would run.
#[test]
fn a_fetch_is_cancelled_when_the_peer_removes_the_object() {
    let mut sim = Sim::new(Shape::small(), 3);
    sim.remote.commit(laws::grant(&[(0, 0)]));
    sim.want(&[0]);
    sim.quiesce();
    sim.local.force_settle = Some(Settle::Fetch);
    sim.local.force_steps = Some(1_000);
    sim.member(0, 0);
    let fetch = |sim: &Sim| {
        sim.local
            .calls
            .iter()
            .find(|(_, flight)| matches!(flight, Flight::Fetch { .. }))
            .map(|(call, _)| *call)
    };
    for _ in 0..40 {
        if fetch(&sim).is_some() {
            break;
        }
        sim.deliver_all();
        for call in sim.local.calls.keys().copied().collect::<Vec<_>>() {
            sim.progress_call(call, false);
        }
        sim.pass_time();
    }
    let call = fetch(&sim).expect("the record answered Fetch and the fetch started");

    sim.remote.commit(laws::remove(&[(0, 0)]));
    for _ in 0..6 {
        if sim.local.calls[&call].cancelled() {
            break;
        }
        sim.deliver_all();
        sim.pass_time();
    }
    assert!(
        sim.local.calls[&call].cancelled(),
        "law 4: the contradicted fetch is cancelled"
    );
    sim.quiesce();
    assert!(sim.local_members(0).is_empty());
}

/// Lanes, latency: while one part drains a backlog many windows long, a write to a part that
/// is caught up is applied within a few `Live` rounds. One lane for everything makes it wait
/// for the whole backlog.
#[test]
fn lanes_a_live_write_is_applied_within_a_few_live_rounds_of_a_backlog_drain() {
    const BACKLOG: u8 = 40;
    // the write is read by the second `Live` round after it is committed: the round in flight
    // may have been served before it, the next one carries it
    const BOUND: usize = 3;
    let mut shape = Shape::small();
    shape.objs = 60;
    shape.obj_routes = 0;
    shape.config.jobs.window = 4;
    shape.config.jobs.fetch_cap = 2;
    shape.config.lanes.live_limit = 2;
    let mut sim = Sim::new(shape, 1);
    sim.local.settle_always = Some(Settle::Done);
    sim.remote.commit(laws::grant(&[(0, 0), (1, 0)]));
    sim.want(&[1]);
    sim.quiesce();
    for obj in 0..BACKLOG {
        sim.member(obj, 0);
    }
    sim.want(&[0]);
    // the backlog is draining: some of it is in, most is not
    while sim.local_members(0).len() < 8 {
        assert!(sim.rounds.len() < 100, "the backlog does not start");
        sim.cycle();
    }
    assert!(sim.local_members(0).len() < usize::from(BACKLOG) / 2);
    assert_eq!(
        sim.sync.synced(&part(0)),
        Some(false),
        "the backlog is still draining"
    );

    sim.member(50, 1);
    let before = sim.live_rounds();
    while !sim.local_members(1).contains(&obj(50)) {
        assert_eq!(
            sim.sync.synced(&part(0)),
            Some(false),
            "the backlog drained first"
        );
        sim.cycle();
        let waited = sim.live_rounds() - before;
        assert!(
            waited <= BOUND,
            "the live write waited {waited} live rounds behind the backlog"
        );
    }
    sim.quiesce();
}

/// Lanes, throughput: a backlog of `n` rows drains in about `n / limit` `Bulk` rounds, when the
/// backend's completions are handled in a batch and flushed once. (Flushed one by one, a
/// round goes out as soon as one slot frees and asks for one row: the rounds then alternate
/// between 1 row and the window less one.)
#[test]
fn lanes_a_backlog_drains_in_about_n_over_limit_bulk_rounds() {
    const BACKLOG: u8 = 48;
    let mut shape = Shape::small();
    shape.objs = 60;
    shape.obj_routes = 0;
    shape.config.jobs.window = 8;
    shape.config.jobs.fetch_cap = 2;
    shape.config.lanes.live_limit = 2;
    let mut sim = Sim::new(shape, 1);
    sim.local.settle_always = Some(Settle::Done);
    sim.remote.commit(laws::grant(&[(0, 0)]));
    for obj in 0..BACKLOG {
        sim.member(obj, 0);
    }
    sim.want(&[0]);
    while sim.sync.synced(&part(0)) != Some(true) {
        assert!(sim.rounds.len() < 100, "the backlog does not drain");
        sim.deliver_all();
        sim.complete_batch();
        sim.pass_time();
    }
    let bulk: Vec<u32> = sim
        .rounds
        .iter()
        .filter(|(lane, _)| *lane == Lane::Bulk)
        .map(|(_, limit)| *limit)
        .collect();
    let limit = *bulk
        .iter()
        .max()
        .expect("a backlog is paged in Bulk rounds");
    assert!(
        limit >= 6,
        "a window of 8 with 2 rows kept for Live leaves Bulk 6 rows a round, not {limit}"
    );
    assert!(
        bulk.len() <= usize::from(BACKLOG).div_ceil(limit as usize) + 1,
        "{} bulk rounds for {BACKLOG} rows at {limit} a round: {bulk:?}",
        bulk.len()
    );
}

/// Harness regression, shrunk from the first simulation run: a payload change re-stamps the
/// object's rows, and rows collapse to their last stamp. The part's round resumed at 3 and
/// never saw the row of object 0 (re-stamped at 5), so the cursor persisted at 3 is safe: the
/// replay from it delivers that row. The crash-safety check must judge rows by their last
/// stamp, not by when membership last changed.
#[test]
fn a_restamped_row_above_a_persisted_cursor_is_the_replays_to_deliver() {
    use Op::*;
    let w = |kind, obj, part, val| Write {
        kind,
        obj,
        part,
        val,
    };
    run(
        vec![
            w(235, 12, 151, 0),
            w(115, 115, 97, 0),
            w(96, 52, 0, 0),
            Access(145),
            w(0, 0, 0, 0),
            Routes {
                add_parts: 2,
                rem_parts: 0,
                add_objs: 1,
                rem_objs: 0,
            },
            Deliver(0),
            w(0, 0, 0, 0),
            w(0, 0, 0, 0),
            Routes {
                add_parts: 0,
                rem_parts: 0,
                add_objs: 0,
                rem_objs: 0,
            },
            w(0, 0, 0, 0),
            w(3, 3, 0, 0),
            w(222, 180, 0, 7),
            Deliver(0),
            w(0, 0, 0, 0),
            w(0, 0, 0, 0),
            Deliver(0),
            w(0, 0, 0, 0),
        ],
        0,
    );
}

/// Shrunk by the simulation (the walk's bucket law 4, fixed in `sync/bucket.rs`): the peer
/// added members to a part after the walk listed its bucket, so the leaf pages held more than
/// the listing counted; the walk stopped at the listed count, missed the older member the new
/// ones displaced to the second page, and upgraded without it.
#[test]
fn a_bucket_that_grows_during_a_walk_is_read_to_its_end() {
    run(
        vec![
            Op::Write {
                kind: 15,
                obj: 3,
                part: 0,
                val: 0,
            },
            Op::Routes {
                add_parts: 5,
                rem_parts: 0,
                add_objs: 1,
                rem_objs: 0,
            },
            Op::Write {
                kind: 31,
                obj: 14,
                part: 141,
                val: 0,
            },
            Op::Write {
                kind: 108,
                obj: 73,
                part: 0,
                val: 0,
            },
            Op::Bias(true),
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 43,
                obj: 3,
                part: 66,
                val: 0,
            },
            Op::Write {
                kind: 87,
                obj: 34,
                part: 0,
                val: 0,
            },
            Op::Access(81),
            Op::Routes {
                add_parts: 0,
                rem_parts: 0,
                add_objs: 0,
                rem_objs: 0,
            },
            Op::Routes {
                add_parts: 0,
                rem_parts: 0,
                add_objs: 2,
                rem_objs: 0,
            },
            Op::Deliver(30),
            Op::Duplicate(0),
            Op::Write {
                kind: 0,
                obj: 12,
                part: 0,
                val: 0,
            },
            Op::Deliver(220),
            Op::Deliver(12),
            Op::Deliver(8),
            Op::Hostile { kind: 0, pick: 0 },
            Op::Deliver(108),
            Op::Write {
                kind: 43,
                obj: 32,
                part: 54,
                val: 0,
            },
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Deliver(0),
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
        ],
        0,
    );
}

/// Pins a gap between crash recovery and the store's route rule (found by the simulation, not
/// fixed here). The cursor lags the backend: a fetch holds the watermark while later jobs
/// settle, so the local store holds members above the persisted cursor. After a crash the replay
/// starts from that cursor, and the store hands out a removal only to a reader whose cursor is at
/// or past the member's `added_at` (`removals_skip_routes_that_never_saw_the_member`). The
/// restarted machine is below it, so the removal never comes and the member stays for good.
/// Unwanting the part and wanting it again restarts its replay from the persisted cursor the same
/// way.
/// Fixing it means changing the rule, the persisted cursor or the recovery; when one does, this
/// test should assert the removal arrives, and the simulation's `ghosts` allowance goes.
#[test]
fn a_removal_of_a_member_applied_above_the_persisted_cursor_is_skipped_after_a_crash() {
    let mut sim = Sim::new(Shape::small(), 5);
    sim.remote.commit(laws::grant(&[(0, 0)]));
    sim.want(&[0]);
    sim.quiesce();
    sim.local.force_settle = Some(Settle::Fetch);
    sim.local.force_steps = Some(1_000);
    sim.member(3, 0);
    let fetching = |sim: &Sim| {
        sim.local
            .calls
            .values()
            .any(|flight| matches!(flight, Flight::Fetch { .. }))
    };
    for _ in 0..40 {
        if fetching(&sim) {
            break;
        }
        sim.deliver_all();
        for call in sim.local.calls.keys().copied().collect::<Vec<_>>() {
            sim.progress_call(call, false);
        }
        sim.pass_time();
    }
    assert!(fetching(&sim));
    assert!(
        sim.local.persisted.get(&part(0)).copied().unwrap_or(0) < sim.remote.head(),
        "the long fetch holds the cursor below the member"
    );

    // the process dies as the fetch lands, and the peer removes the member while it is down
    sim.step(Op::Crash {
        committed: 1,
        close: true,
    });
    sim.remote.commit(laws::remove(&[(3, 0)]));
    sim.quiesce();
    assert!(sim.remote.view_at(sim.remote.head()).members[&part(0)].is_empty());
    assert_eq!(
        sim.local_members(0),
        [obj(3)].into(),
        "the removal was skipped: the member is a ghost"
    );
}

/// Shrunk by the simulation at 4000 cases: the same gap as above without a crash. The part is
/// unwanted and wanted again (`rem_parts` and `add_parts` of one `Routes`), the machine starts
/// the part over from the persisted cursor, which is below the member the local store already
/// holds, and the peer's removal of that member never comes. Passes through the simulation's
/// `ghosts` allowance, like the crash case.
#[test]
fn a_part_wanted_again_replays_from_its_persisted_cursor() {
    run(
        vec![
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Routes {
                add_parts: 1,
                rem_parts: 0,
                add_objs: 0,
                rem_objs: 0,
            },
            Op::Write {
                kind: 24,
                obj: 19,
                part: 0,
                val: 0,
            },
            Op::Access(39),
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 52,
                obj: 3,
                part: 198,
                val: 0,
            },
            Op::Write {
                kind: 9,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Duplicate(0),
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Write {
                kind: 113,
                obj: 107,
                part: 21,
                val: 0,
            },
            Op::Write {
                kind: 0,
                obj: 0,
                part: 0,
                val: 0,
            },
            Op::Advance(0),
            Op::Routes {
                add_parts: 5,
                rem_parts: 5,
                add_objs: 0,
                rem_objs: 0,
            },
            Op::Bias(false),
        ],
        1419127500411581189,
    );
}
