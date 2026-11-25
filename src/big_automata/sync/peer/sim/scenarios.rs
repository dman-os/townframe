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
        self.remote.commit(vec![laws::add(&[(obj, part)])]);
        self.remote.commit(vec![laws::set(&[(obj, 1)])]);
    }

    /// The peer makes every object of `objs` a member of `part` with a payload, in one
    /// transaction: all of them change at one rev.
    fn members(&mut self, objs: &[u8], part: u8) {
        let pairs: Vec<(u8, u8)> = objs.iter().map(|obj| (*obj, part)).collect();
        let payloads: Vec<(u8, u8)> = objs.iter().map(|obj| (*obj, 1)).collect();
        self.remote
            .commit(vec![laws::add(&pairs), laws::set(&payloads)]);
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
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.member(0, 0);
    sim.member(1, 0);
    sim.want(&[0]);
    sim.quiesce();
    let held = sim.local.persisted[&part(0)].rev;
    assert_eq!(sim.local_members(0), [obj(0), obj(1)].into());

    // the peer removes object 0, and we restart into a summary that says "far behind"
    sim.remote.commit(vec![laws::remove(&[(0, 0)])]);
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
        sim.local.persisted[&part(0)].rev,
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
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
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

    sim.remote.commit(vec![laws::remove(&[(0, 0)])]);
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
    shape.config.jobs.window = 5;
    shape.config.jobs.max_tracked = 5;
    shape.config.jobs.fetch_cap = 2;
    shape.config.lanes.live_limit = 2;
    let mut sim = Sim::new(shape, 1);
    sim.local.settle_always = Some(Settle::Done);
    sim.remote.commit(vec![laws::grant(&[(0, 0), (1, 0)])]);
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
    shape.config.jobs.max_tracked = 8;
    shape.config.jobs.fetch_cap = 2;
    shape.config.lanes.live_limit = 2;
    let mut sim = Sim::new(shape, 1);
    sim.local.settle_always = Some(Settle::Done);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
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

/// Regression (found by the simulation). The cursor lags the backend: a fetch holds the
/// watermark while later jobs settle, so the local store holds members above the persisted
/// cursor. After a crash the replay starts from that cursor, and the peer must still send the
/// removal of such a member, though it went live after the cursor (replay law 11). Once the
/// store skipped it, and the member stayed for good. Unwanting the part and wanting it again
/// restarts its replay from the persisted cursor the same way.
#[test]
fn a_removal_of_a_member_applied_above_the_persisted_cursor_arrives_after_a_crash() {
    let mut sim = Sim::new(Shape::small(), 5);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
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
        sim.local
            .persisted
            .get(&part(0))
            .map_or(0, |cursor| cursor.rev)
            < sim.remote.head(),
        "the long fetch holds the cursor below the member"
    );

    // the process dies as the fetch lands, and the peer removes the member while it is down
    sim.step(Op::Crash {
        committed: 1,
        close: true,
    });
    sim.remote.commit(vec![laws::remove(&[(3, 0)])]);
    sim.quiesce();
    assert!(sim.remote.view_at(sim.remote.head()).members[&part(0)].is_empty());
    assert_eq!(
        sim.local_members(0),
        BTreeSet::new(),
        "the removal reached the restarted replay"
    );
}

/// Shrunk by the simulation at 4000 cases: the same gap as above without a crash. The part is
/// unwanted and wanted again (`rem_parts` and `add_parts` of one `Routes`), the machine starts
/// the part over from the persisted cursor, which is below the member the local store already
/// holds, and the peer's removal of that member must still come.
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

/// A revision is per transaction: several objects committed together share one rev. A part
/// following the peer by cursor must report `Synced` only once every one of them is present.
#[test]
fn a_multi_object_commit_on_a_cursor_part_syncs_only_with_every_object() {
    let mut shape = Shape::small();
    shape.objs = 4;
    shape.obj_routes = 0;
    let mut sim = Sim::new(shape, 5);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.want(&[0]);
    sim.quiesce();
    assert!(!sim.sync.walking(&part(0)), "the part follows by cursor");

    let rev = sim.remote.head();
    sim.members(&[0, 1, 2, 3], 0);
    assert_eq!(sim.remote.head(), rev + 1, "one commit, one rev");
    let all: BTreeSet<ObjKey> = (0..4).map(obj).collect();
    for _ in 0..100 {
        sim.cycle();
        if sim.sync.synced(&part(0)) == Some(true) && sim.sync.drained[&part(0)] > rev {
            assert_eq!(sim.local_members(0), all, "Synced with an object missing");
            break;
        }
    }
    sim.quiesce();
    assert_eq!(sim.local_members(0), all);
    assert_eq!(sim.sync.synced(&part(0)), Some(true));
}

/// Found at 4000 proptest cases once law 7 exempted backing-off objects from the window: the
/// quiet drain judged the jobs idle by `room()` while an object still waited out its backoff.
#[test]
fn regression_routed_content_is_converged_after_a_failure_backoff_outside_the_window() {
    let w = |kind, obj, part, val| Op::Write {
        kind,
        obj,
        part,
        val,
    };
    let routes = |add_parts, add_objs| Op::Routes {
        add_parts,
        rem_parts: 0,
        add_objs,
        rem_objs: 0,
    };
    let batch = |kind, obj, part, more| Op::Batch {
        kind,
        obj,
        part,
        val: 0,
        more,
    };
    run(
        vec![
            Op::Access(176),
            Op::Access(54),
            routes(0, 1),
            batch(42, 52, 0, 0),
            routes(1, 2),
            Op::Access(134),
            Op::Access(143),
            w(0, 34, 0, 0),
            Op::Access(44),
            Op::Access(8),
            Op::Access(17),
            w(106, 32, 63, 0),
            Op::Access(188),
            Op::Crash {
                committed: 0,
                close: false,
            },
            Op::Deliver(5),
            w(0, 0, 0, 0),
            Op::Deliver(19),
            Op::Deliver(55),
            Op::Backend(0),
            Op::Deliver(157),
            w(24, 105, 0, 0),
            Op::Backend(0),
            Op::Sweep,
            Op::Duplicate(9),
            Op::Deliver(253),
            batch(0, 191, 1, 77),
            Op::Access(163),
            Op::Deliver(84),
            Op::Deliver(8),
            Op::Drop(86),
            Op::Hostile { kind: 0, pick: 0 },
            Op::Duplicate(3),
            Op::Deliver(188),
            batch(0, 0, 112, 22),
            Op::Deliver(7),
            Op::Deliver(27),
            Op::Deliver(53),
            Op::Access(135),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Backend(0),
        ],
        1494554001155084478,
    );
}

/// Found at 4000 proptest cases with persistent failures (the shrunk case).
#[test]
fn regression_a_contradicted_fetch_of_a_persistently_failing_object() {
    let routes = |add_parts, add_objs| Op::Routes {
        add_parts,
        rem_parts: 0,
        add_objs,
        rem_objs: 0,
    };
    let w = |kind, obj, part| Op::Write {
        kind,
        obj,
        part,
        val: 0,
    };
    let batch = |obj, part, more| Op::Batch {
        kind: 0,
        obj,
        part,
        val: 0,
        more,
    };
    run(
        vec![
            Op::Access(92),
            Op::Poison(50),
            routes(4, 2),
            Op::Deliver(2),
            Op::Deliver(92),
            batch(144, 16, 162),
            Op::Deliver(29),
            Op::Access(19),
            Op::Access(79),
            Op::Advance(0),
            Op::Deliver(8),
            w(180, 89, 0),
            Op::Deliver(0),
            Op::Deliver(0),
            Op::Deliver(0),
            w(0, 0, 0),
            Op::Deliver(0),
            Op::Deliver(7),
            w(0, 0, 0),
            batch(211, 64, 251),
            Op::Deliver(0),
            Op::Backend(0),
            routes(0, 0),
            Op::Access(175),
            batch(124, 52, 122),
            Op::Deliver(0),
            routes(4, 0),
            Op::Deliver(20),
        ],
        0,
    );
}

/// Law 7 of ObjectJobs, through the whole machine: as many persistently failing objects as the
/// window holds back off outside it, so the peer's other objects keep flowing and their part
/// converges, while the part holding the failing objects keeps its cursor below them. When the
/// failure ends, everything converges.
#[test]
fn persistently_failing_objects_do_not_stop_other_parts_and_hold_only_their_own_cursor() {
    let mut shape = Shape::small();
    shape.parts = 2;
    shape.objs = 6;
    shape.obj_routes = 0;
    assert_eq!(
        shape.config.jobs.window, 3,
        "the failing objects below fill the window"
    );
    let mut sim = Sim::new(shape, 11);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.remote.commit(vec![laws::grant(&[(1, 0)])]);
    sim.want(&[0, 1]);
    sim.quiesce();

    let before = sim.remote.head();
    sim.step(Op::Poison(0b000111));
    sim.members(&[0, 1, 2], 0);
    sim.members(&[3, 4, 5], 1);
    let healthy: BTreeSet<ObjKey> = (3..6).map(obj).collect();
    for _ in 0..200 {
        sim.cycle();
        if sim.local_members(1) == healthy {
            break;
        }
    }
    assert_eq!(
        sim.local_members(1),
        healthy,
        "the failing objects pinned the window: {}",
        sim.dump()
    );
    for _ in 0..30 {
        sim.cycle();
    }
    assert!(
        sim.local_members(0).is_empty(),
        "the failing objects cannot be applied"
    );
    let held = sim
        .local
        .persisted
        .get(&part(0))
        .map_or(0, |cursor| cursor.rev);
    assert!(
        held <= before,
        "part 0's cursor {held} passed the failing objects (written after {before})"
    );
    assert_ne!(sim.sync.synced(&part(0)), Some(true));

    sim.step(Op::Poison(0));
    sim.quiesce();
    assert_eq!(sim.local_members(0), (0..3).map(obj).collect());
    assert_eq!(sim.local_members(1), healthy);
    assert!(sim.local.persisted[&part(0)].rev > before);
}

/// Law 7 of ObjectJobs, the cap: a backend that fails every object keeps them all tracked
/// outside the window, so admission stops at `max_tracked` (plus one revision's overshoot)
/// instead of taking in the whole part. When the failure ends, everything converges.
#[test]
fn persistently_failing_objects_stall_admission_at_the_tracked_cap() {
    let mut shape = Shape::small();
    shape.parts = 1;
    shape.objs = 8;
    shape.obj_routes = 0;
    shape.config.jobs.max_tracked = shape.config.jobs.window;
    let cap = shape.config.jobs.max_tracked;
    let mut sim = Sim::new(shape, 11);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.want(&[0]);
    sim.quiesce();

    sim.step(Op::Poison(0xFF));
    // one revision per object, so no page overshoots the cap by more than one object
    for o in 0..8 {
        sim.members(&[o], 0);
    }
    let mut most = 0;
    for _ in 0..200 {
        sim.cycle();
        most = most.max(sim.sync.jobs.tracked());
    }
    assert!(
        most >= cap && most <= cap + 1,
        "{most} objects tracked while everything fails, the cap is {cap}: {}",
        sim.dump()
    );
    assert!(sim.local_members(0).is_empty());

    sim.step(Op::Poison(0));
    sim.quiesce();
    assert_eq!(sim.local_members(0), (0..8).map(obj).collect());
}

// # Store generations (`PartLifecycle` law 10)

impl Sim {
    /// The peer's log as a client that synced it holds it: part 0 with objects 0 and 1, the
    /// client synced and its cursor persisted (rev 3, under the peer's generation).
    fn synced_with_two_members() -> Self {
        let mut sim = Sim::new(Shape::small(), 11);
        sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
        sim.member(0, 0);
        sim.member(1, 0);
        sim.want(&[0]);
        sim.quiesce();
        assert_eq!(
            sim.local.persisted[&part(0)],
            PeerCursor {
                generation: sim.remote.generation(),
                rev: 3
            }
        );
        sim
    }

    /// Writes that leave objects 2 and 3 in part 0 at revs 2 and 3 and push the head to 6: the
    /// part's latest row (3) is not below the cursor of `synced_with_two_members`, so only the
    /// generation tells a client that resuming from rev 3 would never see them.
    fn rewrite_below_the_old_cursor(&mut self) {
        self.remote.commit(vec![laws::grant(&[(0, 0)])]);
        self.member(2, 0);
        self.member(3, 0);
        self.remote.commit(vec![laws::grant(&[(1, 0)])]);
        self.remote.commit(vec![laws::grant(&[(2, 0)])]);
        self.remote.commit(vec![laws::revoke(&[(1, 0)])]);
        assert_eq!(self.remote.head(), 6);
    }
}

/// The peer loses its data: a new store and generation, with the same identity. The client
/// reconnects, finds another generation in the summary, restarts the part from 0 and takes
/// what the new log holds, though the new head is above its old cursor and the row sits below
/// it. The members of the old log stay: a pull-only client has nothing that removes them.
#[test]
fn a_peer_that_lost_its_data_is_synced_again_from_zero() {
    let mut sim = Sim::synced_with_two_members();
    let old = sim.local.persisted[&part(0)];
    sim.step(Op::DataLoss);
    sim.rewrite_below_the_old_cursor();
    sim.quiesce();
    let members = sim.local_members(0);
    assert!(
        members.contains(&obj(2)) && members.contains(&obj(3)),
        "the new log's members: {members:?}"
    );
    assert_eq!(
        sim.restarts,
        [Restart {
            part: part(0),
            held: old,
            peer_generation: sim.remote.generation(),
            peer_latest: 3,
        }]
    );
    assert_ne!(old.generation, sim.remote.generation());
    assert_eq!(
        sim.local.persisted[&part(0)].generation,
        sim.remote.generation(),
        "the cursor persisted after the restart carries the new generation"
    );
}

/// A restore from an older backup after the operator minted: the files bring back the old
/// log, whose revs the client's cursor has passed, and the new generation restarts the part.
#[test]
fn a_restore_after_a_mint_is_synced_again_from_zero() {
    let mut sim = Sim::new(Shape::small(), 12);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.member(0, 0);
    sim.step(Op::Snapshot);
    let backup_generation = sim.remote.generation();
    sim.member(1, 0);
    sim.want(&[0]);
    sim.quiesce();
    let old = sim.local.persisted[&part(0)];
    assert_eq!(old.generation, backup_generation);
    assert_eq!(old.rev, 3);

    sim.step(Op::RestoreMinted);
    assert_ne!(sim.remote.generation(), backup_generation, "minted");
    // the restored log has its own rev 3
    sim.member(2, 0);
    sim.quiesce();
    assert!(sim.local_members(0).contains(&obj(2)));
    assert_eq!(sim.restarts.len(), 1, "{:?}", sim.restarts);
    assert_eq!(sim.restarts[0].held, old);
    assert_eq!(sim.restarts[0].peer_generation, sim.remote.generation());
}

/// A restore the operator forgot to mint: the generation is the old one, but the log ends
/// below the client's cursor, so the part restarts all the same.
#[test]
fn a_restore_without_a_mint_restarts_when_the_head_is_below_the_cursor() {
    let mut sim = Sim::new(Shape::small(), 13);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.member(0, 0);
    sim.step(Op::Snapshot);
    sim.member(1, 0);
    sim.want(&[0]);
    sim.quiesce();
    let old = sim.local.persisted[&part(0)];
    assert_eq!(sim.remote.saved_head(), Some(2));

    sim.step(Op::RestoreUnminted);
    assert_eq!(sim.remote.generation(), old.generation, "not minted");
    assert_eq!(sim.remote.head(), 2);
    assert_eq!(
        sim.restarts,
        [Restart {
            part: part(0),
            held: old,
            peer_generation: old.generation,
            peer_latest: 2,
        }]
    );
    // the log carries on, and the client follows it from the restarted cursor
    sim.member(2, 0);
    sim.quiesce();
    assert!(sim.local_members(0).contains(&obj(2)));
    assert_eq!(sim.restarts.len(), 1);
}

/// The same data loss with a bucket walk on offer: the restart lists the whole part.
#[test]
fn a_peer_that_lost_its_data_is_walked_from_zero_when_it_offers_a_walk() {
    let mut sim = Sim::synced_with_two_members();
    sim.step(Op::DataLoss);
    sim.remote.walk_bias = true;
    sim.rewrite_below_the_old_cursor();
    sim.quiesce();
    let members = sim.local_members(0);
    assert!(members.contains(&obj(2)) && members.contains(&obj(3)));
    assert_eq!(sim.restarts.len(), 1);
}

/// An unminted restore keeps the generation, so the stale cursor (3) would stay stored until
/// the part's first raise. The client crashes right after the restart decision, before it
/// consumed anything, and the peer's head then passes the old cursor: resuming from the stale
/// cursor would skip the rows the restored log stamped below it. The restart's eager write of
/// cursor 0 prevents that.
#[test]
fn a_crash_right_after_an_unminted_restores_restart_does_not_resume_from_the_stale_cursor() {
    let mut sim = Sim::new(Shape::small(), 14);
    sim.remote.commit(vec![laws::grant(&[(0, 0)])]);
    sim.member(0, 0);
    sim.step(Op::Snapshot);
    sim.member(1, 0);
    sim.want(&[0]);
    sim.quiesce();
    let old = sim.local.persisted[&part(0)];
    assert_eq!(old.rev, 3);

    sim.lose_all();
    assert!(sim.remote.restore(false));
    sim.step(Op::Crash {
        committed: 0,
        close: false,
    });
    // the restart decision, and nothing else: no row is consumed before the crash
    for _ in 0..20 {
        if !sim.restarts.is_empty() {
            break;
        }
        sim.deliver_all();
    }
    assert_eq!(sim.restarts.len(), 1, "the part restarted");
    assert_eq!(
        sim.local.persisted[&part(0)],
        PeerCursor {
            generation: old.generation,
            rev: 0
        },
        "the stale cursor is replaced at the decision"
    );
    sim.step(Op::Crash {
        committed: 0,
        close: true,
    });
    // the restored log grows past the old cursor, with rows stamped below it
    sim.member(2, 0);
    sim.member(3, 0);
    assert!(sim.remote.head() > old.rev);
    sim.quiesce();
    let members = sim.local_members(0);
    assert!(
        members.contains(&obj(2)) && members.contains(&obj(3)),
        "{members:?}"
    );
}
