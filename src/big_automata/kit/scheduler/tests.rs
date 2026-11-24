use super::*;

use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::LazyLock;

/// A fixed origin so tests talk in offsets.
static T0: LazyLock<Instant> = LazyLock::new(Instant::now);

fn t(secs: u64) -> Instant {
    *T0 + Duration::from_secs(secs)
}

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seed {
    Diff,
    Sync(u8),
}

fn drained(s: &mut Scheduler<Seed>) -> Vec<TaskId> {
    s.drain_spawn_queue().map(|task| task.id).collect()
}

#[test]
fn queued_tasks_drain_in_queue_order() {
    let mut s = Scheduler::default();
    let a = s.queue(t(0), Seed::Sync(1));
    let b = s.queue(t(0), Seed::Sync(2));
    assert_eq!(drained(&mut s), vec![a, b]);
    assert_eq!(
        s.counts().live,
        2,
        "drained tasks are still owned until retired"
    );
}

#[test]
fn stopping_a_running_task_enters_the_stop_queue() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Diff);
    drained(&mut s);
    assert!(s.stop(id).is_some());
    assert_eq!(s.drain_stop_queue().collect::<Vec<_>>(), vec![id]);
    assert_eq!(s.counts().live, 0);
}

/// A task still in the spawn queue never reached the driver, so there is nothing to abort.
#[test]
fn stopping_a_queued_task_never_reaches_the_driver() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Diff);
    assert!(s.stop(id).is_some());
    assert_eq!(
        s.counts(),
        SchedulerCounts {
            live: 0,
            delayed: 0,
            spawn_queue: 0,
            stop_queue: 0
        }
    );
}

#[test]
fn stopping_a_delayed_task_vanishes() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Diff);
    drained(&mut s);
    let retried = s.retry(id, Seed::Diff, secs(5), t(0)).expect("live");
    assert!(s.stop(retried).is_some());
    assert_eq!(
        s.counts(),
        SchedulerCounts {
            live: 0,
            delayed: 0,
            spawn_queue: 0,
            stop_queue: 0
        }
    );
    assert_eq!(s.next_due(), None);
}

#[test]
fn completing_retires_the_task() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Diff);
    drained(&mut s);
    assert!(s.complete(id).is_some());
    assert_eq!(s.counts().live, 0);
    assert_eq!(
        s.counts().stop_queue,
        0,
        "a completed task has nothing to abort"
    );
}

/// A completion or failure can race a stop: the stop wins and the late report is stale.
#[test]
fn a_report_for_a_stopped_task_is_stale() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Diff);
    drained(&mut s);
    s.stop(id);
    assert!(s.complete(id).is_none());
    assert!(s.retry(id, Seed::Diff, secs(1), t(0)).is_none());
    assert_eq!(s.counts().live, 0);
}

#[test]
#[should_panic(expected = "the driver was never handed")]
fn completing_a_task_still_in_the_spawn_queue_panics() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Diff);
    s.complete(id);
}

#[test]
fn a_failed_task_is_retried_after_its_backoff() {
    let mut s = Scheduler::default();
    let id = s.queue(t(0), Seed::Sync(1));
    drained(&mut s);
    let retried = s.retry(id, Seed::Sync(1), secs(5), t(0)).expect("live");
    assert_ne!(retried, id, "a retry is a new task");
    assert_eq!(s.retry_of(retried).unwrap().attempt_no, 2);
    assert_eq!(s.next_due(), Some(t(5)));

    s.tick(t(4));
    assert_eq!(drained(&mut s), Vec::<TaskId>::new(), "not due yet");
    s.tick(t(5));
    assert_eq!(drained(&mut s), vec![retried]);
    assert_eq!(s.next_due(), None);
}

#[test]
fn backoff_doubles_with_floor_and_cap() {
    let mut s = Scheduler::default();
    s.set_max_backoff(secs(30));
    let mut id = s.queue(t(0), Seed::Sync(1));
    let mut backoffs = vec![];
    for _ in 0..5 {
        drained(&mut s);
        id = s.retry(id, Seed::Sync(1), secs(3), t(0)).expect("live");
        backoffs.push(s.retry_of(id).unwrap().backoff);
        s.tick(t(1000));
    }
    assert_eq!(
        backoffs,
        vec![secs(3), secs(6), secs(12), secs(24), secs(30)]
    );
}

#[test]
fn the_cap_wins_over_the_floor() {
    let mut s = Scheduler::default();
    s.set_max_backoff(secs(2));
    let id = s.queue(t(0), Seed::Diff);
    drained(&mut s);
    let id = s.retry(id, Seed::Diff, secs(5), t(0)).expect("live");
    assert_eq!(s.retry_of(id).unwrap().backoff, secs(2));
}

#[test]
fn zero_max_backoff_falls_back_to_one_minute_cap() {
    let mut s = Scheduler::default();
    s.set_max_backoff(Duration::ZERO);
    let id = s.queue(t(0), Seed::Diff);
    drained(&mut s);
    let id = s.retry(id, Seed::Diff, secs(120), t(0)).expect("live");
    assert_eq!(s.retry_of(id).unwrap().backoff, secs(60));
}

#[test]
fn tick_releases_due_tasks_earliest_first() {
    let mut s = Scheduler::default();
    let a = s.queue(t(0), Seed::Sync(1));
    let b = s.queue(t(0), Seed::Sync(2));
    drained(&mut s);
    let late = s.retry(a, Seed::Sync(1), secs(9), t(0)).unwrap();
    let early = s.retry(b, Seed::Sync(2), secs(3), t(0)).unwrap();
    assert_eq!(s.next_due(), Some(t(3)));
    s.tick(t(10));
    assert_eq!(drained(&mut s), vec![early, late]);
}

#[test]
fn requeued_tasks_stay_first_in_line() {
    let mut s = Scheduler::default();
    let a = s.queue(t(0), Seed::Sync(1));
    let b = s.queue(t(0), Seed::Sync(2));
    let mut taken: Vec<_> = s.drain_spawn_queue().collect();
    let c = s.queue(t(0), Seed::Sync(3));
    // the driver only had budget for `a`
    let unrun = taken.split_off(1);
    s.requeue_spawned(unrun);
    assert_eq!(drained(&mut s), vec![b, c]);
    assert_eq!(taken[0].id, a);
}

// ── model test ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Op {
    Queue,
    Drain,
    /// Complete the n-th running task, if any.
    Complete(usize),
    /// Fail the n-th running task, retrying it after `min_delay` seconds.
    Fail(usize, u64),
    /// Stop the n-th live task, if any.
    Stop(usize),
    /// Advance the clock by this many seconds and tick.
    Advance(u64),
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => Just(Op::Queue),
        3 => Just(Op::Drain),
        2 => any::<usize>().prop_map(Op::Complete),
        2 => (any::<usize>(), 0u64..20).prop_map(|(n, delay)| Op::Fail(n, delay)),
        2 => any::<usize>().prop_map(Op::Stop),
        2 => (0u64..30).prop_map(Op::Advance),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Where {
    Queued,
    Running,
    Delayed(Instant),
}

const MAX_BACKOFF: u64 = 16;

proptest! {
    #[test]
    fn matches_the_model(ops in prop::collection::vec(any_op(), 1..60)) {
        let mut s = Scheduler::default();
        s.set_max_backoff(secs(MAX_BACKOFF));
        let mut now = t(0);
        let mut model: BTreeMap<TaskId, (Where, Duration)> = BTreeMap::new();
        let mut expected_stops = vec![];
        let mut last_id = None;

        for op in ops {
            let ticked = matches!(op, Op::Advance(_));
            match op {
                Op::Queue => {
                    let id = s.queue(now, Seed::Diff);
                    prop_assert!(last_id.is_none_or(|last| id > last), "ids increase");
                    last_id = Some(id);
                    model.insert(id, (Where::Queued, Duration::ZERO));
                }
                Op::Drain => {
                    let expected: Vec<_> = model
                        .iter()
                        .filter(|(_, (place, _))| *place == Where::Queued)
                        .map(|(id, _)| *id)
                        .collect();
                    let mut got = drained(&mut s);
                    got.sort();
                    prop_assert_eq!(got, expected.clone());
                    for id in expected {
                        model.get_mut(&id).unwrap().0 = Where::Running;
                    }
                }
                Op::Complete(n) => {
                    let running: Vec<_> = model.iter().filter(|(_, (place, _))| *place == Where::Running).map(|(id, _)| *id).collect();
                    if running.is_empty() { continue; }
                    let id = running[n % running.len()];
                    prop_assert!(s.complete(id).is_some());
                    model.remove(&id);
                }
                Op::Fail(n, delay) => {
                    let running: Vec<_> = model.iter().filter(|(_, (place, _))| *place == Where::Running).map(|(id, _)| *id).collect();
                    if running.is_empty() { continue; }
                    let id = running[n % running.len()];
                    let (_, prev) = model.remove(&id).unwrap();
                    let next = s.retry(id, Seed::Diff, secs(delay), now).expect("live");
                    let backoff = s.retry_of(next).unwrap().backoff;
                    prop_assert!(backoff <= secs(MAX_BACKOFF), "capped");
                    prop_assert!(backoff >= secs(delay).min(secs(MAX_BACKOFF)), "floored");
                    prop_assert!(backoff >= prev.min(secs(MAX_BACKOFF)), "never shrinks");
                    prop_assert!(last_id.is_none_or(|last| next > last), "ids increase");
                    last_id = Some(next);
                    model.insert(next, (Where::Delayed(now + backoff), backoff));
                }
                Op::Stop(n) => {
                    if model.is_empty() { continue; }
                    let id = *model.keys().nth(n % model.len()).unwrap();
                    let (place, _) = model.remove(&id).unwrap();
                    prop_assert!(s.stop(id).is_some());
                    if place == Where::Running {
                        expected_stops.push(id);
                    }
                }
                Op::Advance(dt) => {
                    now += secs(dt);
                    s.tick(now);
                    for (place, _) in model.values_mut() {
                        if let Where::Delayed(due_at) = *place && due_at <= now {
                            *place = Where::Queued;
                        }
                    }
                }
            }

            let counts = s.counts();
            prop_assert_eq!(counts.live, model.len());
            prop_assert_eq!(counts.delayed, model.values().filter(|(place, _)| matches!(place, Where::Delayed(_))).count());
            prop_assert_eq!(counts.spawn_queue, model.values().filter(|(place, _)| *place == Where::Queued).count());
            let expected_due = model.values().filter_map(|(place, _)| match place { Where::Delayed(due_at) => Some(*due_at), _ => None }).min();
            prop_assert_eq!(s.next_due(), expected_due);
            if ticked {
                prop_assert!(expected_due.is_none_or(|due_at| due_at > now), "a tick leaves nothing due behind");
            }
            prop_assert_eq!(s.drain_stop_queue().collect::<Vec<_>>(), std::mem::take(&mut expected_stops));
        }
    }
}
