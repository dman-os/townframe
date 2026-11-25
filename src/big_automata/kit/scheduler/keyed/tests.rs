use super::*;

use proptest::prelude::*;
use std::sync::LazyLock;

static T0: LazyLock<Instant> = LazyLock::new(Instant::now);

fn t(secs: u64) -> Instant {
    *T0 + Duration::from_secs(secs)
}

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

/// Seeds are lists so a merge is observable: `concat` keeps the old work ahead of the new.
type Seed = Vec<u32>;
type Keyed = KeyedScheduler<&'static str, Seed>;

fn concat(mut old: Seed, new: Seed) -> Seed {
    old.extend(new);
    old
}

fn drained(s: &mut Keyed) -> Vec<(TaskId, Seed)> {
    s.drain_spawn_queue()
        .map(|task| (task.id, task.seed))
        .collect()
}

#[test]
fn replace_queues_one_task_per_key() {
    let mut s = Keyed::default();
    let a = s.replace(t(0), "a", vec![1]).expect("idle key");
    let b = s.replace(t(0), "b", vec![2]).expect("idle key");
    assert_eq!(drained(&mut s), vec![(a, vec![1]), (b, vec![2])]);
    assert_eq!(s.active_task(&"a"), Some(a));
    assert_eq!(s.key_of(b), Some(&"b"));
}

/// Law 1, stopped before replaced: replacing a running task stops it and queues nothing until
/// the driver reports the old task ended.
#[test]
fn replacing_a_running_task_waits_for_it_to_end() {
    let mut s = Keyed::default();
    let old = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    assert_eq!(s.replace(t(0), "a", vec![2]), None, "the old task may run");
    assert_eq!(s.drain_stop_queue().collect::<Vec<_>>(), vec![old]);
    assert_eq!(
        drained(&mut s),
        vec![],
        "no replacement beside the old task"
    );
    assert_eq!(s.stopping_task(&"a"), Some(old));
    assert_eq!(s.active_task(&"a"), None);
    assert_eq!(s.stopping(), 1);

    let new = s.stopped(t(1), old).expect("a replacement waits");
    assert_eq!(drained(&mut s), vec![(new, vec![2])]);
    assert_eq!(s.active_task(&"a"), Some(new));
    assert_eq!(s.stopping(), 0);
}

/// Law 3: what the stopping task reports meanwhile is stale and frees nothing.
#[test]
fn a_stopping_task_reports_nothing() {
    let mut s = Keyed::default();
    let old = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    s.replace(t(0), "a", vec![2]);
    assert_eq!(s.complete(old), None);
    assert!(s.fail(old, secs(1), t(0)).is_none());
    assert_eq!(drained(&mut s), vec![], "a late report is not the end");
    assert_eq!(s.stopping_task(&"a"), Some(old));
    assert!(s.stopped(t(0), old).is_some());
}

/// Law 2: replaces while stopping merge into the one waiting seed, and one task starts.
#[test]
fn replaces_while_stopping_merge_into_one_waiting_seed() {
    let mut s = Keyed::default();
    let old = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    assert_eq!(s.replace_with(t(0), "a", vec![2], concat), None);
    assert_eq!(s.replace_with(t(0), "a", vec![3], concat), None);
    assert_eq!(s.drain_stop_queue().count(), 1, "the task is stopped once");
    let new = s.stopped(t(0), old).unwrap();
    assert_eq!(drained(&mut s), vec![(new, vec![1, 2, 3])]);
}

/// A replaced task that was still queued never reached the driver: nothing to abort, nothing to
/// wait for, and it never drains.
#[test]
fn replacing_a_queued_task_never_reaches_the_driver() {
    let mut s = Keyed::default();
    s.replace(t(0), "a", vec![1]);
    let new = s.replace(t(0), "a", vec![2]).expect("nothing runs");
    assert_eq!(drained(&mut s), vec![(new, vec![2])]);
    assert_eq!(s.drain_stop_queue().count(), 0);
    assert_eq!(s.stopping(), 0);
}

/// A pending retry runs nowhere, so replacing it is immediate.
#[test]
fn replacing_a_pending_retry_is_immediate() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    s.fail(task, secs(5), t(0));
    let new = s
        .replace(t(1), "a", vec![2])
        .expect("a retry waiting out its backoff");
    assert_eq!(s.next_due(), None);
    assert_eq!(drained(&mut s), vec![(new, vec![2])]);
    s.tick(t(100));
    assert_eq!(
        drained(&mut s),
        vec![],
        "the replaced retry does not resurface"
    );
}

#[test]
fn replace_with_merges_the_unfinished_seed() {
    let mut s = Keyed::default();
    s.replace(t(0), "a", vec![1]);
    let new = s.replace_with(t(0), "a", vec![2], concat).unwrap();
    assert_eq!(drained(&mut s), vec![(new, vec![1, 2])]);
}

#[test]
fn complete_retires_the_key_and_hands_back_its_seed() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![7]).unwrap();
    drained(&mut s);
    assert_eq!(s.complete(task), Some(("a", vec![7])));
    assert_eq!(s.active_task(&"a"), None);
    assert!(!s.is_parked(&"a"));
    assert_eq!(s.counts().live, 0);
}

#[test]
fn fail_retries_under_the_same_key_after_a_backoff() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![7]).unwrap();
    drained(&mut s);
    let retried = s.fail(task, secs(5), t(0)).expect("current");
    assert_eq!(s.active_task(&"a"), Some(retried));
    assert_eq!(s.next_due(), Some(t(5)));
    s.tick(t(5));
    assert_eq!(drained(&mut s), vec![(retried, vec![7])]);
}

/// The backoff belongs to the attempt: repeated failures widen it, and a key whose task
/// completed starts again from the first attempt.
#[test]
fn backoff_widens_on_failure_and_resets_after_completion() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    let task = s.fail(task, secs(1), t(0)).unwrap();
    s.tick(t(10));
    drained(&mut s);
    let task = s.fail(task, secs(1), t(10)).unwrap();
    assert_eq!(s.retry_of(task).unwrap().backoff, secs(2));
    s.tick(t(20));
    drained(&mut s);
    s.complete(task);

    let task = s.replace(t(20), "a", vec![1]).unwrap();
    drained(&mut s);
    let task = s.fail(task, secs(1), t(20)).unwrap();
    assert_eq!(s.retry_of(task).unwrap().backoff, secs(1));
}

/// A cancelled running task keeps its key busy until the driver reports it ended; a replace
/// in between waits for the same report.
#[test]
fn cancel_keeps_the_key_busy_until_the_task_ends() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    assert_eq!(s.cancel(&"a"), Some(task));
    assert_eq!(s.drain_stop_queue().collect::<Vec<_>>(), vec![task]);
    assert_eq!(
        s.complete(task),
        None,
        "a cancelled task's completion is stale"
    );
    assert_eq!(s.cancel(&"a"), None, "already stopping");
    assert_eq!(s.replace(t(0), "a", vec![2]), None);
    assert_eq!(
        s.stopped(t(0), task).map(|new| s.key_of(new).copied()),
        Some(Some("a"))
    );
    assert_eq!(drained(&mut s).len(), 1);
}

/// A cancel after a replace drops the waiting replacement.
#[test]
fn cancel_drops_the_waiting_replacement() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    s.replace(t(0), "a", vec![2]);
    assert_eq!(s.cancel(&"a"), None);
    assert_eq!(s.stopped(t(0), task), None);
    assert_eq!(drained(&mut s), vec![]);
    assert_eq!(s.stopping_task(&"a"), None);
    assert_eq!(s.counts().live, 0);
}

#[test]
fn cancel_drops_queued_and_parked_work_at_once() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    assert_eq!(s.cancel(&"a"), Some(task));
    assert_eq!(
        s.drain_stop_queue().count(),
        0,
        "it never reached the driver"
    );
    assert_eq!(s.stopping(), 0);

    s.park("b", vec![2]);
    assert_eq!(s.cancel(&"b"), None, "a parked key has no task to stop");
    assert!(!s.is_parked(&"b"));
    assert_eq!(s.wake(t(0), &"b"), None);
}

#[test]
fn a_parked_key_runs_only_after_wake() {
    let mut s = Keyed::default();
    s.park("a", vec![3]);
    assert!(s.is_parked(&"a"));
    assert_eq!(drained(&mut s), vec![]);
    let task = s.wake(t(0), &"a").expect("parked");
    assert_eq!(drained(&mut s), vec![(task, vec![3])]);
    assert!(!s.is_parked(&"a"));
}

/// Parking a stopping key holds the work until the task ends, and a wake before then queues
/// it at the end.
#[test]
fn parking_and_waking_a_stopping_key_waits_for_the_end() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    drained(&mut s);
    s.cancel(&"a");
    s.park("a", vec![2]);
    assert!(s.is_parked(&"a"));
    assert_eq!(s.stopped(t(0), task), None);
    assert!(s.is_parked(&"a"), "parked once the task has ended");
    assert_eq!(drained(&mut s), vec![]);

    let task = s.replace(t(0), "b", vec![1]).unwrap();
    drained(&mut s);
    s.cancel(&"b");
    s.park("b", vec![2]);
    assert_eq!(s.wake(t(0), &"b"), None, "the task is still ending");
    let new = s.stopped(t(0), task).unwrap();
    assert_eq!(drained(&mut s), vec![(new, vec![2])]);
}

#[test]
fn waking_a_busy_or_idle_key_is_a_no_op() {
    let mut s = Keyed::default();
    assert_eq!(s.wake(t(0), &"idle"), None);
    let task = s.replace(t(0), "busy", vec![1]).unwrap();
    assert_eq!(s.wake(t(0), &"busy"), None);
    assert_eq!(s.active_task(&"busy"), Some(task));
}

#[test]
fn replace_on_a_parked_key_merges_the_parked_seed() {
    let mut s = Keyed::default();
    s.park("a", vec![1]);
    let task = s.replace_with(t(0), "a", vec![2], concat).unwrap();
    assert_eq!(drained(&mut s), vec![(task, vec![1, 2])]);
}

#[test]
#[should_panic(expected = "cannot park a key with a current task")]
fn parking_a_busy_key_panics() {
    let mut s = Keyed::default();
    s.replace(t(0), "a", vec![1]);
    s.park("a", vec![2]);
}

#[test]
#[should_panic(expected = "stopped for a task that is not stopping")]
fn stopped_for_a_task_that_is_not_stopping_panics() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]).unwrap();
    s.stopped(t(0), task);
}

// ── model test ────────────────────────────────────────────────────────────────────────────

static KEYS: [&str; 3] = ["a", "b", "c"];

#[derive(Debug, Clone)]
enum Op {
    Replace(usize, u32),
    Cancel(usize),
    Drain,
    Complete(usize),
    Fail(usize, u64),
    Park(usize, u32),
    Wake(usize),
    Advance(u64),
    /// The driver reports the n-th stopping task ended.
    Stopped(usize),
    /// The n-th stopping task reports a result before the driver reports it ended.
    Late(usize, bool),
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..KEYS.len(), any::<u32>()).prop_map(|(k, v)| Op::Replace(k, v)),
        1 => (0..KEYS.len()).prop_map(Op::Cancel),
        3 => Just(Op::Drain),
        2 => any::<usize>().prop_map(Op::Complete),
        2 => (any::<usize>(), 0u64..10).prop_map(|(n, d)| Op::Fail(n, d)),
        1 => (0..KEYS.len(), any::<u32>()).prop_map(|(k, v)| Op::Park(k, v)),
        1 => (0..KEYS.len()).prop_map(Op::Wake),
        2 => (0u64..15).prop_map(Op::Advance),
        3 => any::<usize>().prop_map(Op::Stopped),
        1 => (any::<usize>(), any::<bool>()).prop_map(|(n, ok)| Op::Late(n, ok)),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Place {
    Queued,
    Running,
    Delayed(Instant),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum After {
    Idle,
    Queue(Seed),
    Park(Seed),
}

/// What a key holds. A key with no entry is idle.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Held {
    Parked(Seed),
    Active(TaskId, Place, Seed),
    /// The old task may be running; the key is busy until it ends.
    Stopping(TaskId, After),
}

type Model = BTreeMap<&'static str, Held>;

fn running(model: &Model) -> Vec<(&'static str, TaskId)> {
    model
        .iter()
        .filter_map(|(key, held)| match held {
            Held::Active(id, Place::Running, _) => Some((*key, *id)),
            _ => None,
        })
        .collect()
}

fn stopping(model: &Model) -> Vec<(&'static str, TaskId)> {
    model
        .iter()
        .filter_map(|(key, held)| match held {
            Held::Stopping(id, _) => Some((*key, *id)),
            _ => None,
        })
        .collect()
}

/// The model with the stops it owes the driver and the tasks whose reports are now stale.
#[derive(Default)]
struct Expect {
    model: Model,
    stops: Vec<TaskId>,
    stale: Vec<TaskId>,
}

impl Expect {
    /// `replace_with`: the old task is stopped and the replacement queued, or made to wait.
    /// Returns whether a task is queued now.
    fn replace(&mut self, key: &'static str, v: u32) -> bool {
        let seed = match self.model.remove(key) {
            None => vec![v],
            Some(Held::Parked(old)) => concat(old, vec![v]),
            Some(Held::Active(id, place, old)) => {
                self.stale.push(id);
                let seed = concat(old, vec![v]);
                if place == Place::Running {
                    self.stops.push(id);
                    self.model
                        .insert(key, Held::Stopping(id, After::Queue(seed)));
                    return false;
                }
                seed
            }
            Some(Held::Stopping(id, then)) => {
                let seed = match then {
                    After::Idle => vec![v],
                    After::Queue(old) | After::Park(old) => concat(old, vec![v]),
                };
                self.model
                    .insert(key, Held::Stopping(id, After::Queue(seed)));
                return false;
            }
        };
        self.model
            .insert(key, Held::Active(TaskId::MAX, Place::Queued, seed));
        true
    }

    /// `cancel`: the task the driver was asked to stop, if any.
    fn cancel(&mut self, key: &'static str) -> Option<TaskId> {
        match self.model.remove(key)? {
            Held::Parked(_) => None,
            Held::Active(id, place, _) => {
                self.stale.push(id);
                if place == Place::Running {
                    self.stops.push(id);
                    self.model.insert(key, Held::Stopping(id, After::Idle));
                }
                Some(id)
            }
            Held::Stopping(id, _) => {
                self.model.insert(key, Held::Stopping(id, After::Idle));
                None
            }
        }
    }
}

proptest! {
    #[test]
    fn matches_the_model(ops in prop::collection::vec(any_op(), 1..80)) {
        let mut s = Keyed::default();
        s.set_max_backoff(secs(8));
        let mut now = t(0);
        let mut e = Expect::default();

        for op in ops {
            match op {
                Op::Replace(k, v) => {
                    let key = KEYS[k];
                    let queued = e.replace(key, v);
                    let got = s.replace_with(now, key, vec![v], concat);
                    prop_assert_eq!(got.is_some(), queued);
                    if let Some(task) = got {
                        let Some(Held::Active(id, ..)) = e.model.get_mut(key) else { unreachable!() };
                        *id = task;
                    }
                }
                Op::Cancel(k) => {
                    let expected = e.cancel(KEYS[k]);
                    prop_assert_eq!(s.cancel(&KEYS[k]), expected);
                }
                Op::Drain => {
                    let mut expected: Vec<_> = e.model.values()
                        .filter_map(|held| match held {
                            Held::Active(id, Place::Queued, seed) => Some((*id, seed.clone())),
                            _ => None,
                        })
                        .collect();
                    expected.sort();
                    let mut got = drained(&mut s);
                    got.sort();
                    prop_assert_eq!(got, expected);
                    for held in e.model.values_mut() {
                        if let Held::Active(_, place @ Place::Queued, _) = held {
                            *place = Place::Running;
                        }
                    }
                }
                Op::Complete(n) => {
                    let running = running(&e.model);
                    if running.is_empty() { continue; }
                    let (key, id) = running[n % running.len()];
                    let Some(Held::Active(_, _, seed)) = e.model.remove(key) else { unreachable!() };
                    prop_assert_eq!(s.complete(id), Some((key, seed)));
                    e.stale.push(id);
                }
                Op::Fail(n, delay) => {
                    let running = running(&e.model);
                    if running.is_empty() { continue; }
                    let (key, id) = running[n % running.len()];
                    let retried = s.fail(id, secs(delay), now).expect("current");
                    let backoff = s.retry_of(retried).unwrap().backoff;
                    let Some(Held::Active(task, place, _)) = e.model.get_mut(key) else { unreachable!() };
                    (*task, *place) = (retried, Place::Delayed(now + backoff));
                    e.stale.push(id);
                }
                Op::Park(k, v) => {
                    let key = KEYS[k];
                    match e.model.get_mut(key) {
                        None => { e.model.insert(key, Held::Parked(vec![v])); }
                        Some(Held::Stopping(_, then @ After::Idle)) => *then = After::Park(vec![v]),
                        Some(_) => continue,
                    }
                    s.park(key, vec![v]);
                }
                Op::Wake(k) => {
                    let key = KEYS[k];
                    let got = s.wake(now, &key);
                    match e.model.get_mut(key) {
                        Some(held @ Held::Parked(_)) => {
                            let Held::Parked(seed) = held.clone() else { unreachable!() };
                            *held = Held::Active(got.expect("a parked key wakes"), Place::Queued, seed);
                        }
                        Some(Held::Stopping(_, then)) => {
                            prop_assert_eq!(got, None);
                            if let After::Park(seed) = then {
                                *then = After::Queue(seed.clone());
                            }
                        }
                        _ => prop_assert_eq!(got, None),
                    }
                }
                Op::Advance(dt) => {
                    now += secs(dt);
                    s.tick(now);
                    for held in e.model.values_mut() {
                        if let Held::Active(_, place, _) = held && let Place::Delayed(due_at) = *place && due_at <= now {
                            *place = Place::Queued;
                        }
                    }
                }
                Op::Stopped(n) => {
                    let stopping = stopping(&e.model);
                    if stopping.is_empty() { continue; }
                    let (key, id) = stopping[n % stopping.len()];
                    let Some(Held::Stopping(_, then)) = e.model.remove(key) else { unreachable!() };
                    let got = s.stopped(now, id);
                    match then {
                        After::Idle => prop_assert_eq!(got, None),
                        After::Park(seed) => {
                            prop_assert_eq!(got, None);
                            e.model.insert(key, Held::Parked(seed));
                        }
                        After::Queue(seed) => {
                            e.model.insert(key, Held::Active(got.expect("a replacement waits"), Place::Queued, seed));
                        }
                    }
                }
                Op::Late(n, ok) => {
                    let stopping = stopping(&e.model);
                    if stopping.is_empty() { continue; }
                    let (_, id) = stopping[n % stopping.len()];
                    if ok {
                        prop_assert_eq!(s.complete(id), None);
                    } else {
                        prop_assert_eq!(s.fail(id, secs(1), now), None);
                    }
                }
            }

            // One current task per key, agreeing with the model, and a stopping key has none.
            for key in KEYS {
                let held = e.model.get(key);
                let active = match held { Some(Held::Active(id, ..)) => Some(*id), _ => None };
                let stopping = match held { Some(Held::Stopping(id, _)) => Some(*id), _ => None };
                prop_assert_eq!(s.active_task(&key), active);
                prop_assert_eq!(s.stopping_task(&key), stopping);
                let parked = matches!(held, Some(Held::Parked(_) | Held::Stopping(_, After::Park(_))));
                prop_assert_eq!(s.is_parked(&key), parked);
                if let Some(id) = active {
                    prop_assert_eq!(s.key_of(id), Some(&key));
                }
            }
            // Every live task belongs to a key, and the stopping ones are counted apart.
            prop_assert_eq!(
                s.counts().live,
                e.model.values().filter(|held| matches!(held, Held::Active(..))).count()
            );
            prop_assert_eq!(s.stopping(), stopping(&e.model).len());
            let expected_due = e.model.values()
                .filter_map(|held| match held {
                    Held::Active(_, Place::Delayed(due_at), _) => Some(*due_at),
                    _ => None,
                })
                .min();
            prop_assert_eq!(s.next_due(), expected_due);
            // Only running tasks that were replaced or cancelled reach the driver's stop queue.
            prop_assert_eq!(
                s.drain_stop_queue().collect::<Vec<_>>(),
                std::mem::take(&mut e.stops)
            );
            // Reports from retired tasks are stale.
            for id in &e.stale {
                prop_assert_eq!(s.key_of(*id), None);
            }
        }
    }
}
