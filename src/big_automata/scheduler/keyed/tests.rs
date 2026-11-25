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
    let a = s.replace(t(0), "a", vec![1]);
    let b = s.replace(t(0), "b", vec![2]);
    assert_eq!(drained(&mut s), vec![(a, vec![1]), (b, vec![2])]);
    assert_eq!(s.active_task(&"a"), Some(a));
    assert_eq!(s.key_of(b), Some(&"b"));
}

/// Replacing a running task stops it, and its late completion is stale.
#[test]
fn replacing_a_running_task_stops_it() {
    let mut s = Keyed::default();
    let old = s.replace(t(0), "a", vec![1]);
    drained(&mut s);
    let new = s.replace(t(0), "a", vec![2]);
    assert_eq!(s.drain_stop_queue().collect::<Vec<_>>(), vec![old]);
    assert_eq!(s.complete(old), None);
    assert!(s.fail(old, secs(1), t(0)).is_none());
    assert_eq!(s.active_task(&"a"), Some(new));
}

/// A replaced task that was still queued never reached the driver: nothing to abort, and it
/// never drains.
#[test]
fn replacing_a_queued_task_never_reaches_the_driver() {
    let mut s = Keyed::default();
    s.replace(t(0), "a", vec![1]);
    let new = s.replace(t(0), "a", vec![2]);
    assert_eq!(drained(&mut s), vec![(new, vec![2])]);
    assert_eq!(s.drain_stop_queue().count(), 0);
}

#[test]
fn replace_with_merges_the_unfinished_seed() {
    let mut s = Keyed::default();
    s.replace(t(0), "a", vec![1]);
    drained(&mut s);
    let new = s.replace_with(t(0), "a", vec![2], concat);
    assert_eq!(drained(&mut s), vec![(new, vec![1, 2])]);
}

#[test]
fn complete_retires_the_key_and_hands_back_its_seed() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![7]);
    drained(&mut s);
    assert_eq!(s.complete(task), Some(("a", vec![7])));
    assert_eq!(s.active_task(&"a"), None);
    assert!(!s.is_parked(&"a"));
    assert_eq!(s.counts().live, 0);
}

#[test]
fn fail_retries_under_the_same_key_after_a_backoff() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![7]);
    drained(&mut s);
    let retried = s.fail(task, secs(5), t(0)).expect("current");
    assert_eq!(s.active_task(&"a"), Some(retried));
    assert_eq!(s.next_due(), Some(t(5)));
    s.tick(t(5));
    assert_eq!(drained(&mut s), vec![(retried, vec![7])]);
}

/// A newer event stays the fast path: replacing a key whose retry is pending replaces the
/// retry, and nothing stays due.
#[test]
fn replacing_a_pending_retry_leaves_nothing_due() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]);
    drained(&mut s);
    s.fail(task, secs(5), t(0));
    let new = s.replace(t(1), "a", vec![2]);
    assert_eq!(s.next_due(), None);
    assert_eq!(drained(&mut s), vec![(new, vec![2])]);
    s.tick(t(100));
    assert_eq!(
        drained(&mut s),
        vec![],
        "the replaced retry does not resurface"
    );
}

/// The backoff belongs to the attempt: repeated failures widen it, and a key whose task
/// completed starts again from the first attempt.
#[test]
fn backoff_widens_on_failure_and_resets_after_completion() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]);
    drained(&mut s);
    let task = s.fail(task, secs(1), t(0)).unwrap();
    s.tick(t(10));
    drained(&mut s);
    let task = s.fail(task, secs(1), t(10)).unwrap();
    assert_eq!(s.retry_of(task).unwrap().backoff, secs(2));
    s.tick(t(20));
    drained(&mut s);
    s.complete(task);

    let task = s.replace(t(20), "a", vec![1]);
    drained(&mut s);
    let task = s.fail(task, secs(1), t(20)).unwrap();
    assert_eq!(s.retry_of(task).unwrap().backoff, secs(1));
}

#[test]
fn cancel_drops_current_and_parked_work() {
    let mut s = Keyed::default();
    let task = s.replace(t(0), "a", vec![1]);
    drained(&mut s);
    assert_eq!(s.cancel(&"a"), Some(task));
    assert_eq!(s.drain_stop_queue().collect::<Vec<_>>(), vec![task]);
    assert_eq!(
        s.complete(task),
        None,
        "a cancelled task's completion is stale"
    );

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

#[test]
fn waking_a_busy_or_idle_key_is_a_no_op() {
    let mut s = Keyed::default();
    assert_eq!(s.wake(t(0), &"idle"), None);
    let task = s.replace(t(0), "busy", vec![1]);
    assert_eq!(s.wake(t(0), &"busy"), None);
    assert_eq!(s.active_task(&"busy"), Some(task));
}

#[test]
fn replace_on_a_parked_key_merges_the_parked_seed() {
    let mut s = Keyed::default();
    s.park("a", vec![1]);
    let task = s.replace_with(t(0), "a", vec![2], concat);
    assert_eq!(drained(&mut s), vec![(task, vec![1, 2])]);
}

#[test]
#[should_panic(expected = "cannot park a key with a current task")]
fn parking_a_busy_key_panics() {
    let mut s = Keyed::default();
    s.replace(t(0), "a", vec![1]);
    s.park("a", vec![2]);
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
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Place {
    Queued,
    Running,
    Delayed(Instant),
}

/// Per key: no entry is idle; `(None, seed)` is parked; `(Some((task, place)), seed)` is a
/// current task.
type Model = BTreeMap<&'static str, (Option<(TaskId, Place)>, Seed)>;

fn running(model: &Model) -> Vec<TaskId> {
    model
        .values()
        .filter_map(|(task, _)| match task {
            Some((id, Place::Running)) => Some(*id),
            _ => None,
        })
        .collect()
}

proptest! {
    #[test]
    fn matches_the_model(ops in prop::collection::vec(any_op(), 1..60)) {
        let mut s = Keyed::default();
        s.set_max_backoff(secs(8));
        let mut now = t(0);
        let mut model = Model::new();
        let mut expected_stops = vec![];
        let mut stale = vec![];

        for op in ops {
            match op {
                Op::Replace(k, v) => {
                    let key = KEYS[k];
                    let task = s.replace_with(now, key, vec![v], concat);
                    let mut seed = vec![];
                    if let Some((old, old_seed)) = model.remove(key) {
                        seed = old_seed;
                        if let Some((old_id, place)) = old {
                            stale.push(old_id);
                            if place == Place::Running {
                                expected_stops.push(old_id);
                            }
                        }
                    }
                    seed.push(v);
                    model.insert(key, (Some((task, Place::Queued)), seed));
                }
                Op::Cancel(k) => {
                    let key = KEYS[k];
                    let expected = match model.remove(key) {
                        Some((Some((id, place)), _)) => {
                            stale.push(id);
                            if place == Place::Running {
                                expected_stops.push(id);
                            }
                            Some(id)
                        }
                        _ => None,
                    };
                    prop_assert_eq!(s.cancel(&key), expected);
                }
                Op::Drain => {
                    let mut expected: Vec<_> = model
                        .values()
                        .filter_map(|(task, seed)| match task {
                            Some((id, Place::Queued)) => Some((*id, seed.clone())),
                            _ => None,
                        })
                        .collect();
                    expected.sort();
                    let mut got = drained(&mut s);
                    got.sort();
                    prop_assert_eq!(got, expected);
                    for (task, _) in model.values_mut() {
                        if let Some((_, place)) = task && *place == Place::Queued {
                            *place = Place::Running;
                        }
                    }
                }
                Op::Complete(n) => {
                    let running = running(&model);
                    if running.is_empty() { continue; }
                    let id = running[n % running.len()];
                    let key = *model.iter().find(|(_, (task, _))| matches!(task, Some((i, _)) if *i == id)).unwrap().0;
                    let (_, seed) = model.remove(key).unwrap();
                    prop_assert_eq!(s.complete(id), Some((key, seed)));
                    stale.push(id);
                }
                Op::Fail(n, delay) => {
                    let running = running(&model);
                    if running.is_empty() { continue; }
                    let id = running[n % running.len()];
                    let key = *model.iter().find(|(_, (task, _))| matches!(task, Some((i, _)) if *i == id)).unwrap().0;
                    let retried = s.fail(id, secs(delay), now).expect("current");
                    let backoff = s.retry_of(retried).unwrap().backoff;
                    model.get_mut(key).unwrap().0 = Some((retried, Place::Delayed(now + backoff)));
                    stale.push(id);
                }
                Op::Park(k, v) => {
                    let key = KEYS[k];
                    if model.contains_key(key) { continue; }
                    s.park(key, vec![v]);
                    model.insert(key, (None, vec![v]));
                }
                Op::Wake(k) => {
                    let key = KEYS[k];
                    let got = s.wake(now, &key);
                    match model.get_mut(key) {
                        Some((task @ None, _)) => {
                            let id = got.expect("a parked key wakes");
                            *task = Some((id, Place::Queued));
                        }
                        _ => prop_assert_eq!(got, None),
                    }
                }
                Op::Advance(dt) => {
                    now += secs(dt);
                    s.tick(now);
                    for (task, _) in model.values_mut() {
                        if let Some((_, place)) = task && let Place::Delayed(due_at) = *place && due_at <= now {
                            *place = Place::Queued;
                        }
                    }
                }
            }

            // One current task per key, agreeing with the model.
            for key in KEYS {
                let expected = model.get(key).and_then(|(task, _)| task.as_ref().map(|(id, _)| *id));
                prop_assert_eq!(s.active_task(&key), expected);
                prop_assert_eq!(s.is_parked(&key), matches!(model.get(key), Some((None, _))));
                if let Some(id) = expected {
                    prop_assert_eq!(s.key_of(id), Some(&key));
                }
            }
            // Every live task belongs to a key.
            prop_assert_eq!(
                s.counts().live,
                model.values().filter(|(task, _)| task.is_some()).count()
            );
            let expected_due = model
                .values()
                .filter_map(|(task, _)| match task {
                    Some((_, Place::Delayed(due_at))) => Some(*due_at),
                    _ => None,
                })
                .min();
            prop_assert_eq!(s.next_due(), expected_due);
            // Only running tasks that were replaced or cancelled reach the driver's stop queue.
            prop_assert_eq!(
                s.drain_stop_queue().collect::<Vec<_>>(),
                std::mem::take(&mut expected_stops)
            );
            // Reports from retired tasks are stale.
            for id in &stale {
                prop_assert_eq!(s.key_of(*id), None);
            }
        }
    }
}
