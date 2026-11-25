//! The keyed scheduler at 10k and 100k keys: start, replace a running task, and retry.

use utils_rs::alloc_count::measure;

use super::assert_linear;
use crate::interlude::*;
use crate::kit::scheduler::Instant;
use crate::kit::scheduler::TaskId;
use crate::kit::scheduler::keyed::KeyedScheduler;

const SMALL: usize = 10_000;

type Keyed = KeyedScheduler<u32, u32>;

fn key(row: usize) -> u32 {
    u32::try_from(row).expect("rows fit u32")
}

fn started(n: usize, now: Instant) -> (Keyed, Vec<TaskId>) {
    let mut scheduler = Keyed::default();
    for row in 0..n {
        scheduler
            .replace(now, key(row), key(row))
            .expect("idle key");
    }
    let tasks = scheduler.drain_spawn_queue().map(|task| task.id).collect();
    (scheduler, tasks)
}

#[test]
fn keyed_scheduler_start_and_complete_are_linear() {
    assert_linear("keyed scheduler start and complete", SMALL, |n| {
        let now = Instant::now();
        let mut scheduler = Keyed::default();
        measure(|| {
            for row in 0..n {
                scheduler
                    .replace(now, key(row), key(row))
                    .expect("idle key");
            }
            let tasks: Vec<_> = scheduler.drain_spawn_queue().map(|task| task.id).collect();
            for task in tasks {
                scheduler.complete(task).expect("current task");
            }
        })
        .1
    });
}

/// Replacing every running task: it stops, then its replacement starts once the stop is
/// reported.
#[test]
fn keyed_scheduler_replace_running_is_linear() {
    assert_linear("keyed scheduler replace running", SMALL, |n| {
        let now = Instant::now();
        let (mut scheduler, _tasks) = started(n, now);
        measure(|| {
            for row in 0..n {
                assert!(scheduler.replace(now, key(row), key(row) + 1).is_none());
            }
            let stopped: Vec<_> = scheduler.drain_stop_queue().collect();
            for task in stopped {
                scheduler.stopped(now, task).expect("a replacement waits");
            }
            assert_eq!(scheduler.drain_spawn_queue().count(), n);
        })
        .1
    });
}

/// Every task fails, waits out its backoff and comes back.
#[test]
fn keyed_scheduler_fail_and_retry_are_linear() {
    assert_linear("keyed scheduler fail and retry", SMALL, |n| {
        let now = Instant::now();
        let (mut scheduler, tasks) = started(n, now);
        measure(|| {
            for task in tasks {
                scheduler
                    .fail(task, Duration::from_secs(1), now)
                    .expect("current task");
            }
            scheduler.tick(now + Duration::from_secs(2));
            assert_eq!(scheduler.drain_spawn_queue().count(), n);
        })
        .1
    });
}
