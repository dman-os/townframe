use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::Notify;
use tokio::sync::watch;
use wflow_core::kvstore::atomic::CommitError;
use wflow_core::kvstore::atomic::MemoryStore;
use wflow_core::kvstore::atomic::MemoryWriter;
use wflow_core::kvstore::atomic::Store;
use wflow_wdk::JobErrorX;
use wflow_wdk::Json;

use super::*;
use crate::local_native_host::LocalNativeHost;

mod cleanup;

/// Counts the atomic commits a runtime issues, including the epoch bump of
/// `Journal::open`, so tests can pin the per-transition commit cost and wait
/// for a transition to become durable.
struct CountingWriter {
    inner: MemoryWriter,
    commits: watch::Sender<u64>,
}

#[async_trait]
impl Writer for CountingWriter {
    fn reader(&self) -> Arc<dyn Store> {
        self.inner.reader()
    }

    async fn commit(
        &mut self,
        expected_revision: u64,
        mutations: Vec<Mutation>,
    ) -> Result<u64, CommitError> {
        let revision = self.inner.commit(expected_revision, mutations).await?;
        self.commits.send_modify(|commits| *commits += 1);
        Ok(revision)
    }
}

/// Starts a runtime over `store` and returns it with its commit counter.
async fn start(
    store: &MemoryStore,
    host: &LocalNativeHost,
) -> (AuthorityRuntime, watch::Receiver<u64>) {
    let (commits, counter) = watch::channel(0);
    let writer = CountingWriter {
        inner: store.writer().unwrap(),
        commits,
    };
    let runtime = AuthorityRuntime::start(writer, 0, Arc::new(host.clone()))
        .await
        .unwrap();
    (runtime, counter)
}

/// Resolves once the runtime has issued `count` commits.
async fn committed(commits: &mut watch::Receiver<u64>, count: u64) {
    commits.wait_for(|commits| *commits >= count).await.unwrap();
}

/// The job's durable transitions, in commit order.
async fn transitions(runtime: &AuthorityRuntime, key: &str) -> Vec<authority::Transition> {
    let frontier = runtime
        .reader
        .read(1, RECOVERY_PAGE)
        .await
        .unwrap()
        .frontier;
    read_transitions(&runtime.reader, 1, frontier)
        .await
        .unwrap()
        .into_iter()
        .filter(|transition| transition_key(transition) == key)
        .collect()
}

fn transition_key(transition: &authority::Transition) -> &str {
    use authority::Transition as T;
    match transition {
        T::Submitted { key, .. }
        | T::Granted { key, .. }
        | T::StepCommitted { key, .. }
        | T::Completed { key, .. }
        | T::Revoked { key, .. }
        | T::WaitStarted { key, .. }
        | T::WaitResolved { key, .. }
        | T::RetryScheduled { key, .. }
        | T::Faulted { key, .. }
        | T::CleanupStarted { key, .. }
        | T::MessageAccepted { key, .. }
        | T::Expired { key, .. } => key,
    }
}

/// Side-effect counters shared by the test workflows, observed across
/// restarts to prove which step closures actually ran.
#[derive(Default)]
struct Probe {
    steps_runs: AtomicU64,
    gated_first_runs: AtomicU64,
    gated_second_runs: AtomicU64,
    gate_open: AtomicBool,
    gate_entered: Notify,
    flaky_first_runs: AtomicU64,
    flaky_second_runs: AtomicU64,
    sleeper_before_runs: AtomicU64,
    sleeper_after_runs: AtomicU64,
    inbox_effect_gate: Gate,
    inbox_replay_gate: Gate,
}

/// Holds every caller of [`Gate::pass`] while closed, announcing each one on
/// `entered`.
#[derive(Default)]
struct Gate {
    closed: AtomicBool,
    entered: Notify,
    opened: Notify,
}

impl Gate {
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn open(&self) {
        self.closed.store(false, Ordering::SeqCst);
        self.opened.notify_waiters();
    }

    async fn pass(&self) {
        // Registered before the check so an `open` racing it still wakes us.
        let opened = self.opened.notified();
        tokio::pin!(opened);
        opened.as_mut().enable();
        if self.closed.load(Ordering::SeqCst) {
            self.entered.notify_one();
            opened.await;
        }
    }
}

fn host(probe: &Arc<Probe>) -> LocalNativeHost {
    let host = LocalNativeHost::new();
    let p = Arc::clone(probe);
    host.register_typed("steps", move |cx, n: u64| {
        let p = Arc::clone(&p);
        Box::pin(async move {
            let mut sum = 0;
            for i in 0..n {
                let p = Arc::clone(&p);
                sum += cx
                    .effect(|| async move {
                        p.steps_runs.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(i + 1))
                    })
                    .await?;
            }
            Ok(sum)
        })
    });
    let p = Arc::clone(probe);
    host.register_typed("gated", move |cx, (): ()| {
        let p = Arc::clone(&p);
        Box::pin(async move {
            let first: u64 = cx
                .effect(|| async {
                    p.gated_first_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(Json(1))
                })
                .await?;
            let second: u64 = cx
                .effect(|| async {
                    p.gated_second_runs.fetch_add(1, Ordering::SeqCst);
                    if !p.gate_open.load(Ordering::SeqCst) {
                        p.gate_entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(Json(10))
                })
                .await?;
            Ok(first + second)
        })
    });
    let p = Arc::clone(probe);
    host.register_typed("flaky", move |cx, (): ()| {
        let p = Arc::clone(&p);
        Box::pin(async move {
            let first: u64 = cx
                .effect(|| async {
                    p.flaky_first_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(Json(1))
                })
                .await?;
            let second: u64 = cx
                .effect(|| async {
                    if p.flaky_second_runs.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(JobErrorX::Transient(ferr!("try again")));
                    }
                    Ok(Json(2))
                })
                .await?;
            Ok(first + second)
        })
    });
    let p = Arc::clone(probe);
    host.register_typed("sleeper", move |cx, sleep_ms: u64| {
        let p = Arc::clone(&p);
        Box::pin(async move {
            let before: u64 = cx
                .effect(|| async {
                    p.sleeper_before_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(Json(1))
                })
                .await?;
            cx.sleep(Duration::from_millis(sleep_ms)).await?;
            let after: u64 = cx
                .effect(|| async {
                    p.sleeper_after_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(Json(2))
                })
                .await?;
            Ok(before + after)
        })
    });
    let p = Arc::clone(probe);
    host.register_typed("inbox", move |cx, (): ()| {
        let p = Arc::clone(&p);
        Box::pin(async move {
            cx.effect(|| async {
                p.inbox_effect_gate.pass().await;
                Ok(Json(()))
            })
            .await?;
            let Json(first): Json<u64> = cx.recv().await?;
            // Outside any step, so it also holds a replaying invocation.
            p.inbox_replay_gate.pass().await;
            let Json(second): Json<u64> = cx.recv().await?;
            Ok(first * 10 + second)
        })
    });
    host.register_typed("fails", |_cx, (): ()| {
        Box::pin(async { Err::<(), _>(JobErrorX::Terminal(ferr!("nope"))) })
    });
    host.register_typed("panics", |_cx, (): ()| {
        Box::pin(async {
            panic!("kaboom");
            #[expect(unreachable_code, reason = "type inference after panic")]
            Ok::<(), JobErrorX>(())
        })
    });
    host
}

fn ok(value: impl Serialize) -> types::JobResult {
    Ok(serde_json::to_string(&value).unwrap())
}

#[tokio::test]
async fn warm_job_commits_once_per_transition_and_survives_restart() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let host = host(&probe);

    let (runtime, commits) = start(&store, &host).await;
    let ack = runtime
        .submit(Submission::new("job", "steps", "3"))
        .await
        .unwrap();
    assert_eq!(ack.disposition, authority::Disposition::Admitted);
    assert_eq!(runtime.await_result("job").await, Some(ok(6)));
    // Epoch bump, Submitted, Granted, three steps, Completed.
    assert_eq!(*commits.borrow(), 7);
    assert_eq!(probe.steps_runs.load(Ordering::SeqCst), 3);
    assert_eq!(runtime.result("job").await.unwrap(), Some(ok(6)));
    assert_eq!(runtime.result("never").await.unwrap(), None);
    assert_eq!(runtime.await_result("never").await, None);
    assert_eq!(
        runtime.submit(Submission::new("job", "steps", "4")).await,
        Err(SubmitError::PayloadConflict)
    );
    assert_eq!(
        runtime
            .submit(Submission::new("other", "missing", "null"))
            .await,
        Err(SubmitError::UnknownWorkflow)
    );
    runtime.stop().await;

    let (runtime, commits) = start(&store, &host).await;
    assert_eq!(runtime.result("job").await.unwrap(), Some(ok(6)));
    let resubmitted = runtime
        .submit(Submission::new("job", "steps", "3"))
        .await
        .unwrap();
    assert_eq!(resubmitted.position, ack.position);
    assert_eq!(resubmitted.disposition, authority::Disposition::Completed);
    assert_eq!(runtime.await_result("job").await, Some(ok(6)));
    // Only the epoch bump: a terminal job is neither granted nor re-run.
    assert_eq!(*commits.borrow(), 1);
    assert_eq!(probe.steps_runs.load(Ordering::SeqCst), 3);
    runtime.stop().await;
}

#[tokio::test]
async fn restart_replays_durable_steps_and_reruns_only_the_interrupted_one() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());

    let (runtime, _) = start(&store, &host(&probe)).await;
    runtime
        .submit(Submission::new("job", "gated", "null"))
        .await
        .unwrap();
    // The second step only starts after the first one's commit resumed it.
    probe.gate_entered.notified().await;
    assert_eq!(runtime.result("job").await.unwrap(), None);
    runtime.stop().await;

    probe.gate_open.store(true, Ordering::SeqCst);
    let (runtime, commits) = start(&store, &host(&probe)).await;
    assert_eq!(runtime.await_result("job").await, Some(ok(11)));
    // Epoch bump, re-Granted, second step, Completed.
    assert_eq!(*commits.borrow(), 4);
    assert_eq!(probe.gated_first_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.gated_second_runs.load(Ordering::SeqCst), 2);
    runtime.stop().await;
}

#[tokio::test]
async fn failures_retry_transients_and_commit_terminal_errors() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let (runtime, commits) = start(&store, &host(&probe)).await;

    runtime
        .submit(Submission::new("flaky", "flaky", "null"))
        .await
        .unwrap();
    assert_eq!(runtime.await_result("flaky").await, Some(ok(3)));
    // Epoch bump, Submitted, Granted, first step, Revoked, re-Granted,
    // second step, Completed.
    assert_eq!(*commits.borrow(), 8);
    assert_eq!(probe.flaky_first_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.flaky_second_runs.load(Ordering::SeqCst), 2);

    runtime
        .submit(Submission::new("fails", "fails", "null"))
        .await
        .unwrap();
    let Some(Err(types::JobError::Terminal(error))) = runtime.await_result("fails").await else {
        panic!("expected a terminal error");
    };
    assert!(error.contains("nope"), "{error}");

    runtime
        .submit(Submission::new("panics", "panics", "null"))
        .await
        .unwrap();
    let Some(Err(types::JobError::Terminal(error))) = runtime.await_result("panics").await else {
        panic!("expected a terminal error");
    };
    assert!(error.contains("kaboom"), "{error}");
    assert_eq!(
        runtime.result("panics").await.unwrap(),
        Some(Err(types::JobError::Terminal(error)))
    );
    runtime.stop().await;
}

#[tokio::test]
async fn retry_budget_exhaustion_settles_with_a_classified_failure() {
    let store = MemoryStore::default();
    // No durable step and no boundary: every invocation fails transiently at
    // the top level, so the retry budget is the only thing that can settle
    // the job. A committed step would reset `retry_attempts` and defeat the
    // budget. Exhausting the forward budget enters cleanup, but a body that
    // never reaches a boundary cannot be interrupted there and has no defers
    // to run, so it runs again and exhausts the cleanup budget the same way.
    let runs = Arc::new(AtomicU64::new(0));
    let host = LocalNativeHost::new();
    {
        let runs = Arc::clone(&runs);
        host.register_typed("doomed", move |_cx, (): ()| {
            let runs = Arc::clone(&runs);
            Box::pin(async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(JobErrorX::Transient(ferr!("doomed")))
            })
        });
    }

    let (runtime, commits) = start(&store, &host).await;
    runtime
        .submit(
            Submission::new("job", "doomed", "null")
                .policy(RetryPolicy::Immediate)
                .budget(RetryBudget {
                    max_retries: Some(1),
                    max_restarts: None,
                }),
        )
        .await
        .unwrap();

    let Some(Err(types::JobError::Terminal(err))) = runtime.await_result("job").await else {
        panic!("expected the exhausted budget to settle terminally");
    };
    // The durable envelope classifies the failure and carries the last error.
    assert!(err.contains("RetriesExhausted"), "{err}");
    assert!(err.contains("doomed"), "{err}");
    // The initial run plus exactly one retry.
    // The initial run plus one retry per phase: forward, then cleanup.
    assert_eq!(runs.load(Ordering::SeqCst), 4);
    // Epoch bump, Submitted, Granted gen1, RetryScheduled, Granted gen2,
    // CleanupStarted, Granted gen3, RetryScheduled, Granted gen4, Completed.
    // The rejected cleanup retry commits nothing of its own.
    assert_eq!(*commits.borrow(), 10);
    let transitions = transitions(&runtime, "job").await;
    let retries = transitions
        .iter()
        .filter(|transition| matches!(transition, authority::Transition::RetryScheduled { .. }))
        .count();
    assert_eq!(retries, 2, "only the accepted retries are durable");
    let cleanup = transitions
        .iter()
        .find_map(|transition| match transition {
            authority::Transition::CleanupStarted { cleanup, .. } => Some(cleanup),
            _ => None,
        })
        .expect("the exhausted forward budget enters cleanup");
    assert!(
        matches!(
            cleanup.cause,
            CleanupCause::RetriesExhausted {
                retries: 1,
                max_retries: 1,
                ..
            }
        ),
        "{cleanup:?}"
    );
    runtime.stop().await;
}

#[tokio::test]
async fn restart_keeps_durable_retry_deadline() {
    let store = MemoryStore::default();
    // One durable effect step, then a top-level transient failure on the first
    // invocation only; the retry re-runs the workflow but replays the step.
    let invocations = Arc::new(AtomicU64::new(0));
    let effect_runs = Arc::new(AtomicU64::new(0));
    let host = LocalNativeHost::new();
    {
        let invocations = Arc::clone(&invocations);
        let effect_runs = Arc::clone(&effect_runs);
        host.register_typed("recovers", move |cx, (): ()| {
            let invocations = Arc::clone(&invocations);
            let effect_runs = Arc::clone(&effect_runs);
            Box::pin(async move {
                let attempt = invocations.fetch_add(1, Ordering::SeqCst);
                let first: u64 = cx
                    .effect(|| async {
                        effect_runs.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(1))
                    })
                    .await?;
                if attempt == 0 {
                    return Err(JobErrorX::Transient(ferr!("first attempt fails")));
                }
                Ok(first)
            })
        });
    }

    let started = Instant::now();
    let (runtime, mut commits) = start(&store, &host).await;
    runtime
        .submit(
            Submission::new("job", "recovers", "null").policy(RetryPolicy::Fixed { delay_ms: 600 }),
        )
        .await
        .unwrap();
    // Epoch bump, Submitted, Granted, StepCommitted, RetryScheduled. A fixed
    // delay keeps the retry from firing while the runtime is still up.
    committed(&mut commits, 5).await;
    // The job is parked on its durable retry, holding no lease to revoke.
    assert!(
        matches!(
            runtime.revoke("job").await,
            Err(RevokeError::RetryPending { .. })
        ),
        "a retry-parked job revokes as RetryPending"
    );
    runtime.stop().await;

    // The deadline is durable: the restart re-arms it from the journal rather
    // than recomputing it, so the job still waits out the original delay.
    let (runtime, commits) = start(&store, &host).await;
    assert_eq!(runtime.await_result("job").await, Some(ok(1)));
    assert!(started.elapsed() >= Duration::from_millis(600));
    // The effect ran once; the retry replayed its committed step.
    assert_eq!(effect_runs.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    // Epoch bump, Granted gen2, Completed: no second retry was scheduled.
    assert_eq!(*commits.borrow(), 3);
    let transitions = transitions(&runtime, "job").await;
    let retries = transitions
        .iter()
        .filter(|transition| matches!(transition, authority::Transition::RetryScheduled { .. }))
        .count();
    assert_eq!(
        retries, 1,
        "the durable retry survives the restart, not doubled"
    );
    runtime.stop().await;
}

#[tokio::test]
async fn sleep_parks_on_a_durable_timer_and_resumes_after_its_deadline() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let (runtime, commits) = start(&store, &host(&probe)).await;

    let started = Instant::now();
    runtime
        .submit(Submission::new("job", "sleeper", "50"))
        .await
        .unwrap();
    assert_eq!(runtime.await_result("job").await, Some(ok(3)));
    assert!(started.elapsed() >= Duration::from_millis(50));
    // Epoch bump, Submitted, Granted, first step, WaitStarted, WaitResolved,
    // last step, Completed.
    assert_eq!(*commits.borrow(), 8);
    assert_eq!(probe.sleeper_before_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.sleeper_after_runs.load(Ordering::SeqCst), 1);
    runtime.stop().await;
}

#[tokio::test]
async fn restart_keeps_durable_timer_deadlines() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());

    let (runtime, mut commits) = start(&store, &host(&probe)).await;
    let started = Instant::now();
    runtime
        .submit(Submission::new("pending", "sleeper", "400"))
        .await
        .unwrap();
    runtime
        .submit(Submission::new("overdue", "sleeper", "100"))
        .await
        .unwrap();
    // Epoch bump, then Submitted, Granted, first step and WaitStarted for
    // each job.
    committed(&mut commits, 9).await;
    runtime.stop().await;
    // `overdue` comes back with a passed deadline.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (runtime, commits) = start(&store, &host(&probe)).await;
    assert_eq!(runtime.await_result("overdue").await, Some(ok(3)));
    assert_eq!(runtime.await_result("pending").await, Some(ok(3)));
    assert!(started.elapsed() >= Duration::from_millis(400));
    // Epoch bump, then WaitResolved, re-Granted, last step and Completed for
    // each job: neither started a second wait.
    assert_eq!(*commits.borrow(), 9);
    for key in ["pending", "overdue"] {
        let transitions = transitions(&runtime, key).await;
        let waits = transitions
            .iter()
            .filter(|transition| matches!(transition, authority::Transition::WaitStarted { .. }))
            .count();
        assert_eq!(waits, 1, "{key}");
        assert!(
            transitions.iter().any(|transition| matches!(
                transition,
                authority::Transition::WaitResolved {
                    resolution: authority::Resolution::Timer,
                    ..
                }
            )),
            "{key}"
        );
        // The overdue timer fired before the re-grant, whose replay carried
        // the resolved step; the pending wait was re-attached to and fired
        // after it.
        let resolved = transitions
            .iter()
            .rposition(|t| matches!(t, authority::Transition::WaitResolved { .. }))
            .unwrap();
        let granted = transitions
            .iter()
            .rposition(|t| matches!(t, authority::Transition::Granted { .. }))
            .unwrap();
        assert_eq!(resolved < granted, key == "overdue", "{key}");
    }
    assert_eq!(probe.sleeper_before_runs.load(Ordering::SeqCst), 2);
    assert_eq!(probe.sleeper_after_runs.load(Ordering::SeqCst), 2);
    runtime.stop().await;
}

#[tokio::test]
async fn messages_queue_resolve_waits_and_deduplicate() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let (runtime, mut commits) = start(&store, &host(&probe)).await;

    assert_eq!(
        runtime.send_message("job", "m1", "1").await,
        Err(SendError::UnknownJob)
    );
    probe.inbox_effect_gate.close();
    runtime
        .submit(Submission::new("job", "inbox", "null"))
        .await
        .unwrap();
    probe.inbox_effect_gate.entered.notified().await;
    // The job is still in its first step, so the message queues.
    let first = runtime.send_message("job", "m1", "1").await.unwrap();
    assert_eq!(first.disposition, authority::Disposition::Admitted);
    assert_eq!(runtime.send_message("job", "m1", "1").await, Ok(first));
    assert_eq!(
        runtime.send_message("job", "m1", "9").await,
        Err(SendError::PayloadConflict)
    );
    probe.inbox_effect_gate.open();
    // Epoch bump, Submitted, Granted, MessageAccepted, first step, the first
    // receive consuming the queued message, WaitStarted for the second.
    committed(&mut commits, 7).await;
    // An accepted message keeps its receipt after being consumed from the
    // inbox while the job is still active.
    assert_eq!(runtime.send_message("job", "m1", "1").await, Ok(first));
    runtime.send_message("job", "m2", "2").await.unwrap();
    assert_eq!(runtime.await_result("job").await, Some(ok(12)));
    assert_eq!(*commits.borrow(), 9);
    assert_eq!(
        runtime.send_message("job", "m3", "3").await,
        Err(SendError::AlreadyTerminal)
    );
    // Once the job settles into a compact terminal receipt, message history is
    // discarded and resends are rejected as terminal.
    assert_eq!(
        runtime.send_message("job", "m1", "1").await,
        Err(SendError::AlreadyTerminal)
    );

    let resolutions: Vec<_> = transitions(&runtime, "job")
        .await
        .into_iter()
        .filter_map(|transition| match transition {
            authority::Transition::WaitResolved { resolution, .. } => Some(resolution),
            _ => None,
        })
        .collect();
    let [
        authority::Resolution::Queued,
        authority::Resolution::Delivered(delivered),
    ] = resolutions.as_slice()
    else {
        panic!("unexpected resolutions: {resolutions:?}");
    };
    assert_eq!(delivered.id, "m2");
    runtime.stop().await;
}

#[tokio::test]
async fn receive_survives_restart_and_a_message_racing_the_replay() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());

    let (runtime, mut commits) = start(&store, &host(&probe)).await;
    runtime
        .submit(Submission::new("job", "inbox", "null"))
        .await
        .unwrap();
    // Epoch bump, Submitted, Granted, first step, WaitStarted.
    committed(&mut commits, 5).await;
    runtime.send_message("job", "m1", "4").await.unwrap();
    // WaitResolved for the first receive, WaitStarted for the second.
    committed(&mut commits, 7).await;
    runtime.stop().await;

    probe.inbox_replay_gate.close();
    let (runtime, commits) = start(&store, &host(&probe)).await;
    // The re-granted invocation is replaying and has not re-attached to the
    // durable wait yet when the message resolves it.
    probe.inbox_replay_gate.entered.notified().await;
    runtime.send_message("job", "m2", "5").await.unwrap();
    probe.inbox_replay_gate.open();
    assert_eq!(runtime.await_result("job").await, Some(ok(45)));
    // Epoch bump, re-Granted, WaitResolved, Completed.
    assert_eq!(*commits.borrow(), 4);
    runtime.stop().await;
}

#[tokio::test]
async fn revoke_recovers_in_flight_job_and_fences_stale_lease_from_continuing() {
    struct NotifyOnDrop(Arc<Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let host = host(&probe);

    let step0_runs = Arc::new(AtomicU64::new(0));
    let step1_runs = Arc::new(AtomicU64::new(0));
    let step2_runs = Arc::new(AtomicU64::new(0));
    let stale_continued_runs = Arc::new(AtomicU64::new(0));
    let stale_gate = Arc::new(Gate::default());
    let fresh_gate = Arc::new(Gate::default());
    let stale_fenced = Arc::new(Notify::new());
    stale_gate.close();
    fresh_gate.close();

    {
        let step0_runs = Arc::clone(&step0_runs);
        let step1_runs = Arc::clone(&step1_runs);
        let step2_runs = Arc::clone(&step2_runs);
        let stale_continued_runs = Arc::clone(&stale_continued_runs);
        let stale_gate = Arc::clone(&stale_gate);
        let fresh_gate = Arc::clone(&fresh_gate);
        let stale_fenced = Arc::clone(&stale_fenced);
        host.register_typed("revocable", move |cx, (): ()| {
            let step0_runs = Arc::clone(&step0_runs);
            let step1_runs = Arc::clone(&step1_runs);
            let step2_runs = Arc::clone(&step2_runs);
            let stale_continued_runs = Arc::clone(&stale_continued_runs);
            let stale_gate = Arc::clone(&stale_gate);
            let fresh_gate = Arc::clone(&fresh_gate);
            let stale_fenced = Arc::clone(&stale_fenced);
            Box::pin(async move {
                let first: u64 = cx
                    .effect(|| async move {
                        step0_runs.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(1))
                    })
                    .await?;
                let attempt = Arc::new(AtomicU64::new(0));
                let attempt_in_step = Arc::clone(&attempt);
                let fenced_guard = std::rc::Rc::new(std::cell::RefCell::new(None));
                let fenced_in_step = std::rc::Rc::clone(&fenced_guard);
                let second: u64 = cx
                    .effect(|| async move {
                        let run = step1_runs.fetch_add(1, Ordering::SeqCst);
                        attempt_in_step.store(run, Ordering::SeqCst);
                        if run == 0 {
                            *fenced_in_step.borrow_mut() =
                                Some(NotifyOnDrop(Arc::clone(&stale_fenced)));
                            stale_gate.pass().await;
                            Ok(Json(10))
                        } else {
                            fresh_gate.pass().await;
                            Ok(Json(20))
                        }
                    })
                    .await?;
                let _keep_until_done = fenced_guard;
                let third: u64 = cx
                    .effect(|| async move {
                        if attempt.load(Ordering::SeqCst) == 0 {
                            stale_continued_runs.fetch_add(1, Ordering::SeqCst);
                        }
                        step2_runs.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(300))
                    })
                    .await?;
                Ok(first + second + third)
            })
        });
    }

    let (runtime, mut commits) = start(&store, &host).await;
    assert_eq!(
        runtime.revoke("missing").await,
        Err(RevokeError::UnknownJob)
    );

    runtime
        .submit(Submission::new("job", "revocable", "null"))
        .await
        .unwrap();
    stale_gate.entered.notified().await;

    // Revoke lease 1 while its step 1 effect is still in flight; the runtime
    // immediately grants lease 2, which replays step 0 without re-running it.
    let ack = runtime.revoke("job").await.unwrap();
    assert_eq!(ack.disposition, authority::Disposition::Park);
    fresh_gate.entered.notified().await;
    assert_eq!(step0_runs.load(Ordering::SeqCst), 1);

    // Let the revoked lease finish step 1 first: the authority rejects its
    // `CommitStep` with `StaleLease` and drops the session without resuming it.
    let fenced = stale_fenced.notified();
    tokio::pin!(fenced);
    fenced.as_mut().enable();
    stale_gate.open();
    fenced.await;
    fresh_gate.open();

    assert_eq!(runtime.await_result("job").await, Some(ok(321)));
    assert_eq!(step0_runs.load(Ordering::SeqCst), 1);
    assert_eq!(step1_runs.load(Ordering::SeqCst), 2);
    assert_eq!(step2_runs.load(Ordering::SeqCst), 1);
    assert_eq!(stale_continued_runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        runtime.revoke("job").await,
        Err(RevokeError::AlreadyTerminal)
    );

    // Also verify revoking a job parked on a durable wait removes the stale
    // waiting session and lets the re-granted session re-attach and complete.
    let before_waiter = *commits.borrow();
    runtime
        .submit(Submission::new("waiter", "inbox", "null"))
        .await
        .unwrap();
    // Submitted, Granted, step 0 Effect, WaitStarted.
    committed(&mut commits, before_waiter + 4).await;
    let waiter_ack = runtime.revoke("waiter").await.unwrap();
    assert_eq!(waiter_ack.disposition, authority::Disposition::Park);
    runtime.send_message("waiter", "m1", "7").await.unwrap();
    runtime.send_message("waiter", "m2", "8").await.unwrap();
    assert_eq!(runtime.await_result("waiter").await, Some(ok(78)));
    runtime.stop().await;
}

#[tokio::test]
async fn nondeterministic_replay_across_restart_and_wait_resolution_fails_terminally() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());

    // 1. Commit step 0 as an Effect, then stop while step 1 is in flight.
    let (runtime, _) = start(&store, &host(&probe)).await;
    runtime
        .submit(Submission::new("cold_kind", "gated", "null"))
        .await
        .unwrap();
    probe.gate_entered.notified().await;
    runtime.stop().await;

    // Restart with a fresh host where `"gated"` instead sleeps at step 0:
    // durable `ReplayStep.kind` detects the Effect -> Sleep mismatch.
    let mutated_host = LocalNativeHost::new();
    mutated_host.register_typed("gated", |cx, (): ()| {
        Box::pin(async move {
            cx.sleep(Duration::from_millis(10)).await?;
            Ok::<u64, _>(0)
        })
    });
    mutated_host.register_typed("race_kind", |cx, (): ()| {
        Box::pin(async move {
            cx.effect(|| async { Ok(Json(1u64)) }).await?;
            let Json(msg): Json<u64> = cx.recv().await?;
            Ok(msg)
        })
    });

    let (runtime, mut commits) = start(&store, &mutated_host).await;
    let Some(Err(types::JobError::Terminal(err))) = runtime.await_result("cold_kind").await else {
        panic!("expected terminal nondeterministic replay error for cold_kind");
    };
    assert!(err.contains("nondeterministic"), "{err}");

    // 2. Start `"race_kind"` so step 0 commits as Effect and step 1 parks on a
    // message wait, then restart with a mutated `"race_kind"` that pauses
    // before step 1 and runs `effect` instead of `recv` after a message
    // resolves the wait in the authority.
    let before_race = *commits.borrow();
    runtime
        .submit(Submission::new("race", "race_kind", "null"))
        .await
        .unwrap();
    committed(&mut commits, before_race + 4).await;
    runtime.stop().await;

    let replay_gate = Arc::new(Gate::default());
    replay_gate.close();
    let race_host = LocalNativeHost::new();
    race_host.register_typed("gated", |_cx, (): ()| Box::pin(async { Ok::<u64, _>(0) }));
    {
        let replay_gate = Arc::clone(&replay_gate);
        race_host.register_typed("race_kind", move |cx, (): ()| {
            let replay_gate = Arc::clone(&replay_gate);
            Box::pin(async move {
                cx.effect(|| async { Ok(Json(1u64)) }).await?;
                replay_gate.pass().await;
                let v: u64 = cx.effect(|| async { Ok(Json(99u64)) }).await?;
                Ok(v)
            })
        });
    }

    let (runtime, _) = start(&store, &race_host).await;
    replay_gate.entered.notified().await;
    // Resolve step 1 as a Message in the authority while the session is still
    // replaying with its 1-step snapshot.
    runtime.send_message("race", "m1", "42").await.unwrap();
    replay_gate.open();

    let Some(Err(types::JobError::Terminal(err))) = runtime.await_result("race").await else {
        panic!("expected terminal nondeterministic error for race");
    };
    // The message resolved step 1 as a durable Recv after the session took its
    // 1-step replay snapshot, so the session cannot catch the fork itself; the
    // authority rejects the replayed Effect commit and the runtime settles it
    // as the explicit `ReplayDiverged` class rather than a generic string.
    assert!(err.contains("ReplayDiverged"), "{err}");
    assert!(err.contains("durably Recv"), "{err}");
    runtime.stop().await;
}

#[tokio::test]
async fn job_handle_retention_expiry_key_reuse_and_compaction() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let (runtime, mut commits) = start(&store, &host(&probe)).await;

    // 1. JobHandle race-free await_result (both while active and after settlement)
    // and receipt inspection.
    probe.inbox_effect_gate.close();
    let (first_ack, gated_handle) = runtime
        .submit_handle(Submission::new("reusable", "inbox", "null").retention_ms(60_000))
        .await
        .unwrap();
    assert_eq!(first_ack.disposition, authority::Disposition::Admitted);
    assert_eq!(gated_handle.key(), "reusable");
    assert_eq!(gated_handle.get_result().await.unwrap(), None);
    assert_eq!(runtime.expire("reusable").await, Err(ExpireError::Active));

    probe.inbox_effect_gate.entered.notified().await;
    runtime.send_message("reusable", "m1", "1").await.unwrap();
    runtime.send_message("reusable", "m2", "2").await.unwrap();
    let waiter = {
        let handle = gated_handle.clone();
        tokio::spawn(async move { handle.await_result().await })
    };
    probe.inbox_effect_gate.open();
    assert_eq!(waiter.await.unwrap(), Some(ok(12)));
    // Calling await_result after settlement reads the durable receipt directly.
    assert_eq!(gated_handle.await_result().await, Some(ok(12)));
    assert_eq!(gated_handle.get_result().await.unwrap(), Some(ok(12)));
    let receipt = gated_handle
        .receipt()
        .await
        .unwrap()
        .expect("durable receipt");
    assert_eq!(receipt.incarnation, 1);
    assert_eq!(receipt.steps, 3);
    assert_eq!(receipt.retention_ms, 60_000);

    // Early expire before the retention deadline is rejected with RetentionPending.
    assert_eq!(
        runtime.expire("reusable").await,
        Err(ExpireError::RetentionPending {
            expires_at_ms: receipt.outcome.expires_at_ms(),
        })
    );

    // 2. Zero-retention job expires via the deadline wheel after await_result
    // reads its durable receipt, and the key can then be reused with a fresh
    // job incarnation.
    probe.inbox_effect_gate.close();
    let (_, short_handle) = runtime
        .submit_handle(Submission::new("ephemeral", "inbox", "null").retention_ms(0))
        .await
        .unwrap();
    probe.inbox_effect_gate.entered.notified().await;
    runtime.send_message("ephemeral", "m1", "3").await.unwrap();
    runtime.send_message("ephemeral", "m2", "0").await.unwrap();
    let short_waiter = {
        let handle = short_handle.clone();
        tokio::spawn(async move { handle.await_result().await })
    };
    probe.inbox_effect_gate.open();
    assert_eq!(short_waiter.await.unwrap(), Some(ok(30)));
    while runtime.receipt("ephemeral").await.unwrap().is_some() {
        commits.changed().await.unwrap();
    }
    assert_eq!(runtime.receipt("ephemeral").await.unwrap(), None);
    assert_eq!(runtime.get_result("ephemeral").await.unwrap(), None);
    assert_eq!(
        runtime.expire("ephemeral").await,
        Err(ExpireError::UnknownJob)
    );

    // Re-submitting the expired key with a different payload is admitted under
    // a bumped incarnation (incarnation 3, since reusable=1, ephemeral=2).
    let (_, reused_handle) = runtime
        .submit_handle(Submission::new("ephemeral", "steps", "3").retention_ms(60_000))
        .await
        .unwrap();
    assert_eq!(reused_handle.await_result().await, Some(ok(6)));
    let reused_receipt = reused_handle
        .receipt()
        .await
        .unwrap()
        .expect("reused receipt");
    assert_eq!(reused_receipt.incarnation, 3);

    // 3. Compact publishes an active-only snapshot + compact receipts and
    // reclaims log entries through the cut; recovery from the compacted store
    // preserves retained receipts and resumes active jobs.
    probe.inbox_effect_gate.close();
    runtime
        .submit(Submission::new("active_across_compact", "inbox", "null"))
        .await
        .unwrap();
    probe.inbox_effect_gate.entered.notified().await;
    runtime
        .send_message("active_across_compact", "m1", "2")
        .await
        .unwrap();
    runtime
        .send_message("active_across_compact", "m2", "1")
        .await
        .unwrap();

    let cut = runtime.compact().await.unwrap();
    assert!(cut > 0);
    // Log entries through `cut` are reclaimed; reading from 1 is rejected.
    assert!(
        runtime
            .reader
            .read(1, NonZeroUsize::new(1).unwrap())
            .await
            .is_err()
    );
    let published = runtime
        .reader
        .snapshot()
        .await
        .unwrap()
        .expect("published snapshot");
    assert_eq!(published.cut, cut);
    assert_eq!(published.compacted_through, cut);
    let snap: authority::Snapshot = serde_json::from_slice(&published.value).unwrap();
    assert!(snap.jobs.contains_key("active_across_compact"));
    assert!(!snap.jobs.contains_key("reusable"));
    assert!(snap.receipts.contains_key("reusable"));
    assert!(snap.receipts.contains_key("ephemeral"));

    runtime.stop().await;

    probe.inbox_effect_gate.open();
    let (runtime, _) = start(&store, &host(&probe)).await;
    assert_eq!(
        runtime.await_result("active_across_compact").await,
        Some(ok(21))
    );
    assert_eq!(runtime.get_result("reusable").await.unwrap(), Some(ok(12)));
    assert_eq!(runtime.get_result("ephemeral").await.unwrap(), Some(ok(6)));
    runtime.stop().await;
}
