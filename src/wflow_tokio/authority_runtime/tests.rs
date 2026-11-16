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
use wflow_core::kvstore::journal::JournalSnapshot;
use wflow_core::kvstore::journal::StaleReaderError;
use wflow_core::partition::MAX_FAULT_TEXT_BYTES;
use wflow_wdk::JobErrorX;
use wflow_wdk::Json;
use wflow_wdk::StepKind;

use super::*;
use crate::local_native_host::LocalNativeHost;

mod lifecycle;

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

/// Returns the durable transition frontier, excluding metadata-only maintenance commits.
async fn journal_frontier(runtime: &AuthorityRuntime) -> u64 {
    if let Some(snapshot) = runtime.reader.snapshot().await.unwrap() {
        return snapshot.frontier;
    }
    runtime
        .reader
        .read(1, RECOVERY_PAGE)
        .await
        .unwrap()
        .frontier
}

/// Waits on the counting writer rather than sleeping while maintenance publishes.
async fn published_snapshot(
    runtime: &AuthorityRuntime,
    commits: &mut watch::Receiver<u64>,
) -> JournalSnapshot {
    loop {
        if let Some(snapshot) = runtime.reader.snapshot().await.unwrap() {
            return snapshot;
        }
        commits.changed().await.unwrap();
    }
}

async fn fully_reclaimed(
    runtime: &AuthorityRuntime,
    commits: &mut watch::Receiver<u64>,
) -> JournalSnapshot {
    loop {
        if let Some(snapshot) = runtime.reader.snapshot().await.unwrap()
            && snapshot.compacted_through == snapshot.cut
        {
            return snapshot;
        }
        commits.changed().await.unwrap();
    }
}

/// The job's durable transitions, in commit order.
async fn transitions(runtime: &AuthorityRuntime, key: &str) -> Vec<authority::Transition> {
    transitions_from(runtime, key, 1).await
}

async fn transitions_from(
    runtime: &AuthorityRuntime,
    key: &str,
    from: u64,
) -> Vec<authority::Transition> {
    let frontier = journal_frontier(runtime).await;
    read_transitions(&runtime.reader, from, frontier)
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
        | T::MessageAccepted { key, .. } => key,
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

    let (runtime, _) = start(&store, &host).await;
    let ack = runtime
        .submit(Submission::new("job", "steps", "3"))
        .await
        .unwrap();
    assert_eq!(ack.disposition, authority::Disposition::Admitted);
    assert_eq!(runtime.await_result("job").await, Some(ok(6)));
    // The frontier counts durable authority transitions, not metadata maintenance commits.
    assert_eq!(journal_frontier(&runtime).await, 6);
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
    let frontier_before_restart = journal_frontier(&runtime).await;
    runtime.stop().await;

    let (runtime, _) = start(&store, &host).await;
    assert_eq!(runtime.result("job").await.unwrap(), Some(ok(6)));
    let resubmitted = runtime
        .submit(Submission::new("job", "steps", "3"))
        .await
        .unwrap();
    assert_eq!(resubmitted.position, ack.position);
    assert_eq!(resubmitted.disposition, authority::Disposition::Completed);
    assert_eq!(runtime.await_result("job").await, Some(ok(6)));
    // Reopening advances metadata only; a terminal job adds no transitions.
    assert_eq!(journal_frontier(&runtime).await, frontier_before_restart);
    assert_eq!(probe.steps_runs.load(Ordering::SeqCst), 3);
    runtime.stop().await;
}

#[tokio::test]
async fn restart_replays_durable_steps_and_reruns_only_the_incomplete_one() {
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
    let frontier_before_restart = journal_frontier(&runtime).await;
    runtime.stop().await;

    probe.gate_open.store(true, Ordering::SeqCst);
    let (runtime, _) = start(&store, &host(&probe)).await;
    assert_eq!(runtime.await_result("job").await, Some(ok(11)));
    // Re-Granted, second step, Completed; the epoch bump is metadata-only.
    assert_eq!(
        journal_frontier(&runtime).await - frontier_before_restart,
        3
    );
    assert_eq!(probe.gated_first_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.gated_second_runs.load(Ordering::SeqCst), 2);
    runtime.stop().await;
}

#[tokio::test]
async fn failures_retry_transients_and_commit_terminal_errors() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let (runtime, _) = start(&store, &host(&probe)).await;

    runtime
        .submit(Submission::new("flaky", "flaky", "null"))
        .await
        .unwrap();
    assert_eq!(runtime.await_result("flaky").await, Some(ok(3)));
    // Seven durable transitions; the epoch bump is metadata-only.
    assert_eq!(journal_frontier(&runtime).await, 7);
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

/// A transient error is guest-supplied prose that outlives the job in its
/// receipt, so the exhausted-retry fault keeps only the head of it.
#[tokio::test]
async fn exhausted_retry_error_is_bounded_in_the_durable_fault() {
    let store = MemoryStore::default();
    let host = LocalNativeHost::new();
    host.register_typed("noisy", |_cx, (): ()| {
        Box::pin(async {
            Err::<u64, _>(JobErrorX::Transient(ferr!(
                "{}",
                "e".repeat(4 * MAX_FAULT_TEXT_BYTES)
            )))
        })
    });
    let (runtime, _) = start(&store, &host).await;

    // A zero retry budget turns the first transient failure into the exhausted
    // rejection, so the fault is settled from the very error the guest reported.
    runtime
        .submit(
            Submission::new("noisy", "noisy", "null").budget(RetryBudget {
                max_retries: Some(0),
                max_restarts: None,
            }),
        )
        .await
        .unwrap();
    let receipt = runtime.handle("noisy").await_receipt().await.unwrap();
    let JobFailure::RetriesExhausted {
        retries,
        max_retries,
        last_error,
    } = fault_of(&receipt)
    else {
        panic!("expected exhausted retries, got {:?}", fault_of(&receipt));
    };
    assert_eq!((*retries, *max_retries), (0, 0));
    // The stored error is the guest's own JSON envelope; only its tail is gone.
    assert_eq!(last_error.len(), MAX_FAULT_TEXT_BYTES);
    assert!(last_error.starts_with(r#"{"msg":"#), "{last_error}");
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
    let frontier_before_restart = journal_frontier(&runtime).await;
    let initial_transitions = transitions(&runtime, "job").await;
    let retries = initial_transitions
        .iter()
        .filter(|transition| matches!(transition, authority::Transition::RetryScheduled { .. }))
        .count();
    assert_eq!(retries, 1, "the durable retry is scheduled once");
    runtime.stop().await;

    // The deadline is durable: the restart re-arms it from the journal rather
    // than recomputing it, so the job still waits out the original delay.
    let (runtime, _) = start(&store, &host).await;
    assert_eq!(runtime.await_result("job").await, Some(ok(1)));
    assert!(started.elapsed() >= Duration::from_millis(600));
    // The effect ran once; the retry replayed its committed step.
    assert_eq!(effect_runs.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    // Granted gen2 and Completed; the epoch bump is metadata-only.
    assert_eq!(
        journal_frontier(&runtime).await - frontier_before_restart,
        2
    );
    // The frontier delta and single invocation above prove the retry was not duplicated.
    runtime.stop().await;
}

#[tokio::test]
async fn sleep_parks_on_a_durable_timer_and_resumes_after_its_deadline() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());
    let (runtime, _) = start(&store, &host(&probe)).await;

    let started = Instant::now();
    runtime
        .submit(Submission::new("job", "sleeper", "50"))
        .await
        .unwrap();
    assert_eq!(runtime.await_result("job").await, Some(ok(3)));
    assert!(started.elapsed() >= Duration::from_millis(50));
    // Seven durable transitions; the epoch bump is metadata-only.
    assert_eq!(journal_frontier(&runtime).await, 7);
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
    let frontier_before_restart = journal_frontier(&runtime).await;
    let initial_pending = transitions(&runtime, "pending").await;
    let initial_overdue = transitions(&runtime, "overdue").await;
    runtime.stop().await;
    // `overdue` comes back with a passed deadline.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (runtime, _) = start(&store, &host(&probe)).await;
    assert_eq!(runtime.await_result("overdue").await, Some(ok(3)));
    assert_eq!(runtime.await_result("pending").await, Some(ok(3)));
    assert!(started.elapsed() >= Duration::from_millis(400));
    // Each job adds WaitResolved, re-Granted, last step and Completed. The
    // restart epoch and maintenance commits do not advance the frontier.
    assert_eq!(
        journal_frontier(&runtime).await - frontier_before_restart,
        8
    );
    for key in ["pending", "overdue"] {
        let mut transitions = match key {
            "pending" => initial_pending.clone(),
            "overdue" => initial_overdue.clone(),
            _ => unreachable!(),
        };
        transitions.extend(transitions_from(&runtime, key, frontier_before_restart + 1).await);
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
    // Wait for the durable transition prefix before racing the next message.
    committed(&mut commits, 7).await;
    // An accepted message keeps its receipt after being consumed from the
    // inbox while the job is still active.
    assert_eq!(runtime.send_message("job", "m1", "1").await, Ok(first));
    runtime.send_message("job", "m2", "2").await.unwrap();
    assert_eq!(runtime.await_result("job").await, Some(ok(12)));
    // Eight durable transitions; metadata maintenance is not part of this count.
    assert_eq!(journal_frontier(&runtime).await, 8);
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
    // Wait for the durable transition prefix before delivering the first message.
    committed(&mut commits, 5).await;
    runtime.send_message("job", "m1", "4").await.unwrap();
    // WaitResolved for the first receive, WaitStarted for the second.
    committed(&mut commits, 7).await;
    let frontier_before_restart = journal_frontier(&runtime).await;
    runtime.stop().await;

    probe.inbox_replay_gate.close();
    let (runtime, _) = start(&store, &host(&probe)).await;
    // The re-granted invocation is replaying and has not re-attached to the
    // durable wait yet when the message resolves it.
    probe.inbox_replay_gate.entered.notified().await;
    runtime.send_message("job", "m2", "5").await.unwrap();
    probe.inbox_replay_gate.open();
    assert_eq!(runtime.await_result("job").await, Some(ok(45)));
    // Re-Granted, WaitResolved, Completed; the epoch bump is metadata-only.
    assert_eq!(
        journal_frontier(&runtime).await - frontier_before_restart,
        3
    );
    runtime.stop().await;
}

/// The classified failure a settled receipt carries.
fn fault_of(receipt: &authority::Receipt) -> &JobFailure {
    match &receipt.outcome {
        authority::Outcome::Faulted(fault) => &fault.failure,
        authority::Outcome::Completed(terminal) => {
            panic!("expected a fault, committed {:?}", terminal.value)
        }
    }
}

#[tokio::test]
async fn adapter_detected_replay_divergence_settles_as_a_fault() {
    let store = MemoryStore::default();
    let probe = Arc::new(Probe::default());

    // Commit step 0 as an Effect, then stop while step 1 is in flight.
    let (runtime, _) = start(&store, &host(&probe)).await;
    runtime
        .submit(Submission::new("cold_kind", "gated", "null"))
        .await
        .unwrap();
    probe.gate_entered.notified().await;
    runtime.stop().await;

    // Restart with a fresh host where `"gated"` instead sleeps at step 0. The
    // replay carries the durable kind, so the adapter notices the Effect ->
    // Sleep fork before the authority is asked to commit anything, and the job
    // must still settle as the engine's divergence class.
    let mutated_host = LocalNativeHost::new();
    mutated_host.register_typed("gated", |cx, (): ()| {
        Box::pin(async move {
            cx.sleep(Duration::from_millis(10)).await?;
            Ok::<u64, _>(0)
        })
    });

    let (runtime, _) = start(&store, &mutated_host).await;
    let receipt = runtime.handle("cold_kind").await_receipt().await.unwrap();
    assert_eq!(
        fault_of(&receipt),
        &JobFailure::ReplayDiverged {
            step: 0,
            replayed: StepKind::Sleep,
        }
    );
    runtime.stop().await;
}

/// Registers the workflow the parity test re-registers in a mutated form: an
/// effect, then two durable receives.
fn fork_host() -> LocalNativeHost {
    let host = LocalNativeHost::new();
    host.register_typed("fork", |cx, (): ()| {
        Box::pin(async move {
            let first: u64 = cx.effect(|| async { Ok(Json(1_u64)) }).await?;
            let Json(second): Json<u64> = cx.recv().await?;
            let Json(third): Json<u64> = cx.recv().await?;
            Ok::<_, JobErrorX>(first + second + third)
        })
    });
    host
}

/// The mutated `"fork"`: it offers an effect at step 1, which is durably a
/// receive, and parks on the gate after the first effect so the caller can
/// sequence a durable change against the replay.
fn diverging_fork_host(gate: Arc<Gate>) -> LocalNativeHost {
    let host = LocalNativeHost::new();
    host.register_typed("fork", move |cx, (): ()| {
        let gate = Arc::clone(&gate);
        Box::pin(async move {
            cx.effect(|| async { Ok(Json(1_u64)) }).await?;
            gate.pass().await;
            let diverge: u64 = cx.effect(|| async { Ok(Json(99_u64)) }).await?;
            let Json(third): Json<u64> = cx.recv().await?;
            Ok::<_, JobErrorX>(diverge + third)
        })
    });
    host
}

#[tokio::test]
async fn adapter_and_authority_divergence_settle_into_the_same_receipt() {
    // The same fork on both sides: durable step 1 is a receive, and the
    // re-registered workflow offers an effect there. The durable outcome must not
    // depend on which side notices first.

    // A: step 1 is already durable when the runtime restarts, so the replay pins
    // it and the adapter notices the fork inside `next_step`.
    let store = MemoryStore::default();
    let (runtime, mut commits) = start(&store, &fork_host()).await;
    runtime
        .submit(Submission::new("fork", "fork", "null"))
        .await
        .unwrap();
    // Epoch bump, Submitted, Granted, step 0, and the receive at step 1.
    committed(&mut commits, 5).await;
    // Deliver step 1's message in this run, so the data job parks on its second
    // receive with two durable steps.
    runtime.send_message("fork", "m1", "10").await.unwrap();
    committed(&mut commits, 7).await;
    runtime.stop().await;

    // An open gate: case A needs no sequencing, the durable log already agrees
    // with itself.
    let (runtime, _) = start(&store, &diverging_fork_host(Arc::new(Gate::default()))).await;
    let adapter_receipt = runtime.handle("fork").await_receipt().await.unwrap();
    assert_eq!(
        fault_of(&adapter_receipt),
        &JobFailure::ReplayDiverged {
            step: 1,
            replayed: StepKind::Effect,
        }
    );
    runtime.stop().await;

    // B: the same fork, but the durable receive at step 1 only appears after the
    // re-granted session took its replay snapshot, so the authority is the side
    // that rejects it.
    let store = MemoryStore::default();
    let (runtime, mut commits) = start(&store, &fork_host()).await;
    runtime
        .submit(Submission::new("fork", "fork", "null"))
        .await
        .unwrap();
    committed(&mut commits, 5).await;
    runtime.stop().await;

    let replay_gate = Arc::new(Gate::default());
    replay_gate.close();
    let (runtime, _) = start(&store, &diverging_fork_host(Arc::clone(&replay_gate))).await;
    replay_gate.entered.notified().await;
    // Resolve step 1 as a message while the session is paused with its 1-step
    // replay snapshot, so only the authority can catch the fork.
    runtime.send_message("fork", "m1", "10").await.unwrap();
    replay_gate.open();
    let authority_receipt = runtime.handle("fork").await_receipt().await.unwrap();
    runtime.stop().await;

    // Everything a caller or a later snapshot reads agrees; only the settlement
    // position differs, because the two runs reached the fork after a different
    // number of durable transitions (a delivery, or a delivery plus a wait).
    assert_eq!(authority_receipt.steps, adapter_receipt.steps);
    assert_eq!(authority_receipt.payload, adapter_receipt.payload);
    assert_eq!(authority_receipt.incarnation, adapter_receipt.incarnation);
    assert_eq!(
        authority_receipt.submission_position,
        adapter_receipt.submission_position
    );
    assert_eq!(authority_receipt.policy, adapter_receipt.policy);
    assert_eq!(authority_receipt.budget, adapter_receipt.budget);
    assert_eq!(
        fault_of(&authority_receipt),
        fault_of(&adapter_receipt),
        "the same fork must settle as the same fault whichever side noticed it"
    );
}

#[tokio::test]
async fn short_completion_faults_instead_of_claiming_unexecuted_durable_steps() {
    let store = MemoryStore::default();
    let gate = Arc::new(Gate::default());
    gate.close();
    let deployed = LocalNativeHost::new();
    {
        let gate = Arc::clone(&gate);
        deployed.register_typed("deploy", move |cx, (): ()| {
            let gate = Arc::clone(&gate);
            Box::pin(async move {
                let first: u64 = cx
                    .named_effect("first", || async { Ok(Json(1_u64)) })
                    .await?;
                let second: u64 = cx
                    .named_effect("second", || async { Ok(Json(2_u64)) })
                    .await?;
                let third: u64 = cx
                    .named_effect("third", || async { Ok(Json(3_u64)) })
                    .await?;
                // Outside any step, so stopping here leaves three committed
                // steps and a run that has not returned.
                gate.pass().await;
                Ok::<_, JobErrorX>(first + second + third)
            })
        });
    }
    let (runtime, _) = start(&store, &deployed).await;
    runtime
        .submit(Submission::new("job", "deploy", "null"))
        .await
        .unwrap();
    gate.entered.notified().await;
    runtime.stop().await;

    // The deployed workflow now performs one effect and returns, while the
    // durable log still holds three steps.
    let shorter = LocalNativeHost::new();
    shorter.register_typed("deploy", |cx, (): ()| {
        Box::pin(async move {
            let first: u64 = cx
                .named_effect("first", || async { Ok(Json(1_u64)) })
                .await?;
            Ok::<_, JobErrorX>(first)
        })
    });
    let (runtime, _) = start(&store, &shorter).await;
    let receipt = runtime.handle("job").await_receipt().await.unwrap();
    assert_eq!(
        fault_of(&receipt),
        &JobFailure::ReplayDiverged {
            step: 1,
            replayed: StepKind::Effect,
        }
    );
    // The three durable steps survive; only the completion claim was wrong.
    assert_eq!(receipt.steps, 3);
    runtime.stop().await;
}
