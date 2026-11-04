//! Cancel/cleanup protocol: cooperative interruption at forward boundaries,
//! durable cleanup cause, defers as `Defer` steps, and the cleanup outcomes.

use std::sync::Mutex;

use super::*;

/// One saga: acquire (defer its release), optionally sleep, a gated middle
/// effect (defer its release), then finish or fail as the args say.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SagaArgs {
    sleep_ms: Option<u64>,
    end: End,
    release_a: Release,
    release_b: Release,
}

impl Default for SagaArgs {
    fn default() -> Self {
        Self {
            sleep_ms: None,
            end: End::Ok,
            release_a: Release::Ok,
            release_b: Release::Ok,
        }
    }
}

/// How forward work ends once the middle effect committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum End {
    Ok,
    Terminal,
    /// The `finish` effect's closure fails terminally, leaving its step
    /// active but never persisted when cleanup begins.
    TerminalInEffect,
    /// A top-level transient failure on the first run only.
    TransientOnce,
    /// The `finish` effect's closure fails transiently every time it runs.
    TransientInEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Release {
    Ok,
    /// Fails transiently the first time it runs.
    TransientOnce,
    Terminal,
}

#[derive(Default)]
struct SagaProbe {
    acquire_runs: AtomicU64,
    middle_runs: AtomicU64,
    finish_runs: AtomicU64,
    /// Defer actions that ran, in order.
    releases: Mutex<Vec<&'static str>>,
    /// Holds the middle effect while it runs.
    middle_gate: Gate,
    /// Holds `release-a` while it runs.
    release_gate: Gate,
    transient_end_done: AtomicBool,
    release_a_failed: AtomicBool,
}

impl SagaProbe {
    fn releases(&self) -> Vec<&'static str> {
        self.releases.lock().unwrap().clone()
    }
}

fn saga_host(probe: &Arc<SagaProbe>) -> LocalNativeHost {
    let host = LocalNativeHost::new();
    let p = Arc::clone(probe);
    host.register_typed("saga", move |cx, args: SagaArgs| {
        let p = Arc::clone(&p);
        Box::pin(async move {
            let pa = Arc::clone(&p);
            let a: u64 = cx
                .named_effect("acquire", || async move {
                    pa.acquire_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(Json(1))
                })
                .await?;
            let pa = Arc::clone(&p);
            cx.defer("release-a", move || async move {
                pa.releases.lock().unwrap().push("release-a");
                pa.release_gate.pass().await;
                match args.release_a {
                    Release::Ok => Ok(()),
                    Release::TransientOnce if !pa.release_a_failed.swap(true, Ordering::SeqCst) => {
                        Err(JobErrorX::Transient(ferr!("release-a flaked")))
                    }
                    Release::TransientOnce => Ok(()),
                    Release::Terminal => Err(JobErrorX::Terminal(ferr!("release-a broke"))),
                }
            })?;
            if let Some(sleep_ms) = args.sleep_ms {
                cx.sleep(Duration::from_millis(sleep_ms)).await?;
            }
            let pb = Arc::clone(&p);
            let b: u64 = cx
                .named_effect("middle", || async move {
                    pb.middle_runs.fetch_add(1, Ordering::SeqCst);
                    pb.middle_gate.pass().await;
                    Ok(Json(10))
                })
                .await?;
            let pb = Arc::clone(&p);
            cx.defer("release-b", move || async move {
                pb.releases.lock().unwrap().push("release-b");
                match args.release_b {
                    Release::Terminal => Err(JobErrorX::Terminal(ferr!("release-b broke"))),
                    Release::Ok | Release::TransientOnce => Ok(()),
                }
            })?;
            match args.end {
                End::Ok | End::TerminalInEffect | End::TransientInEffect => {}
                End::Terminal => return Err(JobErrorX::Terminal(ferr!("forward broke"))),
                End::TransientOnce if !p.transient_end_done.swap(true, Ordering::SeqCst) => {
                    return Err(JobErrorX::Transient(ferr!("forward flaked")));
                }
                End::TransientOnce => {}
            }
            let pc = Arc::clone(&p);
            let c: u64 = cx
                .named_effect("finish", || async move {
                    pc.finish_runs.fetch_add(1, Ordering::SeqCst);
                    match args.end {
                        End::TerminalInEffect => Err(JobErrorX::Terminal(ferr!("finish broke"))),
                        End::TransientInEffect => Err(JobErrorX::Transient(ferr!("finish flaked"))),
                        End::Ok | End::Terminal | End::TransientOnce => Ok(Json(100)),
                    }
                })
                .await?;
            Ok(a + b + c)
        })
    });
    host
}

fn saga(key: &str, args: &SagaArgs) -> Submission {
    Submission::new(key, "saga", serde_json::to_string(args).unwrap())
}

/// The classified failure a terminal result carries.
fn failure(result: Option<types::JobResult>) -> JobFailure {
    let Some(Err(types::JobError::Terminal(json))) = result else {
        panic!("expected a terminal failure, got {result:?}");
    };
    serde_json::from_str(&json).unwrap_or_else(|err| panic!("{json} is not a JobFailure: {err}"))
}

/// The committed `Defer` step names, in commit order.
fn committed_defers(transitions: &[authority::Transition]) -> Vec<String> {
    transitions
        .iter()
        .filter_map(|transition| match transition {
            authority::Transition::StepCommitted { step, .. }
                if step.kind == wflow_wdk::StepKind::Defer =>
            {
                Some(step.name.clone().expect("defers are named"))
            }
            _ => None,
        })
        .collect()
}

fn count(
    transitions: &[authority::Transition],
    pred: impl Fn(&authority::Transition) -> bool,
) -> usize {
    transitions.iter().filter(|t| pred(t)).count()
}

fn is_cleanup_started(transition: &authority::Transition) -> bool {
    matches!(transition, authority::Transition::CleanupStarted { .. })
}

#[tokio::test]
async fn cancel_while_waiting_runs_defers_and_settles_cancelled() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, mut commits) = start(&store, &saga_host(&probe)).await;
    assert_eq!(
        runtime.cancel("missing", "nobody").await,
        Err(CancelError::UnknownJob)
    );

    probe.release_gate.close();
    let args = SagaArgs {
        sleep_ms: Some(60_000),
        ..default()
    };
    runtime.submit(saga("job", &args)).await.unwrap();
    // Epoch bump, Submitted, Granted, acquire, WaitStarted.
    committed(&mut commits, 5).await;

    let ack = runtime.cancel("job", "operator").await.unwrap();
    assert_eq!(ack.disposition, authority::Disposition::Admitted);
    // The woken invocation is running its first defer, held at the gate: the
    // job is in cleanup, not settled, and receives nothing any more.
    probe.release_gate.entered.notified().await;
    assert_eq!(runtime.result("job").await.unwrap(), None);
    assert_eq!(
        runtime.send_message("job", "m1", "1").await,
        Err(SendError::InCleanup)
    );
    // A second cancel returns the original receipt with the job's current
    // disposition: leased and running its cleanup.
    assert_eq!(
        runtime.cancel("job", "again").await,
        Ok(authority::Ack {
            position: ack.position,
            disposition: authority::Disposition::Continue,
        })
    );
    probe.release_gate.open();

    assert_eq!(
        failure(runtime.await_result("job").await),
        JobFailure::Cancelled {
            reason: "operator".into()
        }
    );
    assert_eq!(probe.releases(), ["release-a"]);
    assert_eq!(probe.middle_runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        runtime.cancel("job", "late").await,
        Err(CancelError::AlreadyTerminal)
    );
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(count(&transitions, is_cleanup_started), 1);
    assert_eq!(committed_defers(&transitions), ["release-a"]);
    assert_eq!(
        count(&transitions, |t| matches!(
            t,
            authority::Transition::WaitResolved { .. }
        )),
        0,
        "the abandoned wait never resolves"
    );
    runtime.stop().await;
}

#[tokio::test]
async fn cancel_mid_effect_lets_it_commit_then_cleans_up() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, _) = start(&store, &saga_host(&probe)).await;

    probe.middle_gate.close();
    runtime.submit(saga("job", &default())).await.unwrap();
    probe.middle_gate.entered.notified().await;
    runtime.cancel("job", "operator").await.unwrap();
    probe.middle_gate.open();

    assert_eq!(
        failure(runtime.await_result("job").await),
        JobFailure::Cancelled {
            reason: "operator".into()
        }
    );
    // The in-flight effect committed, so the release it registers runs;
    // the step after it never starts.
    assert_eq!(probe.releases(), ["release-b", "release-a"]);
    assert_eq!(probe.finish_runs.load(Ordering::SeqCst), 0);
    let transitions = transitions(&runtime, "job").await;
    let cleanup_at = transitions.iter().position(is_cleanup_started).unwrap();
    let middle_at = transitions
        .iter()
        .position(|t| {
            matches!(t, authority::Transition::StepCommitted { step, .. } if step.name.as_deref() == Some("middle"))
        })
        .unwrap();
    assert!(cleanup_at < middle_at, "{transitions:?}");
    assert_eq!(committed_defers(&transitions), ["release-b", "release-a"]);
    runtime.stop().await;
}

#[tokio::test]
async fn terminal_failure_records_cause_before_defers_and_survives_restart() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let args = SagaArgs {
        end: End::Terminal,
        ..default()
    };

    probe.release_gate.close();
    let (runtime, _) = start(&store, &saga_host(&probe)).await;
    runtime.submit(saga("job", &args)).await.unwrap();
    // `release-b` ran and committed; `release-a` is in flight when we crash.
    probe.release_gate.entered.notified().await;
    assert_eq!(runtime.result("job").await.unwrap(), None);
    let transitions_before = transitions(&runtime, "job").await;
    assert_eq!(count(&transitions_before, is_cleanup_started), 1);
    assert_eq!(committed_defers(&transitions_before), ["release-b"]);
    runtime.stop().await;

    probe.release_gate.open();
    let (runtime, _) = start(&store, &saga_host(&probe)).await;
    let Some(Err(types::JobError::Terminal(error))) = runtime.await_result("job").await else {
        panic!("expected the forward error");
    };
    assert!(error.contains("forward broke"), "{error}");
    // Forward work replayed, not re-run; the committed defer replayed and the
    // interrupted one ran again.
    assert_eq!(probe.acquire_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.middle_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.finish_runs.load(Ordering::SeqCst), 0);
    assert_eq!(probe.releases(), ["release-b", "release-a", "release-a"]);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(
        count(&transitions, is_cleanup_started),
        1,
        "the replayed failure re-enters the durable cleanup"
    );
    assert_eq!(committed_defers(&transitions), ["release-b", "release-a"]);
    runtime.stop().await;
}

#[tokio::test]
async fn terminal_failure_inside_an_effect_frees_its_slot_for_the_first_defer() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, _) = start(&store, &saga_host(&probe)).await;
    let args = SagaArgs {
        end: End::TerminalInEffect,
        ..default()
    };
    runtime.submit(saga("job", &args)).await.unwrap();

    let Some(Err(types::JobError::Terminal(error))) = runtime.await_result("job").await else {
        panic!("expected the effect's error");
    };
    assert!(error.contains("finish broke"), "{error}");
    assert_eq!(probe.finish_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.releases(), ["release-b", "release-a"]);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(count(&transitions, is_cleanup_started), 1);
    // The abandoned `finish` effect never committed, so the first defer takes
    // its step slot right after the committed forward steps.
    let steps: Vec<_> = transitions
        .iter()
        .filter_map(|t| match t {
            authority::Transition::StepCommitted { step, .. } => {
                Some((step.id, step.name.clone().unwrap()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        steps,
        [
            (0, "acquire".to_owned()),
            (1, "middle".to_owned()),
            (2, "release-b".to_owned()),
            (3, "release-a".to_owned()),
        ]
    );
    runtime.stop().await;
}

#[tokio::test]
async fn transient_defer_failure_retries_and_resumes_the_cleanup() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, _) = start(&store, &saga_host(&probe)).await;
    let args = SagaArgs {
        end: End::Terminal,
        release_a: Release::TransientOnce,
        ..default()
    };
    runtime.submit(saga("job", &args)).await.unwrap();

    let Some(Err(types::JobError::Terminal(error))) = runtime.await_result("job").await else {
        panic!("expected the forward error");
    };
    assert!(error.contains("forward broke"), "{error}");
    assert_eq!(probe.releases(), ["release-b", "release-a", "release-a"]);
    assert_eq!(probe.acquire_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.middle_runs.load(Ordering::SeqCst), 1);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(
        count(&transitions, |t| matches!(
            t,
            authority::Transition::RetryScheduled { .. }
        )),
        1
    );
    assert_eq!(count(&transitions, is_cleanup_started), 1);
    assert_eq!(committed_defers(&transitions), ["release-b", "release-a"]);
    runtime.stop().await;
}

#[tokio::test]
async fn terminal_defer_failure_settles_as_cleanup_failed() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, _) = start(&store, &saga_host(&probe)).await;
    let args = SagaArgs {
        end: End::Terminal,
        release_b: Release::Terminal,
        ..default()
    };
    runtime.submit(saga("job", &args)).await.unwrap();

    let JobFailure::CleanupFailed {
        cause: CleanupCause::Failed { error },
        defer,
        error: defer_error,
    } = failure(runtime.await_result("job").await)
    else {
        panic!("expected CleanupFailed");
    };
    assert!(error.contains("forward broke"), "{error}");
    assert_eq!(defer, "release-b");
    assert!(defer_error.contains("release-b broke"), "{defer_error}");
    // The remaining compensation stays outstanding: nothing is archived as
    // completed cleanup.
    assert_eq!(probe.releases(), ["release-b"]);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(committed_defers(&transitions), Vec::<String>::new());
    runtime.stop().await;
}

#[tokio::test]
async fn exhausted_forward_retries_run_the_cleanup_on_a_fresh_budget() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, _) = start(&store, &saga_host(&probe)).await;
    let args = SagaArgs {
        end: End::TransientInEffect,
        release_a: Release::TransientOnce,
        ..default()
    };
    runtime
        .submit(
            saga("job", &args)
                .policy(RetryPolicy::Immediate)
                .budget(RetryBudget {
                    max_retries: Some(1),
                    max_restarts: None,
                }),
        )
        .await
        .unwrap();

    let JobFailure::RetriesExhausted {
        retries,
        max_retries,
        last_error,
    } = failure(runtime.await_result("job").await)
    else {
        panic!("expected RetriesExhausted");
    };
    assert_eq!((retries, max_retries), (1, 1));
    assert!(last_error.contains("finish flaked"), "{last_error}");
    // The `finish` closure ran on the initial run and its one retry; the
    // cleanup runs replay the committed effects and are interrupted at the
    // `finish` boundary before the closure runs again.
    assert_eq!(probe.finish_runs.load(Ordering::SeqCst), 2);
    assert_eq!(probe.acquire_runs.load(Ordering::SeqCst), 1);
    assert_eq!(probe.middle_runs.load(Ordering::SeqCst), 1);
    // Both defers ran in reverse; `release-a`'s transient failure was retried
    // from the cleanup phase's own budget and the committed `release-b` was
    // not re-run.
    assert_eq!(probe.releases(), ["release-b", "release-a", "release-a"]);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(committed_defers(&transitions), ["release-b", "release-a"]);
    assert_eq!(count(&transitions, is_cleanup_started), 1);
    let cleanup = transitions
        .iter()
        .find_map(|transition| match transition {
            authority::Transition::CleanupStarted { cleanup, .. } => Some(cleanup),
            _ => None,
        })
        .unwrap();
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
    assert_eq!(cleanup.steps, 2);
    assert!(
        cleanup.lease.is_some(),
        "the exhausted retry records the lease it released"
    );
    assert_eq!(
        count(&transitions, |transition| matches!(
            transition,
            authority::Transition::RetryScheduled { .. }
        )),
        2,
        "one accepted retry per phase"
    );
    assert_eq!(
        count(&transitions, |transition| matches!(
            transition,
            authority::Transition::Granted { .. }
        )),
        4
    );
    runtime.stop().await;
}

#[tokio::test]
async fn cancel_of_retry_parked_job_grants_at_once() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, mut commits) = start(&store, &saga_host(&probe)).await;
    let args = SagaArgs {
        end: End::TransientOnce,
        ..default()
    };
    runtime
        .submit(saga("job", &args).policy(RetryPolicy::Fixed { delay_ms: 60_000 }))
        .await
        .unwrap();
    // Epoch bump, Submitted, Granted, acquire, middle, RetryScheduled.
    committed(&mut commits, 6).await;
    assert!(matches!(
        runtime.revoke("job").await,
        Err(RevokeError::RetryPending { .. })
    ));

    let started = Instant::now();
    runtime.cancel("job", "operator").await.unwrap();
    assert_eq!(
        failure(runtime.await_result("job").await),
        JobFailure::Cancelled {
            reason: "operator".into()
        }
    );
    assert!(started.elapsed() < Duration::from_secs(30));
    assert_eq!(probe.releases(), ["release-b", "release-a"]);
    assert_eq!(probe.finish_runs.load(Ordering::SeqCst), 0);
    assert_eq!(probe.acquire_runs.load(Ordering::SeqCst), 1);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(
        count(&transitions, |t| matches!(
            t,
            authority::Transition::Granted { .. }
        )),
        2
    );
    runtime.stop().await;
}

#[tokio::test]
async fn late_timer_after_cancel_is_dropped() {
    let store = MemoryStore::default();
    let probe = Arc::new(SagaProbe::default());
    let (runtime, mut commits) = start(&store, &saga_host(&probe)).await;
    probe.release_gate.close();
    let args = SagaArgs {
        sleep_ms: Some(50),
        ..default()
    };
    runtime.submit(saga("job", &args)).await.unwrap();
    committed(&mut commits, 5).await;
    runtime.cancel("job", "operator").await.unwrap();
    probe.release_gate.entered.notified().await;
    // The abandoned timer's deadline passes while the cleanup is running; the
    // wheel finds no wait to resolve and drops it.
    tokio::time::sleep(Duration::from_millis(120)).await;
    probe.release_gate.open();

    assert_eq!(
        failure(runtime.await_result("job").await),
        JobFailure::Cancelled {
            reason: "operator".into()
        }
    );
    assert_eq!(probe.middle_runs.load(Ordering::SeqCst), 0);
    let transitions = transitions(&runtime, "job").await;
    assert_eq!(
        count(&transitions, |t| matches!(
            t,
            authority::Transition::WaitResolved { .. }
        )),
        0
    );
    runtime.stop().await;
}
