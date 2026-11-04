use crate::interlude::*;
use std::ops::Bound;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use wflow_core::kvstore::atomic::MemoryStore;
use wflow_core::kvstore::atomic::Store;
use wflow_core::partition::CleanupCause;
use wflow_core::partition::authority::Disposition;
use wflow_core::partition::authority::Transition;
use wflow_core::partition::authority::WaitKind;
use wflow_tokio::authority_runtime::AuthorityRuntime;
use wflow_tokio::authority_runtime::Submission;
use wflow_tokio::authority_runtime::session::Host;
use wflow_tokio::authority_runtime::session::Session;
use wflow_wdk::Json;
use wflow_wdk::StepKind;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::types::JobError;

use super::native_wasm_conformance::DeferOnTerminalArgs;
use super::native_wasm_conformance::setup_dual_host_context;
use wflow_core::partition::JobFailure;
use wflow_core::partition::RetryBudget;
use wflow_core::partition::RetryPolicy;
use wflow_tokio::authority_runtime::ExpireError;
use wflow_tokio::authority_runtime::SendError;

#[derive(Serialize, Deserialize)]
struct ProbeArgs {
    key: String,
    millis: u64,
}

fn register_probe(cx: &super::WflowTestContext) {
    let kv = Arc::clone(&cx.keyvalue_plugin);
    cx.local_native_host
        .register_typed("native_authority_probe", move |cx, args: ProbeArgs| {
            let kv = Arc::clone(&kv);
            Box::pin(async move {
                let count: u64 = cx
                    .effect(async || {
                        let mut bytes = kv
                            .get_value("workload_123", "default", &args.key)
                            .await
                            .unwrap_or_default();
                        bytes.push(1);
                        let count = bytes.len() as u64;
                        kv.set_value("workload_123", "default", &args.key, bytes)
                            .await
                            .map_err(|err| wflow_wdk::JobErrorX::Terminal(ferr!("{err:?}")))?;
                        Ok(Json(count))
                    })
                    .await?;
                let Json(message) = cx.recv::<Json<String>>().await?;
                cx.sleep(Duration::from_millis(args.millis)).await?;
                cx.effect(async || Ok(Json(format!("{count}:{message}"))))
                    .await
            })
        });
}

async fn history(store: &MemoryStore) -> Res<Vec<Transition>> {
    let read = store
        .read(
            &[],
            (
                Bound::Included(Arc::from(&b"\x01"[..])),
                Bound::Excluded(Arc::from(&b"\x02"[..])),
            ),
            128,
        )
        .await?;
    read.entries
        .iter()
        .map(|entry| serde_json::from_slice(&entry.value).map_err(Into::into))
        .collect()
}

async fn wait_for(
    store: &MemoryStore,
    predicate: impl Fn(&Transition) -> bool,
) -> Res<Vec<Transition>> {
    let mut revisions = store.subscribe();
    loop {
        revisions.borrow_and_update();
        let entries = history(store).await?;
        if entries.iter().any(&predicate) {
            return Ok(entries);
        }
        revisions.changed().await?;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authority_hosts_match_warm_steps_retry_and_replay_rejection() -> Res<()> {
    let cx = setup_dual_host_context().await?;
    for (host, prefix) in [
        (Arc::clone(&cx.wflow_plugin) as Arc<dyn Host>, ""),
        (
            Arc::clone(&cx.local_native_host) as Arc<dyn Host>,
            "native_",
        ),
    ] {
        let store = MemoryStore::default();
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;
        runtime
            .submit(Submission::new(
                "chain",
                format!("{prefix}effect_chain"),
                "{\"steps\":3}",
            ))
            .await
            .unwrap();
        assert_eq!(runtime.await_result("chain").await, Some(Ok("null".into())));
        let entries = history(&store).await?;
        assert_eq!(entries.len(), 6);
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, Transition::StepCommitted { .. }))
                .count(),
            3
        );
        runtime
            .submit(Submission::new(
                "retry",
                format!("{prefix}fails_once"),
                format!("{{\"key\":\"{prefix}retry\"}}"),
            ))
            .await
            .unwrap();
        assert_eq!(runtime.await_result("retry").await, Some(Ok("null".into())));
        runtime
            .submit(Submission::new(
                "diverged",
                format!("{prefix}nondeterministic_replay"),
                format!("{{\"key\":\"{prefix}diverged\"}}"),
            ))
            .await
            .unwrap();
        let Some(Err(JobError::Terminal(error))) = runtime.await_result("diverged").await else {
            panic!("divergent step name must be terminal");
        };
        assert!(
            error.contains("nondeterministic replay at step 0"),
            "{error}"
        );
        runtime.stop().await;
        host.forget_job("chain");
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;
        assert_eq!(runtime.result("chain").await?, Some(Ok("null".into())));
        assert_eq!(
            runtime
                .submit(Submission::new(
                    "chain",
                    format!("{prefix}effect_chain"),
                    "{\"steps\":3}"
                ))
                .await
                .unwrap()
                .disposition,
            Disposition::Completed
        );
        runtime.stop().await;

        let mut session = host
            .spawn(
                "cold-kind".into(),
                &format!("{prefix}effect_chain"),
                "{\"steps\":1}".into(),
                vec![ReplayStep {
                    kind: Some(StepKind::Recv),
                    // No name is pinned: the legacy journal records none, so
                    // only the durable kind mismatch may reject this replay.
                    name: None,
                    value_json: Some("0".into()),
                    failed_attempts: 0,
                }],
                None,
            )
            .await?;
        let Some(SessionYield::Complete(Err(JobError::Terminal(error)))) =
            session.next_yield().await
        else {
            panic!("cold replay must reject durable kind mismatch before yielding another effect");
        };
        assert!(
            error.contains("nondeterministic replay at step 0"),
            "{error}"
        );
    }
    cx.stop().await?;
    Ok(())
}

/// Wraps the first session `inner` spawns so the first yield matching `hold`
/// is held at the gate until released (or the session is dropped).
struct HeldYieldHost {
    inner: Arc<dyn Host>,
    first: AtomicBool,
    hold: fn(&SessionYield) -> bool,
    gate: Arc<YieldGate>,
}

#[derive(Default)]
struct YieldGate {
    arrived: Notify,
    release: Notify,
    dropped: Notify,
    continuations: AtomicUsize,
}

struct HeldYieldSession {
    inner: Box<dyn Session>,
    hold: fn(&SessionYield) -> bool,
    held: bool,
    gate: Arc<YieldGate>,
}

#[async_trait]
impl Host for HeldYieldHost {
    fn contains_wflow(&self, key: &str) -> bool {
        self.inner.contains_wflow(key)
    }

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
        cleanup: Option<CleanupCause>,
    ) -> Res<Box<dyn Session>> {
        let session = self
            .inner
            .spawn(job_id, wflow_key, args_json, replay, cleanup)
            .await?;
        if self.first.swap(false, Ordering::SeqCst) {
            Ok(Box::new(HeldYieldSession {
                inner: session,
                hold: self.hold,
                held: false,
                gate: Arc::clone(&self.gate),
            }))
        } else {
            Ok(session)
        }
    }

    fn forget_job(&self, job_id: &str) {
        self.inner.forget_job(job_id);
    }
}

#[async_trait]
impl Session for HeldYieldSession {
    async fn next_yield(&mut self) -> Option<SessionYield> {
        let yielded = self.inner.next_yield().await;
        if !self.held && yielded.as_ref().is_some_and(self.hold) {
            self.held = true;
            self.gate.arrived.notify_one();
            self.gate.release.notified().await;
        }
        yielded
    }

    fn resume(&self) -> bool {
        self.gate.continuations.fetch_add(1, Ordering::SeqCst);
        self.inner.resume()
    }

    fn deliver(&self, value_json: Arc<str>) -> bool {
        self.gate.continuations.fetch_add(1, Ordering::SeqCst);
        self.inner.deliver(value_json)
    }

    fn interrupt(&self, cause: CleanupCause) -> bool {
        self.gate.continuations.fetch_add(1, Ordering::SeqCst);
        self.inner.interrupt(cause)
    }
}

impl Drop for HeldYieldSession {
    fn drop(&mut self) {
        self.gate.dropped.notify_one();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authority_hosts_fence_stale_in_flight_yields() -> Res<()> {
    let cx = setup_dual_host_context().await?;
    register_probe(&cx);
    for (inner, prefix) in [
        (Arc::clone(&cx.wflow_plugin) as Arc<dyn Host>, ""),
        (
            Arc::clone(&cx.local_native_host) as Arc<dyn Host>,
            "native_",
        ),
    ] {
        let gate = Arc::new(YieldGate::default());
        let host = Arc::new(HeldYieldHost {
            inner,
            first: AtomicBool::new(true),
            hold: |yielded| matches!(yielded, SessionYield::PersistStep { step_id: 0, .. }),
            gate: Arc::clone(&gate),
        });
        let store = MemoryStore::default();
        let runtime = AuthorityRuntime::start(store.writer()?, 1, host).await?;
        let args = serde_json::to_string(&ProbeArgs {
            key: format!("{prefix}fenced"),
            millis: 0,
        })?;
        runtime
            .submit(Submission::new(
                "fenced",
                format!("{prefix}authority_probe"),
                args,
            ))
            .await
            .unwrap();
        gate.arrived.notified().await;
        runtime.revoke("fenced").await.unwrap();
        wait_for(&store, |entry| matches!(entry, Transition::WaitStarted { wait, .. } if wait.kind == WaitKind::Message)).await?;
        gate.release.notify_one();
        gate.dropped.notified().await;
        assert_eq!(gate.continuations.load(Ordering::SeqCst), 0);
        assert_eq!(
            history(&store)
                .await?
                .iter()
                .filter(|entry| matches!(entry, Transition::StepCommitted { .. }))
                .count(),
            1
        );
        runtime
            .send_message("fenced", "message", "\"fenced\"")
            .await
            .unwrap();
        assert_eq!(
            runtime.await_result("fenced").await,
            Some(Ok("\"2:fenced\"".into()))
        );
        assert_eq!(
            cx.get_keyvalue("default", &format!("{prefix}fenced")).await,
            Some(vec![1, 1])
        );
        runtime.stop().await;
    }
    cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn authority_hosts_recover_waits_and_revoke_without_repeating_committed_effects() -> Res<()> {
    let cx = setup_dual_host_context().await?;
    register_probe(&cx);
    for (host, prefix) in [
        (Arc::clone(&cx.wflow_plugin) as Arc<dyn Host>, ""),
        (
            Arc::clone(&cx.local_native_host) as Arc<dyn Host>,
            "native_",
        ),
    ] {
        let store = MemoryStore::default();
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;
        let args = serde_json::to_string(&ProbeArgs {
            key: format!("{prefix}probe"),
            millis: 100,
        })?;
        runtime
            .submit(Submission::new(
                "probe",
                format!("{prefix}authority_probe"),
                args,
            ))
            .await
            .unwrap();
        wait_for(&store, |entry| matches!(entry, Transition::WaitStarted { wait, .. } if wait.kind == WaitKind::Message)).await?;
        runtime.revoke("probe").await.unwrap();
        runtime.stop().await;
        host.forget_job("probe");
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;
        let first = runtime
            .send_message("probe", "message", "\"hello\"")
            .await
            .unwrap();
        assert_eq!(
            runtime
                .send_message("probe", "message", "\"hello\"")
                .await
                .unwrap(),
            first
        );
        let entries = wait_for(&store, |entry| matches!(entry, Transition::WaitStarted { wait, .. } if matches!(wait.kind, WaitKind::Timer { .. }))).await?;
        let deadline = entries
            .iter()
            .find_map(|entry| match entry {
                Transition::WaitStarted { wait, .. } => match wait.kind {
                    WaitKind::Timer { fire_at_ms } => Some(fire_at_ms),
                    WaitKind::Message => None,
                },
                _ => None,
            })
            .unwrap();
        runtime.stop().await;
        host.forget_job("probe");
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;
        assert_eq!(
            runtime.await_result("probe").await,
            Some(Ok("\"1:hello\"".into()))
        );
        assert_eq!(
            cx.get_keyvalue("default", &format!("{prefix}probe")).await,
            Some(vec![1])
        );
        let entries = history(&store).await?;
        let waits: Vec<_> = entries
            .iter()
            .filter_map(|entry| match entry {
                Transition::WaitStarted { wait, .. } => Some(wait.kind),
                _ => None,
            })
            .collect();
        assert_eq!(
            waits,
            vec![
                WaitKind::Message,
                WaitKind::Timer {
                    fire_at_ms: deadline
                }
            ]
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, Transition::WaitResolved { .. }))
                .count(),
            2
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, Transition::StepCommitted { .. }))
                .count(),
            2
        );
        runtime.stop().await;
    }
    cx.stop().await?;
    Ok(())
}

/// The committed `Defer` step names, in commit order.
fn committed_defers(entries: &[Transition]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            Transition::StepCommitted { step, .. } if step.kind == StepKind::Defer => {
                Some(step.name.clone().expect("defers are named"))
            }
            _ => None,
        })
        .collect()
}

fn cleanups_started(entries: &[Transition]) -> usize {
    entries
        .iter()
        .filter(|entry| matches!(entry, Transition::CleanupStarted { .. }))
        .count()
}

/// The classified failure a settled job carries in its terminal envelope.
fn classified_failure(result: Option<wflow_wdk::types::JobResult>) -> JobFailure {
    let Some(Err(JobError::Terminal(json))) = result else {
        panic!("expected a terminal failure, got {result:?}");
    };
    serde_json::from_str(&json).unwrap_or_else(|err| panic!("{json} is not a JobFailure: {err}"))
}

fn cancelled_reason(result: Option<wflow_wdk::types::JobResult>) -> String {
    match classified_failure(result) {
        JobFailure::Cancelled { reason } => reason,
        other => panic!("expected JobFailure::Cancelled, got {other:?}"),
    }
}

/// Both hosts run the registered defers in reverse order as journaled steps
/// after a terminal failure, after a cancel that lands on a held message
/// wait, and after a crash in the middle of the cleanup itself (the replayed
/// invocation is interrupted at its wait and resumes the outstanding defer).
#[tokio::test(flavor = "multi_thread")]
async fn authority_hosts_run_cleanup_after_failure_cancel_and_crash() -> Res<()> {
    let cx = setup_dual_host_context().await?;
    for (inner, prefix) in [
        (Arc::clone(&cx.wflow_plugin) as Arc<dyn Host>, ""),
        (
            Arc::clone(&cx.local_native_host) as Arc<dyn Host>,
            "native_",
        ),
    ] {
        let wflow_key = format!("{prefix}defer_on_terminal");
        let store = MemoryStore::default();
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&inner)).await?;

        let failed_key = format!("{prefix}cleanup-failed");
        runtime
            .submit(Submission::new(
                "failed",
                &wflow_key,
                serde_json::to_string(&DeferOnTerminalArgs {
                    key: failed_key.clone(),
                    fail: true,
                    wait: false,
                    flake: false,
                })?,
            ))
            .await
            .unwrap();
        let Some(Err(JobError::Terminal(error))) = runtime.await_result("failed").await else {
            panic!("expected the forward error");
        };
        assert!(error.contains("boom after prepared"), "{error}");
        assert_eq!(
            cx.get_keyvalue("default", &failed_key).await,
            Some(b"BA".to_vec())
        );

        let cancelled_key = format!("{prefix}cleanup-cancelled");
        runtime
            .submit(Submission::new(
                "cancelled",
                &wflow_key,
                serde_json::to_string(&DeferOnTerminalArgs {
                    key: cancelled_key.clone(),
                    fail: false,
                    wait: true,
                    flake: false,
                })?,
            ))
            .await
            .unwrap();
        wait_for(&store, |entry| matches!(entry, Transition::WaitStarted { wait, .. } if wait.kind == WaitKind::Message)).await?;
        // Restart first: the cancel then lands on a wait re-attached by a
        // replayed invocation, not on the one that began it.
        runtime.stop().await;
        inner.forget_job("cancelled");
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&inner)).await?;
        assert_eq!(
            runtime
                .cancel("cancelled", "operator")
                .await
                .unwrap()
                .disposition,
            Disposition::Admitted
        );
        assert_eq!(
            cancelled_reason(runtime.await_result("cancelled").await),
            "operator"
        );
        assert_eq!(
            cx.get_keyvalue("default", &cancelled_key).await,
            Some(b"BA".to_vec())
        );
        let entries = history(&store).await?;
        assert_eq!(cleanups_started(&entries), 2);
        assert_eq!(
            committed_defers(&entries),
            ["cleanup-b", "cleanup-a", "cleanup-b", "cleanup-a"]
        );
        assert!(
            !entries
                .iter()
                .any(|entry| matches!(entry, Transition::WaitResolved { .. })),
            "the abandoned wait never resolves: {entries:?}"
        );
        runtime.stop().await;

        // Crash mid-cleanup: the first defer's commit is held and the
        // runtime stops underneath it, so the restarted runtime finds the
        // job in cleanup with no defer committed.
        let gate = Arc::new(YieldGate::default());
        let store = MemoryStore::default();
        let host = Arc::new(HeldYieldHost {
            inner: Arc::clone(&inner),
            first: AtomicBool::new(true),
            hold: |yielded| matches!(yielded, SessionYield::PersistStep { meta, .. } if meta.kind == StepKind::Defer),
            gate: Arc::clone(&gate),
        });
        let runtime = AuthorityRuntime::start(store.writer()?, 1, host).await?;
        let crashed_key = format!("{prefix}cleanup-crashed");
        runtime
            .submit(Submission::new(
                "crashed",
                &wflow_key,
                serde_json::to_string(&DeferOnTerminalArgs {
                    key: crashed_key.clone(),
                    fail: false,
                    wait: true,
                    flake: false,
                })?,
            ))
            .await
            .unwrap();
        wait_for(&store, |entry| matches!(entry, Transition::WaitStarted { wait, .. } if wait.kind == WaitKind::Message)).await?;
        let ack = runtime.cancel("crashed", "operator").await.unwrap();
        gate.arrived.notified().await;
        // In cleanup, not settled: a second cancel acks the same cleanup and
        // the job receives nothing any more.
        assert_eq!(
            runtime.cancel("crashed", "again").await.unwrap().position,
            ack.position
        );
        assert_eq!(
            runtime.send_message("crashed", "late", "\"late\"").await,
            Err(SendError::InCleanup)
        );
        runtime.stop().await;
        gate.dropped.notified().await;
        inner.forget_job("crashed");
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&inner)).await?;
        assert_eq!(
            cancelled_reason(runtime.await_result("crashed").await),
            "operator"
        );
        // The held `cleanup-b` ran but never committed, so it ran again.
        assert_eq!(
            cx.get_keyvalue("default", &crashed_key).await,
            Some(b"BBA".to_vec())
        );
        let entries = history(&store).await?;
        assert_eq!(cleanups_started(&entries), 1);
        assert_eq!(committed_defers(&entries), ["cleanup-b", "cleanup-a"]);
        runtime.stop().await;
    }
    cx.stop().await?;
    Ok(())
}

/// Both hosts enter cleanup when the forward retry budget runs out: the
/// exhausting attempt releases its lease, the cleanup is granted on a fresh
/// budget and runs the registered defers in reverse order, and the job then
/// settles as [`JobFailure::RetriesExhausted`] carrying the last forward error.
#[tokio::test(flavor = "multi_thread")]
async fn authority_hosts_run_cleanup_after_the_forward_retry_budget_runs_out() -> Res<()> {
    let cx = setup_dual_host_context().await?;
    for (inner, prefix) in [
        (Arc::clone(&cx.wflow_plugin) as Arc<dyn Host>, ""),
        (
            Arc::clone(&cx.local_native_host) as Arc<dyn Host>,
            "native_",
        ),
    ] {
        let wflow_key = format!("{prefix}defer_on_terminal");
        let store = MemoryStore::default();
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&inner)).await?;

        let key = format!("{prefix}cleanup-exhausted");
        runtime
            .submit(
                Submission::new(
                    "exhausted",
                    &wflow_key,
                    serde_json::to_string(&DeferOnTerminalArgs {
                        key: key.clone(),
                        fail: false,
                        wait: false,
                        flake: true,
                    })?,
                )
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
        } = classified_failure(runtime.await_result("exhausted").await)
        else {
            panic!("expected JobFailure::RetriesExhausted");
        };
        assert_eq!((retries, max_retries), (1, 1));
        assert!(last_error.contains("flaky after prepared"), "{last_error}");
        assert_eq!(cx.get_keyvalue("default", &key).await, Some(b"BA".to_vec()));

        let entries = history(&store).await?;
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, Transition::RetryScheduled { .. }))
                .count(),
            1
        );
        let cleanups: Vec<_> = entries
            .iter()
            .filter_map(|entry| match entry {
                Transition::CleanupStarted { cleanup, .. } => Some(cleanup),
                _ => None,
            })
            .collect();
        let [cleanup] = cleanups[..] else {
            panic!("expected exactly one cleanup entry, got {cleanups:?}");
        };
        assert!(
            matches!(
                &cleanup.cause,
                CleanupCause::RetriesExhausted {
                    retries: 1,
                    max_retries: 1,
                    ..
                }
            ),
            "{cleanup:?}"
        );
        // Exhaustion released the lease it was holding; the cleanup ran under a
        // fresh grant.
        assert!(cleanup.lease.is_some(), "{cleanup:?}");
        assert_eq!(committed_defers(&entries), ["cleanup-b", "cleanup-a"]);
        runtime.stop().await;
    }
    cx.stop().await?;
    Ok(())
}

/// Both hosts expose race-free `JobHandle` waiting, compact terminal receipts,
/// retention expiry + key reuse under a fresh incarnation, and active-only
/// snapshot compaction across restarts.
#[tokio::test(flavor = "multi_thread")]
async fn authority_hosts_match_job_handle_retention_expiry_key_reuse_and_compaction() -> Res<()> {
    let cx = setup_dual_host_context().await?;
    register_probe(&cx);
    for (host, prefix) in [
        (Arc::clone(&cx.wflow_plugin) as Arc<dyn Host>, ""),
        (
            Arc::clone(&cx.local_native_host) as Arc<dyn Host>,
            "native_",
        ),
    ] {
        let store = MemoryStore::default();
        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;

        // 1. Retained receipt blocks early expiry and survives compaction.
        let (ack, retained_handle) = runtime
            .submit_handle(
                Submission::new("retained", format!("{prefix}effect_chain"), "{\"steps\":2}")
                    .retention_ms(60_000),
            )
            .await
            .unwrap();
        assert_eq!(ack.disposition, Disposition::Admitted);
        assert_eq!(
            retained_handle.await_result().await,
            Some(Ok("null".into()))
        );
        let retained_receipt = retained_handle.receipt().await?.expect("retained receipt");
        assert_eq!(retained_receipt.incarnation, 1);
        assert_eq!(retained_receipt.steps, 2);
        assert_eq!(
            runtime.expire("retained").await,
            Err(ExpireError::RetentionPending {
                expires_at_ms: retained_receipt.outcome.expires_at_ms(),
            })
        );

        // 2. Zero-retention job expires and its key is reused with a new
        // invocation under incarnation 3.
        let (_, ephemeral_handle) = runtime
            .submit_handle(
                Submission::new("reused", format!("{prefix}effect_chain"), "{\"steps\":1}")
                    .retention_ms(0),
            )
            .await
            .unwrap();
        assert_eq!(
            ephemeral_handle.await_result().await,
            Some(Ok("null".into()))
        );
        assert!(matches!(
            runtime.expire("reused").await,
            Ok(_) | Err(ExpireError::UnknownJob)
        ));
        while runtime.receipt("reused").await?.is_some() {
            tokio::task::yield_now().await;
        }
        assert_eq!(runtime.get_result("reused").await?, None);

        let (reused_ack, reused_handle) = runtime
            .submit_handle(
                Submission::new("reused", format!("{prefix}effect_chain"), "{\"steps\":3}")
                    .retention_ms(60_000),
            )
            .await
            .unwrap();
        assert_eq!(reused_ack.disposition, Disposition::Admitted);
        assert_eq!(reused_handle.await_result().await, Some(Ok("null".into())));
        let reused_receipt = reused_handle.receipt().await?.expect("reused receipt");
        assert_eq!(reused_receipt.incarnation, 3);
        assert_eq!(reused_receipt.steps, 3);

        // 3. Start an active job that parks on a message wait, compact the
        // journal, restart from the compacted store, and finish the active job.
        let probe_args = serde_json::to_string(&ProbeArgs {
            key: format!("{prefix}compacted-probe"),
            millis: 0,
        })?;
        let (_, active_handle) = runtime
            .submit_handle(Submission::new(
                "active",
                format!("{prefix}authority_probe"),
                probe_args,
            ))
            .await
            .unwrap();
        wait_for(&store, |entry| {
            matches!(entry, Transition::WaitStarted { key, wait } if key == "active" && wait.kind == WaitKind::Message)
        })
        .await?;
        assert_eq!(active_handle.get_result().await?, None);
        assert_eq!(runtime.expire("active").await, Err(ExpireError::Active));

        let cut = runtime.compact().await?;
        assert!(cut > 0);
        assert!(history(&store).await?.is_empty());

        runtime.stop().await;
        host.forget_job("active");

        let runtime = AuthorityRuntime::start(store.writer()?, 1, Arc::clone(&host)).await?;
        assert_eq!(
            runtime.get_result("retained").await?,
            Some(Ok("null".into()))
        );
        assert_eq!(runtime.get_result("reused").await?, Some(Ok("null".into())));
        runtime
            .send_message("active", "msg", "\"compacted\"")
            .await
            .unwrap();
        assert_eq!(
            runtime.await_result("active").await,
            Some(Ok("\"1:compacted\"".into()))
        );
        assert_eq!(
            cx.get_keyvalue("default", &format!("{prefix}compacted-probe"))
                .await,
            Some(vec![1])
        );
        runtime.stop().await;
    }
    cx.stop().await?;
    Ok(())
}
