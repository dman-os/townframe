use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;
use wflow_core::r#gen::metastore::WflowMeta;
use wflow_core::r#gen::metastore::WflowServiceMeta;
use wflow_core::partition::JobFailure;
use wflow_core::partition::effects;
use wflow_core::partition::job_events;
use wflow_core::partition::state;
use wflow_wdk::JobErrorX;
use wflow_wdk::Json;
use wflow_wdk::route_wflows;

use crate::interlude::*;
use crate::*;

/// Counts the closures a cleanup workflow ran: `a`/`b` are the forward
/// effects, `undo_a` the defer registered after `a` committed.
#[derive(Default)]
struct CleanupRuns {
    a: AtomicUsize,
    b: AtomicUsize,
    undo_a: AtomicUsize,
}

/// `fail_after_a` makes the workflow return a terminal error after
/// registering its defer instead of running effect `b`.
fn register_cleanup_wflow(driver: &NativeHostDriver, runs: &Arc<CleanupRuns>, fail_after_a: bool) {
    let runs = Arc::clone(runs);
    driver.register_typed("cleanup", move |cx, sleep_ms: u64| {
        let runs = Arc::clone(&runs);
        Box::pin(async move {
            let a: i64 = cx
                .named_effect("a", || {
                    let runs = Arc::clone(&runs);
                    async move {
                        runs.a.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(1i64))
                    }
                })
                .await?;
            let undo_runs = Arc::clone(&runs);
            cx.defer("undo-a", move || async move {
                undo_runs.undo_a.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })?;
            if fail_after_a {
                return Err(JobErrorX::Terminal(ferr!("forward work failed")));
            }
            if sleep_ms > 0 {
                cx.named_sleep("nap", Duration::from_millis(sleep_ms))
                    .await?;
            }
            let b: i64 = cx
                .named_effect("b", || {
                    let runs = Arc::clone(&runs);
                    async move {
                        runs.b.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(a + 1))
                    }
                })
                .await?;
            Ok(b)
        })
    });
}

async fn expect_persist(session: &NativeSession, expected_meta: StepMeta) -> Arc<str> {
    let trap = session.next_yield().await.expect("session yielded nothing");
    let SessionYield::PersistStep {
        value_json, meta, ..
    } = trap
    else {
        panic!("expected PersistStep, got {trap:?}");
    };
    assert_eq!(meta, expected_meta);
    value_json
}

async fn expect_terminal(session: &NativeSession) -> String {
    let trap = session.next_yield().await.expect("session yielded nothing");
    let SessionYield::Complete(Err(types::JobError::Terminal(error_json))) = trap else {
        panic!("expected terminal Complete, got {trap:?}");
    };
    error_json
}

/// After the host interrupted the session, the cleanup it runs must consist of
/// exactly the `undo-a` defer followed by the terminal outcome.
async fn expect_cleanup_outcome(session: &NativeSession, runs: &CleanupRuns) -> String {
    let defer_value = expect_persist(session, StepMeta::new(StepKind::Defer, Some("undo-a"))).await;
    assert_eq!(defer_value.as_ref(), "null");
    assert_eq!(runs.undo_a.load(Ordering::SeqCst), 1);
    assert!(session.resume());
    let error_json = expect_terminal(session).await;
    assert_eq!(runs.b.load(Ordering::SeqCst), 0, "effect b must not run");
    error_json
}

#[tokio::test]
async fn interrupt_while_parked_on_persist_runs_defers_with_cancelled_outcome() {
    let driver = NativeHostDriver::new();
    let runs = Arc::new(CleanupRuns::default());
    register_cleanup_wflow(&driver, &runs, false);
    let session = driver
        .spawn(
            Arc::from("job-int-persist"),
            "cleanup",
            "0".into(),
            vec![],
            None,
        )
        .expect("spawn");

    let a_value = expect_persist(&session, StepMeta::new(StepKind::Effect, Some("a"))).await;
    assert_eq!(a_value.as_ref(), "1");
    assert!(session.interrupt(CleanupCause::Cancelled {
        reason: "operator".into(),
    }));

    let error_json = expect_cleanup_outcome(&session, &runs).await;
    let failure: JobFailure = serde_json::from_str(&error_json).expect(ERROR_JSON);
    assert_eq!(
        failure,
        JobFailure::Cancelled {
            reason: "operator".into()
        }
    );
    assert_eq!(runs.a.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn interrupt_while_parked_on_sleep_returns_interrupted_and_runs_defers() {
    let driver = NativeHostDriver::new();
    let runs = Arc::new(CleanupRuns::default());
    register_cleanup_wflow(&driver, &runs, false);
    let session = driver
        .spawn(
            Arc::from("job-int-sleep"),
            "cleanup",
            "60000".into(),
            vec![],
            None,
        )
        .expect("spawn");

    let a_value = expect_persist(&session, StepMeta::new(StepKind::Effect, Some("a"))).await;
    assert_eq!(a_value.as_ref(), "1");
    assert!(session.resume());

    let trap = session.next_yield().await.expect("session yielded nothing");
    let SessionYield::WaitStep { step_id, meta, .. } = trap else {
        panic!("expected WaitStep, got {trap:?}");
    };
    assert_eq!(step_id, 1);
    assert_eq!(meta, StepMeta::new(StepKind::Sleep, Some("nap")));
    assert!(session.interrupt(CleanupCause::Cancelled {
        reason: "while napping".into(),
    }));

    let error_json = expect_cleanup_outcome(&session, &runs).await;
    let failure: JobFailure = serde_json::from_str(&error_json).expect(ERROR_JSON);
    assert_eq!(
        failure,
        JobFailure::Cancelled {
            reason: "while napping".into()
        }
    );
}

#[tokio::test]
async fn spawn_in_cleanup_replays_committed_steps_and_interrupts_the_next_forward_one() {
    let driver = NativeHostDriver::new();
    let runs = Arc::new(CleanupRuns::default());
    register_cleanup_wflow(&driver, &runs, false);
    let replay = vec![ReplayStep::durable(
        &StepMeta::new(StepKind::Effect, Some("a")),
        "1",
    )];
    let session = driver
        .spawn(
            Arc::from("job-int-replay"),
            "cleanup",
            "0".into(),
            replay,
            Some(CleanupCause::Failed {
                error: "durable failure".into(),
            }),
        )
        .expect("spawn");

    let error_json = expect_cleanup_outcome(&session, &runs).await;
    assert_eq!(error_json, "durable failure");
    assert_eq!(runs.a.load(Ordering::SeqCst), 0, "replayed a must not run");
}

#[tokio::test]
async fn begin_cleanup_round_trips_the_durable_cause() {
    let driver = NativeHostDriver::new();
    let runs = Arc::new(CleanupRuns::default());
    register_cleanup_wflow(&driver, &runs, true);
    let session = driver
        .spawn(
            Arc::from("job-begin-cleanup"),
            "cleanup",
            "0".into(),
            vec![],
            None,
        )
        .expect("spawn");

    let a_value = expect_persist(&session, StepMeta::new(StepKind::Effect, Some("a"))).await;
    assert_eq!(a_value.as_ref(), "1");
    assert!(session.resume());

    let trap = session.next_yield().await.expect("session yielded nothing");
    let SessionYield::BeginCleanup { error_json } = trap else {
        panic!("expected BeginCleanup, got {trap:?}");
    };
    assert!(error_json.contains("forward work failed"));
    assert!(session.interrupt(CleanupCause::Failed {
        error: error_json.to_string(),
    }));

    let outcome = expect_cleanup_outcome(&session, &runs).await;
    assert_eq!(outcome, error_json.as_ref());
}

fn make_journal(key: &str, args_json: &str) -> state::JobState {
    state::JobState {
        init_args_json: Arc::from(args_json),
        wflow: WflowMeta {
            key: key.to_owned(),
            service: WflowServiceMeta::LocalNative,
        },
        override_wflow_retry_policy: None,
        cancelling: false,
        runs: Vec::new(),
        steps: Vec::new(),
        pending_messages: default(),
        active_wait: None,
    }
}

fn eff_id(entry_id: u64, effect_idx: u64) -> effects::EffectId {
    effects::EffectId {
        entry_id,
        effect_idx,
    }
}

fn push_step_effect(journal: &mut state::JobState, effect: job_events::JobEffectResult) {
    let idx = effect.step_id as usize;
    while journal.steps.len() <= idx {
        journal.steps.push(state::JobStepState::Effect {
            attempts: default(),
        });
    }
    let state::JobStepState::Effect { attempts } = &mut journal.steps[idx];
    attempts.push(effect);
}

#[tokio::test]
async fn warm_and_cold_sessions_preserve_rc_future_and_skip_replayed_effects() {
    let driver = NativeHostDriver::new();
    let step0_runs = Arc::new(AtomicUsize::new(0));
    let step1_runs = Arc::new(AtomicUsize::new(0));

    let s0 = Arc::clone(&step0_runs);
    let s1 = Arc::clone(&step1_runs);
    driver.register("two-step", move |args, backend| {
        let s0 = Arc::clone(&s0);
        let s1 = Arc::clone(&s1);
        Box::pin(async move {
            let local_rc = Rc::new(Cell::new(10i64));
            route_wflows!(args, backend, {
                "two-step" => |cx, input: i64| async move {
                    let a: i64 = cx
                        .named_effect("step-0", || {
                            let s0 = Arc::clone(&s0);
                            let local_rc = Rc::clone(&local_rc);
                            async move {
                                s0.fetch_add(1, Ordering::SeqCst);
                                local_rc.set(local_rc.get() + input);
                                Ok(Json(local_rc.get()))
                            }
                        })
                        .await?;
                    let b: i64 = cx
                        .named_effect("step-1", || {
                            let s1 = Arc::clone(&s1);
                            let local_rc = Rc::clone(&local_rc);
                            async move {
                                s1.fetch_add(1, Ordering::SeqCst);
                                Ok(Json(a + local_rc.get() + 1))
                            }
                        })
                        .await?;
                    Ok(b)
                },
            })
            .await
        })
    });

    let job_id: Arc<str> = Arc::from("job-warm");
    let mut journal = make_journal("two-step", "5");

    let (res0, session) = driver
        .run_job(
            &eff_id(1, 0),
            0,
            Arc::clone(&job_id),
            journal.clone(),
            None,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::StepEffect(eff0)) = res0 else {
        panic!("expected StepEffect, got {res0:?}");
    };
    push_step_effect(&mut journal, eff0);
    assert_eq!(step0_runs.load(Ordering::SeqCst), 1);
    assert_eq!(step1_runs.load(Ordering::SeqCst), 0);

    let (res1, session) = driver
        .run_job(
            &eff_id(2, 0),
            1,
            Arc::clone(&job_id),
            journal.clone(),
            session,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::StepEffect(eff1)) = res1 else {
        panic!("expected second StepEffect, got {res1:?}");
    };
    push_step_effect(&mut journal, eff1);
    assert_eq!(step0_runs.load(Ordering::SeqCst), 1);
    assert_eq!(step1_runs.load(Ordering::SeqCst), 1);

    let (res2, _session) = driver
        .run_job(
            &eff_id(3, 0),
            2,
            Arc::clone(&job_id),
            journal.clone(),
            session,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::Success { value_json }) = res2 else {
        panic!("expected Success, got {res2:?}");
    };
    assert_eq!(value_json.as_ref(), "31");

    let cold_job_id: Arc<str> = Arc::from("job-cold");
    let mut cold_journal = make_journal("two-step", "5");
    let (res_cold_0, cold_session_0) = driver
        .run_job(
            &eff_id(10, 0),
            0,
            Arc::clone(&cold_job_id),
            cold_journal.clone(),
            None,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::StepEffect(cold_eff0)) = res_cold_0 else {
        panic!("expected cold StepEffect, got {res_cold_0:?}");
    };
    push_step_effect(&mut cold_journal, cold_eff0);
    drop(cold_session_0);
    assert_eq!(step0_runs.load(Ordering::SeqCst), 2);

    let (res_cold_1, cold_session_1) = driver
        .run_job(
            &eff_id(11, 0),
            1,
            Arc::clone(&cold_job_id),
            cold_journal.clone(),
            None,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::StepEffect(cold_eff1)) = res_cold_1 else {
        panic!("expected cold second StepEffect, got {res_cold_1:?}");
    };
    push_step_effect(&mut cold_journal, cold_eff1);
    assert_eq!(
        step0_runs.load(Ordering::SeqCst),
        2,
        "cold replay must not re-run committed step 0 closure"
    );
    assert_eq!(step1_runs.load(Ordering::SeqCst), 2);

    let (res_cold_2, _) = driver
        .run_job(
            &eff_id(12, 0),
            2,
            Arc::clone(&cold_job_id),
            cold_journal,
            cold_session_1,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::Success { value_json }) = res_cold_2 else {
        panic!("expected cold Success, got {res_cold_2:?}");
    };
    assert_eq!(value_json.as_ref(), "26");
}

#[tokio::test]
async fn nondeterministic_replay_and_panic_and_cancel_are_reported() {
    let driver = NativeHostDriver::new();
    let mode = Arc::new(AtomicUsize::new(0));
    let mode_cl = Arc::clone(&mode);
    driver.register_typed("mutating", move |cx, (): ()| {
        let mode_cl = Arc::clone(&mode_cl);
        Box::pin(async move {
            if mode_cl.load(Ordering::SeqCst) == 0 {
                let v: i64 = cx
                    .named_effect("original-step", || async { Ok(Json(7i64)) })
                    .await?;
                let next: i64 = cx
                    .named_effect("second-step", || async move { Ok(Json(v + 1)) })
                    .await?;
                Ok(next)
            } else {
                let v: i64 = cx
                    .named_effect("renamed-step", || async { Ok(Json(7i64)) })
                    .await?;
                Ok(v)
            }
        })
    });

    let job_id: Arc<str> = Arc::from("job-nondet");
    let mut journal = make_journal("mutating", "null");
    let (res0, session0) = driver
        .run_job(
            &eff_id(1, 0),
            0,
            Arc::clone(&job_id),
            journal.clone(),
            None,
            CancellationToken::new(),
        )
        .await;
    let Ok(job_events::JobRunResult::StepEffect(eff0)) = res0 else {
        panic!("expected StepEffect, got {res0:?}");
    };
    push_step_effect(&mut journal, eff0);
    drop(session0);

    mode.store(1, Ordering::SeqCst);
    let (res_diverge, _) = driver
        .run_job(
            &eff_id(2, 0),
            1,
            Arc::clone(&job_id),
            journal,
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(
            res_diverge,
            Err(job_events::JobRunResult::WflowErr(
                job_events::JobError::Terminal { .. }
            ))
        ),
        "expected terminal WflowErr on nondeterministic replay, got {res_diverge:?}"
    );

    driver.register_typed("panics", |_cx, (): ()| {
        Box::pin(async move {
            panic!("boom inside native workflow");
            #[expect(unreachable_code, reason = "type inference after panic")]
            Ok::<(), JobErrorX>(())
        })
    });
    let (res_panic, _) = driver
        .run_job(
            &eff_id(3, 0),
            0,
            Arc::from("job-panic"),
            make_journal("panics", "null"),
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(
            res_panic,
            Err(job_events::JobRunResult::WorkerErr(
                job_events::JobRunWorkerError::Other { .. }
            ))
        ),
        "expected WorkerErr on panic, got {res_panic:?}"
    );

    let (started_tx, started_rx) = async_channel::bounded::<()>(1);
    driver.register_typed("hangs", move |cx, (): ()| {
        let started_tx = started_tx.clone();
        Box::pin(async move {
            cx.effect(|| async move {
                started_tx.send(()).await.expect(ERROR_CHANNEL);
                std::future::pending::<Result<Json<()>, JobErrorX>>().await
            })
            .await
        })
    });
    let cancel_token = CancellationToken::new();
    let cancel_trigger = cancel_token.clone();
    tokio::spawn(async move {
        started_rx.recv().await.expect(ERROR_CHANNEL);
        cancel_trigger.cancel();
    });
    let (res_cancel, _) = driver
        .run_job(
            &eff_id(4, 0),
            0,
            Arc::from("job-cancel"),
            make_journal("hangs", "null"),
            None,
            cancel_token,
        )
        .await;
    assert!(
        matches!(res_cancel, Ok(job_events::JobRunResult::Aborted)),
        "expected Aborted on cancel, got {res_cancel:?}"
    );
}
