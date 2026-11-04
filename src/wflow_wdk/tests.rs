use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

use futures::executor::block_on;

use crate::interlude::*;
use crate::*;

#[derive(Default)]
struct RecordedStep {
    meta: Option<StepMeta>,
    value_json: Option<String>,
}

struct MockBackend {
    cursor: Cell<StepId>,
    active_step: Cell<Option<StepId>>,
    steps: RefCell<Vec<RecordedStep>>,
    incoming_messages: RefCell<Vec<String>>,
    /// Interrupts the forward boundary at this step with the cause, the way a
    /// host does once the job entered cleanup. Defer steps are never
    /// interrupted.
    interrupt_at: RefCell<Option<(StepId, types::CleanupCause)>>,
    /// The cause the host recorded, if `begin_cleanup` was called.
    cleanup: RefCell<Option<types::CleanupCause>>,
}

impl MockBackend {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            cursor: Cell::new(0),
            active_step: Cell::new(None),
            steps: RefCell::new(Vec::new()),
            incoming_messages: RefCell::new(Vec::new()),
            interrupt_at: RefCell::new(None),
            cleanup: RefCell::new(None),
        })
    }

    fn reset_cursor_for_replay(&self) {
        self.cursor.set(0);
        self.active_step.set(None);
    }

    fn push_message(&self, payload_json: impl Into<String>) {
        self.incoming_messages
            .borrow_mut()
            .push(payload_json.into());
    }

    fn committed_metas(&self) -> Vec<StepMeta> {
        self.steps
            .borrow()
            .iter()
            .filter(|step| step.value_json.is_some())
            .map(|step| step.meta.clone().expect("committed step has meta"))
            .collect()
    }
}

impl WflowBackend for MockBackend {
    fn next_step(&self, meta: StepMeta) -> LocalBoxFuture<'_, Result<StepState, BackendError>> {
        Box::pin(async move {
            if self.active_step.get().is_some() {
                return Err(BackendError::Protocol(
                    "concurrent steps not allowed".into(),
                ));
            }
            let step_id = self.cursor.get();
            if meta.kind != StepKind::Defer
                && let Some((at, cause)) = &*self.interrupt_at.borrow()
                && *at == step_id
            {
                return Err(BackendError::Interrupted(cause.clone()));
            }
            let mut steps = self.steps.borrow_mut();
            if let Some(entry) = steps.get(step_id as usize) {
                if let Some(expected) = &entry.meta
                    && expected != &meta
                {
                    return Err(BackendError::NondeterministicReplay {
                        step_id,
                        expected: expected.clone(),
                        found: meta,
                    });
                }
                if let Some(value_json) = &entry.value_json {
                    self.cursor.set(step_id + 1);
                    return Ok(StepState::Completed(CompletedStepState {
                        id: step_id,
                        value_json: value_json.clone(),
                    }));
                }
            }
            while steps.len() <= step_id as usize {
                steps.push(RecordedStep::default());
            }
            steps[step_id as usize].meta = Some(meta);
            self.active_step.set(Some(step_id));
            Ok(StepState::Active(ActiveStepState { id: step_id }))
        })
    }

    fn persist_step<'a>(
        &'a self,
        step_id: StepId,
        value_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<(), BackendError>> {
        Box::pin(async move {
            if self.active_step.take() != Some(step_id) {
                return Err(BackendError::Protocol("given step_id is not active".into()));
            }
            self.steps.borrow_mut()[step_id as usize].value_json = Some(value_json.to_owned());
            self.cursor.set(step_id + 1);
            Ok(())
        })
    }

    fn sleep(
        &self,
        step_id: StepId,
        _duration_ms: u64,
    ) -> LocalBoxFuture<'_, Result<(), BackendError>> {
        Box::pin(async move {
            if self.active_step.take() != Some(step_id) {
                return Err(BackendError::Protocol("given step_id is not active".into()));
            }
            self.steps.borrow_mut()[step_id as usize].value_json = Some("null".into());
            self.cursor.set(step_id + 1);
            Ok(())
        })
    }

    fn recv_message(&self, step_id: StepId) -> LocalBoxFuture<'_, Result<String, BackendError>> {
        Box::pin(async move {
            if self.active_step.take() != Some(step_id) {
                return Err(BackendError::Protocol("given step_id is not active".into()));
            }
            let mut inbox = self.incoming_messages.borrow_mut();
            if inbox.is_empty() {
                return Err(BackendError::Protocol("no message queued".into()));
            }
            let msg = inbox.remove(0);
            self.steps.borrow_mut()[step_id as usize].value_json = Some(msg.clone());
            self.cursor.set(step_id + 1);
            Ok(msg)
        })
    }

    fn begin_cleanup<'a>(
        &'a self,
        error_json: &'a str,
    ) -> LocalBoxFuture<'a, Result<types::CleanupCause, BackendError>> {
        Box::pin(async move {
            let mut cleanup = self.cleanup.borrow_mut();
            let cause = cleanup
                .get_or_insert_with(|| types::CleanupCause::Failed {
                    error: error_json.to_owned(),
                })
                .clone();
            Ok(cause)
        })
    }
}

fn sample_run_args(key: &str, args_json: &str) -> types::RunArgs {
    types::RunArgs {
        ctx: types::JobCtx {
            job_id: "job-1".into(),
        },
        wflow_key: key.into(),
        args_json: args_json.into(),
    }
}

#[test]
fn effect_sleep_recv_and_replay_skip_closures() {
    block_on(async {
        let backend = MockBackend::new();
        backend.push_message("21");
        let effect_calls = Rc::new(Cell::new(0usize));

        for pass in 0..2 {
            backend.reset_cursor_for_replay();
            let calls = Rc::clone(&effect_calls);
            let res = route_wflows!(
                sample_run_args("calc", r#"{"base":10}"#),
                Rc::clone(&backend) as Rc<dyn WflowBackend>,
                {
                    "calc" => |cx, args: serde_json::Value| async move {
                        let base = args["base"].as_i64().unwrap();
                        let step1: i64 = cx
                            .named_effect("compute-base", || async move {
                                calls.set(calls.get() + 1);
                                Ok(Json(base * 2))
                            })
                            .await?;
                        cx.named_sleep("pause", Duration::from_millis(5)).await?;
                        let Json(msg): Json<i64> = cx.named_recv("wait-input").await?;
                        Ok(step1 + msg)
                    },
                }
            )
            .await;
            assert_eq!(res, Ok("41".to_owned()), "pass {pass} failed");
        }
        assert_eq!(effect_calls.get(), 1);
    });
}

#[test]
fn replay_decode_and_nondeterminism_are_terminal_errors() {
    block_on(async {
        let backend = MockBackend::new();
        let mut cx = WflowCtx::new(
            types::JobCtx {
                job_id: "job-diverge".into(),
            },
            Rc::clone(&backend) as Rc<dyn WflowBackend>,
        );

        let v: String = cx
            .named_effect("step-a", || async { Ok(Json("not-an-int".to_owned())) })
            .await
            .unwrap();
        assert_eq!(v, "not-an-int");

        backend.reset_cursor_for_replay();
        let mut cx_bad_type = WflowCtx::new(
            types::JobCtx {
                job_id: "job-diverge".into(),
            },
            Rc::clone(&backend) as Rc<dyn WflowBackend>,
        );
        let decode_err = cx_bad_type
            .named_effect::<_, i64, _>("step-a", || async { Ok(Json(1)) })
            .await
            .unwrap_err();
        assert!(
            matches!(decode_err, JobErrorX::Terminal(_)),
            "replay decode failure must be Terminal, got {decode_err:?}"
        );

        backend.reset_cursor_for_replay();
        let mut cx_bad_name = WflowCtx::new(
            types::JobCtx {
                job_id: "job-diverge".into(),
            },
            Rc::clone(&backend) as Rc<dyn WflowBackend>,
        );
        let name_err = cx_bad_name
            .named_effect::<_, String, _>("step-b", || async { Ok(Json("x".into())) })
            .await
            .unwrap_err();
        assert!(
            matches!(name_err, JobErrorX::Terminal(_)),
            "nondeterministic step name must be Terminal, got {name_err:?}"
        );

        backend.reset_cursor_for_replay();
        let mut cx_bad_kind = WflowCtx::new(
            types::JobCtx {
                job_id: "job-diverge".into(),
            },
            Rc::clone(&backend) as Rc<dyn WflowBackend>,
        );
        let kind_err = cx_bad_kind
            .named_sleep("step-a", Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(
            matches!(kind_err, JobErrorX::Terminal(_)),
            "nondeterministic step kind must be Terminal, got {kind_err:?}"
        );
    });
}

/// A saga that acquires one resource per effect and registers its release,
/// then ends as `outcome` says; `events` records what ran.
type Events = Rc<RefCell<Vec<String>>>;
type SagaHandler = Box<
    dyn for<'a> FnOnce(
        &'a mut WflowCtx,
        &'a str,
        &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u64, JobErrorX>> + 'a>>,
>;

fn saga(
    events: &Events,
    outcome: Result<(), JobErrorX>,
    second_release: impl FnOnce() -> Result<(), JobErrorX> + 'static,
) -> SagaHandler {
    let events = Rc::clone(events);
    Box::new(move |cx, _key, _args| {
        Box::pin(async move {
            let ev = Rc::clone(&events);
            let first: u64 = cx
                .named_effect("acquire-first", || async { Ok(Json(1)) })
                .await?;
            cx.defer("release-first", move || async move {
                ev.borrow_mut().push("release-first".into());
                Ok(())
            })?;
            let ev = Rc::clone(&events);
            let second: u64 = cx
                .named_effect("acquire-second", || async { Ok(Json(2)) })
                .await?;
            cx.defer("release-second", move || async move {
                ev.borrow_mut().push("release-second".into());
                second_release()
            })?;
            let ev = Rc::clone(&events);
            let third: u64 = cx
                .named_effect("acquire-third", || async move {
                    ev.borrow_mut().push("acquire-third".into());
                    Ok(Json(3))
                })
                .await?;
            outcome?;
            Ok(first + second + third)
        })
    })
}

fn run_saga(
    backend: &Rc<MockBackend>,
    events: &Events,
    outcome: Result<(), JobErrorX>,
    second_release: impl FnOnce() -> Result<(), JobErrorX> + 'static,
) -> types::JobResult {
    backend.reset_cursor_for_replay();
    block_on(run_wflow(
        sample_run_args("saga", "{}"),
        Rc::clone(backend) as Rc<dyn WflowBackend>,
        saga(events, outcome, second_release),
    ))
}

fn failure(result: &types::JobResult) -> types::JobFailure {
    let Err(types::JobError::Terminal(json)) = result else {
        panic!("expected a terminal failure, got {result:?}");
    };
    serde_json::from_str(json).expect("terminal failure is a classified JobFailure")
}

#[test]
fn success_disarms_defers() {
    let backend = MockBackend::new();
    let events: Events = default();
    let res = run_saga(&backend, &events, Ok(()), || Ok(()));
    assert_eq!(res, Ok("6".to_owned()));
    assert_eq!(&*events.borrow(), &["acquire-third"]);
    assert_eq!(*backend.cleanup.borrow(), None);
}

#[test]
fn terminal_error_records_cause_then_runs_defers_in_reverse() {
    let backend = MockBackend::new();
    let events: Events = default();
    let res = run_saga(
        &backend,
        &events,
        Err(JobErrorX::Terminal(ferr!("boom"))),
        || Ok(()),
    );
    let Err(types::JobError::Terminal(error)) = &res else {
        panic!("expected the forward error, got {res:?}");
    };
    assert!(error.contains("boom"));
    assert_eq!(
        *backend.cleanup.borrow(),
        Some(types::CleanupCause::Failed {
            error: error.clone()
        }),
        "the cause is recorded before any defer runs"
    );
    assert_eq!(
        &*events.borrow(),
        &["acquire-third", "release-second", "release-first"]
    );
    let kinds: Vec<StepKind> = backend
        .committed_metas()
        .iter()
        .map(|meta| meta.kind)
        .collect();
    assert_eq!(
        kinds,
        [
            StepKind::Effect,
            StepKind::Effect,
            StepKind::Effect,
            StepKind::Defer,
            StepKind::Defer
        ]
    );
}

#[test]
fn terminal_error_without_defers_settles_directly() {
    let backend = MockBackend::new();
    let res = block_on(run_wflow(
        sample_run_args("plain", "{}"),
        Rc::clone(&backend) as Rc<dyn WflowBackend>,
        |_cx, _key, _args| {
            Box::pin(async move { Err::<(), JobErrorX>(JobErrorX::Terminal(ferr!("boom"))) })
        },
    ));
    assert!(matches!(res, Err(types::JobError::Terminal(_))));
    assert_eq!(*backend.cleanup.borrow(), None);
}

#[test]
fn interruption_runs_defers_and_settles_as_cancelled() {
    let backend = MockBackend::new();
    let events: Events = default();
    *backend.interrupt_at.borrow_mut() = Some((
        2,
        types::CleanupCause::Cancelled {
            reason: "operator".into(),
        },
    ));
    let res = run_saga(&backend, &events, Ok(()), || Ok(()));
    assert_eq!(
        failure(&res),
        types::JobFailure::Cancelled {
            reason: "operator".into()
        }
    );
    assert_eq!(
        &*events.borrow(),
        &["release-second", "release-first"],
        "the interrupted effect never ran; defers ran in reverse"
    );
    assert_eq!(*backend.cleanup.borrow(), None, "a cancel needs no Fail");
}

#[test]
fn transient_defer_failure_propagates_and_replay_resumes_at_it() {
    let backend = MockBackend::new();
    let events: Events = default();
    let res = run_saga(
        &backend,
        &events,
        Err(JobErrorX::Terminal(ferr!("boom"))),
        || Err(JobErrorX::Transient(ferr!("flaky"))),
    );
    assert!(matches!(res, Err(types::JobError::Transient(_))), "{res:?}");
    assert_eq!(&*events.borrow(), &["acquire-third", "release-second"]);
    assert_eq!(
        backend.committed_metas().len(),
        3,
        "the failed defer is not committed"
    );

    events.borrow_mut().clear();
    let res = run_saga(
        &backend,
        &events,
        Err(JobErrorX::Terminal(ferr!("boom"))),
        || Ok(()),
    );
    assert!(matches!(res, Err(types::JobError::Terminal(_))));
    assert_eq!(
        &*events.borrow(),
        &["release-second", "release-first"],
        "replay skips committed effects and resumes at the outstanding defer"
    );
}

#[test]
fn terminal_defer_failure_settles_as_cleanup_failed() {
    let backend = MockBackend::new();
    let events: Events = default();
    let res = run_saga(
        &backend,
        &events,
        Err(JobErrorX::Terminal(ferr!("boom"))),
        || Err(JobErrorX::Terminal(ferr!("release broke"))),
    );
    let types::JobFailure::CleanupFailed {
        cause: types::CleanupCause::Failed { error },
        defer,
        error: defer_error,
    } = failure(&res)
    else {
        panic!("expected CleanupFailed, got {res:?}");
    };
    assert!(error.contains("boom"));
    assert_eq!(defer, "release-second");
    assert!(defer_error.contains("release broke"));
    assert_eq!(
        &*events.borrow(),
        &["acquire-third", "release-second"],
        "the remaining defer stays outstanding"
    );
}
