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
}

impl MockBackend {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            cursor: Cell::new(0),
            active_step: Cell::new(None),
            steps: RefCell::new(Vec::new()),
            incoming_messages: RefCell::new(Vec::new()),
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
            let message = inbox.remove(0);
            self.steps.borrow_mut()[step_id as usize].value_json = Some(message.clone());
            self.cursor.set(step_id + 1);
            Ok(message)
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
            let result = route_wflows!(
                sample_run_args("calc", r#"{"base":10}"#),
                Rc::clone(&backend) as Rc<dyn WflowBackend>,
                {
                    "calc" => |cx, args: serde_json::Value| async move {
                        let base = args["base"].as_i64().unwrap();
                        let first: i64 = cx
                            .named_effect("compute-base", || async move {
                                calls.set(calls.get() + 1);
                                Ok(Json(base * 2))
                            })
                            .await?;
                        cx.named_sleep("pause", Duration::from_millis(5)).await?;
                        let Json(message): Json<i64> = cx.named_recv("wait-input").await?;
                        Ok(first + message)
                    },
                }
            )
            .await;
            assert_eq!(result, Ok("41".to_owned()), "pass {pass} failed");
        }
        assert_eq!(effect_calls.get(), 1);
    });
}

#[test]
fn replay_decode_and_boundary_divergence_are_terminal() {
    block_on(async {
        let backend = MockBackend::new();
        let mut cx = WflowCtx::new(
            types::JobCtx {
                job_id: "job-diverge".into(),
            },
            Rc::clone(&backend) as Rc<dyn WflowBackend>,
        );

        let value: String = cx
            .named_effect("step-a", || async { Ok(Json("not-an-int".to_owned())) })
            .await
            .unwrap();
        assert_eq!(value, "not-an-int");

        backend.reset_cursor_for_replay();
        let mut bad_type = cx.clone();
        let decode_error = bad_type
            .named_effect::<_, i64, _>("step-a", || async { Ok(Json(1)) })
            .await
            .unwrap_err();
        assert!(matches!(decode_error, JobErrorX::Terminal(_)));

        backend.reset_cursor_for_replay();
        let mut bad_name = cx.clone();
        let name_error = bad_name
            .named_effect::<_, String, _>("step-b", || async { Ok(Json("x".into())) })
            .await
            .unwrap_err();
        assert!(matches!(name_error, JobErrorX::Terminal(_)));

        backend.reset_cursor_for_replay();
        let kind_error = cx
            .named_sleep("step-a", Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(matches!(kind_error, JobErrorX::Terminal(_)));
    });
}

#[test]
fn nested_boundary_entry_is_terminal() {
    block_on(async {
        let backend = MockBackend::new();
        let mut cx = WflowCtx::new(
            types::JobCtx {
                job_id: "job-nested".into(),
            },
            backend as Rc<dyn WflowBackend>,
        );
        let mut nested = cx.clone();

        let error = cx
            .effect(|| async move {
                nested.sleep(Duration::from_millis(1)).await?;
                Ok(Json(1_u64))
            })
            .await
            .unwrap_err();

        assert!(matches!(error, JobErrorX::Terminal(_)));
        assert!(format!("{error:?}").contains("nested workflow step entry"));
    });
}

#[test]
fn run_wflow_returns_direct_success_terminal_and_transient_outcomes() {
    let backend = MockBackend::new();
    let success = block_on(run_wflow(
        sample_run_args("direct", "{}"),
        Rc::clone(&backend) as Rc<dyn WflowBackend>,
        |_cx, _key, _args| Box::pin(async { Ok::<_, JobErrorX>(42_u64) }),
    ));
    assert_eq!(success, Ok("42".into()));

    let terminal = block_on(run_wflow(
        sample_run_args("direct", "{}"),
        Rc::clone(&backend) as Rc<dyn WflowBackend>,
        |_cx, _key, _args| Box::pin(async { Err::<(), _>(JobErrorX::Terminal(ferr!("terminal"))) }),
    ));
    let Err(types::JobError::Terminal(error)) = terminal else {
        panic!("expected terminal outcome");
    };
    assert!(error.contains("terminal"));

    let transient = block_on(run_wflow(
        sample_run_args("direct", "{}"),
        backend as Rc<dyn WflowBackend>,
        |_cx, _key, _args| {
            Box::pin(async { Err::<(), _>(JobErrorX::Transient(ferr!("transient"))) })
        },
    ));
    let Err(types::JobError::Transient(error)) = transient else {
        panic!("expected transient outcome");
    };
    assert_eq!(error.retry_policy, None);
    assert!(error.error_json.contains("transient"));
}

#[test]
fn retry_policy_delay_is_bounded_and_saturating() {
    use types::RetryPolicy;

    assert_eq!(RetryPolicy::Immediate.delay_ms(5), 0);
    assert_eq!(RetryPolicy::Fixed { delay_ms: 25 }.delay_ms(5), 25);
    let policy = RetryPolicy::CappedExponential {
        base_ms: 100,
        cap_ms: 1_000,
    };
    assert_eq!(policy.delay_ms(0), 100);
    assert_eq!(policy.delay_ms(3), 800);
    assert_eq!(policy.delay_ms(4), 1_000);
    assert_eq!(policy.delay_ms(u64::MAX), 1_000);
}
