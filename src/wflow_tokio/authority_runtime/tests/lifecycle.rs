use std::collections::VecDeque;
use std::sync::Mutex;

use wflow_wdk::StepKind;
use wflow_wdk::StepMeta;

use super::*;
use crate::authority_runtime::session::Host;
use crate::authority_runtime::session::Session;

#[derive(Default)]
struct DropProbe {
    dropped: AtomicU64,
    changed: Notify,
}

impl DropProbe {
    async fn wait_for(&self, count: u64) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.dropped.load(Ordering::SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }
}

struct ScriptSession {
    yields: VecDeque<SessionYield>,
    next_gate: Option<Arc<Gate>>,
    resume: bool,
    deliver: bool,
    dropped: Option<Arc<DropProbe>>,
}

impl ScriptSession {
    fn new(yields: impl IntoIterator<Item = SessionYield>) -> Self {
        Self {
            yields: yields.into_iter().collect(),
            next_gate: None,
            resume: true,
            deliver: true,
            dropped: None,
        }
    }

    fn blocked(mut self, gate: Arc<Gate>) -> Self {
        self.next_gate = Some(gate);
        self
    }

    fn resume_fails(mut self) -> Self {
        self.resume = false;
        self
    }

    fn deliver_fails(mut self) -> Self {
        self.deliver = false;
        self
    }

    fn observed_drop(mut self, dropped: Arc<DropProbe>) -> Self {
        self.dropped = Some(dropped);
        self
    }
}

impl Drop for ScriptSession {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.dropped.fetch_add(1, Ordering::SeqCst);
            dropped.changed.notify_waiters();
        }
    }
}

#[async_trait]
impl Session for ScriptSession {
    async fn next_yield(&mut self) -> Option<SessionYield> {
        if let Some(gate) = self.next_gate.take() {
            gate.pass().await;
        }
        self.yields.pop_front()
    }

    fn resume(&self) -> bool {
        self.resume
    }

    fn deliver(&self, _value_json: Arc<str>) -> bool {
        self.deliver
    }
}

enum SpawnPlan {
    Session(Box<dyn Session>),
    Blocked {
        gate: Arc<Gate>,
        session: Box<dyn Session>,
    },
    Fail,
    Panic,
}

#[derive(Default)]
struct ScriptHost {
    plans: Mutex<HashMap<String, VecDeque<SpawnPlan>>>,
    replay_lengths: Mutex<HashMap<String, Vec<usize>>>,
    forgotten: Mutex<Vec<String>>,
    registered: AtomicBool,
}

impl ScriptHost {
    fn registered() -> Arc<Self> {
        Arc::new(Self {
            registered: AtomicBool::new(true),
            ..Self::default()
        })
    }

    fn push(&self, wflow: &str, plan: SpawnPlan) {
        self.plans
            .lock()
            .unwrap()
            .entry(wflow.to_owned())
            .or_default()
            .push_back(plan);
    }

    fn replay_lengths(&self, wflow: &str) -> Vec<usize> {
        self.replay_lengths
            .lock()
            .unwrap()
            .get(wflow)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl Host for ScriptHost {
    fn contains_wflow(&self, _key: &str) -> bool {
        self.registered.load(Ordering::SeqCst)
    }

    async fn spawn(
        &self,
        _job_id: Arc<str>,
        wflow_key: &str,
        _args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Res<Box<dyn Session>> {
        self.replay_lengths
            .lock()
            .unwrap()
            .entry(wflow_key.to_owned())
            .or_default()
            .push(replay.len());
        let plan = self
            .plans
            .lock()
            .unwrap()
            .get_mut(wflow_key)
            .and_then(VecDeque::pop_front)
            .unwrap_or(SpawnPlan::Fail);
        match plan {
            SpawnPlan::Session(session) => Ok(session),
            SpawnPlan::Blocked { gate, session } => {
                gate.pass().await;
                Ok(session)
            }
            SpawnPlan::Fail => Err(ferr!("host unavailable")),
            SpawnPlan::Panic => panic!("host spawn panic"),
        }
    }

    fn forget_job(&self, job_id: &str) {
        self.forgotten.lock().unwrap().push(job_id.to_owned());
    }
}

async fn start_script(host: Arc<ScriptHost>) -> AuthorityRuntime {
    let store = MemoryStore::default();
    AuthorityRuntime::start(store.writer().unwrap(), 0, host)
        .await
        .unwrap()
}

async fn start_script_counted(
    store: &MemoryStore,
    host: Arc<ScriptHost>,
) -> (AuthorityRuntime, watch::Receiver<u64>) {
    let (commits, counter) = watch::channel(0);
    let writer = CountingWriter {
        inner: store.writer().unwrap(),
        commits,
    };
    let runtime = AuthorityRuntime::start(writer, 0, host).await.unwrap();
    (runtime, counter)
}

/// A session that reached `steps` durable boundaries and returned `value`.
fn complete(steps: u64, value: u64) -> SessionYield {
    SessionYield::Complete {
        steps,
        result: ok(value),
    }
}

fn persist(value: &str) -> SessionYield {
    SessionYield::PersistStep {
        step_id: 0,
        start_at: Timestamp::now(),
        end_at: Timestamp::now(),
        value_json: Arc::from(value),
        meta: StepMeta::new(StepKind::Effect, None),
    }
}

fn receive() -> SessionYield {
    SessionYield::WaitStep {
        step_id: 0,
        start_at: Timestamp::now(),
        deets: WaitYield::Message { wait_id: 0 },
        meta: StepMeta::new(StepKind::Recv, None),
    }
}

#[test]
fn submission_uses_canonical_finite_defaults() {
    let submission = Submission::new("job", "workflow", "null");
    assert_eq!(
        submission.policy,
        RetryPolicy::CappedExponential {
            base_ms: 100,
            cap_ms: 30_000,
        }
    );
    assert_eq!(
        submission.budget,
        RetryBudget {
            max_retries: Some(8),
            max_restarts: Some(3),
        }
    );
}

#[tokio::test]
async fn slow_spawn_does_not_block_unrelated_work_and_late_session_is_dropped() {
    let host = ScriptHost::registered();
    let spawn_gate = Arc::new(Gate::default());
    spawn_gate.close();
    let dropped = Arc::new(DropProbe::default());
    host.push(
        "slow",
        SpawnPlan::Blocked {
            gate: Arc::clone(&spawn_gate),
            session: Box::new(
                ScriptSession::new([complete(0, 1)]).observed_drop(Arc::clone(&dropped)),
            ),
        },
    );
    host.push(
        "fast",
        SpawnPlan::Session(Box::new(ScriptSession::new([complete(0, 2)]))),
    );
    let runtime = start_script(Arc::clone(&host)).await;

    runtime
        .submit(Submission::new("slow", "slow", "null"))
        .await
        .unwrap();
    runtime
        .submit(Submission::new("fast", "fast", "null"))
        .await
        .unwrap();
    assert_eq!(runtime.await_result("fast").await, Some(ok(2)));
    runtime.cancel("slow", "stop").await.unwrap();
    assert!(matches!(
        runtime.await_result("slow").await,
        Some(Err(types::JobError::Terminal(_)))
    ));

    spawn_gate.open();
    dropped.wait_for(1).await;
    assert_eq!(host.replay_lengths("slow"), vec![0]);
    assert!(
        transitions(&runtime, "slow")
            .await
            .iter()
            .all(|event| !matches!(event, authority::Transition::Revoked { .. })),
        "a terminal late spawn must not consume restart budget"
    );
    runtime.stop().await;
}

#[tokio::test]
async fn host_spawn_panic_propagates_without_spending_restart_budget() {
    let host = ScriptHost::registered();
    host.push("panics", SpawnPlan::Panic);
    let runtime = start_script(Arc::clone(&host)).await;
    runtime
        .submit(Submission::new("job", "panics", "null"))
        .await
        .unwrap();

    let panic = runtime
        .actor
        .clone()
        .await
        .expect_err("a Host::spawn panic must fail the actor");
    assert!(panic.is_panic());
    assert!(panic.to_string().contains("host spawn panic"));
    let history = transitions(&runtime, "job").await;
    assert!(
        history
            .iter()
            .all(|event| !matches!(event, authority::Transition::Revoked { .. }))
    );
}

#[tokio::test]
async fn concurrent_stop_survives_cancellation_of_first_waiter() {
    let host = ScriptHost::registered();
    let gate = Arc::new(Gate::default());
    gate.close();
    let dropped = Arc::new(DropProbe::default());
    host.push(
        "blocked",
        SpawnPlan::Blocked {
            gate: Arc::clone(&gate),
            session: Box::new(
                ScriptSession::new([complete(0, 1)]).observed_drop(Arc::clone(&dropped)),
            ),
        },
    );
    let runtime = start_script(host).await;
    runtime
        .submit(Submission::new("job", "blocked", "null"))
        .await
        .unwrap();
    gate.entered.notified().await;

    {
        let mut first_stop = Box::pin(runtime.stop());
        assert!(futures::poll!(first_stop.as_mut()).is_pending());
    }
    tokio::join!(runtime.stop(), runtime.stop());
    dropped.wait_for(1).await;
    assert!(runtime.reader.snapshot().await.unwrap().is_some());
}

#[tokio::test]
async fn stop_aborts_a_pending_host_spawn_before_completing() {
    let host = ScriptHost::registered();
    let spawn_gate = Arc::new(Gate::default());
    spawn_gate.close();
    let dropped = Arc::new(DropProbe::default());
    host.push(
        "blocked",
        SpawnPlan::Blocked {
            gate: Arc::clone(&spawn_gate),
            session: Box::new(
                ScriptSession::new([complete(0, 1)]).observed_drop(Arc::clone(&dropped)),
            ),
        },
    );
    let runtime = start_script(Arc::clone(&host)).await;
    runtime
        .submit(Submission::new("job", "blocked", "null"))
        .await
        .unwrap();

    spawn_gate.entered.notified().await;
    runtime.stop().await;
    dropped.wait_for(1).await;
    assert_eq!(host.replay_lengths("blocked"), vec![0]);
}

#[tokio::test]
async fn cancellation_aborts_a_session_parked_in_next_yield() {
    let host = ScriptHost::registered();
    let yield_gate = Arc::new(Gate::default());
    yield_gate.close();
    let dropped = Arc::new(DropProbe::default());
    host.push(
        "blocked",
        SpawnPlan::Session(Box::new(
            ScriptSession::new([complete(0, 1)])
                .blocked(Arc::clone(&yield_gate))
                .observed_drop(Arc::clone(&dropped)),
        )),
    );
    let runtime = start_script(host).await;
    runtime
        .submit(Submission::new("job", "blocked", "null"))
        .await
        .unwrap();

    runtime.cancel("job", "stop").await.unwrap();
    dropped.wait_for(1).await;
    assert!(matches!(
        runtime.await_result("job").await,
        Some(Err(types::JobError::Terminal(_)))
    ));
    yield_gate.open();
    runtime.stop().await;
}

#[tokio::test]
async fn cancellation_drops_a_parked_wait_and_rejects_late_delivery() {
    let host = ScriptHost::registered();
    let dropped = Arc::new(DropProbe::default());
    host.push(
        "waiting",
        SpawnPlan::Session(Box::new(
            ScriptSession::new([receive()]).observed_drop(Arc::clone(&dropped)),
        )),
    );
    let store = MemoryStore::default();
    let (runtime, mut commits) = start_script_counted(&store, host).await;
    runtime
        .submit(Submission::new("job", "waiting", "null"))
        .await
        .unwrap();
    committed(&mut commits, 4).await;

    runtime.cancel("job", "stop").await.unwrap();
    dropped.wait_for(1).await;
    assert_eq!(
        runtime.send_message("job", "late", "1").await,
        Err(SendError::AlreadyTerminal)
    );
    runtime.stop().await;
}

#[tokio::test]
async fn cancellation_clears_a_durable_retry_deadline() {
    let host = ScriptHost::registered();
    host.push(
        "retrying",
        SpawnPlan::Session(Box::new(ScriptSession::new([SessionYield::Complete {
            steps: 0,
            result: Err(types::JobError::Transient(types::TransientJobError {
                retry_policy: None,
                error_json: "retry".into(),
            })),
        }]))),
    );
    let store = MemoryStore::default();
    let (runtime, mut commits) = start_script_counted(&store, host).await;
    runtime
        .submit(
            Submission::new("job", "retrying", "null")
                .policy(RetryPolicy::Fixed { delay_ms: 60_000 }),
        )
        .await
        .unwrap();
    committed(&mut commits, 4).await;

    runtime.cancel("job", "stop").await.unwrap();
    assert!(matches!(
        runtime.await_result("job").await,
        Some(Err(types::JobError::Terminal(_)))
    ));
    assert_eq!(host_spawn_count(&runtime, "job").await, 1);
    runtime.stop().await;
}

async fn host_spawn_count(runtime: &AuthorityRuntime, key: &str) -> usize {
    transitions(runtime, key)
        .await
        .iter()
        .filter(|event| matches!(event, authority::Transition::Granted { .. }))
        .count()
}

#[tokio::test]
async fn current_session_loss_is_revoked_and_replayed() {
    let host = ScriptHost::registered();
    host.push("loss", SpawnPlan::Session(Box::new(ScriptSession::new([]))));
    host.push(
        "loss",
        SpawnPlan::Session(Box::new(ScriptSession::new([complete(0, 7)]))),
    );
    let runtime = start_script(Arc::clone(&host)).await;
    runtime
        .submit(Submission::new("job", "loss", "null"))
        .await
        .unwrap();

    assert_eq!(runtime.await_result("job").await, Some(ok(7)));
    let history = transitions(&runtime, "job").await;
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event, authority::Transition::Revoked { .. }))
            .count(),
        1
    );
    assert_eq!(host.replay_lengths("loss"), vec![0, 0]);
    runtime.stop().await;
}

#[tokio::test]
async fn resume_loss_after_step_commit_replays_durable_progress() {
    let host = ScriptHost::registered();
    host.push(
        "resume",
        SpawnPlan::Session(Box::new(ScriptSession::new([persist("1")]).resume_fails())),
    );
    host.push(
        "resume",
        SpawnPlan::Session(Box::new(ScriptSession::new([complete(1, 9)]))),
    );
    let runtime = start_script(Arc::clone(&host)).await;
    runtime
        .submit(Submission::new("job", "resume", "null"))
        .await
        .unwrap();

    assert_eq!(runtime.await_result("job").await, Some(ok(9)));
    assert_eq!(host.replay_lengths("resume"), vec![0, 1]);
    runtime.stop().await;
}

#[tokio::test]
async fn deliver_loss_after_wait_resolution_replays_durable_progress() {
    let host = ScriptHost::registered();
    host.push(
        "deliver",
        SpawnPlan::Session(Box::new(ScriptSession::new([receive()]).deliver_fails())),
    );
    host.push(
        "deliver",
        SpawnPlan::Session(Box::new(ScriptSession::new([complete(1, 11)]))),
    );
    let runtime = start_script(Arc::clone(&host)).await;
    runtime
        .submit(Submission::new("job", "deliver", "null"))
        .await
        .unwrap();
    runtime.send_message("job", "message", "3").await.unwrap();

    assert_eq!(runtime.await_result("job").await, Some(ok(11)));
    assert_eq!(host.replay_lengths("deliver"), vec![0, 1]);
    runtime.stop().await;
}

#[tokio::test]
async fn repeated_spawn_loss_exhausts_the_finite_restart_budget() {
    let host = ScriptHost::registered();
    for _ in 0..4 {
        host.push("missing-host", SpawnPlan::Fail);
    }
    let runtime = start_script(host).await;
    runtime
        .submit(Submission::new("job", "missing-host", "null"))
        .await
        .unwrap();

    let Some(Err(types::JobError::Terminal(failure))) = runtime.await_result("job").await else {
        panic!("spawn loss did not settle terminally");
    };
    let failure: JobFailure = serde_json::from_str(&failure).unwrap();
    assert_eq!(
        failure,
        JobFailure::RestartsExhausted {
            restarts: 4,
            max_restarts: 3,
        }
    );
    runtime.stop().await;
}

#[tokio::test]
async fn result_waiters_race_permanent_receipt_publication() {
    let host = ScriptHost::registered();
    let gate = Arc::new(Gate::default());
    gate.close();
    host.push(
        "waiter",
        SpawnPlan::Session(Box::new(
            ScriptSession::new([complete(0, 13)]).blocked(Arc::clone(&gate)),
        )),
    );
    let runtime = start_script(host).await;
    let (_, handle) = runtime
        .submit_handle(Submission::new("job", "waiter", "null"))
        .await
        .unwrap();
    let waiting = tokio::spawn({
        let handle = handle.clone();
        async move { handle.await_result().await }
    });

    gate.open();
    assert_eq!(waiting.await.unwrap(), Some(ok(13)));
    assert_eq!(handle.await_result().await, Some(ok(13)));
    runtime.stop().await;
}

#[tokio::test]
async fn recovery_rejects_an_unregistered_workflow() {
    let store = MemoryStore::default();
    let host = ScriptHost::registered();
    let gate = Arc::new(Gate::default());
    gate.close();
    host.push(
        "configured",
        SpawnPlan::Session(Box::new(
            ScriptSession::new([complete(0, 1)]).blocked(Arc::clone(&gate)),
        )),
    );
    let runtime_host = Arc::clone(&host);
    let runtime = AuthorityRuntime::start(store.writer().unwrap(), 0, runtime_host)
        .await
        .unwrap();
    runtime
        .submit(Submission::new("job", "configured", "null"))
        .await
        .unwrap();
    runtime.stop().await;

    host.registered.store(false, Ordering::SeqCst);
    let error = match AuthorityRuntime::start(store.writer().unwrap(), 0, host).await {
        Ok(_) => panic!("recovery accepted a missing workflow"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("is not registered"), "{error:?}");
    gate.open();
}

#[tokio::test(start_paused = true)]
async fn snapshot_follows_the_dirtying_timer_without_sleeping() {
    let host = ScriptHost::registered();
    host.push(
        "timed",
        SpawnPlan::Session(Box::new(ScriptSession::new([complete(0, 1)]))),
    );
    let store = MemoryStore::default();
    let (runtime, mut commits) = start_script_counted(&store, host).await;
    runtime
        .submit(Submission::new("timed", "timed", "null"))
        .await
        .unwrap();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(runtime.reader.snapshot().await.unwrap().is_none());

    tokio::time::advance(Duration::from_secs(60)).await;
    let snapshot = published_snapshot(&runtime, &mut commits).await;
    assert!(snapshot.cut > 0);
    runtime.stop().await;
}

#[tokio::test]
async fn snapshot_after_transition_threshold_reclaims_in_bounded_pages() {
    let host = ScriptHost::registered();
    for _ in 0..34 {
        host.push(
            "counted",
            SpawnPlan::Session(Box::new(ScriptSession::new([complete(0, 1)]))),
        );
    }
    let store = MemoryStore::default();
    let (counter, mut commits) = watch::channel(0);
    let writer = CountingWriter {
        inner: store.writer().unwrap(),
        commits: counter,
    };
    let runtime = AuthorityRuntime::start_with_config(
        writer,
        0,
        host,
        AuthorityRuntimeConfig {
            compaction_page_size: NonZeroUsize::new(1).unwrap(),
        },
    )
    .await
    .unwrap();
    for index in 0..34 {
        let key = format!("counted-{index}");
        runtime
            .submit(Submission::new(key, "counted", "null"))
            .await
            .unwrap();
    }

    let snapshot = fully_reclaimed(&runtime, &mut commits).await;
    assert!(snapshot.cut >= SNAPSHOT_TRANSITIONS);
    assert_eq!(snapshot.compacted_through, snapshot.cut);
    let stale = runtime
        .reader
        .read(1, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap_err();
    assert!(stale.downcast_ref::<StaleReaderError>().is_some());
    runtime.stop().await;
}

#[tokio::test]
async fn graceful_stop_publishes_the_latest_snapshot() {
    let host = ScriptHost::registered();
    host.push(
        "final",
        SpawnPlan::Session(Box::new(ScriptSession::new([complete(0, 1)]))),
    );
    let store = MemoryStore::default();
    let (runtime, _) = start_script_counted(&store, host).await;
    runtime
        .submit(Submission::new("final", "final", "null"))
        .await
        .unwrap();
    let reader = runtime.reader.clone();
    runtime.stop().await;
    let snapshot = reader.snapshot().await.unwrap().unwrap();
    assert!(snapshot.cut > 0);
}

#[tokio::test]
async fn dropping_runtime_drains_spawns_and_publishes_final_snapshot() {
    let host = ScriptHost::registered();
    let gate = Arc::new(Gate::default());
    gate.close();
    let dropped = Arc::new(DropProbe::default());
    host.push(
        "blocked",
        SpawnPlan::Blocked {
            gate: Arc::clone(&gate),
            session: Box::new(
                ScriptSession::new([complete(0, 1)]).observed_drop(Arc::clone(&dropped)),
            ),
        },
    );
    let store = MemoryStore::default();
    let mut commits = store.subscribe();
    let (runtime, _) = start_script_counted(&store, host).await;
    runtime
        .submit(Submission::new("job", "blocked", "null"))
        .await
        .unwrap();
    gate.entered.notified().await;
    let reader = runtime.reader.clone();
    drop(runtime);
    dropped.wait_for(1).await;
    loop {
        if let Some(snapshot) = reader.snapshot().await.unwrap() {
            assert!(snapshot.cut > 0);
            break;
        }
        commits.changed().await.unwrap();
    }
}
