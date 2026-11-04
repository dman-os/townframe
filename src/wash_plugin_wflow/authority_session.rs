use super::*;
use wflow_tokio::authority_runtime::session::Host;
use wflow_tokio::authority_runtime::session::Session;
use wflow_wdk::StepKind;
use wflow_wdk::StepMeta;
use wflow_wdk::types as shared_types;

/// The component-model `step-meta` record is generated in this crate, so the
/// durable boundary the guest declared can be lifted into the shared type the
/// authority commits against without a WIT round-trip.
impl From<host::StepMeta> for StepMeta {
    fn from(meta: host::StepMeta) -> Self {
        Self {
            kind: match meta.kind {
                host::StepKind::Effect => StepKind::Effect,
                host::StepKind::Sleep => StepKind::Sleep,
                host::StepKind::Recv => StepKind::Recv,
                host::StepKind::Defer => StepKind::Defer,
            },
            name: meta.name,
        }
    }
}

struct WasmSession(SessionHandle);

#[async_trait]
impl Session for WasmSession {
    async fn next_yield(&mut self) -> Option<SessionYield> {
        let trap = self.0.yield_rx.recv().await?;
        Some(match trap {
            JobTrap::PersistStep {
                step_id,
                attempt_id,
                start_at,
                end_at,
                value_json,
                meta,
            } => SessionYield::PersistStep {
                step_id,
                attempt_id,
                start_at,
                end_at,
                value_json,
                meta: meta.into(),
            },
            JobTrap::WaitStep {
                step_id,
                attempt_id,
                start_at,
                deets,
                meta,
            } => SessionYield::WaitStep {
                step_id,
                attempt_id,
                start_at,
                meta: meta.into(),
                deets: match deets {
                    WaitTrapDeets::Timer { wait_id, fire_at } => {
                        WaitYield::Timer { wait_id, fire_at }
                    }
                    WaitTrapDeets::Message { wait_id } => WaitYield::Message { wait_id },
                },
            },
            JobTrap::BeginCleanup { error_json } => SessionYield::BeginCleanup { error_json },
            JobTrap::RunComplete(result) => {
                SessionYield::Complete(result.map_err(|err| match err {
                    types::JobError::Terminal(value) => shared_types::JobError::Terminal(value),
                    types::JobError::Transient(err) => {
                        shared_types::JobError::Transient(shared_types::TransientJobError {
                            error_json: err.error_json,
                            retry_policy: err.retry_policy.map(|policy| match policy {
                                types::RetryPolicy::Immediate => {
                                    shared_types::RetryPolicy::Immediate
                                }
                                types::RetryPolicy::Fixed(fixed) => {
                                    shared_types::RetryPolicy::Fixed {
                                        delay_ms: fixed.delay_ms,
                                    }
                                }
                                types::RetryPolicy::CappedExponential(exp) => {
                                    shared_types::RetryPolicy::CappedExponential {
                                        base_ms: exp.base_ms,
                                        cap_ms: exp.cap_ms,
                                    }
                                }
                            }),
                        })
                    }
                }))
            }
        })
    }

    fn resume(&self) -> bool {
        self.0.resume_tx.send(SessionResume::Continue).is_ok()
    }

    fn deliver(&self, value_json: Arc<str>) -> bool {
        self.0
            .resume_tx
            .send(SessionResume::Deliver(value_json))
            .is_ok()
    }

    fn interrupt(&self, cause: CleanupCause) -> bool {
        self.0.interrupt(cause)
    }
}

#[async_trait]
impl Host for WflowPlugin {
    fn contains_wflow(&self, key: &str) -> bool {
        self.active_keys.contains_key(key)
    }

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
        cleanup: Option<CleanupCause>,
    ) -> Res<Box<dyn Session>> {
        let workload_id = self
            .active_keys
            .get(wflow_key)
            .map(|id| Arc::clone(id.value()))
            .ok_or_else(|| ferr!("workflow not registered: {wflow_key}"))?;
        let workload = self
            .active_workloads
            .read()
            .expect(ERROR_MUTEX)
            .get(&workload_id)
            .cloned()
            .ok_or_else(|| ferr!("workflow workload stopped: {workload_id}"))?;
        let session = self
            .start_session(&workload, job_id, wflow_key, args_json, replay, cleanup)
            .await
            .map_err(|err| ferr!("wasm session could not start: {err:?}"))?;
        Ok(Box::new(WasmSession(session)))
    }

    fn forget_job(&self, job_id: &str) {
        self.job_step_metas
            .lock()
            .expect(ERROR_MUTEX)
            .remove(job_id);
    }
}
