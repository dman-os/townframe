use crate::interlude::*;
use crate::local_native_host::LocalNativeHost;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk_native::NativeSession;

/// Why a host could not start a workflow invocation.
///
/// The runtime has to tell two cases apart, because they lead to different
/// durable outcomes: an engine or store that is temporarily unavailable is worth
/// re-granting behind a backoff, while a workflow this build cannot instantiate
/// will fail identically on every attempt. Collapsing both into one error is
/// what made a deterministic instantiation failure hot-loop through the restart
/// budget and settle as the wrong class with no diagnosis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpawnError {
    /// The host cannot start this workflow now but might later: an engine or
    /// store that is temporarily unavailable.
    Unavailable { message: String },
    /// The host will never start this workflow as configured: an unknown
    /// workflow key, an invalid component, a rejected manifest.
    Rejected { message: String },
}

#[async_trait]
pub trait Host: Send + Sync {
    fn contains_wflow(&self, key: &str) -> bool;

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Result<Box<dyn Session>, SpawnError>;

    fn forget_job(&self, job_id: &str);
}

#[async_trait]
pub trait Session: Send {
    async fn next_yield(&mut self) -> Option<SessionYield>;
    fn resume(&self) -> bool;
    fn deliver(&self, value_json: Arc<str>) -> bool;
}

#[async_trait]
impl Session for NativeSession {
    async fn next_yield(&mut self) -> Option<SessionYield> {
        NativeSession::next_yield(self).await
    }

    fn resume(&self) -> bool {
        NativeSession::resume(self)
    }

    fn deliver(&self, value_json: Arc<str>) -> bool {
        NativeSession::deliver(self, value_json)
    }
}

#[async_trait]
impl Host for LocalNativeHost {
    fn contains_wflow(&self, key: &str) -> bool {
        LocalNativeHost::contains_wflow(self, key)
    }

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Result<Box<dyn Session>, SpawnError> {
        self.driver()
            .spawn(job_id, wflow_key, args_json, replay)
            .map(|session| Box::new(session) as Box<dyn Session>)
            .map_err(|error| match error {
                // The registry is this build's own workflow list: a key that is
                // not in it will not appear later, so no retry can succeed.
                wflow_wdk_native::SpawnError::WflowNotFound => SpawnError::Rejected {
                    message: format!("native workflow {wflow_key:?} is not registered"),
                },
                // The executor thread stopped, which is process state rather
                // than the workflow's configuration, so a later attempt can
                // succeed once the engine is back.
                wflow_wdk_native::SpawnError::ExecutorStopped => SpawnError::Unavailable {
                    message: format!("native workflow executor is stopped ({wflow_key})"),
                },
            })
    }

    fn forget_job(&self, job_id: &str) {
        self.driver().forget_job(job_id);
    }
}
