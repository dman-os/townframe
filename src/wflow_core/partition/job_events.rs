use crate::interlude::*;

use super::RetryPolicy;
use crate::gen::metastore::WflowMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInitEvent {
    pub job_id: Arc<str>,
    pub timestamp: Timestamp,
    pub args_json: Arc<str>,
    pub override_wflow_retry_policy: Option<RetryPolicy>,
    pub wflow: WflowMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCancelEvent {
    pub job_id: Arc<str>,
    pub timestamp: Timestamp,
    pub reason: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMessageEvent {
    pub job_id: Arc<str>,
    pub message_id: Arc<str>,
    pub timestamp: Timestamp,
    pub payload_json: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTimerFiredEvent {
    pub job_id: Arc<str>,
    pub wait_id: u64,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunEvent {
    pub job_id: Arc<str>,
    pub timestamp: Timestamp,
    pub effect_id: crate::partition::effects::EffectId,
    pub run_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<Arc<str>>,
    pub start_at: Timestamp,
    pub end_at: Timestamp,
    pub result: JobRunResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobRunResult {
    Success { value_json: Arc<str> },
    StepEffect(JobEffectResult),
    StepWait(JobWaitResult),
    WorkerErr(JobRunWorkerError),
    WflowErr(JobError),
    Aborted,
}

impl From<eyre::Report> for JobRunResult {
    fn from(value: eyre::Report) -> Self {
        Self::WorkerErr(JobRunWorkerError::Other {
            msg: format!("{value:?}"),
        })
    }
}

impl From<JobError> for JobRunResult {
    fn from(value: JobError) -> Self {
        Self::WflowErr(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobRunWorkerError {
    WflowNotFound,
    JobNotFound,
    Other { msg: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEffectResult {
    pub step_id: u64,
    pub attempt_id: u64,
    pub start_at: Timestamp,
    pub end_at: Timestamp,
    pub deets: JobEffectResultDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEffectResultDeets {
    Success { value_json: Arc<str> },
    EffectErr(JobError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobWaitResult {
    pub step_id: u64,
    pub attempt_id: u64,
    pub start_at: Timestamp,
    pub deets: JobWaitResultDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobWaitResultDeets {
    Timer { wait_id: u64, fire_at: Timestamp },
    Message { wait_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobError {
    Transient {
        error_json: Arc<str>,
        retry_policy: Option<RetryPolicy>,
    },
    Terminal {
        error_json: Arc<str>,
    },
}
