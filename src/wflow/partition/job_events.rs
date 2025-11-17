use crate::interlude::*;

use super::RetryPolicy;

use crate::plugin::binds_metastore::townframe::wflow::metastore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub timestamp: OffsetDateTime,
    pub job_id: Arc<str>,
    pub deets: JobEventDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEventDeets {
    Init(JobInitEvent),
    Run(JobRunEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInitEvent {
    pub args_json: Arc<str>,
    pub override_wflow_retry_policy: Option<RetryPolicy>,
    pub wflow: metastore::WflowMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunEvent {
    pub run_id: u64,
    pub start_at: OffsetDateTime,
    pub end_at: OffsetDateTime,
    pub result: JobRunResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobRunResult {
    Success { value_json: Arc<str> },
    StepEffect(JobEffectResult),
    WorkerErr(JobRunWorkerError),
    WflowErr(JobError),
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
    pub start_at: OffsetDateTime,
    pub end_at: OffsetDateTime,
    pub deets: JobEffectResultDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEffectResultDeets {
    Success { value: Arc<[u8]> },
    EffectErr(JobError),
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
