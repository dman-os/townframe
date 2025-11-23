use crate::interlude::*;

use super::RetryPolicy;
use crate::gen::metastore::WflowMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInitEvent {
    pub job_id: Arc<str>,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub timestamp: OffsetDateTime,
    pub args_json: Arc<str>,
    pub override_wflow_retry_policy: Option<RetryPolicy>,
    pub wflow: WflowMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunEvent {
    pub job_id: Arc<str>,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub timestamp: OffsetDateTime,
    pub effect_id: crate::partition::effects::EffectId,
    pub run_id: u64,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub start_at: OffsetDateTime,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
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
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub start_at: OffsetDateTime,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub end_at: OffsetDateTime,
    pub deets: JobEffectResultDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEffectResultDeets {
    Success { value_json: Arc<str> },
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
