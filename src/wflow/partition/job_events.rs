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
    Effect(JobEffectEvent),
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
pub struct JobEffectEvent {
    pub step_id: u64,
    pub attempt_id: u64,
    pub start_at: OffsetDateTime,
    pub end_at: OffsetDateTime,
    pub result: JobEffectResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobRunResult {
    Success { value_json: Arc<str> },
    EffectInterrupt { step_id: u64 },
    WorkerErr(JobRunWorkerError),
    WflowErr(JobError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobRunWorkerError {
    WflowNotFound,
    JobNotFound,
    Other { msg: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEffectResult {
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
