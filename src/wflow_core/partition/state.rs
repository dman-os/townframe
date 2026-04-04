use crate::interlude::*;

use std::collections::{HashMap, VecDeque};

use crate::gen::metastore::WflowMeta;
use crate::partition::job_events::*;
use crate::partition::RetryPolicy;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PartitionJobsState {
    pub active: HashMap<Arc<str>, JobState>,
    pub archive: HashMap<Arc<str>, JobState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobState {
    pub init_args_json: Arc<str>,
    pub wflow: WflowMeta,
    pub override_wflow_retry_policy: Option<RetryPolicy>,
    // FIXME: could be cleaner
    pub cancelling: bool,
    pub runs: Vec<JobRunEvent>,
    pub steps: Vec<JobStepState>,
    pub pending_messages: VecDeque<JobInboxMessage>,
    pub active_wait: Option<JobWaitState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStepState {
    Effect { attempts: Vec<JobEffectResult> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInboxMessage {
    pub message_id: Arc<str>,
    pub timestamp: Timestamp,
    pub payload_json: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobWaitState {
    pub wait_id: u64,
    pub run_id: u64,
    pub preferred_worker_id: Option<Arc<str>>,
    pub step_id: u64,
    pub attempt_id: u64,
    pub start_at: Timestamp,
    pub deets: crate::partition::job_events::JobWaitResultDeets,
}
