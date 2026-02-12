use crate::interlude::*;

use std::collections::HashMap;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStepState {
    Effect { attempts: Vec<JobEffectResult> },
}
