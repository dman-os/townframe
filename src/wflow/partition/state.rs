use crate::interlude::*;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::partition::effects;
use crate::partition::job_events::*;
use crate::partition::RetryPolicy;
use crate::plugin::binds_metastore::townframe::wflow::metastore;

#[derive(Debug)]
pub struct PartitionWorkingState {
    // FIXME: this probably should go into the metastore
    pub last_applied_entry_id: std::sync::atomic::AtomicU64,
    pub jobs: Mutex<PartitionJobsState>,
    pub effects: Mutex<HashMap<effects::EffectId, effects::PartitionEffect>>,
}

impl Default for PartitionWorkingState {
    fn default() -> Self {
        Self {
            last_applied_entry_id: std::sync::atomic::AtomicU64::new(0),
            jobs: Mutex::new(PartitionJobsState::default()),
            effects: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Default)]
pub struct PartitionJobsState {
    pub active: HashMap<Arc<str>, JobState>,
    pub archive: HashMap<Arc<str>, JobState>,
}

#[derive(Debug, Clone)]
pub struct JobState {
    pub init_args_json: Arc<str>,
    pub wflow: metastore::WflowMeta,
    pub override_wflow_retry_policy: Option<RetryPolicy>,
    pub runs: Vec<JobRunEvent>,
    pub steps: Vec<JobStepState>,
}

#[derive(Debug, Clone)]
pub enum JobStepState {
    Effect { attempts: Vec<JobEffectResult> },
}
