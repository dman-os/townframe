use crate::interlude::*;

use std::sync::atomic::AtomicU64;

use super::job_events::*;
use super::RetryPolicy;
use crate::plugin::binds_metastore::townframe::wflow::metastore;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PartitionWorkingState {
    // FIXME: this probably should go into the metastore
    pub last_applied_entry_id: AtomicU64,
    pub jobs: PartitionJobsState,
    pub effects: ActiveEffectState,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PartitionJobsState {
    pub active: DHashMap<Arc<str>, JobState>,
    pub archive: DHashMap<Arc<str>, JobState>,
}

impl PartitionJobsState {
    pub fn get(&self, id: &str) -> Option<DHashMapRef<'_, Arc<str>, JobState>> {
        self.active.get(id)
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ActiveEffectState {
    map: DHashMap<u64, EffectState>,
}

impl ActiveEffectState {
    pub fn get(&self, id: u64) -> Option<DHashMapRef<'_, u64, EffectState>> {
        self.map.get(&id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EffectState {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobState {
    pub init_args_json: Arc<str>,
    pub wflow: metastore::WflowMeta,
    pub override_wflow_retry_policy: Option<RetryPolicy>,
    pub runs: Vec<JobRunEvent>,
    pub steps: Vec<JobStepState>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobStepState {
    Effect { attempts: Vec<JobEffectResult> },
}

#[derive(Debug, Serialize, Deserialize)]
enum JobEffectAttemptState {
    Ongoing {
        started_at: OffsetDateTime,
    },
    Error {
        error: JobError,
        started_at: OffsetDateTime,
        completed_at: OffsetDateTime,
    },
    Completed {
        value_json: Arc<str>,
        started_at: OffsetDateTime,
        completed_at: OffsetDateTime,
    },
}

impl JobState {
    fn new(evt: JobInitEvent) -> Self {
        Self {
            init_args_json: evt.args_json.clone(),
            override_wflow_retry_policy: evt.override_wflow_retry_policy,
            wflow: evt.wflow,
            runs: default(),
            steps: default(),
        }
    }
}
