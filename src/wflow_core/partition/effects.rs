use crate::interlude::*;

#[derive(Debug, Serialize, Deserialize, std::hash::Hash, PartialEq, PartialOrd, Eq, Clone)]
pub struct EffectId {
    pub entry_id: u64,
    pub effect_idx: u64,
}

pub enum EffectCommand {
    Schedule(PartitionEffect),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PartitionEffect {
    pub job_id: Arc<str>,
    pub deets: PartitionEffectDeets,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PartitionEffectDeets {
    RunJob(RunJobAttemptDeets),
    AbortRun { reason: Arc<str> },
    WaitTimer(WaitTimerDeets),
    WaitMessage(WaitMessageDeets),
    CancelWait(CancelWaitDeets),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunJobAttemptDeets {
    pub run_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_worker_id: Option<Arc<str>>,
}

impl From<RunJobAttemptDeets> for PartitionEffectDeets {
    fn from(value: RunJobAttemptDeets) -> Self {
        Self::RunJob(value)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitTimerDeets {
    pub wait_id: u64,
    pub fire_at: Timestamp,
    pub step_id: u64,
    pub attempt_id: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitMessageDeets {
    pub wait_id: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CancelWaitDeets {
    pub wait_id: u64,
    pub reason: Arc<str>,
}
