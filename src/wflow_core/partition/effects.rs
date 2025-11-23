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
    AbortJob { reason: Arc<str> },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunJobAttemptDeets {
    pub run_id: u64,
}

impl From<RunJobAttemptDeets> for PartitionEffectDeets {
    fn from(value: RunJobAttemptDeets) -> Self {
        Self::RunJob(value)
    }
}
