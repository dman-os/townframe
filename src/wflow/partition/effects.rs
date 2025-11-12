use crate::interlude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionEffect {
    pub job_id: Arc<str>,
    pub deets: PartitionEffectDeets,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PartitionEffectDeets {
    RunJob(RunJobAttemptDeets),
    AbortJob { reason: Arc<str> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunJobAttemptDeets {
    pub run_id: u64,
    pub args_json: Arc<str>,
}

impl From<RunJobAttemptDeets> for PartitionEffectDeets {
    fn from(value: RunJobAttemptDeets) -> Self {
        Self::RunJob(value)
    }
}
