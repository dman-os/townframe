use crate::interlude::*;

use super::effects;
use super::job_events;

#[derive(Debug, Serialize, Deserialize)]
pub enum PartitionLogEntry {
    JobInit(job_events::JobInitEvent),
    JobEffectResult(job_events::JobRunEvent),
    JobCancel(job_events::JobCancelEvent),
    JobMessage(job_events::JobMessageEvent),
    JobTimerFired(job_events::JobTimerFiredEvent),
    JobPartitionEffects(JobPartitionEffectsLogEntry),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobPartitionEffectsLogEntry {
    pub source_entry_id: u64,
    pub effects: Vec<effects::PartitionEffect>,
}
