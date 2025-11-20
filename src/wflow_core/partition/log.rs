use crate::interlude::*;

use super::effects;
use super::job_events;

#[derive(Debug, Serialize, Deserialize)]
pub enum PartitionLogEntry {
    JobEvent(job_events::JobEvent),
    NewPartitionEffects(NewPartitionEffectsLogEntry),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewPartitionEffectsLogEntry {
    pub source_entry_id: u64,
    pub effects: Vec<effects::PartitionEffect>,
}
