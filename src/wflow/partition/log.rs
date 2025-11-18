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

pub struct PartitionLogRef {
    buffer: Vec<u8>,
    log: Arc<dyn crate::log::LogStore>,
}

impl Clone for PartitionLogRef {
    fn clone(&self) -> Self {
        Self {
            buffer: default(),
            log: self.log.clone(),
        }
    }
}
impl PartitionLogRef {
    pub fn new(log: Arc<dyn crate::log::LogStore>) -> Self {
        Self {
            buffer: vec![],
            log,
        }
    }
    pub async fn append(&mut self, evt: &PartitionLogEntry) -> Res<u64> {
        self.buffer.clear();
        serde_json::to_writer(&mut self.buffer, evt).expect(ERROR_JSON);
        self.log.append(&self.buffer).await
    }
}
