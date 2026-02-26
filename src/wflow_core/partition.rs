use crate::interlude::*;

pub mod effects;
pub mod job_events;
pub mod log;
pub mod reduce;
pub mod state;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryPolicy {
    Immediate,
}
