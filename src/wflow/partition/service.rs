use crate::interlude::*;

use crate::partition::job_events;
use crate::partition::state;

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum RunError {
    /// key not found
    WflowNotFound,
    /// wflow error {0:?}
    WflowErr(job_events::JobError),
    /// other {0:?}
    Other(#[from] eyre::Report),
}

#[derive(Debug)]
pub enum RunValue {
    Success { value_json: Arc<str> },
    StepEffect { step_id: u64, value: Arc<[u8]> },
}

pub struct RunArgs {
    pub wflow_key: Arc<str>,
    pub job_id: Arc<str>,
    pub args_json: Arc<str>,
    pub journal: state::JobState,
}

#[async_trait]
pub trait WflowServiceHost {
    async fn run(&self, args: RunArgs) -> Result<RunValue, RunError>;
}
