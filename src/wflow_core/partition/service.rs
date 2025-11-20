use crate::interlude::*;

use crate::partition::job_events;
use crate::partition::state;

#[async_trait]
pub trait WflowServiceHost {
    type ExtraArgs;
    async fn run(
        &self,
        job_id: Arc<str>,
        journal: state::JobState,
        args: &Self::ExtraArgs,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult>;
}
