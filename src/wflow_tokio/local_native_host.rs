use wflow_core::partition::{job_events, service, state};

use crate::interlude::*;

pub struct LocalNativeHost {}

#[async_trait]
impl service::WflowServiceHost for LocalNativeHost {
    type ExtraArgs = ();

    async fn run(
        &self,
        job_id: Arc<str>,
        journal: state::JobState,
        args: &Self::ExtraArgs,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult> {
        todo!()
    }
}
