use wflow_core::partition::{job_events, state};

use crate::interlude::*;
use crate::partition::service;

pub struct LocalNativeHost {}

#[async_trait]
impl service::WflowServiceHost for LocalNativeHost {
    type ExtraArgs = ();

    async fn run(
        &self,
        _ctx: &service::RunJobCtx,
        _job_id: Arc<str>,
        _journal: state::JobState,
        _args: &Self::ExtraArgs,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult> {
        todo!()
    }
}
