use crate::interlude::*;

use wflow_core::partition::effects;
use wflow_core::partition::job_events;
use wflow_core::partition::state;

#[derive(Debug, Clone)]
pub struct RunJobCtx {
    pub effect_id: effects::EffectId,
    pub run_id: u64,
    pub worker_id: Arc<str>,
}

#[async_trait]
pub trait WflowServiceHost {
    type ExtraArgs;
    async fn run(
        &self,
        ctx: &RunJobCtx,
        job_id: Arc<str>,
        journal: state::JobState,
        args: &Self::ExtraArgs,
    ) -> Result<job_events::JobRunResult, job_events::JobRunResult>;
}
