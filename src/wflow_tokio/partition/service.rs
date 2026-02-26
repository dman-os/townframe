use crate::interlude::*;
use std::any::Any;

use wflow_core::partition::effects;
use wflow_core::partition::job_events;
use wflow_core::partition::state;

#[derive(Debug, Clone)]
pub struct RunJobCtx {
    pub effect_id: effects::EffectId,
    pub run_id: u64,
    pub worker_id: Arc<str>,
}

pub trait WflowServiceSession: Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub struct RunJobReply {
    pub result: Result<job_events::JobRunResult, job_events::JobRunResult>,
    pub session: Option<Box<dyn WflowServiceSession>>,
}

#[async_trait]
pub trait WflowServiceHost {
    type ExtraArgs;
    async fn run(
        &self,
        ctx: &RunJobCtx,
        job_id: Arc<str>,
        journal: state::JobState,
        session: Option<Box<dyn WflowServiceSession>>,
        args: &Self::ExtraArgs,
    ) -> RunJobReply;

    fn drop_session(&self, _session: Box<dyn WflowServiceSession>) {}
}
