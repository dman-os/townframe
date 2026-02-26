use wflow_core::partition::state;

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
        _session: Option<Box<dyn service::WflowServiceSession>>,
        _args: &Self::ExtraArgs,
    ) -> service::RunJobReply {
        todo!()
    }
}
