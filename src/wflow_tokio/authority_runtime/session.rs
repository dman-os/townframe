use crate::interlude::*;
use crate::local_native_host::LocalNativeHost;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk_native::NativeSession;

#[async_trait]
pub trait Host: Send + Sync {
    fn contains_wflow(&self, key: &str) -> bool;

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Res<Box<dyn Session>>;

    fn forget_job(&self, job_id: &str);
}

#[async_trait]
pub trait Session: Send {
    async fn next_yield(&mut self) -> Option<SessionYield>;
    fn resume(&self) -> bool;
    fn deliver(&self, value_json: Arc<str>) -> bool;
}

#[async_trait]
impl Session for NativeSession {
    async fn next_yield(&mut self) -> Option<SessionYield> {
        NativeSession::next_yield(self).await
    }

    fn resume(&self) -> bool {
        NativeSession::resume(self)
    }

    fn deliver(&self, value_json: Arc<str>) -> bool {
        NativeSession::deliver(self, value_json)
    }
}

#[async_trait]
impl Host for LocalNativeHost {
    fn contains_wflow(&self, key: &str) -> bool {
        LocalNativeHost::contains_wflow(self, key)
    }

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<ReplayStep>,
    ) -> Res<Box<dyn Session>> {
        let session = self
            .driver()
            .spawn(job_id, wflow_key, args_json, replay)
            .map_err(|err| ferr!("native session could not start: {err:?}"))?;
        Ok(Box::new(session))
    }

    fn forget_job(&self, job_id: &str) {
        self.driver().forget_job(job_id);
    }
}
