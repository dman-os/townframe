use std::any::Any;
use std::rc::Rc;

use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use wflow_core::partition::state;
use wflow_wdk::JobErrorX;
use wflow_wdk::LocalBoxFuture;
use wflow_wdk::WflowBackend;
use wflow_wdk::WflowCtx;
use wflow_wdk::types;
use wflow_wdk_native::NativeHostDriver;
use wflow_wdk_native::NativeSessionHandle;

use crate::interlude::*;
use crate::partition::service;

struct LocalNativeRunSession {
    session: Option<NativeSessionHandle>,
}

impl service::WflowServiceSession for LocalNativeRunSession {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Clone, Default)]
pub struct LocalNativeHost {
    driver: Arc<NativeHostDriver>,
}

impl LocalNativeHost {
    pub fn new() -> Self {
        Self {
            driver: Arc::new(NativeHostDriver::new()),
        }
    }

    pub fn with_driver(driver: Arc<NativeHostDriver>) -> Self {
        Self { driver }
    }

    pub fn driver(&self) -> &Arc<NativeHostDriver> {
        &self.driver
    }

    pub fn register<F>(&self, key: impl Into<Arc<str>>, runner: F)
    where
        F: Fn(types::RunArgs, Rc<dyn WflowBackend>) -> LocalBoxFuture<'static, types::JobResult>
            + Send
            + Sync
            + 'static,
    {
        self.driver.register(key, runner);
    }

    pub fn register_bundle<I, K, F>(&self, keys: I, runner: F)
    where
        I: IntoIterator<Item = K>,
        K: Into<Arc<str>>,
        F: Fn(types::RunArgs, Rc<dyn WflowBackend>) -> LocalBoxFuture<'static, types::JobResult>
            + Send
            + Sync
            + 'static,
    {
        self.driver.register_bundle(keys, runner);
    }

    pub fn register_typed<A, R, F>(&self, key: impl Into<Arc<str>>, handler: F)
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: for<'a> Fn(&'a mut WflowCtx, A) -> LocalBoxFuture<'a, Result<R, JobErrorX>>
            + Send
            + Sync
            + 'static,
    {
        self.driver.register_typed(key, handler);
    }

    pub fn unregister(&self, key: &str) -> bool {
        self.driver.unregister(key)
    }

    pub fn contains_wflow(&self, key: &str) -> bool {
        self.driver.contains_wflow(key)
    }
}

#[async_trait]
impl service::WflowServiceHost for LocalNativeHost {
    type ExtraArgs = ();

    async fn run(
        &self,
        ctx: &service::RunJobCtx,
        job_id: Arc<str>,
        journal: state::JobState,
        mut session: Option<Box<dyn service::WflowServiceSession>>,
        cancel_token: CancellationToken,
        _args: &Self::ExtraArgs,
    ) -> service::RunJobReply {
        let native_session = session.take().map(|mut session_box| {
            session_box
                .as_any_mut()
                .downcast_mut::<LocalNativeRunSession>()
                .expect("invalid session type for local native host")
                .session
                .take()
                .expect("local native session already taken")
        });

        let (result, next_session) = self
            .driver
            .run_job(
                &ctx.effect_id,
                ctx.run_id,
                job_id,
                journal,
                native_session,
                cancel_token,
            )
            .await;

        service::RunJobReply {
            result,
            session: next_session.map(|handle| {
                Box::new(LocalNativeRunSession {
                    session: Some(handle),
                }) as Box<dyn service::WflowServiceSession>
            }),
        }
    }

    fn drop_session(&self, mut session: Box<dyn service::WflowServiceSession>) {
        let handle = session
            .as_any_mut()
            .downcast_mut::<LocalNativeRunSession>()
            .expect("invalid session type for local native host")
            .session
            .take()
            .expect("local native session already taken");
        drop(handle);
    }
}
