use std::rc::Rc;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use wflow_wdk::JobErrorX;
use wflow_wdk::LocalBoxFuture;
use wflow_wdk::WflowBackend;
use wflow_wdk::WflowCtx;
use wflow_wdk::types;
use wflow_wdk_native::NativeHostDriver;

/// Registry-backed native authority host.  Invocations are created by the
/// authority-runtime `Host` implementation; this type only owns registration
/// and the native session executor.
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
