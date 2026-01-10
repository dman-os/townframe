use tokio::sync::oneshot;

use crate::interlude::*;

daybook_types::custom_type_set!();

#[derive(Debug, thiserror::Error, uniffi::Object)]
#[error(transparent)]
pub struct FfiError {
    inner: eyre::Report,
}

impl From<eyre::Report> for FfiError {
    fn from(inner: eyre::Report) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl FfiError {
    fn message(&self) -> String {
        format!("{:#?}", self.inner)
    }
}

#[derive(uniffi::Object)]
pub struct FfiCtx {
    rt: Arc<tokio::runtime::Runtime>,
    pub cx: crate::SharedCtx,
}
pub type SharedFfiCtx = Arc<FfiCtx>;

impl FfiCtx {
    pub async fn do_on_rt<O, F>(&self, future: F) -> O
    where
        O: Send + Sync + 'static,
        F: std::future::Future<Output = O> + Send + 'static,
    {
        do_on_rt(&self.rt, future).await
    }
}

#[uniffi::export]
impl FfiCtx {
    #[uniffi::constructor]
    #[tracing::instrument(err)]
    async fn for_ffi() -> Result<Arc<FfiCtx>, FfiError> {
        utils_rs::testing::setup_tracing_once();
        let rt = crate::init_tokio()?;
        let rt = Arc::new(rt);
        let config = crate::Config::new()
            .wrap_err("error creating default config")
            .inspect_err(|err| tracing::error!(?err))?;
        let cx = do_on_rt(&rt, async { Ctx::init(config).await })
            .await
            .wrap_err("error initializing main Ctx")
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { cx, rt }))
    }
}

async fn do_on_rt<O, F>(rt: &tokio::runtime::Runtime, future: F) -> O
where
    O: Send + Sync + 'static,
    F: std::future::Future<Output = O> + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    rt.spawn(async {
        let res = future.await;
        tx.send(res)
    });
    rx.await.expect(ERROR_CHANNEL)
}
