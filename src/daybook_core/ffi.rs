use tokio::sync::oneshot;

use crate::interlude::*;

uniffi::custom_type!(OffsetDateTime, i64, {
    remote,
    lower: |dt| dt.unix_timestamp(),
    try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
        .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
});

uniffi::custom_type!(Uuid, Vec<u8>, {
    remote,
    lower: |uuid| uuid.as_bytes().to_vec(),
    try_lift: |bytes: Vec<u8>| {
        uuid::Uuid::from_slice(&bytes)
            .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
    }
});

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

impl From<utils_rs::prelude::serde_json::Error> for FfiError {
    fn from(err: utils_rs::prelude::serde_json::Error) -> Self {
        // Wrap serde_json error into an eyre::Report so existing From<Report>
        // implementation can be reused.
        Self::from(eyre::Report::new(err))
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
        utils_rs::setup_tracing_once();
        let rt = crate::init_tokio()?;
        let rt = Arc::new(rt);
        let config = crate::Config::new()
            .wrap_err("error creating default config")
            .inspect_err(|err| tracing::error!(?err))?;
        let cx = do_on_rt(&rt, async { Ctx::new(config).await })
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
    rx.await.expect_or_log(ERROR_CHANNEL)
}
