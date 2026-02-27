use crate::interlude::*;

use daybook_core::app::{globals::KnownRepoEntry, GlobalCtx};
use tokio::sync::oneshot;

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
    #[allow(unused)]
    pub gcx: Arc<GlobalCtx>,
    pub rcx: Arc<daybook_core::repo::RepoCtx>,
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
    #[tracing::instrument(err, skip(gcx))]
    async fn init(repo_root: String, gcx: &GlobalFfiCtx) -> Result<Arc<Self>, FfiError> {
        utils_rs::setup_tracing_once();

        let gcx = gcx.inner.clone();

        let rt = crate::init_tokio()?;
        let rt = Arc::new(rt);

        let repo_root = std::path::PathBuf::from(repo_root);
        let repo_root_for_init = repo_root.clone();

        let (rcx, gcx) = do_on_rt(&rt, async move {
            let rcx = daybook_core::repo::RepoCtx::open(
                &gcx,
                &repo_root_for_init,
                daybook_core::repo::RepoOpenOptions {
                    ensure_initialized: true,
                    peer_id: "daybook_client".to_string(),
                    ws_connector_url: Some("ws://0.0.0.0:8090".to_string()),
                },
            )
            .await?;
            let rcx = Arc::new(rcx);

            // FIXME: garbage, this should be handeld by the RepoCtx itself
            let _repo = daybook_core::repo::upsert_known_repo(&gcx.sql.db_pool, &repo_root).await?;
            daybook_core::repo::mark_repo_initialized(&repo_root).await?;

            eyre::Ok((rcx, gcx))
        })
        .await
        .wrap_err("error initializing main Ctx")
        .inspect_err(|err| tracing::error!(?err))?;

        Ok(Arc::new(Self { rcx, gcx, rt }))
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

#[derive(uniffi::Object)]
pub struct GlobalFfiCtx {
    rt: Arc<tokio::runtime::Runtime>,
    pub inner: Arc<GlobalCtx>,
}

#[uniffi::export]
impl GlobalFfiCtx {
    #[uniffi::constructor]
    #[tracing::instrument(err)]
    async fn new(repo_root: String) -> Result<Arc<Self>, FfiError> {
        utils_rs::setup_tracing_once();

        let rt = crate::init_tokio()?;
        let rt = Arc::new(rt);

        let inner = do_on_rt(&rt, async move {
            let gcx = GlobalCtx::new().await?;
            let gcx = Arc::new(gcx);
            eyre::Ok(gcx)
        })
        .await
        .wrap_err("error initializing ctx")
        .inspect_err(|err| tracing::error!(?err))?;

        Ok(Arc::new(Self { inner, rt }))
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_repo_config(
        self: Arc<Self>,
    ) -> Result<daybook_core::app::globals::RepoConfig, FfiError> {
        let this = self.clone();
        self.do_on_rt(async move {
            daybook_core::app::globals::get_repo_config(&this.inner.sql.db_pool).await
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self))]
    async fn register_repo_path(
        self: Arc<Self>,
        repo_root: String,
    ) -> Result<KnownRepoEntry, FfiError> {
        let repo_root = std::path::PathBuf::from(repo_root);
        let this = self.clone();
        self.do_on_rt(async move {
            let repo =
                daybook_core::repo::upsert_known_repo(&this.inner.sql.db_pool, &repo_root).await?;
            eyre::Ok(repo)
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self))]
    async fn is_repo_usable(self: Arc<Self>, repo_root: String) -> Result<bool, FfiError> {
        let repo_root = std::path::PathBuf::from(repo_root);
        self.do_on_rt(async move {
            if !repo_root.exists() {
                return Ok::<bool, eyre::Report>(false);
            }
            daybook_core::repo::is_repo_initialized(&repo_root).await
        })
        .await
        .map_err(Into::into)
    }
}

impl GlobalFfiCtx {
    pub async fn do_on_rt<O, F>(&self, future: F) -> O
    where
        O: Send + Sync + 'static,
        F: std::future::Future<Output = O> + Send + 'static,
    {
        do_on_rt(&self.rt, future).await
    }
}
