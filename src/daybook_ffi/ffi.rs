use crate::interlude::*;

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
    pub cx: Option<crate::SharedCtx>,
}
pub type SharedFfiCtx = Arc<FfiCtx>;

#[derive(uniffi::Record, Clone, Debug)]
pub struct KnownRepoEntryFfi {
    pub id: String,
    pub path: String,
    pub created_at_unix_secs: i64,
    pub last_opened_at_unix_secs: i64,
}

impl From<daybook_core::repo::KnownRepoEntry> for KnownRepoEntryFfi {
    fn from(value: daybook_core::repo::KnownRepoEntry) -> Self {
        Self {
            id: value.id,
            path: value.path,
            created_at_unix_secs: value.created_at_unix_secs,
            last_opened_at_unix_secs: value.last_opened_at_unix_secs,
        }
    }
}

impl FfiCtx {
    pub async fn do_on_rt<O, F>(&self, future: F) -> O
    where
        O: Send + Sync + 'static,
        F: std::future::Future<Output = O> + Send + 'static,
    {
        do_on_rt(&self.rt, future).await
    }

    pub fn repo_ctx(&self) -> &crate::SharedCtx {
        self.cx
            .as_ref()
            .expect("FfiCtx does not have an attached repo context")
    }
}

#[uniffi::export]
impl FfiCtx {
    #[uniffi::constructor]
    #[tracing::instrument(err)]
    async fn for_globals() -> Result<Arc<FfiCtx>, FfiError> {
        utils_rs::setup_tracing_once();
        let rt = Arc::new(crate::init_tokio()?);
        Ok(Arc::new(Self { cx: None, rt }))
    }

    #[uniffi::constructor]
    #[tracing::instrument(err)]
    async fn for_repo_root(repo_root: String) -> Result<Arc<FfiCtx>, FfiError> {
        utils_rs::setup_tracing_once();
        let rt = Arc::new(crate::init_tokio()?);
        let repo_root = std::path::PathBuf::from(repo_root);
        let repo_root_for_init = repo_root.clone();
        let cx = do_on_rt(&rt, async move {
            Ctx::init(repo_root_for_init, Some("ws://0.0.0.0:8090".to_string())).await
        })
        .await
        .wrap_err("error initializing main Ctx")
        .inspect_err(|err| tracing::error!(?err))?;
        do_on_rt(&rt, async move {
            let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
            let _repo =
                daybook_core::repo::upsert_known_repo(&global_ctx.sql.db_pool, &repo_root).await?;
            daybook_core::repo::mark_repo_initialized(&repo_root).await?;
            Ok::<(), eyre::Report>(())
        })
        .await
        .wrap_err("error writing global repo metadata")
        .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { cx: Some(cx), rt }))
    }

    #[uniffi::constructor]
    #[tracing::instrument(err)]
    async fn for_ffi() -> Result<Arc<FfiCtx>, FfiError> {
        utils_rs::setup_tracing_once();
        let rt = Arc::new(crate::init_tokio()?);
        let repo_root = do_on_rt(&rt, async {
            let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
            if let Some(last_used_repo) =
                daybook_core::repo::get_last_used_repo(&global_ctx.sql.db_pool).await?
            {
                Ok::<std::path::PathBuf, eyre::Report>(std::path::PathBuf::from(
                    last_used_repo.path,
                ))
            } else {
                Ok(global_ctx.config.default_repo_root)
            }
        })
        .await
        .wrap_err("error resolving default repo path")
        .inspect_err(|err| tracing::error!(?err))?;
        let repo_root_for_init = repo_root.clone();
        let cx = do_on_rt(&rt, async move {
            Ctx::init(repo_root_for_init, Some("ws://0.0.0.0:8090".to_string())).await
        })
        .await
        .wrap_err("error initializing main Ctx")
        .inspect_err(|err| tracing::error!(?err))?;
        do_on_rt(&rt, async move {
            let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
            let _repo =
                daybook_core::repo::upsert_known_repo(&global_ctx.sql.db_pool, &repo_root).await?;
            daybook_core::repo::mark_repo_initialized(&repo_root).await?;
            Ok::<(), eyre::Report>(())
        })
        .await
        .wrap_err("error updating global repo metadata")
        .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { cx: Some(cx), rt }))
    }

    #[tracing::instrument(err, skip(self))]
    async fn list_known_repos(self: Arc<Self>) -> Result<Vec<KnownRepoEntryFfi>, FfiError> {
        self.do_on_rt(async move {
            let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
            let repos = daybook_core::repo::list_known_repos(&global_ctx.sql.db_pool).await?;
            Ok::<Vec<KnownRepoEntryFfi>, eyre::Report>(repos.into_iter().map(Into::into).collect())
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_last_used_repo(self: Arc<Self>) -> Result<Option<KnownRepoEntryFfi>, FfiError> {
        self.do_on_rt(async move {
            let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
            let repo = daybook_core::repo::get_last_used_repo(&global_ctx.sql.db_pool).await?;
            Ok::<Option<KnownRepoEntryFfi>, eyre::Report>(repo.map(Into::into))
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self))]
    async fn register_repo_path(
        self: Arc<Self>,
        repo_root: String,
    ) -> Result<KnownRepoEntryFfi, FfiError> {
        let repo_root = std::path::PathBuf::from(repo_root);
        self.do_on_rt(async move {
            let global_ctx = daybook_core::repo::GlobalCtx::new().await?;
            let repo =
                daybook_core::repo::upsert_known_repo(&global_ctx.sql.db_pool, &repo_root).await?;
            Ok::<KnownRepoEntryFfi, eyre::Report>(repo.into())
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
