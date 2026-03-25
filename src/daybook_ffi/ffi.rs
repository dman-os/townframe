use crate::interlude::*;

use daybook_core::app::{globals::KnownRepoEntry, AppCtx};
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
    pub acx: Arc<AppCtx>,
    pub rcx: Arc<daybook_core::repo::RepoCtx>,
}
pub type SharedFfiCtx = Arc<FfiCtx>;

#[derive(Debug, Clone, uniffi::Record)]
pub struct CloneBootstrapInfo {
    pub endpoint_id: String,
    pub repo_id: String,
    pub repo_name: String,
    pub app_doc_id: String,
    pub drawer_doc_id: String,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct CloneInitResult {
    pub repo_path: String,
    pub bootstrap: CloneBootstrapInfo,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct CloneDestinationCheck {
    pub exists: bool,
    pub is_dir: bool,
    pub is_empty: bool,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct CloneTicketWithQr {
    pub ticket_url: String,
    pub qr_png_bytes: Vec<u8>,
}

fn bootstrap_to_ffi(bootstrap: daybook_core::sync::SyncBootstrapState) -> CloneBootstrapInfo {
    CloneBootstrapInfo {
        endpoint_id: bootstrap.endpoint_id.to_string(),
        repo_id: bootstrap.repo_id,
        repo_name: bootstrap.repo_name,
        app_doc_id: bootstrap.app_doc_id.to_string(),
        drawer_doc_id: bootstrap.drawer_doc_id.to_string(),
        device_name: bootstrap.device_name,
    }
}

fn app_private_clone_parent_dir(acx: &AppCtx) -> std::path::PathBuf {
    acx.config.app_data_dir.join("repos")
}

fn resolve_clone_destination_for_app(acx: &AppCtx, destination: &str) -> Res<std::path::PathBuf> {
    fn canonicalize_existing_ancestor(path: &std::path::Path) -> Res<std::path::PathBuf> {
        let mut cursor = path.to_path_buf();
        loop {
            if cursor.exists() {
                return std::fs::canonicalize(&cursor).wrap_err_with(|| {
                    format!("failed resolving clone destination {}", cursor.display())
                });
            }
            let parent = cursor
                .parent()
                .ok_or_eyre("clone destination missing canonicalizable ancestor")?;
            cursor = parent.to_path_buf();
        }
    }

    let parent = app_private_clone_parent_dir(acx);
    std::fs::create_dir_all(&parent).wrap_err_with(|| {
        format!(
            "failed creating clone parent directory {}",
            parent.display()
        )
    })?;
    let parent_canon = std::fs::canonicalize(&parent).wrap_err_with(|| {
        format!(
            "failed resolving clone parent directory {}",
            parent.display()
        )
    })?;
    let raw = std::path::PathBuf::from(destination);
    let destination_candidate = if raw.is_absolute() {
        raw
    } else {
        parent_canon.join(raw)
    };
    let destination_abs = std::path::absolute(&destination_candidate).wrap_err_with(|| {
        format!(
            "failed resolving clone destination {}",
            destination_candidate.display()
        )
    })?;
    let destination_canon = canonicalize_existing_ancestor(&destination_abs)?;
    if !destination_canon.starts_with(&parent_canon) {
        eyre::bail!(
            "clone destination must be under app-private storage: {}",
            parent_canon.display()
        );
    }
    Ok(destination_abs)
}

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
    #[tracing::instrument(err, skip(acx))]
    async fn init(repo_root: String, acx: &AppFfiCtx) -> Result<Arc<Self>, FfiError> {
        utils_rs::setup_tracing_once();

        let rt = Arc::clone(&acx.rt);
        let acx = Arc::clone(&acx.inner);

        let repo_root_for_init = std::path::PathBuf::from(repo_root);

        let (rcx, acx) = do_on_rt(&rt, async move {
            let rcx = if daybook_core::repo::is_repo_initialized(&repo_root_for_init).await? {
                acx.open_repo(
                    &repo_root_for_init,
                    daybook_core::repo::RepoOpenOptions {},
                    format!("daybook-ffi-{}", std::env::consts::ARCH),
                )
                .await?
            } else {
                acx.init_repo(
                    &repo_root_for_init,
                    daybook_core::repo::RepoOpenOptions {},
                    format!("daybook-ffi-{}", std::env::consts::ARCH),
                )
                .await?
            };
            let rcx = Arc::new(rcx);

            eyre::Ok((rcx, acx))
        })
        .await
        .wrap_err("error initializing main Ctx")
        .inspect_err(|err| tracing::error!(?err))?;

        Ok(Arc::new(Self { rcx, acx, rt }))
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
pub struct AppFfiCtx {
    rt: Arc<tokio::runtime::Runtime>,
    pub inner: Arc<AppCtx>,
}

#[uniffi::export]
impl AppFfiCtx {
    #[uniffi::constructor]
    #[tracing::instrument(err)]
    async fn init() -> Result<Arc<Self>, FfiError> {
        utils_rs::setup_tracing_once();

        let rt = crate::init_tokio()?;
        let rt = Arc::new(rt);

        let inner = do_on_rt(&rt, async move {
            let acx = AppCtx::load().await?;
            let acx = Arc::new(acx);
            eyre::Ok(acx)
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
        let this = Arc::clone(&self);
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
        let this = Arc::clone(&self);
        self.do_on_rt(async move {
            let repo =
                daybook_core::repo::upsert_known_repo(&this.inner.sql.db_pool, &repo_root).await?;
            eyre::Ok(repo)
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self))]
    async fn forget_known_repo(self: Arc<Self>, repo_id: String) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.do_on_rt(async move {
            let mut repo_config =
                daybook_core::app::globals::get_repo_config(&this.inner.sql.db_pool).await?;
            repo_config.known_repos.retain(|repo| repo.id != repo_id);
            if repo_config.last_used_repo_id.as_deref() == Some(repo_id.as_str()) {
                repo_config.last_used_repo_id = None;
            }
            daybook_core::app::globals::set_repo_config(&this.inner.sql.db_pool, &repo_config)
                .await?;
            Ok::<(), eyre::Report>(())
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
            daybook_core::repo::is_repo_bootstrapped(&repo_root).await
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self))]
    async fn default_clone_parent_dir(self: Arc<Self>) -> Result<String, FfiError> {
        let this = Arc::clone(&self);
        self.do_on_rt(async move {
            let parent = app_private_clone_parent_dir(&this.inner);
            std::fs::create_dir_all(&parent).wrap_err_with(|| {
                format!(
                    "failed creating clone parent directory {}",
                    parent.display()
                )
            })?;
            Ok::<String, eyre::Report>(parent.display().to_string())
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self, source_url))]
    async fn resolve_clone_url(
        self: Arc<Self>,
        source_url: String,
    ) -> Result<CloneBootstrapInfo, FfiError> {
        self.do_on_rt(async move {
            let bootstrap = daybook_core::sync::request_clone_provision_via_rpc(
                &source_url,
                daybook_core::sync::CloneProvisionRequest {
                    requested_device_name: None,
                    provision: false,
                    requester_endpoint_id: None,
                    requester_peer_key: None,
                },
            )
            .await?
            .to_bootstrap_state()?;
            Ok::<CloneBootstrapInfo, eyre::Report>(bootstrap_to_ffi(bootstrap))
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self, source_url, destination))]
    async fn clone_repo_init_from_url(
        self: Arc<Self>,
        source_url: String,
        destination: String,
    ) -> Result<CloneInitResult, FfiError> {
        let this = Arc::clone(&self);
        self.do_on_rt(async move {
            let destination = resolve_clone_destination_for_app(&this.inner, &destination)?;
            let out = daybook_core::sync::clone_repo_init_from_url(
                &source_url,
                &destination,
                daybook_core::sync::CloneRepoInitOptions::default(),
            )
            .await?;
            daybook_core::repo::upsert_known_repo(&this.inner.sql.db_pool, &out.repo_path).await?;
            Ok::<CloneInitResult, eyre::Report>(CloneInitResult {
                repo_path: out.repo_path.display().to_string(),
                bootstrap: bootstrap_to_ffi(out.bootstrap),
            })
        })
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(err, skip(self, destination))]
    async fn check_clone_destination(
        self: Arc<Self>,
        destination: String,
    ) -> Result<CloneDestinationCheck, FfiError> {
        let this = Arc::clone(&self);
        self.do_on_rt(async move {
            let path = resolve_clone_destination_for_app(&this.inner, &destination)?;
            let exists = path.exists();
            if !exists {
                return Ok::<CloneDestinationCheck, eyre::Report>(CloneDestinationCheck {
                    exists: false,
                    is_dir: false,
                    is_empty: true,
                });
            }
            let is_dir = path.is_dir();
            let is_empty = if is_dir {
                let mut entries = std::fs::read_dir(&path)?;
                entries.next().is_none()
            } else {
                false
            };
            Ok::<CloneDestinationCheck, eyre::Report>(CloneDestinationCheck {
                exists,
                is_dir,
                is_empty,
            })
        })
        .await
        .map_err(Into::into)
    }
}

impl AppFfiCtx {
    pub async fn do_on_rt<O, F>(&self, future: F) -> O
    where
        O: Send + Sync + 'static,
        F: std::future::Future<Output = O> + Send + 'static,
    {
        do_on_rt(&self.rt, future).await
    }
}
