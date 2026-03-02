use crate::interlude::*;

pub struct Config {
    pub cli_config: Arc<crate::config::CliConfig>,
    pub global_ctx: Arc<daybook_core::app::GlobalCtx>,
}

impl Config {
    pub async fn is_repo_initialized(&self) -> Res<bool> {
        daybook_core::repo::is_repo_initialized(&self.cli_config.repo_path).await
    }

    pub async fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        let global_ctx = Arc::new(daybook_core::app::GlobalCtx::new().await?);
        Ok(Self {
            cli_config,
            global_ctx,
        })
    }
}

pub type Ctx = daybook_core::repo::RepoCtx;
pub type SharedCtx = Arc<Ctx>;

pub async fn open_repo_ctx(
    config: &Config,
    ensure_initialized: bool,
    ws_connector_url: Option<String>,
) -> Res<SharedCtx> {
    Ok(Arc::new(
        daybook_core::repo::RepoCtx::open(
            &config.global_ctx,
            &config.cli_config.repo_path,
            daybook_core::repo::RepoOpenOptions {
                ensure_initialized,
                ws_connector_url,
            },
            format!("daybook-cli-{}", std::env::consts::ARCH),
        )
        .await?,
    ))
}
