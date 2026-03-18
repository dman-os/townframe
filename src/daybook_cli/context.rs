use crate::interlude::*;

pub struct Config {
    pub cli_config: Arc<crate::config::CliConfig>,
    pub acx: Arc<daybook_core::app::AppCtx>,
}

impl Config {
    pub async fn is_repo_initialized(&self) -> Res<bool> {
        daybook_core::repo::is_repo_initialized(&self.cli_config.repo_path).await
    }

    pub async fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        let acx = Arc::new(daybook_core::app::AppCtx::load().await?);
        Ok(Self { cli_config, acx })
    }
}

pub type Ctx = daybook_core::repo::RepoCtx;
pub type SharedCtx = Arc<Ctx>;

pub async fn open_repo_ctx(
    config: &Config,
    ensure_initialized: bool,
) -> Res<SharedCtx> {
    let options = daybook_core::repo::RepoOpenOptions { ..default() };
    let local_device_name = format!("daybook-cli-{}", std::env::consts::ARCH);
    let rcx = if ensure_initialized
        && !daybook_core::repo::is_repo_initialized(&config.cli_config.repo_path).await?
    {
        config
            .acx
            .init_repo(&config.cli_config.repo_path, options, local_device_name)
            .await?
    } else {
        config
            .acx
            .open_repo(&config.cli_config.repo_path, options, local_device_name)
            .await?
    };
    Ok(Arc::new(rcx))
}
