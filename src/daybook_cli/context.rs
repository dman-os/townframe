use crate::interlude::*;

pub struct Config {
    pub cli_config: Arc<crate::config::CliConfig>,
}

impl Config {
    pub async fn is_repo_initialized(&self) -> Res<bool> {
        daybook_core::repo::is_repo_initialized(&self.cli_config.repo_path).await
    }

    pub async fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        Ok(Self { cli_config })
    }
}

pub type Ctx = daybook_core::repo::RepoCtx;
pub type SharedCtx = Arc<Ctx>;

pub async fn open_repo_ctx(config: &Config, ensure_initialized: bool) -> Res<SharedCtx> {
    let options = daybook_core::repo::RepoOpenOptions {};
    let local_device_name = format!("daybook-cli-{}", std::env::consts::ARCH);
    let rcx = if ensure_initialized
        && !daybook_core::repo::is_repo_initialized(&config.cli_config.repo_path).await?
    {
        daybook_core::repo::RepoCtx::init(&config.cli_config.repo_path, options, local_device_name)
            .await?
    } else {
        daybook_core::repo::RepoCtx::open(&config.cli_config.repo_path, options, local_device_name)
            .await?
    };
    Ok(Arc::new(rcx))
}
