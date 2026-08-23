use crate::interlude::*;
pub async fn run() -> Res<ExitCode> {
    let conf = lazy::config().await?;

    let is_initialized = conf.is_repo_initialized().await?;
    if is_initialized {
        warn!(
            path = ?conf.cli_config.repo_path,
            "initialized repo already found at path"
        );
        return Ok(ExitCode::SUCCESS);
    }
    let ctx = crate::context::open_repo_ctx(&conf, true).await?;
    ctx.shutdown().await?;
    info!(
        path = ?conf.cli_config.repo_path,
        "repo initialization success"
    );
    Ok(ExitCode::SUCCESS)
}
