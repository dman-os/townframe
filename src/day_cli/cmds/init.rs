use crate::interlude::*;

/// Sets up a new node directory: the directory itself and `node.json` with a fresh identity.
/// Nothing is booted; the node's databases are created by its first boot.
///
/// An existing node is refused, not reused and not touched, so running this twice cannot change
/// a node's identity.
pub async fn run(dir: Option<PathBuf>) -> Res<ExitCode> {
    let node_dir = match dir {
        Some(dir) => {
            crate::context::NodeDir::new(crate::config::CliConfig::source(Some(&dir))?.node_path)?
        }
        None => lazy::config().await?.node_dir.clone(),
    };
    if node_dir.is_initialized() {
        error!(path = %node_dir.root().display(), "a node is already initialized here");
        return Ok(ExitCode::FAILURE);
    }
    let settings = node_dir.init()?;
    println!(
        "initialized node {} at {}",
        settings.node_id(),
        node_dir.root().display()
    );
    Ok(ExitCode::SUCCESS)
}
