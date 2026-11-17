use crate::interlude::*;

/// Sets up a new node directory: the directory itself and `node.json` with a fresh identity.
/// Nothing is booted; the node's databases are created by its first boot.
///
/// Idempotent: a directory that already holds a valid node is reported and left exactly as it
/// is, so running this twice cannot change a node's identity. A directory that holds something
/// that is not a node (an unreadable or unparsable `node.json`, a version this `day` does not
/// understand) is an error, and is not touched either.
pub async fn run(dir: Option<PathBuf>, relay: bool) -> Res<ExitCode> {
    let node_dir = match dir {
        Some(dir) => {
            crate::context::NodeDir::new(crate::config::CliConfig::source(Some(&dir))?.node_path)
        }
        None => lazy::config().await?.node_dir.clone(),
    };
    if node_dir.is_initialized() {
        // `settings` is the validity check: it parses the file and its version.
        let settings = node_dir.settings().wrap_err_with(|| {
            format!(
                "{} exists but is not a valid node; refusing to touch it",
                node_dir.root().display()
            )
        })?;
        println!(
            "node {} already initialized at {}",
            settings.node_id(),
            node_dir.root().display()
        );
        return Ok(ExitCode::SUCCESS);
    }
    node_dir.init()?;
    if relay {
        node_dir.update_settings(|settings| {
            settings.relay = true;
            settings.peers.get_or_insert_with(default);
            Ok(())
        })?;
    }
    let settings = node_dir.settings()?;
    println!(
        "initialized node {} at {}",
        settings.node_id(),
        node_dir.root().display()
    );
    Ok(ExitCode::SUCCESS)
}
