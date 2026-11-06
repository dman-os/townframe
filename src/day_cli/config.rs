use crate::interlude::*;

/// Where the node lives and nothing else: everything about the node itself is read from the
/// directory (see [`crate::context::NodeDir`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    pub node_path: PathBuf,
}

impl CliConfig {
    /// Resolves the node directory. Precedence: the `--node` flag, then `DAY_NODE` (clap folds
    /// both into `node_flag`), then the platform data directory.
    pub fn source(node_flag: Option<&Path>) -> Res<Self> {
        let cwd = std::env::current_dir()?;
        let node_path = match node_flag {
            Some(path) => std::path::absolute(cwd.join(path))
                .wrap_err_with(|| format!("error absolutizing node path {path:?}"))?,
            None => {
                let dirs = directories::ProjectDirs::from("org", "townframe", "day")
                    .ok_or_eyre("failed to get xdg directories")?;
                dirs.data_dir().into()
            }
        };
        Ok(Self { node_path })
    }
}
