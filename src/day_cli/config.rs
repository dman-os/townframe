use crate::interlude::*;

/// Where the node lives and nothing else: everything about the node itself is read from the
/// directory (see [`day_node::NodeDir`]).
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
            None => day_node::NodeDir::default_root()?,
        };
        Ok(Self { node_path })
    }
}
