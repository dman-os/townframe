//! The CLI's view of the node it works on: its configuration and the node directory, which is
//! [`day_node::NodeDir`].

use crate::interlude::*;

use day_node::NodeDir;

pub struct Config {
    pub cli_config: Arc<crate::config::CliConfig>,
    pub node_dir: NodeDir,
}

impl Config {
    pub fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        let node_dir = NodeDir::new(cli_config.node_path.clone());
        Ok(Self {
            cli_config,
            node_dir,
        })
    }
}
