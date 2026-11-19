use crate::interlude::*;

/// The node itself: its directory and its background server.
#[derive(Debug, clap::Subcommand)]
pub enum NodeCommands {
    /// Set up a new node directory with a fresh identity
    Init {
        /// The directory to create the node in (default: the --node directory)
        dir: Option<PathBuf>,
        /// Run the node as a relay: it accepts any peer and mirrors the drawers it is made a
        /// member of
        #[arg(long)]
        relay: bool,
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
    /// Serve the node on its socket in the foreground until SIGTERM or Ctrl-C
    ///
    /// With `--bg` the node is served by a background server instead, and the command returns
    /// once it is serving; a server that is already running is left alone.
    Start {
        /// Serve from a background server and return once it is serving
        #[arg(long)]
        bg: bool,
    },
    /// Stop the background server and return once it has exited
    Stop,
    /// Show the node's id, how it was reached, and what it runs (exit status 3 when no server
    /// is serving it)
    Status {
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(command: NodeCommands) -> Res<ExitCode> {
    match command {
        NodeCommands::Init { dir, relay, json } => super::init::run(dir, relay, json).await,
        NodeCommands::Start { bg } => super::serve::start(bg).await,
        NodeCommands::Stop => super::serve::stop().await,
        NodeCommands::Status { json } => super::status::run(json).await,
    }
}
