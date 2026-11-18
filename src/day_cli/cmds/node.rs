use crate::interlude::*;

/// The node itself: its directory and its background daemon.
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
    },
    /// Serve the node on its socket in the foreground until SIGTERM or Ctrl-C
    Run,
    /// Start a background daemon and return once it is serving
    Start,
    /// Stop the background daemon and return once it has exited
    Stop,
    /// Show the node's id, how it was reached, and what it runs (exit status 3 when no daemon
    /// is serving it)
    Status {
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(command: NodeCommands) -> Res<ExitCode> {
    match command {
        NodeCommands::Init { dir, relay } => super::init::run(dir, relay).await,
        NodeCommands::Run => super::serve::run(super::serve::ServeCommands::Run).await,
        NodeCommands::Start => super::serve::run(super::serve::ServeCommands::Start).await,
        NodeCommands::Stop => super::serve::run(super::serve::ServeCommands::Stop).await,
        NodeCommands::Status { json } => super::status::run(json).await,
    }
}
