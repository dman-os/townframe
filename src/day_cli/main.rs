#![recursion_limit = "256"]

#[cfg(not(unix))]
compile_error!(
    "day_cli needs Unix domain sockets and file locks; there is no Windows transport yet"
);

mod interlude {
    pub use utils_rs::prelude::Arc;
    pub use utils_rs::prelude::Deserialize;
    pub use utils_rs::prelude::ERROR_MUTEX;
    pub use utils_rs::prelude::ERROR_TOKIO;
    pub use utils_rs::prelude::EyreOptExt;
    pub use utils_rs::prelude::Path;
    pub use utils_rs::prelude::PathBuf;
    pub use utils_rs::prelude::Res;
    pub use utils_rs::prelude::Serialize;
    pub use utils_rs::prelude::WrapErr;
    pub use utils_rs::prelude::debug;
    pub use utils_rs::prelude::default;
    pub use utils_rs::prelude::error;
    pub use utils_rs::prelude::eyre;
    pub use utils_rs::prelude::json;
    pub use utils_rs::prelude::warn;

    pub use std::process::ExitCode;

    pub(crate) use crate::lazy;
}

use crate::interlude::*;

use clap::builder::styling::AnsiColor;
use clap::*;

mod client;
mod cmds;
mod config;
mod context;
mod e2e;
mod lazy;

fn main() -> Res<ExitCode> {
    // dotenv_flow::dotenv_flow().ok();
    utils_rs::setup_tracing()?;

    // the static cli is for commands that
    // can be executed without having to
    // build up the dynamic sections of
    // the CLI into clap reprs
    let static_res = match try_static_cli() {
        Ok(StaticCliResult::Exit(code)) => {
            let shutdown_res = lazy::rt().block_on(lazy::shutdown());
            if code == ExitCode::SUCCESS {
                shutdown_res?;
            } else if let Err(err) = shutdown_res {
                warn!(?err, "shutdown failed after command failure");
            }
            return Ok(code);
        }
        Ok(val) => val,
        Err(err) => {
            let shutdown_res = lazy::rt().block_on(lazy::shutdown());
            if let Err(shutdown_err) = shutdown_res {
                warn!(?shutdown_err, "shutdown failed after command error");
            }
            return Err(err);
        }
    };

    let res = lazy::rt().block_on(async {
        tokio::select! {
            // FIXME: is this a good usage of ctrl_c?
            _ = tokio::signal::ctrl_c() => {
                Ok(ExitCode::FAILURE)
            }
            res = dynamic_cli(static_res) => res,
        }
    });
    let shutdown_res = lazy::rt().block_on(lazy::shutdown());
    match res {
        Ok(code) if code == ExitCode::SUCCESS => {
            shutdown_res?;
            Ok(code)
        }
        Ok(code) => {
            if let Err(err) = shutdown_res {
                warn!(?err, "shutdown failed after command failure");
            }
            Ok(code)
        }
        Err(err) => {
            if let Err(shutdown_err) = shutdown_res {
                warn!(?shutdown_err, "shutdown failed after command error");
            }
            Err(err)
        }
    }
}

fn try_static_cli() -> Res<StaticCliResult> {
    let cli = match Cli::try_parse() {
        Err(err) => {
            let kind = err.kind();
            use clap::error::ErrorKind;
            // these might be possible on the dynamic
            // cli so we don't abort immediately
            if kind == ErrorKind::InvalidSubcommand
                || kind == ErrorKind::InvalidValue
                || kind == ErrorKind::DisplayHelp
                || kind == ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            {
                return Ok(StaticCliResult::ClapErr(err));
            }
            err.exit();
        }
        Ok(args) => args,
    };

    match cli.command {
        StaticCommands::Completions { shell } => {
            // don't handle completions now or the dynamic_cli
            // won't be included
            return Ok(StaticCliResult::Completions(shell));
        }
        _ => {
            // the rest of the commands can be statically handled
        }
    }

    lazy::set_node_flag(cli.node.clone());
    // The foreground daemon shuts its node down on SIGINT itself; racing it against Ctrl-C here
    // would drop the serving future with the node still booted.
    let owns_signals = matches!(
        cli.command,
        StaticCommands::Daemon {
            command: cmds::daemon::DaemonCommands::Run
        }
    );
    lazy::rt()
        .block_on(async {
            if owns_signals {
                return static_cli(cli).await;
            }
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    Ok(ExitCode::FAILURE)
                }
                res = static_cli(cli) => res,
            }
        })
        .map(StaticCliResult::Exit)
}

async fn static_cli(cli: Cli) -> Res<ExitCode> {
    if let StaticCommands::Init { dir, relay } = &cli.command {
        return cmds::init::run(dir.clone(), *relay).await;
    }

    let conf = lazy::config().await?;
    if !conf.node_dir.is_initialized() {
        error!(
            path = ?conf.cli_config.node_path,
            "node not initialized at resolved path; run `day init`",
        );
        return Ok(ExitCode::FAILURE);
    }

    match cli.command {
        StaticCommands::Init { .. } | StaticCommands::Completions { .. } => unreachable!(),
        StaticCommands::Daemon { command } => cmds::daemon::run(command).await,
        StaticCommands::Peers { command } => cmds::peers::run(command).await,
        StaticCommands::Status { json } => cmds::status::run(json).await,
        StaticCommands::Exec { nsid, input } => cmds::exec::run(nsid, input).await,
    }
}

/// The part of the CLI that is built from the node rather than compiled in. Nothing is built from
/// the node yet, so this resolves what the static CLI deferred: help and unknown-subcommand
/// errors, and completions. Subcommands derived from the node's nodules would attach to
/// `root_cmd` here, after the node is known to be initialized, as daybook's `exec` did for plugs.
async fn dynamic_cli(static_res: StaticCliResult) -> Res<ExitCode> {
    let mut root_cmd = Cli::command();
    Ok(static_res.exit(Some(&mut root_cmd)))
}

const CLAP_STYLE: clap::builder::Styles = clap::builder::Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Debug, clap::Parser)]
#[clap(
    name = "day",
    version,
    about,
    styles = CLAP_STYLE
)]
struct Cli {
    /// The node's directory
    #[arg(long, global = true, env = "DAY_NODE", value_name = "DIR")]
    node: Option<PathBuf>,
    #[clap(subcommand)]
    command: StaticCommands,
}

#[derive(Debug, clap::Subcommand)]
enum StaticCommands {
    /// Set up a new node directory with a fresh identity
    Init {
        /// The directory to create the node in (default: the --node directory)
        dir: Option<PathBuf>,
        /// Run the node as a relay: it accepts any peer and mirrors the drawers it is made a
        /// member of
        #[arg(long)]
        relay: bool,
    },
    /// Run, start, stop and inspect the node's background daemon
    Daemon {
        #[clap(subcommand)]
        command: cmds::daemon::DaemonCommands,
    },
    /// Manage the peers the node's daemon dials at start
    Peers {
        #[clap(subcommand)]
        command: cmds::peers::PeersCommands,
    },
    /// Show the node's id, how it was reached, and what it runs
    Status {
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
    /// Execute an operation through the node's dispatch router and print the JSON answer
    #[clap(visible_alias = "x")]
    Exec {
        /// The operation's NSID, such as org.example.daybook.nodules.listNodules
        nsid: String,
        /// The operation's input as JSON (default: {})
        input: Option<String>,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
}

enum StaticCliResult {
    ClapErr(clap::Error),
    Exit(ExitCode),
    Completions(clap_complete::Shell),
}

impl StaticCliResult {
    /// Used for deferred exit after we've built the full cli
    fn exit(self, cmd: Option<&mut clap::Command>) -> ExitCode {
        use clap::CommandFactory;
        use clap_complete::aot::generate;
        match self {
            StaticCliResult::ClapErr(err) => err.exit(),
            StaticCliResult::Completions(shell) => {
                let mut stdout = std::io::stdout();
                generate(
                    shell,
                    cmd.unwrap_or(&mut Cli::command()),
                    "day".to_string(),
                    &mut stdout,
                );
                ExitCode::SUCCESS
            }
            StaticCliResult::Exit(_) => unreachable!("can't happen"),
        }
    }
}
