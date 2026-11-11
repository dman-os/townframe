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
        StaticCommands::Node {
            command: cmds::node::NodeCommands::Run
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
    if let StaticCommands::Exec {
        nsid: None,
        command: None,
        ..
    } = &cli.command
    {
        Cli::command()
            .find_subcommand_mut("exec")
            .expect("exec is a subcommand")
            .error(
                clap::error::ErrorKind::MissingRequiredArgument,
                "give a method's NSID or a command (see `day x --help`)",
            )
            .print()?;
        return Ok(ExitCode::from(2));
    }
    if let StaticCommands::Node {
        command: cmds::node::NodeCommands::Init { dir, relay },
    } = &cli.command
    {
        return cmds::init::run(dir.clone(), *relay).await;
    }

    let conf = lazy::config().await?;
    if !conf.node_dir.is_initialized() {
        error!(
            path = ?conf.cli_config.node_path,
            "node not initialized at resolved path; run `day node init`",
        );
        return Ok(ExitCode::FAILURE);
    }

    match cli.command {
        StaticCommands::Completions { .. } => unreachable!(),
        StaticCommands::Node { command } => cmds::node::run(command).await,
        StaticCommands::Peers { command } => cmds::peers::run(command).await,
        StaticCommands::Drawers { command } => cmds::drawers::run(command).await,
        StaticCommands::Exec {
            nsid,
            input,
            get,
            command,
        } => match (command, nsid) {
            (Some(ExecCommands::Intxt { command }), _) => cmds::intxt::run(command).await,
            (None, Some(nsid)) => cmds::exec::run(nsid, input, get).await,
            (None, None) => unreachable!("checked before the node was opened"),
        },
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
    /// Set up, run, stop and inspect the node
    Node {
        #[clap(subcommand)]
        command: cmds::node::NodeCommands,
    },
    /// Manage the peers the node's daemon dials at start
    #[clap(visible_alias = "peer")]
    Peers {
        #[clap(subcommand)]
        command: cmds::peers::PeersCommands,
    },
    /// Share drawers with other nodes
    #[clap(visible_alias = "drawer")]
    Drawers {
        #[clap(subcommand)]
        command: cmds::drawers::DrawersCommands,
    },
    /// Run a method or a cartridge's command, through the node's dispatch router
    ///
    /// `day x <nsid> [input]` calls the method `nsid` and prints the JSON answer. A cartridge's
    /// commands are the subcommands of `day x` (`day x intxt install ..`) and have no other spelling;
    /// they are hard-coded until they are generated from the installed cartridges' command
    /// bindings. An NSID always contains dots, so it cannot be a command's name.
    #[clap(visible_alias = "x", args_conflicts_with_subcommands = true)]
    Exec {
        /// The operation's NSID, such as org.example.daybook.nodules.listNodules
        nsid: Option<String>,
        /// The operation's input as JSON (default: {})
        input: Option<String>,
        /// Call a query: send the input, an object of primitives, as the query string of a GET
        /// instead of the JSON body of a POST
        #[arg(long)]
        get: bool,
        #[clap(subcommand)]
        command: Option<ExecCommands>,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
}

/// The commands of `day x`, one per cartridge.
#[derive(Debug, clap::Subcommand)]
enum ExecCommands {
    /// Install intxt, which the nodule's operations (`day x org.example.intxt.createIntxt`, ..) then serve
    Intxt {
        #[clap(subcommand)]
        command: cmds::intxt::IntxtCommands,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> StaticCommands {
        Cli::try_parse_from(std::iter::once("day").chain(args.iter().copied()))
            .unwrap()
            .command
    }

    #[test]
    fn x_dispatches_a_command_or_an_nsid() {
        let StaticCommands::Exec {
            nsid: None,
            command:
                Some(ExecCommands::Intxt {
                    command: cmds::intxt::IntxtCommands::Install { handler },
                }),
            ..
        } = parse(&[
            "x",
            "intxt",
            "install",
            "--handler",
            "intxt_nodule_guest.wasm",
        ])
        else {
            panic!("`day x intxt install` is the intxt command");
        };
        assert_eq!(handler, PathBuf::from("intxt_nodule_guest.wasm"));
        let StaticCommands::Exec {
            nsid: Some(nsid),
            input: Some(input),
            command: None,
            get: false,
        } = parse(&["x", "org.example.a.b", "{}"])
        else {
            panic!("`day x org.example.a.b '{{}}'` is an operation call");
        };
        assert_eq!((nsid.as_str(), input.as_str()), ("org.example.a.b", "{}"));
    }

    #[test]
    fn intxt_is_no_top_level_command() {
        assert!(Cli::try_parse_from(["day", "intxt", "install"]).is_err());
    }
}
