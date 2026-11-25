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
    pub use utils_rs::prelude::eyre;
    pub use utils_rs::prelude::json;
    pub use utils_rs::prelude::warn;

    pub use std::io::Write;
    pub use std::process::ExitCode;

    pub(crate) use crate::lazy;
}

use crate::interlude::*;

use clap::builder::styling::AnsiColor;
use clap::*;
use std::ffi::OsString;

mod client;
mod cmds;
mod config;
mod context;
mod e2e;
mod lazy;
mod out;

shadow_rs::shadow!(build);

/// Report an escaped error ourselves instead of returning it from `main`. The standard library
/// prints a returned error as `Error: {err:?}`, and color-eyre's `Debug` begins every chain entry
/// with a newline (it expects its own header line above it), so the message would land on the
/// second line under a bare `Error:`. The alternate `Display` puts the message and its causes on
/// the first line.
fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) if out::is_stdout_closed(&err) => ExitCode::SUCCESS,
        Err(err) => {
            // A closed stderr leaves nowhere to report to, so the failed write is dropped and the
            // exit status carries the failure.
            writeln!(std::io::stderr(), "Error: {err:#}").ok();
            ExitCode::FAILURE
        }
    }
}

fn run() -> Res<ExitCode> {
    // dotenv_flow::dotenv_flow().ok();
    let parsed = parse_cli(std::env::args_os().collect());
    // Tracing is the server's log: only the foreground server, which `node start --bg` runs and
    // whose stderr is its log file, narrates at `info`. Every other command prints its results
    // through `out` and shows only warnings on stderr; an explicit `RUST_LOG` overrides both.
    let serves = matches!(
        &parsed,
        Ok(Cli {
            command: StaticCommands::Node {
                command: cmds::node::NodeCommands::Start { bg: false }
            },
            ..
        })
    );
    // The serving node also keeps a rolling JSON-lines log in `<node dir>/logs`, so a run nobody
    // watched leaves something to read. A short command's warnings stay on stderr only.
    let log_file = match &parsed {
        Ok(cli) if serves => Some(utils_rs::tracing::LogFile {
            directory: config::CliConfig::source(cli.node.as_deref())?
                .node_path
                .join("logs"),
            prefix: "day",
        }),
        _ => None,
    };
    utils_rs::tracing::setup_tracing_at(if serves { "info" } else { "warn" }, log_file)?;

    // the static cli is for commands that
    // can be executed without having to
    // build up the dynamic sections of
    // the CLI into clap reprs
    let static_res = match try_static_cli(parsed) {
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

/// Parses `args` as `day`'s command line, or as `day x intxt`'s when `args[0]` is named `intxt`
/// or `day-intxt`.
///
/// `intxt` is `day` linked under another name (`ln -s "$(command -v day)" ~/.local/bin/intxt`),
/// so that the hooks recording agent sessions call a command whose name outlives how `day`'s
/// commands are arranged. It is temporary: a stable `intxt` now, until the platform and product
/// command line settle. `intxt <args>` is exactly `day x intxt <args>`. Parsing is `day`'s, whose
/// usage lines would say `day exec intxt`, so a help or failed parse is redone on [`IntxtCli`],
/// which has the same arguments and says `intxt`.
fn parse_cli(args: Vec<std::ffi::OsString>) -> Result<Cli, clap::Error> {
    let (rewritten, invoked_as_intxt) = multicall(args.clone());
    match (Cli::try_parse_from(rewritten), invoked_as_intxt) {
        (Err(day_error), true) => Err(IntxtCli::try_parse_from(args).err().unwrap_or(day_error)),
        (parsed, _) => parsed,
    }
}

// The command line of `intxt`, for the help and errors of [`parse_cli`]. A plain comment: clap
// would show a doc comment as the program's description.
#[derive(Debug, clap::Parser)]
#[clap(
    name = "intxt",
    version,
    about = "intxt: what was done, why, and how to review it",
    styles = CLAP_STYLE
)]
struct IntxtCli {
    /// The node's directory
    #[arg(long = "node", global = true, env = "DAY_NODE", value_name = "DIR")]
    _node: Option<PathBuf>,
    #[clap(subcommand)]
    _command: cmds::intxt::IntxtCommands,
}

/// When `args[0]`'s file name is `intxt` or `day-intxt` (the name `day intxt` finds among the
/// `PATH` plugins), the arguments of `day x intxt <rest>`, and true. The program name becomes
/// `day` and `x intxt` goes before the rest, so the global `--node` is read wherever the caller
/// put it.
fn multicall(mut args: Vec<std::ffi::OsString>) -> (Vec<std::ffi::OsString>, bool) {
    let invoked_as_intxt = args.first().is_some_and(|arg0| {
        Path::new(arg0)
            .file_name()
            .is_some_and(|name| name == "intxt" || name == "day-intxt")
    });
    if invoked_as_intxt {
        args.splice(0..1, ["day", "x", "intxt"].map(Into::into));
    }
    (args, invoked_as_intxt)
}

fn try_static_cli(parsed: Result<Cli, clap::Error>) -> Res<StaticCliResult> {
    let cli = match parsed {
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
    // The foreground server shuts its node down on SIGINT itself; racing it against Ctrl-C here
    // would drop the serving future with the node still booted.
    let owns_signals = matches!(
        cli.command,
        StaticCommands::Node {
            command: cmds::node::NodeCommands::Start { bg: false }
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
    if let StaticCommands::Plugin(args) = &cli.command {
        // On success the plugin replaces this process; only a missing plugin returns.
        cmds::plugin::exec(args).await?;
        Cli::command()
            .error(
                clap::error::ErrorKind::InvalidSubcommand,
                format!(
                    "unrecognized subcommand '{name}', and no `day-{name}` executable was found on PATH",
                    name = args[0].to_string_lossy()
                ),
            )
            .print()?;
        return Ok(ExitCode::from(2));
    }
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
        command: cmds::node::NodeCommands::Init { dir, relay, json },
    } = &cli.command
    {
        return cmds::init::run(dir.clone(), *relay, *json).await;
    }

    #[cfg(feature = "docs")]
    if let StaticCommands::Docs { command } = cli.command {
        return cmds::docs::run(command);
    }

    let conf = lazy::config().await?;
    if !conf.node_dir.is_initialized() {
        eyre::bail!(
            "node not initialized at {}; run `day node init`",
            conf.cli_config.node_path.display()
        );
    }

    match cli.command {
        StaticCommands::Completions { .. } | StaticCommands::Plugin(_) => unreachable!(),
        #[cfg(feature = "docs")]
        StaticCommands::Docs { .. } => unreachable!("handled before the node is looked up"),
        StaticCommands::Node { command } => cmds::node::run(command).await,
        StaticCommands::Carts { command } => cmds::carts::run(command).await,
        StaticCommands::Peers { command } => cmds::peers::run(command).await,
        StaticCommands::Drawers { command } => cmds::drawers::run(command).await,
        StaticCommands::Grants { command } => cmds::grants::run(command).await,
        StaticCommands::Dump { command } => cmds::dump::run(command).await,
        StaticCommands::Exec {
            nsid,
            input,
            get,
            body,
            range,
            output,
            command,
        } => match (command, nsid) {
            (Some(ExecCommands::Intxt { command }), _) => cmds::intxt::run(command).await,
            (None, Some(nsid)) => cmds::exec::run(nsid, input, get, body, range, output).await,
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
    static_res.exit(Some(&mut root_cmd))
}

const CLAP_STYLE: clap::builder::Styles = clap::builder::Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

/// What a `day` command exits with. Every command follows it; the table is in `--help`.
const EXIT_CODES: &str = "\
Exit codes:
- 0: success
- 1: failure, the command could not do what was asked (the error is on stderr)
- 2: usage error, the arguments were not understood
- 3: no server running (`day node status`, which still reports what it found)

Output goes to stdout and diagnostics to stderr. Only `day node start` logs as it serves;
the other commands show warnings, and RUST_LOG overrides the level. Colour is used on a
terminal unless NO_COLOR is set.";

#[derive(Debug, clap::Parser)]
#[clap(
    name = "day",
    version,
    long_version = build::CLAP_LONG_VERSION,
    allow_external_subcommands = true,
    about,
    after_long_help = EXIT_CODES,
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
    /// Installed cartridges
    #[clap(visible_alias = "cart")]
    Carts {
        #[clap(subcommand)]
        command: cmds::carts::CartsCommands,
    },
    /// Manage the peers the node's server dials at start
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
    /// What cartridges may reach, and the requests they make for more
    #[clap(visible_alias = "grant")]
    Grants {
        #[clap(subcommand)]
        command: cmds::grants::GrantsCommands,
    },
    /// Raw debug view of what the node holds, as JSON
    Dump {
        #[clap(subcommand)]
        command: cmds::dump::DumpCommands,
    },
    /// Run a method or a cartridge's command, through the node's dispatch router
    ///
    /// `day x <nsid> [input]` calls the method `nsid` and prints the JSON answer. A cartridge's
    /// commands are the subcommands of `day x` (`day x intxt install ..`) and have no other spelling;
    /// they are hard-coded until they are generated from the installed cartridges' command
    /// bindings. An NSID always contains dots, so it cannot be a command's name.
    #[clap(visible_alias = "x", args_conflicts_with_subcommands = true)]
    Exec {
        /// The method's NSID, such as org.example.daybook.nodules.listNodules
        nsid: Option<String>,
        /// The method's input as JSON (default: {})
        input: Option<String>,
        /// Call a query: send the input, an object of primitives, as the query string of a GET
        /// instead of the JSON body of a POST
        #[arg(long)]
        get: bool,
        /// Stream this file as the raw request body of an operation whose input is bytes, such
        /// as org.example.daybook.blobs.put; the input is then its parameters, as a query string
        #[arg(long, value_name = "FILE")]
        body: Option<std::path::PathBuf>,
        /// An HTTP Range, such as bytes=0-99, for an operation that answers part of its bytes
        #[arg(long, value_name = "RANGE")]
        range: Option<String>,
        /// Write an answer that is bytes, such as org.example.daybook.blobs.read, to this file
        #[arg(long, value_name = "FILE")]
        output: Option<std::path::PathBuf>,
        #[clap(subcommand)]
        command: Option<ExecCommands>,
    },
    /// Render the man pages and the Markdown reference (build tooling: `cargo x cli`)
    #[cfg(feature = "docs")]
    #[clap(hide = true)]
    Docs {
        #[clap(subcommand)]
        command: cmds::docs::DocsCommands,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
    /// `day <name> <args…>` runs the plugin `day-<name>` found on `PATH`, with the node's
    /// directory as `DAY_NODE`. Only a name no subcommand above has comes here.
    #[clap(external_subcommand)]
    Plugin(Vec<OsString>),
}

/// The commands of `day x`, one per cartridge.
#[derive(Debug, clap::Subcommand)]
enum ExecCommands {
    /// Record agent work and its git history, and package it (`day x intxt --help`)
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
    fn exit(self, cmd: Option<&mut clap::Command>) -> Res<ExitCode> {
        use clap::CommandFactory;
        use clap_complete::aot::generate;
        match self {
            StaticCliResult::ClapErr(err) => err.exit(),
            StaticCliResult::Completions(shell) => {
                // `generate` panics when its writer fails, so it fills a buffer that `out` then
                // writes, where a closed stdout is an error like any other.
                let mut script = Vec::new();
                generate(
                    shell,
                    cmd.unwrap_or(&mut Cli::command()),
                    "day".to_string(),
                    &mut script,
                );
                out::out().write_all(&script)?;
                Ok(ExitCode::SUCCESS)
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
            ..
        } = parse(&["x", "org.example.a.b", "{}"])
        else {
            panic!("`day x org.example.a.b '{{}}'` is an operation call");
        };
        assert_eq!((nsid.as_str(), input.as_str()), ("org.example.a.b", "{}"));
    }

    fn os(args: &[&str]) -> Vec<std::ffi::OsString> {
        args.iter().map(Into::into).collect()
    }

    #[test]
    fn invoked_as_intxt_is_day_x_intxt() {
        for (argv, expected) in [
            (&["intxt", "show"][..], &["day", "x", "intxt", "show"][..]),
            (
                &["/home/o/.local/bin/intxt", "--node", "d", "show"],
                &["day", "x", "intxt", "--node", "d", "show"],
            ),
            (&["intxt"], &["day", "x", "intxt"]),
            (
                &["/usr/bin/day-intxt", "show"],
                &["day", "x", "intxt", "show"],
            ),
        ] {
            assert_eq!(multicall(os(argv)), (os(expected), true));
        }
        for argv in [
            &["day", "x", "intxt", "show"][..],
            &["day", "node", "status"],
            &["intxt-old"],
        ] {
            assert_eq!(multicall(os(argv)), (os(argv), false));
        }
    }

    #[test]
    fn intxt_parses_as_the_intxt_command_with_its_own_program_name() {
        let cli = parse_cli(os(&[
            "intxt",
            "--node",
            "d",
            "record",
            "install",
            "claude-code",
        ]))
        .unwrap();
        assert_eq!(cli.node, Some(PathBuf::from("d")));
        let StaticCommands::Exec {
            command:
                Some(ExecCommands::Intxt {
                    command: cmds::intxt::IntxtCommands::Record { .. },
                }),
            ..
        } = cli.command
        else {
            panic!("`intxt record ..` is `day x intxt record ..`");
        };
        let help = parse_cli(os(&["intxt", "--help"])).unwrap_err();
        assert_eq!(help.kind(), clap::error::ErrorKind::DisplayHelp);
        let help = help.to_string();
        assert!(help.contains("Usage: intxt "), "{help}");
        assert!(!help.contains("day x intxt"), "{help}");
    }

    #[test]
    fn intxt_is_no_built_in_but_a_plugin_name_and_built_ins_win() {
        let StaticCommands::Plugin(args) = parse(&["intxt", "install"]) else {
            panic!("`day intxt` is the plugin `day-intxt`, not a built-in");
        };
        assert_eq!(args, os(&["intxt", "install"]));
        assert!(matches!(
            parse(&["node", "status"]),
            StaticCommands::Node { .. }
        ));
    }
}
