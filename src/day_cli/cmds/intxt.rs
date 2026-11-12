//! intxt's commands, the subcommands of `day x` (`day x intxt install`), hard-coded until command
//! bindings are generated from installed cartridges. `day x <nsid>` reaches a cartridge's
//! operations directly (`day x org.example.intxt.createIntxt '{"title": ".."}'`), so a command is
//! only what the operations cannot do yet: `install` stores the handler component's bytes, which
//! no operation takes from a file, and `capture` observes the host (transcript, worktrees, blob
//! files) and records what it saw.
//!
//! # Claude Code hooks
//!
//! `day x intxt capture` is the command of every hook ADR 039 §4 lists. In `.claude/settings.json`
//! (or the user's `~/.claude/settings.json`):
//!
//! ```json
//! {
//!   "hooks": {
//!     "SessionStart":       [{ "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "UserPromptSubmit":   [{ "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "PreToolUse":         [{ "matcher": "*", "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "PostToolUse":        [{ "matcher": "*", "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "PostToolUseFailure": [{ "matcher": "*", "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "Stop":               [{ "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "SubagentStop":       [{ "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "PreCompact":         [{ "hooks": [{ "type": "command", "command": "day x intxt capture" }] }],
//!     "SessionEnd":         [{ "hooks": [{ "type": "command", "command": "day x intxt capture" }] }]
//!   }
//! }
//! ```
//!
//! This shape (event name, a list of `{ matcher?, hooks: [{ type, command }] }`, the hook's JSON on
//! the command's stdin) is written from memory of Claude Code's hooks documentation and has not
//! been checked against it here; verify it before relying on it. Add `--node <dir>` (or set
//! `DAY_NODE`) to name the node, and `"timeout"` to a hook if a recording can take long. The
//! command needs the node's daemon (`day node start`) and fails fast without one: booting the
//! node in each hook's process would take seconds, and parallel tool calls run parallel hooks.

//! # ACP agents
//!
//! `day x intxt record -- <agent command>` records an ACP agent for the client that launches it:
//! point the editor's agent command at it (`day x intxt record -- claude-code-acp`). It forwards
//! the connection unchanged and records session starts and prompt turns (see `record.rs`).
//!
//! `claude-code-acp`, `gemini --experimental-acp` and `codex-acp` are the agents it is meant for;
//! none was run against it here (the tests use a scripted agent), so check the first recording
//! of each.

pub(crate) mod capture;
pub(crate) mod package;
mod record;

use crate::client::NodeClient;
use crate::interlude::*;

use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::authority::Rights;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::SetNoduleMethodGrants;
use method_core::Method;

#[derive(Debug, clap::Subcommand)]
pub enum IntxtCommands {
    /// Admit intxt's cartridge to this node, host its handler, and enable it
    ///
    /// Stores the built handler component in the node's blob store, admits the cartridge that
    /// names it, grants the nodule the methods it calls, and enables it, which registers intxt's records and
    /// serves `createIntxt` and `addToIntxt`.
    Install {
        /// The handler component: `src/wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm`,
        /// built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest
        /// --target wasm32-wasip3 --release`
        #[arg(long)]
        handler: PathBuf,
    },
    /// Add commits, a branch or a jj change to an intxt, with the sessions that produced them
    ///
    /// Run it in the worktree that holds the work. What is named is resolved now, to commits:
    /// a range is stored as its tip and merge base, never as the text you typed. The objects
    /// the commits need are carried as a git pack (thin against the base unless `--full`), in
    /// the node's blob store, and the sessions whose recorded edits produced content in those
    /// commits come with them. Adding something already in the intxt changes nothing and says
    /// so. Names the intxt by the short id `show` prints, or by its full drawer id.
    #[command(
        group = clap::ArgGroup::new("what").required(true),
        after_long_help = "Examples:\n  day x intxt add m4 --branch feat/refs\n  day x intxt add m4 --commits main..feat/refs\n  day x intxt add m4 --change kxqoltvm"
    )]
    Add {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
        /// Commits: a range `main..feat/x` (its tip and its merge base with `main`), or one
        /// commit (against its first parent). Brings those commits and the files they touch
        #[arg(long, group = "what", value_name = "RANGE")]
        commits: Option<String>,
        /// A local branch: its commits since its merge base with its upstream, or with `main`
        /// (`master`) when it has none, and the branch itself, tracked as this clone sees it, so
        /// commits it moves onto later come in as new revisions. Brings what `--commits` brings
        #[arg(long, group = "what", value_name = "NAME")]
        branch: Option<String>,
        /// A jj change, by its change id, followed through its rewrites: every commit the change
        /// has had comes in. Needs `jj`
        #[arg(long, group = "what", value_name = "CHANGE")]
        change: Option<String>,
        /// Pack every object, not only what the base does not have
        ///
        /// The default pack is thin: it assumes whoever receives the intxt already has the base
        /// commit. A full pack stands alone and is several times larger.
        #[arg(long)]
        full: bool,
    },
    /// Create an intxt and remember it, so it has a short id
    ///
    /// Prints the new intxt's short id, which names it in `add`, `show` and `rm`.
    #[command(after_long_help = "Examples:\n  day x intxt new \"Datum refs move to dtxt\"")]
    New {
        /// The intxt's title
        title: String,
        /// The intxt's description, in Markdown
        #[arg(long)]
        description: Option<String>,
    },
    /// Remove entries from an intxt, by the short ids `show` prints
    ///
    /// Deletes the entries (commits, jj changes, tracked refs, sessions) and the packs only they
    /// carried. The documents already shared with the intxt stay shared, and copies synced to
    /// others stay with them.
    #[command(after_long_help = "Examples:\n  day x intxt rm m4 k7 v1")]
    Rm {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
        /// The entries to remove, by short id
        #[arg(required = true)]
        items: Vec<String>,
    },
    /// Show what an intxt holds: its commits, jj changes, tracked refs and sessions
    ///
    /// Each entry has a short id, the shortest prefix that is unique among the intxt's entries.
    #[command(after_long_help = "Examples:\n  day x intxt show m4")]
    Show {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
    },
    /// Say what is recorded in this worktree and which intxts include it
    ///
    /// Run it in a worktree: it lists the ops and sessions capture recorded here, and the
    /// intxts that include this worktree's commits, its clone's tracked refs or its sessions.
    #[command(after_long_help = "Examples:\n  day x intxt status")]
    Status,
    /// Record one Claude Code hook invocation: the hook's JSON on stdin
    ///
    /// Observes the session's transcript and worktrees, puts changed binary files in the node's
    /// blob store, and records everything in the node's capture drawer. Install it as the command
    /// of Claude Code's hooks (see this module's documentation). It prints nothing unless it has
    /// something to report or `--verbose` is given, and then only to stderr; it never exits with
    /// status 2, which Claude Code feeds back to the model.
    Capture {
        /// Say on stderr what was recorded
        #[arg(long)]
        verbose: bool,
    },
    /// Record an ACP session: run the agent command, passing everything through
    ///
    /// An ACP client (an editor) launches this in place of the agent, e.g. `day x intxt record --
    /// claude-code-acp`. It spawns the agent, forwards the client's stdin to it and its stdout to
    /// the client unchanged, and records the session's messages, tool calls and plan, and the edits
    /// each prompt turn made in the session's working directory, in the node's capture drawer.
    /// Needs the node's daemon; without one the session runs unrecorded. A recording that fails
    /// says why on stderr and stops, and the session goes on. Exits with the agent's status.
    Record {
        /// Record the agent's thinking (`agent_thought_chunk`) as well
        #[arg(long)]
        thinking: bool,
        /// The agent's command and arguments
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        agent: Vec<String>,
    },
}

pub async fn run(command: IntxtCommands) -> Res<ExitCode> {
    match command {
        IntxtCommands::Record { thinking, agent } => record::run(agent, thinking).await,
        IntxtCommands::Add {
            intxt,
            commits,
            branch,
            change,
            full,
        } => {
            let named = match (commits, branch, change) {
                (Some(range), None, None) => package::Named::Commits(range),
                (None, Some(branch), None) => package::Named::Branch(branch),
                (None, None, Some(change)) => package::Named::Change(change),
                _ => unreachable!("clap requires exactly one of --commits, --branch and --change"),
            };
            with_packaging(async |packaging| {
                packaging
                    .add(&mut std::io::stdout(), &intxt, named, full)
                    .await
            })
            .await
        }
        IntxtCommands::New { title, description } => {
            with_packaging(async |packaging| {
                packaging
                    .create(&mut std::io::stdout(), &title, description.as_deref())
                    .await
                    .map(|_| ())
            })
            .await
        }
        IntxtCommands::Rm { intxt, items } => {
            with_packaging(async |packaging| {
                packaging.rm(&mut std::io::stdout(), &intxt, &items).await
            })
            .await
        }
        IntxtCommands::Show { intxt } => {
            with_packaging(async |packaging| packaging.show(&mut std::io::stdout(), &intxt).await)
                .await
        }
        IntxtCommands::Status => {
            with_packaging(async |packaging| packaging.status(&mut std::io::stdout()).await).await
        }
        IntxtCommands::Install { handler } => {
            let client = lazy::node().await?;
            let wasm = std::fs::read(&handler)
                .wrap_err_with(|| format!("reading the handler {}", handler.display()))?;
            let record = install(&client, &wasm).await?;
            eyre::ensure!(
                record.readiness == Readiness::Ready,
                "intxt is installed but not ready: {:?}",
                record.readiness
            );
            println!("intxt is installed");
            Ok(ExitCode::SUCCESS)
        }
        IntxtCommands::Capture { verbose } => {
            // A failure is one line on stderr and status 1: Claude Code shows it to the operator,
            // and status 2 would be fed back to the model.
            match capture_hook(verbose).await {
                Ok(()) => Ok(ExitCode::SUCCESS),
                Err(error) => {
                    let line = format!("{error:#}").replace('\n', " ");
                    eprintln!("intxt capture: {line}");
                    Ok(ExitCode::FAILURE)
                }
            }
        }
    }
}

/// Runs a packaging verb against the node, in the current directory.
async fn with_packaging(
    verb: impl AsyncFnOnce(&package::Packaging<'_>) -> Res<()>,
) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let cwd = std::env::current_dir()?;
    let config = lazy::config().await?;
    verb(&package::Packaging::new(
        &client,
        &cwd,
        config.node_dir.root(),
    ))
    .await?;
    Ok(ExitCode::SUCCESS)
}

/// Capture runs against the node's daemon only. Booting the node in the hook's own process costs
/// seconds per hook, and parallel tool calls run parallel hooks, of which only one process can
/// hold the node. The socket is probed, not connected to through [`NodeClient`], which would boot
/// the node just to be refused.
pub(crate) async fn require_daemon(dir: &crate::context::NodeDir) -> Res<()> {
    eyre::ensure!(
        dir.probe_socket().await? == crate::context::SocketState::Live,
        "no daemon is serving the node at {}; start it with `day node start`",
        dir.root().display()
    );
    Ok(())
}

/// The hook's JSON from stdin, captured against the node `--node` names.
async fn capture_hook(verbose: bool) -> Res<()> {
    let node_dir = lazy::config().await?.node_dir.clone();
    require_daemon(&node_dir).await?;
    let mut input = Vec::new();
    std::io::Read::read_to_end(&mut std::io::stdin(), &mut input)
        .wrap_err("reading the hook's JSON from stdin")?;
    let client = lazy::node().await?;
    let summary = capture::run(&client, node_dir.root(), &input).await?;
    report(&summary, verbose);
    Ok(())
}

/// What a run says on stderr: everything under `--verbose`, otherwise only what the operator
/// should see (a tool call recorded without its cause).
fn report(summary: &capture::Summary, verbose: bool) {
    if summary.uncaused_tool_calls > 0 {
        eprintln!(
            "intxt capture: {} op(s) of a tool call the transcript did not show yet were recorded without a cause",
            summary.uncaused_tool_calls
        );
    }
    if verbose {
        eprintln!(
            "intxt capture: {}",
            if summary.recorded.is_some() {
                "recorded"
            } else {
                "nothing to record"
            }
        );
    }
}

/// Installs intxt with the handler component `wasm`, the way a node hosts any nodule's handler
/// (`big_node`'s `nodule_wasm` e2e): the bytes go into the blob store first, so the executor finds
/// them when the nodule is enabled; the cartridge's artifact names them by their BLAKE3.
///
/// Returns the nodule's record. Its readiness says whether the executor loaded the handler: a
/// component the host cannot run is `ExecutorUnavailable`, which is the record's to report, not
/// an error here.
pub(crate) async fn install(client: &NodeClient, wasm: &[u8]) -> Res<NoduleRecord> {
    eyre::ensure!(
        wasm.len() <= big_blobs_core::MAX_PUT_BYTES,
        "the handler is {} bytes, over the {} one blob put carries",
        wasm.len(),
        big_blobs_core::MAX_PUT_BYTES
    );
    let capability = call(
        client,
        MintBlobsCapability {
            scope: BlobsScope {
                put: true,
                ..BlobsScope::default()
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id;
    call(
        client,
        PutBlob {
            capability,
            bytes: wasm.to_vec().into(),
        },
    )
    .await?;

    let cartridge = intxt_core::cartridge::cartridge(intxt_core::cartridge::handler_artifact(wasm));
    let cartridge_id = cartridge.id.clone();
    call(
        client,
        AdmitNodule {
            cartridge,
            drawers: None,
        },
    )
    .await?;
    call(
        client,
        SetNoduleMethodGrants {
            cartridge_id: cartridge_id.clone(),
            methods: DispatchScope {
                operations: intxt_core::methods::REQUIRED_OPERATIONS
                    .map(str::to_owned)
                    .into(),
                ..DispatchScope::default()
            },
        },
    )
    .await?;
    let enabled = call(
        client,
        SetNoduleEnabled {
            cartridge_id,
            enabled: true,
        },
    )
    .await?;
    // A nodule holds its dispatch capability only while enabled, so whether the grant took is
    // known from the enabled record. Every method intxt's handler calls is grantable, so a refusal
    // means the node and this build disagree about them.
    let grants = enabled
        .methods
        .as_ref()
        .ok_or_else(|| eyre::eyre!("the node reports no method grants for intxt"))?;
    eyre::ensure!(
        grants.granted,
        "the node did not grant intxt the methods its handler calls: {}",
        grants.error.as_deref().unwrap_or("no reason given")
    );
    Ok(enabled)
}

/// Calls `input`'s operation and decodes its answer; a non-success answer is the operation's own
/// error, reported as the node gave it.
pub(crate) async fn call<M>(client: &NodeClient, input: M) -> Res<M::Output>
where
    M: Method,
    M::Output: serde::de::DeserializeOwned,
{
    let reply = client
        .call(M::XRPC.nsid, M::XRPC.method, &serde_json::to_value(input)?)
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "{} failed with {}: {}",
        M::XRPC.nsid,
        reply.status,
        reply.body
    );
    Ok(serde_json::from_value(reply.body)?)
}
