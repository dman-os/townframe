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
pub(crate) mod review;

use crate::client::NodeClient;
use crate::client::Raw;
use crate::client::Upload;
use crate::interlude::*;

use big_blobs_core::BlobInfo;
use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::authority::Rights;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::registry::version_key;
use big_cartridges_core::registry::version_pin;
use big_dispatch_core::Readiness;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::NoduleRecord;
use intxt_core::client::Caller;
use method_core::Method;
use service_utils_rs::RequestContext;

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
    /// so. Names the intxt by the short id `show` prints, or by its full drawer id, and with
    /// neither, adds to the intxt this worktree is on.
    #[command(
        group = clap::ArgGroup::new("what").required(true),
        after_long_help = "Examples:\n  day x intxt add qx --branch feat/refs\n  day x intxt add qx --commits main..feat/refs\n  day x intxt add qx --change kxqoltvm"
    )]
    Add {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`)
        intxt: Option<String>,
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
    /// Remove entries and comments from an intxt, by the short ids `show` and `comment` print
    ///
    /// Deletes the entries (commits, jj changes, tracked refs, sessions) and the packs only they
    /// carried, and comments (a thread's first comment takes its replies with it). The documents already shared with the intxt stay shared, and copies synced to
    /// others stay with them.
    #[command(after_long_help = "Examples:\n  day x intxt rm qx kq vx")]
    Rm {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
        /// The entries to remove, by short id
        #[arg(required = true)]
        items: Vec<String>,
    },
    /// List every intxt remembered by this node, with counts of its commits, refs and sessions
    ///
    #[command(
        after_long_help = "Examples:\n  day x intxt list\n  day x intxt list --node ~/.local/share/day/node"
    )]
    List,
    /// Forget an intxt on this node
    ///
    /// The drawer and anything already shared stay; it only stops being listed here, and naming it by its full id brings it back.
    #[command(after_long_help = "Examples:\n  day x intxt drop qx\n  day x intxt drop qy")]
    Drop {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
    },
    /// Say which intxts on this node hold a commit
    ///
    /// Names the commit by a full sha, a unique prefix or any name git resolves in this worktree.
    /// Prints one line per intxt that holds it: its short id, its title and its `link:`. Prints
    /// nothing when no intxt does.
    #[command(after_long_help = "Examples:\n  day x intxt which 4c1f9a0")]
    Which {
        /// The commit, as this worktree's git resolves it
        commit: String,
    },
    /// Show what an intxt holds: its commits, jj changes, tracked refs and sessions
    ///
    /// Each entry has a short id, the shortest prefix that is unique among the intxt's entries.
    #[command(after_long_help = "Examples:\n  day x intxt show qx")]
    Show {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`)
        intxt: Option<String>,
    },
    /// Comment on lines of a commit or an op the intxt holds
    ///
    /// The target is `<commit>:<path>:<lines>` (a commit the intxt holds, `4c1f9a0` or any name
    /// git resolves) or `op<seq>:<path>:<lines>` (an uncommitted state of this worktree); lines
    /// are `a-b` or `a`, 1-based in the path's content at that commit or op. Run it in the
    /// worktree whose repository has the commit. The comment records the quoted text, so it
    /// can be found again after a rebase.
    #[command(
        after_long_help = "Examples:\n  day x intxt comment qx 4c1f9a0:src/lib.rs:120-124 -m \"Why not reuse the fragment here?\"\n  day x intxt comment qx 4c1f9a0:src/lib.rs:12 --side old -m \"This line was fine.\""
    )]
    Comment {
        /// The intxt: its short id, or its full drawer id
        intxt: String,
        /// What to comment on: `<commit>:<path>:<lines>` or `op<seq>:<path>:<lines>`
        target: String,
        /// The comment, in Markdown
        #[arg(short, long)]
        message: String,
        /// Count the lines in the commit's parent, as the old side of its diff
        #[arg(long, value_enum, default_value = "new")]
        side: review::Side,
        /// Why: one of W3C Web Annotation's motivations
        #[arg(long, default_value = "commenting", value_parser = clap::builder::PossibleValuesParser::new([
            "assessing", "bookmarking", "classifying", "commenting", "describing", "editing",
            "highlighting", "identifying", "linking", "moderating", "questioning", "tagging",
        ]))]
        motivation: String,
    },
    /// Reply to a comment, and resolve or reopen its thread
    ///
    /// Names the comment by the short id `comment` and `inbox` print. With `--resolve` or
    /// `--reopen` and no message, only the thread's state changes.
    #[command(
        after_long_help = "Examples:\n  day x intxt reply kz -m \"Done in 77b2\" --resolve\n  day x intxt reply kz --reopen"
    )]
    Reply {
        /// The comment to answer
        comment: String,
        /// The reply, in Markdown
        #[arg(short, long, required_unless_present_any = ["resolve", "reopen"])]
        message: Option<String>,
        /// Mark the thread resolved
        #[arg(long, conflicts_with = "reopen")]
        resolve: bool,
        /// Mark the thread unresolved
        #[arg(long)]
        reopen: bool,
    },
    /// List the threads waiting for you and where their comments are on the current revision
    ///
    /// A thread is listed when it is unresolved and you have not seen its latest comments, or
    /// someone else wrote last. Each comment is carried to the newest revision the intxt holds
    /// (same path only): its placed lines, `(context changed)` when the text is there but its
    /// surroundings changed, or `outdated`. Run it in the worktree whose repository has the
    /// commits. Listing marks the threads seen.
    #[command(after_long_help = "Examples:\n  day x intxt inbox qx")]
    Inbox {
        /// The intxt: its short id, or its full drawer id. Default: the one this worktree is on
        /// (`day x intxt use`)
        intxt: Option<String>,
    },
    /// Say which intxt this worktree is on, or put it on another
    ///
    /// `add`, `show` and `inbox` act on that intxt when none is named, and `new` puts the
    /// worktree it runs in on the intxt it creates. Kept in the worktree's local state
    /// (`<git dir>/intxt/state.json`), never synced: each checkout is on its own intxt.
    #[command(
        after_long_help = "Examples:\n  day x intxt use qx\n  day x intxt use\n  day x intxt use --none"
    )]
    Use {
        /// The intxt: its short id, or its full drawer id
        intxt: Option<String>,
        /// Put the worktree on no intxt
        #[arg(long, conflicts_with = "intxt")]
        none: bool,
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
                    .add(&mut std::io::stdout(), intxt.as_deref(), named, full)
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
        IntxtCommands::Comment {
            intxt,
            target,
            message,
            side,
            motivation,
        } => {
            let motivation = serde_json::from_value(serde_json::Value::String(motivation))?;
            with_packaging(async |packaging| {
                packaging
                    .comment(
                        &mut std::io::stdout(),
                        &intxt,
                        &target,
                        &message,
                        side,
                        motivation,
                    )
                    .await
            })
            .await
        }
        IntxtCommands::Reply {
            comment,
            message,
            resolve,
            reopen,
        } => {
            let resolved = match (resolve, reopen) {
                (true, _) => Some(true),
                (_, true) => Some(false),
                _ => None,
            };
            with_packaging(async |packaging| {
                packaging
                    .reply(
                        &mut std::io::stdout(),
                        &comment,
                        message.as_deref(),
                        resolved,
                    )
                    .await
            })
            .await
        }
        IntxtCommands::Inbox { intxt } => {
            with_packaging(async |packaging| {
                packaging
                    .inbox(&mut std::io::stdout(), intxt.as_deref())
                    .await
            })
            .await
        }
        IntxtCommands::Which { commit } => {
            with_packaging(async |packaging| packaging.which(&mut std::io::stdout(), &commit).await)
                .await
        }
        IntxtCommands::Show { intxt } => {
            with_packaging(async |packaging| {
                packaging
                    .show(&mut std::io::stdout(), intxt.as_deref())
                    .await
            })
            .await
        }
        IntxtCommands::Use { intxt, none } => {
            with_packaging(async |packaging| {
                packaging
                    .use_intxt(&mut std::io::stdout(), intxt.as_deref(), none)
                    .await
            })
            .await
        }
        IntxtCommands::List => {
            with_packaging(async |packaging| packaging.list(&mut std::io::stdout()).await).await
        }
        IntxtCommands::Drop { intxt } => {
            with_packaging(async |packaging| packaging.drop(&mut std::io::stdout(), &intxt).await)
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

/// Installs intxt with the handler component `wasm` through ADR 044's and ADR 047's cartridge flow, the way a
/// node hosts any nodule's handler (`big_node`'s `nodule_wasm` e2e): the bytes go into the blob
/// store first, because the registry refuses a version whose components it cannot find and the
/// executor loads them from there when the nodule is enabled; the cartridge's component names them
/// by their BLAKE3. The version's payloads are then staged and the envelope published to the node's registry, installed by the pin of
/// the version just published, granted the methods its handler calls, and enabled.
///
/// The envelope is signed as the fixture namespace's owner, the namespace intxt's cartridge sits
/// under until it has its own (see [`intxt_core::cartridge::cartridge`]).
///
/// Returns the nodule's record. Its readiness says whether the executor loaded the handler: a
/// component the host cannot run is `ExecutorUnavailable`, which is the record's to report, not
/// an error here.
pub(crate) async fn install(client: &NodeClient, wasm: &[u8]) -> Res<NoduleRecord> {
    put_blob(client, wasm).await?;

    let fixture = intxt_core::cartridge::cartridge(wasm);
    let (cartridge_id, version) = (fixture.id.clone(), fixture.version.clone());
    // The payloads (Lexicons, sidecars, components) are datums of the registry document the
    // envelope pins (ADR 047 §1), so they are staged first and the envelope that carries their
    // pins is published, signed as the fixture namespace's owner.
    let staged = call(client, fixture.stage_request()).await?;
    call(
        client,
        PublishCartridgeVersion {
            envelope: fixture.publishable(staged),
        },
    )
    .await?;
    // The pin names the version at the heads of the registry document it was published to.
    let doc_id = call(
        client,
        GetCartridge {
            id: cartridge_id.clone(),
        },
    )
    .await?
    .doc_id;
    let key = version_key(&version);
    let (heads, _) = Caller::new(
        client,
        RequestContext::root(),
        None,
        format!("intxt-install-{cartridge_id}"),
    )
    .read(&doc_id, &key)
    .await?;
    call(
        client,
        InstallCartridge {
            cartridge: version_pin(doc_id, &version, heads),
        },
    )
    .await?;
    for operation in intxt_core::methods::REQUIRED_OPERATIONS {
        call(
            client,
            SetCartridgeGrant {
                cartridge_id: cartridge_id.clone(),
                component: Component::Nodule,
                grant: GrantKey::Method(operation.to_owned()),
                value: Some(GrantBody::method(false)),
            },
        )
        .await?;
    }
    call(
        client,
        SetCartridgeEnabled {
            cartridge_id: cartridge_id.clone(),
            component: Component::Nodule,
            enabled: true,
        },
    )
    .await?;
    let enabled = call(client, GetNodule { cartridge_id }).await?;
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

/// Puts `bytes` in the node's blob store as the operator and answers what the node stored. The
/// bytes are the request body (ADR 042), so the node reads them as a stream and refuses
/// (`BlobsError::TooLarge`) past [`big_blobs_core::MAX_PUT_OCTETS`]; the check here only gives
/// that refusal a message that names the way out.
pub(crate) async fn put_blob(client: &NodeClient, bytes: &[u8]) -> Res<BlobInfo> {
    eyre::ensure!(
        bytes.len() as u64 <= big_blobs_core::MAX_PUT_OCTETS,
        "{} bytes is over the {} octets one blob put carries",
        bytes.len(),
        big_blobs_core::MAX_PUT_OCTETS
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
    let reply = client
        .call_raw(
            PutBlob::XRPC.nsid,
            PutBlob::XRPC.method,
            &serde_json::to_value(PutBlob { capability })?,
            Raw {
                upload: Some(Upload::Bytes(bytes)),
                ..Raw::default()
            },
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "{} failed with {}: {}",
        PutBlob::XRPC.nsid,
        reply.status,
        reply.body
    );
    Ok(serde_json::from_value(reply.body)?)
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
