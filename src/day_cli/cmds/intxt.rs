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
//! command needs the node's daemon (`day daemon start`) and fails fast without one: booting the
//! node in each hook's process would take seconds, and parallel tool calls run parallel hooks.

pub(crate) mod capture;

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
use big_nodule_core::protocol::SetNoduleOperationGrants;
use method_core::Method;

#[derive(Debug, clap::Subcommand)]
pub enum IntxtCommands {
    /// Admit intxt's cartridge to this node, host its handler, and enable it
    ///
    /// Stores the built handler component in the node's blob store, admits the cartridge that
    /// names it, grants the nodule the operations it calls, and enables it, which registers intxt's records and
    /// serves `createIntxt`, `addToIntxt` and `recordObservations`.
    ///
    /// The node's `nodule_operations` ceiling must cover the operations the handler calls, or the
    /// grant is refused: raise it first with `day nodules ceiling add --authority <NSID prefix>`
    /// (install prints the exact command) and restart the daemon, which reads the
    /// ceiling at start. Install never edits node.json.
    Install {
        /// The handler component: `src/wasm_utils_rs/target/wasm32-wasip3/release/intxt_guest.wasm`,
        /// built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_guest
        /// --target wasm32-wasip3 --release`
        #[arg(long)]
        handler: PathBuf,
    },
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
}

pub async fn run(command: IntxtCommands) -> Res<ExitCode> {
    match command {
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

/// Capture runs against the node's daemon only. Booting the node in the hook's own process costs
/// seconds per hook, and parallel tool calls run parallel hooks, of which only one process can
/// hold the node. The socket is probed, not connected to through [`NodeClient`], which would boot
/// the node just to be refused.
pub(crate) async fn require_daemon(dir: &crate::context::NodeDir) -> Res<()> {
    eyre::ensure!(
        dir.probe_socket().await? == crate::context::SocketState::Live,
        "no daemon is serving the node at {}; start it with `day daemon start`",
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
/// should see (a blob that was not put, a tool call recorded without its cause).
fn report(summary: &capture::Summary, verbose: bool) {
    if summary.uncaused_tool_calls > 0 {
        eprintln!(
            "intxt capture: {} op(s) of a tool call the transcript did not show yet were recorded without a cause",
            summary.uncaused_tool_calls
        );
    }
    for path in &summary.stale_blobs {
        eprintln!("intxt capture: {path} changed after it was observed; its bytes were not stored");
    }
    for path in &summary.oversize_blobs {
        eprintln!(
            "intxt capture: {path} is over 8 MiB; recorded as changed, its bytes were not stored"
        );
    }
    if verbose {
        eprintln!(
            "intxt capture: {} ({} blob(s) stored)",
            if summary.recorded.is_some() {
                "recorded"
            } else {
                "nothing to record"
            },
            summary.blobs_put
        );
    }
}

/// The `day nodules ceiling add` command that covers `REQUIRED_OPERATIONS`, by the minimal
/// authority prefixes (an operation's NSID without its last segment).
fn ceiling_command() -> String {
    let authorities: std::collections::BTreeSet<&str> = intxt_nodule::REQUIRED_OPERATIONS
        .iter()
        .filter_map(|nsid| nsid.rsplit_once('.').map(|(authority, _)| authority))
        .collect();
    let flags: String = authorities
        .into_iter()
        .map(|authority| format!(" --authority {authority}"))
        .collect();
    format!("day nodules ceiling add{flags}")
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

    let cartridge = intxt_nodule::cartridge(intxt_nodule::handler_artifact(wasm));
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
        SetNoduleOperationGrants {
            cartridge_id: cartridge_id.clone(),
            operations: DispatchScope {
                operations: intxt_nodule::REQUIRED_OPERATIONS.map(str::to_owned).into(),
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
    // known from the enabled record: the node's ceiling (`nodule_operations` in node.json) must
    // cover what intxt's handler calls.
    let grants = enabled
        .operations
        .as_ref()
        .ok_or_else(|| eyre::eyre!("the node reports no operation grants for intxt"))?;
    eyre::ensure!(
        grants.granted,
        "the node did not grant intxt the operations its handler calls ({}): they exceed the node's `nodule_operations` ceiling. Raise it with `{}` and restart the daemon (`day daemon stop`, `day daemon start`), then run install again. (`install` never edits node.json.)",
        grants.error.as_deref().unwrap_or("no reason given"),
        ceiling_command()
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
