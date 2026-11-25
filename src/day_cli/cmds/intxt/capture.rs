//! `day x intxt capture`: one Claude Code hook invocation (ADR 039 §4). It reads the hook's JSON,
//! observes the session and its worktrees with `intxt_capture`'s host half, records the batch
//! with intxt's recorder (`intxt_core::client`, run here against the node's cabinet methods), and
//! commits the recorder's answer as the host's state. File content is not uploaded: ops name git
//! blobs that the host keeps in the clone (ADR 040 §4).
//!
//! Output goes to stderr only, and only under `--verbose` or for a finding the operator should
//! see: Claude Code adds the stdout of some hooks (`SessionStart`, `UserPromptSubmit`) to the
//! model's context, and capture never feeds the agent.

use super::call;
use crate::client::NodeClient;
use crate::interlude::*;

use big_cabinet_core::ActorUrl;
use big_cabinet_core::authority::ResolveActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use intxt_capture::hook::Hook;
use intxt_capture::host::Prepared;
use intxt_core::capture::RecordedIds;
use intxt_core::client::Caller;
use service_utils_rs::RequestContext;

/// The id of the node's capture drawer and the machine it records for, in
/// `<node dir>/intxt/capture.json`. Written once the drawer exists.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaptureNode {
    drawer: DrawerId,
    /// The operator's actor URL: `gitClone.machine`.
    machine: ActorUrl,
}

/// What a run did, for `--verbose` and the findings worth saying without it.
#[derive(Debug, Default)]
pub(crate) struct Summary {
    /// What the recorder answered; none when the hook observed nothing to record.
    pub recorded: Option<RecordedIds>,
    /// Post hooks whose tool call the transcript did not show yet.
    pub uncaused_tool_calls: u64,
}

/// What git's `post-rewrite` reports.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum RewriteKind {
    Amend,
    Rebase,
}

/// `day x intxt capture <command>`: capture's git hooks.
#[derive(Debug, clap::Subcommand)]
pub(crate) enum CaptureCommands {
    /// Record the commits git's `post-rewrite` hook reports as replaced (stdin: `<old> <new>`
    /// per line) in this worktree's capture
    ///
    /// The command of the `post-rewrite` hook capture installs; git runs it with `amend` or
    /// `rebase`. The pairs are recorded as `rewrite` and `rewrittenFrom` datums beside the
    /// worktree's commits in the capture drawer, where `day x intxt add` finds them. A failure
    /// is one line on stderr and status 1; git ignores it, since `post-rewrite` cannot block.
    #[command(after_long_help = "Example (what the hook runs):
  day x intxt capture git-rewrite amend")]
    GitRewrite {
        /// What was rewritten, git's argument to the hook
        #[arg(value_enum)]
        kind: RewriteKind,
    },
    /// Install capture's `post-rewrite` hook in this worktree's clone, and say what was done
    ///
    /// Capture does this itself the first time it records a clone. A `post-rewrite` hook that is
    /// not capture's is left alone, and `day x intxt status` says so.
    InstallGitHooks,
}

impl CaptureCommands {
    /// Runs `day x intxt capture [command]`. A failure is one line on stderr and status 1: Claude
    /// Code shows it to the operator, and status 2 would be fed back to the model.
    pub(super) async fn run(command: Option<Self>, verbose: bool) -> Res<ExitCode> {
        let outcome = match command {
            None => Self::hook(verbose).await,
            Some(Self::GitRewrite { kind }) => Self::rewritten(kind, verbose).await,
            Some(Self::InstallGitHooks) => Self::install_git_hooks(),
        };
        match outcome {
            Ok(()) => Ok(ExitCode::SUCCESS),
            Err(error) => {
                let line = format!("{error:#}").replace('\n', " ");
                eprintln!("intxt capture: {line}");
                Ok(ExitCode::FAILURE)
            }
        }
    }

    /// The hook's JSON from stdin, captured against the node `--node` names.
    async fn hook(verbose: bool) -> Res<()> {
        let node_dir = lazy::config().await?.node_dir.clone();
        super::require_daemon(&node_dir).await?;
        let mut input = Vec::new();
        std::io::Read::read_to_end(&mut std::io::stdin(), &mut input)
            .wrap_err("reading the hook's JSON from stdin")?;
        let client = lazy::node().await?;
        let summary = run(&client, node_dir.root(), &input).await?;
        Self::report(&summary, verbose);
        Ok(())
    }

    /// `capture git-rewrite`: git's `post-rewrite` stdin, recorded for the worktree git ran the hook
    /// in (the current directory).
    async fn rewritten(kind: RewriteKind, verbose: bool) -> Res<()> {
        let node_dir = lazy::config().await?.node_dir.clone();
        super::require_daemon(&node_dir).await?;
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)
            .wrap_err("reading post-rewrite's pairs from stdin")?;
        let client = lazy::node().await?;
        let cwd = std::env::current_dir()?;
        let recorded = git_rewrite(&client, node_dir.root(), &cwd, &input).await?;
        if verbose {
            eprintln!("intxt capture: recorded {recorded} {kind:?} rewrite(s)");
        }
        Ok(())
    }

    /// `capture install-git-hooks`: installs the hook in the current directory's clone.
    fn install_git_hooks() -> Res<()> {
        let cwd = std::env::current_dir()?;
        let identity = intxt_capture::identity::resolve(&cwd)?.ok_or_else(|| {
            eyre::eyre!(
                "{} is not in a git worktree; run this in the clone to install the hook in",
                cwd.display()
            )
        })?;
        match intxt_capture::host::install_git_hooks(&identity)? {
            intxt_capture::git_hooks::Install::Installed(path) => {
                println!("installed the post-rewrite hook at {}", path.display());
            }
            intxt_capture::git_hooks::Install::AlreadyInstalled(path) => {
                println!(
                    "the post-rewrite hook is already installed at {}",
                    path.display()
                );
            }
            intxt_capture::git_hooks::Install::Foreign(path) => {
                println!(
                    "hook not installed: foreign post-rewrite at {} (left alone; rewrites are matched by patch id)",
                    path.display()
                );
            }
        }
        Ok(())
    }

    /// What a run says on stderr: everything under `--verbose`, otherwise only what the operator
    /// should see (a tool call recorded without its cause).
    fn report(summary: &Summary, verbose: bool) {
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
}

pub(super) fn write_atomically(path: &Path, bytes: &[u8]) -> Res<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

/// A fixed command id, so a run that created the drawer but died before storing its id finds the
/// same drawer on the next run instead of making another.
fn fixed_command(name: &str) -> CommandId {
    CommandId::try_from(format!("intxt-capture-{name}")).expect("a fixed command id")
}

/// The capture drawer: a single-group drawer owned by the operator, synced to no one, holding
/// every document capture writes (ADR 039 §2). Created on first use and remembered in
/// `<node dir>/intxt/capture.json`.
async fn capture_node(client: &NodeClient, node_dir: &Path) -> Res<CaptureNode> {
    let path = node_dir.join("intxt/capture.json");
    match std::fs::read(&path) {
        Ok(bytes) => {
            return serde_json::from_slice(&bytes)
                .wrap_err_with(|| format!("parsing {}", path.display()));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).wrap_err_with(|| format!("reading {}", path.display())),
    }
    let creating = call(
        client,
        MintDrawerCapability {
            capability: None,
            scope: DrawerScope {
                drawers: Default::default(),
                create: true,
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?;
    let drawer = call(
        client,
        CreateDrawer {
            capability: creating.capability_id,
            command: fixed_command("drawer"),
            layout: CreateLayout::SingleGroup,
        },
    )
    .await?
    .drawer_id;
    // The machine is the operator's actor: the subject of the handles the operator mints.
    let operator = creating
        .subject
        .ok_or_else(|| eyre::eyre!("the operator's handle has no actor"))?;
    let machine: ActorUrl = call(client, ResolveActor { actor_id: operator })
        .await?
        .url
        .parse()
        .map_err(|error| eyre::eyre!("the operator's actor URL: {error}"))?;
    let node = CaptureNode { drawer, machine };
    write_atomically(&path, &serde_json::to_vec_pretty(&node)?)?;
    Ok(node)
}

/// The idempotency key of a batch: the session, each worktree's sequence range and the last turn
/// the batch carries, so a retried hook (same state, same observations) sends the same key and a
/// later one a different key.
fn idempotency_key(batch: &intxt_core::capture::Batch) -> String {
    let mut key = batch.session.session_id.clone();
    for worktree in &batch.worktrees {
        key.push_str(&format!(
            "/{}:{}-{}",
            worktree.identity.worktree_id,
            worktree.next_seq,
            worktree.next_seq + worktree.ops.len() as u64
        ));
        for backfill in &worktree.cause_backfills {
            key.push_str(&format!("+cause{}", backfill.seq));
        }
    }
    let turns = &batch.session.imported.turns;
    if let Some(last) = turns.keys().next_back() {
        key.push_str(&format!("/turns:{}:{last}", turns.len()));
    }
    for agent in &batch.session.subagents {
        key.push_str(&format!(
            "/agent:{}:{}",
            agent.agent_id,
            agent.imported.turns.len()
        ));
    }
    key
}

/// The directory of the host's per-session state files: a hook session's, and an ACP session's
/// (`intxt_capture::host::prepare_acp`).
pub(crate) fn sessions_dir(node_dir: &Path) -> PathBuf {
    node_dir.join("intxt/sessions")
}

/// Runs the hook whose JSON is `input`, with the node's capture state under `node_dir`.
pub(crate) async fn run(client: &NodeClient, node_dir: &Path, input: &[u8]) -> Res<Summary> {
    let hook = Hook::parse(input)?;
    eyre::ensure!(
        !hook.session_id.is_empty()
            && hook
                .session_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        "the session id `{}` is not a safe file name",
        hook.session_id
    );
    let session_path = sessions_dir(node_dir).join(format!("{}.json", hook.session_id));
    // Locks and git: blocking work, so off the runtime's threads. The locks are held until the
    // `Prepared` is committed or dropped, whichever thread that is on.
    let prepared =
        tokio::task::spawn_blocking(move || intxt_capture::host::prepare(&hook, &session_path))
            .await??;

    let mut summary = Summary {
        uncaused_tool_calls: prepared.batch.uncaused_tool_calls,
        ..Summary::default()
    };
    let recorded = record_prepared(client, node_dir, prepared).await?;
    summary.recorded = recorded;
    Ok(summary)
}

/// Records the pairs `input` (git's `post-rewrite` stdin) holds for the worktree `cwd` is in, the
/// way a hook's batch is recorded: observed under the worktree's lock, written into the capture
/// drawer. Returns how many pairs git reported.
pub(crate) async fn git_rewrite(
    client: &NodeClient,
    node_dir: &Path,
    cwd: &Path,
    input: &str,
) -> Res<usize> {
    let rewrites = intxt_capture::git_hooks::parse_post_rewrite(input)?;
    if rewrites.is_empty() {
        return Ok(0);
    }
    let identity = intxt_capture::identity::resolve(cwd)?.ok_or_else(|| {
        eyre::eyre!(
            "{} is not in a git worktree; git runs the hook in one",
            cwd.display()
        )
    })?;
    let recorded = rewrites.len();
    let prepared = tokio::task::spawn_blocking(move || {
        intxt_capture::host::prepare_rewrite(&identity, &rewrites)
    })
    .await??;
    record_prepared(client, node_dir, prepared).await?;
    Ok(recorded)
}

/// Records `prepared`'s batch through intxt's `recordObservations` and commits the recorder's
/// answer as the host's state: the one place a batch reaches the node, for hooks and for ACP
/// recordings alike. Returns the answer, or none when the batch was empty and nothing was sent.
pub(crate) async fn record_prepared(
    client: &NodeClient,
    node_dir: &Path,
    prepared: Prepared,
) -> Res<Option<RecordedIds>> {
    if prepared.batch.is_empty() {
        prepared.commit(RecordedIds::default())?;
        return Ok(None);
    }
    let capture = capture_node(client, node_dir).await?;
    let key = idempotency_key(&prepared.batch);
    // The node's operator calls as itself, so the recorder presents no capability. The command ids
    // start from the batch's key: a retried hook (same state, same observations) reuses them.
    let recorded = Caller::new(client, RequestContext::root(), None, key)
        .record_observations(&capture.drawer, &capture.machine, &prepared.batch)
        .await?;
    prepared.commit(recorded.clone())?;
    Ok(Some(recorded))
}
