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

fn write_atomically(path: &Path, bytes: &[u8]) -> Res<()> {
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
    let session_path = node_dir
        .join("intxt/sessions")
        .join(format!("{}.json", hook.session_id));
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
