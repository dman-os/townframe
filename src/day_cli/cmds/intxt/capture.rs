//! `day x intxt capture`: one Claude Code hook invocation (ADR 039 §4). It reads the hook's JSON,
//! observes the session and its worktrees with `intxt_capture`'s host half, puts the bytes of
//! blob-classified files in the node's blob store (ledger D45), records the batch through intxt's
//! `recordObservations` operation, and commits the recorder's answer as the host's state.
//!
//! Output goes to stderr only, and only under `--verbose` or for a finding the operator should
//! see: Claude Code adds the stdout of some hooks (`SessionStart`, `UserPromptSubmit`) to the
//! model's context, and capture never feeds the agent.

use super::call;
use crate::client::NodeClient;
use crate::interlude::*;

use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::ActorUrl;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::ResolveActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use intxt_capture::hook::Hook;
use intxt_capture::host::Prepared;
use intxt_core::capture::BlobRef;
use intxt_core::capture::ContentClass;
use intxt_core::capture::RecordedIds;
use method_core::HttpMethod;
use std::collections::BTreeMap;

/// The id of the node's capture drawer and the machine it records for, in
/// `<node dir>/intxt/capture.json`. Written once the drawer and its blob inventory exist.
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
    pub blobs_put: usize,
    /// Blob files that changed after they were observed, so their bytes were not put.
    pub stale_blobs: Vec<String>,
    /// Blob files over 8 MiB, recorded as changed with their size and hash only.
    pub oversize_blobs: Vec<String>,
    /// Post hooks whose tool call the transcript did not show yet.
    pub uncaused_tool_calls: u64,
}

/// A file the blob store takes at most this many bytes of (`MAX_PUT_BYTES`).
const MAX_BLOB_BYTES: u64 = big_blobs_core::MAX_PUT_BYTES as u64;

fn mime_of(path: &str) -> &'static str {
    match path
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
    {
        Some(extension) => match extension.as_str() {
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "svg" => "image/svg+xml",
            "pdf" => "application/pdf",
            "zip" => "application/zip",
            "gz" | "tgz" => "application/gzip",
            "wasm" => "application/wasm",
            "json" | "lock" => "application/json",
            "mp3" => "audio/mpeg",
            "mp4" => "video/mp4",
            _ => "application/octet-stream",
        },
        None => "application/octet-stream",
    }
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
/// every document capture writes (ADR 039 §2), with a blob inventory so the blob workers pin what
/// capture puts (D45). Created on first use and remembered in `<node dir>/intxt/capture.json`.
///
/// The inventory's root is `sensitive`: it pins the contents of captured files, which are the
/// operator's work and may hold secrets, so it belongs to the drawer's sensitive group.
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
    let administering = call(
        client,
        MintDrawerCapability {
            scope: DrawerScope {
                drawers: [(drawer.clone(), DrawerAccess::Admin)].into(),
                create: false,
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?;
    call(
        client,
        CreateMetaDoc {
            capability: administering.capability_id,
            command: fixed_command("inventory"),
            drawer_id: drawer.clone(),
            kind: "blobInventory".into(),
            name: "capture".into(),
            root: MetaRoot::Sensitive,
        },
    )
    .await?;
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

/// Puts the bytes of every blob-classified change in `batch` that the node can hold, and fills
/// the change's `content.blob`. A file whose bytes no longer hash to what was observed is left
/// without one (the next observation sees its new state), and so is a file over 8 MiB.
async fn put_blobs(
    client: &NodeClient,
    batch: &mut intxt_core::capture::Batch,
    summary: &mut Summary,
) -> Res<()> {
    let mut put_capability: Option<CapId> = None;
    // Hash → blob, so one run puts equal bytes once.
    let mut put: BTreeMap<String, BlobRef> = BTreeMap::new();
    for worktree in &mut batch.worktrees {
        let root = PathBuf::from(&worktree.identity.root);
        for op in &mut worktree.ops {
            for change in &mut op.changes {
                let Some(content) = &mut change.content else {
                    continue;
                };
                if content.class != ContentClass::Blob || content.blob.is_some() {
                    continue;
                }
                let (Some(size), Some(hash)) = (content.size, content.hash.clone()) else {
                    continue;
                };
                let path = root.join(&change.path);
                if size > MAX_BLOB_BYTES {
                    summary.oversize_blobs.push(path.display().to_string());
                    continue;
                }
                if let Some(blob) = put.get(&hash) {
                    content.blob = Some(blob.clone());
                    continue;
                }
                let read_root = root.clone();
                let read_path = change.path.clone();
                let observed = hash.clone();
                // Read and hash off the runtime's threads: a file and a `git` child.
                let bytes = tokio::task::spawn_blocking(move || -> Res<Option<Vec<u8>>> {
                    let bytes = match std::fs::read(read_root.join(&read_path)) {
                        Ok(bytes) => bytes,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(None);
                        }
                        Err(error) => return Err(error.into()),
                    };
                    // The hash git gives the bytes as the index would store them, which is what
                    // the observation recorded.
                    let output = std::process::Command::new("git")
                        .arg("-C")
                        .arg(&read_root)
                        .args(["hash-object", "--", &read_path])
                        .output()?;
                    eyre::ensure!(
                        output.status.success(),
                        "git hash-object {read_path}: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                    Ok((String::from_utf8_lossy(&output.stdout).trim() == observed)
                        .then_some(bytes))
                })
                .await??;
                let Some(bytes) = bytes else {
                    summary.stale_blobs.push(path.display().to_string());
                    continue;
                };
                let capability = match put_capability {
                    Some(capability) => capability,
                    None => {
                        let minted = call(
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
                        put_capability = Some(minted);
                        minted
                    }
                };
                let info = call(
                    client,
                    PutBlob {
                        capability,
                        bytes: bytes.into(),
                    },
                )
                .await?;
                let blob = BlobRef {
                    digest: info.blob_id.digest_str(),
                    length_octets: info.length_octets,
                    mime: mime_of(&change.path).to_owned(),
                    extra: Default::default(),
                };
                summary.blobs_put += 1;
                put.insert(hash, blob.clone());
                content.blob = Some(blob);
            }
        }
    }
    Ok(())
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
    let recorded = record_prepared(client, node_dir, prepared, &mut summary).await?;
    summary.recorded = recorded;
    Ok(summary)
}

/// Records `prepared`'s batch through intxt's `recordObservations` and commits the recorder's
/// answer as the host's state: the one place a batch reaches the node, for hooks and for ACP
/// recordings alike. Returns the answer, or none when the batch was empty and nothing was sent.
pub(crate) async fn record_prepared(
    client: &NodeClient,
    node_dir: &Path,
    mut prepared: Prepared,
    summary: &mut Summary,
) -> Res<Option<RecordedIds>> {
    if prepared.batch.is_empty() {
        prepared.commit(RecordedIds::default())?;
        return Ok(None);
    }
    let capture = capture_node(client, node_dir).await?;
    put_blobs(client, &mut prepared.batch, summary).await?;
    let key = idempotency_key(&prepared.batch);
    let reply = client
        .call_with(
            "org.example.intxt.recordObservations",
            HttpMethod::Post,
            &json!({
                "drawer": capture.drawer,
                "machine": capture.machine,
                "batch": &prepared.batch,
            }),
            Some(&key),
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "recordObservations answered {}: {}",
        reply.status,
        reply.body
    );
    let recorded: RecordedIds = serde_json::from_value(reply.body)?;
    prepared.commit(recorded.clone())?;
    Ok(Some(recorded))
}
