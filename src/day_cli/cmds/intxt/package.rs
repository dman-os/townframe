//! intxt's packaging verbs (FDR 008): `add` puts commits, a branch or a jj change in an intxt
//! with the sessions that produced them, `show` lists what an intxt holds, and `status` says what
//! is recorded in a worktree and which intxts include it.
//!
//! Packaging is client code (ledger D41, D50): git and jj run here, and the entries are written
//! with the operator's own authority through [`Caller`], the way the recorder writes, so there
//! is no nodule method for it. The pack of git objects an entry carries (ADR 040 §4) goes into
//! the node's blob store and is named by a blob datum in the intxt's own descriptor document
//! (`org.example.daybook.blob/<digest>`, ledger D45), which the entry references.
//!
//! The verbs write to a `Write` and take the node client and the directories they work in, so
//! tests drive them without a process around them.

use super::call;
use crate::client::NodeClient;
use crate::interlude::*;

use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::listing::KnownDrawers;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::ListKnownDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use intxt_capture::git::Git;
use intxt_capture::host;
use intxt_capture::identity;
use intxt_capture::identity::Identity;
use intxt_capture::pack;
use intxt_capture::pack::PackRequest;
use intxt_capture::revisions;
use intxt_core::client::Caller;
use intxt_core::client::key;
use intxt_core::generic;
use intxt_core::work;
use serde_json::Value;
use service_utils_rs::RequestContext;
use short_ids::ShortIds;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;

/// Files over this size are recorded by blob id and left out of the pack (ADR 040 §4, D45).
const MAX_PACKED_BLOB_BYTES: u64 = 8 * 1024 * 1024;

/// What an `add` names: exactly one per call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Named {
    /// A range (`main..feat/x`) or a single commit.
    Commits(String),
    /// A local branch: its commits since the merge base with its upstream (or `main`), and the
    /// branch itself, tracked as this clone sees it.
    Branch(String),
    /// A jj change, followed through its rewrites.
    Change(String),
}

/// What `add` named, resolved now: the entry that names the commits, the commits, and what the
/// pack is made of.
struct Resolved {
    entry: DatumKey,
    /// Where git can read the commits: the worktree, or jj's git directory.
    git_dir: PathBuf,
    tips: Vec<String>,
    bases: Vec<String>,
    commits: Vec<String>,
    label: String,
    /// The entry's value, given the pack's reference, the tips and the bases.
    value: fn(DatumReference, &[String], &[String]) -> Value,
    /// A branch to track on this clone: its full ref name and its base.
    tracked: Option<(String, Option<String>)>,
}

impl Resolved {
    fn new(named: &Named, identity: &Identity) -> Res<Self> {
        if let Named::Change(change) = named {
            let jj = revisions::resolve_jj_change(&identity.root, change)?;
            return Ok(Self {
                entry: key(work::JjChangeEntry::ID, &jj.change_id)?,
                git_dir: jj.git_dir,
                label: format!(
                    "jj change {}: {} commit(s)",
                    &jj.change_id[..jj.change_id.len().min(12)],
                    jj.commits.len()
                ),
                tips: jj.commits.clone(),
                bases: jj.bases,
                commits: jj.commits,
                value: |pack, _, _| {
                    serde_json::to_value(work::JjChangeEntry {
                        pack,
                        extra: Default::default(),
                    })
                    .expect("an entry serializes")
                },
                tracked: None,
            });
        }
        let git = Git::new(&identity.root);
        let (spec, revisions, tracked) = match named {
            Named::Commits(spec) => (spec, revisions::resolve_commits(&git, spec)?, None),
            Named::Branch(branch) => {
                let (reference, revisions) = revisions::resolve_branch(&git, branch)?;
                let base = revisions.base.clone();
                (branch, revisions, Some((reference, base)))
            }
            Named::Change(_) => unreachable!("returned above"),
        };
        let commits = revisions::commits_of(&git, &revisions)?;
        Ok(Self {
            entry: key(work::GitCommitEntry::ID, &revisions.tip)?,
            git_dir: identity.root.clone(),
            label: format!(
                "{spec}: {}{}, {} commit(s)",
                revisions
                    .base
                    .as_deref()
                    .map(|base| format!("{}..", short_sha(base)))
                    .unwrap_or_default(),
                short_sha(&revisions.tip),
                commits.len()
            ),
            tips: vec![revisions.tip],
            bases: revisions.base.into_iter().collect(),
            commits,
            value: |pack, tips, bases| {
                serde_json::to_value(work::GitCommitEntry {
                    tip: commit_key(&tips[0]),
                    base: bases.first().map(|base| commit_key(base)),
                    pack,
                    extra: Default::default(),
                })
                .expect("an entry serializes")
            },
            tracked,
        })
    }
}

/// What an `add` is about to write: the datums, which of them are entries (in body order), the
/// documents to share with the intxt, and what to tell the operator.
#[derive(Default)]
struct Plan {
    set: BTreeMap<DatumKey, Value>,
    written: Vec<DatumKey>,
    shared: BTreeSet<DocId>,
    lines: Vec<String>,
}

/// A node, and the worktree and node directory a verb works against.
pub(crate) struct Packaging<'a> {
    client: &'a NodeClient,
    caller: Caller<&'a NodeClient>,
    cwd: &'a Path,
    node_dir: &'a Path,
}

/// An intxt as `show` and `add` read it: its title and its entries in body order.
struct View {
    title: generic::Title,
    entries: Vec<(DatumKey, Value)>,
}

impl View {
    fn has(&self, entry: &DatumKey) -> bool {
        self.entries.iter().any(|(key, _)| key == entry)
    }
}

/// The key an entry names a commit by, until the cabinet resolves `#keyRef` (ADR 035 §2.9).
fn commit_key(sha: &str) -> String {
    format!("{}/{sha}", work::GitCommit::ID)
}

/// The sha a commit key names.
fn sha_of(commit_key: &str) -> &str {
    commit_key.rsplit('/').next().unwrap_or(commit_key)
}

fn short_sha(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

/// The shortest unique prefix of `id` among `ids`.
fn shown(ids: &ShortIds<String>, id: &str) -> String {
    ids.render(&id.to_owned()).unique
}

/// The sessions whose ops produced content in `written`, as their documents (ledger D61): an op
/// follows when it names a cause and one of its `files` is a (path, blob id) the commits wrote.
/// The cause is a tool call of a session document. Ops with no cause, and content the commits
/// changed again afterwards, bring nothing.
fn sessions_following<'a>(
    written: &BTreeSet<(String, String)>,
    ops: impl IntoIterator<Item = &'a work::WorktreeOp>,
) -> BTreeSet<DocId> {
    let mut blobs: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (path, blob) in written {
        blobs.entry(path).or_default().insert(blob);
    }
    let mut sessions = BTreeSet::new();
    for op in ops {
        let Some(cause) = &op.cause else { continue };
        let produced = op.files.iter().any(|(path, blob)| {
            blobs
                .get(path.as_str())
                .is_some_and(|wanted| wanted.contains(blob.as_str()))
        });
        if produced && let ReferenceDocId::Concrete(doc) = cause.reference().document() {
            sessions.insert(doc.clone());
        }
    }
    sessions
}

impl<'a> Packaging<'a> {
    /// `cwd` is where the verb runs (a worktree for `add` and `status`); `node_dir` holds
    /// capture's state.
    pub(crate) fn new(client: &'a NodeClient, cwd: &'a Path, node_dir: &'a Path) -> Self {
        // Every run writes under its own commands: a retry of one run is the same command and
        // dedupes, while adding the same thing again after it was removed is a new one.
        let nonce: [u8; 8] = rand::random();
        Self {
            client,
            caller: Caller::new(
                client,
                RequestContext::root(),
                None,
                format!("intxt-package-{}", data_encoding::HEXLOWER.encode(&nonce)),
            ),
            cwd,
            node_dir,
        }
    }

    fn identity(&self) -> Res<Identity> {
        identity::resolve(self.cwd)?.ok_or_else(|| {
            eyre::eyre!(
                "{} is not in a git worktree; run this in the worktree that holds the work",
                self.cwd.display()
            )
        })
    }

    /// A capability on the node's known-drawers list, which lists and remembers drawers.
    async fn known_drawers(&self) -> Res<CapId> {
        let info = crate::cmds::status::node_info(self.client).await?;
        Ok(self
            .caller
            .call(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(info.known_drawers_id, DrawerAccess::Admin)].into(),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id)
    }

    async fn is_intxt(&self, drawer: &DrawerId) -> Res<bool> {
        Ok(self
            .caller
            .try_read(drawer, &key(generic::Intxt::ID, "main")?)
            .await?
            .1
            .is_some())
    }

    /// The intxts the node knows: its known drawers whose descriptor carries the intxt marker.
    async fn live_intxts(&self) -> Res<Vec<DrawerId>> {
        let listed: KnownDrawers = self
            .caller
            .call(ListKnownDrawers {
                capability: self.known_drawers().await?,
            })
            .await?;
        let mut live = Vec::new();
        for drawer in listed.drawers {
            if self.is_intxt(&drawer.drawer_id).await? {
                live.push(drawer.drawer_id);
            }
        }
        Ok(live)
    }

    /// The intxt `name` names, and the short ids of the live intxts. `name` is a short id over
    /// the live intxts or a full drawer id.
    ///
    /// DECISION NEEDED: `createIntxt` does not remember the drawer it creates, so an intxt
    /// is only "live" once something remembers it. A full drawer id that is an intxt is
    /// remembered here, which is what makes it addressable by short id afterwards. The
    /// alternative is `createIntxt` remembering its drawer, which needs the known-drawers
    /// capability the nodule is not granted.
    async fn resolve(&self, name: &str) -> Res<(DrawerId, ShortIds<String>)> {
        let live = self.live_intxts().await?;
        let mut ids: Vec<String> = live.iter().map(ToString::to_string).collect();
        if let Ok(drawer) = name.parse::<DrawerId>() {
            eyre::ensure!(
                self.is_intxt(&drawer).await?,
                "{drawer} is not an intxt: its descriptor has no intxt marker"
            );
            if !live.contains(&drawer) {
                self.caller
                    .call(RememberDrawer {
                        capability: self.known_drawers().await?,
                        drawer_id: drawer.clone(),
                        mirror: false,
                    })
                    .await?;
                ids.push(drawer.to_string());
            }
            return Ok((drawer, ShortIds::new(ids)));
        }
        let short = ShortIds::new(ids);
        let found = short
            .resolve(name)
            .map_err(|error| {
                eyre::eyre!(
                    "{error}; the live intxts are {}",
                    live.iter()
                        .map(|drawer| shown(&short, &drawer.to_string()))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?
            .parse()?;
        Ok((found, short))
    }

    async fn load(&self, drawer: &DrawerId) -> Res<View> {
        let (_, mut datums) = self.caller.read_datums(drawer, None).await?;
        let mut accepted = |key: &DatumKey| match datums.remove(key) {
            Some(DatumRead::Accepted { value, .. }) => Ok(value),
            other => Err(eyre::eyre!("{key} in {drawer} is not readable: {other:?}")),
        };
        let title = serde_json::from_value(accepted(&key(generic::Title::ID, "main")?)?)?;
        let body: generic::Body =
            serde_json::from_value(accepted(&key(generic::Body::ID, "main")?)?)?;
        let mut entries = Vec::with_capacity(body.order.len());
        for reference in &body.order {
            entries.push((reference.key().clone(), accepted(reference.key())?));
        }
        Ok(View { title, entries })
    }

    /// The ops of the worktree's op logs, which are in the capture drawer's documents.
    async fn worktree_ops(&self, recorded: &host::RecordedWorktree) -> Res<Vec<work::WorktreeOp>> {
        let Some(doc) = &recorded.docs.worktree else {
            return Ok(Vec::new());
        };
        let doc: DocId = doc.parse()?;
        let (_, worktree) = self
            .caller
            .read(&doc, &key(work::Worktree::ID, "main")?)
            .await?;
        let worktree: work::Worktree = serde_json::from_value(worktree)?;
        let mut ops = Vec::new();
        for log in &worktree.op_logs {
            let (_, datums) = self.caller.read_datums(&log.doc.parse()?, None).await?;
            for (key, datum) in datums {
                if key.tag().as_str() != work::WorktreeOp::ID {
                    continue;
                }
                let DatumRead::Accepted { value, .. } = datum else {
                    eyre::bail!("{key} in op log {} is not readable: {datum:?}", log.doc);
                };
                ops.push(serde_json::from_value(value)?);
            }
        }
        Ok(ops)
    }

    /// Puts `pack` in the blob store and answers the blob datum that names it: its key and value.
    async fn put_pack(&self, pack: Vec<u8>) -> Res<(DatumKey, Value)> {
        eyre::ensure!(
            pack.len() <= big_blobs_core::MAX_PUT_BYTES,
            "the pack is {} bytes, over the {} one blob put carries; name fewer commits, or leave \
             out --full so the pack is thin against the base",
            pack.len(),
            big_blobs_core::MAX_PUT_BYTES
        );
        let capability = call(
            self.client,
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
        let info = call(
            self.client,
            PutBlob {
                capability,
                bytes: pack.into(),
            },
        )
        .await?;
        let digest = info.blob_id.digest_str();
        Ok((
            key(big_blobs_core::Blob::ID, &digest)?,
            json!({
                "mime": "application/x-git-pack",
                "digest": digest,
                "lengthOctets": info.length_octets,
            }),
        ))
    }

    /// What `add` writes for the commits `resolved` names: the pack, its blob datum and the
    /// entry, unless the intxt already holds the entry. Says whether the entry is new.
    async fn plan_commits(
        &self,
        plan: &mut Plan,
        view: &View,
        resolved: &Resolved,
        recorded: &host::RecordedWorktree,
        full: bool,
    ) -> Res<bool> {
        if view.has(&resolved.entry) {
            plan.lines
                .push(format!("  commits    {} (already in)", resolved.label));
            return Ok(false);
        }
        let request = PackRequest {
            tips: resolved.tips.clone(),
            bases: if full {
                Vec::new()
            } else {
                resolved.bases.clone()
            },
            objects: Vec::new(),
            max_blob_bytes: Some(MAX_PACKED_BLOB_BYTES),
        };
        let git_dir = resolved.git_dir.clone();
        let pack = tokio::task::spawn_blocking(move || pack::build(&Git::new(&git_dir), &request))
            .await??;
        let kind = if pack.thin { "thin" } else { "full" };
        let size = pack.bytes.len();
        let (blob_key, blob) = self.put_pack(pack.bytes).await?;
        let pack_ref = DatumReference::new(ReferenceDocId::SelfDoc, blob_key.clone());
        plan.set.insert(blob_key, blob);
        plan.set.insert(
            resolved.entry.clone(),
            (resolved.value)(pack_ref, &resolved.tips, &resolved.bases),
        );
        plan.written.push(resolved.entry.clone());
        plan.lines.push(format!(
            "  commits    {} ({kind} pack, {size} bytes)",
            resolved.label
        ));
        // The commits' own datums live in the repository's document.
        if let Some(repo) = &recorded.docs.repo {
            plan.shared.insert(repo.parse()?);
        }
        Ok(true)
    }

    /// What `add` writes to track the branch `resolved` names on this clone, which is nothing
    /// when the intxt already tracks it. The `gitRef` the entry follows must have been recorded.
    async fn plan_ref(
        &self,
        plan: &mut Plan,
        view: &View,
        resolved: &Resolved,
        identity: &Identity,
        recorded: &host::RecordedWorktree,
    ) -> Res<()> {
        let Some((reference, base)) = &resolved.tracked else {
            return Ok(());
        };
        let slot = format!("{}/{reference}", identity.clone_id);
        let entry = key(work::GitRefEntry::ID, &slot)?;
        if view.has(&entry) {
            plan.lines
                .push(format!("  ref        {reference} (already tracked)"));
            return Ok(());
        }
        let clone_doc: DocId = recorded
            .docs
            .clone
            .as_deref()
            .ok_or_else(|| {
                eyre::eyre!(
                    "no hook has recorded this clone yet, so there is nothing to track; run an \
                     agent session here with capture on first"
                )
            })?
            .parse()?;
        let ref_key = key(work::GitRef::ID, &slot)?;
        eyre::ensure!(
            self.caller
                .try_read(&clone_doc, &ref_key)
                .await?
                .1
                .is_some(),
            "{reference} has not been recorded: capture saw no move of it. Importing history \
             that was never recorded is not built yet"
        );
        plan.set.insert(
            entry.clone(),
            serde_json::to_value(work::GitRefEntry {
                reference: DatumReference::new(
                    ReferenceDocId::Concrete(clone_doc.clone()),
                    ref_key,
                ),
                base: base.as_deref().map(commit_key),
                extra: Default::default(),
            })?,
        );
        plan.written.push(entry);
        plan.shared.insert(clone_doc);
        plan.lines
            .push(format!("  ref        {reference} (tracked on this clone)"));
        Ok(())
    }

    /// The sessions whose recorded ops produced the content of `resolved`'s commits: the
    /// `agentSession` parts to add, with how to describe each, and the descriptions of those the
    /// intxt already holds.
    async fn following_sessions(
        &self,
        view: &View,
        resolved: &Resolved,
        recorded: &host::RecordedWorktree,
    ) -> Res<(Vec<(DatumReference, String)>, Vec<String>)> {
        let wrote = revisions::written_files(&Git::new(&resolved.git_dir), &resolved.commits)?;
        let ops = self.worktree_ops(recorded).await?;
        let part_key = key(work::AgentSession::ID, "main")?;
        let mut to_add = Vec::new();
        let mut present = Vec::new();
        for doc in sessions_following(&wrote, &ops) {
            // A document that is not a session (a subagent's holds none) is not one to add.
            let Some(session) = self.caller.try_read(&doc, &part_key).await?.1 else {
                continue;
            };
            let session: work::AgentSession = serde_json::from_value(session)?;
            let described = match &session.title {
                Some(title) => format!("{} \"{title}\"", session.harness),
                None => session.harness.clone(),
            };
            if view.has(&key(work::SessionEntry::ID, &session.session_id)?) {
                present.push(described);
            } else {
                to_add.push((
                    DatumReference::new(ReferenceDocId::Concrete(doc), part_key.clone()),
                    described,
                ));
            }
        }
        Ok((to_add, present))
    }

    /// `day x intxt add`: includes what `named` names, with the sessions that produced it, in
    /// the intxt `intxt` names, and says what was added and how to undo it. Something already
    /// in the intxt is left alone and said so.
    pub(crate) async fn add(
        &self,
        out: &mut impl Write,
        intxt: &str,
        named: Named,
        full: bool,
    ) -> Res<()> {
        let (drawer, intxts) = self.resolve(intxt).await?;
        let view = self.load(&drawer).await?;
        let identity = self.identity()?;
        let recorded = host::recorded_worktree(&identity)?;
        let resolved = Resolved::new(&named, &identity)?;

        let mut plan = Plan::default();
        let new_commits = self
            .plan_commits(&mut plan, &view, &resolved, &recorded, full)
            .await?;
        self.plan_ref(&mut plan, &view, &resolved, &identity, &recorded)
            .await?;
        // The sessions of commits already in the intxt were derived when they were added.
        let (sessions, present) = if new_commits {
            self.following_sessions(&view, &resolved, &recorded).await?
        } else {
            Default::default()
        };

        let mut written = plan.written.clone();
        if !written.is_empty() {
            self.caller
                .include(&drawer, plan.set, &plan.written)
                .await?;
            for doc in &plan.shared {
                self.caller.share(&drawer, doc).await?;
            }
        }
        for (part, described) in &sessions {
            let included = self.caller.add_part(&drawer, part).await?;
            written.push(included.entry.key().clone());
            plan.lines
                .push(format!("  session    {described} (follows the commits)"));
        }
        for described in &present {
            plan.lines
                .push(format!("  session    {described} (already in)"));
        }

        let heading = format!(
            "{} \"{}\"",
            shown(&intxts, &drawer.to_string()),
            view.title.title
        );
        writeln!(
            out,
            "{} {heading}",
            if written.is_empty() {
                "nothing to add to"
            } else {
                "added to"
            }
        )?;
        for line in &plan.lines {
            writeln!(out, "{line}")?;
        }
        if !written.is_empty() {
            // Short ids among the intxt's items as they are now.
            let ids = ShortIds::new(
                view.entries
                    .iter()
                    .map(|(key, _)| key.to_string())
                    .chain(written.iter().map(ToString::to_string))
                    .collect::<Vec<_>>(),
            );
            writeln!(
                out,
                "  undo: day x intxt rm {} {}",
                shown(&intxts, &drawer.to_string()),
                written
                    .iter()
                    .map(|entry| shown(&ids, &entry.to_string()))
                    .collect::<Vec<_>>()
                    .join(" ")
            )?;
        }
        Ok(())
    }

    /// `day x intxt show`: the title, the description, and the entries by kind, each with its
    /// short id among the intxt's entries.
    pub(crate) async fn show(&self, out: &mut impl Write, intxt: &str) -> Res<()> {
        let (drawer, intxts) = self.resolve(intxt).await?;
        let view = self.load(&drawer).await?;
        let ids = ShortIds::new(
            view.entries
                .iter()
                .map(|(key, _)| key.to_string())
                .collect::<Vec<_>>(),
        );
        writeln!(
            out,
            "{}  \"{}\"",
            shown(&intxts, &drawer.to_string()),
            view.title.title
        )?;
        if let Some(description) = &view.title.description {
            writeln!(out, "{description}")?;
        }
        let mut commits = Vec::new();
        let mut changes = Vec::new();
        let mut refs = Vec::new();
        let mut sessions = Vec::new();
        let mut others = Vec::new();
        for (entry, value) in &view.entries {
            let id = shown(&ids, &entry.to_string());
            let tag = entry.tag().as_str();
            let slot = String::from_utf8_lossy(entry.slot_bytes()).into_owned();
            if tag == work::GitCommitEntry::ID {
                let commit: work::GitCommitEntry = serde_json::from_value(value.clone())?;
                commits.push(format!(
                    "  {id}  {}{}",
                    commit
                        .base
                        .as_deref()
                        .map(|base| format!("{}..", short_sha(sha_of(base))))
                        .unwrap_or_default(),
                    short_sha(sha_of(&commit.tip))
                ));
            } else if tag == work::JjChangeEntry::ID {
                changes.push(format!("  {id}  {}", &slot[..slot.len().min(12)]));
            } else if tag == work::GitRefEntry::ID {
                let tracked: work::GitRefEntry = serde_json::from_value(value.clone())?;
                let (clone, reference) = slot.split_once('/').unwrap_or(("", &slot));
                refs.push(format!(
                    "  {id}  {reference}  (clone {}){}",
                    &clone[..clone.len().min(8)],
                    tracked
                        .base
                        .as_deref()
                        .map(|base| format!("  base {}", short_sha(sha_of(base))))
                        .unwrap_or_default()
                ));
            } else if tag == work::SessionEntry::ID {
                let included: work::SessionEntry = serde_json::from_value(value.clone())?;
                let session = match included.session.document() {
                    ReferenceDocId::Concrete(doc) => {
                        self.caller.try_read(doc, included.session.key()).await?.1
                    }
                    ReferenceDocId::SelfDoc => None,
                };
                let described = match session {
                    Some(session) => {
                        let session: work::AgentSession = serde_json::from_value(session)?;
                        format!(
                            "{}  {}",
                            session.harness,
                            session.title.unwrap_or_else(|| session.cwd.clone())
                        )
                    }
                    None => "(document not synced)".to_owned(),
                };
                sessions.push(format!("  {id}  {slot}  {described}"));
            } else {
                others.push(format!("  {id}  {entry}"));
            }
        }
        for (heading, lines) in [
            ("commits", commits),
            ("jj changes", changes),
            ("tracked refs", refs),
            ("sessions", sessions),
            ("other", others),
        ] {
            if lines.is_empty() {
                continue;
            }
            writeln!(out, "{heading} ({}):", lines.len())?;
            for line in lines {
                writeln!(out, "{line}")?;
            }
        }
        if view.entries.is_empty() {
            writeln!(out, "nothing in it yet")?;
        }
        Ok(())
    }

    /// `day x intxt status`: what is recorded in the worktree, and which intxts include its
    /// commits, its clone's refs and its sessions.
    pub(crate) async fn status(&self, out: &mut impl Write) -> Res<()> {
        let identity = self.identity()?;
        let git = Git::new(&identity.root);
        let recorded = host::recorded_worktree(&identity)?;
        let sessions = host::sessions_in(&self.node_dir.join("intxt/sessions"), &identity.root)?;
        let branch = revisions::head_branch(&git)?;
        writeln!(
            out,
            "worktree {} ({})",
            identity.root.display(),
            branch
                .as_deref()
                .map(|branch| branch.trim_start_matches("refs/heads/"))
                .unwrap_or("detached HEAD")
        )?;
        writeln!(out, "recorded here: {} op(s)", recorded.ops)?;
        if sessions.is_empty() {
            writeln!(out, "sessions recorded here: none")?;
        } else {
            writeln!(out, "sessions recorded here:")?;
            for (index, session) in sessions.iter().enumerate() {
                let ago = session
                    .modified
                    .elapsed()
                    .map(|elapsed| format!("{}s ago", elapsed.as_secs()))
                    .unwrap_or_else(|_| "just now".to_owned());
                writeln!(
                    out,
                    "  {}{}  last recorded {ago}",
                    session.session_id,
                    if index == 0 { "  (current)" } else { "" }
                )?;
            }
        }

        let session_docs: BTreeSet<DocId> = sessions
            .iter()
            .filter_map(|session| session.doc.as_deref())
            .map(str::parse)
            .collect::<Result<_, _>>()?;
        let mut including = Vec::new();
        let live = self.live_intxts().await?;
        let live_ids = ShortIds::new(live.iter().map(ToString::to_string).collect::<Vec<_>>());
        for drawer in &live {
            let view = self.load(drawer).await?;
            let mut matched = Vec::new();
            for (entry, value) in &view.entries {
                let tag = entry.tag().as_str();
                if tag == work::GitCommitEntry::ID {
                    let commit: work::GitCommitEntry = serde_json::from_value(value.clone())?;
                    if revisions::is_ancestor(&git, sha_of(&commit.tip), "HEAD")? {
                        matched.push(format!("commits to {}", short_sha(sha_of(&commit.tip))));
                    }
                } else if tag == work::GitRefEntry::ID {
                    let slot = String::from_utf8_lossy(entry.slot_bytes()).into_owned();
                    if let Some((clone, reference)) = slot.split_once('/')
                        && clone == identity.clone_id
                    {
                        matched.push(format!("tracks {reference}"));
                    }
                } else if tag == work::SessionEntry::ID {
                    let included: work::SessionEntry = serde_json::from_value(value.clone())?;
                    if let ReferenceDocId::Concrete(doc) = included.session.document()
                        && session_docs.contains(doc)
                    {
                        matched.push(format!(
                            "session {}",
                            String::from_utf8_lossy(entry.slot_bytes())
                        ));
                    }
                }
            }
            if !matched.is_empty() {
                including.push(format!(
                    "  {}  \"{}\": {}",
                    shown(&live_ids, &drawer.to_string()),
                    view.title.title,
                    matched.join(", ")
                ));
            }
        }
        if including.is_empty() {
            writeln!(out, "in intxts: none")?;
            if let Some(branch) = &branch {
                writeln!(
                    out,
                    "  package it:  day x intxt add <intxt> --branch {}",
                    branch.trim_start_matches("refs/heads/")
                )?;
            }
        } else {
            writeln!(out, "in intxts:")?;
            for line in including {
                writeln!(out, "{line}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::reference::Location;

    fn op(cause: Option<&str>, files: &[(&str, &str)]) -> work::WorktreeOp {
        serde_json::from_value(json!({
            "at": "2026-11-10T10:00:00Z",
            "kind": "edit",
            "cause": cause,
            "files": files.iter().copied().collect::<BTreeMap<_, _>>(),
        }))
        .unwrap()
    }

    fn cause(doc: &DocId, turn: &str) -> String {
        Location::new(
            DatumReference::new(
                ReferenceDocId::Concrete(doc.clone()),
                key(work::PromptTurn::ID, turn).unwrap(),
            ),
            Some("toolCalls/t1".to_owned()),
        )
        .unwrap()
        .to_string()
    }

    #[test]
    fn sessions_follow_the_ops_that_produced_the_committed_content() {
        let first = DocId::new([1; 32]);
        let second = DocId::new([2; 32]);
        let third = DocId::new([3; 32]);
        let written: BTreeSet<(String, String)> = [
            ("a.txt".to_owned(), "aaa".to_owned()),
            ("src/b.rs".to_owned(), "bbb".to_owned()),
        ]
        .into();
        let ops = [
            // Produced a committed blob at its committed path.
            op(Some(&cause(&first, "u1")), &[("a.txt", "aaa")]),
            // The right blob under another path is not the content the commit wrote there.
            op(Some(&cause(&second, "u1")), &[("moved.txt", "bbb")]),
            // An intermediate state the commits no longer hold.
            op(Some(&cause(&third, "u1")), &[("a.txt", "older")]),
            // The right content with no cause: nothing to follow.
            op(None, &[("src/b.rs", "bbb")]),
            // A second op of the first session, and another that also produced a written file.
            op(Some(&cause(&first, "u2")), &[("src/b.rs", "bbb")]),
            op(Some(&cause(&second, "u2")), &[("src/b.rs", "bbb")]),
        ];
        assert_eq!(
            sessions_following(&written, &ops),
            [first, second].into_iter().collect::<BTreeSet<_>>()
        );
        assert!(sessions_following(&BTreeSet::new(), &ops).is_empty());
    }
}
