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

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::listing::KnownDrawers;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::ForgetDrawer;
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
use intxt_core::client::to_value;
use intxt_core::generic;
use intxt_core::methods::CreateIntxt;
use intxt_core::methods::IntxtCreated;
use intxt_core::work;
use serde_json::Value;
use service_utils_rs::RequestContext;
use short_ids::ShortIds;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::IsTerminal;
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
    /// The jj change the commits are the evolution of, which their datums name.
    change_id: Option<String>,
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
                change_id: Some(jj.change_id),
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
            change_id: None,
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
    pub(super) client: &'a NodeClient,
    pub(super) caller: Caller<&'a NodeClient>,
    pub(super) cwd: &'a Path,
    pub(super) node_dir: &'a Path,
}

/// An intxt as `show` and `add` read it: its title and its entries in body order.
pub(super) struct View {
    pub(super) title: generic::Title,
    pub(super) entries: Vec<(DatumKey, Value)>,
}

impl View {
    pub(super) fn has(&self, entry: &DatumKey) -> bool {
        self.entries.iter().any(|(key, _)| key == entry)
    }
}

/// The key an entry names a commit by, until the cabinet resolves `#keyRef` (ADR 035 §2.9).
pub(super) fn commit_key(sha: &str) -> String {
    format!("{}/{sha}", work::GitCommit::ID)
}

/// Which `predecessors` (held commits outside the range, with their patch ids) the `new` commits
/// rewrote: (old sha, new sha) pairs, and a line for each patch id that names no single pair. A
/// pair needs the patch id to be unique on both sides: several held commits with it leave the
/// predecessor unknown, and several new commits with it would each claim the same predecessor.
fn match_rewrites(
    new: &BTreeMap<String, work::GitCommit>,
    predecessors: &BTreeMap<String, Option<String>>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut held: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
    for (sha, patch_id) in predecessors {
        if let Some(patch_id) = patch_id {
            held.entry(patch_id).or_default().push(sha);
        }
    }
    let mut added: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
    for (sha, commit) in new {
        if let Some(patch_id) = &commit.patch_id {
            added.entry(patch_id).or_default().push(sha);
        }
    }
    let (mut pairs, mut lines) = (Vec::new(), Vec::new());
    for (patch_id, successors) in added {
        match (held.get(patch_id).map(Vec::as_slice), successors.as_slice()) {
            (None, _) => {}
            (Some([old]), [successor]) => pairs.push(((*old).clone(), (*successor).clone())),
            (Some(olds), successors) => lines.push(format!(
                "  rewrite    {} matches {} held commit(s) by patch id and names no single predecessor: nothing written",
                successors.iter().map(|sha| short_sha(sha)).collect::<Vec<_>>().join(", "),
                olds.len()
            )),
        }
    }
    (pairs, lines)
}

/// The sha a commit key names.
pub(super) fn sha_of(commit_key: &str) -> &str {
    commit_key.rsplit('/').next().unwrap_or(commit_key)
}

pub(super) fn short_sha(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

/// The shortest unique prefix of `id` among `ids`, with its remainder dimmed on a terminal.
pub(super) fn shown(ids: &ShortIds<String>, id: &str) -> String {
    let rendered = ids.render(&id.to_owned());
    if std::io::stdout().is_terminal() && !rendered.rest.is_empty() {
        format!("{}\x1b[2m{}\x1b[22m", rendered.unique, rendered.rest)
    } else {
        rendered.unique
    }
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

    /// The same verbs run as `actor`, a descendant of the operator: what they write is attributed
    /// to it, which is how a second side of a review exists on one node.
    #[cfg(test)]
    pub(crate) fn acting_as(mut self, actor: big_cabinet_core::dmeta::Uuid7) -> Self {
        self.caller = self.caller.acting_as(actor);
        self
    }

    pub(super) fn identity(&self) -> Res<Identity> {
        identity::resolve(self.cwd)?.ok_or_else(|| {
            eyre::eyre!(
                "{} is not in a git worktree; run this in the worktree that holds the work",
                self.cwd.display()
            )
        })
    }

    /// A capability on the node's known-drawers list, which lists and remembers drawers.
    pub(super) async fn known_drawers(&self) -> Res<CapId> {
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

    /// A capability on `drawer` at Admin, which creates its meta documents and describes it.
    pub(super) async fn drawer_capability(&self, drawer: &DrawerId) -> Res<CapId> {
        Ok(self
            .caller
            .call(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(drawer.clone(), DrawerAccess::Admin)].into(),
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
    pub(super) async fn live_intxts(&self) -> Res<Vec<DrawerId>> {
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

    /// The intxt's commits document, made if the intxt has none yet: a meta document
    /// (`metaDoc/commits/main`) of its drawer, like the notes document, so it is in the drawer
    /// without an entry and `which` finds it by its datums, not by name. The command id is fixed
    /// per drawer, so creating it again answers the first creation, and two writers converge on
    /// one document.
    pub(super) async fn ensure_commits_doc(&self, drawer: &DrawerId) -> Res<DocId> {
        Ok(self
            .caller
            .call(CreateMetaDoc {
                capability: self.drawer_capability(drawer).await?,
                command: CommandId::try_from(format!("intxt-commits-{drawer}"))?,
                drawer_id: drawer.clone(),
                kind: "commits".into(),
                name: "main".into(),
                root: MetaRoot::Sensitive,
            })
            .await?
            .doc_id)
    }

    /// Writes a `gitCommit/<sha>` for every commit `resolved` names into the intxt's commits
    /// document, so the document holding the key is what says the intxt holds the commit (the
    /// membership query of `which`). A commit is the same value wherever it is read, so one the
    /// document holds already is left alone, which makes adding again, or adding a range that
    /// overlaps, write only what is new.
    ///
    /// A new commit whose patch id matches exactly one commit the document already holds from
    /// outside the range is a rewrite of it (an amend, a rebase, a force-push seen from another
    /// machine): `rewrite/<old>` and `rewrittenFrom/<new>` are written beside them. Several
    /// matches name no single predecessor, so nothing is written and the line says so.
    ///
    /// Answers how many commits were written and the lines to report.
    async fn write_commits(
        &self,
        drawer: &DrawerId,
        resolved: &Resolved,
    ) -> Res<(usize, Vec<String>)> {
        if resolved.commits.is_empty() {
            return Ok((0, Vec::new()));
        }
        let doc = self.ensure_commits_doc(drawer).await?;
        // One read of the document gives the held commits and their patch ids.
        let (_, datums) = self.caller.read_datums(&doc, None).await?;
        let mut held: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut rewritten = BTreeSet::new();
        for (datum_key, read) in datums {
            let slot = String::from_utf8_lossy(datum_key.slot_bytes()).into_owned();
            let DatumRead::Accepted { value, .. } = read else {
                eyre::bail!("{datum_key} in {doc} is not readable: {read:?}");
            };
            match datum_key.tag().as_str() {
                work::GitCommit::ID => {
                    let commit: work::GitCommit = serde_json::from_value(value)?;
                    held.insert(slot, commit.patch_id);
                }
                work::Rewrite::ID => {
                    rewritten.insert(slot);
                }
                _ => {}
            }
        }
        let missing: Vec<String> = resolved
            .commits
            .iter()
            .filter(|sha| !held.contains_key(*sha))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok((0, Vec::new()));
        }
        let (git_dir, change_id) = (resolved.git_dir.clone(), resolved.change_id.clone());
        let new = tokio::task::spawn_blocking(move || -> Res<BTreeMap<String, work::GitCommit>> {
            let git = Git::new(&git_dir);
            missing
                .into_iter()
                .map(|sha| {
                    let mut commit = intxt_capture::git::commit(&git, &sha)?;
                    commit.change_id = change_id.clone();
                    Ok::<_, eyre::Report>((sha, commit))
                })
                .collect()
        })
        .await??;
        // The predecessors are held commits outside the range, that no rewrite replaced yet.
        let in_range: BTreeSet<&String> = resolved.commits.iter().collect();
        let predecessors: BTreeMap<String, Option<String>> = held
            .into_iter()
            .filter(|(sha, _)| !in_range.contains(sha) && !rewritten.contains(sha))
            .collect();
        let (pairs, mut lines) = match_rewrites(&new, &predecessors);
        let mut write = BTreeMap::new();
        for (old, successor) in &pairs {
            write.insert(
                key(work::Rewrite::ID, old)?,
                to_value(&work::Rewrite {
                    successor: successor.clone(),
                    how: work::RewriteHow::PatchId,
                    extra: Default::default(),
                }),
            );
            write.insert(
                key(work::RewrittenFrom::ID, successor)?,
                to_value(&work::RewrittenFrom {
                    predecessor: old.clone(),
                    how: work::RewriteHow::PatchId,
                    extra: Default::default(),
                }),
            );
            lines.push(format!(
                "  rewrite    {} replaces {} (same patch id)",
                short_sha(successor),
                short_sha(old)
            ));
        }
        let written = new.len();
        for (sha, commit) in &new {
            write.insert(key(work::GitCommit::ID, sha)?, to_value(commit));
        }
        self.caller
            .write_datums(&doc, write, BTreeSet::new())
            .await?;
        Ok((written, lines))
    }

    /// Remembers `drawer` so the node lists it, which is what makes an intxt live.
    async fn remember(&self, drawer: &DrawerId) -> Res<()> {
        self.caller
            .call(RememberDrawer {
                capability: self.known_drawers().await?,
                drawer_id: drawer.clone(),
                mirror: false,
            })
            .await?;
        Ok(())
    }

    /// The intxt `name` names, and the short ids of the live intxts. `name` is a short id over
    /// the live intxts or a full drawer id; none is the intxt the worktree is on (`use`). An intxt is live once the node remembers its drawer:
    /// `new` remembers the drawers it creates, and a full drawer id that is an intxt is
    /// remembered when it is named, so an intxt made by `createIntxt` directly becomes
    /// addressable by short id the first time it is named in full.
    pub(super) async fn resolve(&self, name: Option<&str>) -> Res<(DrawerId, ShortIds<String>)> {
        let on;
        let name = match name {
            Some(name) => name,
            None => {
                on = host::recorded_worktree(&self.identity()?)?.intxt.ok_or_else(|| {
                    eyre::eyre!(
                        "name an intxt, or put this worktree on one with `day x intxt use <intxt>`"
                    )
                })?;
                on.as_str()
            }
        };
        let live = self.live_intxts().await?;
        let mut ids: Vec<String> = live.iter().map(ToString::to_string).collect();
        if let Some(drawer) = full_drawer_id(name) {
            eyre::ensure!(
                self.is_intxt(&drawer).await?,
                "{drawer} is not an intxt: its descriptor has no intxt marker"
            );
            if !live.contains(&drawer) {
                self.remember(&drawer).await?;
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

    /// `day x intxt list`: lists the intxts remembered by this node, with their entry counts.
    ///
    /// Drawer creation timestamps are not part of the known-drawers listing, so titles provide the
    /// deterministic fallback ordering promised by the command.
    pub(crate) async fn list(&self, out: &mut impl Write) -> Res<()> {
        let live = self.live_intxts().await?;
        let ids = ShortIds::new(live.iter().map(ToString::to_string).collect::<Vec<_>>());
        let mut listed = Vec::with_capacity(live.len());
        for drawer in live {
            let view = self.load(&drawer).await?;
            listed.push((drawer, view));
        }
        listed.sort_by(|(left_drawer, left), (right_drawer, right)| {
            left.title
                .title
                .cmp(&right.title.title)
                .then_with(|| left_drawer.cmp(right_drawer))
        });

        for (drawer, view) in listed {
            let (mut commits, mut refs, mut sessions) = (0, 0, 0);
            for (entry, _) in view.entries {
                match entry.tag().as_str() {
                    work::GitCommitEntry::ID => commits += 1,
                    work::GitRefEntry::ID => refs += 1,
                    work::SessionEntry::ID => sessions += 1,
                    _ => {}
                }
            }
            writeln!(
                out,
                "{}  \"{}\"  commits: {commits}, refs: {refs}, sessions: {sessions}",
                shown(&ids, &drawer.to_string()),
                view.title.title,
            )?;
        }
        Ok(())
    }

    /// `day x intxt drop`: forgets an intxt on this node without changing its drawer or shared data.
    pub(crate) async fn drop(&self, out: &mut impl Write, intxt: &str) -> Res<()> {
        let (drawer, intxts) = self.resolve(Some(intxt)).await?;
        let view = self.load(&drawer).await?;
        let short = shown(&intxts, &drawer.to_string());
        self.caller
            .call(ForgetDrawer {
                capability: self.known_drawers().await?,
                drawer_id: drawer.clone(),
            })
            .await?;
        writeln!(out, "dropped {short} \"{}\"", view.title.title)?;
        writeln!(out, "undo: day x intxt show {drawer}")?;
        Ok(())
    }

    /// `day x intxt new`: creates an intxt, remembers it, and says its short id. Answers its
    /// drawer.
    pub(crate) async fn create(
        &self,
        out: &mut impl Write,
        title: &str,
        description: Option<&str>,
    ) -> Res<DrawerId> {
        let IntxtCreated { drawer_id } = call(
            self.client,
            CreateIntxt {
                title: title.to_owned(),
                description: description.map(str::to_owned),
            },
        )
        .await?;
        self.remember(&drawer_id).await?;
        let live = self.live_intxts().await?;
        let ids = ShortIds::new(live.iter().map(ToString::to_string).collect::<Vec<_>>());
        let short = shown(&ids, &drawer_id.to_string());
        writeln!(out, "intxt {short} \"{title}\"")?;
        if let Some(identity) = identity::resolve(self.cwd)? {
            host::put_on_intxt(&identity, Some(drawer_id.to_string()))?;
            writeln!(out, "  this worktree is on it now (day x intxt use)")?;
        }
        writeln!(out, "  undo: day x intxt drop {short}")?;
        Ok(drawer_id)
    }

    /// Makes sure the intxt's drawer has a blob inventory, so the blob workers pin and sync the
    /// packs it holds (ledger D45). The root is `sensitive`: a pack is the operator's work. The
    /// command id is fixed per drawer, so creating it again answers the first creation.
    async fn ensure_inventory(&self, drawer: &DrawerId) -> Res<()> {
        let capability = self.drawer_capability(drawer).await?;
        self.caller
            .call(CreateMetaDoc {
                capability,
                command: CommandId::try_from(format!("intxt-inventory-{drawer}"))?,
                drawer_id: drawer.clone(),
                kind: "blobInventory".into(),
                name: "intxt".into(),
                root: MetaRoot::Sensitive,
            })
            .await?;
        Ok(())
    }

    /// `day x intxt rm`: removes the entries and comments named by their short ids, and the packs
    /// only the entries referenced. The documents lent to the intxt stay lent: whether another
    /// entry still needs one is not derivable from the entries alone. Removing a thread's first
    /// comment removes its replies and its state with it.
    pub(crate) async fn rm(&self, out: &mut impl Write, intxt: &str, items: &[String]) -> Res<()> {
        let (drawer, intxts) = self.resolve(Some(intxt)).await?;
        let view = self.load(&drawer).await?;
        let comments = self.comment_keys(&drawer).await?;
        let ids = ShortIds::new(
            view.entries
                .iter()
                .map(|(key, _)| key)
                .chain(&comments)
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        );
        let mut removed = BTreeSet::new();
        let mut removed_comments = BTreeSet::new();
        for item in items {
            let found = ids.resolve(item)?.parse::<DatumKey>()?;
            if comments.contains(&found) {
                removed_comments.insert(found);
            } else {
                removed.insert(found);
            }
        }
        let pack_of = |value: &Value| -> Option<DatumKey> {
            let pack: DatumReference = value.get("pack")?.as_str()?.parse().ok()?;
            Some(pack.key().clone())
        };
        let kept: BTreeSet<DatumKey> = view
            .entries
            .iter()
            .filter(|(key, _)| !removed.contains(key))
            .filter_map(|(_, value)| pack_of(value))
            .collect();
        let packs: BTreeSet<DatumKey> = view
            .entries
            .iter()
            .filter(|(key, _)| removed.contains(key))
            .filter_map(|(_, value)| pack_of(value))
            .filter(|pack| !kept.contains(pack))
            .collect();
        if !removed.is_empty() {
            self.caller
                .remove_entries(&drawer, &removed, &packs)
                .await?;
        }
        let removed_comments = self.remove_comments(&drawer, removed_comments).await?;
        writeln!(
            out,
            "removed from {} \"{}\"",
            shown(&intxts, &drawer.to_string()),
            view.title.title
        )?;
        for entry in removed.iter().chain(&removed_comments) {
            writeln!(out, "  {entry}")?;
        }
        Ok(())
    }

    pub(super) async fn load(&self, drawer: &DrawerId) -> Res<View> {
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

    /// What capture holds for the worktree `identity` names, after recording the branch
    /// `resolved` tracks if capture never saw it where it is now (FDR 008 W6): the work was done
    /// before intxt was installed or on another machine, so no hook saw the branch move. The
    /// commits are not recorded: they are in the local repository, `resolved` names them from
    /// git, and `add` writes them into the intxt.
    ///
    /// Recording is an observation under the host's worktree lock ([`host::prepare_observation`])
    /// whose batch the recorder writes (the identity documents and the `gitRef`). The host commits
    /// the recorder's answer as its state, the state hooks continue from, so a hook after it
    /// reports only what moves later. No session is derived: none was recorded.
    async fn recorded_with_ref(
        &self,
        identity: &Identity,
        resolved: &Resolved,
    ) -> Res<host::RecordedWorktree> {
        let recorded = host::recorded_worktree(identity)?;
        // A jj change's commits live in jj's git directory, which the worktree's observation
        // does not read; its sessions come from what capture saw of the worktree.
        if resolved.git_dir != identity.root
            || self.ref_recorded(&recorded, identity, resolved).await?
        {
            return Ok(recorded);
        }
        let observed = identity.clone();
        let prepared =
            tokio::task::spawn_blocking(move || host::prepare_observation(&observed)).await??;
        super::capture::record_prepared(self.client, self.node_dir, prepared).await?;
        host::recorded_worktree(identity).map_err(Into::into)
    }

    /// Whether the clone's document holds the branch `resolved` tracks where it points now.
    /// Without a tracked branch there is nothing to hold.
    async fn ref_recorded(
        &self,
        recorded: &host::RecordedWorktree,
        identity: &Identity,
        resolved: &Resolved,
    ) -> Res<bool> {
        let Some((reference, _)) = &resolved.tracked else {
            return Ok(true);
        };
        let Some(clone) = &recorded.docs.clone else {
            return Ok(false);
        };
        let ref_key = key(
            work::GitRef::ID,
            &format!("{}/{reference}", identity.clone_id),
        )?;
        let Some(stored) = self.caller.try_read(&clone.parse()?, &ref_key).await?.1 else {
            return Ok(false);
        };
        let stored: work::GitRef = serde_json::from_value(stored)?;
        Ok(resolved.tips.contains(&stored.target))
    }

    /// The ops of the worktree whose document is `doc`, by sequence number, each with the op-log
    /// document that holds it: they are in the capture drawer's documents.
    pub(super) async fn worktree_ops(
        &self,
        doc: &DocId,
    ) -> Res<BTreeMap<u64, (DocId, work::WorktreeOp)>> {
        let (_, worktree) = self
            .caller
            .read(doc, &key(work::Worktree::ID, "main")?)
            .await?;
        let worktree: work::Worktree = serde_json::from_value(worktree)?;
        let mut ops = BTreeMap::new();
        for log in &worktree.op_logs {
            let log_doc: DocId = log.doc.parse()?;
            let (_, datums) = self.caller.read_datums(&log_doc, None).await?;
            for (key, datum) in datums {
                if key.tag().as_str() != work::WorktreeOp::ID {
                    continue;
                }
                let DatumRead::Accepted { value, .. } = datum else {
                    eyre::bail!("{key} in op log {} is not readable: {datum:?}", log.doc);
                };
                let seq = String::from_utf8_lossy(key.slot_bytes()).parse::<u64>()?;
                ops.insert(seq, (log_doc.clone(), serde_json::from_value(value)?));
            }
        }
        Ok(ops)
    }

    /// Puts `pack` in the blob store and answers the blob datum that names it: its key and value.
    async fn put_pack(&self, pack: Vec<u8>) -> Res<(DatumKey, Value)> {
        let info = super::put_blob(self.client, &pack).await?;
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
        drawer: &DrawerId,
        view: &View,
        resolved: &Resolved,
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
        self.ensure_inventory(drawer).await?;
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
        // `recorded_with_ref` recorded the clone and the branch's position if they were not.
        let clone_doc: DocId = recorded
            .docs
            .clone
            .as_deref()
            .ok_or_else(|| eyre::eyre!("importing {reference} left the clone unrecorded"))?
            .parse()?;
        let ref_key = key(work::GitRef::ID, &slot)?;
        eyre::ensure!(
            self.caller
                .try_read(&clone_doc, &ref_key)
                .await?
                .1
                .is_some(),
            "importing {reference} left it unrecorded"
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
        let ops: Vec<work::WorktreeOp> = match &recorded.docs.worktree {
            Some(doc) => self
                .worktree_ops(&doc.parse()?)
                .await?
                .into_values()
                .map(|(_, op)| op)
                .collect(),
            None => Vec::new(),
        };
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
        intxt: Option<&str>,
        named: Named,
        full: bool,
    ) -> Res<()> {
        let (drawer, intxts) = self.resolve(intxt).await?;
        let view = self.load(&drawer).await?;
        let identity = self.identity()?;
        let resolved = Resolved::new(&named, &identity)?;
        let recorded = self.recorded_with_ref(&identity, &resolved).await?;

        let mut plan = Plan::default();
        let new_commits = self
            .plan_commits(&mut plan, &drawer, &view, &resolved, full)
            .await?;
        self.plan_ref(&mut plan, &view, &resolved, &identity, &recorded)
            .await?;
        // The sessions of commits already in the intxt were derived when they were added.
        let (sessions, present) = if new_commits {
            self.following_sessions(&view, &resolved, &recorded).await?
        } else {
            Default::default()
        };

        // Written even when the entry is already in, so an intxt that holds a range from before
        // its commits document existed gets its commits by adding the range again.
        let (wrote_commits, commit_lines) = self.write_commits(&drawer, &resolved).await?;
        if wrote_commits > 0 {
            plan.lines.push(format!(
                "  commit data {wrote_commits} commit(s) in the intxt's commits document"
            ));
        }
        plan.lines.extend(commit_lines);
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
        if new_commits && sessions.is_empty() && present.is_empty() {
            plan.lines
                .push("  sessions   none recorded for these commits".to_owned());
        }

        let heading = format!(
            "{} \"{}\"",
            shown(&intxts, &drawer.to_string()),
            view.title.title
        );
        writeln!(
            out,
            "{} {heading}",
            if written.is_empty() && wrote_commits == 0 {
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

    /// `day x intxt which`: the intxts on this node that hold the commit `name` names in this
    /// worktree's git (a full sha, a unique prefix or any name git resolves), one line each: its
    /// short id, its title and its `link:`. None holding it prints nothing.
    ///
    /// The answer is the membership query anchored on each live intxt's drawer in turn
    /// ([`Caller::commit_holders`]). That per-drawer loop goes away with the per-user root
    /// drawer that holds every intxt drawer: one transitive query on it answers for all.
    pub(crate) async fn which(&self, out: &mut impl Write, name: &str) -> Res<()> {
        let sha = revisions::commit_of(&Git::new(&self.identity()?.root), name)?;
        let live = self.live_intxts().await?;
        let ids = ShortIds::new(live.iter().map(ToString::to_string).collect::<Vec<_>>());
        for drawer in &live {
            if self.caller.commit_holders(drawer, &sha).await?.is_empty() {
                continue;
            }
            let (_, title) = self
                .caller
                .read(drawer, &key(generic::Title::ID, "main")?)
                .await?;
            let title: generic::Title = serde_json::from_value(title)?;
            writeln!(
                out,
                "{}  \"{}\"  link: {}",
                shown(&ids, &drawer.to_string()),
                title.title,
                generic::Intxt::reference(drawer)
            )?;
        }
        Ok(())
    }

    /// `day x intxt show`: the title, the description, and the entries by kind, each with its
    /// short id among the intxt's entries.
    pub(crate) async fn show(&self, out: &mut impl Write, intxt: Option<&str>) -> Res<()> {
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
        // The text that names this intxt on any node, for a PR description or a commit message.
        writeln!(out, "link: {}", generic::Intxt::reference(&drawer))?;
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
        // Commits imported from git history (FDR 008 W6) come with no sessions, none having been
        // recorded; an intxt with commits and no sessions says so rather than leaving the
        // reader to wonder whether they were missed.
        let no_sessions = !(commits.is_empty() && changes.is_empty()) && sessions.is_empty();
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
        if no_sessions {
            writeln!(out, "sessions: none recorded")?;
        }
        if view.entries.is_empty() {
            writeln!(out, "nothing in it yet")?;
        }
        Ok(())
    }

    /// `day x intxt status`: what is recorded in the worktree, and which intxts include its
    /// commits, its clone's refs and its sessions.
    /// `day x intxt use`: puts the worktree on the intxt `name` names, or on none, or says which
    /// one it is on.
    pub(crate) async fn use_intxt(
        &self,
        out: &mut impl Write,
        name: Option<&str>,
        none: bool,
    ) -> Res<()> {
        let identity = self.identity()?;
        if none {
            host::put_on_intxt(&identity, None)?;
            writeln!(out, "this worktree is on no intxt")?;
            return Ok(());
        }
        if name.is_none() && host::recorded_worktree(&identity)?.intxt.is_none() {
            writeln!(out, "this worktree is on no intxt")?;
            return Ok(());
        }
        let (drawer, intxts) = self.resolve(name).await?;
        if name.is_some() {
            host::put_on_intxt(&identity, Some(drawer.to_string()))?;
        }
        let view = self.load(&drawer).await?;
        writeln!(
            out,
            "this worktree is on {}  \"{}\"",
            shown(&intxts, &drawer.to_string()),
            view.title.title
        )?;
        writeln!(out, "link: {}", generic::Intxt::reference(&drawer))?;
        Ok(())
    }

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
        match &recorded.intxt {
            Some(drawer) => writeln!(out, "on intxt {drawer}")?,
            None => writeln!(out, "on no intxt (day x intxt use <intxt>)")?,
        }
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

/// `name` as a full drawer id, when it is one. A drawer id's text is `z` and base58, and short ids
/// are drawn from jj's `z`–`k` alphabet, which base58 accepts too, so a short id starting with `z`
/// parses as a key of a few bytes (`z` alone as the empty key). Drawers are 32-byte keys and a short
/// id is too short to spell one, so the width is what tells them apart.
fn full_drawer_id(name: &str) -> Option<DrawerId> {
    name.parse::<DrawerId>()
        .ok()
        .filter(|drawer| drawer.as_bytes().len() == 32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::reference::Location;

    #[test]
    fn short_ids_starting_with_z_are_not_drawer_ids() {
        let drawer = DrawerId::new([7; 32]);
        assert_eq!(full_drawer_id(&drawer.to_string()), Some(drawer));
        for short in ["z", "zq", "zyxwvutsrqponmk", "kz"] {
            assert_eq!(full_drawer_id(short), None, "{short}");
        }
    }

    #[test]
    fn an_amended_commit_keeps_its_patch_id_and_a_changed_patch_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(["-c", "user.name=t", "-c", "user.email=t@example.org"])
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success(), "{args:?}: {output:?}");
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        let read = || {
            let sha = git(&["rev-parse", "HEAD"]);
            intxt_capture::git::commit(&Git::new(dir.path()), &sha).unwrap()
        };
        git(&["init", "-q", "-b", "main"]);
        std::fs::write(dir.path().join("a"), "one\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "base"]);
        std::fs::write(dir.path().join("b"), "two\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "work"]);
        let before = read();
        git(&["commit", "-q", "--amend", "-m", "work, reworded"]);
        let reworded = read();
        assert_eq!(before.patch_id, reworded.patch_id);
        assert_ne!(before.message, reworded.message);
        std::fs::write(dir.path().join("b"), "three\n").unwrap();
        git(&["commit", "-q", "-a", "--amend", "-m", "work, changed"]);
        assert_ne!(before.patch_id, read().patch_id);

        let patch_id = before.patch_id.clone();
        let held = |shas: &[&str]| -> BTreeMap<String, Option<String>> {
            shas.iter()
                .map(|sha| ((*sha).to_owned(), patch_id.clone()))
                .collect()
        };
        let added = BTreeMap::from([("new".to_owned(), reworded.clone())]);
        let (pairs, lines) = match_rewrites(&added, &held(&["old"]));
        assert_eq!(pairs, [("old".to_owned(), "new".to_owned())]);
        assert!(lines.is_empty());

        // Two held commits with the patch id: no single predecessor.
        let (pairs, lines) = match_rewrites(&added, &held(&["old1", "old2"]));
        assert!(pairs.is_empty());
        assert_eq!(lines.len(), 1, "{lines:?}");
        // Two new commits with it would claim one predecessor: also nothing.
        let both = BTreeMap::from([
            ("new1".to_owned(), reworded.clone()),
            ("new2".to_owned(), reworded),
        ]);
        let (pairs, lines) = match_rewrites(&both, &held(&["old"]));
        assert!(pairs.is_empty());
        assert_eq!(lines.len(), 1, "{lines:?}");
        // A held commit without a patch id (an older datum) matches nothing.
        let unknown = BTreeMap::from([("old".to_owned(), None)]);
        let (pairs, lines) = match_rewrites(&added, &unknown);
        assert!(pairs.is_empty() && lines.is_empty());
    }

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
