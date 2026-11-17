//! intxt's review verbs (FDR 008 "Review"): `comment` puts a comment on lines of a commit or an op
//! of an intxt, `reply` answers one and resolves or reopens its thread, and `inbox` lists what is
//! waiting for the caller and where each comment is now.
//!
//! The datums are ADR 041's. They live in the intxt drawer's notes document (a meta document
//! named `notes/annotations`, ADR 041 §6), which the first comment creates and enters with an
//! `annotationsEntry/main`. Like the packaging verbs these are client code: the caller writes with
//! the operator's own authority, and what makes two readers two sides is the actor the cabinet
//! attests wrote each datum, not anything a comment carries.
//!
//! A comment's code target is read in the git repository of the worktree the verb runs in, so the
//! commits an intxt holds must be objects there; carrying a comment to the intxt's current
//! revision is [`anchor::carry`].

use super::package::Packaging;
use super::package::View;
use super::package::sha_of;
use super::package::short_sha;
use super::package::shown;
use crate::interlude::*;

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::GetDrawer;
use big_cabinet_core::reference::DatumPin;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::Location;
use big_cabinet_core::reference::ReferenceDocId;
use intxt_capture::anchor;
use intxt_capture::git::Git;
use intxt_capture::host;
use intxt_capture::revisions;
use intxt_capture::revisions::Revisions;
use intxt_core::annotation;
use intxt_core::annotation::Content;
use intxt_core::annotation::LineRange;
use intxt_core::annotation::Motivation;
use intxt_core::annotation::Posted;
use intxt_core::annotation::Thread;
use intxt_core::annotation::ThreadState;
use intxt_core::client::key;
use intxt_core::client::to_value;
use intxt_core::work;
use short_ids::ShortIds;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;

/// The meta document that holds an intxt's annotations: `notes/annotations`.
const NOTES_SLOT: &str = "notes/annotations";

/// Which end of a commit's diff a line number counts on, as GitHub's `side` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Side {
    /// The commit's parent.
    Old,
    /// The commit itself.
    New,
}

/// The notes document as the reader sees it.
struct Notes {
    doc: DocId,
    posted: Vec<Posted>,
    /// Thread state by the root comment's slot.
    resolved: BTreeMap<String, bool>,
    /// This reader's `seen` slots.
    seen: BTreeMap<DatumKey, annotation::Seen>,
}

impl Notes {
    /// Every datum key of one reader's seen record: comment key to the heads last read.
    fn seen_heads(&self) -> BTreeMap<String, Vec<String>> {
        self.seen
            .values()
            .flat_map(|seen| seen.datums.clone())
            .collect()
    }
}

/// What the intxt holds that a code target can name.
struct Held {
    /// Every commit its commit entries name.
    commits: BTreeSet<String>,
    /// How to say what it holds when a target names something else.
    ranges: Vec<String>,
}

/// The lines as people spell them: `a-b`, or `a` for one line.
fn spelled(lines: &LineRange) -> String {
    if lines.start == lines.end {
        lines.start.to_string()
    } else {
        format!("{}-{}", lines.start, lines.end)
    }
}

fn sorted_heads(version: &big_cabinet_core::doc::DatumVersion) -> Vec<String> {
    let mut heads: Vec<String> = version.heads.iter().map(ToString::to_string).collect();
    heads.sort();
    heads
}

impl Packaging<'_> {
    /// The intxt's notes document, if anything has been written to it.
    async fn notes_doc(&self, drawer: &DrawerId) -> Res<Option<DocId>> {
        let view = self
            .caller
            .call(GetDrawer {
                capability: self.drawer_capability(drawer).await?,
                drawer_id: drawer.clone(),
            })
            .await?;
        Ok(view
            .meta_docs
            .get(&MetaRoot::Sensitive)
            .and_then(|docs| docs.get(NOTES_SLOT))
            .cloned())
    }

    /// The notes document, made if the intxt has none yet, and the intxt's entry for it. The
    /// command id is fixed per drawer, so two writers creating it converge on one document.
    async fn ensure_notes_doc(&self, drawer: &DrawerId, view: &View) -> Res<DocId> {
        let created = self
            .caller
            .call(CreateMetaDoc {
                capability: self.drawer_capability(drawer).await?,
                command: CommandId::try_from(format!("intxt-notes-{drawer}"))?,
                drawer_id: drawer.clone(),
                kind: "notes".into(),
                name: "annotations".into(),
                root: MetaRoot::Sensitive,
            })
            .await?;
        let entry = key(annotation::AnnotationsEntry::ID, "main")?;
        if !view.has(&entry) {
            self.caller
                .include(
                    drawer,
                    [(
                        entry.clone(),
                        to_value(&annotation::AnnotationsEntry {
                            empty: Some(true),
                            extra: Default::default(),
                        }),
                    )]
                    .into(),
                    &[entry],
                )
                .await?;
        }
        Ok(created.doc_id)
    }

    /// The keys of the comments in the intxt's notes document.
    pub(super) async fn comment_keys(
        &self,
        drawer: &DrawerId,
    ) -> Result<BTreeSet<DatumKey>, eyre::Report> {
        let Some(doc) = self.notes_doc(drawer).await? else {
            return Ok(BTreeSet::new());
        };
        Ok(self
            .caller
            .datum_keys(&doc)
            .await?
            .into_iter()
            .filter(|key| key.tag().as_str() == annotation::Comment::ID)
            .collect())
    }

    /// Removes `comments` from the intxt's notes document, each thread whose first comment is
    /// among them whole, with its state. Answers the keys removed.
    pub(super) async fn remove_comments(
        &self,
        drawer: &DrawerId,
        comments: BTreeSet<DatumKey>,
    ) -> Res<BTreeSet<DatumKey>> {
        if comments.is_empty() {
            return Ok(comments);
        }
        let me = self.caller.actor().await?;
        let notes = self
            .read_notes(drawer, &me)
            .await?
            .ok_or_else(|| eyre::eyre!("{drawer} has no notes document to remove comments from"))?;
        let mut remove = comments.clone();
        for thread in Thread::group(notes.posted) {
            if comments.contains(&thread.root) {
                remove.extend(thread.comments.into_iter().map(|post| post.key));
                remove.insert(key(
                    ThreadState::ID,
                    &String::from_utf8_lossy(thread.root.slot_bytes()),
                )?);
            }
        }
        self.caller
            .write_datums(&notes.doc, BTreeMap::new(), remove.clone())
            .await?;
        Ok(remove)
    }

    /// The notes document with `me`'s seen records; none when the intxt has no notes document.
    async fn read_notes(&self, drawer: &DrawerId, me: &Uuid7) -> Res<Option<Notes>> {
        let Some(doc) = self.notes_doc(drawer).await? else {
            return Ok(None);
        };
        let (_, datums) = self.caller.read_datums(&doc, None).await?;
        let mut notes = Notes {
            doc,
            posted: Vec::new(),
            resolved: BTreeMap::new(),
            seen: BTreeMap::new(),
        };
        let mine = format!("{me}/");
        for (datum_key, read) in datums {
            let tag = datum_key.tag().as_str();
            let slot = String::from_utf8_lossy(datum_key.slot_bytes()).into_owned();
            if ![
                annotation::Comment::ID,
                ThreadState::ID,
                annotation::Seen::ID,
            ]
            .contains(&tag)
            {
                continue;
            }
            if tag == annotation::Seen::ID && !slot.starts_with(&mine) {
                continue;
            }
            let DatumRead::Accepted {
                value,
                version,
                authors,
            } = read
            else {
                eyre::bail!("{datum_key} in the notes document is not readable: {read:?}");
            };
            if tag == annotation::Comment::ID {
                notes.posted.push(Posted {
                    key: datum_key,
                    comment: serde_json::from_value(value)?,
                    heads: sorted_heads(&version),
                    authors: authors.into_iter().map(|author| author.actor).collect(),
                });
            } else if tag == ThreadState::ID {
                let state: ThreadState = serde_json::from_value(value)?;
                notes.resolved.insert(slot, state.resolved);
            } else {
                notes.seen.insert(datum_key, serde_json::from_value(value)?);
            }
        }
        Ok(Some(notes))
    }

    /// The commits the intxt's commit entries name, read in the worktree's repository.
    fn held(&self, git: &Git, view: &View) -> Res<Held> {
        let mut held = Held {
            commits: BTreeSet::new(),
            ranges: Vec::new(),
        };
        for (entry, value) in &view.entries {
            if entry.tag().as_str() != work::GitCommitEntry::ID {
                continue;
            }
            let entry: work::GitCommitEntry = serde_json::from_value(value.clone())?;
            let revisions = Revisions {
                tip: sha_of(&entry.tip).to_owned(),
                base: entry.base.as_deref().map(|base| sha_of(base).to_owned()),
            };
            let commits = revisions::commits_of(git, &revisions).wrap_err_with(|| {
                format!(
                    "reading the commits {}..{} the intxt holds in {}",
                    revisions.base.as_deref().map(short_sha).unwrap_or(""),
                    short_sha(&revisions.tip),
                    self.cwd.display()
                )
            })?;
            held.ranges.push(format!(
                "{}..{} ({} commit(s))",
                revisions.base.as_deref().map(short_sha).unwrap_or(""),
                short_sha(&revisions.tip),
                commits.len()
            ));
            held.commits.extend(commits);
        }
        Ok(held)
    }

    /// The op-log documents of the worktree `doc` and its ops, when the intxt holds that
    /// worktree.
    async fn held_worktree(
        &self,
        view: &View,
        doc: &DocId,
    ) -> Res<Option<BTreeMap<u64, (DocId, work::WorktreeOp)>>> {
        for (entry, value) in &view.entries {
            if entry.tag().as_str() != work::WorktreeEntry::ID {
                continue;
            }
            let entry: work::WorktreeEntry = serde_json::from_value(value.clone())?;
            if entry.worktree.document() == &ReferenceDocId::Concrete(doc.clone()) {
                return Ok(Some(self.worktree_ops(doc).await?));
            }
        }
        Ok(None)
    }

    /// `day x intxt comment`: a comment on `spelling` (`<commit>:<path>:<lines>` or
    /// `op<seq>:<path>:<lines>`), which must name a commit or an op the intxt holds.
    pub(crate) async fn comment(
        &self,
        out: &mut impl Write,
        intxt: &str,
        spelling: &str,
        text: &str,
        side: Side,
        motivation: Motivation,
    ) -> Res<()> {
        let (drawer, intxts) = self.resolve(Some(intxt)).await?;
        let view = self.load(&drawer).await?;
        let identity = self.identity()?;
        let git = Git::new(&identity.root);
        let recorded = host::recorded_worktree(&identity)?;
        let target = anchor::Target::parse(spelling)?;
        let holds = |held: &Held| {
            format!(
                "the intxt {} holds {}",
                shown(&intxts, &drawer.to_string()),
                if held.ranges.is_empty() {
                    "no commits".to_owned()
                } else {
                    held.ranges.join(", ")
                }
            )
        };

        // The source the target is read at, as the datum the intxt holds, and the blob there.
        let (location, blob, at) = match &target.source {
            anchor::Source::Commit(spec) => {
                let mut sha = revisions::commit_of(&git, spec)?;
                if side == Side::Old {
                    sha = anchor::first_parent(&git, &sha)?.ok_or_else(|| {
                        eyre::eyre!("{} is a root commit: it has no old side", short_sha(&sha))
                    })?;
                }
                let held = self.held(&git, &view)?;
                eyre::ensure!(
                    held.commits.contains(&sha),
                    "{} is not a commit {}{}",
                    short_sha(&sha),
                    holds(&held),
                    if side == Side::Old {
                        " (--side old targets the commit's parent, which must be held too)"
                    } else {
                        ""
                    }
                );
                // The commit's datum is in the intxt's commits document, written by `add`.
                let commits_doc = self.ensure_commits_doc(&drawer).await?;
                let blob = anchor::commit_blob(&git, &sha, &target.path)?
                    .ok_or_else(|| eyre::eyre!("{} has no {}", short_sha(&sha), target.path))?;
                let location = Location::new(
                    DatumReference::new(
                        ReferenceDocId::Concrete(commits_doc),
                        key(work::GitCommit::ID, &sha)?,
                    ),
                    None,
                )?;
                (location, blob, short_sha(&sha).to_owned())
            }
            anchor::Source::Op(seq) => {
                eyre::ensure!(
                    side == Side::New,
                    "--side old needs a commit target: an op has no parent"
                );
                let worktree: DocId = recorded
                    .docs
                    .worktree
                    .as_deref()
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "capture has recorded no ops for {}",
                            identity.root.display()
                        )
                    })?
                    .parse()?;
                let ops = self.held_worktree(&view, &worktree).await?.ok_or_else(|| {
                    eyre::eyre!(
                        "the intxt {} holds no worktree entry for {}, so no op of it",
                        shown(&intxts, &drawer.to_string()),
                        identity.root.display()
                    )
                })?;
                let (log, _) = ops.get(seq).ok_or_else(|| {
                    eyre::eyre!(
                        "op{seq} is not in this worktree's log, which has ops 0..{}",
                        ops.keys().next_back().map_or(0, |last| last + 1)
                    )
                })?;
                let blob = anchor::blob_at_op(
                    &git,
                    &ops.iter()
                        .map(|(seq, (_, op))| (*seq, op.clone()))
                        .collect(),
                    *seq,
                    &target.path,
                )?
                .ok_or_else(|| eyre::eyre!("op{seq} has no {}", target.path))?;
                let location = Location::new(
                    DatumReference::new(
                        ReferenceDocId::Concrete(log.clone()),
                        key(work::WorktreeOp::ID, &seq.to_string())?,
                    ),
                    None,
                )?;
                (location, blob, format!("op{seq}"))
            }
        };
        let quote = anchor::quote(&anchor::blob_text(&git, &blob)?, &target.lines)?;
        let comment = annotation::Comment {
            targets: vec![annotation::Target {
                source: DatumPin::new(location),
                path: Some(target.path.clone()),
                lines: Some(target.lines.clone()),
                quote: Some(quote),
                to: None,
                extra: Default::default(),
            }],
            body: Content {
                mime: "text/markdown".to_owned(),
                content: text.to_owned(),
                extra: Default::default(),
            },
            motivation,
            reply_to: None,
            extra: Default::default(),
        };
        let comment_key = key(annotation::Comment::ID, &Uuid7::new().to_string())?;
        let notes = self.ensure_notes_doc(&drawer, &view).await?;
        self.caller
            .write_datums(
                &notes,
                [(comment_key.clone(), to_value(&comment))].into(),
                BTreeSet::new(),
            )
            .await?;
        let short = self.comment_short(&drawer, &view, &comment_key).await?;
        writeln!(
            out,
            "comment {short} on {at} {}:{}",
            target.path,
            spelled(&target.lines)
        )?;
        writeln!(
            out,
            "  undo: day x intxt rm {} {short}",
            shown(&intxts, &drawer.to_string())
        )?;
        Ok(())
    }

    /// `comment`'s short id among what `rm` resolves in the intxt: its entries and comments.
    async fn comment_short(
        &self,
        drawer: &DrawerId,
        view: &View,
        comment: &DatumKey,
    ) -> Res<String> {
        let ids = ShortIds::new(
            view.entries
                .iter()
                .map(|(key, _)| key)
                .chain(&self.comment_keys(drawer).await?)
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        );
        Ok(shown(&ids, &comment.to_string()))
    }

    /// The comment `name` names among the comments of every live intxt, and the intxt it is in.
    async fn resolve_comment(&self, name: &str) -> Res<(DrawerId, DatumKey)> {
        let mut all: Vec<(DrawerId, DatumKey)> = Vec::new();
        for drawer in self.live_intxts().await? {
            for comment in self.comment_keys(&drawer).await? {
                all.push((drawer.clone(), comment));
            }
        }
        let ids = ShortIds::new(
            all.iter()
                .map(|(_, key)| key.to_string())
                .collect::<Vec<_>>(),
        );
        let found = ids.resolve(name).map_err(|error| {
            eyre::eyre!("{error}; comments are named by the short id `comment` and `inbox` print")
        })?;
        all.into_iter()
            .find(|(_, key)| &key.to_string() == found)
            .ok_or_else(|| eyre::eyre!("comment {name} vanished while resolving"))
    }

    /// `day x intxt reply`: answers the comment `comment` names with `text`, and resolves or
    /// reopens its thread when asked. With no text, only the thread's state is written.
    pub(crate) async fn reply(
        &self,
        out: &mut impl Write,
        comment: &str,
        text: Option<&str>,
        resolved: Option<bool>,
    ) -> Res<()> {
        let (drawer, parent_key) = self.resolve_comment(comment).await?;
        let intxts = ShortIds::new(
            self.live_intxts()
                .await?
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        );
        let me = self.caller.actor().await?;
        let notes = self
            .read_notes(&drawer, &me)
            .await?
            .ok_or_else(|| eyre::eyre!("{drawer} has no notes document"))?;
        let thread = Thread::group(notes.posted)
            .into_iter()
            .find(|thread| thread.comments.iter().any(|post| post.key == parent_key))
            .ok_or_else(|| eyre::eyre!("{parent_key} is in no thread"))?;
        let root_slot = String::from_utf8_lossy(thread.root.slot_bytes()).into_owned();
        let mut set = BTreeMap::new();
        let mut reply_key = None;
        if let Some(text) = text {
            let new_key = key(annotation::Comment::ID, &Uuid7::new().to_string())?;
            set.insert(
                new_key.clone(),
                to_value(&annotation::Comment {
                    targets: Vec::new(),
                    body: Content {
                        mime: "text/markdown".to_owned(),
                        content: text.to_owned(),
                        extra: Default::default(),
                    },
                    motivation: Motivation::Replying,
                    reply_to: Some(DatumReference::new(
                        ReferenceDocId::Concrete(notes.doc.clone()),
                        parent_key.clone(),
                    )),
                    extra: Default::default(),
                }),
            );
            reply_key = Some(new_key);
        }
        if let Some(resolved) = resolved {
            set.insert(
                key(ThreadState::ID, &root_slot)?,
                to_value(&ThreadState {
                    resolved,
                    extra: Default::default(),
                }),
            );
        }
        self.caller
            .write_datums(&notes.doc, set, BTreeSet::new())
            .await?;

        let view = self.load(&drawer).await?;
        let drawer_short = shown(&intxts, &drawer.to_string());
        let thread_short = self.comment_short(&drawer, &view, &thread.root).await?;
        match (&reply_key, resolved) {
            (Some(reply), _) => {
                let short = self.comment_short(&drawer, &view, reply).await?;
                writeln!(
                    out,
                    "reply {short} to {}{}",
                    self.comment_short(&drawer, &view, &parent_key).await?,
                    match resolved {
                        Some(true) => ", thread resolved",
                        Some(false) => ", thread reopened",
                        None => "",
                    }
                )?;
                writeln!(out, "  undo: day x intxt rm {drawer_short} {short}")?;
            }
            (None, Some(resolved)) => {
                writeln!(
                    out,
                    "thread {thread_short} {}",
                    if resolved { "resolved" } else { "reopened" }
                )?;
            }
            (None, None) => unreachable!("clap requires a message, --resolve or --reopen"),
        }
        if let Some(resolved) = resolved {
            writeln!(
                out,
                "  undo: day x intxt reply {thread_short} {}",
                if resolved { "--reopen" } else { "--resolve" }
            )?;
        }
        Ok(())
    }

    /// The commits the intxt's tracked refs point at now that the intxt also holds: the
    /// revisions its work is currently at.
    async fn current_tips(&self, view: &View, held: &Held) -> Res<Vec<String>> {
        let mut tips = Vec::new();
        for (entry, value) in &view.entries {
            if entry.tag().as_str() != work::GitRefEntry::ID {
                continue;
            }
            let tracked: work::GitRefEntry = serde_json::from_value(value.clone())?;
            let ReferenceDocId::Concrete(clone) = tracked.reference.document() else {
                continue;
            };
            let Some(stored) = self
                .caller
                .try_read(clone, tracked.reference.key())
                .await?
                .1
            else {
                continue;
            };
            let stored: work::GitRef = serde_json::from_value(stored)?;
            if held.commits.contains(&stored.target) && !tips.contains(&stored.target) {
                tips.push(stored.target);
            }
        }
        Ok(tips)
    }

    /// Where the code target `target` is on the intxt's current revision, spelled for `inbox`:
    /// the path with its placed lines, whether the context around them changed, or that the
    /// comment is outdated.
    async fn placed(
        &self,
        git: &Git,
        tips: &[String],
        worktrees: &[BTreeMap<u64, (DocId, work::WorktreeOp)>],
        target: &annotation::Target,
    ) -> Res<String> {
        let (Some(path), Some(lines), Some(quote)) = (&target.path, &target.lines, &target.quote)
        else {
            eyre::bail!("a code target has a path, lines and a quote");
        };
        let location = target.source.location();
        let reference = location.reference();
        let slot = String::from_utf8_lossy(reference.key().slot_bytes()).into_owned();
        let was = |at: &str| format!("was {at}:{}", spelled(lines));
        let (old, new, at, from) = if reference.key().tag().as_str() == work::GitCommit::ID {
            if revisions::commit_of(git, &slot).is_err() {
                return Ok(format!(
                    "{path} (commit {} is not in this clone)",
                    short_sha(&slot)
                ));
            }
            let current = anchor::current_commit(git, &slot, tips)?;
            (
                anchor::commit_blob(git, &slot, path)?,
                anchor::commit_blob(git, &current, path)?,
                short_sha(&current).to_owned(),
                was(short_sha(&slot)),
            )
        } else {
            let seq: u64 = slot.parse()?;
            let ReferenceDocId::Concrete(log) = reference.document() else {
                eyre::bail!("an op is named by its log's document");
            };
            let Some(ops) = worktrees
                .iter()
                .find(|ops| ops.values().any(|(doc, _)| doc == log))
            else {
                return Ok(format!(
                    "{path} (op{seq}'s worktree is not held by this intxt)"
                ));
            };
            let plain: BTreeMap<u64, work::WorktreeOp> = ops
                .iter()
                .map(|(seq, (_, op))| (*seq, op.clone()))
                .collect();
            let latest = *plain.keys().next_back().expect("the op's own log is in it");
            (
                anchor::blob_at_op(git, &plain, seq, path)?,
                anchor::blob_at_op(git, &plain, latest, path)?,
                format!("op{latest}"),
                was(&format!("op{seq}")),
            )
        };
        let (Some(old), Some(new)) = (old, new) else {
            return Ok(format!("{path} (outdated: it is gone at {at}; {from})"));
        };
        Ok(match anchor::carry(git, &old, &new, lines, quote)? {
            anchor::Placement::Placed { lines, exact } => format!(
                "{path}:{} @ {at}{}",
                spelled(&lines),
                if exact { "" } else { " (context changed)" }
            ),
            anchor::Placement::Outdated => format!("{path} (outdated at {at}; {from})"),
        })
    }

    /// `day x intxt inbox`: the unresolved threads of the intxt that the caller has not seen, or
    /// whose last comment someone else wrote, each placed on the intxt's current revision. Marks
    /// them seen afterwards.
    pub(crate) async fn inbox(&self, out: &mut impl Write, intxt: Option<&str>) -> Res<()> {
        let (drawer, intxts) = self.resolve(intxt).await?;
        let heading = shown(&intxts, &drawer.to_string());
        let me = self.caller.actor().await?;
        let Some(notes) = self.read_notes(&drawer, &me).await? else {
            writeln!(out, "inbox {heading}: no comments yet")?;
            return Ok(());
        };
        let seen = notes.seen_heads();
        let view = self.load(&drawer).await?;
        let ids = ShortIds::new(
            view.entries
                .iter()
                .map(|(key, _)| key)
                .chain(notes.posted.iter().map(|post| &post.key))
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        );
        let waiting: Vec<Thread> = Thread::group(notes.posted.clone())
            .into_iter()
            .filter(|thread| {
                let slot = String::from_utf8_lossy(thread.root.slot_bytes());
                !notes.resolved.get(slot.as_ref()).copied().unwrap_or(false)
                    && (thread.waiting_on(&me) || thread.unseen_by(&me, &seen))
            })
            .collect();
        if waiting.is_empty() {
            writeln!(out, "inbox {heading}: nothing waiting")?;
            return Ok(());
        }

        let identity = self.identity()?;
        let git = Git::new(&identity.root);
        let held = self.held(&git, &view)?;
        let tips = self.current_tips(&view, &held).await?;
        let mut worktrees = Vec::new();
        for (entry, value) in &view.entries {
            if entry.tag().as_str() == work::WorktreeEntry::ID {
                let entry: work::WorktreeEntry = serde_json::from_value(value.clone())?;
                if let ReferenceDocId::Concrete(doc) = entry.worktree.document() {
                    worktrees.push(self.worktree_ops(doc).await?);
                }
            }
        }

        writeln!(out, "inbox {heading}: {} thread(s)", waiting.len())?;
        let mut now_seen: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        for thread in &waiting {
            let first = &thread.comments[0];
            let place = match first
                .comment
                .targets
                .iter()
                .find(|target| target.path.is_some())
            {
                Some(target) => self.placed(&git, &tips, &worktrees, target).await?,
                None => "(no code target)".to_owned(),
            };
            writeln!(
                out,
                "thread {}  {place}{}",
                shown(&ids, &thread.root.to_string()),
                if thread.waiting_on(&me) {
                    "  waiting on you"
                } else {
                    ""
                }
            )?;
            for post in &thread.comments {
                writeln!(
                    out,
                    "  {}  {}: {}",
                    shown(&ids, &post.key.to_string()),
                    if post.authors.contains(&me) {
                        "you"
                    } else {
                        "them"
                    },
                    post.comment.body.content.replace('\n', "\n      ")
                )?;
                // One seen slot per range of comment slots, UUIDv7's first four hex digits: its
                // timestamp's top 16 bits, which changes about every 50 days.
                let slot = String::from_utf8_lossy(post.key.slot_bytes()).into_owned();
                now_seen
                    .entry(slot[..4].to_owned())
                    .or_default()
                    .insert(post.key.to_string(), post.heads.clone());
            }
        }

        let mut write = BTreeMap::new();
        for (range, datums) in now_seen {
            let seen_key = key(annotation::Seen::ID, &format!("{me}/{range}"))?;
            let mut record =
                notes
                    .seen
                    .get(&seen_key)
                    .cloned()
                    .unwrap_or_else(|| annotation::Seen {
                        datums: BTreeMap::new(),
                        reviewed: BTreeMap::new(),
                        extra: Default::default(),
                    });
            record.datums.extend(datums);
            write.insert(seen_key, to_value(&record));
        }
        self.caller
            .write_datums(&notes.doc, write, BTreeSet::new())
            .await?;
        Ok(())
    }
}
