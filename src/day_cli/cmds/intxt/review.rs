//! intxt's review verbs (FDR 008 "Review"): `comment` puts a comment on lines of a commit or an op
//! of an intxt, `reply` answers one and resolves or reopens its thread, and `inbox` lists what is
//! waiting for the caller and where each comment is now.
//!
//! The datums are ADR 041's. They live in notes documents, content documents of the intxt's
//! drawer: one per writer, made by the writer's first comment and found by their
//! `org.example.intxt.notes/main` marker (ADR 041 §6), so two nodes' first comments never race
//! for one document. Like the packaging verbs these are client code: the caller writes with
//! the operator's own authority, so the cabinet attests the operator for every caller on a node;
//! what makes two readers two sides is the `creator` a comment claims, detected by
//! [`super::participant`] (ADR 041 §2).
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
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::drawer::protocol::DrawerId;
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
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;

/// Which end of a commit's diff a line number counts on, as GitHub's `side` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Side {
    /// The commit's parent.
    Old,
    /// The commit itself.
    New,
}

/// The intxt's notes documents as the reader sees them, every writer's together.
struct Notes {
    posted: Vec<Posted>,
    /// Thread state by the root comment's slot.
    resolved: BTreeMap<String, bool>,
    /// This participant's `seen` slots.
    seen: BTreeMap<DatumKey, annotation::Seen>,
}

impl Notes {
    /// Every datum key of one participant's seen record: comment key to the heads last read.
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
    /// The intxt's notes documents, one per writer that has commented, as their markers say.
    async fn notes_docs(&self, drawer: &DrawerId) -> Res<Vec<DocId>> {
        Ok(self
            .caller
            .tagged_docs(drawer, annotation::Notes::ID)
            .await?
            .into_keys()
            .collect())
    }

    /// The keys of the comments in the intxt's notes documents.
    pub(super) async fn comment_keys(
        &self,
        drawer: &DrawerId,
    ) -> Result<BTreeSet<DatumKey>, eyre::Report> {
        let mut keys = BTreeSet::new();
        for doc in self.notes_docs(drawer).await? {
            keys.extend(
                self.caller
                    .datum_keys(&doc)
                    .await?
                    .into_iter()
                    .filter(|key| key.tag().as_str() == annotation::Comment::ID),
            );
        }
        Ok(keys)
    }

    /// Removes `comments` from the intxt's notes documents, each thread whose first comment is
    /// among them whole, with its state: every comment from the document it is in, and the state
    /// from the document of the thread's root. Answers the keys removed.
    pub(super) async fn remove_comments(
        &self,
        drawer: &DrawerId,
        comments: BTreeSet<DatumKey>,
    ) -> Res<BTreeSet<DatumKey>> {
        if comments.is_empty() {
            return Ok(comments);
        }
        let notes = self.read_notes(drawer).await?;
        let homes: BTreeMap<DatumKey, DocId> = notes
            .posted
            .iter()
            .map(|post| (post.key.clone(), post.doc.clone()))
            .collect();
        let mut remove = comments.clone();
        let mut states = BTreeMap::new();
        for thread in Thread::group(notes.posted) {
            if comments.contains(&thread.root) {
                remove.extend(thread.comments.into_iter().map(|post| post.key));
                states.insert(
                    key(
                        ThreadState::ID,
                        &String::from_utf8_lossy(thread.root.slot_bytes()),
                    )?,
                    homes
                        .get(&thread.root)
                        .ok_or_else(|| eyre::eyre!("{} is in no notes document", thread.root))?
                        .clone(),
                );
            }
        }
        let mut by_doc: BTreeMap<DocId, BTreeSet<DatumKey>> = BTreeMap::new();
        for comment in &remove {
            let doc = homes
                .get(comment)
                .ok_or_else(|| eyre::eyre!("{comment} is in no notes document"))?;
            by_doc
                .entry(doc.clone())
                .or_default()
                .insert(comment.clone());
        }
        for (state, doc) in states {
            remove.insert(state.clone());
            by_doc.entry(doc).or_default().insert(state);
        }
        for (doc, keys) in by_doc {
            self.caller
                .write_datums(&doc, BTreeMap::new(), keys)
                .await?;
        }
        Ok(remove)
    }

    /// The slot prefix of this participant's `seen` records: `<actor uuid>/` for the operator's
    /// `Person`, `<actor uuid>.<scope>/` for any other participant (see [`annotation::Seen`]).
    async fn seen_prefix(&self) -> Res<String> {
        let actor = self.caller.actor().await?;
        Ok(match self.participant.creator.seen_scope() {
            None => format!("{actor}/"),
            Some(scope) => format!("{actor}.{scope}/"),
        })
    }

    /// Every writer's notes documents read together, with this participant's seen records (the
    /// only ones read: a reader's seen records are in the reader's own notes document, and
    /// another actor's slots never match this actor's prefix).
    async fn read_notes(&self, drawer: &DrawerId) -> Res<Notes> {
        let mut notes = Notes {
            posted: Vec::new(),
            resolved: BTreeMap::new(),
            seen: BTreeMap::new(),
        };
        let mine = self.seen_prefix().await?;
        for doc in self.notes_docs(drawer).await? {
            let (_, datums) = self.caller.read_datums(&doc, None).await?;
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
                    eyre::bail!(
                        "{datum_key} in the notes document {doc} is not readable: {read:?}"
                    );
                };
                if tag == annotation::Comment::ID {
                    notes.posted.push(Posted {
                        doc: doc.clone(),
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
        }
        Ok(notes)
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
                // The commit's datum is in a commits document of the intxt, written by whoever
                // `add`ed it: the same datum in any of them, so the first is as good as another.
                let commits_doc = self
                    .caller
                    .holders(&drawer, key(work::GitCommit::ID, &sha)?)
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "no commits document of the intxt {} holds {}: run `day x intxt add` again",
                            shown(&intxts, &drawer.to_string()),
                            short_sha(&sha)
                        )
                    })?;
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
            creator: self.participant.creator.clone(),
            generator: self.participant.generator.clone(),
            extra: Default::default(),
        };
        let comment_key = key(annotation::Comment::ID, &Uuid7::new().to_string())?;
        let notes = self.caller.own_doc(&drawer, annotation::Notes::ID).await?;
        self.caller
            .write_datums(
                &notes,
                [(comment_key.clone(), to_value(&comment))].into(),
                BTreeSet::new(),
            )
            .await?;
        let names = view.names(&self.comment_keys(&drawer).await?);
        let short = shown(&names, &comment_key.to_string());
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

    /// `day x intxt reply`: answers the comment `comment` names in `intxt` (default: the intxt
    /// this worktree is on) with `text`, and resolves or reopens its thread when asked. With no
    /// text, only the thread's state is written. `comment` is resolved against the names
    /// `comment` and `inbox` printed it from: that one intxt's entries and comments.
    pub(crate) async fn reply(
        &self,
        out: &mut impl Write,
        intxt: Option<&str>,
        comment: &str,
        text: Option<&str>,
        resolved: Option<bool>,
    ) -> Res<()> {
        let on;
        let intxt = match intxt {
            Some(intxt) => intxt,
            None => {
                on = host::recorded_worktree(&self.identity()?)?.intxt.ok_or_else(|| {
                    eyre::eyre!(
                        "name the intxt the comment is in with `--intxt <intxt>`, or put this worktree on one with `day x intxt use <intxt>`"
                    )
                })?;
                on.as_str()
            }
        };
        let (drawer, intxts) = self.resolve(Some(intxt)).await?;
        let names = self
            .load(&drawer)
            .await?
            .names(&self.comment_keys(&drawer).await?);
        let parent_key = names
            .resolve(comment)
            .map_err(|error| {
                eyre::eyre!(
                    "{error}; comments are named by the short id `comment` and `inbox` print for this intxt"
                )
            })?
            .parse::<DatumKey>()?;
        let notes = self.read_notes(&drawer).await?;
        let thread = Thread::group(notes.posted)
            .into_iter()
            .find(|thread| thread.comments.iter().any(|post| post.key == parent_key))
            .ok_or_else(|| eyre::eyre!("{parent_key} is in no thread"))?;
        let post_of = |key: &DatumKey| {
            thread
                .comments
                .iter()
                .find(|post| &post.key == key)
                .ok_or_else(|| eyre::eyre!("{key} is gone from its thread"))
        };
        let root_slot = String::from_utf8_lossy(thread.root.slot_bytes()).into_owned();
        // The reply goes in the caller's own notes document and points at its parent in the
        // parent's, so a thread spans documents. The thread's state is in the document of its
        // root comment, which exists before anyone resolves the thread.
        let mut writes: BTreeMap<DocId, BTreeMap<DatumKey, serde_json::Value>> = BTreeMap::new();
        let mut reply_key = None;
        if let Some(text) = text {
            let new_key = key(annotation::Comment::ID, &Uuid7::new().to_string())?;
            let own = self.caller.own_doc(&drawer, annotation::Notes::ID).await?;
            writes.entry(own).or_default().insert(
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
                        ReferenceDocId::Concrete(post_of(&parent_key)?.doc.clone()),
                        parent_key.clone(),
                    )),
                    creator: self.participant.creator.clone(),
                    generator: self.participant.generator.clone(),
                    extra: Default::default(),
                }),
            );
            reply_key = Some(new_key);
        }
        if let Some(resolved) = resolved {
            writes
                .entry(post_of(&thread.root)?.doc.clone())
                .or_default()
                .insert(
                    key(ThreadState::ID, &root_slot)?,
                    to_value(&ThreadState {
                        resolved,
                        extra: Default::default(),
                    }),
                );
        }
        for (doc, set) in writes {
            self.caller.write_datums(&doc, set, BTreeSet::new()).await?;
        }

        let names = self
            .load(&drawer)
            .await?
            .names(&self.comment_keys(&drawer).await?);
        let drawer_short = shown(&intxts, &drawer.to_string());
        let thread_short = shown(&names, &thread.root.to_string());
        match (&reply_key, resolved) {
            (Some(reply), _) => {
                let short = shown(&names, &reply.to_string());
                writeln!(
                    out,
                    "reply {short} to {}{}",
                    shown(&names, &parent_key.to_string()),
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
                "  undo: day x intxt reply {thread_short} --intxt {drawer_short} {}",
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
    /// them seen afterwards. "Someone else" and "seen" are the detected participant's: an agent and
    /// the operator are one actor but two sides.
    pub(crate) async fn inbox(&self, out: &mut impl Write, intxt: Option<&str>) -> Res<()> {
        let (drawer, intxts) = self.resolve(intxt).await?;
        let heading = shown(&intxts, &drawer.to_string());
        let me = &self.participant.creator;
        let seen_prefix = self.seen_prefix().await?;
        let notes = self.read_notes(&drawer).await?;
        if notes.posted.is_empty() {
            writeln!(out, "inbox {heading}: no comments yet")?;
            return Ok(());
        }
        let seen = notes.seen_heads();
        let view = self.load(&drawer).await?;
        let actor = self.caller.actor().await?;
        let ids = view.names(notes.posted.iter().map(|post| &post.key));
        let waiting: Vec<Thread> = Thread::group(notes.posted.clone())
            .into_iter()
            .filter(|thread| {
                let slot = String::from_utf8_lossy(thread.root.slot_bytes());
                !notes.resolved.get(slot.as_ref()).copied().unwrap_or(false)
                    && (thread.waiting_on(me) || thread.unseen_by(me, &seen))
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
                if thread.waiting_on(me) {
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
                    self.participant.name(
                        &post.comment.creator,
                        post.comment.generator.as_ref(),
                        post.authors.contains(&actor),
                    ),
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
            let seen_key = key(annotation::Seen::ID, &format!("{seen_prefix}{range}"))?;
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
        // The reader's seen records go in the reader's own notes document, which a reader who
        // never commented has none of yet.
        let own = self.caller.own_doc(&drawer, annotation::Notes::ID).await?;
        self.caller
            .write_datums(&own, write, BTreeSet::new())
            .await?;
        Ok(())
    }
}
