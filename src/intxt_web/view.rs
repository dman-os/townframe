//! The view model: everything a page shows about one intxt, as plain data.
//!
//! [`IntxtView`] knows nothing about where it came from. It is built from `intxt_core`'s datum
//! types by [`IntxtView::new`], so a host that can read the datums (the relay node, the review
//! micro's node, a browser webnode) builds the same value, and the components in `ui` render it
//! without knowing which host that was.

use crate::diff::Diff;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use intxt_core::annotation::Comment;
use intxt_core::annotation::Participant;
use intxt_core::annotation::Target;
use intxt_core::generic::PrLinks;
use intxt_core::generic::Title;
use intxt_core::work::AgentSession;
use intxt_core::work::GitCommit;
use intxt_core::work::GitCommitEntry;
use intxt_core::work::GitRef;
use intxt_core::work::GitRefEntry;
use intxt_core::work::GithubPull;
use intxt_core::work::GithubPullEntry;
use intxt_core::work::JjChange;
use intxt_core::work::JjChangeEntry;
use intxt_core::work::MessageKind;
use intxt_core::work::PromptTurn;
use intxt_core::work::SessionEntry;
use intxt_core::work::ToolKind;
use jiff::Timestamp;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// One intxt as a reader sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct IntxtView {
    /// The drawer id, which names the intxt on every node it is shared with.
    pub drawer: String,
    /// The nodes the reader was told hold the intxt (the `via` hint), as node keys.
    pub via: Vec<String>,
    pub title: String,
    /// Markdown source, rendered by the page with raw HTML escaped.
    pub description: Option<String>,
    pub entries: Vec<EntryView>,
    pub threads: Vec<ThreadView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Commits,
    GitRef,
    JjChange,
    Session,
    GithubPull,
}

impl EntryKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Commits => "commits",
            Self::GitRef => "branch",
            Self::JjChange => "jj change",
            Self::Session => "session",
            Self::GithubPull => "pull request",
        }
    }
}

/// What an intxt includes, one row of its entry list.
#[derive(Debug, Clone, PartialEq)]
pub struct EntryView {
    pub kind: EntryKind,
    /// The one line that names it: a commit range, a branch, a session or PR title.
    pub label: String,
    /// Labelled facts: `("base", "main")`, `("state", "merged")`.
    pub refs: Vec<EntryRef>,
    /// Where the entry lives outside the node (a PR's URL), when it has such a place.
    pub href: Option<String>,
    /// For a pull request: its description as Markdown source, without the intxt block.
    pub description: Option<String>,
    /// For a pull request: the intxts its description names (including this one), and a note when
    /// the description's intxt block is there but unreadable.
    pub pr_links: PrLinksView,
    /// For a commit range or a jj change: the commits it names that the intxt holds, newest first.
    pub commits: Vec<CommitView>,
    /// For a commit range or a jj change: the recorded patch of the whole entry (the diff from
    /// its base to its tip), when the intxt holds one.
    pub patch: Option<Diff>,
    /// For a session: what happened in it.
    pub session: Option<SessionSummary>,
}

/// What happened in a session, folded from its `promptTurn` datums one at a time (see
/// [`SessionSummary::add`]), so a host reads the turns in batches and never holds more than a
/// batch. Everything kept is bounded: counts are numbers, at most [`Self::MAX_PROMPTS`] prompts
/// are kept with their text cut at [`Self::MAX_PROMPT_OCTETS`], and at most
/// [`Self::MAX_EDITED_TRACKED`] distinct edited paths are kept (the smallest in sort order, so
/// what is kept does not depend on the order turns arrive in).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSummary {
    /// The earliest and latest turn start.
    pub span: Option<(Timestamp, Timestamp)>,
    pub turns: usize,
    pub tool_calls: usize,
    /// Turns the host could not read (absent, not accepted yet, or not a turn). They are in none
    /// of the other numbers, and the page says so.
    pub unreadable: usize,
    /// Tool calls by `ToolKind` (`edit`, `execute`, ...), as the harness spelled it.
    pub tools: BTreeMap<String, usize>,
    /// The paths an edit, delete or move call touched, at most [`Self::MAX_EDITED_TRACKED`].
    edited: BTreeSet<String>,
    /// Paths that did not fit in `edited`, counted once per touch: past the bound the number of
    /// files is "at least this many more", not exact.
    edited_dropped: usize,
    /// The operator's first [`Self::MAX_PROMPTS`] prompts in session order, each under its sort key
    /// (turn `seq`, message `seq`).
    prompts: Vec<((u64, u64), Prompt)>,
    prompt_count: usize,
}

/// One prompt the operator gave, as far as the page shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Prompt {
    /// The first non-empty line, cut to [`SessionSummary::MAX_HEADLINE_OCTETS`].
    pub headline: String,
    /// The whole prompt, cut to [`SessionSummary::MAX_PROMPT_OCTETS`].
    pub text: String,
    /// The text was cut: the prompt is longer than what is kept.
    pub cut: bool,
}

/// `text` cut to at most `max` octets at a character boundary, and whether it was cut.
fn cut(text: &str, max: usize) -> (&str, bool) {
    if text.len() <= max {
        return (text, false);
    }
    let end = (0..=max)
        .rev()
        .find(|&end| text.is_char_boundary(end))
        .expect("0 is a boundary");
    (&text[..end], true)
}

impl SessionSummary {
    /// The prompts kept.
    pub const MAX_PROMPTS: usize = 50;
    /// The most octets of one prompt's text kept.
    pub const MAX_PROMPT_OCTETS: usize = 4096;
    /// The most octets of a prompt's first line kept.
    pub const MAX_HEADLINE_OCTETS: usize = 120;
    /// The edited paths [`Self::edited_files`] gives.
    pub const MAX_EDITED_FILES: usize = 20;
    /// The distinct edited paths held, which is what makes [`Self::edited_omitted`] exact below it.
    pub const MAX_EDITED_TRACKED: usize = 1000;

    /// Folds one turn in. Turns may come in any order (a document lists its datums by key, not by
    /// `seq`); the summary orders what it keeps by `seq`.
    pub fn add(&mut self, turn: &PromptTurn) {
        self.turns += 1;
        self.span = Some(match self.span {
            Some((first, last)) => (first.min(turn.at), last.max(turn.at)),
            None => (turn.at, turn.at),
        });
        for call in &turn.tool_calls {
            self.tool_calls += 1;
            *self.tools.entry(spelling(&call.kind)).or_default() += 1;
            if matches!(
                call.kind,
                ToolKind::Edit | ToolKind::Delete | ToolKind::Move
            ) {
                // A harness that reports diffs names the files in them; one that only says where it
                // acted names them in the locations.
                let paths: Vec<&str> = if call.diffs.is_empty() {
                    call.locations.iter().map(|at| at.path.as_str()).collect()
                } else {
                    call.diffs.iter().map(|diff| diff.path.as_str()).collect()
                };
                for path in paths {
                    if self.edited.len() >= Self::MAX_EDITED_TRACKED
                        && !self.edited.contains(path)
                        && self.edited.last().is_some_and(|last| last.as_str() < path)
                    {
                        self.edited_dropped += 1;
                        continue;
                    }
                    if self.edited.insert(path.to_owned())
                        && self.edited.len() > Self::MAX_EDITED_TRACKED
                    {
                        self.edited.pop_last();
                        self.edited_dropped += 1;
                    }
                }
            }
        }
        for message in &turn.messages {
            if message.kind != MessageKind::UserMessage {
                continue;
            }
            self.prompt_count += 1;
            let order = (turn.seq, message.seq);
            let at = self.prompts.partition_point(|(held, _)| *held < order);
            if at >= Self::MAX_PROMPTS {
                continue;
            }
            let (text, cut_text) = cut(&message.text, Self::MAX_PROMPT_OCTETS);
            let line = text.lines().find(|line| !line.trim().is_empty());
            let (headline, cut_line) = cut(line.unwrap_or("").trim(), Self::MAX_HEADLINE_OCTETS);
            let prompt = Prompt {
                headline: if cut_line {
                    format!("{headline}…")
                } else {
                    headline.to_owned()
                },
                text: text.to_owned(),
                cut: cut_text,
            };
            self.prompts.insert(at, (order, prompt));
            self.prompts.truncate(Self::MAX_PROMPTS);
        }
    }

    /// The prompts kept, in session order.
    pub fn prompts(&self) -> impl Iterator<Item = &Prompt> {
        self.prompts.iter().map(|(_, prompt)| prompt)
    }

    /// How many prompts the session has beyond those kept.
    pub fn prompts_omitted(&self) -> usize {
        self.prompt_count - self.prompts.len()
    }

    /// The first [`Self::MAX_EDITED_FILES`] edited paths, sorted.
    pub fn edited_files(&self) -> impl Iterator<Item = &str> {
        self.edited
            .iter()
            .take(Self::MAX_EDITED_FILES)
            .map(String::as_str)
    }

    /// How many edited paths there are beyond those [`Self::edited_files`] gives, and whether the
    /// number is a lower bound (more paths were touched than are tracked).
    pub fn edited_omitted(&self) -> (usize, bool) {
        (
            self.edited.len().saturating_sub(Self::MAX_EDITED_FILES) + self.edited_dropped,
            self.edited_dropped > 0,
        )
    }
}

/// One commit of an entry, from the `gitCommit` datum the intxt carries for it.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitView {
    pub sha: String,
    /// `Name <email>`, as the commit says. A claim of the content, not of who wrote the datum.
    pub author: String,
    /// The author date, as `YYYY-MM-DD HH:MM` in UTC.
    pub date: String,
    /// The whole message, subject first.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryRef {
    pub name: &'static str,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PrLinksView {
    pub intxts: Vec<LinkedIntxt>,
    pub malformed: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinkedIntxt {
    /// The viewer path of the linked intxt, `/i/<id>?via=<keys>`.
    pub path: String,
    /// The PR author's name for it, or the drawer id when it gave none.
    pub label: String,
    pub refs: Vec<String>,
}

/// A root comment and the replies under it, oldest first as given.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadView {
    pub root: CommentView,
    pub replies: Vec<CommentView>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommentView {
    /// Who the comment says wrote it. Nothing here proves it: the viewer has no accounts.
    pub author: String,
    /// The software that produced it, when an agent wrote it.
    pub generator: Option<String>,
    /// Why it was written (`commenting`, `questioning`, ...).
    pub motivation: String,
    pub body: String,
    pub targets: Vec<TargetView>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetView {
    /// `path:12-14` for code, else the datum location the comment is pinned to.
    pub label: String,
    pub quote: Option<String>,
}

/// An entry datum together with the datum it names, which the source resolved. The entry alone
/// holds a reference (a branch's name, a session's id), and the page shows the resolved datum.
#[derive(Debug, Clone, Copy)]
pub enum EntryDatum<'a> {
    Commits {
        entry: &'a GitCommitEntry,
        commits: &'a [CommitDatum],
        patch: Option<&'a PatchDatum>,
    },
    GitRef {
        entry: &'a GitRefEntry,
        git_ref: &'a GitRef,
    },
    JjChange {
        entry: &'a JjChangeEntry,
        change: &'a JjChange,
        commits: &'a [CommitDatum],
        patch: Option<&'a PatchDatum>,
    },
    Session {
        entry: &'a SessionEntry,
        session: &'a AgentSession,
        summary: &'a SessionSummary,
    },
    GithubPull {
        entry: &'a GithubPullEntry,
        pull: &'a GithubPull,
    },
}

/// A `gitCommit` datum and the sha of its slot.
#[derive(Debug, Clone)]
pub struct CommitDatum {
    pub sha: String,
    pub commit: GitCommit,
}

/// A patch blob as far as the source read it: `text` is the first octets of `total_octets`.
#[derive(Debug, Clone)]
pub struct PatchDatum {
    pub text: String,
    pub total_octets: u64,
}

/// A comment datum and the reference that names it, which its replies' `replyTo` point at.
#[derive(Debug, Clone, Copy)]
pub struct CommentDatum<'a> {
    pub id: &'a DatumReference,
    pub comment: &'a Comment,
}

impl IntxtView {
    /// The view of the intxt in `drawer`, read through `via`. `entries` and `comments` come in the
    /// order the host reads them (the descriptor's `body` order, the comments' creation order).
    pub fn new(
        drawer: &DocId,
        via: &[MultibaseKey],
        title: &Title,
        entries: &[EntryDatum<'_>],
        comments: &[CommentDatum<'_>],
    ) -> Self {
        Self {
            drawer: drawer.to_string(),
            via: via.iter().map(ToString::to_string).collect(),
            title: title.title.clone(),
            description: title.description.clone(),
            entries: entries.iter().map(entry_view).collect(),
            threads: threads(comments),
        }
    }

    /// The viewer path of this intxt, the one `/i/<id>?via=<keys>` route parses.
    pub fn path(&self) -> String {
        intxt_path(&self.drawer, &self.via)
    }
}

fn intxt_path(drawer: impl std::fmt::Display, via: &[impl std::fmt::Display]) -> String {
    let mut path = format!("/i/{drawer}");
    if !via.is_empty() {
        let keys: Vec<String> = via.iter().map(ToString::to_string).collect();
        path.push_str("?via=");
        path.push_str(&keys.join("|"));
    }
    path
}

/// The wire spelling of an open vocabulary's member, which is what a reader is shown.
fn spelling(member: &impl Serialize) -> String {
    match serde_json::to_value(member).expect("a vocabulary member serializes") {
        serde_json::Value::String(text) => text,
        other => panic!("an open string vocabulary serialized as {other}"),
    }
}

fn short(sha: &str) -> &str {
    sha.get(..8).unwrap_or(sha)
}

fn entry_view(entry: &EntryDatum<'_>) -> EntryView {
    let row = |kind, label: String, refs: Vec<EntryRef>| EntryView {
        kind,
        label,
        refs,
        href: None,
        description: None,
        pr_links: PrLinksView::default(),
        commits: Vec::new(),
        patch: None,
        session: None,
    };
    match entry {
        EntryDatum::Commits {
            entry,
            commits,
            patch,
        } => {
            // The entry's base and tip are commit keys (`<tag>/<sha>`); the page shows shas.
            let tip = sha_of(&entry.tip);
            let label = match &entry.base {
                Some(base) => format!("{}..{}", short(sha_of(base)), short(tip)),
                None => short(tip).to_owned(),
            };
            let mut view = row(EntryKind::Commits, label, Vec::new());
            view.commits = commits.iter().map(commit_view).collect();
            view.patch = patch.map(patch_view);
            view
        }
        EntryDatum::GitRef { entry, git_ref } => {
            // The reference's slot is `<worktree>/<ref name>`; the name is what a reader knows.
            let slot = String::from_utf8_lossy(entry.reference.key().slot_bytes()).into_owned();
            let name = slot.split_once('/').map_or(slot.as_str(), |(_, name)| name);
            let mut refs = vec![EntryRef {
                name: "tip",
                value: short(sha_of(&git_ref.target)).to_owned(),
            }];
            if let Some(base) = git_ref.base.as_ref().or(entry.base.as_ref()) {
                refs.push(EntryRef {
                    name: "base",
                    value: short(sha_of(base)).to_owned(),
                });
            }
            row(EntryKind::GitRef, name.to_owned(), refs)
        }
        EntryDatum::JjChange {
            change,
            commits,
            patch,
            ..
        } => {
            let mut view = row(
                EntryKind::JjChange,
                "change".to_owned(),
                change
                    .commits
                    .iter()
                    .map(|commit| EntryRef {
                        name: "commit",
                        value: short(commit).to_owned(),
                    })
                    .collect(),
            );
            view.commits = commits.iter().map(commit_view).collect();
            view.patch = patch.map(patch_view);
            view
        }
        EntryDatum::Session {
            session, summary, ..
        } => {
            let mut view = row(
                EntryKind::Session,
                session
                    .title
                    .clone()
                    .unwrap_or_else(|| session.session_id.clone()),
                vec![
                    EntryRef {
                        name: "harness",
                        value: session.harness.clone(),
                    },
                    EntryRef {
                        name: "session",
                        value: session.session_id.clone(),
                    },
                ],
            );
            view.session = Some((*summary).clone());
            view
        }
        EntryDatum::GithubPull { pull, .. } => {
            let mut view = row(
                EntryKind::GithubPull,
                pull.title.clone(),
                vec![
                    EntryRef {
                        name: "state",
                        value: spelling(&pull.state),
                    },
                    EntryRef {
                        name: "head",
                        value: pull.head.clone(),
                    },
                    EntryRef {
                        name: "base",
                        value: pull.base.clone(),
                    },
                ],
            );
            view.href = Some(pull.url.clone());
            let (description, links) = pr_description(&pull.body);
            view.description = Some(description).filter(|text| !text.trim().is_empty());
            view.pr_links = links;
            view
        }
    }
}

/// The PR description a reader sees (the text without its intxt block) and the intxts the block
/// names. A block that is there but unreadable leaves the description as written and says so.
fn pr_description(description: &str) -> (String, PrLinksView) {
    match PrLinks::split(description) {
        Ok((text, links)) => (
            text,
            PrLinksView {
                intxts: links
                    .intxts
                    .iter()
                    .map(|link| {
                        let drawer = link.reference.document();
                        LinkedIntxt {
                            path: intxt_path(drawer, link.reference.via()),
                            label: link.title.clone().unwrap_or_else(|| drawer.to_string()),
                            refs: link.refs.clone(),
                        }
                    })
                    .collect(),
                malformed: None,
            },
        ),
        Err(error) => (
            description.to_owned(),
            PrLinksView {
                intxts: Vec::new(),
                malformed: Some(error.to_string()),
            },
        ),
    }
}

fn commit_view(commit: &CommitDatum) -> CommitView {
    let author = &commit.commit.author;
    CommitView {
        sha: commit.sha.clone(),
        author: format!("{} <{}>", author.name, author.email),
        date: author.at.strftime("%Y-%m-%d %H:%M").to_string(),
        message: commit.commit.message.trim_end().to_owned(),
    }
}

fn patch_view(patch: &PatchDatum) -> Diff {
    Diff::parse(&patch.text, patch.total_octets)
}

/// The sha of a commit key (`org.example.intxt.work.gitCommit/<sha>`), or of a bare sha.
pub fn sha_of(commit_key: &str) -> &str {
    commit_key.rsplit('/').next().unwrap_or(commit_key)
}

/// Groups comments into threads by `replyTo`. A reply names its parent, and a reply to a reply
/// joins the thread of the root above it. A parent is only looked for among the comments before
/// the reply (the order is creation order, and a reply cannot predate its parent), which also makes
/// a `replyTo` cycle in unvalidated data harmless. A reply whose parent is not found (not synced to
/// the reading node yet) stands as its own thread rather than disappearing.
fn threads(comments: &[CommentDatum<'_>]) -> Vec<ThreadView> {
    let mut root_of: Vec<usize> = Vec::with_capacity(comments.len());
    for (index, comment) in comments.iter().enumerate() {
        let parent = comment.comment.reply_to.as_ref().and_then(|parent| {
            comments[..index]
                .iter()
                .position(|candidate| candidate.id == parent)
        });
        root_of.push(parent.map_or(index, |parent| root_of[parent]));
    }
    let mut threads: Vec<(usize, ThreadView)> = Vec::new();
    for (index, comment) in comments.iter().enumerate() {
        let view = comment_view(comment.comment);
        if root_of[index] == index {
            threads.push((
                index,
                ThreadView {
                    root: view,
                    replies: Vec::new(),
                },
            ));
        } else {
            let (_, thread) = threads
                .iter_mut()
                .find(|(root, _)| *root == root_of[index])
                .expect("a reply's root precedes it and opened a thread");
            thread.replies.push(view);
        }
    }
    threads.into_iter().map(|(_, thread)| thread).collect()
}

fn comment_view(comment: &Comment) -> CommentView {
    CommentView {
        author: participant(&comment.creator),
        generator: comment.generator.as_ref().map(participant),
        motivation: spelling(&comment.motivation),
        body: comment.body.content.clone(),
        targets: comment.targets.iter().map(target_view).collect(),
    }
}

fn participant(participant: &Participant) -> String {
    participant
        .name
        .clone()
        .unwrap_or_else(|| spelling(&participant.kind).to_lowercase())
}

fn target_view(target: &Target) -> TargetView {
    let location = target.source.location().to_string();
    let label = match (&target.path, &target.lines) {
        (Some(path), Some(lines)) => format!("{path}:{}-{}", lines.start, lines.end),
        (Some(path), None) => path.clone(),
        (None, Some(lines)) => format!("{location} lines {}-{}", lines.start, lines.end),
        (None, None) => location,
    };
    TargetView {
        label,
        quote: target.quote.as_ref().map(|quote| quote.exact.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn turn(
        seq: u64,
        at: &str,
        messages: &[(&str, &str)],
        calls: &[serde_json::Value],
    ) -> PromptTurn {
        let messages: Vec<_> = messages
            .iter()
            .enumerate()
            .map(|(index, (kind, text))| {
                json!({"id": format!("m{seq}-{index}"), "seq": index, "at": at, "kind": kind, "text": text})
            })
            .collect();
        serde_json::from_value(
            json!({"seq": seq, "at": at, "messages": messages, "toolCalls": calls}),
        )
        .unwrap()
    }

    fn call(kind: &str, diff_paths: &[&str], location_paths: &[&str]) -> serde_json::Value {
        json!({
            "id": "c", "seq": 1, "title": "t", "kind": kind, "status": "completed",
            "diffs": diff_paths.iter().map(|path| json!({"path": path, "patch": ""})).collect::<Vec<_>>(),
            "locations": location_paths.iter().map(|path| json!({"path": path})).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn a_summary_counts_turns_calls_and_edited_files_whatever_the_order() {
        let mut summary = SessionSummary::default();
        summary.add(&turn(
            2,
            "2026-11-03T12:00:00Z",
            &[("userMessage", "second")],
            &[
                call("edit", &[], &["/a.rs"]),
                call("move", &["/b.rs", "/c.rs"], &[]),
            ],
        ));
        summary.add(&turn(
            1,
            "2026-11-03T10:00:00Z",
            &[
                ("userMessage", "first"),
                ("agentMessage", "no"),
                ("notification", "no"),
            ],
            &[
                call("read", &[], &["/never.rs"]),
                call("edit", &["/a.rs"], &["/x.rs"]),
            ],
        ));
        assert_eq!((summary.turns, summary.tool_calls), (2, 4));
        assert_eq!(
            summary.tools,
            BTreeMap::from([("edit".into(), 2), ("move".into(), 1), ("read".into(), 1)])
        );
        // Diffs name the files when there are any, else the locations; reads touch nothing.
        assert_eq!(
            summary.edited_files().collect::<Vec<_>>(),
            ["/a.rs", "/b.rs", "/c.rs"]
        );
        let (first, last) = summary.span.unwrap();
        assert_eq!(
            (first.to_string(), last.to_string()),
            ("2026-11-03T10:00:00Z".into(), "2026-11-03T12:00:00Z".into())
        );
        // Only the operator's own messages, in session order and not in the order they arrived.
        let prompts: Vec<_> = summary.prompts().map(|p| p.text.as_str()).collect();
        assert_eq!(prompts, ["first", "second"]);
    }

    #[test]
    fn prompts_are_cut_and_capped_without_losing_their_order() {
        let mut summary = SessionSummary::default();
        let long_line = "é".repeat(SessionSummary::MAX_HEADLINE_OCTETS);
        let huge = format!(
            "\n\n  {long_line}\nsecond line {}",
            "y".repeat(SessionSummary::MAX_PROMPT_OCTETS)
        );
        // Newest first, more than the cap.
        for seq in (0..SessionSummary::MAX_PROMPTS as u64 + 5).rev() {
            let text = if seq == 0 { huge.as_str() } else { "short" };
            summary.add(&turn(
                seq,
                "2026-11-03T10:00:00Z",
                &[("userMessage", text)],
                &[],
            ));
        }
        assert_eq!(summary.prompts().count(), SessionSummary::MAX_PROMPTS);
        assert_eq!(summary.prompts_omitted(), 5);
        let first = summary.prompts().next().unwrap();
        // The headline is the first non-empty line, cut on a character boundary with an ellipsis.
        assert!(first.headline.ends_with('…'), "{}", first.headline);
        assert!(first.headline.len() <= SessionSummary::MAX_HEADLINE_OCTETS + '…'.len_utf8());
        assert!(first.cut);
        assert!(first.text.len() <= SessionSummary::MAX_PROMPT_OCTETS);
        assert!(
            summary
                .prompts()
                .skip(1)
                .all(|p| p.headline == "short" && !p.cut)
        );
    }

    #[test]
    fn edited_files_are_capped_and_the_rest_counted() {
        let mut summary = SessionSummary::default();
        let calls: Vec<_> = (0..SessionSummary::MAX_EDITED_FILES + 3)
            .map(|index| call("edit", &[&format!("/f{index:03}.rs")], &[]))
            .collect();
        summary.add(&turn(1, "2026-11-03T10:00:00Z", &[], &calls));
        assert_eq!(
            summary.edited_files().count(),
            SessionSummary::MAX_EDITED_FILES
        );
        assert_eq!(summary.edited_omitted(), (3, false));
    }

    #[test]
    fn tracked_edited_paths_are_bounded_and_deterministic() {
        let paths: Vec<String> = (0..SessionSummary::MAX_EDITED_TRACKED + 7)
            .map(|index| format!("/f{index:05}.rs"))
            .collect();
        let fold = |order: Vec<&String>| {
            let mut summary = SessionSummary::default();
            for path in order {
                summary.add(&turn(
                    1,
                    "2026-11-03T10:00:00Z",
                    &[],
                    &[call("edit", &[path], &[])],
                ));
            }
            summary
        };
        let forward = fold(paths.iter().collect());
        let backward = fold(paths.iter().rev().collect());
        // The smallest paths are the ones kept, whatever order the turns came in.
        assert_eq!(forward.edited, backward.edited);
        assert_eq!(forward.edited.len(), SessionSummary::MAX_EDITED_TRACKED);
        assert_eq!(
            forward.edited_omitted(),
            (
                SessionSummary::MAX_EDITED_TRACKED - SessionSummary::MAX_EDITED_FILES + 7,
                true
            )
        );
    }
}
