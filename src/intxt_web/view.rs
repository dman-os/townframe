//! The view model: everything a page shows about one intxt, as plain data.
//!
//! [`IntxtView`] knows nothing about where it came from. It is built from `intxt_core`'s datum
//! types by [`IntxtView::new`], so a host that can read the datums (the relay node, the review
//! micro's node, a browser webnode) builds the same value, and the components in `ui` render it
//! without knowing which host that was.

use crate::diff::Diff;
use crate::guide::GuideSection;
use crate::guide::StructureDatum;
use crate::session::SessionSummary;
use crate::session::cut;
use big_cabinet_core::DatumKey;
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
use intxt_core::work::SessionEntry;
use jiff::Timestamp;
use serde::Serialize;

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
    /// When the host read the intxt: "active now" is measured against it, so the page is a pure
    /// function of the view.
    pub as_of: Timestamp,
    pub entries: Vec<EntryView>,
    pub threads: Vec<ThreadView>,
    /// The intxt's `structure` datums as a guide (ADR 041 §4), in the author's order; empty when
    /// the intxt has none.
    pub guide: Vec<GuideSection>,
}

/// The pages of one intxt, each its own route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Conversation,
    Commits,
    Files,
    Sessions,
    Guide,
}

impl Tab {
    pub const ALL: [Tab; 5] = [
        Self::Conversation,
        Self::Commits,
        Self::Files,
        Self::Sessions,
        Self::Guide,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Conversation => "Conversation",
            Self::Commits => "Commits",
            Self::Files => "Files changed",
            Self::Sessions => "Sessions",
            Self::Guide => "Guide",
        }
    }

    /// The route segment after `/i/<drawer>`; the conversation is the intxt's own path.
    pub fn segment(self) -> Option<&'static str> {
        match self {
            Self::Conversation => None,
            Self::Commits => Some("commits"),
            Self::Files => Some("files"),
            Self::Sessions => Some("sessions"),
            Self::Guide => Some("guide"),
        }
    }

    pub fn from_segment(segment: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|tab| tab.segment() == Some(segment))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Commits,
    GitRef,
    JjChange,
    Session,
    GithubPull,
    /// A datum kind the page does not know: shown by its kind name, never dropped (FDR 006).
    Generic,
    /// An entry the host could not read: the page states it where it would have been (D95).
    Gap,
}

impl EntryKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Commits => "commits",
            Self::GitRef => "branch",
            Self::JjChange => "jj change",
            Self::Session => "session",
            Self::GithubPull => "pull request",
            Self::Generic => "datum",
            Self::Gap => "unreadable",
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
    /// The time the entry's own datums carry, when they carry one: a session's last turn, a pull
    /// request's `updatedAt`, a generic datum's `at` or UUIDv7 slot. A commit range, a branch and
    /// a jj change carry none (their commits do), and an entry has no "added at".
    pub at: Option<Timestamp>,
    /// Who the datums say did it: a session's harness, a generic datum's `creator`.
    pub actor: Option<String>,
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

/// One commit of an entry, from the `gitCommit` datum the intxt carries for it.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitView {
    pub sha: String,
    /// The author's name, as the commit says. A claim of the content, not of who wrote the datum.
    pub author: String,
    /// The author date.
    pub at: Timestamp,
    /// The whole message, subject first.
    pub message: String,
    /// How many files the commit changes.
    pub files: usize,
}

impl CommitView {
    pub fn subject(&self) -> &str {
        self.message.lines().next().unwrap_or("")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryRef {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PrLinksView {
    pub intxts: Vec<LinkedIntxt>,
    pub malformed: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinkedIntxt {
    /// The linked intxt's drawer id.
    pub drawer: String,
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
    /// When it was written, from its UUIDv7 slot: a comment has no other time. None for a slot
    /// that is not a UUIDv7.
    pub at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetView {
    /// `path:12-14` for code, else the datum location the comment is pinned to.
    pub label: String,
    pub quote: Option<String>,
    /// The file the target points into, when it points into one.
    pub path: Option<String>,
    /// First and last line, when the target names lines.
    pub lines: Option<(u32, u32)>,
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
    /// An entry of a kind the page does not know, as the datum is.
    Generic {
        key: &'a DatumKey,
        value: &'a serde_json::Value,
    },
    /// An entry the host named and could not read.
    Gap { key: &'a str, reason: &'a str },
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
        as_of: Timestamp,
        title: &Title,
        entries: &[EntryDatum<'_>],
        comments: &[CommentDatum<'_>],
        structures: &[StructureDatum<'_>],
    ) -> Self {
        Self {
            drawer: drawer.to_string(),
            via: via.iter().map(ToString::to_string).collect(),
            title: title.title.clone(),
            description: title.description.clone(),
            as_of,
            entries: entries.iter().map(entry_view).collect(),
            threads: threads(comments),
            guide: crate::guide::build(structures),
        }
    }

    /// The viewer path of this intxt, the one `/i/<id>?via=<keys>` route parses.
    pub fn path(&self) -> String {
        intxt_path(&self.drawer, &self.via, None)
    }

    /// The viewer path of one of this intxt's pages, keeping the `via` hint.
    pub fn tab_path(&self, tab: Tab) -> String {
        intxt_path(&self.drawer, &self.via, tab.segment())
    }

    /// How many things `tab` lists: the number beside its name.
    pub fn count(&self, tab: Tab) -> usize {
        match tab {
            Tab::Conversation => self.feed().len(),
            Tab::Commits => self.revisions().count(),
            Tab::Files => self.files().iter().map(|entry| entry.files.len()).sum(),
            Tab::Sessions => self.sessions().count(),
            Tab::Guide => self.guide.len(),
        }
    }

    /// The entries that hold commits, each one revision of the work.
    pub fn revisions(&self) -> impl Iterator<Item = (usize, &EntryView)> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| matches!(entry.kind, EntryKind::Commits | EntryKind::JjChange))
    }

    pub fn sessions(&self) -> impl Iterator<Item = &EntryView> {
        self.entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::Session)
    }
}

fn intxt_path(
    drawer: impl std::fmt::Display,
    via: &[impl std::fmt::Display],
    segment: Option<&str>,
) -> String {
    let mut path = format!("/i/{drawer}");
    if let Some(segment) = segment {
        path.push('/');
        path.push_str(segment);
    }
    if !via.is_empty() {
        let keys: Vec<String> = via.iter().map(ToString::to_string).collect();
        path.push_str("?via=");
        path.push_str(&keys.join("|"));
    }
    path
}

/// The wire spelling of an open vocabulary's member, which is what a reader is shown.
pub(crate) fn spelling(member: &impl Serialize) -> String {
    match serde_json::to_value(member).expect("a vocabulary member serializes") {
        serde_json::Value::String(text) => text,
        other => panic!("an open string vocabulary serialized as {other}"),
    }
}

pub(crate) fn short(sha: &str) -> &str {
    sha.get(..8).unwrap_or(sha)
}

fn entry_view(entry: &EntryDatum<'_>) -> EntryView {
    let row = |kind, label: String, refs: Vec<EntryRef>| EntryView {
        kind,
        label,
        refs,
        at: None,
        actor: None,
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
            let mut refs = vec![entry_ref("tip", short(sha_of(&git_ref.target)))];
            if let Some(base) = git_ref.base.as_ref().or(entry.base.as_ref()) {
                refs.push(entry_ref("base", short(sha_of(base))));
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
                    .map(|commit| entry_ref("commit", short(commit)))
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
                    entry_ref("harness", session.harness.clone()),
                    entry_ref("session", session.session_id.clone()),
                ],
            );
            view.actor = Some(session.harness.clone());
            view.at = summary.span.map(|(_, last)| last);
            view.session = Some((*summary).clone());
            view
        }
        EntryDatum::GithubPull { pull, .. } => {
            let mut view = row(
                EntryKind::GithubPull,
                pull.title.clone(),
                vec![
                    entry_ref("state", spelling(&pull.state)),
                    entry_ref("head", pull.head.clone()),
                    entry_ref("base", pull.base.clone()),
                ],
            );
            // The importer's snapshot time: the PR's opening and merging are not recorded.
            view.at = Some(pull.updated_at);
            view.href = Some(pull.url.clone());
            let (description, links) = pr_description(&pull.body);
            view.description = Some(description).filter(|text| !text.trim().is_empty());
            view.pr_links = links;
            view
        }
        EntryDatum::Generic { key, value } => generic_view(key, value),
        EntryDatum::Gap { key, reason } => row(
            EntryKind::Gap,
            (*key).to_owned(),
            vec![entry_ref("reason", *reason)],
        ),
    }
}

fn entry_ref(name: &str, value: impl Into<String>) -> EntryRef {
    EntryRef {
        name: name.to_owned(),
        value: value.into(),
    }
}

/// The most scalar fields of an unknown datum the page lists.
const GENERIC_FIELDS: usize = 6;
/// The most octets of one such field's value the page keeps.
const GENERIC_VALUE_OCTETS: usize = 120;

/// What the page can say of a datum it does not know: the kind, the top-level fields that are
/// plain values, and whoever and whenever the datum names by the common spellings.
fn generic_view(key: &DatumKey, value: &serde_json::Value) -> EntryView {
    let fields = value.as_object().into_iter().flatten();
    let refs = fields
        .filter_map(|(name, field)| {
            let text = match field {
                serde_json::Value::String(text) => text.clone(),
                serde_json::Value::Number(number) => number.to_string(),
                serde_json::Value::Bool(flag) => flag.to_string(),
                _ => return None,
            };
            let (kept, was_cut) = cut(&text, GENERIC_VALUE_OCTETS);
            Some(entry_ref(
                name,
                if was_cut {
                    format!("{kept}…")
                } else {
                    kept.to_owned()
                },
            ))
        })
        .take(GENERIC_FIELDS)
        .collect();
    let name_of = |field: &str| match value.get(field)? {
        serde_json::Value::String(text) => Some(text.clone()),
        object => object.get("name")?.as_str().map(str::to_owned),
    };
    let named_time = ["at", "createdAt", "updatedAt", "time"]
        .iter()
        .find_map(|field| value.get(field)?.as_str()?.parse::<Timestamp>().ok());
    EntryView {
        kind: EntryKind::Generic,
        label: key.tag().to_string(),
        refs,
        at: slot_time(key).or(named_time),
        actor: ["creator", "author", "writer"]
            .iter()
            .find_map(|field| name_of(field)),
        href: None,
        description: None,
        pr_links: PrLinksView::default(),
        commits: Vec::new(),
        patch: None,
        session: None,
    }
}

/// The time a UUIDv7 slot carries in its first 48 bits, none for a slot that is not a UUIDv7.
pub(crate) fn slot_time(key: &DatumKey) -> Option<Timestamp> {
    let slot = String::from_utf8_lossy(key.slot_bytes());
    let hex: String = slot.chars().filter(|ch| *ch != '-').collect();
    let is_v7 = hex.len() == 32
        && hex.as_bytes()[12] == b'7'
        && matches!(hex.as_bytes()[16], b'8' | b'9' | b'a' | b'b' | b'A' | b'B');
    if !is_v7 {
        return None;
    }
    let millis = i64::from_str_radix(&hex[..12], 16).ok()?;
    Timestamp::from_millisecond(millis).ok()
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
                            drawer: drawer.to_string(),
                            path: intxt_path(drawer, link.reference.via(), None),
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
        author: author.name.clone(),
        at: author.at,
        message: commit.commit.message.trim_end().to_owned(),
        files: commit.commit.files.len(),
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
        let view = comment_view(comment);
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

fn comment_view(datum: &CommentDatum<'_>) -> CommentView {
    let (comment, at) = (datum.comment, slot_time(datum.id.key()));
    CommentView {
        author: participant(&comment.creator),
        generator: comment.generator.as_ref().map(participant),
        motivation: spelling(&comment.motivation),
        body: comment.body.content.clone(),
        // A reply's target that says nothing but the parent comment is the thread, not an anchor.
        targets: comment
            .targets
            .iter()
            .filter(|target| {
                comment.reply_to.is_none()
                    || target.path.is_some()
                    || target.lines.is_some()
                    || target.quote.is_some()
            })
            .map(target_view)
            .collect(),
        at,
    }
}

fn participant(participant: &Participant) -> String {
    participant
        .name
        .clone()
        .unwrap_or_else(|| spelling(&participant.kind).to_lowercase())
}

/// What a reader calls the kind of datum a target points at, from the datum's tag.
fn target_noun(tag: &str) -> &'static str {
    match tag.rsplit('.').next().unwrap_or(tag) {
        "comment" => "a comment",
        "promptTurn" => "a session message",
        "agentSession" => "a session",
        "gitCommit" => "a commit",
        "gitRef" => "a branch",
        "githubPull" => "a pull request",
        "worktreeOp" => "a working-copy change",
        "structure" => "a guide section",
        _ => "a datum",
    }
}

pub(crate) fn target_view(target: &Target) -> TargetView {
    let noun = target_noun(target.source.location().reference().key().tag().as_str());
    let label = match (&target.path, &target.lines) {
        (Some(path), Some(lines)) => format!("{path}:{}-{}", lines.start, lines.end),
        (Some(path), None) => path.clone(),
        (None, Some(lines)) => format!("{noun}, lines {}-{}", lines.start, lines.end),
        (None, None) => noun.to_owned(),
    };
    TargetView {
        label,
        quote: target.quote.as_ref().map(|quote| quote.exact.clone()),
        path: target.path.clone(),
        lines: target.lines.as_ref().map(|lines| (lines.start, lines.end)),
    }
}
