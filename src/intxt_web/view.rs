//! The view model: everything a page shows about one intxt, as plain data.
//!
//! [`IntxtView`] knows nothing about where it came from. It is built from `intxt_core`'s datum
//! types by [`IntxtView::new`], so a host that can read the datums (the relay node, the review
//! micro's node, a browser webnode) builds the same value, and the components in `ui` render it
//! without knowing which host that was.

use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use intxt_core::annotation::Comment;
use intxt_core::annotation::Participant;
use intxt_core::annotation::Target;
use intxt_core::generic::PrLinks;
use intxt_core::generic::Title;
use intxt_core::work::AgentSession;
use intxt_core::work::GitCommitEntry;
use intxt_core::work::GitRef;
use intxt_core::work::GitRefEntry;
use intxt_core::work::GithubPull;
use intxt_core::work::GithubPullEntry;
use intxt_core::work::JjChange;
use intxt_core::work::JjChangeEntry;
use intxt_core::work::SessionEntry;
use serde::Serialize;

/// One intxt as a reader sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct IntxtView {
    /// The drawer id, which names the intxt on every node it is shared with.
    pub drawer: String,
    /// The nodes the reader was told hold the intxt (the `via` hint), as node keys.
    pub via: Vec<String>,
    pub title: String,
    /// Markdown source. Shown as written; the viewer does not render markdown yet.
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
    /// For a pull request: the intxts its description names (including this one), and a note when
    /// the description's intxt block is there but unreadable.
    pub pr_links: PrLinksView,
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
    Commits(&'a GitCommitEntry),
    GitRef {
        entry: &'a GitRefEntry,
        git_ref: &'a GitRef,
    },
    JjChange {
        entry: &'a JjChangeEntry,
        change: &'a JjChange,
    },
    Session {
        entry: &'a SessionEntry,
        session: &'a AgentSession,
    },
    GithubPull {
        entry: &'a GithubPullEntry,
        pull: &'a GithubPull,
    },
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
        pr_links: PrLinksView::default(),
    };
    match entry {
        EntryDatum::Commits(entry) => {
            let label = match &entry.base {
                Some(base) => format!("{}..{}", short(base), short(&entry.tip)),
                None => short(&entry.tip).to_owned(),
            };
            row(EntryKind::Commits, label, Vec::new())
        }
        EntryDatum::GitRef { entry, git_ref } => {
            // The reference's slot is `<worktree>/<ref name>`; the name is what a reader knows.
            let slot = String::from_utf8_lossy(entry.reference.key().slot_bytes()).into_owned();
            let name = slot.split_once('/').map_or(slot.as_str(), |(_, name)| name);
            let mut refs = vec![EntryRef {
                name: "tip",
                value: short(&git_ref.target).to_owned(),
            }];
            if let Some(base) = git_ref.base.as_ref().or(entry.base.as_ref()) {
                refs.push(EntryRef {
                    name: "base",
                    value: short(base).to_owned(),
                });
            }
            row(EntryKind::GitRef, name.to_owned(), refs)
        }
        EntryDatum::JjChange { change, .. } => row(
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
        ),
        EntryDatum::Session { session, .. } => row(
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
        ),
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
            view.pr_links = pr_links(&pull.body);
            view
        }
    }
}

fn pr_links(description: &str) -> PrLinksView {
    match PrLinks::read(description) {
        Ok(links) => PrLinksView {
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
        Err(error) => PrLinksView {
            intxts: Vec::new(),
            malformed: Some(error.to_string()),
        },
    }
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
