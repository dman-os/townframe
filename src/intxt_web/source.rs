//! Where an [`IntxtView`] comes from: the one seam between the page and the world.
//!
//! The server awaits [`IntxtSource::load`] before it renders anything, so the page itself is a pure
//! function of the loaded value. The only implementation so far is [`FixtureSource`]; the relay
//! node's source replaces it without touching a component.

use crate::guide::StructureDatum;
use crate::session::SessionSummary;
use crate::view::CommentDatum;
use crate::view::CommitDatum;
use crate::view::EntryDatum;
use crate::view::IntxtView;
use crate::view::PatchDatum;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use intxt_core::annotation::Comment;
use intxt_core::annotation::Structure;
use intxt_core::generic::PrLink;
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
use intxt_core::work::PromptTurn;
use intxt_core::work::SessionEntry;
use jiff::Timestamp;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The source answered and does not hold the intxt.
    #[error("no intxt {0} here")]
    NotFound(DocId),
    /// The source could not ask: none of the `via` nodes (nor any default) answered.
    #[error("could not reach a node holding {drawer}: {reason}")]
    Unreachable { drawer: DocId, reason: String },
    /// The source answered and refused the viewer's capability for the intxt.
    #[error("this viewer's capability does not reach intxt {0}")]
    Forbidden(DocId),
    /// The source answered with something that is not an intxt as the viewer reads one: a datum
    /// that is missing, unreadable or of another shape. What a peer wrote is not trusted to be
    /// well formed, so this is a page and not a crash.
    #[error("what the node holds for {drawer} is not an intxt this viewer can read: {reason}")]
    Unreadable { drawer: DocId, reason: String },
}

impl LoadError {
    /// The HTTP status the error page is served with.
    pub fn status(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::Forbidden(_) => 403,
            Self::Unreachable { .. } | Self::Unreadable { .. } => 502,
        }
    }
}

/// Where the pages' data comes from, as the index page says it.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    /// The built-in fixture, because no node is configured.
    Fixture { example_path: String },
    /// The node at this base URL.
    Node { url: String },
}

/// Loads the intxt in `drawer`, reading through the nodes `via` names (a locator hint; the drawer
/// id is the identity).
// Public so the node-backed source can live in another crate. Its futures need no `Send`: the
// component runs single-threaded on `wit_bindgen::spawn_local`.
#[expect(
    async_fn_in_trait,
    reason = "single-threaded guest, futures are never sent"
)]
pub trait IntxtSource {
    async fn load(&self, drawer: &DocId, via: &[MultibaseKey]) -> Result<IntxtView, LoadError>;

    /// Where `load` reads from, for the index page.
    fn origin(&self) -> Origin;
}

/// One intxt, built from real `intxt_core` datums, served under [`FixtureSource::DRAWER`] whatever
/// `via` says.
pub struct FixtureSource;

impl FixtureSource {
    pub const DRAWER: &'static str = "z11111111111111111111111111111111";

    /// The fixture intxt, which is what `load` answers for [`Self::DRAWER`].
    pub fn view() -> IntxtView {
        fixture()
    }

    /// When the fixture was "read": ten minutes after its last commit is the edge of "active
    /// now", so this is inside the window of the newest commit and comment only.
    pub const AS_OF: &'static str = "2026-11-23T14:12:00Z";

    pub fn drawer() -> DocId {
        Self::DRAWER
            .parse()
            .expect("the fixture drawer id is valid")
    }
}

impl IntxtSource for FixtureSource {
    async fn load(&self, drawer: &DocId, _via: &[MultibaseKey]) -> Result<IntxtView, LoadError> {
        if *drawer != Self::drawer() {
            return Err(LoadError::NotFound(drawer.clone()));
        }
        Ok(Self::view())
    }

    fn origin(&self) -> Origin {
        Origin::Fixture {
            example_path: format!("/i/{}", Self::DRAWER),
        }
    }
}

/// A patch of two files: a rename-free modification with a line that would be markup if it were
/// not escaped, and one whose hunk holds the lines a comment of the fixture is anchored at.
const FIXTURE_PATCH: &str = "\
diff --git a/src/intxt_web/ui.rs b/src/intxt_web/ui.rs
index 3f4c1a2..9b7e0d4 100644
--- a/src/intxt_web/ui.rs
+++ b/src/intxt_web/ui.rs
@@ -10,7 +10,9 @@ use crate::view::EntryView;
 use crate::view::IntxtView;
-use crate::view::OldThing;
+use crate::view::CommitView;
+use crate::diff::Diff;
 const KEEP: &str = \"unchanged\";
+const MARKUP: &str = \"<script>alert('not run')</script> & more\";
 fn page() {}
\\ No newline at end of file
diff --git a/src/intxt_web/view.rs b/src/intxt_web/view.rs
index 5d2a7e1..0c9f3b8 100644
--- a/src/intxt_web/view.rs
+++ b/src/intxt_web/view.rs
@@ -118,4 +118,7 @@ impl IntxtView {
     fn root_of() {}
+    // A reply names its parent.
+    // A reply to a reply joins the root.
+    fn threads(comments: &[CommentDatum<'_>]) -> Vec<ThreadView> {
     let mut root_of = Vec::new();
     for comment in comments {
         root_of.push(0);
";

fn node(byte: u8) -> MultibaseKey {
    MultibaseKey::try_from([byte; 32].as_slice()).expect("32 bytes")
}

fn datum_ref(tag: &str, slot: &str) -> DatumReference {
    format!(
        "dtxt:datum:{}/org.example.intxt.{tag}/{slot}",
        FixtureSource::DRAWER
    )
    .parse()
    .expect("fixture reference")
}

/// A UUIDv7 slot carrying `at`, which is where a comment's or structure's time lives. `tail`
/// tells two written in the same millisecond apart.
fn uuid7_slot(at: &str, tail: u16) -> String {
    let millis = at
        .parse::<Timestamp>()
        .expect("fixture time")
        .as_millisecond();
    format!(
        "{:08x}-{:04x}-7000-8000-{tail:012x}",
        millis >> 16,
        millis & 0xffff
    )
}

fn fixture() -> IntxtView {
    let drawer = FixtureSource::drawer();
    let relay = node(7);
    let title: Title = serde_json::from_value(json!({
        "title": "Make the intxt viewer stateless",
        "description": "Review the viewer slice.\n\nEverything the page shows is derived from the drawer; the page keeps no state of its own."
    }))
    .unwrap();
    let work = fixture_work();
    let (session_entry, session, summary) = fixture_session();
    let (pull_entry, pull) = fixture_pull(&drawer, &relay);
    let comments = fixture_comments();
    let structures = fixture_structures();
    let generic = fixture_generic();

    IntxtView::new(
        &drawer,
        &[relay],
        FixtureSource::AS_OF.parse().expect("fixture time"),
        &title,
        &[
            EntryDatum::Commits {
                entry: &work.commits,
                commits: &work.commit_datums,
                patch: Some(&PatchDatum {
                    text: FIXTURE_PATCH.into(),
                    total_octets: FIXTURE_PATCH.len() as u64,
                }),
            },
            EntryDatum::GitRef {
                entry: &work.ref_entry,
                git_ref: &work.git_ref,
            },
            EntryDatum::JjChange {
                entry: &work.jj_entry,
                change: &work.jj_change,
                commits: &work.jj_commits,
                patch: None,
            },
            EntryDatum::Session {
                entry: &session_entry,
                session: &session,
                summary: &summary,
            },
            EntryDatum::GithubPull {
                entry: &pull_entry,
                pull: &pull,
            },
            EntryDatum::Generic {
                key: &generic.0,
                value: &generic.1,
            },
            EntryDatum::Gap {
                key: "org.example.intxt.work.githubPullEntry/o/r/99",
                reason: "not readable in the index document",
            },
        ],
        &comments
            .iter()
            .map(|(id, comment)| CommentDatum { id, comment })
            .collect::<Vec<_>>(),
        &structures
            .iter()
            .map(|(id, structure)| StructureDatum { id, structure })
            .collect::<Vec<_>>(),
    )
}

/// The datums of the entries that hold the work: a commit range, a branch and a jj change.
struct FixtureWork {
    commits: GitCommitEntry,
    commit_datums: Vec<CommitDatum>,
    ref_entry: GitRefEntry,
    git_ref: GitRef,
    jj_entry: JjChangeEntry,
    jj_change: JjChange,
    /// The change's working-copy states, newest first: on the Commits tab, out of the feed.
    jj_commits: Vec<CommitDatum>,
}

/// Four commits: Ada's from the day before, then three by dman, each within 30 minutes of the
/// last (a burst), the newest at 14:05; and two working-copy states of a jj change in the middle
/// of that burst, which the feed leaves out.
fn fixture_work() -> FixtureWork {
    let pack = format!(
        "dtxt:datum:{}/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949",
        FixtureSource::DRAWER
    );
    let commit = |sha: &str, parent: &str, name: &str, at: &str, message: &str, path: &str| {
        let ident = json!({"name": name, "email": format!("{name}@example.org"), "at": at});
        CommitDatum {
            sha: sha.into(),
            commit: serde_json::from_value(json!({
                "parents": [parent], "author": ident, "committer": ident, "message": message,
                "files": [{"path": path, "status": "modified"}]
            }))
            .unwrap(),
        }
    };
    let commit_datums = vec![
        commit(
            "aa0fcdcc77a1b2c3",
            "bb0fcdcc77a1b2c3",
            "dman",
            "2026-11-23T14:05:00Z",
            "feat(intxt_web): show commits and their patch\n\nThe page renders the diff as plain HTML: <b>no markup</b> from the patch gets through.\n",
            "src/intxt_web/ui.rs",
        ),
        commit(
            "bb0fcdcc77a1b2c3",
            "cc0fcdcc77a1b2c3",
            "dman",
            "2026-11-23T13:48:00Z",
            "test(intxt_web): the fixture holds a patch",
            "src/intxt_web/source.rs",
        ),
        commit(
            "cc0fcdcc77a1b2c3",
            "dd0fcdcc77a1b2c3",
            "dman",
            "2026-11-23T13:30:00Z",
            "feat(intxt_web): a diff per file",
            "src/intxt_web/diff.rs",
        ),
        commit(
            "dd0fcdcc77a1b2c3",
            "43f9b548c0de1234",
            "Ada",
            "2026-11-22T09:30:00Z",
            "refactor(intxt_web): the view carries entries, not datums",
            "src/intxt_web/view.rs",
        ),
    ];
    let jj_commits = vec![
        commit(
            "77b2e0aa11223344",
            "4c1f0a9b55667788",
            "dman",
            "2026-11-23T13:58:00Z",
            "wip: a diff per file",
            "src/intxt_web/diff.rs",
        ),
        commit(
            "4c1f0a9b55667788",
            "cc0fcdcc77a1b2c3",
            "dman",
            "2026-11-23T13:55:00Z",
            "wip: a diff per file",
            "src/intxt_web/diff.rs",
        ),
    ];
    FixtureWork {
        jj_commits,
        commits: serde_json::from_value(json!({
            "base": "org.example.intxt.work.gitCommit/43f9b548c0de1234",
            "tip": "org.example.intxt.work.gitCommit/aa0fcdcc77a1b2c3",
            "pack": pack
        }))
        .unwrap(),
        commit_datums,
        ref_entry: serde_json::from_value(json!({
            "ref": datum_ref("work.gitRef", "0193c/refs/heads/feat/intxt-web").to_string(),
            "base": "aa0fcdcc77a1b2c3"
        }))
        .unwrap(),
        git_ref: serde_json::from_value(json!({"target": "d1f00d5e9a0e1122"})).unwrap(),
        jj_entry: serde_json::from_value(json!({"pack": pack})).unwrap(),
        jj_change: serde_json::from_value(
            json!({"commits": ["77b2e0aa11223344", "4c1f0a9b55667788"]}),
        )
        .unwrap(),
    }
}

/// A merged pull request whose block names this intxt and a sibling.
fn fixture_pull(drawer: &DocId, relay: &MultibaseKey) -> (GithubPullEntry, GithubPull) {
    let sibling_drawer: DocId = "z11111111111111111111111111111112"
        .parse()
        .expect("fixture drawer id");
    let mut sibling = PrLink::new(intxt_core::generic::Intxt::reference(&sibling_drawer, []));
    sibling.title = Some("intxt: commits live with the intxt".into());
    sibling.refs = vec!["refs/heads/stack/intxt-16".into()];
    let links = PrLinks {
        intxts: vec![
            PrLink::new(intxt_core::generic::Intxt::reference(
                drawer,
                [relay.clone()],
            )),
            sibling,
        ],
        ..Default::default()
    };
    let entry = serde_json::from_value(json!({
        "pull": datum_ref("work.githubPull", "o/r/12").to_string()
    }))
    .unwrap();
    let pull = serde_json::from_value(json!({
        "title": "Splits the reference parser",
        "body": format!("Splits the parser.\n\n{}", links.block(None)),
        "state": "merged", "head": "feat/refs", "base": "main",
        "headSha": "4c1f0a", "baseSha": "9a0e11",
        "updatedAt": "2026-11-17T10:00:00Z", "url": "https://github.com/o/r/pull/12"
    }))
    .unwrap();
    (entry, pull)
}

/// An entry of a kind the viewer does not know, as another tool might add one.
fn fixture_generic() -> (DatumKey, serde_json::Value) {
    (
        DatumKey::from_text("org.example.intxt.checkRun", "ci-1").expect("fixture key"),
        json!({
            "name": "ci", "status": "passed", "commit": "aa0fcdcc",
            "creator": {"type": "Software", "name": "ci-bot"},
            "at": "2026-11-23T13:10:00Z", "log": {"lines": 40}
        }),
    )
}

/// A session entry, its session and the summary of three turns and one that could not be read.
fn fixture_session() -> (SessionEntry, AgentSession, SessionSummary) {
    let session_entry: SessionEntry = serde_json::from_value(json!({
        "session": datum_ref("work.agentSession", "main").to_string()
    }))
    .unwrap();
    let session: AgentSession = serde_json::from_value(json!({
        "harness": "claude-code", "sessionId": "013k65AAMJi9bejF5GWSm1De",
        "title": "intxt web viewer", "cwd": "/work/townframe"
    }))
    .unwrap();

    // Three turns, given out of `seq` order the way a document lists them by key.
    let turn = |value| -> PromptTurn { serde_json::from_value(value).unwrap() };
    let turns = [
        turn(json!({
            "seq": 2, "at": "2026-11-23T11:30:00Z", "stopReason": "end_turn",
            "messages": [
                {"id": "m3", "seq": 1, "at": "2026-11-23T11:30:00Z", "kind": "userMessage",
                 "text": "Now the phone layout.\n\nNo horizontal scroll, <b>please</b>."},
                {"id": "m4", "seq": 2, "at": "2026-11-23T11:31:00Z", "kind": "agentMessage",
                 "text": "Done."}
            ],
            "toolCalls": [
                {"id": "c4", "seq": 3, "title": "Edit intxt_web.css", "kind": "edit",
                 "status": "completed",
                 "diffs": [{"path": "src/intxt_web/intxt_web.css", "patch": "@@"}]}
            ]
        })),
        turn(json!({
            "seq": 1, "at": "2026-11-23T10:00:00Z", "stopReason": "end_turn",
            "messages": [
                {"id": "m1", "seq": 1, "at": "2026-11-23T10:00:00Z", "kind": "userMessage",
                 "text": "Render the session's turns in the viewer."},
                {"id": "m2", "seq": 4, "at": "2026-11-23T10:20:00Z", "kind": "agentMessage",
                 "text": "Reading the datums first."}
            ],
            "toolCalls": [
                {"id": "c1", "seq": 2, "title": "Read view.rs", "kind": "read", "status": "completed"},
                {"id": "c2", "seq": 3, "title": "Edit ui.rs", "kind": "edit", "status": "completed",
                 "diffs": [{"path": "src/intxt_web/ui.rs", "patch": "@@"}]},
                {"id": "c3", "seq": 5, "title": "cargo test", "kind": "execute", "status": "failed"}
            ]
        })),
        turn(json!({
            "seq": 3, "at": "2026-11-23T12:15:00Z",
            "messages": [
                {"id": "m5", "seq": 1, "at": "2026-11-23T12:15:00Z", "kind": "userMessage",
                 "text": "Ship it."}
            ],
            "toolCalls": [
                {"id": "c5", "seq": 2, "title": "Edit ui.rs", "kind": "edit", "status": "completed",
                 "locations": [{"path": "src/intxt_web/ui.rs"}]}
            ]
        })),
    ];
    let mut summary = SessionSummary::default();
    for turn in &turns {
        summary.add(turn);
    }
    // One more turn the node listed and could not read.
    summary.unreadable = 1;
    (session_entry, session, summary)
}

/// A question with a reply on a line of the patch, a remark on a branch, a remark on a line the
/// patch does not show, and one on a line the patch does.
fn fixture_comments() -> Vec<(DatumReference, Comment)> {
    let comment = |at: &str, tail: u16, value: serde_json::Value| {
        let id = datum_ref("comment", &uuid7_slot(at, tail));
        (id, serde_json::from_value::<Comment>(value).unwrap())
    };
    let question = comment(
        "2026-11-23T14:07:00Z",
        1,
        json!({
            "targets": [{
                "source": {"ref": datum_ref("work.worktreeOp", "41").to_string()},
                "path": "src/intxt_web/view.rs",
                "lines": {"start": 120, "end": 124},
                "quote": {"exact": "fn threads("}
            }],
            "body": {"mime": "text/markdown", "content": "Does a session entry need a transcript range?"},
            "motivation": "questioning",
            "creator": {"type": "Person", "name": "dman"}
        }),
    );
    let reply = comment(
        "2026-11-23T14:08:00Z",
        2,
        json!({
            "targets": [{"source": {"ref": question.0.to_string()}}],
            "body": {"mime": "text/markdown", "content": "Not for the viewer: it shows the session's title."},
            "motivation": "replying",
            "replyTo": question.0.to_string(),
            "creator": {"type": "Software", "name": "reviewer-agent"},
            "generator": {"type": "Software", "name": "claude-code", "version": "2.1.322"}
        }),
    );
    let on_the_patch = comment(
        "2026-11-23T13:20:00Z",
        3,
        json!({
            "targets": [{
                "source": {"ref": datum_ref("work.worktreeOp", "41").to_string()},
                "path": "src/intxt_web/ui.rs", "lines": {"start": 14, "end": 14}
            }],
            "body": {"mime": "text/markdown", "content": "Is this constant still needed?"},
            "motivation": "questioning",
            "creator": {"type": "Person", "name": "Ada"}
        }),
    );
    let off_the_patch = comment(
        "2026-11-23T12:30:00Z",
        4,
        json!({
            "targets": [{
                "source": {"ref": datum_ref("work.worktreeOp", "41").to_string()},
                "path": "src/intxt_web/ui.rs", "lines": {"start": 200, "end": 210}
            }],
            "body": {"mime": "text/markdown", "content": "The footer further down wants the same treatment."},
            "motivation": "commenting",
            "creator": {"type": "Person", "name": "Ada"}
        }),
    );
    let remark = comment(
        "2026-11-22T16:00:00Z",
        5,
        json!({
            "targets": [{"source": {"ref": datum_ref("work.gitRef", "0193c/refs/heads/feat/intxt-web").to_string()}}],
            "body": {"mime": "text/markdown", "content": "Branch entries should pin a head commit."},
            "motivation": "commenting",
            "creator": {"type": "Person"}
        }),
    );
    // Creation order, which is key order for UUIDv7 slots.
    let mut all = vec![question, reply, on_the_patch, off_the_patch, remark];
    all.sort_by(|(left, _), (right, _)| left.key().cmp(right.key()));
    all
}

/// A two-section guide, the first with a child, pointing at hunks of the patch.
fn fixture_structures() -> Vec<(DatumReference, Structure)> {
    let first = datum_ref("structure", &uuid7_slot("2026-11-23T14:00:00Z", 1));
    let section = |slot: &DatumReference,
                   title: &str,
                   order: &str,
                   parent: Option<&DatumReference>,
                   target: serde_json::Value| {
        let structure: Structure = serde_json::from_value(json!({
            "kind": "section", "title": title, "order": order,
            "body": {"mime": "text/markdown", "content": format!("Read **{title}** first.")},
            "targets": [target],
            "parent": parent.map(ToString::to_string),
        }))
        .unwrap();
        (slot.clone(), structure)
    };
    let target = |path: &str, start: u32, end: u32| {
        json!({
            "source": {"ref": datum_ref("work.worktreeOp", "41").to_string()},
            "path": path, "lines": {"start": start, "end": end}
        })
    };
    let child = datum_ref("structure", &uuid7_slot("2026-11-23T14:00:00Z", 2));
    let second = datum_ref("structure", &uuid7_slot("2026-11-23T14:00:00Z", 3));
    vec![
        section(
            &second,
            "The thread grouping",
            "2",
            None,
            target("src/intxt_web/view.rs", 118, 124),
        ),
        section(
            &first,
            "The imports",
            "1",
            None,
            target("src/intxt_web/ui.rs", 10, 14),
        ),
        section(
            &child,
            "The new constant",
            "1",
            Some(&first),
            target("src/intxt_web/ui.rs", 14, 14),
        ),
    ]
}
