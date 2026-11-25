//! Where an [`IntxtView`] comes from: the one seam between the page and the world.
//!
//! The server awaits [`IntxtSource::load`] before it renders anything, so the page itself is a pure
//! function of the loaded value. The only implementation so far is [`FixtureSource`]; the relay
//! node's source replaces it without touching a component.

use crate::view::CommentDatum;
use crate::view::CommitDatum;
use crate::view::EntryDatum;
use crate::view::IntxtView;
use crate::view::PatchDatum;
use crate::view::SessionSummary;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use intxt_core::annotation::Comment;
use intxt_core::generic::PrLink;
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
use intxt_core::work::PromptTurn;
use intxt_core::work::SessionEntry;
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

/// A patch with a rename-free modification and a line that would be markup if it were not
/// escaped.
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

fn fixture() -> IntxtView {
    let drawer = FixtureSource::drawer();
    let relay = node(7);

    let title: Title = serde_json::from_value(json!({
        "title": "Make the intxt viewer stateless",
        "description": "Review the viewer slice.\n\nEverything the page shows is derived from the drawer; the page keeps no state of its own."
    }))
    .unwrap();

    let pack = format!(
        "dtxt:datum:{}/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949",
        FixtureSource::DRAWER
    );
    let commits: GitCommitEntry = serde_json::from_value(json!({
        "base": "org.example.intxt.work.gitCommit/43f9b548c0de1234",
        "tip": "org.example.intxt.work.gitCommit/aa0fcdcc77a1b2c3",
        "pack": pack
    }))
    .unwrap();
    let tip_commit: GitCommit = serde_json::from_value(json!({
        "parents": ["bb0fcdcc77a1b2c3"],
        "author": {"name": "dman", "email": "dman@example.org", "at": "2026-11-23T14:05:00Z"},
        "committer": {"name": "dman", "email": "dman@example.org", "at": "2026-11-23T14:05:00Z"},
        "message": "feat(intxt_web): show commits and their patch\n\nThe page renders the diff as plain HTML: <b>no markup</b> from the patch gets through.\n",
        "files": [{"path": "src/intxt_web/ui.rs", "status": "modified"}]
    }))
    .unwrap();
    let base_commit: GitCommit = serde_json::from_value(json!({
        "parents": ["43f9b548c0de1234"],
        "author": {"name": "Ada", "email": "ada@example.org", "at": "2026-11-22T09:30:00Z"},
        "committer": {"name": "Ada", "email": "ada@example.org", "at": "2026-11-22T09:30:00Z"},
        "message": "refactor(intxt_web): the view carries entries, not datums",
        "files": [{"path": "src/intxt_web/view.rs", "status": "modified"}]
    }))
    .unwrap();
    let ref_entry: GitRefEntry = serde_json::from_value(json!({
        "ref": datum_ref("work.gitRef", "0193c/refs/heads/feat/intxt-web").to_string(),
        "base": "aa0fcdcc77a1b2c3"
    }))
    .unwrap();
    let git_ref: GitRef = serde_json::from_value(json!({"target": "d1f00d5e9a0e1122"})).unwrap();
    let jj_entry: JjChangeEntry = serde_json::from_value(json!({"pack": pack})).unwrap();
    let jj_change: JjChange =
        serde_json::from_value(json!({"commits": ["77b2e0aa11223344", "4c1f0a9b55667788"]}))
            .unwrap();
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

    let sibling_drawer: DocId = "z11111111111111111111111111111112"
        .parse()
        .expect("fixture drawer id");
    let mut sibling = PrLink::new(intxt_core::generic::Intxt::reference(&sibling_drawer, []));
    sibling.title = Some("intxt: commits live with the intxt".into());
    sibling.refs = vec!["refs/heads/stack/intxt-16".into()];
    let links = PrLinks {
        intxts: vec![
            PrLink::new(intxt_core::generic::Intxt::reference(
                &drawer,
                [relay.clone()],
            )),
            sibling,
        ],
        ..Default::default()
    };
    let pull_entry: GithubPullEntry = serde_json::from_value(json!({
        "pull": datum_ref("work.githubPull", "o/r/12").to_string()
    }))
    .unwrap();
    let pull: GithubPull = serde_json::from_value(json!({
        "title": "Splits the reference parser",
        "body": format!("Splits the parser.\n\n{}", links.block(None)),
        "state": "merged", "head": "feat/refs", "base": "main",
        "headSha": "4c1f0a", "baseSha": "9a0e11",
        "updatedAt": "2026-11-17T10:00:00Z", "url": "https://github.com/o/r/pull/12"
    }))
    .unwrap();

    let question_id = datum_ref("comment", "0193");
    let question: Comment = serde_json::from_value(json!({
        "targets": [{
            "source": {"ref": datum_ref("work.worktreeOp", "41").to_string()},
            "path": "src/intxt_web/view.rs",
            "lines": {"start": 120, "end": 124},
            "quote": {"exact": "fn threads("}
        }],
        "body": {"mime": "text/markdown", "content": "Does a session entry need a transcript range?"},
        "motivation": "questioning",
        "creator": {"type": "Person", "name": "dman"}
    }))
    .unwrap();
    let reply_id = datum_ref("comment", "0194");
    let reply: Comment = serde_json::from_value(json!({
        "targets": [{"source": {"ref": question_id.to_string()}}],
        "body": {"mime": "text/markdown", "content": "Not for the viewer: it shows the session's title."},
        "motivation": "replying",
        "replyTo": question_id.to_string(),
        "creator": {"type": "Software", "name": "reviewer-agent"},
        "generator": {"type": "Software", "name": "claude-code", "version": "2.1.322"}
    }))
    .unwrap();
    let remark_id = datum_ref("comment", "0195");
    let remark: Comment = serde_json::from_value(json!({
        "targets": [{"source": {"ref": datum_ref("work.gitRef", "0193c/refs/heads/feat/intxt-web").to_string()}}],
        "body": {"mime": "text/markdown", "content": "Branch entries should pin a head commit."},
        "motivation": "commenting",
        "creator": {"type": "Person"}
    }))
    .unwrap();

    IntxtView::new(
        &drawer,
        &[relay],
        &title,
        &[
            EntryDatum::Commits {
                entry: &commits,
                commits: &[
                    CommitDatum {
                        sha: "aa0fcdcc77a1b2c3".into(),
                        commit: tip_commit,
                    },
                    CommitDatum {
                        sha: "bb0fcdcc77a1b2c3".into(),
                        commit: base_commit,
                    },
                ],
                patch: Some(&PatchDatum {
                    text: FIXTURE_PATCH.into(),
                    total_octets: FIXTURE_PATCH.len() as u64,
                }),
            },
            EntryDatum::GitRef {
                entry: &ref_entry,
                git_ref: &git_ref,
            },
            EntryDatum::JjChange {
                entry: &jj_entry,
                change: &jj_change,
                commits: &[],
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
        ],
        &[
            CommentDatum {
                id: &question_id,
                comment: &question,
            },
            CommentDatum {
                id: &reply_id,
                comment: &reply,
            },
            CommentDatum {
                id: &remark_id,
                comment: &remark,
            },
        ],
    )
}
