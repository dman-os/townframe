//! Where an [`IntxtView`] comes from: the one seam between the page and the world.
//!
//! The server awaits [`IntxtSource::load`] before it renders anything, so the page itself is a pure
//! function of the loaded value. The only implementation so far is [`FixtureSource`]; the relay
//! node's source replaces it without touching a component.

use crate::view::CommentDatum;
use crate::view::EntryDatum;
use crate::view::IntxtView;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use intxt_core::annotation::Comment;
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
}

impl LoadError {
    /// The HTTP status the error page is served with.
    pub fn status(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::Unreachable { .. } => 502,
        }
    }
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
}

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
        "base": "43f9b548c0de1234", "tip": "aa0fcdcc77a1b2c3", "pack": pack
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
        "body": format!("Splits the parser.\n\n{}", links.block()),
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
            EntryDatum::Commits(&commits),
            EntryDatum::GitRef {
                entry: &ref_entry,
                git_ref: &git_ref,
            },
            EntryDatum::JjChange {
                entry: &jj_entry,
                change: &jj_change,
            },
            EntryDatum::Session {
                entry: &session_entry,
                session: &session,
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
