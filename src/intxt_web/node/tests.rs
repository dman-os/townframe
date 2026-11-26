//! `NodeSource` against an in-memory node that answers the XRPC methods the viewer calls, with
//! the same envelopes a real node writes.

use super::*;
use crate::feed::FeedKind;
use crate::route::render;
use crate::view::EntryKind;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DatumsRead;
use serde_json::Value;
use serde_json::json;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use std::cell::RefCell;
use std::rc::Rc;

fn doc(byte: u8) -> DocId {
    MultibaseKey::try_from([byte; 32].as_slice())
        .expect("32 bytes")
        .to_string()
        .parse()
        .expect("a key is a doc id")
}

fn key(tag: &str, slot: &str) -> DatumKey {
    DatumKey::from_text(tag, slot).unwrap()
}

fn cap(byte: u8) -> CapId {
    CapId::from_bytes([byte; 16])
}

const DRAWER: u8 = 1;
const INDEX: u8 = 2;
const COMMITS: u8 = 3;
const NOTES: u8 = 4;
const PULL: u8 = 5;
const SESSION: u8 = 6;
/// More turns than one read takes, so the summary is folded across batches.
const TURNS: usize = 40;

const TIP: &str = "bbbbbbbbbbbbbbbb";
const BASE: &str = "aaaaaaaaaaaaaaaa";
const OLDER: &str = "cccccccccccccccc";

const PATCH: &str = "\
diff --git a/f b/f
--- a/f
+++ b/f
@@ -1 +1 @@
-old
+<b>new</b> & more
";

fn blob_digest(seed: u8) -> String {
    BlobId::new([seed; 32]).digest_str()
}

fn commit(parents: &[&str], name: &str, at: &str, message: &str) -> Value {
    let ident = json!({"name": name, "email": format!("{name}@example.org"), "at": at});
    json!({
        "parents": parents, "author": ident, "committer": ident,
        "message": message, "files": []
    })
}

/// A node holding one intxt: a commits entry (two commits of the range, a third below its base,
/// and a patch blob), a pull request entry, a comment and a reply.
#[derive(Default)]
struct FakeNode {
    docs: RefCell<BTreeMap<DocId, BTreeMap<DatumKey, Value>>>,
    blobs: RefCell<BTreeMap<BlobId, Vec<u8>>>,
    /// Every request's NSID, in order.
    calls: RefCell<Vec<String>>,
    down: bool,
    refuses: bool,
}

impl FakeNode {
    fn with_intxt() -> Self {
        let node = Self::default();
        let digest = blob_digest(9);
        node.blobs
            .borrow_mut()
            .insert(BlobId::new([9; 32]), PATCH.as_bytes().to_vec());
        let patch_ref = format!(
            "dtxt:datum:{}/org.example.daybook.blob/{digest}",
            doc(COMMITS)
        );
        let pull_ref = format!(
            "dtxt:datum:{}/org.example.intxt.work.githubPull/o/r/7",
            doc(PULL)
        );
        let tip_entry = key("org.example.intxt.work.gitCommitEntry", TIP);
        let pull_entry = key("org.example.intxt.work.githubPullEntry", "o/r/7");
        let self_ref = |entry: &DatumKey| {
            serde_json::to_value(DatumReference::new(ReferenceDocId::SelfDoc, entry.clone()))
                .unwrap()
        };
        let session_entry = key("org.example.intxt.work.sessionEntry", "s1");
        let session_ref = format!(
            "dtxt:datum:{}/org.example.intxt.work.agentSession/main",
            doc(SESSION)
        );
        let mut docs = node.docs.borrow_mut();
        docs.insert(
            doc(DRAWER),
            [(
                key("org.example.intxt.index", "main"),
                json!({"doc": doc(INDEX).to_string()}),
            )]
            .into(),
        );
        docs.insert(
            doc(INDEX),
            [
                (
                    key("org.example.intxt.title", "main"),
                    json!({"title": "Tested intxt", "description": "A **bold** claim <script>x</script>"}),
                ),
                (
                    key("org.example.intxt.body", "main"),
                    json!({"order": [self_ref(&tip_entry), self_ref(&pull_entry), self_ref(&session_entry)]}),
                ),
                (
                    tip_entry,
                    json!({
                        "tip": format!("org.example.intxt.work.gitCommit/{TIP}"),
                        "base": format!("org.example.intxt.work.gitCommit/{BASE}"),
                        "pack": format!("dtxt:datum:{}/org.example.daybook.blob/pack", doc(COMMITS)),
                        "patch": patch_ref,
                    }),
                ),
                (pull_entry, json!({"pull": pull_ref})),
                (session_entry, json!({"session": session_ref})),
            ]
            .into(),
        );
        // The turns' ids sort the other way round from their `seq`, as a real document's may.
        let mut session: BTreeMap<DatumKey, Value> = (0..TURNS)
            .map(|seq| {
                (
                    key(
                        "org.example.intxt.work.promptTurn",
                        &format!("t{:02}", TURNS - seq),
                    ),
                    json!({
                        "seq": seq,
                        "at": format!("2026-11-03T10:{seq:02}:00Z"),
                        "stopReason": "end_turn",
                        "messages": [
                            {"id": format!("m{seq}"), "seq": 1, "at": "2026-11-03T10:00:00Z",
                             "kind": "userMessage", "text": format!("prompt {seq}\nmore")},
                            {"id": format!("a{seq}"), "seq": 2, "at": "2026-11-03T10:00:00Z",
                             "kind": "agentMessage", "text": "x".repeat(10_000)}
                        ],
                        "toolCalls": [
                            {"id": format!("c{seq}"), "seq": 3, "title": "Edit", "kind": "edit",
                             "status": "completed",
                             "diffs": [{"path": format!("src/f{}.rs", seq % 3), "patch": "@@"}]}
                        ]
                    }),
                )
            })
            .collect();
        session.insert(
            key("org.example.intxt.work.agentSession", "main"),
            json!({"harness": "claude-code", "sessionId": "s1", "cwd": "/w"}),
        );
        // A turn that is not a turn: counted as unreadable, the rest still summarized.
        session.insert(
            key("org.example.intxt.work.promptTurn", "zz-bad"),
            json!({"seq": "not a number"}),
        );
        session.insert(
            key("org.example.intxt.work.plan", "main"),
            json!({"entries": []}),
        );
        docs.insert(doc(SESSION), session);
        docs.insert(
            doc(COMMITS),
            [
                (
                    key("org.example.intxt.commits", "main"),
                    json!({"writer": "0192"}),
                ),
                (
                    key("org.example.intxt.work.gitCommit", TIP),
                    commit(
                        &[OLDER],
                        "ada",
                        "2026-11-02T10:00:00Z",
                        "feat: the tip\n\nbody line",
                    ),
                ),
                (
                    key("org.example.intxt.work.gitCommit", OLDER),
                    commit(
                        &[BASE],
                        "grace",
                        "2026-11-01T10:00:00Z",
                        "fix: the older one",
                    ),
                ),
                // Below the range's base: held by the document (another entry's), not in the range.
                (
                    key("org.example.intxt.work.gitCommit", BASE),
                    commit(&[], "ada", "2026-10-01T10:00:00Z", "chore: the base"),
                ),
                (
                    key("org.example.daybook.blob", &digest),
                    json!({"mime": "text/x-diff", "digest": digest, "lengthOctets": PATCH.len()}),
                ),
            ]
            .into(),
        );
        docs.insert(
            doc(PULL),
            [(
                key("org.example.intxt.work.githubPull", "o/r/7"),
                json!({
                    "title": "A pull", "body": "Does a thing.\n\n<details>\n<summary>intxt</summary>\n\n```json\n{\"$type\":\"org.example.intxt.prLinks\",\"intxts\":[]}\n```\n\n</details>",
                    "state": "open", "head": "feat", "base": "main",
                    "headSha": "1", "baseSha": "2",
                    "updatedAt": "2026-11-17T10:00:00Z", "url": "https://github.com/o/r/pull/7"
                }),
            )]
            .into(),
        );
        let question = key("org.example.intxt.comment", "0192-01");
        let reply = key("org.example.intxt.comment", "0192-02");
        docs.insert(
            doc(NOTES),
            [
                (key("org.example.intxt.notes", "main"), json!({"writer": "0192"})),
                (
                    question.clone(),
                    json!({
                        "targets": [{"source": {"ref": format!("dtxt:datum:{}/{}", doc(COMMITS), key("org.example.intxt.work.gitCommit", TIP))}}],
                        "body": {"mime": "text/markdown", "content": "Why?"},
                        "motivation": "questioning",
                        "creator": {"type": "Person", "name": "ada"}
                    }),
                ),
                (
                    reply,
                    json!({
                        "targets": [],
                        "body": {"mime": "text/markdown", "content": "Because."},
                        "motivation": "replying",
                        "replyTo": format!("dtxt:datum:{}/{question}", doc(NOTES)),
                        "creator": {"type": "Person", "name": "grace"}
                    }),
                ),
            ]
            .into(),
        );
        drop(docs);
        node
    }

    fn error<E: serde::Serialize + std::fmt::Display>(error: E) -> XrpcResponse {
        XrpcResponse {
            status: service_core_rs::xrpc::DOMAIN_ERROR_STATUS,
            headers: Default::default(),
            body: XrpcBody::Json(
                serde_json::to_vec(&service_core_rs::xrpc::domain_error_envelope(&error)).unwrap(),
            ),
        }
    }

    fn ok(value: &impl serde::Serialize) -> XrpcResponse {
        XrpcResponse {
            status: http::StatusCode::OK,
            headers: Default::default(),
            body: XrpcBody::Json(serde_json::to_vec(value).unwrap()),
        }
    }

    fn body(request: &XrpcRequest) -> Value {
        let XrpcBody::Json(bytes) = &request.body else {
            panic!("the viewer sends JSON bodies only");
        };
        serde_json::from_slice(bytes).unwrap()
    }
}

/// The node as a transport; the test keeps its own handle to look at what was asked.
struct Wire(Rc<FakeNode>);

impl std::ops::Deref for Wire {
    type Target = FakeNode;

    fn deref(&self) -> &FakeNode {
        &self.0
    }
}

impl HttpTransport for Wire {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        if self.down {
            return Err(HttpTransportError {
                message: "connection refused".to_owned(),
            });
        }
        // The node's TCP edge refuses a request with no bearer, so every call carries the
        // dispatch capability as `<id>.<id>`.
        let bearer = request.headers.get("authorization").expect("a bearer");
        assert_eq!(
            bearer.to_str().unwrap(),
            format!("Bearer {0}.{0}", Authorization::id_text(cap(3)))
        );
        let target = request.path.strip_prefix("xrpc/").expect("an xrpc path");
        let (nsid, query) = target.split_once('?').unwrap_or((target, ""));
        self.calls.borrow_mut().push(nsid.to_owned());
        match nsid {
            "org.example.daybook.docs.readDatums" => {
                if self.refuses {
                    return Ok(FakeNode::error(DocsError::Denied));
                }
                let read: ReadDatums = serde_json::from_value(FakeNode::body(&request)).unwrap();
                let docs = self.docs.borrow();
                let Some(held) = docs.get(&read.doc_id) else {
                    return Ok(FakeNode::error(DocsError::DocNotFound {
                        doc_id: read.doc_id,
                    }));
                };
                let version = DatumVersion {
                    heads: vec![],
                    clock: 0,
                };
                let datums = match &read.datums {
                    None => held
                        .iter()
                        .map(|(key, value)| {
                            (
                                key.clone(),
                                DatumRead::Accepted {
                                    value: value.clone(),
                                    version: version.clone(),
                                    authors: vec![],
                                },
                            )
                        })
                        .collect(),
                    Some(keys) => keys
                        .iter()
                        .map(|key| {
                            let read = match held.get(key) {
                                Some(value) => DatumRead::Accepted {
                                    value: value.clone(),
                                    version: version.clone(),
                                    authors: vec![],
                                },
                                None => DatumRead::Absent {
                                    version: version.clone(),
                                },
                            };
                            (key.clone(), read)
                        })
                        .collect(),
                };
                Ok(FakeNode::ok(&DatumsRead {
                    heads: vec![],
                    datums,
                }))
            }
            "org.example.daybook.docs.pollDatums" => {
                let poll: PollDatums = serde_json::from_value(FakeNode::body(&request)).unwrap();
                let Predicate::And { clauses } = poll.predicate else {
                    panic!("the viewer anchors its polls on a drawer");
                };
                // A poll names either the one document it wants or a marker key's holders.
                let named = clauses.iter().find_map(|clause| match clause {
                    Predicate::IsDoc { doc_id } => Some(doc_id.clone()),
                    _ => None,
                });
                let marker = clauses.iter().find_map(|clause| match clause {
                    Predicate::HasKey { key } => Some(key.clone()),
                    _ => None,
                });
                let rows: Vec<(DocId, DatumKey)> = self
                    .docs
                    .borrow()
                    .iter()
                    .filter(|(doc_id, datums)| {
                        named.as_ref().is_none_or(|named| named == *doc_id)
                            && marker
                                .as_ref()
                                .is_none_or(|marker| datums.contains_key(marker))
                    })
                    .flat_map(|(doc_id, datums)| {
                        datums
                            .keys()
                            .filter(|key| poll.datums.selects(key))
                            .map(|key| (doc_id.clone(), key.clone()))
                    })
                    .collect();
                let events = rows
                    .into_iter()
                    .enumerate()
                    .map(|(revision, (doc_id, datum_key))| DatumEvent::Datum {
                        revision: revision as u64 + 1,
                        doc_id,
                        datum_key,
                        datum: ObservedDatum::Present {
                            doc_heads: vec![],
                            version: DatumVersion {
                                heads: vec![],
                                clock: 0,
                            },
                        },
                    })
                    .collect::<Vec<_>>();
                Ok(FakeNode::ok(&DatumPage {
                    through: events.len() as u64,
                    events,
                    caught_up: true,
                }))
            }
            "org.example.daybook.blobs.read" => {
                let blob_id = query
                    .split('&')
                    .find_map(|pair| pair.strip_prefix("blobId="))
                    .expect("a blobId parameter");
                let blob_id: BlobId = blob_id.parse().expect("a blob id");
                let Some(bytes) = self.blobs.borrow().get(&blob_id).cloned() else {
                    return Ok(FakeNode::error(BlobsError::NotFound));
                };
                let range = request
                    .headers
                    .get("range")
                    .and_then(|value| ByteRange::from_header(value.to_str().ok()?));
                let bytes = match range {
                    Some(ByteRange::Between { start, end }) => {
                        let end = (end as usize + 1).min(bytes.len());
                        bytes[start as usize..end].to_vec()
                    }
                    _ => bytes,
                };
                Ok(XrpcResponse {
                    status: http::StatusCode::PARTIAL_CONTENT,
                    headers: Default::default(),
                    body: XrpcBody::Json(bytes),
                })
            }
            other => panic!("the viewer called {other}"),
        }
    }
}

fn source(node: &Rc<FakeNode>) -> NodeSource<Wire> {
    NodeSource::new(
        Wire(node.clone()),
        NodeConfig {
            node: "http://127.0.0.1:1".to_owned(),
            doc_cap: cap(1),
            blob_cap: cap(2),
            bearer: cap(3),
        },
    )
}

fn load(node: &Rc<FakeNode>, drawer: u8) -> Result<IntxtView, LoadError> {
    pollster::block_on(source(node).load(&doc(drawer), &[]))
}

#[test]
fn an_intxt_loads_with_its_commits_patch_comments_and_pull_request() {
    let node = Rc::new(FakeNode::with_intxt());
    let view = load(&node, DRAWER).unwrap();

    assert_eq!(view.title, "Tested intxt");
    assert_eq!(
        view.description.as_deref(),
        Some("A **bold** claim <script>x</script>")
    );
    assert_eq!(view.entries.len(), 3);

    let range = &view.entries[0];
    assert_eq!(range.kind, EntryKind::Commits);
    assert_eq!(range.label, "aaaaaaaa..bbbbbbbb");
    // The range is the tip and what is above the base, newest first; the base itself is out.
    let shas: Vec<&str> = range.commits.iter().map(|c| c.sha.as_str()).collect();
    assert_eq!(shas, [TIP, OLDER]);
    assert_eq!(range.commits[0].author, "ada");
    assert_eq!(range.commits[0].at.to_string(), "2026-11-02T10:00:00Z");
    assert_eq!(range.commits[0].message, "feat: the tip\n\nbody line");
    let patch = range.patch.as_ref().expect("the patch blob is held");
    assert_eq!(patch.omitted_octets, 0);
    assert_eq!(patch.lines.last().unwrap().text, "+<b>new</b> & more");

    let pull = &view.entries[1];
    assert_eq!(pull.kind, EntryKind::GithubPull);
    // The description is shown without the intxt block.
    assert_eq!(pull.description.as_deref(), Some("Does a thing."));

    let session = &view.entries[2];
    assert_eq!(session.kind, EntryKind::Session);
    let summary = session.session.as_ref().expect("a session has its summary");
    assert_eq!(summary.turns, TURNS);
    assert_eq!(summary.unreadable, 1);
    assert_eq!(summary.tool_calls, TURNS);
    assert_eq!(summary.tools["edit"], TURNS);
    // Folded across batches whatever the order the node listed the turns in.
    let prompts: Vec<&str> = summary.prompts().map(|p| p.headline.as_str()).collect();
    assert_eq!(
        prompts.len(),
        crate::session::SessionSummary::MAX_PROMPTS.min(TURNS)
    );
    assert_eq!(&prompts[..3], ["prompt 0", "prompt 1", "prompt 2"]);
    assert_eq!(
        summary.edited_files().collect::<Vec<_>>(),
        ["src/f0.rs", "src/f1.rs", "src/f2.rs"]
    );
    let (first, last) = summary.span.unwrap();
    assert_eq!(first.to_string(), "2026-11-03T10:00:00Z");
    assert_eq!(last.to_string(), "2026-11-03T10:39:00Z");

    assert_eq!(view.threads.len(), 1);
    assert_eq!(view.threads[0].root.body, "Why?");
    assert_eq!(view.threads[0].replies[0].body, "Because.");

    // The page escapes the diff and the description's raw HTML.
    let page = render(crate::route::intxt_page, (view, crate::view::Tab::Files));
    assert!(
        page.contains("+&#60;b&#62;new&#60;/b&#62; &#38; more"),
        "{page}"
    );
    assert!(!page.contains("<b>new</b>"), "{page}");
    assert!(!page.contains("<script>x"), "{page}");
    assert!(page.contains("<strong>bold</strong>"), "{page}");
}

#[test]
fn the_node_is_asked_only_what_the_page_needs() {
    let node = Rc::new(FakeNode::with_intxt());
    load(&node, DRAWER).unwrap();
    let calls = node.calls.borrow();
    let count = |nsid: &str| calls.iter().filter(|call| call.ends_with(nsid)).count();
    // The pointer, the index, the PR document, the patch's blob datum, the commits document and
    // the notes document; the session's own datum, and its turns read in batches (the plan and
    // the session datum are not asked for).
    let batches = (TURNS + 1).div_ceil(TURNS_PER_READ);
    assert_eq!(count("readDatums"), 7 + batches, "{calls:?}");
    // One poll for the commits documents (read once for every entry that needs them), one for
    // the notes documents and one for the session's turn keys.
    assert_eq!(count("pollDatums"), 3, "{calls:?}");
    assert_eq!(count("blobs.read"), 1, "{calls:?}");
}

#[test]
fn a_drawer_the_node_does_not_hold_is_not_found() {
    let node = Rc::new(FakeNode::with_intxt());
    let error = load(&node, 77).unwrap_err();
    assert!(matches!(error, LoadError::NotFound(_)), "{error}");
    assert_eq!(error.status(), 404);
}

#[test]
fn a_drawer_with_no_index_pointer_is_not_an_intxt() {
    let node = Rc::new(FakeNode::with_intxt());
    node.docs.borrow_mut().insert(doc(50), BTreeMap::new());
    let error = load(&node, 50).unwrap_err();
    assert!(matches!(error, LoadError::NotFound(_)), "{error}");
}

#[test]
fn a_node_that_cannot_be_reached_is_unreachable() {
    let node = Rc::new(FakeNode {
        down: true,
        ..FakeNode::with_intxt()
    });
    let error = load(&node, DRAWER).unwrap_err();
    assert!(
        matches!(&error, LoadError::Unreachable { reason, .. } if reason.contains("connection refused")),
        "{error}"
    );
    assert_eq!(error.status(), 502);
}

#[test]
fn a_refused_capability_is_forbidden() {
    let node = Rc::new(FakeNode {
        refuses: true,
        ..FakeNode::with_intxt()
    });
    let error = load(&node, DRAWER).unwrap_err();
    assert!(matches!(error, LoadError::Forbidden(_)), "{error}");
    assert_eq!(error.status(), 403);
}

#[test]
fn an_index_that_is_not_an_intxts_is_unreadable_not_a_crash() {
    let node = Rc::new(FakeNode::with_intxt());
    node.docs
        .borrow_mut()
        .get_mut(&doc(INDEX))
        .unwrap()
        .insert(key("org.example.intxt.title", "main"), json!({"nope": 1}));
    let error = load(&node, DRAWER).unwrap_err();
    assert!(matches!(error, LoadError::Unreadable { .. }), "{error}");
    assert_eq!(error.status(), 502);
}

/// Adds `entries` (datum key, value) to the index document and the end of the body's order.
fn add_entries(node: &FakeNode, entries: &[(DatumKey, Value)]) {
    let mut docs = node.docs.borrow_mut();
    let index = docs.get_mut(&doc(INDEX)).unwrap();
    let body_key = key("org.example.intxt.body", "main");
    for (entry_key, value) in entries {
        let reference = serde_json::to_value(DatumReference::new(
            ReferenceDocId::SelfDoc,
            entry_key.clone(),
        ))
        .unwrap();
        index.get_mut(&body_key).unwrap()["order"]
            .as_array_mut()
            .unwrap()
            .push(reference);
        // An empty object stands for an entry the index does not hold.
        if !value.as_object().unwrap().is_empty() {
            index.insert(entry_key.clone(), value.clone());
        }
    }
}

#[test]
fn an_entry_that_cannot_be_read_is_a_gap_and_the_rest_of_the_page_stands() {
    let node = Rc::new(FakeNode::with_intxt());
    add_entries(
        &node,
        &[
            // Named by the body and absent from the index document.
            (
                key("org.example.intxt.work.sessionEntry", "gone"),
                json!({}),
            ),
            // Present and not an entry.
            (
                key("org.example.intxt.work.githubPullEntry", "bad"),
                json!({"pull": 7}),
            ),
        ],
    );
    let view = load(&node, DRAWER).unwrap();
    assert_eq!(view.entries.len(), 5);
    let gaps: Vec<_> = view
        .entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Gap)
        .collect();
    assert_eq!(gaps.len(), 2);
    assert!(
        gaps[0].label.ends_with("sessionEntry/gone"),
        "{}",
        gaps[0].label
    );
    assert!(
        gaps[0].refs[0].value.contains("not readable"),
        "{:?}",
        gaps[0].refs
    );
    assert_eq!(view.entries[0].kind, EntryKind::Commits);
    // Said in the feed too, after the dated items.
    let in_feed = view
        .feed()
        .iter()
        .filter(|item| matches!(item.what, FeedKind::Entry(index) if view.entries[index].kind == EntryKind::Gap))
        .count();
    assert_eq!(in_feed, 2);
}

#[test]
fn a_comment_or_structure_that_is_not_one_is_a_gap_and_the_rest_stands() {
    let node = Rc::new(FakeNode::with_intxt());
    {
        let mut docs = node.docs.borrow_mut();
        let notes = docs.get_mut(&doc(NOTES)).unwrap();
        notes.insert(
            key("org.example.intxt.comment", "0192-bad"),
            json!({"body": 3}),
        );
        notes.insert(
            key("org.example.intxt.structure", "0192-bad"),
            json!({"kind": "section"}),
        );
    }
    let view = load(&node, DRAWER).unwrap();
    assert_eq!(view.threads.len(), 1);
    let gaps: Vec<_> = view
        .entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Gap)
        .map(|entry| entry.label.as_str())
        .collect();
    assert_eq!(gaps.len(), 2, "{gaps:?}");
    assert!(gaps.iter().any(|label| label.contains("comment/0192-bad")));
    assert!(
        gaps.iter()
            .any(|label| label.contains("structure/0192-bad"))
    );
}

#[test]
fn an_entry_of_an_unknown_kind_is_kept_as_a_generic_datum() {
    let node = Rc::new(FakeNode::with_intxt());
    add_entries(
        &node,
        &[(
            key("org.example.intxt.checkRun", "ci"),
            json!({"name": "ci", "status": "passed", "creator": {"name": "ci-bot"},
                   "at": "2026-11-03T09:00:00Z", "nested": {"x": 1}}),
        )],
    );
    let view = load(&node, DRAWER).unwrap();
    let generic = view.entries.last().unwrap();
    assert_eq!(generic.kind, EntryKind::Generic);
    assert_eq!(generic.label, "org.example.intxt.checkRun");
    assert_eq!(generic.actor.as_deref(), Some("ci-bot"));
    assert_eq!(generic.at.unwrap().to_string(), "2026-11-03T09:00:00Z");
    let fields: Vec<_> = generic
        .refs
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(fields, ["at", "name", "status"]);
}

#[test]
fn structures_in_the_notes_become_the_guide_and_comment_slots_give_times() {
    let node = Rc::new(FakeNode::with_intxt());
    {
        let mut docs = node.docs.borrow_mut();
        let notes = docs.get_mut(&doc(NOTES)).unwrap();
        notes.insert(
            key(
                "org.example.intxt.structure",
                "0193c000-0000-7000-8000-000000000001",
            ),
            json!({"kind": "section", "title": "Start here", "order": "1"}),
        );
        // A comment slot that is a UUIDv7 carries its time.
        notes.insert(
            key(
                "org.example.intxt.comment",
                "01935f3a-8c00-7000-8000-000000000009",
            ),
            json!({"targets": [], "body": {"mime": "text/markdown", "content": "timed"},
                   "motivation": "commenting", "creator": {"type": "Person", "name": "ada"}}),
        );
    }
    let view = load(&node, DRAWER).unwrap();
    assert_eq!(view.guide.len(), 1);
    assert_eq!(view.guide[0].title, "Start here");
    assert!(view.guide[0].at.is_some());
    let timed = view
        .threads
        .iter()
        .find(|thread| thread.root.body == "timed")
        .unwrap();
    assert!(timed.root.at.is_some());
    // The slots the older tests use are not UUIDv7: no time, and not invented.
    assert!(view.threads.iter().any(|thread| thread.root.at.is_none()));
}

#[test]
fn a_patch_blob_the_node_does_not_hold_leaves_the_entry_without_one() {
    let node = Rc::new(FakeNode::with_intxt());
    node.blobs.borrow_mut().clear();
    let view = load(&node, DRAWER).unwrap();
    assert_eq!(view.entries[0].commits.len(), 2);
    assert_eq!(view.entries[0].patch, None);
}

#[test]
fn a_patch_over_the_render_cap_is_fetched_as_a_prefix_and_says_how_much_is_left() {
    let node = Rc::new(FakeNode::with_intxt());
    let digest = blob_digest(9);
    let line = format!("+{}\n", "x".repeat(99));
    let big = format!("@@ -0,0 +1 @@\n{}", line.repeat(MAX_RENDERED_OCTETS / 50));
    node.docs
        .borrow_mut()
        .get_mut(&doc(COMMITS))
        .unwrap()
        .insert(
            key("org.example.daybook.blob", &digest),
            json!({"mime": "text/x-diff", "digest": digest, "lengthOctets": big.len()}),
        );
    node.blobs
        .borrow_mut()
        .insert(BlobId::new([9; 32]), big.clone().into_bytes());
    let view = load(&node, DRAWER).unwrap();
    let patch = view.entries[0].patch.as_ref().unwrap();
    let shown: usize = patch.lines.iter().map(|line| line.text.len() + 1).sum();
    assert!(shown <= MAX_RENDERED_OCTETS);
    assert_eq!(patch.omitted_octets, (big.len() - shown) as u64);
}

#[test]
fn the_environment_names_a_node_the_fixture_or_a_mistake() {
    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find_map(|(key, value)| (*key == name).then(|| (*value).to_owned()))
        }
    }
    assert_eq!(NodeConfig::from_env(env(&[])), Ok(None));
    let cap = "00000000-0000-0000-0000-000000000001";
    let full = NodeConfig::from_env(env(&[
        (NODE_VAR, "http://127.0.0.1:8080"),
        (DOC_CAP_VAR, cap),
        (BLOB_CAP_VAR, cap),
        (DISPATCH_CAP_VAR, cap),
    ]))
    .unwrap()
    .unwrap();
    assert_eq!(full.node, "http://127.0.0.1:8080");
    assert_eq!(
        NodeConfig::from_env(env(&[(NODE_VAR, "http://x"), (BLOB_CAP_VAR, cap)])),
        Err(ConfigError::Incomplete {
            missing: vec![DOC_CAP_VAR, DISPATCH_CAP_VAR]
        })
    );
    assert!(matches!(
        NodeConfig::from_env(env(&[
            (NODE_VAR, "http://x"),
            (DOC_CAP_VAR, "nope"),
            (BLOB_CAP_VAR, cap),
            (DISPATCH_CAP_VAR, cap)
        ])),
        Err(ConfigError::BadCapability {
            var: DOC_CAP_VAR,
            ..
        })
    ));
}

#[test]
fn a_base_url_splits_into_what_wasi_http_wants() {
    let base = BaseUrl::parse("http://127.0.0.1:8080").unwrap();
    assert_eq!(
        base,
        BaseUrl {
            https: false,
            authority: "127.0.0.1:8080".to_owned(),
            path_prefix: String::new()
        }
    );
    assert_eq!(base.target("xrpc/a.b?x=1"), "/xrpc/a.b?x=1");
    let base = BaseUrl::parse("https://node.example/relay/").unwrap();
    assert!(base.https);
    assert_eq!(base.target("xrpc/a.b"), "/relay/xrpc/a.b");
    assert!(BaseUrl::parse("ftp://x").is_err());
    assert!(BaseUrl::parse("http://").is_err());
}
