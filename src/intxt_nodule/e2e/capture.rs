//! A capture session end to end (ADR 039 §4): scripted hooks over a real git repository and a
//! synthetic transcript, observed by `intxt_capture`'s host half and recorded by the client
//! library's recorder (`intxt_core::client`) against the node, whose answer the host commits as
//! its state. The nodule is booted only because the node needs its cartridge's records, and
//! serves `createIntxt` for the capture drawer.

use super::*;
use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::ActorUrl;
use big_cabinet_core::drawer::protocol::DrawerId;
use intxt_capture::hook::Hook;
use intxt_core::capture::Batch;
use intxt_core::capture::BlobRef;
use intxt_core::capture::Content;
use intxt_core::capture::RecordedIds;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_NAME", "Grace")
        .env("GIT_COMMITTER_EMAIL", "grace@example.com")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

struct Session {
    _dir: tempfile::TempDir,
    repo: PathBuf,
    transcript: PathBuf,
    state: PathBuf,
    drawer: DrawerId,
    machine: ActorUrl,
    calls: usize,
    /// The repository's root commit, which keys its `gitRepo` datum.
    root_commit: String,
}

/// A session in a fresh git repository, recording into a new intxt drawer of `node`.
async fn start(node: &TestNode) -> Session {
    let drawer = node
        .call(intxt_core::methods::CreateIntxt {
            title: "capture".to_owned(),
            description: None,
        })
        .await
        .unwrap()
        .drawer_id;
    Session::new(drawer)
}

/// A transcript record of Claude Code's shape.
fn record(kind: &str, uuid: &str, parent: Option<&str>, message: Value, extra: Value) -> Value {
    let mut record = json!({"type": kind, "uuid": uuid, "parentUuid": parent,
        "timestamp": "2026-11-10T10:00:00Z", "cwd": "/work/repo", "sessionId": "s1",
        "message": message});
    record
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    record
}

impl Session {
    fn new(drawer: DrawerId) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let repo = base.join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join("a.txt"), "one\ntwo\n").unwrap();
        std::fs::write(repo.join("b.txt"), "bee\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "first"]);
        let root_commit = git(&repo, &["rev-parse", "HEAD"]);
        let transcript = base.join("claude/s1.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        Self {
            state: base.join("node/s1.json"),
            _dir: dir,
            repo,
            transcript,
            drawer,
            machine: serde_json::from_value(json!("dtxt:actor:z11111111111111111111111111111111"))
                .unwrap(),
            calls: 0,
            root_commit,
        }
    }

    fn hook(&self, event: &str, tool: Option<(&str, &str)>) -> Hook {
        let mut input = json!({
            "session_id": "s1",
            "transcript_path": self.transcript,
            "cwd": self.repo,
            "hook_event_name": event,
        });
        if let Some((name, id)) = tool {
            input["tool_name"] = json!(name);
            input["tool_use_id"] = json!(id);
            input["tool_input"] = json!({"command": "sed -i s/two/TWO/ a.txt"});
        }
        Hook::parse(input.to_string().as_bytes()).unwrap()
    }

    fn append(&self, records: &[Value]) {
        self.append_to(&self.transcript, records);
    }

    fn append_to(&self, path: &Path, records: &[Value]) {
        use std::io::Write;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = std::fs::File::options()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        for record in records {
            writeln!(file, "{record}").unwrap();
        }
    }

    /// Records `batch` with the library's recorder under idempotency key `key`.
    async fn send(&self, node: &TestNode, batch: &Batch, key: &str) -> RecordedIds {
        operator(node, key)
            .record_observations(&self.drawer, &self.machine, batch)
            .await
            .unwrap_or_else(|error| panic!("the recorder failed: {error}"))
    }

    /// Runs `hook`: observe, record what was observed (twice, under one idempotency key, when
    /// `retry`, which must answer the same), and commit the answer as the host's state. None when
    /// the hook observed nothing.
    async fn run(&mut self, node: &TestNode, hook: Hook, retry: bool) -> Option<RecordedIds> {
        let prepared = intxt_capture::host::prepare(&hook, &self.state).unwrap();
        if prepared.batch.is_empty() {
            prepared.commit(RecordedIds::default()).unwrap();
            return None;
        }
        self.calls += 1;
        let key = format!("hook-{}", self.calls);
        let recorded = self.send(node, &prepared.batch, &key).await;
        if retry {
            assert_eq!(
                self.send(node, &prepared.batch, &key).await,
                recorded,
                "a retried call answers the same"
            );
        }
        prepared.commit(recorded.clone()).unwrap();
        Some(recorded)
    }
}

/// The accepted value of `tag/slot` in `doc`; panics when the document lacks it.
async fn value(node: &TestNode, doc: &str, tag: &str, slot: &str) -> Value {
    let datum_key: DatumKey = DatumKey::new(tag, slot.as_bytes()).unwrap();
    let doc: DocId = doc.parse().unwrap();
    read(node, &doc, std::slice::from_ref(&datum_key))
        .await
        .remove(&datum_key)
        .unwrap_or_else(|| panic!("{tag}/{slot} is not in {doc}"))
}

/// A session that edits two files, renames one, and commits: the worktree's documents, ops, file
/// documents, git layer and the session's turns are all where ADR 040 puts them, and a retried
/// call changes nothing.
#[tokio::test]
async fn a_capture_session_is_recorded_as_adr_040_lays_it_out() {
    let node = boot().await;
    let drawer = node
        .call(intxt_core::methods::CreateIntxt {
            title: "capture".to_owned(),
            description: None,
        })
        .await
        .unwrap()
        .drawer_id;
    let mut session = Session::new(drawer);
    let root = session.repo.display().to_string();

    // The session starts: its first turn names a tool call.
    session.append(&[
        json!({"type": "user", "uuid": "u1", "parentUuid": null, "timestamp": "2026-11-10T10:00:00Z",
               "cwd": session.repo, "sessionId": "s1", "message": {"role": "user", "content": "edit it"}}),
        json!({"type": "assistant", "uuid": "a1", "parentUuid": "u1", "timestamp": "2026-11-10T10:00:01Z",
               "cwd": session.repo, "sessionId": "s1",
               "message": {"id": "m-t1", "content": [{"type": "tool_use", "id": "t1", "name": "Bash",
                           "input": {"command": "sed -i s/two/TWO/ a.txt"}}]}}),
    ]);
    let start = session
        .run(&node, session.hook("SessionStart", None), false)
        .await
        .expect("the first hook records the session");
    let session_doc = start.session.clone().expect("the session got a document");
    let turn = value(&node, &session_doc, work::PromptTurn::ID, "u1").await;
    assert_eq!(turn["toolCalls"][0]["id"], "t1");
    assert_eq!(
        value(&node, &session_doc, work::AgentSession::ID, "main").await["sessionId"],
        "s1"
    );

    // The tool call edits two files.
    assert!(
        session
            .run(
                &node,
                session.hook("PreToolUse", Some(("Bash", "t1"))),
                false
            )
            .await
            .is_none()
    );
    std::fs::write(session.repo.join("a.txt"), "one\nTWO\n").unwrap();
    std::fs::write(session.repo.join("b.txt"), "bee\nedited\n").unwrap();
    let edited = session
        .run(
            &node,
            session.hook("PostToolUse", Some(("Bash", "t1"))),
            true,
        )
        .await
        .expect("the call changed files");
    let docs = &edited.worktrees[&root].docs;
    let worktree_doc = docs.worktree.clone().unwrap();
    let worktree: work::Worktree =
        serde_json::from_value(value(&node, &worktree_doc, work::Worktree::ID, "main").await)
            .unwrap();
    assert_eq!(worktree.path, root);
    assert_eq!(worktree.op_logs.len(), 1);
    assert_eq!(worktree.op_logs[0].from, 0);
    let log = worktree.op_logs[0].doc.clone();
    let first_op = value(&node, &log, work::WorktreeOp::ID, "0").await;
    assert_eq!(first_op["kind"], "edit");
    assert_eq!(
        first_op["cause"],
        format!(
            "dtxt:datum:{session_doc}/org.example.intxt.work.promptTurn/u1#toolCalls/%7B%22id%22:%22t1%22%7D"
        )
    );
    let file_doc = worktree.files["a.txt"].clone();
    assert_eq!(
        value(&node, &file_doc, generic::Note::ID, "main").await,
        json!({"mime": "text/plain", "content": "one\nTWO\n"})
    );
    assert_eq!(
        value(&node, &file_doc, generic::PathName::ID, "main").await,
        json!({"path": "a.txt"})
    );
    assert_eq!(
        value(&node, &file_doc, work::File::ID, "main").await["worktree"],
        format!("dtxt:datum:{worktree_doc}/org.example.intxt.work.worktree/main")
    );
    assert_eq!(first_op["files"].as_object().unwrap().len(), 2);
    let b_doc = worktree.files["b.txt"].clone();

    // A file moved: its document continues under the new path.
    std::fs::rename(session.repo.join("b.txt"), session.repo.join("c.txt")).unwrap();
    session
        .run(&node, session.hook("Stop", None), false)
        .await
        .expect("the move is an op");
    let worktree: work::Worktree =
        serde_json::from_value(value(&node, &worktree_doc, work::Worktree::ID, "main").await)
            .unwrap();
    assert_eq!(worktree.files.get("c.txt"), Some(&b_doc));
    assert!(!worktree.files.contains_key("b.txt"));
    assert_eq!(
        value(&node, &b_doc, generic::PathName::ID, "main").await,
        json!({"path": "c.txt"})
    );
    let moved = value(&node, &log, work::WorktreeOp::ID, "1").await;
    assert_eq!(moved["renames"], json!({"c.txt": "b.txt"}));
    assert!(moved.get("cause").is_none(), "{moved}");

    // A commit: an op of its kind, the commit in the repository document, the ref in the clone's.
    git(&session.repo, &["add", "-A"]);
    git(&session.repo, &["commit", "-q", "-m", "second"]);
    let sha = git(&session.repo, &["rev-parse", "HEAD"]);
    let committed = session
        .run(&node, session.hook("Stop", None), false)
        .await
        .expect("the commit is an op");
    let docs = &committed.worktrees[&root].docs;
    assert_eq!(docs, &edited.worktrees[&root].docs);
    let commit_op = value(&node, &log, work::WorktreeOp::ID, "2").await;
    assert_eq!(commit_op["kind"], "commit");
    assert_eq!(commit_op["git"]["head"], sha);
    let commit = value(
        &node,
        docs.repo.as_ref().unwrap(),
        work::GitCommit::ID,
        &sha,
    )
    .await;
    assert_eq!(commit["message"], "second");
    let clone_id_slot = String::from_utf8(worktree.clone.key().slot_bytes().to_vec()).unwrap();
    // The repository is keyed by its root commit, which any clone of it derives alike.
    let repo = value(
        &node,
        docs.repo.as_ref().unwrap(),
        work::GitRepo::ID,
        &session.root_commit,
    )
    .await;
    assert_eq!(repo["roots"], json!([session.root_commit]));
    assert_eq!(
        value(
            &node,
            docs.clone.as_ref().unwrap(),
            work::GitClone::ID,
            &clone_id_slot
        )
        .await["repo"],
        format!(
            "dtxt:datum:{}/org.example.intxt.work.gitRepo/{}",
            docs.repo.as_ref().unwrap(),
            session.root_commit
        )
    );
    let main_ref = value(
        &node,
        docs.clone.as_ref().unwrap(),
        work::GitRef::ID,
        &format!("{clone_id_slot}/refs/heads/main"),
    )
    .await;
    assert_eq!(main_ref["target"], sha);
    assert_eq!(committed.worktrees[&root].next_seq, 3);
    node.stop().await;
}

/// A first attempt that did not finish leaves the same key behind: re-sending it with a batch
/// whose content changed in the meantime is a new write, not a refusal, and records the change.
#[tokio::test]
async fn a_changed_batch_under_the_same_key_records_the_change() {
    let node = boot().await;
    let drawer = node
        .call(intxt_core::methods::CreateIntxt {
            title: "capture".to_owned(),
            description: None,
        })
        .await
        .unwrap()
        .drawer_id;
    let session = Session::new(drawer);
    let root = session.repo.display().to_string();
    session.append(&[json!({"type": "user", "uuid": "u1", "parentUuid": null,
        "timestamp": "2026-11-10T10:00:00Z", "cwd": session.repo, "sessionId": "s1",
        "message": {"role": "user", "content": "edit it"}})]);
    std::fs::write(session.repo.join("a.txt"), "one\nTWO\n").unwrap();
    let prepared =
        intxt_capture::host::prepare(&session.hook("Stop", None), &session.state).unwrap();
    let first = session.send(&node, &prepared.batch, "hook-1").await;

    let mut batch = prepared.batch.clone();
    let ops = &mut batch.worktrees[0].ops;
    ops[0].changes[0].content = Some(Content::text("one\nCHANGED\n"));
    let again = ops[0].clone();
    ops.push(again);
    let second = session.send(&node, &batch, "hook-1").await;
    assert_eq!(
        second.worktrees[&root].next_seq,
        first.worktrees[&root].next_seq + 1
    );

    let docs = &second.worktrees[&root].docs;
    let worktree: work::Worktree = serde_json::from_value(
        value(
            &node,
            docs.worktree.as_ref().unwrap(),
            work::Worktree::ID,
            "main",
        )
        .await,
    )
    .unwrap();
    assert_eq!(
        value(&node, &worktree.files["a.txt"], generic::Note::ID, "main").await["content"],
        "one\nCHANGED\n"
    );
    let log = &worktree.op_logs[0].doc;
    value(&node, log, work::WorktreeOp::ID, "1").await;
    prepared.commit(second).unwrap();
    node.stop().await;
}

/// Whether `tag/slot` is absent from `doc`.
async fn is_absent(node: &TestNode, doc: &str, tag: &str, slot: &str) -> bool {
    let datum_key = DatumKey::new(tag, slot.as_bytes()).unwrap();
    let doc: DocId = doc.parse().unwrap();
    let capability = capability(
        node,
        DocScope {
            read: BTreeSet::from([doc.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let mut read = node
        .big_node()
        .docs
        .client
        .call(
            RequestContext::root(),
            ReadDatums {
                capability,
                doc_id: doc,
                at: At::Current,
                datums: Some(BTreeSet::from([datum_key.clone()])),
                optimistic: false,
            },
        )
        .await
        .unwrap();
    matches!(
        read.datums.remove(&datum_key),
        Some(DatumRead::Absent { .. })
    )
}

/// A blob-classified file whose bytes the host put in the blob service is recorded as a
/// `org.example.daybook.blob` datum in its file document, which `file.blob` references; when the
/// path turns into text, the blob datum goes in the write that stores the note (ledger D45).
#[tokio::test]
async fn a_blob_file_is_a_blob_datum_until_it_becomes_text() {
    let node = boot().await;
    let drawer = node
        .call(intxt_core::methods::CreateIntxt {
            title: "capture".to_owned(),
            description: None,
        })
        .await
        .unwrap()
        .drawer_id;
    let session = Session::new(drawer);
    let root = session.repo.display().to_string();
    session.append(&[json!({"type": "user", "uuid": "u1", "parentUuid": null,
        "timestamp": "2026-11-10T10:00:00Z", "cwd": session.repo, "sessionId": "s1",
        "message": {"role": "user", "content": "edit it"}})]);
    std::fs::write(session.repo.join("a.txt"), [0u8, 159, 146, 150]).unwrap();
    let prepared =
        intxt_capture::host::prepare(&session.hook("Stop", None), &session.state).unwrap();

    // The host puts the bytes first, as the operator.
    let blobs = &node.big_node().blobs.client;
    let operator = Authorization::identity(node.big_node().authority.root).metadata();
    let put = blobs
        .call_with(
            RequestContext::root(),
            operator,
            MintBlobsCapability {
                scope: BlobsScope {
                    put: true,
                    ..Default::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let info = blobs
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: vec![0u8, 159, 146, 150].into(),
            },
        )
        .await
        .unwrap();
    let digest = info.blob_id.digest_str();

    let mut batch = prepared.batch.clone();
    let content = &mut batch.worktrees[0].ops[0].changes[0].content;
    assert_eq!(content.as_ref().unwrap().text, None, "a.txt is a blob");
    content.as_mut().unwrap().blob = Some(BlobRef {
        digest: digest.clone(),
        length_octets: info.length_octets,
        mime: "application/octet-stream".to_owned(),
        extra: Default::default(),
    });
    let first = session.send(&node, &batch, "hook-1").await;
    let recorded = &first.worktrees[&root];
    let worktree: work::Worktree = serde_json::from_value(
        value(
            &node,
            recorded.docs.worktree.as_ref().unwrap(),
            work::Worktree::ID,
            "main",
        )
        .await,
    )
    .unwrap();
    let file_doc = worktree.files["a.txt"].clone();
    assert_eq!(
        value(&node, &file_doc, "org.example.daybook.blob", "main").await,
        json!({"mime": "application/octet-stream", "digest": digest, "lengthOctets": 4})
    );
    assert_eq!(
        value(&node, &file_doc, work::File::ID, "main").await["blob"],
        "dtxt:datum:self/org.example.daybook.blob/main"
    );
    assert!(is_absent(&node, &file_doc, generic::Note::ID, "main").await);

    // The next observation finds the path as text.
    let mut batch = prepared.batch.clone();
    batch.worktrees[0].docs = recorded.docs.clone();
    batch.worktrees[0].next_seq = recorded.next_seq;
    batch.worktrees[0].ops[0].changes[0].content = Some(Content::text("now text\n"));
    session.send(&node, &batch, "hook-2").await;
    assert_eq!(
        value(&node, &file_doc, generic::Note::ID, "main").await["content"],
        "now text\n"
    );
    assert!(is_absent(&node, &file_doc, "org.example.daybook.blob", "main").await);
    assert!(
        value(&node, &file_doc, work::File::ID, "main")
            .await
            .get("blob")
            .is_none()
    );
    prepared.commit(first).unwrap();
    node.stop().await;
}

/// The worktree datum of the worktree `recorded` names.
async fn worktree_of(node: &TestNode, docs: &intxt_core::capture::WorktreeDocs) -> work::Worktree {
    serde_json::from_value(
        value(
            node,
            docs.worktree.as_ref().unwrap(),
            work::Worktree::ID,
            "main",
        )
        .await,
    )
    .unwrap()
}

/// A batch of four ops numbered from 254 crosses the 256-op boundary: a second op-log document
/// opens, the worktree datum lists both with the sequence number each starts at, and a retry of
/// the batch changes nothing; the next batch appends to the open document.
#[tokio::test]
async fn ops_roll_over_into_a_second_op_log_document() {
    let node = boot().await;
    let session = start(&node).await;
    let root = session.repo.display().to_string();
    std::fs::write(session.repo.join("a.txt"), "one\nTWO\n").unwrap();
    let prepared =
        intxt_capture::host::prepare(&session.hook("Stop", None), &session.state).unwrap();
    let mut batch = prepared.batch.clone();
    let worktree = &mut batch.worktrees[0];
    worktree.next_seq = 254;
    let op = worktree.ops[0].clone();
    worktree.ops = vec![op.clone(); 4];

    let first = session.send(&node, &batch, "hook-1").await;
    let docs = first.worktrees[&root].docs.clone();
    assert_eq!(first.worktrees[&root].next_seq, 258);
    let worktree = worktree_of(&node, &docs).await;
    let froms: Vec<u64> = worktree.op_logs.iter().map(|log| log.from).collect();
    assert_eq!(froms, [254, 256]);
    let (log0, log1) = (&worktree.op_logs[0].doc, &worktree.op_logs[1].doc);
    assert_ne!(log0, log1);
    for (seq, log) in [(254, log0), (255, log0), (256, log1), (257, log1)] {
        assert_eq!(
            value(&node, log, work::WorktreeOp::ID, &seq.to_string()).await["kind"],
            "edit",
            "op {seq}"
        );
    }
    assert!(is_absent(&node, log0, work::WorktreeOp::ID, "256").await);
    assert!(is_absent(&node, log1, work::WorktreeOp::ID, "255").await);

    // A retry answers the same and writes nothing.
    assert_eq!(session.send(&node, &batch, "hook-1").await, first);
    let again = worktree_of(&node, &docs).await;
    assert_eq!(again, worktree);
    assert!(is_absent(&node, log1, work::WorktreeOp::ID, "258").await);

    // The next batch continues in the open document.
    let mut next = prepared.batch.clone();
    next.worktrees[0].docs = docs.clone();
    next.worktrees[0].next_seq = 258;
    next.worktrees[0].ops.truncate(1);
    let second = session.send(&node, &next, "hook-2").await;
    assert_eq!(second.worktrees[&root].next_seq, 259);
    assert_eq!(worktree_of(&node, &docs).await.op_logs.len(), 2);
    value(&node, log1, work::WorktreeOp::ID, "258").await;
    prepared.commit(second).unwrap();
    node.stop().await;
}

/// A subagent is its own session document: its `parent` names the main session's document, and
/// its turns are its own.
#[tokio::test]
async fn a_subagent_is_recorded_as_a_session_of_its_own() {
    let node = boot().await;
    let mut session = start(&node).await;
    session.append(&[
        record(
            "user",
            "u1",
            None,
            json!({"role": "user", "content": "explore"}),
            json!({}),
        ),
        record(
            "assistant",
            "a1",
            Some("u1"),
            json!({"id": "m-t1", "content": [{"type": "tool_use", "id": "t1", "name": "Agent",
                "input": {"description": "look"}}]}),
            json!({}),
        ),
    ]);
    let start = session
        .run(&node, session.hook("SessionStart", None), false)
        .await
        .unwrap();
    let main_doc = start.session.unwrap();

    // The call returns, naming the agent; its transcript sits beside the session's.
    session.append(&[record(
        "user",
        "r1",
        Some("a1"),
        json!({"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "found"}]}),
        json!({"toolUseResult": {"status": "completed", "agentId": "ab12"}}),
    )]);
    let sub_path = session
        .transcript
        .with_extension("")
        .join("subagents/agent-ab12.jsonl");
    let sub = |uuid: &str, parent: Option<&str>, message: Value, kind: &str| {
        record(
            kind,
            uuid,
            parent,
            message,
            json!({"agentId": "ab12", "isSidechain": true}),
        )
    };
    session.append_to(
        &sub_path,
        &[
            sub(
                "s1",
                None,
                json!({"role": "user", "content": "look around"}),
                "user",
            ),
            sub(
                "s2",
                Some("s1"),
                json!({"id": "sm1", "content": [{"type": "text", "text": "done"}]}),
                "assistant",
            ),
        ],
    );
    let stopped = session
        .run(&node, session.hook("Stop", None), true)
        .await
        .expect("the subagent's import is recorded");
    assert_eq!(
        stopped.session,
        Some(main_doc.clone()),
        "the main session's document is answered"
    );
    let agent_doc = stopped.subagents["ab12"].clone();
    assert_ne!(agent_doc, main_doc);
    let agent = value(&node, &agent_doc, work::AgentSession::ID, "main").await;
    assert_eq!(agent["sessionId"], "agent-ab12");
    assert_eq!(
        agent["parent"],
        format!("dtxt:datum:{main_doc}/org.example.intxt.work.agentSession/main")
    );
    let turn = value(&node, &agent_doc, work::PromptTurn::ID, "s1").await;
    assert_eq!(turn["messages"][0]["text"], "look around");
    assert_eq!(turn["messages"][1]["text"], "done");

    // The host remembered the subagent's document: nothing new is nothing to record.
    assert!(
        session
            .run(&node, session.hook("Stop", None), false)
            .await
            .is_none()
    );
    node.stop().await;
}

/// Ref moves: a checkout, a branch forced while HEAD was elsewhere, and a reflog that expired
/// between two looks, each as the op of its kind with the `gitRef` datums moved to match.
#[tokio::test]
async fn ref_moves_and_a_reflog_gap_are_recorded() {
    let node = boot().await;
    let mut session = start(&node).await;
    let root = session.repo.display().to_string();
    let first_sha = session.root_commit.clone();
    let seen = session
        .run(&node, session.hook("Stop", None), false)
        .await
        .expect("the first look records the clone");
    let docs = seen.worktrees[&root].docs.clone();
    let clone_doc = docs.clone.clone().unwrap();
    let clone_slot = {
        let worktree = worktree_of(&node, &docs).await;
        String::from_utf8(worktree.clone.key().slot_bytes().to_vec()).unwrap()
    };
    let target_of = async |name: &str| {
        value(
            &node,
            &clone_doc,
            work::GitRef::ID,
            &format!("{clone_slot}/{name}"),
        )
        .await["target"]
            .clone()
    };
    assert_eq!(target_of("refs/heads/main").await, first_sha);
    let ops_since = async |from: u64, to: u64| {
        let worktree = worktree_of(&node, &docs).await;
        let log = &worktree.op_logs[0].doc;
        let mut ops = Vec::new();
        for seq in from..to {
            ops.push(value(&node, log, work::WorktreeOp::ID, &seq.to_string()).await);
        }
        ops
    };

    // A checkout of a new branch; another branch is made beside it.
    git(&session.repo, &["checkout", "-q", "-b", "feat"]);
    git(&session.repo, &["branch", "other"]);
    let checkout = session
        .run(&node, session.hook("Stop", None), false)
        .await
        .unwrap();
    let after_checkout = checkout.worktrees[&root].next_seq;
    let ops = ops_since(0, after_checkout).await;
    let checkout_op = ops
        .iter()
        .find(|op| op["kind"] == "checkout")
        .unwrap_or_else(|| panic!("no checkout op: {ops:?}"));
    assert_eq!(checkout_op["git"]["ref"], "refs/heads/feat");
    assert_eq!(target_of("refs/heads/feat").await, first_sha);
    assert_eq!(target_of("refs/heads/other").await, first_sha);

    // A commit on feat, and `other` forced onto it while it is not checked out.
    git(
        &session.repo,
        &["commit", "-q", "--allow-empty", "-m", "second"],
    );
    let second_sha = git(&session.repo, &["rev-parse", "HEAD"]);
    git(&session.repo, &["branch", "-q", "-f", "other", "feat"]);
    let forced = session
        .run(&node, session.hook("Stop", None), false)
        .await
        .unwrap();
    let after_forced = forced.worktrees[&root].next_seq;
    let ops = ops_since(after_checkout, after_forced).await;
    assert!(ops.iter().any(|op| op["kind"] == "commit"), "{ops:?}");
    let moved = &ops
        .iter()
        .find(|op| op["kind"] == "refMove" && op["git"]["ref"] == "refs/heads/other")
        .unwrap_or_else(|| panic!("no refMove of other: {ops:?}"))["git"];
    assert_eq!(moved["ref"], "refs/heads/other");
    assert_eq!(moved["from"], first_sha);
    assert_eq!(moved["to"], second_sha);
    assert!(
        moved["reflog"]
            .as_str()
            .is_some_and(|text| !text.is_empty()),
        "{moved}"
    );
    assert_eq!(target_of("refs/heads/other").await, second_sha);
    assert_eq!(target_of("refs/heads/feat").await, second_sha);

    // The reflog expires between two looks: one synthetic move, with no reflog message.
    git(
        &session.repo,
        &["commit", "-q", "--allow-empty", "-m", "third"],
    );
    let third_sha = git(&session.repo, &["rev-parse", "HEAD"]);
    git(
        &session.repo,
        &["reflog", "expire", "--expire=now", "--all"],
    );
    let gap = session
        .run(&node, session.hook("Stop", None), false)
        .await
        .unwrap();
    let ops = ops_since(after_forced, gap.worktrees[&root].next_seq).await;
    assert_eq!(ops.len(), 1, "{ops:?}");
    assert_eq!(ops[0]["kind"], "refMove");
    assert_eq!(ops[0]["git"]["from"], second_sha);
    assert_eq!(ops[0]["git"]["to"], third_sha);
    assert!(ops[0]["git"].get("reflog").is_none(), "{}", ops[0]["git"]);
    assert_eq!(target_of("refs/heads/feat").await, third_sha);
    node.stop().await;
}
