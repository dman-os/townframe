//! A capture session end to end (ADR 039 §4): scripted hooks over a real git repository and a
//! synthetic transcript, observed by `intxt_capture`'s host half and recorded through the
//! nodule's `recordObservations`, whose answer the host commits as its state.

use super::*;
use big_cabinet_core::ActorUrl;
use big_cabinet_core::drawer::protocol::DrawerId;
use intxt_capture::hook::Hook;
use intxt_core::capture::Batch;
use intxt_core::capture::Content;
use intxt_core::capture::RecordedIds;
use intxt_handlers::RecordObservations;
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
        use std::io::Write;
        let mut file = std::fs::File::options()
            .create(true)
            .append(true)
            .open(&self.transcript)
            .unwrap();
        for record in records {
            writeln!(file, "{record}").unwrap();
        }
    }

    /// Sends `batch` to `recordObservations` under idempotency key `key`.
    async fn send(&self, node: &TestNode, batch: &Batch, key: &str) -> RecordedIds {
        let output = node
            .invoke(
                RecordObservations::XRPC.nsid,
                serde_json::to_value(RecordObservations {
                    drawer: self.drawer.clone(),
                    machine: self.machine.clone(),
                    batch: batch.clone(),
                })
                .unwrap(),
                InvocationContext {
                    idempotency_key: Some(key.to_owned()),
                    http: None,
                },
                RequestContext::root(),
            )
            .await
            .unwrap_or_else(|error| panic!("recordObservations refused: {error:?}"));
        serde_json::from_value(output).unwrap()
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
        .call(intxt_handlers::CreateIntxt {
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
        .call(intxt_handlers::CreateIntxt {
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
