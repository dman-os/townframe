//! A scripted hook sequence over a real git repository and a synthetic transcript.

use super::*;
use crate::hook::Hook;
use crate::testing::Repo;
use crate::testing::git_in;
use intxt_core::capture::ChangeKind;
use intxt_core::capture::RecordedWorktree;
use serde_json::Value;
use serde_json::json;
use std::io::Write;

const DOC: &str = "z11111111111111111111111111111111";

struct Env {
    repo: Repo,
    _claude: tempfile::TempDir,
    transcript: PathBuf,
    session_state: PathBuf,
}

impl Env {
    fn new() -> Self {
        let repo = Repo::new();
        repo.write("a.txt", "one\n");
        repo.commit_all("first");
        let claude = tempfile::tempdir().unwrap();
        let transcript = claude.path().canonicalize().unwrap().join("proj/s1.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let session_state = claude.path().join("node/s1.json");
        Self {
            repo,
            _claude: claude,
            transcript,
            session_state,
        }
    }

    fn hook(&self, event: &str, extra: Value) -> Hook {
        let mut input = json!({
            "session_id": "s1",
            "transcript_path": self.transcript,
            "cwd": self.repo.root,
            "hook_event_name": event
        });
        input
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        Hook::parse(input.to_string().as_bytes()).unwrap()
    }

    fn tool_hook(&self, event: &str, tool: &str, id: &str, input: Value) -> Hook {
        self.hook(
            event,
            json!({"tool_name": tool, "tool_use_id": id, "tool_input": input}),
        )
    }

    fn append(&self, path: &Path, records: &[Value]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = File::options()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        for record in records {
            writeln!(file, "{record}").unwrap();
        }
    }

    fn state_bytes(&self) -> Vec<Option<Vec<u8>>> {
        let git_dir = self.repo.root.join(".git/intxt/state.json");
        vec![
            std::fs::read(&self.session_state).ok(),
            std::fs::read(git_dir).ok(),
        ]
    }
}

fn user(uuid: &str, parent: Option<&str>, text: &str) -> Value {
    json!({"type": "user", "uuid": uuid, "parentUuid": parent, "timestamp": "2026-11-10T10:00:00Z",
           "cwd": "/work/repo", "sessionId": "s1", "message": {"role": "user", "content": text}})
}

fn tool_use(uuid: &str, parent: &str, id: &str, name: &str, input: Value) -> Value {
    json!({"type": "assistant", "uuid": uuid, "parentUuid": parent, "timestamp": "2026-11-10T10:00:01Z",
           "cwd": "/work/repo", "sessionId": "s1",
           "message": {"id": format!("m-{id}"), "content": [{"type": "tool_use", "id": id, "name": name, "input": input}]}})
}

fn tool_result(uuid: &str, parent: &str, id: &str, text: &str, result: Value) -> Value {
    json!({"type": "user", "uuid": uuid, "parentUuid": parent, "timestamp": "2026-11-10T10:00:02Z",
           "cwd": "/work/repo", "sessionId": "s1", "toolUseResult": result,
           "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": id, "content": text}]}})
}

/// The batch with its wall-clock times fixed, to compare two preparations of one state.
fn normalized(batch: &Batch) -> Batch {
    let mut batch = batch.clone();
    for worktree in &mut batch.worktrees {
        for op in &mut worktree.ops {
            op.at = Timestamp::UNIX_EPOCH;
        }
    }
    batch
}

fn recorded_for(prepared: &Prepared, next_seq: u64) -> RecordedIds {
    RecordedIds {
        session: Some(DOC.to_owned()),
        subagents: BTreeMap::new(),
        worktrees: prepared
            .batch
            .worktrees
            .iter()
            .map(|worktree| {
                (
                    worktree.identity.root.clone(),
                    RecordedWorktree {
                        docs: WorktreeDocs {
                            worktree: Some(format!("w-{}", worktree.identity.worktree_id)),
                            clone: Some("c".into()),
                            repo: Some("r".into()),
                            extra: Default::default(),
                        },
                        next_seq,
                        extra: Default::default(),
                    },
                )
            })
            .collect(),
        extra: Default::default(),
    }
}

#[test]
fn a_bash_call_that_edits_a_file_becomes_a_caused_op_and_nothing_persists_without_commit() {
    let env = Env::new();
    env.append(
        &env.transcript,
        &[
            user("u1", None, "sed it"),
            tool_use(
                "a1",
                "u1",
                "t1",
                "Bash",
                json!({"command": "sed -i s/one/two/ a.txt"}),
            ),
        ],
    );

    // SessionStart records the session and the baseline of the worktree.
    let start = prepare(&env.hook("SessionStart", json!({})), &env.session_state).unwrap();
    assert_eq!(start.batch.session.session_id, "s1");
    assert_eq!(start.batch.session.doc, None);
    assert_eq!(
        start
            .batch
            .session
            .imported
            .session
            .as_ref()
            .unwrap()
            .session_id,
        "s1"
    );
    assert_eq!(
        start
            .batch
            .session
            .imported
            .turns
            .keys()
            .collect::<Vec<_>>(),
        ["u1"]
    );
    assert_eq!(start.batch.worktrees.len(), 1);
    let worktree = &start.batch.worktrees[0];
    assert_eq!(worktree.docs, WorktreeDocs::default());
    assert_eq!(worktree.next_seq, 0);
    assert!(worktree.ops.is_empty());
    assert_eq!(worktree.commits.len(), 1);
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();

    // PreToolUse only stores the snapshot: nothing to record.
    let pre = prepare(
        &env.hook(
            "PreToolUse",
            json!({"tool_name": "Bash", "tool_use_id": "t1", "tool_input": {"command": "sed"}}),
        ),
        &env.session_state,
    )
    .unwrap();
    assert!(pre.batch.is_empty());
    assert_eq!(pre.batch.session.doc.as_deref(), Some(DOC));
    pre.commit(RecordedIds::default()).unwrap();

    env.repo.write("a.txt", "two\n");

    // PostToolUse: the change is one edit op caused by the call.
    let post_hook = env.tool_hook("PostToolUse", "Bash", "t1", json!({"command": "sed"}));
    let before = env.state_bytes();
    let first = prepare(&post_hook, &env.session_state).unwrap();
    let worktree = &first.batch.worktrees[0];
    assert_eq!(
        worktree
            .docs
            .worktree
            .as_deref()
            .map(|w| w.starts_with("w-")),
        Some(true)
    );
    assert_eq!(worktree.ops.len(), 1);
    let op = &worktree.ops[0];
    assert_eq!(op.kind, OpKind::Edit);
    assert_eq!(
        op.cause,
        Some(Cause {
            agent_id: None,
            turn: Some("u1".into()),
            tool_use_id: "t1".into(),
            extra: Default::default(),
        })
    );
    assert_eq!(op.changes.len(), 1);
    assert_eq!(op.changes[0].path, "a.txt");
    assert_eq!(op.changes[0].kind, ChangeKind::Modified);
    let first_batch = normalized(&first.batch);

    // Dropped, not committed: nothing persisted, locks released, and the same hook prepares the
    // same batch again.
    drop(first);
    assert_eq!(env.state_bytes(), before);
    let second = prepare(&post_hook, &env.session_state).unwrap();
    assert_eq!(normalized(&second.batch), first_batch);
    let recorded = recorded_for(&second, 1);
    second.commit(recorded).unwrap();
    assert_ne!(env.state_bytes(), before);

    // Stop: nothing new anywhere.
    let stop = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    assert!(stop.batch.is_empty(), "{:?}", stop.batch);
    stop.commit(RecordedIds::default()).unwrap();

    // A change outside any tool call is an op without a cause, numbered after the last.
    env.repo.write("a.txt", "three\n");
    let stop = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    let worktree = &stop.batch.worktrees[0];
    assert_eq!(worktree.next_seq, 1);
    assert_eq!(worktree.ops.len(), 1);
    assert_eq!(worktree.ops[0].cause, None);
}

#[test]
fn changes_between_the_last_look_and_a_tool_call_are_not_the_tools() {
    let env = Env::new();
    env.append(
        &env.transcript,
        &[
            user("u1", None, "go"),
            tool_use("a1", "u1", "t1", "Bash", json!({"command": "x"})),
        ],
    );
    let start = prepare(&env.hook("SessionStart", json!({})), &env.session_state).unwrap();
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();

    env.repo.write("operator.txt", "typed by hand\n");
    let pre = prepare(
        &env.tool_hook("PreToolUse", "Bash", "t1", json!({})),
        &env.session_state,
    )
    .unwrap();
    pre.commit(RecordedIds::default()).unwrap();
    env.repo.write("tool.txt", "made by the call\n");
    let post = prepare(
        &env.tool_hook("PostToolUse", "Bash", "t1", json!({})),
        &env.session_state,
    )
    .unwrap();
    let ops = &post.batch.worktrees[0].ops;
    assert_eq!(ops.len(), 2);
    assert_eq!(
        (ops[0].cause.is_none(), ops[0].changes[0].path.as_str()),
        (true, "operator.txt")
    );
    assert_eq!(
        (ops[1].cause.is_some(), ops[1].changes[0].path.as_str()),
        (true, "tool.txt")
    );
}

#[test]
fn a_session_that_touches_two_worktrees_observes_both() {
    let env = Env::new();
    let linked_root = env.repo.root.parent().unwrap().join("linked");
    env.repo.git(&[
        "worktree",
        "add",
        "-q",
        "-b",
        "other",
        linked_root.to_str().unwrap(),
    ]);
    env.append(
        &env.transcript,
        &[
            user("u1", None, "go"),
            tool_use("a1", "u1", "t1", "Edit", json!({})),
        ],
    );

    let start = prepare(&env.hook("SessionStart", json!({})), &env.session_state).unwrap();
    assert_eq!(start.batch.worktrees.len(), 1);
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();

    // The tool's path is in the other worktree: it joins the session.
    let input = json!({"file_path": linked_root.join("a.txt")});
    let pre = prepare(
        &env.tool_hook("PreToolUse", "Edit", "t1", input.clone()),
        &env.session_state,
    )
    .unwrap();
    pre.commit(RecordedIds::default()).unwrap();
    std::fs::write(linked_root.join("a.txt"), "edited in the linked worktree\n").unwrap();
    let post = prepare(
        &env.tool_hook("PostToolUse", "Edit", "t1", input),
        &env.session_state,
    )
    .unwrap();
    assert_eq!(post.batch.worktrees.len(), 1);
    let linked = &post.batch.worktrees[0];
    assert_eq!(Path::new(&linked.identity.root), linked_root);
    assert_eq!(linked.ops.len(), 1);
    assert_eq!(linked.ops[0].changes[0].path, "a.txt");
    assert!(linked.ops[0].cause.is_some());
    let recorded = recorded_for(&post, 1);
    post.commit(recorded).unwrap();

    // The session remembers it: a later hook in the main worktree still sees the linked one.
    git_in(&linked_root, &["checkout", "-q", "--", "a.txt"]);
    std::fs::write(linked_root.join("b.txt"), "b\n").unwrap();
    let stop = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    let roots: Vec<&Path> = stop
        .batch
        .worktrees
        .iter()
        .map(|w| Path::new(&w.identity.root))
        .collect();
    assert_eq!(roots, [linked_root.as_path()]);
    assert_eq!(stop.batch.worktrees[0].next_seq, 1);
}

#[test]
fn a_prepared_hook_holds_the_locks_until_it_is_dropped() {
    let env = Env::new();
    env.append(&env.transcript, &[user("u1", None, "go")]);
    let try_lock = |path: &Path| {
        File::options()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .try_lock()
    };
    let worktree_lock = env.repo.root.join(".git/intxt/lock");
    let mut session_lock = env.session_state.as_os_str().to_owned();
    session_lock.push(".lock");
    let session_lock = PathBuf::from(session_lock);

    let prepared = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    assert!(matches!(
        try_lock(&worktree_lock),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    assert!(matches!(
        try_lock(&session_lock),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(prepared);
    assert!(try_lock(&worktree_lock).is_ok());
    assert!(try_lock(&session_lock).is_ok());

    // And a commit releases them too.
    let prepared = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    let recorded = recorded_for(&prepared, 0);
    prepared.commit(recorded).unwrap();
    assert!(try_lock(&worktree_lock).is_ok());
    assert!(try_lock(&session_lock).is_ok());
}

#[test]
fn a_subagent_transcript_is_imported_once_the_session_has_its_document() {
    let env = Env::new();
    env.append(
        &env.transcript,
        &[
            user("u1", None, "explore"),
            tool_use("a1", "u1", "t1", "Agent", json!({"description": "look"})),
        ],
    );
    let start = prepare(&env.hook("SessionStart", json!({})), &env.session_state).unwrap();
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();

    // The call returns, naming the agent; its transcript sits beside the session's.
    env.append(
        &env.transcript,
        &[tool_result(
            "r1",
            "a1",
            "t1",
            "found",
            json!({"status": "completed", "agentId": "ab12"}),
        )],
    );
    let sub = |uuid: &str, parent: Option<&str>, message: Value, kind: &str| {
        json!({"type": kind, "uuid": uuid, "parentUuid": parent, "timestamp": "2026-11-10T10:00:03Z",
               "cwd": "/work/repo", "sessionId": "s1", "agentId": "ab12", "isSidechain": true, "message": message})
    };
    let sub_path = env
        .transcript
        .with_extension("")
        .join("subagents/agent-ab12.jsonl");
    env.append(
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
    let prepared = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    assert_eq!(prepared.batch.session.subagents.len(), 1);
    let agent = &prepared.batch.session.subagents[0];
    assert_eq!(agent.agent_id, "ab12");
    assert_eq!(agent.doc, None);
    let session = agent.imported.session.as_ref().unwrap();
    assert_eq!(session.session_id, "agent-ab12");
    assert_eq!(
        serde_json::to_value(&session.parent).unwrap(),
        json!(format!(
            "dtxt:datum:{DOC}/org.example.intxt.work.agentSession/main"
        ))
    );
    assert_eq!(agent.imported.turns.len(), 1);
    let mut recorded = RecordedIds::default();
    recorded.subagents.insert("ab12".into(), "zsub".into());
    prepared.commit(recorded).unwrap();

    let next = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
    assert_eq!(next.batch.session.subagents[0].doc.as_deref(), Some("zsub"));
    assert!(next.batch.session.subagents[0].imported.turns.is_empty());
}

#[test]
fn a_post_hook_before_the_transcript_shows_the_call_records_an_uncaused_op_and_counts_it() {
    let env = Env::new();
    env.append(&env.transcript, &[user("u1", None, "go")]);
    let start = prepare(&env.hook("SessionStart", json!({})), &env.session_state).unwrap();
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();
    let pre = prepare(
        &env.tool_hook("PreToolUse", "Bash", "t9", json!({})),
        &env.session_state,
    )
    .unwrap();
    pre.commit(RecordedIds::default()).unwrap();
    env.repo.write("late.txt", "x\n");

    // The transcript has no `t9` yet.
    let post = prepare(
        &env.tool_hook("PostToolUse", "Bash", "t9", json!({})),
        &env.session_state,
    )
    .unwrap();
    assert_eq!(post.batch.uncaused_tool_calls, 1);
    assert_eq!(post.batch.worktrees[0].ops.len(), 1);
    assert_eq!(post.batch.worktrees[0].ops[0].cause, None);
    assert_eq!(post.batch.worktrees[0].ops[0].changes[0].path, "late.txt");
}

#[test]
fn stop_and_session_end_drop_pre_snapshots_of_calls_that_never_reported() {
    let env = Env::new();
    env.append(&env.transcript, &[user("u1", None, "go")]);
    let pending = |env: &Env| -> usize {
        let state: Value = serde_json::from_slice(
            &std::fs::read(env.repo.root.join(".git/intxt/state.json")).unwrap(),
        )
        .unwrap();
        state["pre"].as_object().unwrap().len()
    };
    for event in ["Stop", "SessionEnd"] {
        let pre = prepare(
            &env.tool_hook("PreToolUse", "Bash", "lost", json!({})),
            &env.session_state,
        )
        .unwrap();
        pre.commit(RecordedIds::default()).unwrap();
        assert_eq!(pending(&env), 1);
        let end = prepare(&env.hook(event, json!({})), &env.session_state).unwrap();
        let recorded = recorded_for(&end, 0);
        end.commit(recorded).unwrap();
        assert_eq!(pending(&env), 0, "{event}");
    }
}

fn acp_session(session_id: &str) -> SessionBatch {
    SessionBatch {
        session_id: session_id.to_owned(),
        doc: None,
        imported: Default::default(),
        subagents: Vec::new(),
        extra: Default::default(),
    }
}

#[test]
fn an_acp_window_observes_the_cwd_worktree_and_resumes_from_its_state() {
    let env = Env::new();
    let none = BTreeSet::new();
    // The start of a session: first sight of the worktree, with what already differs.
    let start = prepare_acp(
        acp_session("acp1"),
        env.session_state.parent().unwrap(),
        &env.repo.root,
        &none,
        &Window::Boundary,
    )
    .unwrap();
    assert!(start.batch.worktrees.is_empty() || start.batch.worktrees[0].ops.is_empty());
    let roots: BTreeSet<PathBuf> = start.worktree_roots().map(Path::to_owned).collect();
    assert_eq!(roots, BTreeSet::from([env.repo.root.clone()]));
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();

    // A turn's edits: one op without a cause, with its sequence continuing the stored one.
    env.repo.write("a.txt", "two\n");
    let turn = prepare_acp(
        acp_session("acp1"),
        env.session_state.parent().unwrap(),
        &env.repo.root,
        &roots,
        &Window::Boundary,
    )
    .unwrap();
    let [worktree] = &turn.batch.worktrees[..] else {
        panic!("the edited worktree is batched");
    };
    assert_eq!(worktree.ops.len(), 1);
    assert_eq!(worktree.ops[0].cause, None);
    assert_eq!(worktree.ops[0].changes[0].path, "a.txt");
    assert_eq!(turn.batch.uncaused_tool_calls, 0);
    let recorded = recorded_for(&turn, 1);
    turn.commit(recorded).unwrap();

    // Nothing more happened: nothing to record.
    let idle = prepare_acp(
        acp_session("acp1"),
        env.session_state.parent().unwrap(),
        &env.repo.root,
        &roots,
        &Window::Boundary,
    )
    .unwrap();
    assert!(idle.batch.worktrees.is_empty());
}

#[test]
fn acp_tool_call_windows_give_one_caused_op_each_and_the_rest_goes_to_the_turn_end() {
    use crate::acp::ToolWindow;
    let env = Env::new();
    let none = BTreeSet::new();
    let start = prepare_acp(
        acp_session("acp1"),
        env.session_state.parent().unwrap(),
        &env.repo.root,
        &none,
        &Window::Boundary,
    )
    .unwrap();
    let roots: BTreeSet<PathBuf> = start.worktree_roots().map(Path::to_owned).collect();
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();

    let tool = |id: &str| ToolWindow {
        cause: Cause {
            agent_id: None,
            turn: Some("turn-1".to_owned()),
            tool_use_id: id.to_owned(),
            extra: Default::default(),
        },
        paths: Vec::new(),
    };
    let mut next_seq = 0;
    let mut window = |env: &Env, id: &str, file: &str, content: &str| -> Batch {
        let open = prepare_acp(
            acp_session("acp1"),
            env.session_state.parent().unwrap(),
            &env.repo.root,
            &roots,
            &Window::Open(tool(id)),
        )
        .unwrap();
        assert!(open.batch.is_empty(), "opening a window records nothing");
        open.commit(RecordedIds::default()).unwrap();
        env.repo.write(file, content);
        let close = prepare_acp(
            acp_session("acp1"),
            env.session_state.parent().unwrap(),
            &env.repo.root,
            &roots,
            &Window::Close(tool(id)),
        )
        .unwrap();
        let batch = close.batch.clone();
        next_seq += batch.worktrees[0].ops.len() as u64;
        let recorded = recorded_for(&close, next_seq);
        close.commit(recorded).unwrap();
        batch
    };
    let first = window(&env, "call_1", "one.txt", "first\n");
    let second = window(&env, "call_2", "two.txt", "second\n");
    for (batch, id, path, content) in [
        (&first, "call_1", "one.txt", "first\n"),
        (&second, "call_2", "two.txt", "second\n"),
    ] {
        let [worktree] = &batch.worktrees[..] else {
            panic!("one worktree");
        };
        let [op] = &worktree.ops[..] else {
            panic!("one op per window");
        };
        let cause = op.cause.as_ref().expect("the call caused the op");
        assert_eq!(
            (cause.tool_use_id.as_str(), cause.turn.as_deref()),
            (id, Some("turn-1"))
        );
        let [change] = &op.changes[..] else {
            panic!("only this call's file changed: {:?}", op.changes);
        };
        assert_eq!(change.path, path);
        let blob = change.blob.as_deref().unwrap();
        assert_eq!(
            env.repo.git(&["cat-file", "blob", blob]),
            content.trim_end()
        );
        assert_eq!(batch.uncaused_tool_calls, 0);
    }
    assert_eq!(
        second.worktrees[0].next_seq, 1,
        "sequence continues across windows"
    );

    // Outside any window: the turn-end observation records it, without a cause.
    env.repo.write("between.txt", "x\n");
    let end = prepare_acp(
        acp_session("acp1"),
        env.session_state.parent().unwrap(),
        &env.repo.root,
        &roots,
        &Window::Boundary,
    )
    .unwrap();
    let [op] = &end.batch.worktrees[0].ops[..] else {
        panic!("one op");
    };
    assert_eq!(op.cause, None);
    assert_eq!(
        op.changes
            .iter()
            .map(|c| c.path.as_str())
            .collect::<Vec<_>>(),
        ["between.txt"]
    );
}

#[test]
fn acp_sessions_reusing_a_tool_call_id_keep_their_own_pre_snapshots() {
    use crate::acp::ToolWindow;
    let env = Env::new();
    let roots = BTreeSet::new();
    let window = |id: &str| ToolWindow {
        cause: Cause {
            agent_id: None,
            turn: Some("t".to_owned()),
            tool_use_id: id.to_owned(),
            extra: Default::default(),
        },
        paths: Vec::new(),
    };
    let pending = || -> usize {
        let state: Value = serde_json::from_slice(
            &std::fs::read(env.repo.root.join(".git/intxt/state.json")).unwrap(),
        )
        .unwrap();
        state["pre"].as_object().unwrap().len()
    };
    for session in ["acp1", "acp2"] {
        let open = prepare_acp(
            acp_session(session),
            env.session_state.parent().unwrap(),
            &env.repo.root,
            &roots,
            &Window::Open(window("call")),
        )
        .unwrap();
        open.commit(RecordedIds::default()).unwrap();
    }
    assert_eq!(pending(), 2, "one pre-snapshot per session, same call id");
    let close = prepare_acp(
        acp_session("acp1"),
        env.session_state.parent().unwrap(),
        &env.repo.root,
        &roots,
        &Window::Close(window("call")),
    )
    .unwrap();
    let recorded = recorded_for(&close, 0);
    close.commit(recorded).unwrap();
    assert_eq!(
        pending(),
        1,
        "closing one session's call leaves the other's"
    );
}

/// The blob ids every op of `batch` names.
fn blobs_of(batch: &Batch) -> BTreeSet<String> {
    batch
        .worktrees
        .iter()
        .flat_map(|worktree| &worktree.ops)
        .flat_map(|op| &op.changes)
        .filter_map(|change| change.blob.clone())
        .collect()
}

#[test]
fn recorded_blobs_stay_reachable_from_the_keep_ref_and_survive_gc() {
    let env = Env::new();
    env.append(&env.transcript, &[user("u1", None, "edit")]);
    let start = prepare(&env.hook("SessionStart", json!({})), &env.session_state).unwrap();
    let worktree_id = start.batch.worktrees[0].identity.worktree_id.clone();
    let recorded = recorded_for(&start, 0);
    start.commit(recorded).unwrap();
    let keep_ref = format!("refs/intxt/keep/{worktree_id}");
    // Nothing observed, so no objects were written and there is no keep ref.
    assert_eq!(env.repo.git(&["for-each-ref", "refs/intxt"]), "");

    // Two observations that each write objects: the second keep commit chains on the first.
    let mut recorded_blobs = BTreeSet::new();
    let mut next_seq = 0;
    for content in ["two\n", "three\nwith an untracked sibling\n"] {
        env.repo.write("a.txt", content);
        env.repo.write(
            &format!("untracked-{next_seq}.txt"),
            format!("sibling of {content}"),
        );
        let stop = prepare(&env.hook("Stop", json!({})), &env.session_state).unwrap();
        let blobs = blobs_of(&stop.batch);
        assert_eq!(blobs.len(), 2, "{blobs:?}");
        recorded_blobs.extend(blobs);
        next_seq += stop.batch.worktrees[0].ops.len() as u64;
        let recorded = recorded_for(&stop, next_seq);
        stop.commit(recorded).unwrap();
    }
    assert_eq!(recorded_blobs.len(), 4);
    assert_eq!(
        env.repo.git(&["rev-list", "--count", &keep_ref]),
        "2",
        "one keep commit per observation that wrote objects"
    );
    let reachable = env.repo.git(&["rev-list", "--objects", &keep_ref]);
    for blob in &recorded_blobs {
        assert!(
            reachable.contains(blob.as_str()),
            "{blob} is not under {keep_ref}"
        );
    }

    // Unreachable loose objects are what gc prunes; these are reachable from the keep ref.
    env.repo.git(&["gc", "--prune=now", "--quiet"]);
    for blob in &recorded_blobs {
        env.repo.git(&["cat-file", "-e", blob]);
    }
}
