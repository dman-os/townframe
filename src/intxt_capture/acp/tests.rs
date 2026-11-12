use super::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

const SESSION: &str = include_str!("fixtures/session.trace");
const LOAD: &str = include_str!("fixtures/load.trace");

fn at() -> Timestamp {
    "2026-05-01T10:00:00Z".parse().unwrap()
}

/// A trace's lines with their direction; `#` lines are comments.
fn lines(trace: &str) -> Vec<(Direction, String)> {
    trace
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (arrow, json) = line.split_at(2);
            let direction = match arrow {
                "> " => Direction::ToAgent,
                "< " => Direction::FromAgent,
                _ => panic!("a trace line starts with `> ` or `< `: {line}"),
            };
            (direction, format!("{json}\n"))
        })
        .collect()
}

fn play(recorder: &mut Recorder, trace: &str) -> Vec<Emit> {
    lines(trace)
        .iter()
        .flat_map(|(direction, line)| recorder.feed(*direction, at(), line.as_bytes()).unwrap())
        .collect()
}

/// The emits that end a window nothing caused: the session's start and its turns' ends.
fn boundaries(emits: Vec<Emit>) -> Vec<Emit> {
    emits
        .into_iter()
        .filter(|emit| emit.window == Window::Boundary)
        .collect()
}

fn assert_round_trips<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(datum: &T) {
    let value = serde_json::to_value(datum).unwrap();
    let back: T = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(&back, datum);
}

fn assert_all_round_trip(emit: &Emit) {
    let imported = &emit.session.imported;
    imported.session.iter().for_each(assert_round_trips);
    imported.turns.values().for_each(assert_round_trips);
    imported.plan.iter().for_each(assert_round_trips);
}

#[test]
fn a_session_becomes_its_datum_then_one_batch_per_turn() {
    let mut recorder = Recorder::new();
    let emits = boundaries(play(&mut recorder, SESSION));
    emits.iter().for_each(assert_all_round_trip);
    assert_eq!(emits.len(), 3, "the session, then two turns");
    assert!(recorder.finish().is_empty());

    let start = &emits[0];
    assert_eq!(start.cwd, PathBuf::from("/work/repo"));
    assert_eq!(start.session.session_id, "sess_abc123def456");
    let session = start.session.imported.session.as_ref().unwrap();
    assert_eq!(session.harness, "example-agent");
    assert_eq!(session.cwd, "/work/repo");
    assert_eq!(session.additional_directories, ["/work/lib"]);
    assert!(start.session.imported.turns.is_empty());

    // The first turn: the prompt, the message chunks joined, no thought, one call per id.
    let first = &emits[1].session.imported;
    // The title changed in this turn, so the datum comes again.
    assert_eq!(
        first.session.as_ref().unwrap().title.as_deref(),
        Some("Config loader")
    );
    let [(id, turn)] = first.turns.iter().collect::<Vec<_>>()[..] else {
        panic!("one turn");
    };
    assert!(Uuid::parse_str(id).is_ok());
    assert_eq!((turn.seq, turn.parent_turn.as_deref()), (0, None));
    assert_eq!(turn.stop_reason, Some(StopReason::EndTurn));
    let messages: Vec<_> = turn
        .messages
        .iter()
        .map(|message| (message.seq, &message.kind, message.text.as_str()))
        .collect();
    assert_eq!(
        messages,
        [
            (
                0,
                &MessageKind::UserMessage,
                "Fix the config loader\nfile:///work/repo/src/config.rs"
            ),
            (1, &MessageKind::AgentMessage, "I'll look at the file."),
            (3, &MessageKind::AgentMessage, "Now the fix."),
        ]
    );
    let calls: Vec<_> = turn
        .tool_calls
        .iter()
        .map(|call| (call.seq, call.id.as_str(), &call.kind, &call.status))
        .collect();
    assert_eq!(
        calls,
        [
            (2, "call_001", &ToolKind::Read, &ToolStatus::Completed),
            (4, "call_002", &ToolKind::Edit, &ToolStatus::Completed),
        ]
    );
    let read = &turn.tool_calls[0];
    assert_eq!(read.title, "Reading configuration file");
    assert_eq!(read.locations[0].path, "/work/repo/src/config.rs");
    assert_eq!(read.locations[0].line, Some(1));
    assert_eq!(read.output.as_deref(), Some("fn load() {}"));
    let edit = &turn.tool_calls[1];
    assert_eq!(edit.locations[0].line, None);
    // Content text outranks `rawOutput`; with none, `rawOutput` is the output.
    assert_eq!(edit.output.as_deref(), Some("applied"));
    assert_eq!(edit.diffs.len(), 1);
    assert_eq!(edit.diffs[0].path, "/work/repo/src/config.rs");
    assert_eq!(
        edit.diffs[0].patch,
        "@@ -1 +1,3 @@\n-fn load() {}\n+fn load() -> Result<()> {\n+    Ok(())\n+}"
    );
    let plan = first.plan.as_ref().unwrap();
    assert_eq!(plan.entries.len(), 2);
    assert_eq!(plan.entries[1].content, "Make it fail loudly");

    // The second turn follows the first and ends cancelled; the plan did not change.
    let second = &emits[2].session.imported;
    assert!(second.plan.is_none() && second.session.is_none());
    let [(second_id, turn)] = second.turns.iter().collect::<Vec<_>>()[..] else {
        panic!("one turn");
    };
    assert_eq!((turn.seq, turn.parent_turn.as_ref()), (1, Some(id)));
    assert_ne!(second_id, id);
    assert_eq!(turn.stop_reason, Some(StopReason::Cancelled));
    assert_eq!(turn.tool_calls[0].status, ToolStatus::Failed);
    assert_eq!(turn.tool_calls[0].output.as_deref(), Some(r#"{"exit":1}"#));
}

#[test]
fn a_tool_calls_window_opens_at_its_first_report_and_closes_at_a_terminal_status() {
    let mut recorder = Recorder::new();
    let emits = play(&mut recorder, SESSION);
    let windows: Vec<_> = emits
        .iter()
        .filter_map(|emit| match &emit.window {
            Window::Boundary => None,
            Window::Open(tool) => Some(("open", tool)),
            Window::Close(tool) => Some(("close", tool)),
        })
        .map(|(what, tool)| (what, tool.cause.tool_use_id.as_str(), tool.paths.clone()))
        .collect();
    let config = vec![PathBuf::from("/work/repo/src/config.rs")];
    assert_eq!(
        windows,
        [
            ("open", "call_001", vec![]),
            ("close", "call_001", config.clone()),
            ("open", "call_002", config.clone()),
            ("close", "call_002", config),
            ("open", "call_003", vec![]),
            ("close", "call_003", vec![]),
        ]
    );
    // A window's emit carries no session content, and its cause names the turn that holds the
    // call, which is the turn a later boundary emit records.
    let recorded_turns: Vec<_> = emits
        .iter()
        .filter(|emit| emit.window == Window::Boundary)
        .flat_map(|emit| emit.session.imported.turns.keys().cloned())
        .collect();
    let caused_in = |call: &str| -> String {
        emits
            .iter()
            .find_map(|emit| match &emit.window {
                Window::Close(tool) if tool.cause.tool_use_id == call => tool.cause.turn.clone(),
                _ => None,
            })
            .unwrap()
    };
    assert!(
        emits
            .iter()
            .filter(|emit| emit.window != Window::Boundary)
            .all(|emit| emit.session.imported.is_empty())
    );
    assert_eq!(caused_in("call_001"), recorded_turns[0]);
    assert_eq!(caused_in("call_002"), recorded_turns[0]);
    assert_eq!(caused_in("call_003"), recorded_turns[1]);
    // Emits follow the messages: a window's close precedes the turn end that holds its call.
    let last_close = emits
        .iter()
        .position(|emit| matches!(&emit.window, Window::Close(tool) if tool.cause.tool_use_id == "call_002"))
        .unwrap();
    let turn_end = emits
        .iter()
        .position(|emit| emit.session.imported.turns.len() == 1)
        .unwrap();
    assert!(last_close < turn_end);
}

#[test]
fn a_call_first_reported_finished_has_no_window() {
    let mut recorder = Recorder::new();
    let emits = play(
        &mut recorder,
        r#"> {"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w","mcpServers":[]}}
< {"jsonrpc":"2.0","id":1,"result":{"sessionId":"s"}}
> {"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"s","prompt":[]}}
< {"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"c","title":"t","status":"completed"}}}
< {"jsonrpc":"2.0","id":2,"result":{"stopReason":"end_turn"}}
"#,
    );
    assert!(emits.iter().all(|emit| emit.window == Window::Boundary));
}

#[test]
fn chunking_the_bytes_changes_nothing() {
    let mut whole = Recorder::new();
    let expected = play(&mut whole, SESSION);
    let bytes: Vec<(Direction, Vec<u8>)> = lines(SESSION)
        .into_iter()
        .map(|(direction, line)| (direction, line.into_bytes()))
        .collect();
    let mut chunked = Recorder::new();
    let mut emits = Vec::new();
    for (direction, line) in &bytes {
        for piece in line.chunks(7) {
            emits.extend(chunked.feed(*direction, at(), piece).unwrap());
        }
    }
    // Turn ids are minted afresh, and message ids derive from them: compare all but the UUIDs.
    let strip = |emits: Vec<Emit>| -> Vec<String> {
        emits
            .iter()
            .map(|emit| without_uuids(&serde_json::to_string(&emit.session).unwrap()))
            .collect()
    };
    assert_eq!(strip(emits), strip(expected));
}

fn without_uuids(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(c) = rest.chars().next() {
        match rest.get(..36).filter(|head| Uuid::parse_str(head).is_ok()) {
            Some(_) => {
                out.push_str("<uuid>");
                rest = &rest[36..];
            }
            None => {
                out.push(c);
                rest = &rest[c.len_utf8()..];
            }
        }
    }
    out
}

#[test]
fn thinking_is_recorded_only_when_asked() {
    let mut recorder = Recorder::new().recording_thinking();
    let emits = boundaries(play(&mut recorder, SESSION));
    let turn = emits[1].session.imported.turns.values().next().unwrap();
    let thoughts: Vec<_> = turn
        .messages
        .iter()
        .filter(|message| message.kind == MessageKind::AgentThought)
        .map(|message| message.text.as_str())
        .collect();
    assert_eq!(thoughts, ["The loader probably ignores errors."]);
    // Thoughts and messages are separate messages in the turn's order.
    assert_eq!(turn.messages.len(), 4);
}

#[test]
fn a_load_replay_is_not_recorded_and_a_cut_connection_closes_its_turn() {
    let mut recorder = Recorder::new();
    let emits = play(&mut recorder, LOAD);
    assert_eq!(emits.len(), 1, "only the session: the replay made no turn");
    assert_eq!(emits[0].session.session_id, "sess_old");
    assert_eq!(
        emits[0].session.imported.session.as_ref().unwrap().harness,
        "acp"
    );

    let finished = recorder.finish();
    assert_eq!(finished.len(), 1);
    let turn = finished[0].session.imported.turns.values().next().unwrap();
    assert_eq!(turn.stop_reason, None);
    // A messageId joins the chunks that name it.
    let texts: Vec<_> = turn
        .messages
        .iter()
        .map(|m| (m.id.as_str(), m.text.as_str()))
        .collect();
    assert_eq!(
        texts,
        [
            (turn.messages[0].id.as_str(), "continue"),
            ("m1", "Sure, continuing.")
        ]
    );
    assert!(turn.tool_calls.is_empty());
    assert!(recorder.finish().is_empty());
}

#[test]
fn doc_ids_from_the_recorder_ride_later_emits() {
    let mut recorder = Recorder::new();
    let all = lines(SESSION);
    let mut emits = Vec::new();
    for (direction, line) in &all[..4] {
        emits.extend(recorder.feed(*direction, at(), line.as_bytes()).unwrap());
    }
    assert_eq!(emits[0].session.doc, None);
    recorder.set_doc("sess_abc123def456", "zdoc".to_owned());
    for (direction, line) in &all[4..] {
        emits.extend(recorder.feed(*direction, at(), line.as_bytes()).unwrap());
    }
    assert!(
        emits[1..]
            .iter()
            .all(|emit| emit.session.doc.as_deref() == Some("zdoc"))
    );
}

#[test]
fn protocol_violations_are_errors_naming_the_message() {
    let feed = |direction, line: &str| {
        Recorder::new()
            .feed(direction, at(), format!("{line}\n").as_bytes())
            .unwrap_err()
            .to_string()
    };
    assert!(feed(Direction::FromAgent, "not json").contains("not a JSON-RPC message"));
    assert!(
        feed(
            Direction::FromAgent,
            r#"{"jsonrpc":"2.0","id":9,"result":{}}"#
        )
        .contains("unknown request 9")
    );
    assert!(
        feed(
            Direction::ToAgent,
            r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"x","prompt":[]}}"#
        )
        .contains("unknown session x")
    );
    let mut recorder = Recorder::new();
    for (direction, line) in &lines(SESSION)[..6] {
        recorder.feed(*direction, at(), line.as_bytes()).unwrap();
    }
    let outside = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess_abc123def456","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"late"}}}}"#;
    // Inside the first turn that is fine; after it ends it is outside any turn.
    recorder
        .feed(
            Direction::FromAgent,
            at(),
            format!("{outside}\n").as_bytes(),
        )
        .unwrap();
    let end = r#"{"jsonrpc":"2.0","id":2,"result":{"stopReason":"end_turn"}}"#;
    recorder
        .feed(Direction::FromAgent, at(), format!("{end}\n").as_bytes())
        .unwrap();
    let error = recorder
        .feed(
            Direction::FromAgent,
            at(),
            format!("{outside}\n").as_bytes(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("outside a prompt turn"),
        "{error}"
    );
}
