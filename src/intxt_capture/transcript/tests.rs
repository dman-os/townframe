use super::*;
use serde::de::DeserializeOwned;
use serde_json::json;

const MAIN: &str = include_str!("fixtures/main.jsonl");
const SUB: &str = include_str!("fixtures/sub.jsonl");

fn parent() -> DatumReference {
    serde_json::from_value(json!(
        "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.agentSession/main"
    ))
    .unwrap()
}

fn import_once(parent: Option<DatumReference>, transcript: &str) -> Imported {
    TranscriptImporter::new(parent)
        .import(transcript.as_bytes())
        .unwrap()
}

/// Fold a later call's changes into an earlier one's, as a store keyed by datum would.
fn merge(into: &mut Imported, next: Imported) {
    into.offset = next.offset;
    into.session = next.session.or(into.session.take());
    into.turns.extend(next.turns);
    into.plan = next.plan.or(into.plan.take());
    into.spawned.extend(next.spawned);
}

/// Every datum survives its intxt_core type: decoding what the importer produced and encoding it
/// again changes nothing.
fn assert_round_trips<T>(datum: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let value = serde_json::to_value(datum).unwrap();
    let back: T = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(&back, datum);
    assert_eq!(serde_json::to_value(&back).unwrap(), value);
}

fn assert_all_round_trip(imported: &Imported) {
    if let Some(session) = &imported.session {
        assert_round_trips(session);
    }
    for turn in imported.turns.values() {
        assert_round_trips(turn);
    }
    if let Some(plan) = &imported.plan {
        assert_round_trips(plan);
    }
}

#[test]
fn main_transcript_becomes_session_turns_and_plan() {
    let imported = import_once(None, MAIN);
    assert_all_round_trip(&imported);
    assert_eq!(imported.offset, MAIN.len() as u64);
    assert_eq!(imported.spawned, ["ab12"]);

    assert_eq!(
        serde_json::to_value(imported.session.as_ref().unwrap()).unwrap(),
        json!({
            "harness": "claude-code",
            "sessionId": "s-main",
            "title": "Fix the parser",
            "cwd": "/work/repo"
        })
    );

    assert_eq!(
        serde_json::to_value(imported.plan.as_ref().unwrap()).unwrap(),
        json!({"entries": [
            {"content": "Fix parser", "priority": "medium", "status": "in_progress"},
            {"content": "Run tests", "priority": "medium", "status": "pending"}
        ]})
    );

    // The meta reminder, the notification and the compaction summary stay in the first turn; the
    // sidechain record `sc1` is not this session's; the second prompt starts a turn whose parent
    // is the first.
    assert_eq!(imported.turns.keys().collect::<Vec<_>>(), ["u1", "u3"]);
    let message = |id: &str, seq: u64, at: &str, kind: &str, text: &str| json!({"id": id, "seq": seq, "at": at, "kind": kind, "text": text});
    assert_eq!(
        serde_json::to_value(&imported.turns["u1"]).unwrap(),
        json!({
            "seq": 0,
            "at": "2026-11-10T10:00:00Z",
            "stopReason": "end_turn",
            "messages": [
                message("u1", 0, "2026-11-10T10:00:00Z", "userMessage", "Fix the parser bug"),
                message("u2", 1, "2026-11-10T10:00:01Z", "meta", "<system-reminder>Plan mode is off</system-reminder>"),
                message("m1", 2, "2026-11-10T10:00:03Z", "agentMessage", "I'll read the parser."),
                message("m7", 9, "2026-11-10T10:00:17Z", "agentMessage", "Fixed."),
                message("n1", 10, "2026-11-10T10:00:18Z", "notification", "<task-notification>build finished</task-notification>"),
                message("s1", 11, "2026-11-10T10:00:20Z", "compactionSummary", "This session is being continued from a previous conversation."),
            ],
            "toolCalls": [
                {
                    "id": "t1", "seq": 3, "title": "Read /work/repo/src/parser.rs", "kind": "read",
                    "status": "completed",
                    "locations": [{"path": "/work/repo/src/parser.rs", "line": 10}],
                    "output": "fn parse() {}"
                },
                {
                    "id": "t2", "seq": 4, "title": "Edit parser.rs: replace", "kind": "edit",
                    "status": "completed", "cwd": "/work/repo/src",
                    "locations": [{"path": "/work/repo/src/parser.rs"}],
                    "diffs": [{"path": "/work/repo/src/parser.rs", "patch": "@@ -3,2 +3,2 @@\n c\n-a\n+b"}],
                    "output": "updated"
                },
                {
                    "id": "t3", "seq": 5, "title": "Bash Run tests", "kind": "execute",
                    "status": "failed",
                    "output": "test parser::it_parses ... FAILED\n1 failure"
                },
                {
                    "id": "t4", "seq": 6, "title": "TodoWrite", "kind": "other",
                    "status": "completed", "output": "Todos have been modified"
                },
                {
                    "id": "t5", "seq": 7, "title": "Write notes.txt: write", "kind": "edit",
                    "status": "completed",
                    "locations": [{"path": "/work/repo/notes.txt"}],
                    "diffs": [{"path": "/work/repo/notes.txt", "patch": "@@ -0,0 +1,2 @@\n+l1\n+l2"}],
                    "output": "created"
                },
                {
                    "id": "t6", "seq": 8, "title": "Agent Explore the lexer", "kind": "other",
                    "status": "completed", "output": "The lexer is in lex.rs"
                },
            ]
        })
    );
    assert_eq!(
        serde_json::to_value(&imported.turns["u3"]).unwrap(),
        json!({
            "seq": 1,
            "at": "2026-11-10T10:00:21Z",
            "parentTurn": "u1",
            "stopReason": "end_turn",
            "messages": [
                message("u3", 0, "2026-11-10T10:00:21Z", "userMessage", "Thanks, now rename it"),
                message("m8", 1, "2026-11-10T10:00:22Z", "agentMessage", "Renamed."),
            ],
            "toolCalls": []
        })
    );
}

#[test]
fn subagent_transcript_is_its_own_session_with_a_parent() {
    let imported = import_once(Some(parent()), SUB);
    assert_all_round_trip(&imported);
    assert_eq!(
        serde_json::to_value(imported.session.as_ref().unwrap()).unwrap(),
        json!({
            "harness": "claude-code",
            "sessionId": "agent-ab12",
            "cwd": "/work/repo",
            "parent": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.agentSession/main"
        })
    );
    assert_eq!(
        serde_json::to_value(&imported.turns).unwrap(),
        json!({"sc1": {
            "seq": 0,
            "at": "2026-11-10T10:00:15Z",
            "stopReason": "end_turn",
            "messages": [
                {"id": "sc1", "seq": 0, "at": "2026-11-10T10:00:15Z", "kind": "spawnPrompt", "text": "Find the lexer"},
                {"id": "sm2", "seq": 2, "at": "2026-11-10T10:00:16Z", "kind": "agentMessage", "text": "The lexer is in lex.rs"}
            ],
            "toolCalls": [{
                "id": "st1", "seq": 1, "title": "Grep Lexer in /work/repo/src", "kind": "search",
                "status": "completed",
                "locations": [{"path": "/work/repo/src"}],
                "output": "src/lex.rs:1:struct Lexer"
            }]
        }})
    );
}

/// The invariant the capture hooks rest on: however the file's bytes arrive, over however many
/// calls, with the state persisted and reloaded between them, the datums are the same.
#[test]
fn chunked_import_equals_one_shot() {
    for (parent, transcript) in [(None, MAIN), (Some(parent()), SUB)] {
        let whole = import_once(parent.clone(), transcript);
        let bytes = transcript.as_bytes();
        let line_ends: Vec<usize> = bytes
            .iter()
            .enumerate()
            .filter_map(|(at, byte)| (*byte == b'\n').then_some(at))
            .collect();
        // Fixed-size steps, and steps that stop one byte before, at and one byte after each
        // newline.
        let mut plans: Vec<Vec<usize>> = [1, 2, 3, 7, 64, 500]
            .iter()
            .map(|step| {
                (1..=bytes.len().div_ceil(*step))
                    .map(|n| n * step)
                    .collect()
            })
            .collect();
        for end in &line_ends {
            plans.push(vec![end.saturating_sub(1), *end, end + 1, bytes.len()]);
        }
        for plan in plans {
            let mut importer = TranscriptImporter::new(parent.clone());
            let mut merged = Imported::default();
            for limit in plan {
                let limit = limit.min(bytes.len());
                let from = importer.offset() as usize;
                merge(&mut merged, importer.import(&bytes[from..limit]).unwrap());
                importer =
                    serde_json::from_str(&serde_json::to_string(&importer).unwrap()).unwrap();
            }
            assert_eq!(merged, whole);
        }
    }
}

#[test]
fn a_partial_last_line_waits_for_its_newline() {
    let first_line = MAIN.find('\n').unwrap() + 1;
    let second_line = first_line + MAIN[first_line..].find('\n').unwrap() + 1;
    let mut importer = TranscriptImporter::new(None);

    let held = importer.import(&MAIN.as_bytes()[..first_line - 1]).unwrap();
    assert_eq!(held, Imported::default());
    assert_eq!(importer.offset(), 0);

    let one = importer
        .import(&MAIN.as_bytes()[..second_line - 3])
        .unwrap();
    assert_eq!(one.offset, first_line as u64);
    assert_eq!(importer.offset(), first_line as u64);
}

#[test]
fn import_file_resumes_from_the_stored_offset() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("transcript/fixtures/main.jsonl");
    let mut importer = TranscriptImporter::new(None);
    assert_eq!(
        importer.import_file(&path).unwrap(),
        import_once(None, MAIN)
    );
    let again = importer.import_file(&path).unwrap();
    assert_eq!(again.offset, MAIN.len() as u64);
    assert!(again.turns.is_empty() && again.session.is_none());

    let mut ahead = TranscriptImporter::new(None);
    ahead.offset = MAIN.len() as u64 + 1;
    assert!(matches!(
        ahead.import_file(&path),
        Err(ImportError::Shrank { .. })
    ));
}

#[test]
fn records_before_the_first_prompt_open_a_turn_of_their_own() {
    let transcript = concat!(
        r#"{"type":"user","uuid":"c0","parentUuid":null,"timestamp":"2026-11-10T10:00:00Z","cwd":"/w","sessionId":"s","isMeta":true,"message":{"role":"user","content":"<local-command-caveat>x</local-command-caveat>"}}"#,
        "\n",
        r#"{"type":"user","uuid":"u1","parentUuid":"c0","timestamp":"2026-11-10T10:00:01Z","cwd":"/w","sessionId":"s","message":{"role":"user","content":"hello"}}"#,
        "\n",
    );
    let imported = import_once(None, transcript);
    assert_all_round_trip(&imported);
    assert_eq!(imported.turns.keys().collect::<Vec<_>>(), ["c0", "u1"]);
    assert_eq!(
        imported.turns["c0"].messages[0].kind,
        MessageKind::LocalCommand
    );
    assert_eq!(imported.turns["u1"].parent_turn.as_deref(), Some("c0"));
}

#[test]
fn malformed_input_is_an_error_not_a_skip() {
    let mut importer = TranscriptImporter::new(None);
    assert!(matches!(
        importer.import(b"{\"type\":\n"),
        Err(ImportError::Line { at: 0, .. })
    ));
    let orphan_result = r#"{"type":"user","uuid":"r","parentUuid":null,"timestamp":"2026-11-10T10:00:00Z","cwd":"/w","sessionId":"s","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"nope","content":"x"}]}}"#;
    assert!(matches!(
        TranscriptImporter::new(None).import(format!("{orphan_result}\n").as_bytes()),
        Err(ImportError::Record { .. })
    ));
}

#[test]
fn state_drops_closed_turns_but_keeps_what_resolves_parents() {
    let mut importer = TranscriptImporter::new(None);
    importer.import(MAIN.as_bytes()).unwrap();
    let state = serde_json::to_string(&importer).unwrap();
    // Turn u1 is closed: its tool outputs are gone from the state, and only the current turn
    // (u3) remains.
    assert!(!state.contains("fn parse() {}"));
    assert!(!state.contains("FAILED"));
    assert_eq!(importer.turns.keys().collect::<Vec<_>>(), ["u3"]);
    assert_eq!(importer.turn_of["u3"].as_deref(), Some("u3"));
    assert_eq!(importer.turn_of["a9"].as_deref(), Some("u1"));
}

#[test]
fn thinking_is_recorded_only_when_asked() {
    let off = import_once(None, MAIN);
    let on = TranscriptImporter::new(None)
        .recording_thinking()
        .import(MAIN.as_bytes())
        .unwrap();
    assert_all_round_trip(&on);
    let thought = &on.turns["u1"].messages[2];
    assert_eq!(thought.id, "m1.thought");
    assert_eq!(thought.kind, MessageKind::AgentThought);
    assert_eq!(thought.text, "Look at the parser first.");
    // Nothing else changes but the seqs after the thought.
    assert_eq!(
        on.turns["u1"].messages.len(),
        off.turns["u1"].messages.len() + 1
    );
    assert_eq!(on.turns["u1"].messages[3].id, "m1");
    assert_eq!(on.turns["u1"].messages[3].seq, 3);
    assert!(
        off.turns["u1"]
            .messages
            .iter()
            .all(|m| m.kind != MessageKind::AgentThought)
    );
}
