use super::*;
use intxt_core::work::MessageKind;
use intxt_core::work::StopReason;

const INIT: &str =
    r#"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":1}}"#;
const NEW: &str = r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/work/repo","mcpServers":[]}}"#;
const PROMPT: &str = r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"s1","prompt":[{"type":"text","text":"hello"}]}}"#;

const INIT_OK: &str =
    r#"{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":1,"agentInfo":{"name":"fake"}}}"#;
const NEW_OK: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s1"}}"#;
// Spaces and key order a re-serializing proxy would change.
const CHUNK: &str = r#"{ "method": "session/update",  "params": {"update": {"content": {"text": "hi ", "type": "text"}, "sessionUpdate": "agent_message_chunk"}, "sessionId": "s1"}, "jsonrpc": "2.0" }"#;
const CHUNK_2: &str = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"there"}}}}"#;
const DONE: &str = r#"{"jsonrpc":"2.0","id":2,"result":{"stopReason":"end_turn"}}"#;

/// An agent that answers each line the client sends with the lines of one `printf`.
fn fake_agent() -> Vec<String> {
    let script = format!(
        "read -r _; printf '%s\\n' '{INIT_OK}'; read -r _; printf '%s\\n' '{NEW_OK}'; \
         read -r _; printf '%s\\n' '{CHUNK}' '{CHUNK_2}' '{DONE}'; echo agent noise >&2"
    );
    vec!["sh".to_owned(), "-c".to_owned(), script]
}

/// Run the proxy over the fake agent with a client that answers causally, as an editor does: it
/// sends each request only after the agent's previous answer arrived. Returns the agent's exit
/// status and everything the client received.
async fn drive(record: impl AsyncFnMut(Emit) -> Res<Option<String>>) -> (ExitStatus, Vec<u8>) {
    use tokio::io::AsyncBufReadExt;
    let (mut client_writes, agent_reads) = tokio::io::duplex(64 * 1024);
    let (agent_writes, client_reads) = tokio::io::duplex(64 * 1024);
    let client = async {
        let mut received = Vec::new();
        let mut lines = tokio::io::BufReader::new(client_reads);
        for (request, answers) in [(INIT, 1), (NEW, 1), (PROMPT, 3)] {
            client_writes
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            for _ in 0..answers {
                lines.read_until(b'\n', &mut received).await.unwrap();
            }
        }
        // The client leaves: the agent sees its stdin end.
        drop(client_writes);
        lines.read_to_end(&mut received).await.unwrap();
        received
    };
    let (status, received) = tokio::join!(
        proxy(&fake_agent(), false, agent_reads, agent_writes, record),
        client
    );
    (status.unwrap(), received)
}

fn agent_output() -> Vec<u8> {
    format!("{INIT_OK}\n{NEW_OK}\n{CHUNK}\n{CHUNK_2}\n{DONE}\n").into_bytes()
}

#[tokio::test]
async fn bytes_pass_through_unchanged_and_the_turn_is_recorded() {
    let mut seen: Vec<Emit> = Vec::new();
    let (status, received) = drive(async |emit: Emit| {
        seen.push(emit);
        Ok(Some("zdoc".to_owned()))
    })
    .await;
    assert!(status.success());
    assert_eq!(
        String::from_utf8(received).unwrap(),
        String::from_utf8(agent_output()).unwrap()
    );

    assert_eq!(seen.len(), 2, "the session, then the turn");
    let session = seen[0].session.imported.session.as_ref().unwrap();
    assert_eq!(
        (session.harness.as_str(), session.session_id.as_str()),
        ("fake", "s1")
    );
    assert_eq!(seen[0].cwd, PathBuf::from("/work/repo"));
    // The answer of the first record rides the second.
    assert_eq!(seen[0].session.doc, None);
    assert_eq!(seen[1].session.doc.as_deref(), Some("zdoc"));
    let turn = seen[1].session.imported.turns.values().next().unwrap();
    assert_eq!(turn.stop_reason, Some(StopReason::EndTurn));
    let messages: Vec<_> = turn
        .messages
        .iter()
        .map(|m| (&m.kind, m.text.as_str()))
        .collect();
    assert_eq!(
        messages,
        [
            (&MessageKind::UserMessage, "hello"),
            (&MessageKind::AgentMessage, "hi there")
        ]
    );
}

#[tokio::test]
async fn a_failing_recording_stops_recording_and_not_the_session() {
    let mut calls = 0;
    let (status, received) = drive(async |_: Emit| {
        calls += 1;
        Err(eyre::eyre!("the node is gone"))
    })
    .await;
    assert!(status.success());
    assert_eq!(received, agent_output());
    assert_eq!(calls, 1, "recording stopped at its first failure");
}

#[tokio::test]
async fn an_agent_that_is_not_acp_is_still_proxied() {
    let mut output = Vec::new();
    let agent = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        "echo not json; exit 3".to_owned(),
    ];
    let status = proxy(&agent, false, &b""[..], &mut output, async |_: Emit| {
        Ok(None)
    })
    .await
    .unwrap();
    assert_eq!(status.code(), Some(3));
    assert_eq!(output, b"not json\n");
}
