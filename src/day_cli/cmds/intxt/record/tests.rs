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
async fn drive(
    record: impl AsyncFnMut(Emit) -> Res<Option<String>>,
    stopped: impl FnMut(&str),
) -> (ExitStatus, Vec<u8>) {
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
    let agent = fake_agent();
    let (status, received) = tokio::join!(
        proxy(
            &agent,
            Recorder::new(PathBuf::new()),
            agent_reads,
            agent_writes,
            TEE_CHUNKS,
            record,
            stopped
        ),
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
    let (status, received) = drive(
        async |emit: Emit| {
            seen.push(emit);
            Ok(Some("zdoc".to_owned()))
        },
        |reason| panic!("recording stopped: {reason}"),
    )
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
    let mut reasons = Vec::new();
    let (status, received) = drive(
        async |_: Emit| {
            calls += 1;
            Err(eyre::eyre!("the node is gone"))
        },
        |reason| reasons.push(reason.to_owned()),
    )
    .await;
    assert!(status.success());
    assert_eq!(received, agent_output());
    assert_eq!(calls, 1, "recording stopped at its first failure");
    assert_eq!(reasons, ["the node is gone"]);
}

#[tokio::test]
async fn an_agent_that_is_not_acp_is_still_proxied() {
    let mut output = Vec::new();
    let agent = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        "echo not json; exit 3".to_owned(),
    ];
    let mut reasons: Vec<String> = Vec::new();
    let status = proxy(
        &agent,
        Recorder::new(PathBuf::new()),
        &b""[..],
        &mut output,
        TEE_CHUNKS,
        async |_: Emit| Ok(None),
        |reason| reasons.push(reason.to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(status.code(), Some(3));
    assert!(reasons[0].contains("not a JSON-RPC message"), "{reasons:?}");
    assert_eq!(output, b"not json\n");
}

/// A recorder that never answers must not slow or stall the session: once the tee is full the
/// pumps go on forwarding, and recording stops saying it fell behind.
#[tokio::test]
async fn a_recorder_that_never_drains_does_not_block_forwarding() {
    use tokio::io::AsyncBufReadExt;
    // The agent answers `initialize` and `session/new` (whose batch the recorder then hangs on),
    // then writes 4 MB, far more than the tee's 4 chunks of up to 64 KiB.
    let script = format!(
        "read -r _; printf '%s\\n' '{INIT_OK}'; read -r _; printf '%s\\n' '{NEW_OK}'; \
         head -c 4000000 /dev/zero | tr '\\0' x; echo"
    );
    let agent = vec!["sh".to_owned(), "-c".to_owned(), script];
    let (mut client_writes, agent_reads) = tokio::io::duplex(64 * 1024);
    let (agent_writes, client_reads) = tokio::io::duplex(64 * 1024);
    let client = async {
        let mut received = Vec::new();
        let mut lines = tokio::io::BufReader::new(client_reads);
        for request in [INIT, NEW] {
            client_writes
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            lines.read_until(b'\n', &mut received).await.unwrap();
        }
        drop(client_writes);
        lines.read_to_end(&mut received).await.unwrap();
        received
    };
    let mut reasons = Vec::new();
    let (status, received) = tokio::join!(
        proxy(
            &agent,
            Recorder::new(PathBuf::new()),
            agent_reads,
            agent_writes,
            4,
            async |_: Emit| std::future::pending().await,
            |reason| reasons.push(reason.to_owned()),
        ),
        client
    );
    assert!(status.unwrap().success());
    assert_eq!(received.len(), INIT_OK.len() + NEW_OK.len() + 2 + 4_000_001);
    assert_eq!(reasons.len(), 1);
    assert!(
        reasons[0].starts_with("recording fell behind the session (4 chunks"),
        "{}",
        reasons[0]
    );
}
