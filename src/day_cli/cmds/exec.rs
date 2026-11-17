use crate::client::Raw;
use crate::interlude::*;
use method_core::HttpMethod;

/// Executes one operation through the node's dispatch router and prints the JSON answer. The
/// input is the JSON body of a procedure, or with `get` the query string of a query (the node
/// holds the verb to the method's kind, as atproto does). An operation whose body is bytes
/// (ADR 042) takes `body`, a file streamed as the request body, and the input is its parameters;
/// one that answers bytes writes them to `output`, and `range` asks for part of them. The
/// answer is printed whether it is the operation's output or its XRPC error envelope; an error
/// status also fails the command, as `curl --fail-with-body` does.
pub async fn run(
    nsid: String,
    input: Option<String>,
    get: bool,
    body: Option<std::path::PathBuf>,
    range: Option<String>,
    output: Option<std::path::PathBuf>,
) -> Res<ExitCode> {
    let input: serde_json::Value = match input {
        Some(text) => serde_json::from_str(&text).wrap_err("the input is not valid JSON")?,
        None => json!({}),
    };
    let verb = if get {
        HttpMethod::Get
    } else {
        HttpMethod::Post
    };
    let node = lazy::node().await?;
    let reply = if body.is_some() || range.is_some() || output.is_some() {
        node.call_raw(
            &nsid,
            verb,
            &input,
            Raw {
                upload: body.as_deref(),
                range: range.as_deref(),
                download: output.as_deref(),
            },
        )
        .await?
    } else {
        node.call(&nsid, verb, &input).await?
    };
    println!("{}", serde_json::to_string_pretty(&reply.body)?);
    if !reply.is_success() {
        error!(status = reply.status, %nsid, "the node refused the call");
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}
