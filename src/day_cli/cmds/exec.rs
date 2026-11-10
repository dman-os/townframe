use crate::interlude::*;

/// Executes one operation through the node's dispatch router and prints the JSON answer. The
/// answer is printed whether it is the operation's output or its XRPC error envelope; an error
/// status also fails the command, as `curl --fail-with-body` does.
pub async fn run(nsid: String, input: Option<String>) -> Res<ExitCode> {
    let input: serde_json::Value = match input {
        Some(text) => serde_json::from_str(&text).wrap_err("the input is not valid JSON")?,
        None => json!({}),
    };
    let reply = lazy::node().await?.call(&nsid, &input).await?;
    println!("{}", serde_json::to_string_pretty(&reply.body)?);
    if !reply.is_success() {
        error!(status = reply.status, %nsid, "the node refused the call");
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}
