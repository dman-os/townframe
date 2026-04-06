#[allow(unused)]
mod interlude {
    pub use serde::{Deserialize, Serialize};
    pub use std::time::Duration;
}

use crate::interlude::*;

pub const COMMAND_SCHEME: &str = "db+command";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRef {
    pub plug_id: String,
    pub command_name: String,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum CommandUrlError {
    /// unsupported command url scheme '{0}'
    UnsupportedScheme(String),
    /// command url authority must be empty
    NonEmptyAuthority,
    /// command url path is malformed
    MalformedPath,
    /// command url missing plug namespace
    MissingNamespace,
    /// command url missing plug name
    MissingPlugName,
    /// command url missing command name
    MissingCommandName,
    /// command url has unexpected extra path segments
    ExtraPathSegments,
    /// command url parse error: {0}
    Parse(String),
}

pub fn build_command_url(plug_id: &str, command_name: &str) -> Result<url::Url, CommandUrlError> {
    let plug = plug_id
        .strip_prefix('@')
        .ok_or_else(|| CommandUrlError::Parse(format!("plug id must start with @: {plug_id}")))?;
    let (namespace, name) = plug
        .split_once('/')
        .ok_or_else(|| CommandUrlError::Parse(format!("plug id must be @ns/name: {plug_id}")))?;
    if namespace.is_empty() || name.is_empty() {
        return Err(CommandUrlError::Parse(format!(
            "plug id must be @ns/name: {plug_id}"
        )));
    }
    if command_name.is_empty() {
        return Err(CommandUrlError::Parse(
            "command name must be non-empty".to_string(),
        ));
    }
    let url = format!("{COMMAND_SCHEME}:///@{namespace}/{name}/{command_name}");
    url::Url::parse(&url).map_err(|err| CommandUrlError::Parse(err.to_string()))
}

pub fn parse_command_url(url: &url::Url) -> Result<CommandRef, CommandUrlError> {
    if url.scheme() != COMMAND_SCHEME {
        return Err(CommandUrlError::UnsupportedScheme(url.scheme().to_string()));
    }
    if url.host_str().is_some() {
        return Err(CommandUrlError::NonEmptyAuthority);
    }

    let mut parts = url
        .path_segments()
        .ok_or(CommandUrlError::MalformedPath)?
        .filter(|segment| !segment.is_empty());

    let namespace_or_at = parts.next().ok_or(CommandUrlError::MissingNamespace)?;
    let namespace = namespace_or_at
        .strip_prefix('@')
        .unwrap_or(namespace_or_at)
        .to_string();
    if namespace.is_empty() {
        return Err(CommandUrlError::MissingNamespace);
    }

    let plug_name = parts.next().ok_or(CommandUrlError::MissingPlugName)?;
    if plug_name.is_empty() {
        return Err(CommandUrlError::MissingPlugName);
    }

    let command_name = parts.next().ok_or(CommandUrlError::MissingCommandName)?;
    if command_name.is_empty() {
        return Err(CommandUrlError::MissingCommandName);
    }

    if parts.next().is_some() {
        return Err(CommandUrlError::ExtraPathSegments);
    }

    Ok(CommandRef {
        plug_id: format!("@{namespace}/{plug_name}"),
        command_name: command_name.to_string(),
    })
}

pub fn parse_command_url_str(url: &str) -> Result<CommandRef, CommandUrlError> {
    let parsed = url::Url::parse(url).map_err(|err| CommandUrlError::Parse(err.to_string()))?;
    parse_command_url(&parsed)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InvokeCommandRequest {
    pub request_id: String,
    pub args_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InvokeCommandAccepted {
    pub dispatch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum InvokeCommandStatus {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InvokeCommandReply {
    pub request_id: String,
    pub status: InvokeCommandStatus,
    #[serde(default)]
    pub value_json: Option<String>,
    #[serde(default)]
    pub error_json: Option<String>,
}

pub fn invoke_command_effect<F>(
    cx: &mut wflow_sdk::WflowCtx,
    request: &InvokeCommandRequest,
    invoke: F,
) -> Result<InvokeCommandAccepted, wflow_sdk::JobErrorX>
where
    F: FnOnce(
        &InvokeCommandRequest,
    ) -> Result<wflow_sdk::Json<InvokeCommandAccepted>, wflow_sdk::JobErrorX>,
{
    cx.effect(|| invoke(request))
}

pub fn wait_command_reply(
    cx: &mut wflow_sdk::WflowCtx,
) -> Result<InvokeCommandReply, wflow_sdk::JobErrorX> {
    let wflow_sdk::Json(reply) = cx.recv::<wflow_sdk::Json<InvokeCommandReply>>()?;
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_url_roundtrip() {
        let url = build_command_url("@daybook/plabels", "label-note").unwrap();
        assert_eq!(url.as_str(), "db+command:///@daybook/plabels/label-note");
        let parsed = parse_command_url(&url).unwrap();
        assert_eq!(parsed.plug_id, "@daybook/plabels");
        assert_eq!(parsed.command_name, "label-note");
    }

    #[test]
    fn command_reply_serde_roundtrip() {
        let reply = InvokeCommandReply {
            request_id: "req-1".into(),
            status: InvokeCommandStatus::Succeeded,
            value_json: Some("{\"ok\":true}".into()),
            error_json: None,
        };
        let json = serde_json::to_string(&reply).unwrap();
        let decoded: InvokeCommandReply = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, reply);
    }
}
