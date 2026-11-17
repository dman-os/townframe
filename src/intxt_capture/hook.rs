//! Claude Code's hook input (ADR 039 §4): the JSON a hook command reads on stdin.
//!
//! Field names are Claude Code's. `session_id`, `transcript_path`, `cwd` and `hook_event_name`
//! are on every event; tool events add `tool_name`, `tool_use_id` and `tool_input` (and a
//! `tool_response` after the call, which is carried, not read). Fields and events this crate does
//! not know are carried, not errors, so a newer Claude Code does not break capture.

use crate::Error;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// The hook events of ADR 039 §4's table, and any other name as it came.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum Event {
    SessionStart,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    UserPromptSubmit,
    Stop,
    SubagentStop,
    PreCompact,
    SessionEnd,
    Other(String),
}

impl From<String> for Event {
    fn from(name: String) -> Self {
        match name.as_str() {
            "SessionStart" => Self::SessionStart,
            "PreToolUse" => Self::PreToolUse,
            "PostToolUse" => Self::PostToolUse,
            "PostToolUseFailure" => Self::PostToolUseFailure,
            "UserPromptSubmit" => Self::UserPromptSubmit,
            "Stop" => Self::Stop,
            "SubagentStop" => Self::SubagentStop,
            "PreCompact" => Self::PreCompact,
            "SessionEnd" => Self::SessionEnd,
            _ => Self::Other(name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Hook {
    pub session_id: String,
    pub transcript_path: PathBuf,
    pub cwd: PathBuf,
    #[serde(rename = "hook_event_name")]
    pub event: Event,
    /// On tool events only.
    pub tool_name: Option<String>,
    /// On tool events only: the id of the transcript's `tool_use` block.
    pub tool_use_id: Option<String>,
    pub tool_input: Option<Value>,
    /// Everything else the event carried.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Hook {
    /// Parse a hook's stdin. A missing required field is an error naming it.
    pub fn parse(input: &[u8]) -> Result<Self, Error> {
        let hook: Self =
            serde_json::from_slice(input).map_err(|error| Error::Hook(error.to_string()))?;
        if matches!(
            hook.event,
            Event::PreToolUse | Event::PostToolUse | Event::PostToolUseFailure
        ) {
            for (name, present) in [
                ("tool_name", hook.tool_name.is_some()),
                ("tool_use_id", hook.tool_use_id.is_some()),
            ] {
                if !present {
                    return Err(Error::Hook(format!(
                        "a {:?} event lacks required field `{name}`",
                        hook.event
                    )));
                }
            }
        }
        Ok(hook)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base(event: &str) -> Value {
        json!({
            "session_id": "s1",
            "transcript_path": "/home/u/.claude/projects/p/s1.jsonl",
            "cwd": "/work/repo",
            "hook_event_name": event
        })
    }

    #[test]
    fn tool_events_carry_their_tool_and_unknown_fields() {
        let mut input = base("PostToolUse");
        input["tool_name"] = json!("Bash");
        input["tool_use_id"] = json!("toolu_01");
        input["tool_input"] = json!({"command": "ls"});
        input["tool_response"] = json!({"stdout": ""});
        input["permission_mode"] = json!("default");
        let hook = Hook::parse(input.to_string().as_bytes()).unwrap();
        assert_eq!(hook.event, Event::PostToolUse);
        assert_eq!(hook.session_id, "s1");
        assert_eq!(hook.tool_name.as_deref(), Some("Bash"));
        assert_eq!(hook.tool_use_id.as_deref(), Some("toolu_01"));
        assert_eq!(hook.tool_input, Some(json!({"command": "ls"})));
        assert_eq!(hook.extra["permission_mode"], "default");
        assert_eq!(hook.extra["tool_response"], json!({"stdout": ""}));
    }

    #[test]
    fn every_table_event_parses_and_an_unknown_one_is_carried() {
        for (name, event) in [
            ("SessionStart", Event::SessionStart),
            ("UserPromptSubmit", Event::UserPromptSubmit),
            ("Stop", Event::Stop),
            ("SubagentStop", Event::SubagentStop),
            ("PreCompact", Event::PreCompact),
            ("SessionEnd", Event::SessionEnd),
            ("Notification", Event::Other("Notification".into())),
        ] {
            let hook = Hook::parse(base(name).to_string().as_bytes()).unwrap();
            assert_eq!(hook.event, event);
        }
    }

    #[test]
    fn a_missing_required_field_is_named() {
        let error = |input: Value| {
            Hook::parse(input.to_string().as_bytes())
                .unwrap_err()
                .to_string()
        };
        for field in ["session_id", "transcript_path", "cwd", "hook_event_name"] {
            let mut input = base("Stop");
            input.as_object_mut().unwrap().remove(field);
            assert!(error(input).contains(field), "{field}");
        }
        let mut input = base("PreToolUse");
        input["tool_name"] = json!("Bash");
        assert!(error(input).contains("tool_use_id"));
        let mut input = base("PostToolUseFailure");
        input["tool_use_id"] = json!("t");
        assert!(error(input).contains("tool_name"));
        assert!(Hook::parse(b"not json").is_err());
    }
}
