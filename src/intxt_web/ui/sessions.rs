//! The sessions tab: one block per recorded session, with the summary and the operator's prompts.

use super::RefList;
use crate::session::SessionSummary;
use crate::view::IntxtView;
use dioxus::prelude::*;

pub(super) fn sessions(view: &IntxtView) -> Element {
    let sessions: Vec<_> = view.sessions().collect();
    if sessions.is_empty() {
        return rsx! {
            p { class: "intxt-muted", "No sessions are recorded in this intxt." }
        };
    }
    rsx! {
        for entry in sessions {
            section { class: "intxt-revision",
                h3 { "{entry.label}" }
                RefList { refs: entry.refs.clone() }
                if let Some(summary) = &entry.session {
                    Session { summary: summary.clone() }
                }
            }
        }
    }
}

/// What happened in a session. Prompt text is a text node, so the operator's words are escaped
/// like any other text on the page.
#[component]
fn Session(summary: SessionSummary) -> Element {
    let when = |at: jiff::Timestamp| at.strftime("%Y-%m-%d %H:%M").to_string();
    let tools = summary
        .tools
        .iter()
        .map(|(kind, count)| format!("{kind} {count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (omitted, at_least) = summary.edited_omitted();
    let plus = if at_least { "+" } else { "" };
    rsx! {
        div { class: "intxt-session",
            p { class: "intxt-muted intxt-small",
                "{summary.turns} prompt turns, {summary.tool_calls} tool calls"
                if let Some((first, last)) = summary.span {
                    ", {when(first)} to {when(last)} UTC"
                }
            }
            if summary.unreadable > 0 {
                p { class: "intxt-muted intxt-small",
                    "{summary.unreadable} turns could not be read and are not counted."
                }
            }
            if !tools.is_empty() {
                p { class: "intxt-muted intxt-small", "tools: {tools}" }
            }
            if summary.edited_files().next().is_some() {
                p { class: "intxt-muted intxt-small", "edited" }
                ul { class: "intxt-session-files",
                    for path in summary.edited_files() {
                        li { code { "{path}" } }
                    }
                }
                if omitted > 0 {
                    p { class: "intxt-muted intxt-small", "and {omitted}{plus} more files" }
                }
            }
            for prompt in summary.prompts() {
                details { class: "intxt-prompt",
                    summary { "{prompt.headline}" }
                    p { class: "intxt-prompt-text", "{prompt.text}" }
                    if prompt.cut {
                        p { class: "intxt-muted intxt-small", "The prompt is cut here." }
                    }
                }
            }
            if summary.prompts_omitted() > 0 {
                p { class: "intxt-muted intxt-small", "and {summary.prompts_omitted()} more prompts" }
            }
        }
    }
}
