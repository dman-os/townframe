//! What happened in a session, as the page shows it: a bounded fold of its `promptTurn` datums.

use crate::view::spelling;
use intxt_core::work::MessageKind;
use intxt_core::work::PromptTurn;
use intxt_core::work::ToolKind;
use jiff::Timestamp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// What happened in a session, folded from its `promptTurn` datums one at a time (see
/// [`SessionSummary::add`]), so a host reads the turns in batches and never holds more than a
/// batch. Everything kept is bounded: counts are numbers, at most [`Self::MAX_PROMPTS`] prompts
/// are kept with their text cut at [`Self::MAX_PROMPT_OCTETS`], and at most
/// [`Self::MAX_EDITED_TRACKED`] distinct edited paths are kept (the smallest in sort order, so
/// what is kept does not depend on the order turns arrive in).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSummary {
    /// The earliest and latest turn start.
    pub span: Option<(Timestamp, Timestamp)>,
    pub turns: usize,
    pub tool_calls: usize,
    /// Turns the host could not read (absent, not accepted yet, or not a turn). They are in none
    /// of the other numbers, and the page says so.
    pub unreadable: usize,
    /// Tool calls by `ToolKind` (`edit`, `execute`, ...), as the harness spelled it.
    pub tools: BTreeMap<String, usize>,
    /// The paths an edit, delete or move call touched, at most [`Self::MAX_EDITED_TRACKED`].
    edited: BTreeSet<String>,
    /// Paths that did not fit in `edited`, counted once per touch: past the bound the number of
    /// files is "at least this many more", not exact.
    edited_dropped: usize,
    /// The operator's first [`Self::MAX_PROMPTS`] prompts in session order, each under its sort key
    /// (turn `seq`, message `seq`).
    prompts: Vec<((u64, u64), Prompt)>,
    prompt_count: usize,
}

/// One prompt the operator gave, as far as the page shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Prompt {
    /// The first non-empty line, cut to [`SessionSummary::MAX_HEADLINE_OCTETS`].
    pub headline: String,
    /// The whole prompt, cut to [`SessionSummary::MAX_PROMPT_OCTETS`].
    pub text: String,
    /// The text was cut: the prompt is longer than what is kept.
    pub cut: bool,
}

/// `text` cut to at most `max` octets at a character boundary, and whether it was cut.
pub(crate) fn cut(text: &str, max: usize) -> (&str, bool) {
    if text.len() <= max {
        return (text, false);
    }
    let end = (0..=max)
        .rev()
        .find(|&end| text.is_char_boundary(end))
        .expect("0 is a boundary");
    (&text[..end], true)
}

impl SessionSummary {
    /// The prompts kept.
    pub const MAX_PROMPTS: usize = 50;
    /// The most octets of one prompt's text kept.
    pub const MAX_PROMPT_OCTETS: usize = 4096;
    /// The most octets of a prompt's first line kept.
    pub const MAX_HEADLINE_OCTETS: usize = 120;
    /// The edited paths [`Self::edited_files`] gives.
    pub const MAX_EDITED_FILES: usize = 20;
    /// The distinct edited paths held, which is what makes [`Self::edited_omitted`] exact below it.
    pub const MAX_EDITED_TRACKED: usize = 1000;

    /// Folds one turn in. Turns may come in any order (a document lists its datums by key, not by
    /// `seq`); the summary orders what it keeps by `seq`.
    pub fn add(&mut self, turn: &PromptTurn) {
        self.turns += 1;
        self.span = Some(match self.span {
            Some((first, last)) => (first.min(turn.at), last.max(turn.at)),
            None => (turn.at, turn.at),
        });
        for call in &turn.tool_calls {
            self.tool_calls += 1;
            *self.tools.entry(spelling(&call.kind)).or_default() += 1;
            if matches!(
                call.kind,
                ToolKind::Edit | ToolKind::Delete | ToolKind::Move
            ) {
                // A harness that reports diffs names the files in them; one that only says where it
                // acted names them in the locations.
                let paths: Vec<&str> = if call.diffs.is_empty() {
                    call.locations.iter().map(|at| at.path.as_str()).collect()
                } else {
                    call.diffs.iter().map(|diff| diff.path.as_str()).collect()
                };
                for path in paths {
                    if self.edited.len() >= Self::MAX_EDITED_TRACKED
                        && !self.edited.contains(path)
                        && self.edited.last().is_some_and(|last| last.as_str() < path)
                    {
                        self.edited_dropped += 1;
                        continue;
                    }
                    if self.edited.insert(path.to_owned())
                        && self.edited.len() > Self::MAX_EDITED_TRACKED
                    {
                        self.edited.pop_last();
                        self.edited_dropped += 1;
                    }
                }
            }
        }
        for message in &turn.messages {
            if message.kind != MessageKind::UserMessage {
                continue;
            }
            self.prompt_count += 1;
            let order = (turn.seq, message.seq);
            let at = self.prompts.partition_point(|(held, _)| *held < order);
            if at >= Self::MAX_PROMPTS {
                continue;
            }
            let (text, cut_text) = cut(&message.text, Self::MAX_PROMPT_OCTETS);
            let line = text.lines().find(|line| !line.trim().is_empty());
            let (headline, cut_line) = cut(line.unwrap_or("").trim(), Self::MAX_HEADLINE_OCTETS);
            let prompt = Prompt {
                headline: if cut_line {
                    format!("{headline}…")
                } else {
                    headline.to_owned()
                },
                text: text.to_owned(),
                cut: cut_text,
            };
            self.prompts.insert(at, (order, prompt));
            self.prompts.truncate(Self::MAX_PROMPTS);
        }
    }

    /// The prompts kept, in session order.
    pub fn prompts(&self) -> impl Iterator<Item = &Prompt> {
        self.prompts.iter().map(|(_, prompt)| prompt)
    }

    /// How many prompts the session has beyond those kept.
    pub fn prompts_omitted(&self) -> usize {
        self.prompt_count - self.prompts.len()
    }

    /// The first [`Self::MAX_EDITED_FILES`] edited paths, sorted.
    pub fn edited_files(&self) -> impl Iterator<Item = &str> {
        self.edited
            .iter()
            .take(Self::MAX_EDITED_FILES)
            .map(String::as_str)
    }

    /// How many edited paths there are beyond those [`Self::edited_files`] gives, and whether the
    /// number is a lower bound (more paths were touched than are tracked).
    pub fn edited_omitted(&self) -> (usize, bool) {
        (
            self.edited.len().saturating_sub(Self::MAX_EDITED_FILES) + self.edited_dropped,
            self.edited_dropped > 0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn turn(
        seq: u64,
        at: &str,
        messages: &[(&str, &str)],
        calls: &[serde_json::Value],
    ) -> PromptTurn {
        let messages: Vec<_> = messages
            .iter()
            .enumerate()
            .map(|(index, (kind, text))| {
                json!({"id": format!("m{seq}-{index}"), "seq": index, "at": at, "kind": kind, "text": text})
            })
            .collect();
        serde_json::from_value(
            json!({"seq": seq, "at": at, "messages": messages, "toolCalls": calls}),
        )
        .unwrap()
    }

    fn call(kind: &str, diff_paths: &[&str], location_paths: &[&str]) -> serde_json::Value {
        json!({
            "id": "c", "seq": 1, "title": "t", "kind": kind, "status": "completed",
            "diffs": diff_paths.iter().map(|path| json!({"path": path, "patch": ""})).collect::<Vec<_>>(),
            "locations": location_paths.iter().map(|path| json!({"path": path})).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn a_summary_counts_turns_calls_and_edited_files_whatever_the_order() {
        let mut summary = SessionSummary::default();
        summary.add(&turn(
            2,
            "2026-11-03T12:00:00Z",
            &[("userMessage", "second")],
            &[
                call("edit", &[], &["/a.rs"]),
                call("move", &["/b.rs", "/c.rs"], &[]),
            ],
        ));
        summary.add(&turn(
            1,
            "2026-11-03T10:00:00Z",
            &[
                ("userMessage", "first"),
                ("agentMessage", "no"),
                ("notification", "no"),
            ],
            &[
                call("read", &[], &["/never.rs"]),
                call("edit", &["/a.rs"], &["/x.rs"]),
            ],
        ));
        assert_eq!((summary.turns, summary.tool_calls), (2, 4));
        assert_eq!(
            summary.tools,
            BTreeMap::from([("edit".into(), 2), ("move".into(), 1), ("read".into(), 1)])
        );
        // Diffs name the files when there are any, else the locations; reads touch nothing.
        assert_eq!(
            summary.edited_files().collect::<Vec<_>>(),
            ["/a.rs", "/b.rs", "/c.rs"]
        );
        let (first, last) = summary.span.unwrap();
        assert_eq!(
            (first.to_string(), last.to_string()),
            ("2026-11-03T10:00:00Z".into(), "2026-11-03T12:00:00Z".into())
        );
        // Only the operator's own messages, in session order and not in the order they arrived.
        let prompts: Vec<_> = summary.prompts().map(|p| p.text.as_str()).collect();
        assert_eq!(prompts, ["first", "second"]);
    }

    #[test]
    fn prompts_are_cut_and_capped_without_losing_their_order() {
        let mut summary = SessionSummary::default();
        let long_line = "é".repeat(SessionSummary::MAX_HEADLINE_OCTETS);
        let huge = format!(
            "\n\n  {long_line}\nsecond line {}",
            "y".repeat(SessionSummary::MAX_PROMPT_OCTETS)
        );
        // Newest first, more than the cap.
        for seq in (0..SessionSummary::MAX_PROMPTS as u64 + 5).rev() {
            let text = if seq == 0 { huge.as_str() } else { "short" };
            summary.add(&turn(
                seq,
                "2026-11-03T10:00:00Z",
                &[("userMessage", text)],
                &[],
            ));
        }
        assert_eq!(summary.prompts().count(), SessionSummary::MAX_PROMPTS);
        assert_eq!(summary.prompts_omitted(), 5);
        let first = summary.prompts().next().unwrap();
        // The headline is the first non-empty line, cut on a character boundary with an ellipsis.
        assert!(first.headline.ends_with('…'), "{}", first.headline);
        assert!(first.headline.len() <= SessionSummary::MAX_HEADLINE_OCTETS + '…'.len_utf8());
        assert!(first.cut);
        assert!(first.text.len() <= SessionSummary::MAX_PROMPT_OCTETS);
        assert!(
            summary
                .prompts()
                .skip(1)
                .all(|p| p.headline == "short" && !p.cut)
        );
    }

    #[test]
    fn edited_files_are_capped_and_the_rest_counted() {
        let mut summary = SessionSummary::default();
        let calls: Vec<_> = (0..SessionSummary::MAX_EDITED_FILES + 3)
            .map(|index| call("edit", &[&format!("/f{index:03}.rs")], &[]))
            .collect();
        summary.add(&turn(1, "2026-11-03T10:00:00Z", &[], &calls));
        assert_eq!(
            summary.edited_files().count(),
            SessionSummary::MAX_EDITED_FILES
        );
        assert_eq!(summary.edited_omitted(), (3, false));
    }

    #[test]
    fn tracked_edited_paths_are_bounded_and_deterministic() {
        let paths: Vec<String> = (0..SessionSummary::MAX_EDITED_TRACKED + 7)
            .map(|index| format!("/f{index:05}.rs"))
            .collect();
        let fold = |order: Vec<&String>| {
            let mut summary = SessionSummary::default();
            for path in order {
                summary.add(&turn(
                    1,
                    "2026-11-03T10:00:00Z",
                    &[],
                    &[call("edit", &[path], &[])],
                ));
            }
            summary
        };
        let forward = fold(paths.iter().collect());
        let backward = fold(paths.iter().rev().collect());
        // The smallest paths are the ones kept, whatever order the turns came in.
        assert_eq!(forward.edited, backward.edited);
        assert_eq!(forward.edited.len(), SessionSummary::MAX_EDITED_TRACKED);
        assert_eq!(
            forward.edited_omitted(),
            (
                SessionSummary::MAX_EDITED_TRACKED - SessionSummary::MAX_EDITED_FILES + 7,
                true
            )
        );
    }
}
