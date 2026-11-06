// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/parseDiffDecorations.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `DecorationItem` is declared in `highlighter/types.ts` upstream
// (the highlighter adapter, S7); the subset used here is defined in this file
// and will move with the adapter. `character` offsets are UTF-16 code units
// (`crate::utf16`), as upstream's `value.length`; `ChangeObject` is
// `crate::diff::ChangeObject`.

use std::collections::BTreeMap;

use crate::diff::ChangeObject;
use crate::utf16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecorationPosition {
    pub line: usize,
    pub character: usize,
}

/// The `DecorationItem` fields `createDiffSpanDecoration` sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecorationItem {
    pub start: DecorationPosition,
    pub end: DecorationPosition,
    pub properties: BTreeMap<String, String>,
    pub always_wrap: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct CreateDiffSpanDecorationProps {
    pub line: usize,
    pub span_start: usize,
    pub span_length: usize,
}

pub fn create_diff_span_decoration(props: CreateDiffSpanDecorationProps) -> DecorationItem {
    let CreateDiffSpanDecorationProps {
        line,
        span_start,
        span_length,
    } = props;
    DecorationItem {
        start: DecorationPosition {
            line,
            character: span_start,
        },
        end: DecorationPosition {
            line,
            character: span_start + span_length,
        },
        properties: BTreeMap::from([(String::from("data-diff-span"), String::new())]),
        always_wrap: true,
    }
}

/// One span of a line: `highlighted` is upstream's `1`, `false` its `0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    pub highlighted: bool,
    pub text: String,
}

#[derive(Debug, Clone, Copy)]
pub struct PushOrJoinSpanProps<'item> {
    pub item: &'item ChangeObject,
    pub enable_join: bool,
    pub is_neutral: bool,
    pub is_last_item: bool,
}

// For diff decoration spans, we want to be sure that if there is a single
// white-space gap between diffs that we join them together into a longer diff span.
// Spans are basically just a tuple - highlighted means the content should be
// highlighted, otherwise not, we still need to the span data to figure
// out span positions
pub fn push_or_join_span(spans: &mut Vec<Span>, props: PushOrJoinSpanProps<'_>) {
    let PushOrJoinSpanProps {
        item,
        enable_join,
        is_neutral,
        is_last_item,
    } = props;
    let new_span = || Span {
        highlighted: !is_neutral,
        text: item.value.clone(),
    };
    let Some(last_item) = spans.last_mut() else {
        spans.push(new_span());
        return;
    };
    if is_last_item || !enable_join {
        spans.push(new_span());
        return;
    }
    let is_last_item_neutral = !last_item.highlighted;
    if is_neutral == is_last_item_neutral
        // If we have a single space neutral item, lets join it to a previous
        // space non-neutral item to avoid single space gaps
        || (is_neutral && utf16::len(&item.value) == 1 && !is_last_item_neutral)
    {
        last_item.text.push_str(&item.value);
        return;
    }
    spans.push(new_span());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(value: &str) -> ChangeObject {
        ChangeObject {
            count: 1,
            added: false,
            removed: false,
            value: value.to_owned(),
        }
    }

    fn push(
        spans: &mut Vec<Span>,
        value: &str,
        is_neutral: bool,
        enable_join: bool,
        is_last_item: bool,
    ) {
        push_or_join_span(
            spans,
            PushOrJoinSpanProps {
                item: &item(value),
                enable_join,
                is_neutral,
                is_last_item,
            },
        );
    }

    fn span(highlighted: bool, text: &str) -> Span {
        Span {
            highlighted,
            text: text.to_owned(),
        }
    }

    #[test]
    fn span_decoration_covers_the_span_on_one_line() {
        let decoration = create_diff_span_decoration(CreateDiffSpanDecorationProps {
            line: 3,
            span_start: 5,
            span_length: 4,
        });
        assert_eq!(
            decoration.start,
            DecorationPosition {
                line: 3,
                character: 5
            }
        );
        assert_eq!(
            decoration.end,
            DecorationPosition {
                line: 3,
                character: 9
            }
        );
        assert!(decoration.always_wrap);
        assert_eq!(
            decoration
                .properties
                .get("data-diff-span")
                .map(String::as_str),
            Some("")
        );
    }

    #[test]
    fn same_kind_neighbours_join_when_joining_is_enabled() {
        let mut spans = Vec::new();
        push(&mut spans, "ab", false, true, false);
        push(&mut spans, "cd", false, true, false);
        assert_eq!(spans, [span(true, "abcd")]);
    }

    #[test]
    fn a_single_space_neutral_item_joins_the_previous_highlighted_span() {
        let mut spans = Vec::new();
        push(&mut spans, "ab", false, true, false);
        push(&mut spans, " ", true, true, false);
        push(&mut spans, "cd", false, true, false);
        // The single space bridges the gap, then `cd` (highlighted again) joins.
        assert_eq!(spans, [span(true, "ab cd")]);
    }

    #[test]
    fn longer_neutral_runs_and_the_last_item_start_new_spans() {
        let mut spans = Vec::new();
        push(&mut spans, "ab", false, true, false);
        push(&mut spans, "  ", true, true, false);
        push(&mut spans, "cd", false, true, true);
        assert_eq!(
            spans,
            [span(true, "ab"), span(false, "  "), span(true, "cd")]
        );

        let mut unjoined = Vec::new();
        push(&mut unjoined, "ab", false, false, false);
        push(&mut unjoined, "cd", false, false, false);
        assert_eq!(unjoined, [span(true, "ab"), span(true, "cd")]);
    }
}
