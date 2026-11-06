// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/types.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: TypeScript string-literal unions are enums with serde renames that
// reproduce the exact JSON strings; `type`-tagged object unions
// (`ContextContent | ChangeContent`, the CodeView items and scroll targets)
// are internally tagged enums whose payload structs omit the tag field;
// `number` is `usize` for counts and indexes and `f64` for pixels and offsets
// (`RenderRange.totalLines` may be `Infinity`, which JSON writes as `null`);
// `LineAnnotation<T>`'s conditional `metadata` is `Option<T>`; `Required<Omit<..>>`
// becomes an explicit struct. NOT ported here, with the matrix row for each:
// the `export type { .. } from './highlighter/..'` re-exports (S7), callback
// types (`FileDiffContentsLoader`, `Render*Callback`; closures belong to the
// consuming app), types holding DOM elements (`LineEventBaseProps`,
// `DiffLineEventBaseProps`, `TokenEventBase`, `DiffTokenEventBaseProps`,
// `ObservedAnnotationNodes`, `ObservedGridNodes`), and types holding hast
// (`ThemedFileResult`, `RenderDiffFilesResult`, `ThemedDiffResult`,
// `RenderFileResult`, `RenderDiffResult`, `RenderedFileASTCache`,
// `RenderedDiffASTCache`, `SharedRenderState`), which arrive with the hast
// model in S5/S6.

//! Rendered-line model: line info, hunk data, expansion regions and the plain-text force options.

use crate::types::view::DiffLineAnnotation;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GapSpan {
    pub rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnnotationSpan {
    pub hunk_index: usize,
    pub line_index: usize,
    pub annotations: Vec<String>,
}

/// `GapSpan | AnnotationSpan`, discriminated by `type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LineSpans {
    Gap(GapSpan),
    Annotation(AnnotationSpan),
}

/// Types of rendered lines in a rendered diff.
// Should we have yet another type for files? seems silly for
// use to have a type in that case?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LineTypes {
    ChangeDeletion,
    ChangeAddition,
    Context,
    ContextExpanded,
}

/// `number | `${number},${number}``: a plain index, or a split-view pair
/// serialised as `"<deletion>,<addition>"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "LineIndexRepr", into = "LineIndexRepr")]
pub enum LineIndex {
    Single(usize),
    Pair(usize, usize),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum LineIndexRepr {
    Number(usize),
    Text(String),
}

impl From<LineIndex> for LineIndexRepr {
    fn from(value: LineIndex) -> Self {
        match value {
            LineIndex::Single(index) => Self::Number(index),
            LineIndex::Pair(first, second) => Self::Text(format!("{first},{second}")),
        }
    }
}

impl TryFrom<LineIndexRepr> for LineIndex {
    type Error = String;

    fn try_from(value: LineIndexRepr) -> Result<Self, String> {
        match value {
            LineIndexRepr::Number(index) => Ok(Self::Single(index)),
            LineIndexRepr::Text(text) => {
                let (first, second) = text
                    .split_once(',')
                    .ok_or_else(|| format!("line index {text:?} is not \"<number>,<number>\""))?;
                let parse = |part: &str| {
                    part.parse::<usize>()
                        .map_err(|error| format!("line index {text:?}: {error}"))
                };
                Ok(Self::Pair(parse(first)?, parse(second)?))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LineInfo {
    #[serde(rename = "type")]
    pub line_type: LineTypes,
    pub line_number: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alt_line_number: Option<usize>,
    pub line_index: LineIndex,
}

/// `'unified' | 'additions' | 'deletions'`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodeColumnType {
    Unified,
    Additions,
    Deletions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HunkDataExpandable {
    pub chunked: bool,
    pub up: bool,
    pub down: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HunkData {
    pub slot_name: String,
    pub hunk_index: usize,
    pub lines: usize,
    pub line_count_known: bool,
    #[serde(rename = "type")]
    pub column_type: CodeColumnType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expandable: Option<HunkDataExpandable>,
}

/// `Record<number, DiffLineAnnotation<LAnnotation>[] | undefined>`
pub type AnnotationLineMap<Metadata> = HashMap<usize, Option<Vec<DiffLineAnnotation<Metadata>>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExpansionDirections {
    Up,
    Down,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HunkExpansionRegion {
    pub from_start: usize,
    pub from_end: usize,
}

/// `Map<number, HunkExpansionRegion> | true`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandedHunks {
    All,
    Regions(HashMap<usize, HunkExpansionRegion>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForceDiffPlainTextOptions {
    pub force_plain_text: bool,
    pub lazy_line_ast: Option<bool>,
    pub starting_line: Option<usize>,
    pub total_lines: Option<usize>,
    pub expanded_hunks: Option<ExpandedHunks>,
    pub collapsed_context_threshold: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceFilePlainTextOptions {
    pub force_plain_text: bool,
    pub lazy_line_ast: Option<bool>,
    pub starting_line: Option<usize>,
    pub total_lines: Option<usize>,
    // Pre-split lines for caching in windowing scenarios
    pub lines: Option<Vec<String>>,
}
