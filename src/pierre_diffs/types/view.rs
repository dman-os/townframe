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

//! Annotation, selection and CodeView item/scroll-target types.

use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodeViewScrollBehavior {
    Instant,
    Smooth,
    SmoothAuto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnnotationSide {
    Deletions,
    Additions,
}

/// Upstream declares `SelectionSide` as a separate alias of the same union.
pub type SelectionSide = AnnotationSide;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SelectedLineRange {
    pub start: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<SelectionSide>,
    pub end: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_side: Option<SelectionSide>,
}

/// Annotation rendered for a file line. Use `lineNumber: 0` to render a
/// file-level annotation above the first rendered file line.
///
/// Upstream's `OptionalMetadata<LAnnotation>` makes `metadata` absent for
/// `LAnnotation = undefined` and required otherwise; here it is `Option<T>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LineAnnotation<Metadata> {
    pub line_number: usize,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

/// Annotation rendered for one side of a diff line. Use `lineNumber: 0` to
/// render a side-specific file-level annotation above the first hunk/separator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffLineAnnotation<Metadata> {
    pub side: AnnotationSide,
    pub line_number: usize,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewFileItem<Metadata> {
    pub id: String,
    pub file: FileContents,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Vec<LineAnnotation<Metadata>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collapsed: Option<bool>,
    /// Put this item into edit mode. Requires `EditProvider` in React or the
    /// CodeView `createEditor` option in vanilla; ignored while `collapsed` is
    /// true. Make sure you bump the version when also changing the value. Edit
    /// mode is out of scope for this port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewDiffItem<Metadata> {
    pub id: String,
    pub file_diff: FileDiffMetadata,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Vec<DiffLineAnnotation<Metadata>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collapsed: Option<bool>,
    /// See [`CodeViewFileItem::edit`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit: Option<bool>,
}

/// `CodeViewFileItem | CodeViewDiffItem`, discriminated by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CodeViewItem<Metadata> {
    File(CodeViewFileItem<Metadata>),
    Diff(CodeViewDiffItem<Metadata>),
}

/// `'start' | 'center' | 'end' | 'nearest'`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScrollAlign {
    Start,
    Center,
    End,
    Nearest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewPositionScrollTarget {
    pub position: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<CodeViewScrollBehavior>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewLineScrollTarget {
    pub id: String,
    pub line_number: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<SelectionSide>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub align: Option<ScrollAlign>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<CodeViewScrollBehavior>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewRangeScrollTarget {
    pub id: String,
    pub range: SelectedLineRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub align: Option<ScrollAlign>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<CodeViewScrollBehavior>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NumericScrollLineAnchor {
    pub line_number: usize,
    pub top: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<SelectionSide>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewItemScrollTarget {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub align: Option<ScrollAlign>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<CodeViewScrollBehavior>,
}

/// The four `CodeView*ScrollTarget` interfaces, discriminated by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CodeViewScrollTarget {
    Position(CodeViewPositionScrollTarget),
    Line(CodeViewLineScrollTarget),
    Range(CodeViewRangeScrollTarget),
    Item(CodeViewItemScrollTarget),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionPoint {
    #[serde(rename = "lineNumber")]
    pub line_number: usize,
    pub side: Option<SelectionSide>,
}
