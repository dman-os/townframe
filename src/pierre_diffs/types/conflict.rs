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

//! Merge-conflict and accept/reject data types (the UI is deferred; the data shapes round-trip).

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MergeConflictMarkerRowType {
    MarkerStart,
    MarkerBase,
    MarkerSeparator,
    MarkerEnd,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MergeConflictMarkerRow {
    #[serde(rename = "type")]
    pub row_type: MergeConflictMarkerRowType,
    pub hunk_index: usize,
    /// Index into `hunk.hunkContent` for the structural block this row belongs to.
    pub content_index: usize,
    pub conflict_index: usize,
    pub line_text: String,
    /// Unified rendered-row index where this virtual row should be injected.
    pub line_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MergeConflictResolution {
    Current,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MergeConflictRegion {
    pub conflict_index: usize,
    pub start_line_index: usize,
    pub start_line_number: usize,
    pub separator_line_index: usize,
    pub separator_line_number: usize,
    pub end_line_index: usize,
    pub end_line_number: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_marker_line_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_marker_line_number: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MergeConflictActionPayload {
    pub resolution: MergeConflictResolution,
    pub conflict: MergeConflictRegion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffAcceptRejectHunkType {
    Accept,
    Reject,
    Both,
}

/// Upstream declares `ConflictResolverTypes` as a separate alias of the same union.
pub type ConflictResolverTypes = MergeConflictResolution;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiffAcceptRejectHunkConfig {
    #[serde(rename = "type")]
    pub hunk_type: DiffAcceptRejectHunkType,
    pub change_index: usize,
}

/// Unresolved merge conflict indexes use three different coordinate spaces:
/// - source line indexes live on `conflict.*LineIndex`
/// - hunk-content indexes live on the fields below, with `startContentIndex`
///   serving as both the conflict-range start and the start-marker anchor
/// - rendered row indexes live on unresolved `markerRows`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessFileConflictData {
    /// Index of the hunk that owns this unresolved conflict.
    pub hunk_index: usize,
    /// First hunk-content entry that belongs to the conflict region.
    pub start_content_index: usize,
    /// Last hunk-content entry that belongs to the conflict region.
    pub end_content_index: usize,
    /// Hunk-content index for the current/ours change block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_content_index: Option<usize>,
    /// Hunk-content index for the optional base context block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_content_index: Option<usize>,
    /// Hunk-content index for the incoming/theirs change block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incoming_content_index: Option<usize>,
    /// Hunk-content index that anchors the end marker row.
    pub end_marker_content_index: usize,
}
