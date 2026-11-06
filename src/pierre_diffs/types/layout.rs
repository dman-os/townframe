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

//! Windowing and layout types: `RenderRange`, virtual-file metrics, scroll settings.

use serde::Deserialize;
use serde::Serialize;

fn infinity_as_null<'de, De: serde::Deserializer<'de>>(deserializer: De) -> Result<f64, De::Error> {
    Ok(Option::<f64>::deserialize(deserializer)?.unwrap_or(f64::INFINITY))
}

/// A window of rendered content. Two unit interpretations exist:
///
/// - Renderer consumers (`iterateOverDiff` window predicates, windowed AST
///   requests, buffers, sticky specs) read `startingLine`/`totalLines` as
///   dense rendered-row indexes for the active diff style.
/// - The editor reads them as zero-based document-line indexes of the new
///   file. `FileDiff.computeEditorRenderRange` converts the renderer range on
///   the editor-bound copy; the two coincide only while every line renders
///   (`expandUnchanged`).
///
/// All four fields are JavaScript numbers; `total_lines` is `Infinity` for
/// "no limit" (serialised as `null`, as `JSON.stringify` does).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenderRange {
    pub starting_line: f64,
    #[serde(deserialize_with = "infinity_as_null")]
    pub total_lines: f64,
    pub buffer_before: f64,
    pub buffer_after: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderWindow {
    pub top: f64,
    pub bottom: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualWindowSpecs {
    /// Absolute top edge of the active virtual window in scroll-space pixels.
    pub top: f64,
    /// Absolute bottom edge of the active virtual window in scroll-space pixels.
    pub bottom: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VirtualFileMetrics {
    /// Number of rendered lines per hunk chunk when virtualization batches line rendering.
    pub hunk_line_count: usize,
    /// Estimated single-line row height used before a line is measured.
    pub line_height: f64,
    /// Height reserved for the file or diff header region.
    pub diff_header_height: f64,
    /// Height reserved for each collapsed-context separator row. Only set this
    /// if you customized the size of hunk separators via unsafeCSS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hunk_separator_height: Option<f64>,
    /// Vertical spacing used around hunks and file-level padding. You should not
    /// change this from the default if you aren't applying custom CSS.
    pub spacing: f64,
    /// Optional top padding applied after the file header, or before content
    /// when the header is disabled. Defaults to 0 with a header, otherwise
    /// defaults to spacing if header is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub padding_top: Option<f64>,
    /// Optional bottom padding applied after file content, and only if there is
    /// code for the diff. Defaults to spacing if none provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub padding_bottom: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PendingCodeViewLayoutReset {
    pub metrics: Option<VirtualFileMetrics>,
    pub reset_file_layout_cache: bool,
    pub reset_diff_layout_cache: bool,
    pub include_estimated_diff_heights: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeViewLayout {
    /// Top padding applied to the CodeView sticky container offset.
    pub padding_top: f64,
    /// Bottom padding added after the final rendered item in CodeView.
    pub padding_bottom: f64,
    /// Vertical gap between virtualized CodeView items.
    pub gap: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SmoothScrollSettings {
    /// Natural frequency of the critically-damped spring, in rad/ms. 99% settle
    /// takes roughly `6.6 / omega`; 0.015 gives ~440ms. Raise for a snappier
    /// animation; lower for a longer glide.
    pub omega: f64,
    /// Distance from destination (in CSS pixels) below which the spring is
    /// considered settled. Must also clear `velocityEpsilon` before the
    /// animation actually stops and snaps to destination.
    pub position_epsilon: f64,
    /// Velocity magnitude (in CSS pixels per millisecond) below which the
    /// spring is considered effectively stationary. Pairs with
    /// `positionEpsilon` to gate the settle transition.
    pub velocity_epsilon: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StickySpecs {
    pub top_offset: f64,
    pub height: f64,
}
