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

//! Patch and file-diff model: `FileDiffMetadata`, `Hunk` and the inputs that produce them.

use crate::diff::PatchOptions;
use serde::Deserialize;
use serde::Serialize;

/// Parser options used when computing diffs from file contents: jsdiff's
/// `CreatePatchOptionsNonabortable` plus `disableLineMatching`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParseDiffOptions {
    pub create_patch: PatchOptions,
    /// Preserve the patch's change blocks and line order without similarity
    /// realignment. Defaults to false.
    pub disable_line_matching: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsePatchOptions {
    /// Preserve the patch's change blocks and line order without similarity
    /// realignment. Defaults to false.
    pub disable_line_matching: Option<bool>,
    /// Prefix used to derive cache keys for parsed files, enabling worker-pool caching.
    pub cache_key_prefix: Option<String>,
    /// Throw on invalid patch data. Defaults to false: errors are reported
    /// and recovery is best-effort, without guaranteed valid output.
    pub throw_on_error: Option<bool>,
}

/// A language id: a Shiki `BundledLanguage`, `text`, `ansi`, or any string.
pub type SupportedLanguages = String;

/// Represents a file's contents for generating diffs via `parseDiffFromFile`
/// or for when rendering a file directly using the File components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileContents {
    /// Filename used for display in headers and for inferring the language for
    /// syntax highlighting.
    pub name: String,
    /// The raw text contents of the file.
    pub contents: String,
    /// Explicitly set the syntax highlighting language instead of inferring from
    /// filename. Generally you should not be setting this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lang: Option<SupportedLanguages>,
    /// Optional header passed to the jsdiff library's `createTwoFilesPatch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// Identifies a file for worker pool highlight caching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
}

/// `FileDiffLoadedFiles`: what `loadDiffFiles` resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileDiffLoadedFiles {
    /// `FileDiffLoadedChangedFiles`
    Changed {
        old_file: FileContents,
        new_file: FileContents,
    },
    /// `FileDiffLoadedPureRenamedFile` (`oldFile: null`)
    PureRenamed { new_file: FileContents },
}

/// `DiffFileInput`: at least one side is present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffFileInput {
    Both {
        old_file: FileContents,
        new_file: FileContents,
    },
    /// `oldFile: null`
    Added { new_file: FileContents },
    /// `newFile: null`
    Deleted { old_file: FileContents },
}

/// `MaybeDiffFileInput` is `Option<DiffFileInput>`.
pub type MaybeDiffFileInput = Option<DiffFileInput>;

/// Describes the type of change for a file in a diff.
/// - `change`: File content was modified, name unchanged.
/// - `rename-pure`: File was renamed/moved without content changes (100% similarity).
/// - `rename-changed`: File was renamed/moved and content was also modified.
/// - `new`: A new file was added.
/// - `deleted`: An existing file was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChangeTypes {
    Change,
    RenamePure,
    RenameChanged,
    New,
    Deleted,
}

/// Represents a parsed patch file, typically corresponding to a single commit.
/// Returned by `parsePatchFiles` when parsing raw patch/diff strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParsedPatch {
    /// Optional raw introductory text before the file diffs that may have been
    /// included in the patch (e.g., commit message, author, date).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch_metadata: Option<String>,
    /// Array of file changes contained in the patch.
    pub files: Vec<FileDiffMetadata>,
}

/// A count as a signed line-index delta. Line indexes are signed because
/// upstream's `start - 1` is `-1` for a side that starts at line 0 (an added
/// file's deletion side, a deleted file's addition side, any zero-length
/// range at the top of a file); that "no line on this side" index is
/// preserved rather than saturated, so parsed hunks equal upstream's.
pub fn signed(count: usize) -> i64 {
    i64::try_from(count).expect("line counts fit in i64")
}

/// The array index of a line index that points at a line. Panics on the
/// `-1` of an empty side: only callers that have already established the side
/// has lines (count > 0) may use it.
pub fn line_slot(line_index: i64) -> usize {
    usize::try_from(line_index).expect("a side with lines has a non-negative line index")
}

/// Represents a block of unchanged context lines within a hunk. Basically a
/// batch of lines in a hunk that are prefixed with a space ` `. Consecutive
/// lines prefixed with a ` ` are grouped together into a single ContextContent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContextContent {
    /// Number of unchanged lines in this context block.
    pub lines: usize,
    /// Zero-based index into `FileDiffMetadata.additionLines` where this context
    /// block starts.
    pub addition_line_index: i64,
    /// Zero-based index into `FileDiffMetadata.deletionLines` where this context
    /// block starts.
    pub deletion_line_index: i64,
}

/// Represents a block of changes (additions and/or deletions) within a hunk.
/// Consecutive `+` and `-` lines are grouped together into a single
/// ChangeContent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChangeContent {
    /// Number of lines prefixed with `-` in this change block.
    pub deletions: usize,
    /// Zero-based index into `FileDiffMetadata.deletionLines` where the deleted
    /// lines start.
    pub deletion_line_index: i64,
    /// Number of lines prefixed with `+` in this change block.
    pub additions: usize,
    /// Zero-based index into `FileDiffMetadata.additionLines` where the added
    /// lines start.
    pub addition_line_index: i64,
}

/// `ContextContent | ChangeContent`, discriminated by `type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum HunkContent {
    Context(ContextContent),
    Change(ChangeContent),
}

/// Represents a single hunk from a diff, corresponding to one `@@ ... @@`
/// block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Hunk {
    /// Number of unchanged lines between the previous hunk (or file start) and
    /// this hunk.
    pub collapsed_before: usize,

    /// Starting line number in the new file version, parsed from the `+X`
    /// in the hunk header.
    pub addition_start: usize,
    /// Total line count in the new file version for this hunk, parsed from
    /// `+X,count` in the hunk header. If this hunk was viewed in `diffStyle:
    /// split` this would correspond to the number of lines in the right
    /// `additions` column. It includes both `context` lines and lines
    /// prefixed with `+`.
    pub addition_count: usize,
    /// This corresponds to the number of lines prefixed with `+` in this hunk.
    pub addition_lines: usize,
    /// Zero-based index into `FileDiffMetadata.additionLines` where this hunk's
    /// content starts.
    pub addition_line_index: i64,

    /// Starting line number in the old file version, parsed from the `-X`
    /// in the hunk header.
    pub deletion_start: usize,
    /// Total line count in the old file version for this hunk, parsed from
    /// `-X,count` in the hunk header. If this hunk was viewed in `diffStyle:
    /// split` this would correspond to the number of lines in the left
    /// `deletions` column. It includes both `context` lines and lines
    /// prefixed with `-`.
    pub deletion_count: usize,
    /// This corresponds to the number of lines prefixed with `-` in this hunk.
    pub deletion_lines: usize,
    /// Zero-based index into `FileDiffMetadata.deletionLines` where this hunk's
    /// content starts.
    pub deletion_line_index: i64,

    /// Array of content segments within this hunk, each representing either
    /// a context line group or a change group.
    pub hunk_content: Vec<HunkContent>,
    /// Function/method name that appears after the `@@` markers if it existed in
    /// the diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hunk_context: Option<String>,
    /// Raw hunk header string (e.g., `@@ -1,5 +1,7 @@`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hunk_specs: Option<String>,

    /// Starting line index for this hunk when rendered in split (side-by-side)
    /// view.
    pub split_line_start: usize,
    /// Total rendered line count for this hunk in split view.
    pub split_line_count: usize,

    /// Starting line index for this hunk when rendered in unified view.
    pub unified_line_start: usize,
    /// Total rendered line count for this hunk in unified view.
    pub unified_line_count: usize,

    /// True if the old file version has no trailing newline at end of file. This
    /// is parsed from the patch file directly at the end of the hunk. If the
    /// final hunkContent is a `context` group, then both values will be true or
    /// false together. If it's from a `change` content group, then it may differ
    /// depending on the patch.
    #[serde(rename = "noEOFCRDeletions")]
    pub no_eofcr_deletions: bool,
    /// True if the new file version has no trailing newline at end of file. This
    /// is parsed from the patch file directly at the end of the hunk. If the
    /// final hunkContent is a `context` group, then both values will be true or
    /// false together. If it's from a `change` content group, then it may differ
    /// depending on the patch.
    #[serde(rename = "noEOFCRAdditions")]
    pub no_eofcr_additions: bool,
}

/// Metadata and content for a single file's diff. Think of this as a JSON
/// compatible representation of a diff for a single file.
///
/// When a renderer uses `loadDiffFiles` to hydrate a partial diff, it upgrades
/// this metadata object in place. Keep the same object identity stable when
/// callers want that hydrated state to persist across later renders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileDiffMetadata {
    /// The file's name and path.
    pub name: String,
    /// Previous file path, present only if file was renamed or moved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_name: Option<String>,
    /// Explicitly override the syntax highlighting language instead of inferring
    /// from the filename. This will never be set by default, since all internal
    /// diff APIs will attempt to detect the language automatically. If you'd
    /// like to specify a language override, you can do so via the method
    /// `setLanguageOverride`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lang: Option<SupportedLanguages>,

    /// Object ID for the new file content parsed from the `index` line in a
    /// patch file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_object_id: Option<String>,
    /// Object ID for the previous file content parsed from the `index` line in a
    /// patch file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_object_id: Option<String>,

    /// Git file mode parsed from the diff (e.g., `100644` for regular files) when
    /// present in the patch metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Previous git file mode, present if the mode changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_mode: Option<String>,

    /// The type of change for this file.
    #[serde(rename = "type")]
    pub change_type: ChangeTypes,

    /// Array of diff hunks containing line-level change information. Each hunk
    /// corresponds to a `@@ -X,X +X,X @@` group in a diff.
    pub hunks: Vec<Hunk>,
    /// Pre-computed line size for this diff if rendered in `split` diffStyle.
    pub split_line_count: usize,
    /// Pre-computed line size for this diff if rendered in `unified` diffStyle.
    pub unified_line_count: usize,

    /// Whether the diff was parsed from a patch file (true) or generated from
    /// full file contents (false).
    ///
    /// When true, `deletionLines`/`additionLines` contain only the lines present
    /// in the patch and hunk expansion is unavailable.
    ///
    /// When false, they contain the complete file contents.
    ///
    /// A hydrating renderer mutates a partial metadata object to flip this to
    /// false after `loadDiffFiles` resolves. Passing a freshly parsed partial
    /// object later is treated as a new partial render model.
    pub is_partial: bool,

    /// Array of lines from previous version of the file. If `isPartial` is false,
    /// it means that `deletionLines` can be considered the entire contents of the
    /// old version of the file. Otherwise `deletionLines` will just be an array
    /// of all the content processed from the `context` and `deletion` lines of
    /// the patch.
    pub deletion_lines: Vec<String>,
    /// Array of lines from new version of the file. If `isPartial` is false, it
    /// means that `additionLines` can be considered the entire contents of the
    /// new version of the file. Otherwise `additionLines` will just be an array
    /// of all the content processed from the `context` and `addition` lines of
    /// the patch.
    pub addition_lines: Vec<String>,
    /// Total character count of every entry in `deletionLines`, in UTF-16 code
    /// units (`String.length`; see `crate::utf16`). Producers and anything that
    /// replaces or mutates `deletionLines` must keep it in sync.
    pub deletion_character_count: usize,
    /// Total character count of every entry in `additionLines`, in UTF-16 code
    /// units (`String.length`; see `crate::utf16`). Producers and anything that
    /// replaces or mutates `additionLines` must keep it in sync.
    pub addition_character_count: usize,
    /// Set while an attached editor's session-scoped hunk updates have reshaped
    /// `hunks` away from a plain recompute (regions stay frozen during an edit
    /// session). Consumed by the session-exit recompute, which restores
    /// recompute-shaped hunks and clears the flag. It lives on the metadata so
    /// it survives instance recycling and pooling. Edit mode is out of scope for
    /// this port; the field exists so metadata round-trips.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit_session_dirty: Option<bool>,
    /// This unique key is only used for Worker Pools to avoid subsequent requests
    /// to highlight if we've already highlighted the diff. Please note that if
    /// you modify the contents of the diff in any way, you will need to update
    /// the `cacheKey`.
    ///
    /// When `loadDiffFiles` hydrates a partial diff, Diffs appends `:hydrated` to
    /// this key. Without a partial key, changed diffs derive a key from both
    /// loaded file keys and `parseDiffOptions.disableLineMatching`. Pure renames
    /// use the loaded new file's key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
}

/// Line types that we can parse from a patch file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HunkLineType {
    Context,
    Expanded,
    Addition,
    Deletion,
    Metadata,
}
