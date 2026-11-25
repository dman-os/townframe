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

//! Option and theme types: `BaseDiffOptions`, `ThemeOption`, the highlighter and rendering enums.

use crate::types::patch::ParseDiffOptions;
use crate::types::patch::SupportedLanguages;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HighlighterTypes {
    ShikiJs,
    ShikiWasm,
    Highlights,
}

/// `[char: number, fg: string, text: string]`
pub type HighlightedToken = (u32, String, String);

/// Diffs accepts Shiki's bundled theme names and any additional theme name a
/// consumer registers through the highlighter/theming catalog.
pub type DiffsThemeNames = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThemesType {
    pub dark: DiffsThemeNames,
    pub light: DiffsThemeNames,
}

/// `DiffsThemeNames | ThemesType`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ThemeOption {
    Name(DiffsThemeNames),
    Themes(ThemesType),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeTypes {
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PostRenderPhase {
    Mount,
    Update,
    Unmount,
}

/// The `'custom'` variant is deprecated upstream and will be removed in a
/// future version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HunkSeparators {
    Simple,
    Metadata,
    LineInfo,
    LineInfoBasic,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LineDiffTypes {
    WordAlt,
    WordLine,
    Word,
    Char,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffIndicators {
    Classic,
    Bars,
    None,
}

/// `'scroll' | 'wrap'`, `'scroll'` is default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Overflow {
    Scroll,
    Wrap,
}

/// `'unified' | 'split'`, `'split'` is default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffStyle {
    Unified,
    Split,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BaseCodeOptions {
    pub theme: Option<ThemeOption>,
    pub disable_line_numbers: Option<bool>,
    pub overflow: Option<Overflow>,     // 'scroll' is default
    pub theme_type: Option<ThemeTypes>, // 'system' is default
    pub collapsed: Option<bool>,
    pub disable_file_header: Option<bool>,
    pub disable_virtualization_buffers: Option<bool>,
    pub sticky_header: Option<bool>,

    // Highlighter options, ignored if you're using a WorkerPoolManager
    pub preferred_highlighter: Option<HighlighterTypes>,
    pub use_css_classes: Option<bool>,
    pub use_token_transformer: Option<bool>,
    pub tokenize_max_line_length: Option<usize>,
    pub tokenize_max_length: Option<usize>,

    // Custom CSS injection
    pub unsafe_css: Option<String>,
}

/// `BaseDiffOptions`. `loadDiffFiles` (a callback) is not a field; the
/// consuming app owns it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BaseDiffOptions {
    pub base: BaseCodeOptions,
    pub diff_style: Option<DiffStyle>,           // split is default
    pub diff_indicators: Option<DiffIndicators>, // bars is default
    pub disable_background: Option<bool>,
    pub hunk_separators: Option<HunkSeparators>, // line-info is default
    pub expand_unchanged: Option<bool>,          // false is default
    // Auto-expand collapsed context at or below this size.
    pub collapsed_context_threshold: Option<usize>, // 2 is default
    // NOTE(amadeus): 'word-alt' attempts to join word regions that are separated
    // by a single character
    // 'word-line' joins all changed word regions into one highlight per line.
    pub line_diff_type: Option<LineDiffTypes>, // 'word-alt' is default
    pub max_line_diff_length: Option<usize>,   // 1000 is default

    // How many lines to expand per click
    pub expansion_line_count: Option<usize>, // 100 is default

    /// Parser options used when computing diffs from file contents
    /// (oldFile/newFile) or recomputing edited diffs. Includes jsdiff settings
    /// and the disableLineMatching escape hatch. Also used when deriving cache
    /// keys during partial diff hydration; use the same matching mode as the
    /// supplied patch. Does not change the hunks in supplied pre-parsed patches.
    pub parse_diff_options: Option<ParseDiffOptions>,
}

/// `Required<Omit<BaseDiffOptions, 'unsafeCSS' | 'preferredHighlighter' |
/// 'parseDiffOptions' | 'loadDiffFiles'>>`: every option resolved.
#[derive(Debug, Clone, PartialEq)]
pub struct BaseDiffOptionsWithDefaults {
    pub theme: ThemeOption,
    pub disable_line_numbers: bool,
    pub overflow: Overflow,
    pub theme_type: ThemeTypes,
    pub collapsed: bool,
    pub disable_file_header: bool,
    pub disable_virtualization_buffers: bool,
    pub sticky_header: bool,
    pub use_css_classes: bool,
    pub use_token_transformer: bool,
    pub tokenize_max_line_length: usize,
    pub tokenize_max_length: usize,
    pub diff_style: DiffStyle,
    pub diff_indicators: DiffIndicators,
    pub disable_background: bool,
    pub hunk_separators: HunkSeparators,
    pub expand_unchanged: bool,
    pub collapsed_context_threshold: usize,
    pub line_diff_type: LineDiffTypes,
    pub max_line_diff_length: usize,
    pub expansion_line_count: usize,
}

/// `string | number | undefined` values of `CustomPreProperties`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CustomPreProperty {
    Text(String),
    Number(f64),
}

pub type CustomPreProperties = HashMap<String, Option<CustomPreProperty>>;

/// `'diff' | 'file'`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PreType {
    Diff,
    File,
}

// NOTE(amadeus): This is the shared config that all `pre` nodes will need to
// get setup properly. Whether it's via direct DOM manipulation or via HAST
// html rendering, this interface can be shared across both of these areas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrePropertiesConfig {
    // Required<Pick<BaseDiffOptions, 'diffIndicators' | 'disableBackground' |
    // 'disableLineNumbers' | 'overflow'>>
    pub diff_indicators: DiffIndicators,
    pub disable_background: bool,
    pub disable_line_numbers: bool,
    pub overflow: Overflow,
    #[serde(rename = "type")]
    pub pre_type: PreType,
    pub split: bool,
    pub total_lines: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_properties: Option<CustomPreProperties>,
}

/// `'default' | 'custom'`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileHeaderRenderMode {
    Default,
    Custom,
}

/// `Record<string, SupportedLanguages | undefined>`
pub type ExtensionFormatMap = HashMap<String, Option<SupportedLanguages>>;

#[derive(Debug, Clone, PartialEq)]
pub struct RenderFileOptions {
    pub theme: ThemeOption,
    pub use_token_transformer: bool,
    pub tokenize_max_line_length: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenderDiffOptions {
    pub theme: ThemeOption,
    pub use_token_transformer: bool,
    pub tokenize_max_line_length: usize,
    pub line_diff_type: LineDiffTypes,
    pub max_line_diff_length: usize,
}

/// `'light' | 'dark' | undefined`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BaseThemeType {
    Light,
    Dark,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppliedThemeStyleCache {
    pub theme: ThemeOption,
    pub theme_styles: String,
    pub theme_type: ThemeTypes,
    pub base_theme_type: Option<BaseThemeType>,
    pub scrollbar_gutter: Option<f64>,
}
