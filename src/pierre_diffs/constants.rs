// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/constants.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the twelve `RegExp` constants are not constants here. Rust's
// `regex` crate has no lookaround and the JavaScript semantics (`m`-flag
// anchors on four line terminators, `\s`, `.`) differ from Rust's, so each is
// a hand-written scanner in `crate::scanners`, listed below by name.
// `DIFFS_DEVELOPMENT_BUILD` reads `cfg!(debug_assertions)` instead of
// `process.env.NODE_ENV`. `EMPTY_LINE_ANNOTATIONS` (a frozen empty array used
// as an identity-stable default in JavaScript) has no Rust counterpart: an
// empty slice is `&[]`. `Object.freeze` on `DEFAULT_EXPANDED_REGION` is
// `const`.
//
// Regex -> scanner:
//   COMMIT_METADATA_SPLIT                  -> scanners::split_commit_metadata
//   GIT_DIFF_FILE_BREAK_REGEX              -> scanners::split_git_diff_files, scanners::has_git_diff_file_break
//   UNIFIED_DIFF_FILE_BREAK_REGEX          -> scanners::split_unified_diff_files
//   FILE_CONTEXT_BLOB                      -> scanners::split_file_context_blobs
//   HUNK_HEADER                            -> scanners::match_hunk_header
//   SPLIT_WITH_NEWLINES                    -> scanners::split_with_newlines
//   FILENAME_HEADER_REGEX                  -> scanners::match_filename_header
//   INDEX_LINE_METADATA                    -> scanners::match_index_line
//   MERGE_CONFLICT_{START,BASE,SEPARATOR,END}_MARKER_REGEX
//                                          -> scanners::is_merge_conflict_*_marker

use crate::types::layout::CodeViewLayout;
use crate::types::layout::RenderRange;
use crate::types::layout::SmoothScrollSettings;
use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::HunkExpansionRegion;
use crate::types::options::HighlighterTypes;
use crate::types::options::ThemesType;

pub const DIFFS_TAG_NAME: &str = "diffs-container";

// Upstream keeps this as a NODE_ENV read so app builds can hard-disable
// development-only checks unless they are explicitly built for development.
pub const DIFFS_DEVELOPMENT_BUILD: bool = cfg!(debug_assertions);

pub const HEADER_PREFIX_SLOT_ID: &str = "header-prefix";
pub const HEADER_FILENAME_SUFFIX_SLOT_ID: &str = "header-filename-suffix";
pub const HEADER_METADATA_SLOT_ID: &str = "header-metadata";
pub const CUSTOM_HEADER_SLOT_ID: &str = "header-custom";

/// `DEFAULT_THEMES`: `{ dark: 'pierre-dark', light: 'pierre-light' }`.
pub fn default_themes() -> ThemesType {
    ThemesType {
        dark: String::from("pierre-dark"),
        light: String::from("pierre-light"),
    }
}

// The highlighter type used when no request names one and none is active yet.
pub const DEFAULT_HIGHLIGHTER: HighlighterTypes = HighlighterTypes::ShikiJs;

pub const THEME_CSS_ATTRIBUTE: &str = "data-theme-css";
pub const UNSAFE_CSS_ATTRIBUTE: &str = "data-unsafe-css";
pub const CORE_CSS_ATTRIBUTE: &str = "data-core-css";
pub const DIFFS_SCROLLBAR_MEASURE_ATTRIBUTE: &str = "data-diffs-scrollbar-measure";
pub const CODE_VIEW_HEADER_ATTRIBUTE: &str = "data-diffs-code-view-header";
pub const CODE_VIEW_FOOTER_ATTRIBUTE: &str = "data-diffs-code-view-footer";
pub const DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY: &str = "--diffs-scrollbar-gutter-measured";

pub const DEFAULT_COLLAPSED_CONTEXT_THRESHOLD: usize = 1;
pub const DEFAULT_TOKENIZE_MAX_LENGTH: usize = 100_000;
pub const DEFAULT_VIRTUAL_FILE_METRICS: VirtualFileMetrics = VirtualFileMetrics {
    hunk_line_count: 50,
    line_height: 20.0,
    diff_header_height: 44.0,
    hunk_separator_height: None,
    spacing: 8.0,
    padding_top: None,
    padding_bottom: None,
};

pub const DEFAULT_CODE_VIEW_FILE_METRICS: VirtualFileMetrics = VirtualFileMetrics {
    hunk_line_count: 1,
    ..DEFAULT_VIRTUAL_FILE_METRICS
};

pub const DEFAULT_CODE_VIEW_LAYOUT: CodeViewLayout = CodeViewLayout {
    padding_top: 8.0,
    padding_bottom: 8.0,
    gap: 8.0,
};

pub const DEFAULT_SMOOTH_SCROLL_SETTINGS: SmoothScrollSettings = SmoothScrollSettings {
    omega: 0.015,
    position_epsilon: 0.5,
    velocity_epsilon: 0.05,
};

pub const DEFAULT_EXPANDED_REGION: HunkExpansionRegion = HunkExpansionRegion {
    from_start: 0,
    from_end: 0,
};

pub const DEFAULT_RENDER_RANGE: RenderRange = RenderRange {
    starting_line: 0.0,
    total_lines: f64::INFINITY,
    buffer_before: 0.0,
    buffer_after: 0.0,
};

pub const EMPTY_RENDER_RANGE: RenderRange = RenderRange {
    starting_line: 0.0,
    total_lines: 0.0,
    buffer_before: 0.0,
    buffer_after: 0.0,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_constants_ts() {
        assert_eq!(DEFAULT_VIRTUAL_FILE_METRICS.hunk_line_count, 50);
        assert_eq!(DEFAULT_CODE_VIEW_FILE_METRICS.hunk_line_count, 1);
        assert_eq!(
            DEFAULT_CODE_VIEW_FILE_METRICS.line_height,
            DEFAULT_VIRTUAL_FILE_METRICS.line_height
        );
        assert_eq!(default_themes().dark, "pierre-dark");
        assert_eq!(default_themes().light, "pierre-light");
        assert!(DEFAULT_RENDER_RANGE.total_lines.is_infinite());
        assert_eq!(EMPTY_RENDER_RANGE.total_lines, 0.0);
    }

    #[test]
    fn infinite_render_range_round_trips_through_json_null() {
        let json = serde_json::to_value(DEFAULT_RENDER_RANGE).expect("serializes");
        assert_eq!(json["totalLines"], serde_json::Value::Null);
        let back: RenderRange = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, DEFAULT_RENDER_RANGE);
    }
}
