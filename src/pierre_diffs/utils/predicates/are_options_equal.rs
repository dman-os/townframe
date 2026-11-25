// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areOptionsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: options are the `FileDiffOptions` struct, so `areObjectsEqual(optionsA, optionsB, [theme, parseDiffOptions])` is field-wise equality of every other field, written as an exhaustive destructuring so a new field cannot be forgotten, with callbacks compared by `Rc` identity (`PtrEq`) as `===` compares functions; the `theme` comparison substitutes `DEFAULT_THEMES` for an absent theme as upstream does, and `parseDiffOptions` compares by value (`areObjectsEqual` over its keys). A key present with value `undefined` cannot be told from an absent key (upstream distinguishes them), so such inputs compare equal here.

use crate::components::file_diff::FileDiffOptions;
use crate::constants::default_themes;
use crate::types::options::BaseCodeOptions;
use crate::types::options::BaseDiffOptions;
use crate::types::options::ThemeOption;
use crate::utils::predicates::are_themes_equal::are_themes_equal;

pub fn are_options_equal<Element, Rendered, LAnnotation>(
    options_a: Option<&FileDiffOptions<Element, Rendered, LAnnotation>>,
    options_b: Option<&FileDiffOptions<Element, Rendered, LAnnotation>>,
) -> bool {
    let default_theme = ThemeOption::Themes(default_themes());
    let theme_a = options_a
        .and_then(|options| options.base.base.theme.as_ref())
        .unwrap_or(&default_theme);
    let theme_b = options_b
        .and_then(|options| options.base.base.theme.as_ref())
        .unwrap_or(&default_theme);
    let (Some(options_a), Some(options_b)) = (options_a, options_b) else {
        // `areObjectsEqual` with an undefined side: equal only if both are.
        return options_a.is_none()
            && options_b.is_none()
            && are_themes_equal(Some(theme_a), Some(theme_b));
    };
    are_themes_equal(Some(theme_a), Some(theme_b))
        && same_except_theme_and_parse_options(options_a, options_b)
        && options_a.base.parse_diff_options == options_b.base.parse_diff_options
}

fn same_except_theme_and_parse_options<Element, Rendered, LAnnotation>(
    first: &FileDiffOptions<Element, Rendered, LAnnotation>,
    second: &FileDiffOptions<Element, Rendered, LAnnotation>,
) -> bool {
    let FileDiffOptions {
        base,
        interaction,
        hunk_separators,
        render_header_prefix,
        render_header_filename_suffix,
        render_header_metadata,
        render_custom_header,
        disable_error_handling,
        render_annotation,
        render_gutter_utility,
        on_post_render,
        has_diff_file_loader,
    } = first;
    let BaseDiffOptions {
        base: code,
        diff_style,
        diff_indicators,
        disable_background,
        hunk_separators: _,
        expand_unchanged,
        collapsed_context_threshold,
        line_diff_type,
        max_line_diff_length,
        expansion_line_count,
        parse_diff_options: _,
    } = base;
    let BaseCodeOptions {
        theme: _,
        disable_line_numbers,
        overflow,
        theme_type,
        collapsed,
        disable_file_header,
        disable_virtualization_buffers,
        sticky_header,
        preferred_highlighter,
        use_css_classes,
        use_token_transformer,
        tokenize_max_line_length,
        tokenize_max_length,
        unsafe_css,
    } = code;
    let other = &second.base;
    let other_code = &other.base;
    *diff_style == other.diff_style
        && *diff_indicators == other.diff_indicators
        && *disable_background == other.disable_background
        && *expand_unchanged == other.expand_unchanged
        && *collapsed_context_threshold == other.collapsed_context_threshold
        && *line_diff_type == other.line_diff_type
        && *max_line_diff_length == other.max_line_diff_length
        && *expansion_line_count == other.expansion_line_count
        && *disable_line_numbers == other_code.disable_line_numbers
        && *overflow == other_code.overflow
        && *theme_type == other_code.theme_type
        && *collapsed == other_code.collapsed
        && *disable_file_header == other_code.disable_file_header
        && *disable_virtualization_buffers == other_code.disable_virtualization_buffers
        && *sticky_header == other_code.sticky_header
        && *preferred_highlighter == other_code.preferred_highlighter
        && *use_css_classes == other_code.use_css_classes
        && *use_token_transformer == other_code.use_token_transformer
        && *tokenize_max_line_length == other_code.tokenize_max_line_length
        && *tokenize_max_length == other_code.tokenize_max_length
        && *unsafe_css == other_code.unsafe_css
        && *interaction == second.interaction
        && *hunk_separators == second.hunk_separators
        && *render_header_prefix == second.render_header_prefix
        && *render_header_filename_suffix == second.render_header_filename_suffix
        && *render_header_metadata == second.render_header_metadata
        && *render_custom_header == second.render_custom_header
        && *disable_error_handling == second.disable_error_handling
        && *render_annotation == second.render_annotation
        && *render_gutter_utility == second.render_gutter_utility
        && *on_post_render == second.on_post_render
        && *has_diff_file_loader == second.has_diff_file_loader
}
