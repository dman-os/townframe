// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getDiffHunksRendererOptions.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `FileDiffOptions` is the Rust struct (`components::file_diff`); a function `hunkSeparators` is `HunkSeparators::Custom`; `renderCustomHeader != null` is `headerRenderMode: Custom`; `useTokenTransformer` goes through `shouldUseTokenTransformer` with the token callbacks absent (DEFER); `loadDiffFiles` is the `has_diff_file_loader` flag. Upstream builds the snapshot with direct property reads because CodeView items may inherit prototype getters; a struct has no such getters.

use crate::components::file_diff::FileDiffOptions;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererOptions;
use crate::types::options::BaseCodeOptions;
use crate::types::options::BaseDiffOptions;
use crate::types::options::FileHeaderRenderMode;
use crate::utils::options::should_use_token_transformer::ShouldUseTokenTransformerProps;
use crate::utils::options::should_use_token_transformer::should_use_token_transformer;

// Build the renderer option snapshot with direct property reads.
pub fn get_diff_hunks_renderer_options<Element, Rendered, LAnnotation>(
    options: &FileDiffOptions<Element, Rendered, LAnnotation>,
) -> DiffHunksRendererOptions {
    let base = &options.base.base;
    DiffHunksRendererOptions {
        base: BaseDiffOptions {
            base: BaseCodeOptions {
                theme: base.theme.clone(),
                disable_line_numbers: base.disable_line_numbers,
                overflow: base.overflow,
                // `themeType` and `unsafeCSS` are not part of the snapshot.
                theme_type: None,
                collapsed: base.collapsed,
                disable_file_header: base.disable_file_header,
                disable_virtualization_buffers: base.disable_virtualization_buffers,
                sticky_header: base.sticky_header,
                preferred_highlighter: base.preferred_highlighter,
                use_css_classes: base.use_css_classes,
                use_token_transformer: Some(should_use_token_transformer(Some(
                    &ShouldUseTokenTransformerProps {
                        use_token_transformer: base.use_token_transformer,
                        ..ShouldUseTokenTransformerProps::default()
                    },
                ))),
                tokenize_max_line_length: base.tokenize_max_line_length,
                tokenize_max_length: base.tokenize_max_length,
                unsafe_css: None,
            },
            diff_style: options.base.diff_style,
            diff_indicators: options.base.diff_indicators,
            disable_background: options.base.disable_background,
            hunk_separators: options.renderer_hunk_separators(),
            expand_unchanged: options.base.expand_unchanged,
            collapsed_context_threshold: options.base.collapsed_context_threshold,
            line_diff_type: options.base.line_diff_type,
            max_line_diff_length: options.base.max_line_diff_length,
            expansion_line_count: options.base.expansion_line_count,
            parse_diff_options: options.base.parse_diff_options.clone(),
        },
        header_render_mode: Some(if options.render_custom_header.is_some() {
            FileHeaderRenderMode::Custom
        } else {
            FileHeaderRenderMode::Default
        }),
        has_diff_file_loader: options.has_diff_file_loader,
    }
}
