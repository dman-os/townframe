// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getTokenOptions.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::highlighter_types::CodeToTokensOptions;
use crate::types::options::ThemeOption;
use crate::utils::render::format_css_variable_prefix::CssVariablePrefix;
use crate::utils::render::format_css_variable_prefix::format_css_variable_prefix;

pub fn get_token_options(
    lang: &str,
    theme: &ThemeOption,
    tokenize_max_line_length: Option<usize>,
) -> CodeToTokensOptions {
    CodeToTokensOptions {
        lang: lang.to_owned(),
        theme: theme.clone(),
        default_color: false,
        css_variable_prefix: format_css_variable_prefix(CssVariablePrefix::Token).to_owned(),
        tokenize_max_line_length,
        // Timed aborts can leave incomplete tokens; limit line length instead.
        tokenize_time_limit: 0,
    }
}
