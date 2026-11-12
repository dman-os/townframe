// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/formatCSSVariablePrefix.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `'global' | 'token'` union is `CssVariablePrefix`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CssVariablePrefix {
    Global,
    Token,
}

pub fn format_css_variable_prefix(prefix_type: CssVariablePrefix) -> &'static str {
    match prefix_type {
        CssVariablePrefix::Token => "--diffs-token-",
        CssVariablePrefix::Global => "--diffs-",
    }
}
