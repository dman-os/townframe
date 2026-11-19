// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/tokenStyle.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `cacheHtmlStyle` (a WeakMap keyed by object identity) is dropped: building the string is the same work Rust would key a cache by, and the cache only exists to dodge JavaScript allocation.

use crate::highlighter_types::ThemedToken;

/// Returns CSS declarations for a highlighted token.
pub fn token_style(token: &ThemedToken) -> String {
    if let Some(html_style) = &token.html_style {
        return html_style
            .iter()
            .map(|(name, value)| format!("{name}:{value}"))
            .collect::<Vec<_>>()
            .join(";");
    }
    let mut style = token
        .color
        .as_ref()
        .map_or_else(String::new, |color| format!("color:{color}"));
    if let Some(bg_color) = &token.bg_color {
        style.push_str(&format!(";background-color:{bg_color}"));
    }
    if let Some(font_style) = token.font_style {
        if font_style & 1 != 0 {
            style.push_str(";font-style:italic");
        }
        if font_style & 2 != 0 {
            style.push_str(";font-weight:bold");
        }
        let underline = font_style & 4 != 0;
        let strikethrough = font_style & 8 != 0;
        if underline || strikethrough {
            style.push_str(&format!(
                ";text-decoration:{}{}{}",
                if underline { "underline" } else { "" },
                if underline && strikethrough { " " } else { "" },
                if strikethrough { "line-through" } else { "" },
            ));
        }
    }
    // `style.replace(/^;/, '')`
    style.strip_prefix(';').map_or(style.clone(), str::to_owned)
}
