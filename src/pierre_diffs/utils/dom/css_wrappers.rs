// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/cssWrappers.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `rawStyles` (`import ... from '../style.css?inline'`) is `assets/style.css`, exported from the pinned dist by `oracle/generate.mjs` and embedded with `include_str!`. `SCROLLBAR_GUTTER_DECLARATION_PATTERN` (a `RegExp`) is a hand-written scan with the regex's semantics: the first occurrence of the property name followed by optional whitespace, a colon, and at least one non-`;` character before a `;` (`\s*` gives characters back to `[^;]+`). `String.prototype.replace` with a string pattern replacement has no `$` sequences in the two possible replacement texts, so it is a plain splice. `themeType` defaults to `System`.

use crate::constants::DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY;
use crate::types::options::ThemeTypes;
use crate::utils::dom::scrollbar_gutter::create_measured_scrollbar_gutter_declaration;

const RAW_STYLES: &str = include_str!("../../assets/style.css");
const LAYER_ORDER: &str = "@layer base, theme, rendered, unsafe;";

pub fn wrap_core_css(main_css: &str) -> String {
    format!("{LAYER_ORDER}\n{RAW_STYLES}\n@layer theme {{\n  {main_css}\n}}")
}

pub fn wrap_unsafe_css(unsafe_css: &str) -> String {
    format!("{LAYER_ORDER}\n@layer unsafe {{\n  {unsafe_css}\n}}")
}

pub fn wrap_theme_css(
    theme_css: &str,
    theme_type: ThemeTypes,
    scrollbar_gutter: Option<f64>,
) -> String {
    let color_scheme_rule = match theme_type {
        ThemeTypes::System => String::new(),
        ThemeTypes::Light => "\n  color-scheme: light;".to_owned(),
        ThemeTypes::Dark => "\n  color-scheme: dark;".to_owned(),
    };
    let scrollbar_gutter_var = create_measured_scrollbar_gutter_declaration(scrollbar_gutter);
    let types: &[&str] = match theme_type {
        ThemeTypes::System => &["light", "dark"],
        ThemeTypes::Light => &["light"],
        ThemeTypes::Dark => &["dark"],
    };
    let token_font_styles = types
        .iter()
        .map(|kind| {
            let styles = format!(
                "\n  [data-line] span, [data-edit-prediction-suffix] span {{\n    font-weight: var(--diffs-token-{kind}-font-weight, inherit);\n    font-style: var(--diffs-token-{kind}-font-style, inherit);\n    text-decoration: var(--diffs-token-{kind}-text-decoration, inherit);\n  }}"
            );
            if theme_type == ThemeTypes::System && *kind == "dark" {
                format!("@media (prefers-color-scheme: dark) {{{styles}\n}}")
            } else {
                styles
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "{LAYER_ORDER}\n@layer base {{\n  {token_font_styles}\n}}\n@layer rendered {{\n  :host {{{color_scheme_rule}\n  {scrollbar_gutter_var}\n  {theme_css}\n  }}\n}}"
    )
}

pub fn patch_scrollbar_gutter_size(theme_css: &str, scrollbar_gutter: Option<f64>) -> String {
    let scrollbar_gutter_rule = create_measured_scrollbar_gutter_declaration(scrollbar_gutter);
    match find_scrollbar_gutter_declaration(theme_css) {
        Some((start, end)) => format!(
            "{}{scrollbar_gutter_rule}{}",
            &theme_css[..start],
            &theme_css[end..]
        ),
        None => theme_css.to_owned(),
    }
}

// `/--diffs-scrollbar-gutter-measured\s*:\s*[^;]+;/`: the first match as a byte range.
fn find_scrollbar_gutter_declaration(theme_css: &str) -> Option<(usize, usize)> {
    let mut from = 0;
    while let Some(offset) = theme_css[from..].find(DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY) {
        let start = from + offset;
        let after_name = start + DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY.len();
        let after_space = after_name + leading_whitespace_len(&theme_css[after_name..]);
        if theme_css[after_space..].starts_with(':') {
            let value_start = after_space + 1;
            // `\s*[^;]+;`: the value is everything up to the next `;`, and it
            // must hold at least one character (whitespace counts).
            if let Some(semicolon) = theme_css[value_start..].find(';')
                && semicolon > 0
            {
                return Some((start, value_start + semicolon + 1));
            }
        }
        // Next occurrence: the regex retries one character further on.
        from = start + 1;
        while !theme_css.is_char_boundary(from) {
            from += 1;
        }
    }
    None
}

fn leading_whitespace_len(text: &str) -> usize {
    text.len() - crate::js_text::trim_start(text).len()
}
