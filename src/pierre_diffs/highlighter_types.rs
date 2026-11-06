// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): the subset of
// packages/diffs/src/highlighter/types.ts and the Shiki types it re-exports
// (`ThemedToken`, `DiffsHighlighter`, `CodeToTokensOptions`) that the renderers
// call, plus a deterministic plain-text implementation.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `DiffsHighlighter` is a trait with the three members upstream's
// renderers use (`name`, `getTheme`, `codeToTokens`). The highlighter itself
// (Shiki, `@pierre/highlights`) is S7; `PlainTextHighlighter` is the
// deterministic stub the S5/S6 oracle runs against: one token per non-empty
// line, no colors. `DecorationItem` is defined in
// `utils::parse_diff_decorations` until S7 moves it here.

use std::collections::BTreeMap;

use crate::types::options::ThemeOption;
use crate::types::options::ThemesType;

/// Shiki's `ThemedToken`, the fields `renderTokenLines`/`tokenStyle` read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ThemedToken {
    pub content: String,
    /// UTF-16 offset of the token in the tokenised code.
    pub offset: usize,
    pub color: Option<String>,
    pub bg_color: Option<String>,
    /// Bit flags: 1 italic, 2 bold, 4 underline, 8 strikethrough.
    pub font_style: Option<u8>,
    /// Inline CSS declarations as ordered `(name, value)` pairs.
    pub html_style: Option<Vec<(String, String)>>,
    pub html_attrs: Option<Vec<(String, String)>>,
}

/// The theme record `highlighter.getTheme(name)` returns.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ThemeData {
    /// `'light' | 'dark'`
    pub theme_type: Option<crate::types::options::BaseThemeType>,
    pub fg: String,
    pub bg: String,
    pub colors: BTreeMap<String, String>,
}

/// `CodeToTokensOptions` as built by `getTokenOptions`.
#[derive(Debug, Clone, PartialEq)]
pub struct CodeToTokensOptions {
    pub lang: String,
    pub theme: ThemeOption,
    pub default_color: bool,
    pub css_variable_prefix: String,
    pub tokenize_max_line_length: Option<usize>,
    pub tokenize_time_limit: usize,
}

/// The slice of Shiki's highlighter interface the diff renderers use.
///
/// Implementations tokenise `code` (lines separated by LF, CRLF or CR) into
/// one token list per line.
pub trait DiffsHighlighter {
    /// `highlighter.name`; `"highlights"` selects the immutable-`htmlStyle`
    /// caching path in `renderTokenLines`.
    fn name(&self) -> &str;
    fn get_theme(&self, theme_name: &str) -> ThemeData;
    fn code_to_tokens(&self, code: &str, options: &CodeToTokensOptions) -> Vec<Vec<ThemedToken>>;
}

/// Deterministic stand-in tokenizer: each non-empty line is one uncolored
/// token and each empty line has no tokens; themes are fixed per
/// light/dark. Mirrors the stub in `oracle/generate.mjs`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlainTextHighlighter;

impl PlainTextHighlighter {
    fn is_light(theme_name: &str) -> bool {
        theme_name.contains("light")
    }
}

impl DiffsHighlighter for PlainTextHighlighter {
    fn name(&self) -> &str {
        "plain-text-stub"
    }

    fn get_theme(&self, theme_name: &str) -> ThemeData {
        use crate::types::options::BaseThemeType;
        let light = Self::is_light(theme_name);
        ThemeData {
            theme_type: Some(if light {
                BaseThemeType::Light
            } else {
                BaseThemeType::Dark
            }),
            fg: String::from(if light { "#070707" } else { "#fbfbfb" }),
            bg: String::from(if light { "#ffffff" } else { "#0b0b0b" }),
            colors: BTreeMap::from([
                (
                    String::from("terminal.ansiGreen"),
                    String::from(if light { "#0dbe4e" } else { "#5ecc71" }),
                ),
                (
                    String::from("gitDecoration.deletedResourceForeground"),
                    String::from(if light { "#ff2e3f" } else { "#ff6762" }),
                ),
            ]),
        }
    }

    fn code_to_tokens(&self, code: &str, _options: &CodeToTokensOptions) -> Vec<Vec<ThemedToken>> {
        let mut lines = Vec::new();
        let mut offset = 0;
        let mut rest = code;
        loop {
            // Shiki splits on /\r\n|\r|\n/.
            let (line, terminator_len) = match rest.find(['\r', '\n']) {
                Some(index) => {
                    let terminator = if rest[index..].starts_with("\r\n") {
                        2
                    } else {
                        1
                    };
                    (&rest[..index], terminator)
                }
                None => (rest, 0),
            };
            lines.push(if line.is_empty() {
                Vec::new()
            } else {
                vec![ThemedToken {
                    content: line.to_owned(),
                    offset,
                    ..ThemedToken::default()
                }]
            });
            offset += crate::utf16::len(line) + terminator_len;
            if terminator_len == 0 {
                return lines;
            }
            rest = &rest[line.len() + terminator_len..];
        }
    }
}

/// The `ThemesType | string` a highlighter option names, as the list of theme
/// names `getTheme` is asked for.
pub fn theme_names(theme: &ThemeOption) -> Vec<&str> {
    match theme {
        ThemeOption::Name(name) => vec![name.as_str()],
        ThemeOption::Themes(ThemesType { dark, light }) => vec![dark.as_str(), light.as_str()],
    }
}
