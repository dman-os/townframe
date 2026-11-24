// Ported from @pierre/highlights 0.4.0 (Apache-2.0, Pierre Computer Company):
// dist/highlighter.js (lib/highlighter.ts), the glue around the wasm:
// `langIdOf`, `isSupportedLanguage` and `codeToTokens`.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the wasm itself is behind [`HighlightRecords`]: given the text and a
// language id it returns the mode-3 records (UTF-16 line records), so this crate
// has no engine dependency and the host decides where `highlights.wasm` runs
// (a wasmtime component, a browser, a test double). Not ported: the
// instance and memory management (`writeInput`, `growMemory`, `bindMemory`,
// the shared-instance replacement above 8 MiB, `is_id_*` callbacks), the HTML
// modes 0, 1, 2 and 4 (`codeToHtml`), `StreamTokenizer`, `LiveTokenizer`, and
// the `ansi` language (`ansiTokens`, `AnsiState`): `ansi` is reported by
// [`is_supported_language`] as upstream does but tokenising it panics. Fuel
// and per-call slicing for long inputs are the host's concern (the wasm cannot
// be interrupted from here).

use super::data::language_id;
use super::tokens::DefaultColor;
use super::tokens::HighlightsError;
use super::tokens::ThemeChoice;
use super::tokens::ThemeMeta;
use super::tokens::line_records_to_tokens;
use super::tokens::resolve_option_themes;
use super::tokens::theme_meta;
use crate::highlighter_types::ThemedToken;

/// Runs the unmodified `highlights.wasm` in output mode 3: UTF-16 token
/// records, `[end, highlight id]` pairs per token with a `0xffffffff` record
/// closing each line (and carrying the next line's start).
pub trait HighlightRecords {
    fn records(&self, text: &str, lang_id: u8) -> Vec<u32>;
}

/// `langIdOf`: the wasm language id of a name or alias.
pub fn lang_id_of(lang: &str) -> Result<u8, HighlightsError> {
    language_id(&lang.to_lowercase())
        .ok_or_else(|| HighlightsError::UnknownLanguage(lang.to_owned()))
}

/// `isAnsi`
pub fn is_ansi(lang: &str) -> bool {
    lang.to_lowercase() == "ansi"
}

/// Whether a name or alias has a built-in lexer, including `plain`/`text`, or is `ansi`.
pub fn is_supported_language(lang: &str) -> bool {
    language_id(&lang.to_lowercase()).is_some() || is_ansi(lang)
}

/// `CodeToTokensOptions` of `@pierre/highlights`.
#[derive(Debug, Clone)]
pub struct HighlightsOptions {
    pub lang: String,
    pub theme: ThemeChoice,
    pub default_color: DefaultColor,
    pub css_variable_prefix: String,
    pub tokenize_max_line_length: Option<usize>,
}

/// A Shiki-compatible `TokensResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct TokensResult {
    pub tokens: Vec<Vec<ThemedToken>>,
    pub meta: ThemeMeta,
}

/// `codeToTokens`: Shiki-compatible themed tokens, one list per line. The wasm
/// lexes the input and splits lines; this converts the style records.
pub fn code_to_tokens(
    source: &dyn HighlightRecords,
    code: &str,
    options: &HighlightsOptions,
) -> Result<TokensResult, HighlightsError> {
    assert!(
        !is_ansi(&options.lang),
        "the ansi language (ansiTokens) is not ported"
    );
    let themes = resolve_option_themes(
        &options.theme,
        &options.default_color,
        &options.css_variable_prefix,
    )?;
    let lang_id = lang_id_of(&options.lang)?;
    let records = source.records(code, lang_id);
    Ok(TokensResult {
        tokens: line_records_to_tokens(
            code,
            &records,
            &themes,
            &options.css_variable_prefix,
            options.tokenize_max_line_length,
            0,
        ),
        meta: theme_meta(&themes, &options.css_variable_prefix),
    })
}
