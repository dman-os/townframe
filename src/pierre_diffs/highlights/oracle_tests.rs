//! Differential tests for S7. `oracle/golden/highlights.json` holds, per case
//! (sample text, language, theme or theme pair, line-length limit), the mode-3
//! records the unmodified `highlights.wasm` produced in node and the tokens the
//! dist's full pipeline (diffs' `HighlightsHighlighter` over `@pierre/highlights`)
//! returned for them. Our glue is fed the same records through a replaying
//! [`HighlightRecords`] and must produce the same tokens (exactly, including
//! `type`, `htmlStyle` and offsets), the same theme metadata, the same
//! language ids and the same `getTheme` records. Test-only, original to this crate.

use std::collections::HashMap;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::backend::HighlightsHighlighter;
use super::data::bundled_themes;
use super::highlighter::HighlightRecords;
use super::highlighter::HighlightsOptions;
use super::highlighter::code_to_tokens;
use super::highlighter::is_supported_language;
use super::highlighter::lang_id_of;
use super::theme::ZedTheme;
use super::tokens::DefaultColor;
use super::tokens::ThemeChoice;
use crate::highlighter_types::DiffsHighlighter;
use crate::highlighter_types::ThemedToken;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::options::ThemeOption;
use crate::utils::render::get_token_options::get_token_options;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Golden {
    themes: HashMap<String, Value>,
    is_ansi: bool,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    sample: String,
    code: String,
    lang: String,
    theme: ThemeOption,
    tokenize_max_line_length: Option<usize>,
    supported: bool,
    lang_id: u8,
    records: Vec<u32>,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    tokens: Value,
    fg: Option<String>,
    bg: Option<String>,
    theme_name: String,
    root_style: Option<String>,
}

/// Replays the records the wasm produced for each `(text, language id)`.
struct Replay(HashMap<(String, u8), Vec<u32>>);

impl HighlightRecords for Replay {
    fn records(&self, text: &str, lang_id: u8) -> Vec<u32> {
        self.0
            .get(&(text.to_owned(), lang_id))
            .unwrap_or_else(|| panic!("no recorded records for {text:?} lang {lang_id}"))
            .clone()
    }
}

fn replay(golden: &Golden) -> Replay {
    Replay(
        golden
            .cases
            .iter()
            .map(|case| ((case.code.clone(), case.lang_id), case.records.clone()))
            .collect(),
    )
}

fn zed(highlighter: &HighlightsHighlighter<Replay>, name: &str) -> Rc<ZedTheme> {
    Rc::clone(&highlighter.resolved_theme(name).zed)
}

fn token_json(token: &ThemedToken) -> Value {
    let mut json = json!({ "content": token.content, "offset": token.offset });
    if let Some(color) = &token.color {
        json["color"] = json!(color);
    }
    if let Some(font_style) = token.font_style {
        json["fontStyle"] = json!(font_style);
    }
    if let Some(html_style) = &token.html_style {
        json["htmlStyle"] = Value::Object(
            html_style
                .iter()
                .map(|(name, value)| (name.clone(), json!(value)))
                .collect(),
        );
    }
    if let Some(token_type) = token.token_type {
        json["type"] = json!(token_type);
    }
    json
}

fn golden() -> Golden {
    let golden: Golden = load_golden("highlights.json");
    assert!(golden.cases.len() >= 60);
    golden
}

#[test]
fn language_support_and_ids_match() {
    let golden = golden();
    assert!(golden.is_ansi);
    assert!(is_supported_language("ansi") && is_supported_language("ANSI"));
    for case in &golden.cases {
        assert_eq!(
            is_supported_language(&case.lang),
            case.supported,
            "{}",
            case.lang
        );
        let resolved = if case.supported {
            case.lang.as_str()
        } else {
            "text"
        };
        assert_eq!(lang_id_of(resolved), Ok(case.lang_id), "{}", case.lang);
    }
    assert!(lang_id_of("nonexistent-lang").is_err());
}

#[test]
fn tokens_match_the_dist_pipeline() {
    let golden = golden();
    let highlighter = HighlightsHighlighter::new(replay(&golden));
    for (index, case) in golden.cases.iter().enumerate() {
        let options = get_token_options(&case.lang, &case.theme, case.tokenize_max_line_length);
        let tokens = highlighter.code_to_tokens(&case.code, &options);
        let actual = Value::Array(
            tokens
                .iter()
                .map(|line| Value::Array(line.iter().map(token_json).collect()))
                .collect(),
        );
        assert_json_eq(
            &format!(
                "case #{index} {} lang={} theme={:?} max={:?}",
                case.sample, case.lang, case.theme, case.tokenize_max_line_length
            ),
            &case.expected.tokens,
            &actual,
        );
    }
}

#[test]
fn theme_metadata_matches() {
    let golden = golden();
    let highlighter = HighlightsHighlighter::new(replay(&golden));
    for (index, case) in golden.cases.iter().enumerate() {
        let theme = match &case.theme {
            ThemeOption::Name(name) => ThemeChoice::Single(zed(&highlighter, name)),
            ThemeOption::Themes(themes) => ThemeChoice::Keyed(vec![
                (String::from("dark"), zed(&highlighter, &themes.dark)),
                (String::from("light"), zed(&highlighter, &themes.light)),
            ]),
        };
        let lang = if case.supported {
            case.lang.clone()
        } else {
            String::from("text")
        };
        let result = code_to_tokens(
            &replay(&golden),
            &case.code,
            &HighlightsOptions {
                lang,
                theme,
                default_color: DefaultColor::False,
                css_variable_prefix: get_token_options("text", &case.theme, None)
                    .css_variable_prefix,
                tokenize_max_line_length: case.tokenize_max_line_length,
            },
        )
        .expect("tokens");
        let context = format!("case #{index} {}", case.sample);
        assert_eq!(result.meta.fg, case.expected.fg, "{context}");
        assert_eq!(result.meta.bg, case.expected.bg, "{context}");
        assert_eq!(
            result.meta.theme_name, case.expected.theme_name,
            "{context}"
        );
        assert_eq!(
            result.meta.root_style, case.expected.root_style,
            "{context}"
        );
    }
}

#[test]
fn get_theme_matches_the_dist() {
    let golden = golden();
    let highlighter = HighlightsHighlighter::new(replay(&golden));
    for (name, expected) in &golden.themes {
        let theme = highlighter.resolved_theme(name);
        let actual = json!({
            "name": theme.name,
            "type": theme.theme_type,
            "fg": theme.fg,
            "bg": theme.bg,
            "colors": theme.colors,
        });
        assert_json_eq(&format!("getTheme({name})"), expected, &actual);
        let data = highlighter.get_theme(name);
        assert_eq!((data.fg, data.bg), (theme.fg.clone(), theme.bg.clone()));
    }
    assert_eq!(golden.themes.len(), 2);
    assert_eq!(bundled_themes().count(), 2);
}

#[test]
fn bundled_theme_objects_parse() {
    for (name, raw) in bundled_themes() {
        let theme = ZedTheme::from_json(raw);
        assert!(!theme.style.syntax.is_empty(), "{name}");
        assert!(!theme.style.players.is_empty(), "{name}");
    }
}
