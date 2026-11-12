// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/highlighter/backends/highlights.ts
// (and the `DiffsHighlighter` base class members it uses,
// packages/diffs/src/highlighter/DiffsHighlighter.ts).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: implements the crate's [`DiffsHighlighter`] trait over a
// [`HighlightRecords`] source (the wasm host). Not ported: the editor and stream
// tokenizers (`createEditorTokenizer`, `createStreamTokenizer`,
// `HighlightsEditorTokenizer`, `HighlightsStreamTokenizer`), `codeToHtml`, the
// highlighter-type reference counting (`acquireHighlighterType`; a Rust value
// is an instance), and the async `loadLanguages`/`attachLanguages` (bundled
// lexers resolve immediately, so loading is a no-op and
// `has_loaded_languages` is "not disposed"). `getTheme` loads a theme the
// resolver can produce instead of throwing until `resolveThemes` ran, because
// loading is synchronous. A theme the resolver cannot produce, or an option
// error, panics: those are configuration invariants, not runtime input.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

use super::highlighter::HighlightRecords;
use super::highlighter::HighlightsOptions;
use super::highlighter::code_to_tokens;
use super::highlighter::is_supported_language;
use super::theme::ZedTheme;
use super::themes::DiffsTheme;
use super::themes::create_diffs_highlights_theme_resolver;
use super::theming::ThemeResolver;
use super::tokens::DefaultColor;
use super::tokens::ThemeChoice;
use crate::highlighter_types::CodeToTokensOptions;
use crate::highlighter_types::DiffsHighlighter;
use crate::highlighter_types::ThemeData;
use crate::highlighter_types::ThemedToken;
use crate::types::options::ThemeOption;
use crate::types::options::ThemesType;

/// Highlights uses bundled lexers. Unsupported and custom languages render as text.
pub struct HighlightsHighlighter<Source> {
    source: Source,
    pub theme_resolver: ThemeResolver<DiffsTheme>,
    // Independent instances need their themes after the shared cache is cleared.
    used_themes: RefCell<HashMap<String, Rc<DiffsTheme>>>,
    disposed: Cell<bool>,
}

impl<Source: HighlightRecords> HighlightsHighlighter<Source> {
    pub fn new(source: Source) -> Self {
        Self::with_custom_themes(source, BTreeMap::new())
    }

    /// `custom` are Zed theme objects registered by name.
    pub fn with_custom_themes(source: Source, custom: BTreeMap<String, serde_json::Value>) -> Self {
        Self {
            source,
            theme_resolver: create_diffs_highlights_theme_resolver(custom),
            used_themes: RefCell::new(HashMap::new()),
            disposed: Cell::new(false),
        }
    }

    fn assert_not_disposed(&self) {
        assert!(!self.disposed.get(), "highlighter is disposed");
    }

    pub fn is_disposed(&self) -> bool {
        self.disposed.get()
    }

    /// Disposed instances cannot be reused.
    pub fn dispose(&self) {
        if !self.disposed.replace(true) {
            self.used_themes.borrow_mut().clear();
        }
    }

    pub fn resolved_theme(&self, name: &str) -> Rc<DiffsTheme> {
        self.assert_not_disposed();
        let theme = self
            .theme_resolver
            .resolve_theme(name)
            .unwrap_or_else(|error| panic!("{error}"));
        self.used_themes
            .borrow_mut()
            .insert(name.to_owned(), Rc::clone(&theme));
        theme
    }

    /// Bundled lexers resolve immediately: every language counts as loaded.
    pub fn has_loaded_languages(&self, _languages: &[&str]) -> bool {
        !self.disposed.get()
    }

    fn zed_theme(&self, name: &str) -> Rc<ZedTheme> {
        Rc::clone(&self.resolved_theme(name).zed)
    }

    /// `resolveOptions`: the theme names become Zed themes and an unsupported
    /// language becomes `text`.
    fn resolve_options(&self, options: &CodeToTokensOptions) -> HighlightsOptions {
        let lang = if is_supported_language(&options.lang) {
            options.lang.clone()
        } else {
            String::from("text")
        };
        let theme = match &options.theme {
            ThemeOption::Name(name) => ThemeChoice::Single(self.zed_theme(name)),
            ThemeOption::Themes(ThemesType { dark, light }) => ThemeChoice::Keyed(vec![
                (String::from("dark"), self.zed_theme(dark)),
                (String::from("light"), self.zed_theme(light)),
            ]),
        };
        HighlightsOptions {
            lang,
            theme,
            default_color: if options.default_color {
                DefaultColor::default()
            } else {
                DefaultColor::False
            },
            css_variable_prefix: options.css_variable_prefix.clone(),
            tokenize_max_line_length: options.tokenize_max_line_length,
        }
    }
}

impl<Source: HighlightRecords> DiffsHighlighter for HighlightsHighlighter<Source> {
    fn name(&self) -> &str {
        "highlights"
    }

    fn get_theme(&self, theme_name: &str) -> ThemeData {
        self.resolved_theme(theme_name).to_theme_data()
    }

    fn code_to_tokens(&self, code: &str, options: &CodeToTokensOptions) -> Vec<Vec<ThemedToken>> {
        self.assert_not_disposed();
        code_to_tokens(&self.source, code, &self.resolve_options(options))
            .unwrap_or_else(|error| panic!("{error}"))
            .tokens
    }
}
