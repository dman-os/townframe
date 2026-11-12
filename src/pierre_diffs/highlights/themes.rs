// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/highlighter/themes/themeResolver.ts
// (the `highlights` backend half) and the `DiffsTheme` shape of
// packages/diffs/src/highlighter/themes/types.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: synchronous (see `theming.rs`). Only the `highlights` backend is
// ported: the TextMate/Shiki branch (`createCssVariablesTheme`,
// `normalizeTheme`, the `@pierre/theming/themes` catalog), the worker-context
// guard and the `registerCustomTheme` TextMate/`diffs` loaders are dropped; a
// custom theme is a Zed theme object registered by name. The bundled catalog is
// the two Pierre themes (`pierre-dark`, `pierre-light`) exported from the
// pinned dist; the other 150 bundled Zed themes are not embedded.

use std::collections::BTreeMap;
use std::rc::Rc;

use serde_json::Value;

use super::data::bundled_themes;
use super::theme::CssVariables;
use super::theme::ZedTheme;
use super::theming::ThemeError;
use super::theming::ThemeResolver;
use crate::highlighter_types::ThemeData;
use crate::types::options::BaseThemeType;

/// Shared editor colors with the syntax theme (`DiffsTheme`).
#[derive(Debug, Clone, PartialEq)]
pub struct DiffsTheme {
    pub name: String,
    pub theme_type: BaseThemeType,
    pub fg: String,
    pub bg: String,
    pub colors: BTreeMap<String, String>,
    pub zed: Rc<ZedTheme>,
}

impl DiffsTheme {
    /// The record `highlighter.getTheme(name)` hands the renderers.
    pub fn to_theme_data(&self) -> ThemeData {
        ThemeData {
            theme_type: Some(self.theme_type),
            fg: self.fg.clone(),
            bg: self.bg.clone(),
            colors: self.colors.clone(),
        }
    }
}

/// Maps Zed colors to the VS Code keys used by editor and diff overlays:
/// `[target, ...sources]`.
const ZED_COLOR_ALIASES: &[&[&str]] = &[
    &[
        "editor.lineHighlightBackground",
        "editor.active_line.background",
    ],
    &["editor.selectionBackground", "element.selected"],
    &["editor.findMatchBackground", "search.match_background"],
    &[
        "editor.findMatchHighlightBackground",
        "search.match_background",
    ],
    &[
        "editorBracketMatch.background",
        "editor.document_highlight.bracket_background",
    ],
    &["editorError.foreground", "error"],
    &["editorWarning.foreground", "warning"],
    &["editorInfo.foreground", "info"],
    &["editorHint.foreground", "hint"],
    &[
        "gitDecoration.addedResourceForeground",
        "created",
        "terminal.ansi.green",
    ],
    &[
        "gitDecoration.deletedResourceForeground",
        "deleted",
        "terminal.ansi.red",
    ],
    &[
        "gitDecoration.modifiedResourceForeground",
        "modified",
        "terminal.ansi.blue",
    ],
];

/// `createHighlightsTheme`: a [`DiffsTheme`] from a Zed theme or family.
pub fn create_highlights_theme(name: &str, raw: &Value) -> DiffsTheme {
    let zed = ZedTheme::from_json(raw).with_name(name);
    // Every string-valued style entry is a color.
    let mut colors: BTreeMap<String, String> = zed.style.strings.clone();
    // Zed keeps the local user's caret and selection in the first player slot.
    if let Some(player) = zed.style.players.first() {
        if let Some(cursor) = &player.cursor {
            colors
                .entry(String::from("editorCursor.foreground"))
                .or_insert_with(|| cursor.clone());
        }
        if let Some(selection) = &player.selection {
            colors
                .entry(String::from("editor.selectionBackground"))
                .or_insert_with(|| selection.clone());
        }
    }
    for alias in ZED_COLOR_ALIASES {
        let (target, sources) = alias.split_first().expect("an alias has a target");
        if colors.contains_key(*target) {
            continue;
        }
        if let Some(value) = sources
            .iter()
            .find_map(|source| colors.get(*source))
            .cloned()
        {
            colors.insert((*target).to_owned(), value);
        }
    }
    match &zed.css_variables {
        CssVariables::All => {
            colors.insert(
                String::from("editor.foreground"),
                String::from("var(--hls-foreground)"),
            );
            colors.insert(
                String::from("editor.background"),
                String::from("var(--hls-background)"),
            );
        }
        CssVariables::Palette(palette) => {
            let foreground = colors
                .get("text")
                .or_else(|| colors.get("foreground"))
                .cloned()
                .unwrap_or_else(|| String::from("foreground"));
            colors
                .entry(String::from("editor.foreground"))
                .or_insert(foreground);
            let background = colors
                .get("background")
                .cloned()
                .unwrap_or_else(|| String::from("background"));
            colors
                .entry(String::from("editor.background"))
                .or_insert(background);
            let prefix = palette.prefix.as_deref().unwrap_or("--hls-");
            for value in colors.values_mut() {
                let fallback = palette
                    .defaults
                    .get(value.as_str())
                    .filter(|d| !d.is_empty());
                *value = match fallback {
                    Some(fallback) => format!("var({prefix}{value}, {fallback})"),
                    None => format!("var({prefix}{value})"),
                };
            }
        }
        CssVariables::None => {}
    }
    let theme_type = if zed.appearance.as_deref() == Some("light") {
        BaseThemeType::Light
    } else {
        BaseThemeType::Dark
    };
    let light = theme_type == BaseThemeType::Light;
    let pick = |keys: &[&str], fallback: &str| {
        keys.iter()
            .find_map(|key| colors.get(*key))
            .cloned()
            .unwrap_or_else(|| fallback.to_owned())
    };
    DiffsTheme {
        name: name.to_owned(),
        theme_type,
        fg: pick(
            &["editor.foreground", "text", "foreground"],
            if light { "#333333" } else { "#bbbbbb" },
        ),
        bg: pick(
            &["editor.background", "background"],
            if light { "#ffffff" } else { "#1e1e1e" },
        ),
        colors,
        zed: Rc::new(zed),
    }
}

/// `createDiffsThemeResolver('highlights')`. `custom` holds the Zed theme
/// objects registered by name (`registerCustomTheme(name, loader, 'zed')`),
/// which win over the bundled catalog.
pub fn create_diffs_highlights_theme_resolver(
    custom: BTreeMap<String, Value>,
) -> ThemeResolver<DiffsTheme> {
    ThemeResolver::new(
        Some(Box::new(move |name| {
            if let Some(raw) = custom.get(name) {
                return Some(create_highlights_theme(name, raw));
            }
            bundled_themes()
                .find(|(bundled, _)| *bundled == name)
                .map(|(_, raw)| create_highlights_theme(name, raw))
        })),
        Some(Box::new(|theme, name| {
            if theme.name == name {
                Ok(theme)
            } else {
                Err(ThemeError::Invalid(format!(
                    "resolvedTheme: themeName: {name} does not match theme.name: {}",
                    theme.name
                )))
            }
        })),
    )
}
