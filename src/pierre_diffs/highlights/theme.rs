// Ported from @pierre/highlights 0.4.0 (Apache-2.0, Pierre Computer Company):
// dist/theme.js (lib/theme.ts), the style-resolution half.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: themes are the typed `ZedTheme` (parsed once from the theme JSON),
// so `resolveTheme`/`describeThemeValue`/`isThemeObject` (JavaScript input
// validation of "theme or family object") are unnecessary: a family is
// flattened to its first member by `ZedTheme::from_json`. The pieces that only
// serve the wasm HTML emitters (`packHtmlOpeners`, `themeHtmlOpeners`, the
// 384-byte packed `table`, `themeAnsiColors`, `escapeAttribute`) are not
// ported: this port consumes records (mode 3) only. `prepareTheme` caches by
// theme identity upstream (WeakMap); here each `ZedTheme` owns its cache,
// keyed by the CSS variable prefix.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::Value;

use super::data::token_types;

pub const DEFAULT_CSS_VARIABLE_PREFIX: &str = "--hls-";

/// A resolved token style: the color (validated, normalised) and font settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThemeStyle {
    pub color: Option<String>,
    pub italic: bool,
    /// 0 (unset) or 100..=900 in steps of 100.
    pub weight: u16,
}

/// One entry of a theme's `syntax` map: a bare color or a settings object.
#[derive(Debug, Clone, PartialEq)]
pub enum SyntaxEntry {
    Color(String),
    Settings(SyntaxSettings),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SyntaxSettings {
    pub color: Option<String>,
    pub font_style: Option<String>,
    pub font_weight: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Player {
    pub cursor: Option<String>,
    pub selection: Option<String>,
}

/// A theme's `style`: string-valued entries plus `players` and `syntax`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZedStyle {
    /// Every string-valued entry (`editor.background`, `terminal.ansi.red`, ...).
    pub strings: BTreeMap<String, String>,
    pub players: Vec<Player>,
    pub syntax: BTreeMap<String, SyntaxEntry>,
}

/// `cssVariables?: true | { prefix?, defaults? }`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CssVariables {
    #[default]
    None,
    /// `true`: every slot is `var(<prefix><slot>)`.
    All,
    Palette(CssPalette),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CssPalette {
    pub prefix: Option<String>,
    pub defaults: BTreeMap<String, String>,
}

/// The styles of every token slot of one theme (`prepareTheme`'s result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTheme {
    pub name: String,
    /// Indexed by token slot; `None` for slots the theme leaves unstyled.
    pub styles: Vec<Option<ThemeStyle>>,
    pub fg: Option<String>,
    pub bg: Option<String>,
}

#[derive(Debug)]
pub struct ZedTheme {
    pub name: String,
    pub appearance: Option<String>,
    pub style: ZedStyle,
    pub css_variables: CssVariables,
    prepared: RefCell<HashMap<String, Rc<PreparedTheme>>>,
}

impl PartialEq for ZedTheme {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.appearance == other.appearance
            && self.style == other.style
            && self.css_variables == other.css_variables
    }
}

impl Clone for ZedTheme {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            appearance: self.appearance.clone(),
            style: self.style.clone(),
            css_variables: self.css_variables.clone(),
            prepared: RefCell::new(HashMap::new()),
        }
    }
}

fn string_of(value: &Value, what: &str) -> String {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{what} must be a string"))
        .to_owned()
}

impl ZedTheme {
    /// Parses a Zed theme object, or a `ThemeFamily` (`{ themes: [...] }`,
    /// whose first member is used, as `createHighlightsTheme` does).
    ///
    /// Panics on a malformed theme: bundled and registered themes are data
    /// under our control, not external input.
    pub fn from_json(value: &Value) -> Self {
        let value = match value.get("themes").and_then(Value::as_array) {
            Some(themes) => themes.first().expect("an empty Zed theme family"),
            None => value,
        };
        let style = value.get("style").and_then(Value::as_object);
        let mut parsed = ZedStyle::default();
        for (key, entry) in style.into_iter().flatten() {
            match (key.as_str(), entry) {
                ("players", Value::Array(players)) => {
                    parsed.players = players
                        .iter()
                        .map(|player| Player {
                            cursor: player
                                .get("cursor")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            selection: player
                                .get("selection")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        })
                        .collect();
                }
                ("syntax", Value::Object(syntax)) => {
                    for (name, setting) in syntax {
                        parsed
                            .syntax
                            .insert(name.clone(), Self::syntax_entry(setting));
                    }
                }
                (_, Value::String(text)) => {
                    parsed.strings.insert(key.clone(), text.clone());
                }
                _ => {}
            }
        }
        let css_variables = match value.get("cssVariables") {
            None | Some(Value::Null) | Some(Value::Bool(false)) => CssVariables::None,
            Some(Value::Bool(true)) => CssVariables::All,
            Some(Value::Object(palette)) => CssVariables::Palette(CssPalette {
                prefix: palette
                    .get("prefix")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                defaults: palette
                    .get("defaults")
                    .and_then(Value::as_object)
                    .map(|defaults| {
                        defaults
                            .iter()
                            .map(|(key, value)| (key.clone(), string_of(value, "css default")))
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
            Some(other) => panic!("unexpected cssVariables {other}"),
        };
        Self {
            name: string_of(&value["name"], "theme name"),
            appearance: value
                .get("appearance")
                .and_then(Value::as_str)
                .map(str::to_owned),
            style: parsed,
            css_variables,
            prepared: RefCell::new(HashMap::new()),
        }
    }

    fn syntax_entry(setting: &Value) -> SyntaxEntry {
        match setting {
            Value::String(color) => SyntaxEntry::Color(color.clone()),
            Value::Object(settings) => SyntaxEntry::Settings(SyntaxSettings {
                color: settings
                    .get("color")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                font_style: settings
                    .get("font_style")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                font_weight: settings.get("font_weight").and_then(Value::as_f64),
            }),
            other => panic!("unexpected syntax entry {other}"),
        }
    }

    /// A copy carrying `name`, as `{ ...theme, name }` in `createHighlightsTheme`.
    pub fn with_name(&self, name: &str) -> Self {
        let mut theme = self.clone();
        theme.name = name.to_owned();
        theme
    }
}

/// `editor.foreground`, `text`, then `foreground`.
pub fn theme_foreground(style: &ZedStyle) -> Option<&str> {
    ["editor.foreground", "text", "foreground"]
        .into_iter()
        .find_map(|key| style.strings.get(key))
        .map(String::as_str)
}

/// `editor.background`, then `background`.
pub fn theme_background(style: &ZedStyle) -> Option<&str> {
    ["editor.background", "background"]
        .into_iter()
        .find_map(|key| style.strings.get(key))
        .map(String::as_str)
}

fn is_hex_color(value: &str) -> bool {
    let Some(digits) = value.strip_prefix('#') else {
        return false;
    };
    matches!(digits.len(), 3 | 4 | 6 | 8) && digits.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `\d+(\.\d*)?|\.\d+` with an optional sign, as a prefix of `text`.
fn skip_number(text: &str) -> Option<&str> {
    let text = text.strip_prefix(['+', '-']).unwrap_or(text);
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    if digits > 0 {
        let rest = &text[digits..];
        return Some(match rest.strip_prefix('.') {
            Some(fraction) => &fraction[fraction.bytes().take_while(u8::is_ascii_digit).count()..],
            None => rest,
        });
    }
    let fraction = text.strip_prefix('.')?;
    let count = fraction.bytes().take_while(u8::is_ascii_digit).count();
    (count > 0).then(|| &fraction[count..])
}

fn is_display_p3(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("color(display-p3") else {
        return false;
    };
    let mut rest = rest;
    for _ in 0..3 {
        let trimmed = rest.trim_start();
        // `\s+` before each component: at least one whitespace character.
        if trimmed.len() == rest.len() {
            return false;
        }
        let Some(after) = skip_number(trimmed) else {
            return false;
        };
        rest = after;
    }
    if let Some(alpha) = rest.trim_start().strip_prefix('/') {
        let Some(after) = skip_number(alpha.trim_start()) else {
            return false;
        };
        rest = after;
    }
    rest.trim_start() == ")"
}

/// Hex and numeric Display P3 colors that cannot escape a CSS declaration.
pub fn is_theme_color(value: &str) -> bool {
    let value = value.trim();
    is_hex_color(value) || is_display_p3(value)
}

/// `{ color?, font_style?, font_weight? }` after inheritance through parents.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResolvedSyntax {
    pub color: Option<String>,
    pub font_style: Option<String>,
    pub font_weight: Option<f64>,
}

/// Keeps the nearest scope's font settings while finding an inherited color.
pub fn resolve_theme_syntax(
    syntax: &BTreeMap<String, SyntaxEntry>,
    name: &str,
) -> Option<ResolvedSyntax> {
    let mut settings: Option<&SyntaxSettings> = None;
    let mut key = name;
    while !key.is_empty() {
        match syntax.get(key) {
            Some(SyntaxEntry::Color(color)) => {
                return Some(ResolvedSyntax {
                    color: Some(color.clone()),
                    font_style: settings.and_then(|s| s.font_style.clone()),
                    font_weight: settings.and_then(|s| s.font_weight),
                });
            }
            Some(SyntaxEntry::Settings(entry)) => {
                let nearest = *settings.get_or_insert(entry);
                if let Some(color) = &entry.color {
                    return Some(ResolvedSyntax {
                        color: Some(color.clone()),
                        font_style: nearest.font_style.clone(),
                        font_weight: nearest.font_weight,
                    });
                }
            }
            None => {}
        }
        key = key.rfind('.').map_or("", |dot| &key[..dot]);
    }
    settings.map(|settings| ResolvedSyntax {
        color: settings.color.clone(),
        font_style: settings.font_style.clone(),
        font_weight: settings.font_weight,
    })
}

/// `Math.round(weight / 100) * 100` for a valid weight, 0 otherwise.
fn normalise_weight(weight: Option<f64>) -> u16 {
    match weight {
        Some(weight) if (100.0..=900.0).contains(&weight) => (weight / 100.0).round() as u16 * 100,
        _ => 0,
    }
}

/// Resolves identical colors and font settings for Wasm and JavaScript output.
pub fn resolve_theme_style(style: &ZedStyle, name: &str) -> ThemeStyle {
    let (mut color, font_style, font_weight) = match name {
        "background" => (theme_background(style).map(str::to_owned), None, None),
        "foreground" => (theme_foreground(style).map(str::to_owned), None, None),
        _ => {
            let resolved = resolve_theme_syntax(&style.syntax, name).unwrap_or_default();
            let mut color = resolved.color;
            if (resolved.font_style.is_some() || resolved.font_weight.is_some()) && color.is_none()
            {
                color = theme_foreground(style).map(str::to_owned);
            }
            (color, resolved.font_style, resolved.font_weight)
        }
    };
    color = color.and_then(|color| {
        if !is_theme_color(&color) {
            return None;
        }
        let mut color = color.trim().to_lowercase();
        if color.starts_with('#') {
            if color.len() <= 5 {
                // `replace(/[^#]/g, '$&$&')`: every digit doubled.
                color = color
                    .chars()
                    .flat_map(|ch| std::iter::repeat_n(ch, if ch == '#' { 1 } else { 2 }))
                    .collect();
            }
            if color.len() == 9 && color.ends_with("ff") {
                color.truncate(7);
            }
            if color == "#00000000" {
                return None;
            }
        }
        Some(color)
    });
    ThemeStyle {
        color,
        italic: font_style.as_deref() == Some("italic"),
        weight: normalise_weight(font_weight),
    }
}

/// `var(<prefix><name with . and _ as ->)`
pub fn css_variable(name: &str, css_variable_prefix: &str) -> String {
    format!(
        "var({css_variable_prefix}{})",
        name.replace(['.', '_'], "-")
    )
}

fn prepare_css_variables(theme: &ZedTheme, css_variable_prefix: &str) -> PreparedTheme {
    let names = token_types();
    let mut styles = vec![None; names.len()];
    for (slot, name) in names.iter().enumerate().skip(1) {
        styles[slot] = Some(ThemeStyle {
            color: Some(css_variable(name, css_variable_prefix)),
            italic: false,
            weight: 0,
        });
    }
    PreparedTheme {
        name: theme.name.clone(),
        styles,
        fg: Some(css_variable("foreground", css_variable_prefix)),
        bg: Some(css_variable("background", css_variable_prefix)),
    }
}

/// Palette colors are variable suffixes, not literal CSS values.
fn prepare_css_palette(theme: &ZedTheme, palette: &CssPalette) -> PreparedTheme {
    let prefix = palette
        .prefix
        .as_deref()
        .unwrap_or(DEFAULT_CSS_VARIABLE_PREFIX);
    let variable = |name: &str| match palette.defaults.get(name).filter(|d| !d.is_empty()) {
        Some(fallback) => format!("var({prefix}{name}, {fallback})"),
        None => format!("var({prefix}{name})"),
    };
    let names = token_types();
    let mut styles = vec![None; names.len()];
    for (slot, name) in names.iter().enumerate().skip(1) {
        let setting = resolve_theme_syntax(&theme.style.syntax, name);
        styles[slot] = Some(ThemeStyle {
            color: Some(variable(
                setting
                    .as_ref()
                    .and_then(|setting| setting.color.as_deref())
                    .unwrap_or("foreground"),
            )),
            italic: setting.as_ref().and_then(|s| s.font_style.as_deref()) == Some("italic"),
            weight: setting
                .as_ref()
                .and_then(|setting| setting.font_weight)
                .map_or(0, |weight| (weight / 100.0).round() as u16 * 100),
        });
    }
    PreparedTheme {
        name: theme.name.clone(),
        styles,
        fg: Some(variable(
            theme_foreground(&theme.style).unwrap_or("foreground"),
        )),
        bg: Some(variable(
            theme_background(&theme.style).unwrap_or("background"),
        )),
    }
}

/// Resolves the color and font settings of every token slot.
fn prepare_styles(theme: &ZedTheme) -> PreparedTheme {
    let names = token_types();
    let mut styles = vec![None; names.len()];
    let (mut fg, mut bg) = (None, None);
    for (slot, name) in names.iter().enumerate().skip(1) {
        let style = resolve_theme_style(&theme.style, name);
        if style.color.is_none() && !style.italic && style.weight == 0 {
            continue;
        }
        match name.as_str() {
            "foreground" => fg = style.color,
            "background" => bg = style.color,
            _ => styles[slot] = Some(style),
        }
    }
    PreparedTheme {
        name: theme.name.clone(),
        styles,
        fg,
        bg,
    }
}

/// Resolves all token styles of a theme. Cached per theme and, for
/// `cssVariables: true`, per prefix.
pub fn prepare_theme(theme: &ZedTheme, css_variable_prefix: &str) -> Rc<PreparedTheme> {
    let prefix = if theme.css_variables == CssVariables::All {
        css_variable_prefix
    } else {
        ""
    };
    if let Some(cached) = theme.prepared.borrow().get(prefix) {
        return Rc::clone(cached);
    }
    let prepared = Rc::new(match &theme.css_variables {
        CssVariables::All => prepare_css_variables(theme, css_variable_prefix),
        CssVariables::Palette(palette) => prepare_css_palette(theme, palette),
        CssVariables::None => prepare_styles(theme),
    });
    theme
        .prepared
        .borrow_mut()
        .insert(prefix.to_owned(), Rc::clone(&prepared));
    prepared
}
