// Ported from @pierre/highlights 0.4.0 (Apache-2.0, Pierre Computer Company):
// dist/tokens.js (lib/tokens.ts), the token half: `resolveOptionThemes`,
// `lineRecordsToTokens`, `rangeToToken`, `themeHtmlStyle`, `lightDarkStyle`,
// `themeMeta`, `standardTypes`.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `multiThemeBlob`/`multiThemeHtmlOpeners` (packed themes for the wasm
// HTML emitter) are not ported; records are the only output mode. The
// JavaScript caches keyed by object identity (`htmlStyleCache`, `blobCache`,
// `resolvedSetCache`, `preparedIds`) are replaced by per-call vectors indexed by
// token slot. Token `content` is sliced by UTF-16 record offsets through a
// forward-only cursor over the UTF-8 text. `TypeError`s of option validation are
// `HighlightsError`s.

use std::rc::Rc;

use super::data::token_types;
use super::theme::PreparedTheme;
use super::theme::ThemeStyle;
use super::theme::ZedTheme;
use super::theme::prepare_theme;
use crate::highlighter_types::ThemedToken;

/// `0xffffffff`: the record that ends a line.
pub const LINE_MARKER: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HighlightsError {
    /// `themes must not be empty`
    EmptyThemes,
    /// `` `themes` must contain `light` and `dark` when defaultColor is 'light-dark()' ``
    MissingLightDark,
    /// `` `themes` must contain the defaultColor key `<key>` ``
    MissingDefaultColor(String),
    /// `unknown lang: <lang>`
    UnknownLanguage(String),
}

impl std::fmt::Display for HighlightsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyThemes => write!(formatter, "themes must not be empty"),
            Self::MissingLightDark => write!(
                formatter,
                "`themes` must contain `light` and `dark` when defaultColor is 'light-dark()'"
            ),
            Self::MissingDefaultColor(key) => {
                write!(
                    formatter,
                    "`themes` must contain the defaultColor key `{key}`"
                )
            }
            Self::UnknownLanguage(lang) => write!(formatter, "unknown lang: {lang}"),
        }
    }
}

impl std::error::Error for HighlightsError {}

/// `defaultColor?: string | false | 'light-dark()'` (default `'light'`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultColor {
    /// The named theme key is the inline default.
    Key(String),
    /// Every theme is a custom property.
    False,
    /// `'light-dark()'`: pairs the `light` and `dark` themes.
    LightDark,
}

impl Default for DefaultColor {
    fn default() -> Self {
        Self::Key(String::from("light"))
    }
}

/// The theme options of `CodeToTokensOptions`: one theme, or keyed themes.
#[derive(Debug, Clone)]
pub enum ThemeChoice {
    Single(Rc<ZedTheme>),
    /// `themes: { key: theme }`.
    Keyed(Vec<(String, Rc<ZedTheme>)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeRole {
    Single,
    Default,
    Variable,
    LightDark,
}

/// A prepared theme tagged with its color key and role.
#[derive(Debug, Clone)]
pub struct TaggedTheme {
    pub color: Option<String>,
    pub role: ThemeRole,
    pub prepared: Rc<PreparedTheme>,
}

/// Converts Shiki-style options to a theme list. With `themes`, the inline
/// `defaultColor` theme comes first; the remaining themes are sorted by key and
/// use custom properties. `defaultColor: false` uses custom properties for all.
/// `'light-dark()'` pairs the `light` and `dark` themes.
pub fn resolve_option_themes(
    choice: &ThemeChoice,
    default_color: &DefaultColor,
    css_variable_prefix: &str,
) -> Result<Vec<TaggedTheme>, HighlightsError> {
    let keyed = match choice {
        ThemeChoice::Single(theme) => {
            return Ok(vec![TaggedTheme {
                color: None,
                role: ThemeRole::Single,
                prepared: prepare_theme(theme, css_variable_prefix),
            }]);
        }
        ThemeChoice::Keyed(keyed) => keyed,
    };
    let mut entries: Vec<&(String, Rc<ZedTheme>)> = keyed.iter().collect();
    // `a < b ? -1 : 1` over strings compares UTF-16 code units.
    entries.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
    if entries.is_empty() {
        return Err(HighlightsError::EmptyThemes);
    }
    let roles: Vec<ThemeRole>;
    match default_color {
        DefaultColor::LightDark => {
            let find = |key: &str| entries.iter().find(|(entry, _)| entry == key).copied();
            let (Some(light), Some(dark)) = (find("light"), find("dark")) else {
                return Err(HighlightsError::MissingLightDark);
            };
            entries = vec![light, dark];
            roles = vec![ThemeRole::LightDark; 2];
        }
        other => {
            let inline = !matches!(other, DefaultColor::False);
            if let DefaultColor::Key(key) = other {
                let at = entries
                    .iter()
                    .position(|(entry, _)| entry == key)
                    .ok_or_else(|| HighlightsError::MissingDefaultColor(key.clone()))?;
                let chosen = entries.remove(at);
                entries.insert(0, chosen);
            }
            roles = (0..entries.len())
                .map(|index| {
                    if inline && index == 0 {
                        ThemeRole::Default
                    } else {
                        ThemeRole::Variable
                    }
                })
                .collect();
        }
    }
    Ok(entries
        .into_iter()
        .zip(roles)
        .map(|((color, theme), role)| TaggedTheme {
            color: Some(color.clone()),
            role,
            prepared: prepare_theme(theme, css_variable_prefix),
        })
        .collect())
}

/// Maps each token slot to Shiki's `ThemedToken.type`: 1 comment, 2 string,
/// 3 regex, 0 other (`standardTypes`).
pub fn standard_types() -> Vec<u8> {
    token_types()
        .iter()
        .map(|name| match name.as_str() {
            "string.regex" => 3,
            name if name == "comment" || name.starts_with("comment.") => 1,
            name if name == "string" || name.starts_with("string.") => 2,
            _ => 0,
        })
        .collect()
}

type HtmlStyle = Rc<[(String, String)]>;

/// `htmlStyle` for `defaultColor: 'light-dark()'`: both colors with CSS
/// `light-dark()`, plain font settings when the themes agree, per-theme custom
/// properties when they differ.
fn light_dark_style(themes: &[TaggedTheme], hl: usize, prefix: &str) -> Vec<(String, String)> {
    let [light, dark] = themes else {
        panic!("light-dark() pairs exactly two themes");
    };
    let a = light.prepared.styles[hl].as_ref();
    let b = dark.prepared.styles[hl].as_ref();
    let color_of = |style: Option<&ThemeStyle>, theme: &TaggedTheme| {
        style
            .and_then(|style| style.color.clone())
            .or_else(|| theme.prepared.fg.clone())
            .unwrap_or_else(|| String::from("currentcolor"))
    };
    let (ac, bc) = (color_of(a, light), color_of(b, dark));
    let mut html_style = vec![(
        String::from("color"),
        if ac == bc {
            ac
        } else {
            format!("light-dark({ac}, {bc})")
        },
    )];
    let (ai, bi) = (a.is_some_and(|s| s.italic), b.is_some_and(|s| s.italic));
    if ai && bi {
        html_style.push((String::from("font-style"), String::from("italic")));
    } else {
        if ai {
            html_style.push((format!("{prefix}light-font-style"), String::from("italic")));
        }
        if bi {
            html_style.push((format!("{prefix}dark-font-style"), String::from("italic")));
        }
    }
    let (aw, bw) = (a.map_or(0, |s| s.weight), b.map_or(0, |s| s.weight));
    if aw != 0 && aw == bw {
        html_style.push((String::from("font-weight"), aw.to_string()));
    } else {
        if aw != 0 {
            html_style.push((format!("{prefix}light-font-weight"), aw.to_string()));
        }
        if bw != 0 {
            html_style.push((format!("{prefix}dark-font-weight"), bw.to_string()));
        }
    }
    html_style
}

/// `htmlStyle` for token slot `hl`: plain styles for the default theme and
/// custom properties for the others, or `light-dark()` pairs.
fn theme_html_style(themes: &[TaggedTheme], hl: usize, prefix: &str) -> Vec<(String, String)> {
    if themes[0].role == ThemeRole::LightDark {
        return light_dark_style(themes, hl, prefix);
    }
    let mut html_style: Vec<(String, String)> = Vec::new();
    for theme in themes {
        let style = theme.prepared.styles[hl].as_ref();
        let plain = theme.role == ThemeRole::Default;
        let color = theme.color.as_deref().unwrap_or("");
        let key = |plain_name: &str, suffix: &str| {
            if plain {
                plain_name.to_owned()
            } else {
                format!("{prefix}{color}{suffix}")
            }
        };
        html_style.push((
            key("color", ""),
            style
                .and_then(|style| style.color.clone())
                .or_else(|| theme.prepared.fg.clone())
                .unwrap_or_else(|| String::from("inherit")),
        ));
        if style.is_some_and(|style| style.italic) {
            html_style.push((key("font-style", "-font-style"), String::from("italic")));
        }
        if let Some(style) = style.filter(|style| style.weight != 0) {
            html_style.push((key("font-weight", "-font-weight"), style.weight.to_string()));
        }
    }
    html_style
}

/// A forward-only cursor mapping UTF-16 offsets to byte offsets of one text.
struct Utf16Cursor<'text> {
    text: &'text str,
    chars: std::str::CharIndices<'text>,
    units: usize,
    byte: usize,
}

impl<'text> Utf16Cursor<'text> {
    fn new(text: &'text str) -> Self {
        Self {
            text,
            chars: text.char_indices(),
            units: 0,
            byte: 0,
        }
    }

    /// The byte offset of UTF-16 offset `target`, which must not be before the
    /// previous one and must fall on a character boundary.
    fn byte_at(&mut self, target: usize) -> usize {
        while self.units < target {
            let Some((index, ch)) = self.chars.next() else {
                panic!("record offset {target} is past the end of the text");
            };
            self.units += ch.len_utf16();
            self.byte = index + ch.len_utf8();
        }
        assert!(
            self.units == target,
            "record offset {target} splits a surrogate pair"
        );
        if self.units == 0 { 0 } else { self.byte }
    }

    fn slice(&mut self, start: usize, end: usize) -> &'text str {
        let from = self.byte_at(start);
        let to = self.byte_at(end);
        &self.text[from..to]
    }
}

/// Per-call state shared by the tokens of one `codeToTokens`.
struct TokenBuilder<'themes> {
    themes: &'themes [TaggedTheme],
    css_variable_prefix: &'themes str,
    standard: Vec<u8>,
    html_styles: Vec<Option<HtmlStyle>>,
}

impl TokenBuilder<'_> {
    fn html_style(&mut self, hl: usize) -> HtmlStyle {
        let (themes, prefix) = (self.themes, self.css_variable_prefix);
        Rc::clone(
            self.html_styles[hl].get_or_insert_with(|| theme_html_style(themes, hl, prefix).into()),
        )
    }

    fn token(&mut self, content: &str, offset: usize, hl: usize) -> ThemedToken {
        let mut token = ThemedToken {
            content: content.to_owned(),
            offset,
            ..ThemedToken::default()
        };
        if self.themes[0].role == ThemeRole::Single {
            let prepared = &self.themes[0].prepared;
            let style = prepared.styles[hl].as_ref();
            token.color = style
                .and_then(|style| style.color.clone())
                .or_else(|| prepared.fg.clone());
            token.font_style = Some(
                u8::from(style.is_some_and(|style| style.italic))
                    | if style.is_some_and(|style| style.weight >= 600) {
                        2
                    } else {
                        0
                    },
            );
        } else {
            token.html_style = Some(self.html_style(hl));
        }
        let standard = self.standard[hl];
        if standard != 0 {
            token.token_type = Some(standard);
        }
        token
    }
}

/// Converts UTF-16 token records with `0xffffffff` line markers to tokens, one
/// list per line. `offset_base` is the UTF-16 offset of `code` in a larger input
/// (streaming); a line of at least `max_line_length` units becomes one token.
pub fn line_records_to_tokens(
    code: &str,
    records: &[u32],
    themes: &[TaggedTheme],
    css_variable_prefix: &str,
    max_line_length: Option<usize>,
    offset_base: usize,
) -> Vec<Vec<ThemedToken>> {
    let slots = token_types().len();
    let mut builder = TokenBuilder {
        themes,
        css_variable_prefix,
        standard: standard_types(),
        html_styles: vec![None; slots],
    };
    let mut cursor = Utf16Cursor::new(code);
    let max = max_line_length.unwrap_or(0);
    let mut lines = Vec::new();
    let mut start = 0usize;
    let mut rec = 0usize;
    loop {
        let first = rec;
        while rec < records.len() && records[rec + 1] != LINE_MARKER {
            rec += 2;
        }
        let end = if rec == first {
            start
        } else {
            records[rec - 2] as usize
        };
        let line = if max > 0 && end - start >= max {
            vec![builder.token(cursor.slice(start, end), start + offset_base, 0)]
        } else {
            let mut line = Vec::with_capacity((rec - first) / 2);
            let mut at = first;
            while at < rec {
                let end = records[at] as usize;
                if end > start {
                    let hl = records[at + 1] as usize;
                    line.push(builder.token(cursor.slice(start, end), start + offset_base, hl));
                    start = end;
                }
                at += 2;
            }
            line
        };
        lines.push(line);
        if rec == records.len() {
            break;
        }
        start = records[rec] as usize;
        rec += 2;
    }
    lines
}

/// `fg`, `bg`, `themeName` and `rootStyle` of a Shiki `TokensResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThemeMeta {
    pub fg: Option<String>,
    pub bg: Option<String>,
    pub theme_name: String,
    pub root_style: Option<String>,
}

/// With `themes`, `fg` and `bg` are CSS declaration lists starting with the
/// default theme's plain color; `root_style` is set only when no theme is inline.
pub fn theme_meta(themes: &[TaggedTheme], css_variable_prefix: &str) -> ThemeMeta {
    if themes[0].role == ThemeRole::Single {
        let prepared = &themes[0].prepared;
        return ThemeMeta {
            fg: prepared.fg.clone(),
            bg: prepared.bg.clone(),
            theme_name: prepared.name.clone(),
            root_style: None,
        };
    }
    let names: Vec<&str> = themes
        .iter()
        .map(|theme| theme.prepared.name.as_str())
        .collect();
    let theme_name = format!("highlights-themes {}", names.join(" "));
    if themes[0].role == ThemeRole::LightDark {
        let pair = |a: &Option<String>, b: &Option<String>, fallback: &str| {
            let x = a.as_deref().unwrap_or(fallback);
            let y = b.as_deref().unwrap_or(fallback);
            if x == y {
                x.to_owned()
            } else {
                format!("light-dark({x}, {y})")
            }
        };
        return ThemeMeta {
            fg: Some(pair(
                &themes[0].prepared.fg,
                &themes[1].prepared.fg,
                "currentcolor",
            )),
            bg: Some(pair(
                &themes[0].prepared.bg,
                &themes[1].prepared.bg,
                "transparent",
            )),
            theme_name,
            root_style: None,
        };
    }
    let join = |value: fn(&PreparedTheme) -> &Option<String>, suffix: &str| {
        themes
            .iter()
            .map(|theme| {
                let color = value(&theme.prepared).as_deref().unwrap_or("inherit");
                if theme.role == ThemeRole::Default {
                    color.to_owned()
                } else {
                    format!(
                        "{css_variable_prefix}{}{suffix}:{color}",
                        theme.color.as_deref().unwrap_or("")
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(";")
    };
    let fg = join(|prepared| &prepared.fg, "");
    let bg = join(|prepared| &prepared.bg, "-bg");
    let root_style = (themes[0].role != ThemeRole::Default).then(|| format!("{fg};{bg}"));
    ThemeMeta {
        fg: Some(fg),
        bg: Some(bg),
        theme_name,
        root_style,
    }
}
