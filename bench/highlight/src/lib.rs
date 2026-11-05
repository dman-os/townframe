//! Tokenizer candidates behind one shape: `tokenize(engine, lang, text)` returns, per line, a list of
//! styled runs `Tok { start, end, rgba, font_style }` with byte offsets into `text`. Every engine pays
//! its own conversion into that shape inside the timed region, because a consumer needs exactly this
//! (text range plus style) to emit spans.

pub mod run;
#[cfg(feature = "irosashi")]
pub mod probe;

#[derive(Clone, Copy, Debug)]
pub struct Tok {
    pub start: u32,
    pub end: u32,
    /// 0xRRGGBBAA; 0 = engine emitted no colour.
    pub rgba: u32,
    /// Shiki flags: italic 1, bold 2, underline 4, strikethrough 8.
    pub font_style: u8,
}

pub type Lines = Vec<Vec<Tok>>;

pub const THEME_JSON: &str = include_str!("../themes/pierre-dark.json");

pub struct Engine {
    inner: Inner,
}

enum Inner {
    #[cfg(feature = "irosashi")]
    Irosashi(irosashi::Highlighter),
    #[cfg(feature = "syntect")]
    Syntect(syntect_engine::State),
    #[cfg(feature = "arborium")]
    Arborium(std::cell::RefCell<arborium::Highlighter>, scope_match::Rules),
    None,
}

impl Engine {
    /// Construction is the cold cost: grammar/theme registry build.
    pub fn new(name: &str) -> Engine {
        let inner = match name {
            #[cfg(feature = "irosashi-full")]
            "irosashi" => Inner::Irosashi(
                irosashi::HighlighterBuilder::embedded()
                    .theme("pierre-dark", THEME_JSON.as_bytes().to_vec())
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            // Only the grammars this benchmark uses, compiled in with include_bytes!: the wasm size of
            // the engine without the 257-grammar asset directory.
            #[cfg(all(feature = "irosashi", not(feature = "irosashi-full")))]
            "irosashi-min" => Inner::Irosashi(
                irosashi::HighlighterBuilder::without_bundled_assets()
                    .grammar("rust", &include_bytes!("../grammars/rust.json")[..])
                    .unwrap()
                    .grammar("typescript", &include_bytes!("../grammars/typescript.json")[..])
                    .unwrap()
                    .grammar("javascript", &include_bytes!("../grammars/javascript.json")[..])
                    .unwrap()
                    .grammar("json", &include_bytes!("../grammars/json.json")[..])
                    .unwrap()
                    .grammar("markdown", &include_bytes!("../grammars/markdown.json")[..])
                    .unwrap()
                    .theme("pierre-dark", THEME_JSON.as_bytes().to_vec())
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            #[cfg(feature = "syntect")]
            "syntect" => Inner::Syntect(syntect_engine::State::new()),
            #[cfg(feature = "arborium")]
            "arborium" => Inner::Arborium(
                std::cell::RefCell::new(arborium::Highlighter::new()),
                scope_match::Rules::from_vscode(THEME_JSON),
            ),
            "none" => Inner::None,
            other => panic!("engine {other} not compiled in"),
        };
        Engine { inner }
    }

    pub fn tokenize(&self, lang: &str, text: &str) -> Lines {
        match &self.inner {
            #[cfg(feature = "irosashi")]
            Inner::Irosashi(h) => {
                let l = match lang {
                    "ts" => "typescript",
                    "md" => "markdown",
                    other => other,
                };
                let res = h
                    .code_to_tokens(text, &irosashi::CodeToTokensOptions::new(l, "pierre-dark"))
                    .unwrap();
                let mut out = Vec::with_capacity(res.lines.len());
                for line in &res.lines {
                    let base = line.range.start as u32;
                    let mut toks = Vec::with_capacity(line.tokens.len());
                    for t in &line.tokens {
                        toks.push(Tok {
                            start: base + t.start as u32,
                            end: base + t.end as u32,
                            rgba: t.style.color.map_or(0, |c| parse_hex(res.color(c))),
                            font_style: t.style.font_style.bits(),
                        });
                    }
                    out.push(toks);
                }
                out
            }
            #[cfg(feature = "syntect")]
            Inner::Syntect(s) => s.tokenize(lang, text),
            #[cfg(feature = "arborium")]
            Inner::Arborium(h, rules) => arborium_engine::tokenize(&mut h.borrow_mut(), rules, lang, text),
            Inner::None => Vec::new(),
        }
    }

    /// Resumable per-line tokenization for the irosashi session API: tokenizes lines
    /// `[from, from+count)` given the saved state of line `from-1`, returning the state after the
    /// last line and the number of tokens produced. Used by the incremental probe.
    #[cfg(feature = "irosashi")]
    pub fn irosashi(&self) -> &irosashi::Highlighter {
        match &self.inner {
            Inner::Irosashi(h) => h,
            _ => panic!("not irosashi"),
        }
    }
}

pub fn parse_hex(s: &str) -> u32 {
    let h = s.trim_start_matches('#');
    let v = u32::from_str_radix(h, 16).unwrap();
    match h.len() {
        6 => (v << 8) | 0xff,
        8 => v,
        3 => {
            let (r, g, b) = ((v >> 8) & 0xf, (v >> 4) & 0xf, v & 0xf);
            (((r << 4 | r) << 24) | ((g << 4 | g) << 16) | ((b << 4 | b) << 8)) | 0xff
        }
        n => panic!("colour {s} has {n} digits"),
    }
}

/// Minimal VS Code tokenColors matcher for engines that emit capture names instead of TextMate
/// scope stacks (tree-sitter). Longest matching selector prefix wins; ties go to the later rule,
/// like vscode-textmate. Parent-scope selectors ("a b") are ignored: a capture has no scope stack.
#[cfg(feature = "arborium")]
mod scope_match {
    pub struct Rules {
        rules: Vec<(String, Option<u32>, u8)>,
        pub fg: u32,
    }

    impl Rules {
        pub fn from_vscode(json: &str) -> Rules {
            let v: serde_json::Value = serde_json::from_str(json).unwrap();
            let fg = super::parse_hex(v["colors"]["editor.foreground"].as_str().unwrap());
            let mut rules = Vec::new();
            for item in v["tokenColors"].as_array().unwrap() {
                let Some(scope) = item.get("scope") else { continue };
                let sels: Vec<String> = match scope {
                    serde_json::Value::String(s) => s.split(',').map(|x| x.trim().to_owned()).collect(),
                    serde_json::Value::Array(a) => a.iter().map(|x| x.as_str().unwrap().to_owned()).collect(),
                    other => panic!("scope shape {other}"),
                };
                let st = &item["settings"];
                let color = st.get("foreground").and_then(|x| x.as_str()).map(super::parse_hex);
                let mut fs = 0u8;
                if let Some(f) = st.get("fontStyle").and_then(|x| x.as_str()) {
                    for w in f.split_whitespace() {
                        match w { "italic" => fs |= 1, "bold" => fs |= 2, "underline" => fs |= 4, _ => {} }
                    }
                }
                for sel in sels {
                    if !sel.contains(' ') {
                        rules.push((sel, color, fs));
                    }
                }
            }
            Rules { rules, fg }
        }

        pub fn resolve(&self, scope: &str) -> (u32, u8) {
            let (mut best_len, mut color, mut fs) = (0usize, self.fg, 0u8);
            for (sel, c, f) in &self.rules {
                let hit = scope == sel || (scope.len() > sel.len() && scope.starts_with(sel.as_str()) && scope.as_bytes()[sel.len()] == b'.');
                if hit && sel.len() >= best_len {
                    best_len = sel.len();
                    if let Some(c) = c { color = *c; }
                    fs = *f;
                }
            }
            (color, fs)
        }
    }
}

#[cfg(feature = "arborium")]
mod arborium_engine {
    use super::{Lines, Tok, scope_match::Rules};

    /// tree-sitter capture name -> TextMate scope that themes know about. Approximate by nature:
    /// this table is the lossy step between a tree-sitter highlighter and a TextMate theme.
    fn scope_of(capture: &str) -> String {
        let (head, rest) = capture.split_once('.').unwrap_or((capture, ""));
        let base = match head {
            "keyword" => "keyword",
            "function" | "method" => "entity.name.function",
            "type" => "entity.name.type",
            "string" => "string",
            "comment" => "comment",
            "number" => "constant.numeric",
            "constant" => "constant",
            "variable" if rest == "builtin" => "variable.language",
            "variable" => "variable",
            "property" => "variable.other.property",
            "operator" => "keyword.operator",
            "punctuation" => "punctuation",
            "tag" => "entity.name.tag",
            "attribute" => "entity.other.attribute-name",
            "module" | "namespace" => "entity.name.namespace",
            "label" => "entity.name.label",
            "escape" => "constant.character.escape",
            "constructor" => "entity.name.type",
            other => other,
        };
        if rest.is_empty() || matches!(head, "variable" if rest == "builtin") { base.to_owned() } else { format!("{base}.{rest}") }
    }

    pub fn tokenize(h: &mut arborium::Highlighter, rules: &Rules, lang: &str, text: &str) -> Lines {
        let l = match lang { "ts" => "typescript", "md" => "markdown", o => o };
        let mut spans = h.highlight_spans(l, text).unwrap();
        // Outer spans first so inner (later-painted) spans win; equal ranges: higher pattern index wins.
        spans.sort_by(|a, b| (a.start, std::cmp::Reverse(a.end), a.pattern_index).cmp(&(b.start, std::cmp::Reverse(b.end), b.pattern_index)));
        let mut cache: std::collections::HashMap<String, u16> = Default::default();
        let mut table: Vec<(u32, u8)> = vec![(rules.fg, 0)];
        let mut paint = vec![0u16; text.len()];
        for s in &spans {
            let id = *cache.entry(s.capture.clone()).or_insert_with(|| {
                table.push(rules.resolve(&scope_of(&s.capture)));
                (table.len() - 1) as u16
            });
            paint[s.start as usize..s.end as usize].fill(id);
        }
        let mut out = Vec::new();
        let bytes = text.as_bytes();
        let mut line_start = 0usize;
        while line_start < bytes.len() {
            let line_end = bytes[line_start..].iter().position(|&b| b == b'\n').map_or(bytes.len(), |p| line_start + p);
            let mut toks = Vec::new();
            let mut i = line_start;
            while i < line_end {
                let id = paint[i];
                let mut j = i + 1;
                while j < line_end && paint[j] == id { j += 1; }
                let (rgba, fs) = table[id as usize];
                toks.push(Tok { start: i as u32, end: j as u32, rgba, font_style: fs });
                i = j;
            }
            out.push(toks);
            line_start = line_end + 1;
        }
        out
    }
}

#[cfg(feature = "syntect")]
mod syntect_engine {
    use super::{Lines, Tok, parse_hex};
    use std::str::FromStr;
    use syntect::easy::HighlightLines;
    use syntect::highlighting::{
        Color, FontStyle, ScopeSelectors, StyleModifier, Theme, ThemeItem, ThemeSettings,
    };
    use syntect::parsing::SyntaxSet;
    use syntect::util::LinesWithEndings;

    pub struct State {
        syntaxes: SyntaxSet,
        theme: Theme,
    }

    fn color(hex: &str) -> Color {
        let v = parse_hex(hex);
        Color { r: (v >> 24) as u8, g: (v >> 16) as u8, b: (v >> 8) as u8, a: v as u8 }
    }

    /// syntect has no VS Code theme loader: build its `Theme` from the same tokenColors the oracle
    /// (Shiki) uses, so both engines colour from one source of truth.
    fn theme_from_vscode(json: &str) -> Theme {
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        let fg = v["colors"]["editor.foreground"].as_str().unwrap();
        let mut scopes = Vec::new();
        for item in v["tokenColors"].as_array().unwrap() {
            let Some(scope) = item.get("scope") else { continue };
            let sel = match scope {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Array(a) => {
                    a.iter().map(|x| x.as_str().unwrap()).collect::<Vec<_>>().join(", ")
                }
                other => panic!("scope shape {other}"),
            };
            let st = &item["settings"];
            let mut fs = None;
            if let Some(f) = st.get("fontStyle").and_then(|x| x.as_str()) {
                let mut bits = FontStyle::empty();
                for w in f.split_whitespace() {
                    match w {
                        "italic" => bits |= FontStyle::ITALIC,
                        "bold" => bits |= FontStyle::BOLD,
                        "underline" => bits |= FontStyle::UNDERLINE,
                        _ => {}
                    }
                }
                fs = Some(bits);
            }
            scopes.push(ThemeItem {
                scope: ScopeSelectors::from_str(&sel).unwrap(),
                style: StyleModifier {
                    foreground: st.get("foreground").and_then(|x| x.as_str()).map(color),
                    background: None,
                    font_style: fs,
                },
            });
        }
        Theme {
            name: Some("pierre-dark".into()),
            author: None,
            settings: ThemeSettings { foreground: Some(color(fg)), ..Default::default() },
            scopes,
        }
    }

    impl State {
        pub fn new() -> State {
            State {
                syntaxes: two_face::syntax::extra_newlines(),
                theme: theme_from_vscode(super::THEME_JSON),
            }
        }

        pub fn tokenize(&self, lang: &str, text: &str) -> Lines {
            let ext = match lang {
                "rust" => "rs",
                other => other,
            };
            let syntax = self.syntaxes.find_syntax_by_extension(ext).unwrap();
            let mut hl = HighlightLines::new(syntax, &self.theme);
            let mut out = Vec::new();
            let base = text.as_ptr() as usize;
            for line in LinesWithEndings::from(text) {
                let mut toks = Vec::new();
                for (style, piece) in hl.highlight_line(line, &self.syntaxes).unwrap() {
                    let start = (piece.as_ptr() as usize - base) as u32;
                    let c = style.foreground;
                    let mut fs = 0;
                    if style.font_style.contains(FontStyle::ITALIC) { fs |= 1; }
                    if style.font_style.contains(FontStyle::BOLD) { fs |= 2; }
                    if style.font_style.contains(FontStyle::UNDERLINE) { fs |= 4; }
                    toks.push(Tok {
                        start,
                        end: start + piece.len() as u32,
                        rgba: (c.r as u32) << 24 | (c.g as u32) << 16 | (c.b as u32) << 8 | c.a as u32,
                        font_style: fs,
                    });
                }
                out.push(toks);
            }
            out
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web {
    use wasm_bindgen::prelude::*;

    /// std::time::Instant panics on wasm32-unknown-unknown, so the JS driver does the timing around
    /// `Hl::tokenize`.
    #[wasm_bindgen]
    pub struct Hl(crate::Engine);

    #[wasm_bindgen]
    impl Hl {
        #[wasm_bindgen(constructor)]
        pub fn new(engine: &str) -> Hl {
            Hl(crate::Engine::new(engine))
        }

        /// Returns the token count.
        pub fn tokenize(&self, lang: &str, text: &str) -> usize {
            self.0.tokenize(lang, text).iter().map(Vec::len).sum()
        }

        pub fn memory_bytes() -> f64 {
            crate::run::peak_memory_bytes() as f64
        }
    }
}
