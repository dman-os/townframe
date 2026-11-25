use super::*;

const HOSTILE: &str = r#"
@charset "utf-8";
@import url("https://evil.example/a.css");
@import "https://evil.example/b.css";
@namespace svg url(http://www.w3.org/2000/svg);
@font-face { font-family: x; src: url(https://evil.example/f.woff2); }
@page { margin: 1cm; }
@unknown-thing foo { a { color: red } }
@property --p { syntax: "<color>"; inherits: false; initial-value: red; }
.kept { color: red; background: URL( "https://evil.example/q.png" ); }
.unquoted { background: url(https://evil.example/u.png) no-repeat; margin: 1px; }
.quoted { background-image: url("https://evil.example/q.png"); padding: 2px; }
.escaped { background: u\72l(https://evil.example/e.png); border: 0; }
.iset { background: image-set("a.png" 1x, "b.png" 2x); top: 0; }
.wiset { background: -webkit-image-set(url(a.png) 1x); left: 0; }
.srcf { content: src("https://evil.example/s.png"); right: 0; }
.imgf { background: image("https://evil.example/i.png"); bottom: 0; }
.xfade { background: cross-fade(url(a.png), url(b.png), 50%); width: 1px; }
.nested-fn { background: linear-gradient(red, blue), calc(1px + url(x.png)); height: 1px; }
.cursor { cursor: url(https://evil.example/c.cur), auto; display: block; }
.mask { mask-image: url(#frag); filter: blur(1px); }
.custom { --leak: url(https://evil.example/c.png); --safe: 3px; color: var(--safe); }
.important { background: url(x.png) !important; color: blue !important; }
.block-in-value { --blk: { background: url(x.png) }; --ok: 1; }
@media (min-width: 10px) { .m { background: url(m.png); color: green; } @font-face { src: url(x) } }
@supports (display: grid) { .s { display: grid; } }
@container (min-width: 5px) { .c { color: red; } }
@layer base, theme;
@layer base { .l { margin: 0; } }
@keyframes spin { from { transform: rotate(0); background: url(k.png); } to { transform: rotate(1turn); } }
:host { display: block; --x: 1; }
:host(.a) .b { color: red; }
.parent { color: red; &:hover { color: blue; background: url(h.png); } @media (width > 1px) { margin: 0; } }
"#;

#[test]
fn hostile_sheet_loses_every_fetch_path_and_keeps_the_rest() {
    let sanitized = sanitize_stylesheet(HOSTILE);
    for needle in [
        "evil.example",
        "url(",
        "URL(",
        "image-set",
        "src(",
        "@import",
        "@font-face",
        "@namespace",
        "@page",
        "@unknown",
        "@property",
        "cross-fade",
        "image(",
        "u\\72l",
        "frag",
        ".png",
        "c.cur",
    ] {
        assert!(
            !sanitized.contains(needle),
            "{needle:?} survived sanitizing:\n{sanitized}"
        );
    }
    insta::assert_snapshot!(sanitized);
}

#[test]
fn pierre_stylesheet_is_kept_but_for_fetching_declarations() {
    let source = include_str!("fixtures/pierre_style.css");
    insta::assert_snapshot!(sanitize_stylesheet(source));
}

#[test]
fn an_inert_sheet_sanitizes_to_itself_up_to_whitespace() {
    let sheet = ":host { display: block; }\n@media (min-width: 1px) {\n.a { color: red; }\n}";
    let sanitized = sanitize_stylesheet(sheet);
    let squash = |text: &str| text.split_whitespace().collect::<String>();
    assert_eq!(squash(&sanitized), squash(sheet));
}

#[test]
fn a_theme_with_any_fetch_path_is_refused_not_repaired() {
    for fetching in [
        "@import url(https://evil.example/a.css);",
        "@import \"https://evil.example/b.css\";",
        "@font-face { font-family: x; src: local(x); }",
        "@namespace svg \"http://www.w3.org/2000/svg\";",
        ".a { background: url(x.png); }",
        ".a { background: URL( \"x.png\" ); }",
        ".a { background: u\\72l(x.png); }",
        ".a { background: image-set(\"a.png\" 1x); }",
        ".a { background: -webkit-image-set(\"a.png\" 1x); }",
        ".a { content: src(\"x\"); }",
        ".a { background: cross-fade(red, blue, 50%); }",
        ".a { --leak: url(x.png); }",
        "@media (min-width: 1px) { .a { cursor: url(c.cur), auto; } }",
        ".a { &:hover { mask-image: url(#m); } }",
        "@media (min-width: 1px) { @font-face { src: url(x); } }",
        "@unknown-thing foo { a { color: red } }",
    ] {
        assert!(
            refuse_fetching_stylesheet(fetching).is_err(),
            "{fetching:?} was accepted"
        );
    }
    assert!(refuse_fetching_stylesheet(HOSTILE).is_err());
}

#[test]
fn an_inert_theme_is_accepted_whole() {
    // Text that merely mentions the words is not a fetch: strings and comments are inert.
    let theme = r#"
/* a comment about url(x) and @import */
@layer base, theme;
:root, .df-theme { --color-primary: #6750a4; --label: "url(x)"; }
@media (prefers-color-scheme: dark) { :root { --color-primary: #d0bcff; } }
@supports (display: grid) { .card { display: grid; background: linear-gradient(red, blue); } }
@keyframes pulse { from { opacity: 0.5; } to { opacity: 1; } }
.btn { color: var(--color-primary); &:hover { color: color-mix(in srgb, red 20%, blue); } }
"#;
    assert_eq!(refuse_fetching_stylesheet(theme), Ok(()));
}
