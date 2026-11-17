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
