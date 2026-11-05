//! Host-provided stylesheets for guest presentations.
//!
//! A guest emits the vocabulary `clay_surface` allows (classes, not CSS), so a presentation that
//! needs rules (a component library's stylesheet) names them in its package manifest and the host
//! delivers them. There is no shadow DOM to keep one micro's rules from another's, so the host
//! scopes: it wraps the presentation in an element carrying `data-clay-scope`, and the stylesheet
//! goes inside an `@scope` rule rooted at that element. A rule can then only select inside the
//! presentation (`:scope` is the wrapper itself, the place a shadow DOM's `:host` rules land).
use std::rc::Rc;

/// A stylesheet vetted at admission and scoped to the wrapper element of one presentation.
#[derive(Clone)]
pub struct SurfaceStyle {
    scope: Rc<str>,
    scoped: Rc<str>,
}

impl PartialEq for SurfaceStyle {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.scoped, &other.scoped)
    }
}

impl SurfaceStyle {
    pub const MAX_BYTES: usize = 256 * 1024;

    /// What an author-supplied sheet may not contain, because it would let the sheet name
    /// something outside its scope or reach the network: anything that fetches (`url(`, the
    /// image functions, `@import`, `@font-face`), and the at-rules that address the document
    /// rather than the scope (`@namespace`, `@scope`, which would nest a scope of its own).
    const FORBIDDEN: [&'static str; 11] = [
        "@import",
        "@namespace",
        "@font-face",
        "@scope",
        "url(",
        "image(",
        "image-set(",
        "cross-fade(",
        "element(",
        "src(",
        "expression(",
    ];

    /// Vet `stylesheet` and scope it to `scope`, the name the wrapper element carries.
    ///
    /// The vet is deliberately blunt, not a CSS parser: the sheet must have balanced braces
    /// outside comments and strings (so no rule can close the `@scope` block and select
    /// document-wide), no backslash (so no escape can spell a forbidden token), and none of
    /// [`Self::FORBIDDEN`]. A production host replaces it with a parser and a property allowlist.
    pub fn new(scope: &str, stylesheet: &str) -> Result<Self, String> {
        if scope.is_empty()
            || !scope.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
            })
        {
            return Err(format!("scope name {scope:?} is not a plain identifier"));
        }
        if stylesheet.len() > Self::MAX_BYTES {
            return Err(format!("stylesheet exceeds {} bytes", Self::MAX_BYTES));
        }
        if stylesheet.contains('\\') {
            return Err("stylesheet contains a backslash".into());
        }
        let mut code = String::with_capacity(stylesheet.len());
        let mut depth = 0_usize;
        let mut chars = stylesheet.chars().peekable();
        while let Some(character) = chars.next() {
            match character {
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    let mut closed = false;
                    while let Some(inner) = chars.next() {
                        if inner == '*' && chars.peek() == Some(&'/') {
                            chars.next();
                            closed = true;
                            break;
                        }
                    }
                    if !closed {
                        return Err("unterminated comment".into());
                    }
                }
                quote @ ('"' | '\'') => {
                    if !chars.by_ref().any(|inner| inner == quote) {
                        return Err("unterminated string".into());
                    }
                }
                '{' => {
                    depth += 1;
                    code.push(character);
                }
                '}' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or("a rule closes more blocks than it opens")?;
                    code.push(character);
                }
                _ => code.push(character.to_ascii_lowercase()),
            }
        }
        if depth != 0 {
            return Err("unbalanced braces".into());
        }
        if let Some(token) = Self::FORBIDDEN.iter().find(|token| code.contains(**token)) {
            return Err(format!("stylesheet contains {token}"));
        }
        Ok(Self {
            scope: scope.into(),
            scoped: format!("@scope ([data-clay-scope=\"{scope}\"]) {{\n{stylesheet}\n}}").into(),
        })
    }

    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    pub(crate) fn scoped(&self) -> &str {
        &self.scoped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vetted_sheet_is_wrapped_in_a_scope_rule_rooted_at_the_wrapper() {
        let style = SurfaceStyle::new(
            "pierre/rows",
            "@layer a { :scope { color: red } .x { color: blue } }",
        )
        .unwrap();
        assert_eq!(
            style.scoped(),
            "@scope ([data-clay-scope=\"pierre/rows\"]) {\n@layer a { :scope { color: red } .x { color: blue } }\n}"
        );
    }

    #[test]
    fn a_sheet_cannot_close_its_scope_or_reach_outside_it() {
        for hostile in [
            "} body { display: none",
            "a { color: red } } b { color: red }",
            "a { background: url(http://x/y) }",
            "a { background: URL(http://x/y) }",
            "a { background: image-set(\"x.png\" 1x) }",
            "@import \"x.css\";",
            "@font-face { font-family: x }",
            "a { background: \\75rl(x) }",
            "a { color: red",
            "/* open",
            "a { content: \"open }",
            "@scope (body) { a { color: red } }",
        ] {
            assert!(
                SurfaceStyle::new("x", hostile).is_err(),
                "{hostile:?} was admitted"
            );
        }
        // Braces and forbidden words inside comments and strings are not code.
        SurfaceStyle::new("x", "/* } url( */ a { content: \"}{ url(\" }").unwrap();
        assert!(SurfaceStyle::new("x\"] { } a[", "a { }").is_err());
    }
}
