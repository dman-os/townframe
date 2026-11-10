//! Sanitizer for a guest's stylesheet. A mount's sheet is parsed with `cssparser` and re-emitted
//! rule by rule: everything the renderer can interpret locally is kept, everything that can make
//! the renderer fetch is dropped. Fetching is egress and needs a capability; none exists yet, so
//! there is no allow path, only removal.
use cssparser::AtRuleParser;
use cssparser::CowRcStr;
use cssparser::DeclarationParser;
use cssparser::ParseError;
use cssparser::Parser;
use cssparser::ParserState;
use cssparser::QualifiedRuleParser;
use cssparser::RuleBodyItemParser;
use cssparser::RuleBodyParser;
use cssparser::StyleSheetParser;
use cssparser::Token;

/// Functions whose arguments are resolved to a URL and fetched. `url()` itself is a token of its
/// own (`UnquotedUrl`) or a function named `url` when its argument is quoted. `image()` and
/// `cross-fade()` take string URLs, so they are as much a fetch path as `image-set()`.
const FETCHING_FUNCTIONS: &[&str] = &[
    "url",
    "src",
    "image",
    "image-set",
    "-webkit-image-set",
    "cross-fade",
    "-webkit-cross-fade",
];

/// At-rules kept, because their prelude and body are rules or declarations the renderer
/// interprets locally. Any other at-rule is dropped: `@import`, `@font-face`, `@namespace` fetch
/// or bind external resources, and an unknown at-rule has no reviewed meaning.
const KEPT_AT_RULES: &[&str] = &[
    "media",
    "supports",
    "container",
    "layer",
    "keyframes",
    "-webkit-keyframes",
];

/// Returns `sheet` re-emitted without anything that can make the renderer fetch: `@import`,
/// `@font-face`, `@namespace`, unknown at-rules, and every declaration whose value contains
/// `url()` or one of [`FETCHING_FUNCTIONS`] (custom properties included, since `var()` would
/// carry a URL into another property). Rules that fail to parse are dropped as a browser would.
/// Comments are not kept.
pub(crate) fn sanitize_stylesheet(sheet: &str) -> String {
    let mut parser = Parser::new(sheet);
    let mut sanitizer = Sanitizer { nested: false };
    let rules: Vec<String> = StyleSheetParser::new(&mut parser, &mut sanitizer)
        .filter_map(Result::ok)
        .collect();
    rules.join("\n")
}

/// Wraps `sheet` in `@media <query> { ... }` so a `document::Style { media }` keeps its meaning.
/// The query is checked token by token first: anything that could end the `@media` prelude or
/// its block early (a brace, semicolon, unbalanced bracket, bad string or url, at-keyword) would
/// let the text escape the wrapper, so it is refused rather than escaped. The host sanitizes the
/// result like any other sheet, so this check gives the guest an error; it is not the only guard.
pub fn wrap_in_media_query(query: &str, sheet: &str) -> Result<String, String> {
    fn well_formed(input: &mut Parser<'_>) -> bool {
        while let Ok(token) = input.next() {
            let nested = match token {
                Token::CurlyBracketBlock
                | Token::CloseCurlyBracket
                | Token::CloseParenthesis
                | Token::CloseSquareBracket
                | Token::Semicolon
                | Token::BadString(_)
                | Token::BadUrl(_)
                | Token::UnquotedUrl(_)
                | Token::AtKeyword(_) => return false,
                Token::Function(_) | Token::ParenthesisBlock | Token::SquareBracketBlock => true,
                _ => false,
            };
            if nested
                && !input
                    .parse_nested_block(|inner| Ok::<_, ParseError<()>>(well_formed(inner)))
                    .unwrap_or(false)
            {
                return false;
            }
        }
        true
    }
    let query = query.trim();
    if query.is_empty() || !well_formed(&mut Parser::new(query)) {
        return Err(format!("`{query}` is not a media query"));
    }
    Ok(format!("@media {query} {{\n{sheet}\n}}"))
}

/// One parser for every context: top level (rules only) and rule bodies (`nested`, where
/// declarations and nested rules mix, as in CSS nesting and inside `@media` within a style rule).
struct Sanitizer {
    nested: bool,
}

impl Sanitizer {
    fn body(input: &mut Parser<'_>, nested: bool) -> String {
        let mut inner = Sanitizer { nested };
        RuleBodyParser::new(input, &mut inner)
            .filter_map(Result::ok)
            .collect::<Vec<String>>()
            .join("\n")
    }
}

/// Whether any token of the remaining input, at any nesting depth, can fetch.
fn fetches(input: &mut Parser<'_>) -> bool {
    while let Ok(token) = input.next() {
        let nested = match token {
            Token::UnquotedUrl(_) | Token::BadUrl(_) => return true,
            Token::Function(name) => {
                if FETCHING_FUNCTIONS
                    .iter()
                    .any(|fetching| name.eq_ignore_ascii_case(fetching))
                {
                    return true;
                }
                true
            }
            Token::ParenthesisBlock | Token::SquareBracketBlock | Token::CurlyBracketBlock => true,
            _ => false,
        };
        if nested
            && input
                .parse_nested_block(|inner| Ok::<_, ParseError<()>>(fetches(inner)))
                .unwrap_or(true)
        {
            return true;
        }
    }
    false
}

impl<'i> DeclarationParser<'i> for Sanitizer {
    type Declaration = String;
    type Error = ();

    fn parse_value(
        &mut self,
        name: CowRcStr<'i>,
        input: &mut Parser<'i>,
        _declaration_start: &ParserState,
    ) -> Result<String, ParseError<()>> {
        input.skip_whitespace();
        let start = input.position();
        // `fetches` walks every token through to the end of the value unless it finds a fetch.
        if fetches(input) {
            return Err(ParseError::custom(()));
        }
        let value = input.slice_from(start).trim();
        if value.is_empty() {
            return Err(ParseError::custom(()));
        }
        Ok(format!("{name}: {value};"))
    }
}

impl<'i> AtRuleParser<'i> for Sanitizer {
    /// The at-rule's lowercase name and its prelude text.
    type Prelude = (String, String);
    type AtRule = String;
    type Error = ();

    fn parse_prelude(
        &mut self,
        name: CowRcStr<'i>,
        input: &mut Parser<'i>,
    ) -> Result<Self::Prelude, ParseError<()>> {
        if !KEPT_AT_RULES
            .iter()
            .any(|kept| name.eq_ignore_ascii_case(kept))
        {
            return Err(ParseError::custom(()));
        }
        let start = input.position();
        while input.next_including_whitespace_and_comments().is_ok() {}
        Ok((
            name.to_ascii_lowercase(),
            input.slice_from(start).trim().to_owned(),
        ))
    }

    fn rule_without_block(
        &mut self,
        (name, prelude): Self::Prelude,
        _start: &ParserState,
    ) -> Result<String, ()> {
        // Only `@layer a, b;` is a complete rule without a body.
        if name == "layer" {
            Ok(format!("@layer {prelude};"))
        } else {
            Err(())
        }
    }

    fn parse_block(
        &mut self,
        (name, prelude): Self::Prelude,
        _start: &ParserState,
        input: &mut Parser<'i>,
    ) -> Result<String, ParseError<()>> {
        // Conditional group rules inherit the context they sit in; keyframes bodies are rules
        // whose own bodies are declarations.
        let body = Self::body(input, self.nested);
        Ok(format!("@{name} {prelude} {{\n{body}\n}}"))
    }
}

impl<'i> QualifiedRuleParser<'i> for Sanitizer {
    type Prelude = String;
    type QualifiedRule = String;
    type Error = ();

    fn parse_prelude(&mut self, input: &mut Parser<'i>) -> Result<String, ParseError<()>> {
        let start = input.position();
        while input.next_including_whitespace_and_comments().is_ok() {}
        Ok(input.slice_from(start).trim().to_owned())
    }

    fn parse_block(
        &mut self,
        prelude: String,
        _start: &ParserState,
        input: &mut Parser<'i>,
    ) -> Result<String, ParseError<()>> {
        let body = Self::body(input, true);
        Ok(format!("{prelude} {{\n{body}\n}}"))
    }
}

impl<'i> RuleBodyItemParser<'i, String, ()> for Sanitizer {
    fn parse_declarations(&self) -> bool {
        self.nested
    }

    fn parse_qualified(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
