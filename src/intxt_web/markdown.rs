//! Descriptions as HTML. Markdown is the one place the page writes markup from text it did not
//! write, so two things are closed before it reaches the writer: raw HTML in the source becomes
//! text (which the writer escapes), and a link or image whose destination is not an `http(s)`,
//! `mailto:` or relative URL (`javascript:`, `data:`) loses its destination. A description can
//! format itself but cannot add an element or run a script.

use pulldown_cmark::CowStr;
use pulldown_cmark::Event;
use pulldown_cmark::Options;
use pulldown_cmark::Parser;
use pulldown_cmark::Tag;

pub fn to_html(source: &str) -> String {
    let events = Parser::new_ext(
        source,
        Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TABLES,
    )
    .map(|event| match event {
        Event::Html(raw) | Event::InlineHtml(raw) => Event::Text(raw),
        Event::Start(Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Link {
            link_type,
            dest_url: safe(dest_url),
            title,
            id,
        }),
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Image {
            link_type,
            dest_url: safe(dest_url),
            title,
            id,
        }),
        other => other,
    });
    let mut html = String::with_capacity(source.len() * 3 / 2);
    pulldown_cmark::html::push_html(&mut html, events);
    html
}

/// The destination when it is a web, mail or relative URL; `#` otherwise. A scheme is what
/// precedes the first `:` when no `/`, `?` or `#` comes before it.
fn safe(destination: CowStr<'_>) -> CowStr<'_> {
    let scheme = destination
        .split_once(':')
        .map(|(scheme, _)| scheme)
        .filter(|scheme| !scheme.contains(['/', '?', '#']));
    match scheme {
        None => destination,
        Some(scheme)
            if ["http", "https", "mailto"]
                .iter()
                .any(|allowed| scheme.eq_ignore_ascii_case(allowed)) =>
        {
            destination
        }
        Some(_) => CowStr::Borrowed("#"),
    }
}

#[cfg(test)]
mod tests {
    use super::to_html;

    #[test]
    fn markdown_formats() {
        assert_eq!(
            to_html("one **two**\n\n- three\n"),
            "<p>one <strong>two</strong></p>\n<ul>\n<li>three</li>\n</ul>\n"
        );
    }

    #[test]
    fn raw_html_is_text_not_markup() {
        let html = to_html("<script>alert(1)</script>\n\nhi <img src=x onerror=y> there\n");
        assert!(!html.contains("<script"), "{html}");
        assert!(!html.contains("<img"), "{html}");
        assert!(html.contains("&lt;script&gt;"), "{html}");
        assert!(html.contains("&lt;img src=x onerror=y&gt;"), "{html}");
    }

    #[test]
    fn only_web_mail_and_relative_destinations_survive() {
        let html = to_html(
            "[a](https://x.example/p) [b](mailto:a@x) [c](/i/z1?via=k) [d](javascript:alert(1)) \
             [e](JaVaScRiPt:alert(1)) [f](data:text/html,x) ![g](vbscript:x)",
        );
        assert!(html.contains("href=\"https://x.example/p\""), "{html}");
        assert!(html.contains("href=\"mailto:a@x\""), "{html}");
        assert!(html.contains("href=\"/i/z1?via=k\""), "{html}");
        assert!(!html.to_lowercase().contains("javascript"), "{html}");
        assert!(!html.contains("data:"), "{html}");
        assert!(!html.contains("vbscript"), "{html}");
    }
}
