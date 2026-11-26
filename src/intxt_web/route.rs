//! Request path to page: parse the route, load on the server, then render plain SSR.

use crate::source::IntxtSource;
use crate::source::Origin;
use crate::ui::Document;
use crate::ui::ErrorPage;
use crate::ui::IndexPage;
use crate::ui::IntxtPage;
use crate::ui::STYLESHEET_PATH;
use crate::view::IntxtView;
use crate::view::Tab;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use dioxus::prelude::*;

/// The viewer's own stylesheet, served as is. Its colours are the kit's semantic tokens by name
/// (see the file), so no kit bundle is loaded.
const STYLESHEET: &str = include_str!("intxt_web.css");

#[derive(Debug, Clone, PartialEq)]
pub enum Route {
    Index,
    Stylesheet,
    /// `/i/<drawer id>[/commits|/files|/sessions|/guide]?via=<node key>|<node key>`
    Intxt {
        drawer: DocId,
        via: Vec<MultibaseKey>,
        tab: Tab,
    },
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RouteError {
    #[error("no page at {0}")]
    NotFound(String),
    #[error("bad request: {0}")]
    BadRequest(String),
}

impl RouteError {
    pub fn status(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::BadRequest(_) => 400,
        }
    }
}

impl Route {
    pub fn parse(path_with_query: &str) -> Result<Self, RouteError> {
        let (path, query) = path_with_query
            .split_once('?')
            .unwrap_or((path_with_query, ""));
        if path == STYLESHEET_PATH {
            return Ok(Self::Stylesheet);
        }
        let segments: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
        let not_found = || RouteError::NotFound(path_with_query.to_owned());
        let (drawer, tab) = match segments[..] {
            [] => return Ok(Self::Index),
            ["i", drawer] => (drawer, Tab::Conversation),
            ["i", drawer, segment] => (drawer, Tab::from_segment(segment).ok_or_else(not_found)?),
            _ => return Err(not_found()),
        };
        let drawer = drawer
            .parse()
            .map_err(|error| RouteError::BadRequest(format!("drawer id: {error}")))?;
        Ok(Self::Intxt {
            drawer,
            via: parse_via(query)?,
            tab,
        })
    }
}

/// `via=a|b` (the `|` may arrive percent-encoded). Absent means no hint.
fn parse_via(query: &str) -> Result<Vec<MultibaseKey>, RouteError> {
    let Some(hint) = query.split('&').find_map(|pair| pair.strip_prefix("via=")) else {
        return Ok(Vec::new());
    };
    hint.replace("%7C", "|")
        .replace("%7c", "|")
        .split('|')
        .filter(|key| !key.is_empty())
        .map(|key| {
            key.parse()
                .map_err(|error| RouteError::BadRequest(format!("via {key:?}: {error}")))
        })
        .collect()
}

/// A response ready to hand to the HTTP layer.
#[derive(Debug, PartialEq)]
pub struct Page {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

impl Page {
    fn html(status: u16, body: String) -> Self {
        Self {
            status,
            content_type: "text/html; charset=utf-8",
            body,
        }
    }
}

/// Serves a GET: loads whatever the route needs through `source`, then renders. Loading finishes
/// before rendering starts, so a failure is an error page with its own status, never a half page.
pub async fn serve(source: &impl IntxtSource, path_with_query: &str) -> Page {
    match Route::parse(path_with_query) {
        Ok(Route::Index) => Page::html(200, render(index_page, source.origin())),
        Ok(Route::Stylesheet) => Page {
            status: 200,
            content_type: "text/css; charset=utf-8",
            body: STYLESHEET.to_owned(),
        },
        Ok(Route::Intxt { drawer, via, tab }) => match source.load(&drawer, &via).await {
            Ok(view) => Page::html(200, render(intxt_page, (view, tab))),
            Err(error) => error_page_for(error.status(), error.to_string()),
        },
        Err(error) => error_page_for(error.status(), error.to_string()),
    }
}

/// The viewer is read-only: anything but GET is refused.
pub fn method_not_allowed() -> Page {
    error_page_for(405, "this viewer is read-only".to_owned())
}

/// The viewer's own configuration is wrong: every page says so instead of guessing a source.
pub fn misconfigured(message: String) -> Page {
    error_page_for(500, message)
}

fn error_page_for(status: u16, message: String) -> Page {
    Page::html(status, render(error_page, (status, message)))
}

pub(crate) fn intxt_page((view, tab): (IntxtView, Tab)) -> Element {
    rsx! {
        Document { title: view.title.clone(),
            IntxtPage { view, tab }
        }
    }
}

fn index_page(origin: Origin) -> Element {
    rsx! {
        Document { title: "intxt".to_owned(),
            IndexPage { origin }
        }
    }
}

fn error_page((status, message): (u16, String)) -> Element {
    rsx! {
        Document { title: format!("{status}"),
            ErrorPage { status, message }
        }
    }
}

/// Renders a page component once, with no event loop: SSR has no interactivity to run.
pub fn render<P: Clone + 'static>(root: fn(P) -> Element, props: P) -> String {
    let mut dom = VirtualDom::new_with_props(root, props);
    dom.rebuild_in_place();
    let mut out = String::from("<!doctype html><html lang=\"en\">");
    out.push_str(&dioxus_ssr::render(&dom));
    out.push_str("</html>");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::FixtureSource;
    use crate::source::LoadError;

    const DRAWER: &str = "z11111111111111111111111111111111";

    fn key(byte: u8) -> MultibaseKey {
        MultibaseKey::try_from([byte; 32].as_slice()).unwrap()
    }

    #[test]
    fn routes_parse_to_the_page_they_name() {
        assert_eq!(Route::parse("/"), Ok(Route::Index));
        assert_eq!(Route::parse("/assets/intxt_web.css"), Ok(Route::Stylesheet));
        assert_eq!(
            Route::parse(&format!("/i/{DRAWER}")),
            Ok(Route::Intxt {
                drawer: DRAWER.parse().unwrap(),
                via: vec![],
                tab: Tab::Conversation
            })
        );
        for tab in Tab::ALL {
            let segment = tab
                .segment()
                .map_or(String::new(), |segment| format!("/{segment}"));
            assert_eq!(
                Route::parse(&format!("/i/{DRAWER}{segment}?via={}", key(3))),
                Ok(Route::Intxt {
                    drawer: DRAWER.parse().unwrap(),
                    via: vec![key(3)],
                    tab
                })
            );
        }
        assert_eq!(
            Route::parse(&format!("/i/{DRAWER}?via={}|{}", key(3), key(7))),
            Ok(Route::Intxt {
                drawer: DRAWER.parse().unwrap(),
                via: vec![key(3), key(7)],
                tab: Tab::Conversation
            })
        );
        // Clients may percent-encode the separator.
        assert_eq!(
            Route::parse(&format!("/i/{DRAWER}?x=1&via={}%7C{}", key(3), key(7))),
            Route::parse(&format!("/i/{DRAWER}?via={}|{}", key(3), key(7)))
        );
    }

    /// The page `path` is, as the server serves it for the fixture.
    fn fixture_body(path: &str) -> String {
        let page = pollster::block_on(serve(&FixtureSource, path));
        assert_eq!(page.status, 200, "{path}");
        page.body
    }

    #[test]
    fn the_conversation_tab_renders() {
        insta::assert_snapshot!(fixture_body(&format!("/i/{DRAWER}")));
    }

    #[test]
    fn the_commits_tab_renders() {
        insta::assert_snapshot!(fixture_body(&format!("/i/{DRAWER}/commits")));
    }

    #[test]
    fn the_files_tab_renders() {
        insta::assert_snapshot!(fixture_body(&format!("/i/{DRAWER}/files")));
    }

    #[test]
    fn the_sessions_tab_renders() {
        insta::assert_snapshot!(fixture_body(&format!("/i/{DRAWER}/sessions")));
    }

    #[test]
    fn the_guide_tab_renders() {
        insta::assert_snapshot!(fixture_body(&format!("/i/{DRAWER}/guide")));
    }

    #[test]
    fn a_tab_link_keeps_the_via_hint_and_an_intxt_without_a_guide_says_so() {
        let mut view = FixtureSource::view();
        let via = view.via.join("|");
        let page = render(intxt_page, (view.clone(), Tab::Guide));
        for tab in Tab::ALL {
            let segment = tab
                .segment()
                .map_or(String::new(), |segment| format!("/{segment}"));
            assert!(
                page.contains(&format!("href=\"/i/{DRAWER}{segment}?via={via}\"")),
                "{tab:?}: {page}"
            );
        }
        view.guide.clear();
        let page = render(intxt_page, (view, Tab::Guide));
        assert!(
            page.contains("The author left no guide for this intxt."),
            "{page}"
        );
    }

    #[test]
    fn the_index_and_error_pages_render() {
        insta::assert_snapshot!("index", render(index_page, FixtureSource.origin()));
        insta::assert_snapshot!(
            "index_of_a_node",
            render(
                index_page,
                Origin::Node {
                    url: "http://127.0.0.1:8080".to_owned()
                }
            )
        );
        insta::assert_snapshot!(
            "error",
            render(error_page, (404, "no page at /nope".to_owned()))
        );
    }

    #[test]
    fn serve_loads_before_it_renders_and_maps_errors_to_statuses() {
        struct Unreachable;
        impl IntxtSource for Unreachable {
            async fn load(
                &self,
                drawer: &DocId,
                _via: &[MultibaseKey],
            ) -> Result<IntxtView, LoadError> {
                Err(LoadError::Unreachable {
                    drawer: drawer.clone(),
                    reason: "no via node answered".into(),
                })
            }

            fn origin(&self) -> Origin {
                Origin::Node {
                    url: "http://down.example".into(),
                }
            }
        }
        let fixture = |path: &str| pollster::block_on(super::serve(&FixtureSource, path));
        let down = |path: &str| pollster::block_on(super::serve(&Unreachable, path));
        let id = format!("/i/{DRAWER}");
        assert_eq!(fixture(&id).status, 200);
        assert!(
            fixture(&id)
                .body
                .contains("Make the intxt viewer stateless")
        );
        assert_eq!(fixture("/i/z11111111111111111111111111111119").status, 404);
        assert_eq!(fixture("/nope").status, 404);
        assert_eq!(down(&id).status, 502);
        let stylesheet = fixture("/assets/intxt_web.css");
        assert_eq!(stylesheet.content_type, "text/css; charset=utf-8");
        // The page links the stylesheet it is served, which answers both color schemes.
        assert!(fixture(&id).body.contains("href=\"/assets/intxt_web.css\""));
        assert!(stylesheet.body.contains("prefers-color-scheme: dark"));
    }

    #[test]
    fn unknown_and_malformed_paths_are_refused() {
        assert_eq!(Route::parse("/nope").unwrap_err().status(), 404);
        assert_eq!(Route::parse("/i").unwrap_err().status(), 404);
        assert_eq!(
            Route::parse(&format!("/i/{DRAWER}/x"))
                .unwrap_err()
                .status(),
            404
        );
        assert_eq!(Route::parse("/i/not-an-id").unwrap_err().status(), 400);
        assert_eq!(
            Route::parse(&format!("/i/{DRAWER}?via=nope"))
                .unwrap_err()
                .status(),
            400
        );
    }
}
