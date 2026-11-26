#![allow(
    unreachable_code,
    reason = "the Routable derive emits code after the catch-all route, which always matches"
)]

//! The viewer as an app: the routes, the server functions that load what a route shows, and the
//! root component. A route component asks a server function for its data and hands the answer to a
//! page component of [`crate::ui`], which is pure rsx over the view model. The first request is
//! rendered on the server with the answers embedded; the client hydrates from them, and from then
//! on a tab or an intxt is a client route change that calls the same server function.

use crate::ui::ErrorPage;
use crate::ui::IndexPage;
use crate::ui::IntxtPage;
use crate::ui::Navigate;
use crate::view::IntxtView;
use crate::view::Origin;
use crate::view::Tab;
use dioxus::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

/// `?via=<node key>|<node key>` is the locator hint of an intxt and rides along on every route of
/// one, as the `via` argument; the drawer id is its identity. A route whose pieces do not parse
/// is still a route: the server function answers it with a 400 page.
#[derive(Debug, Clone, PartialEq, Routable)]
pub enum Route {
    #[layout(Shell)]
    #[route("/")]
    Index {},
    #[route("/i/:drawer?:via")]
    Conversation { drawer: String, via: String },
    #[route("/i/:drawer/commits?:via")]
    Commits { drawer: String, via: String },
    #[route("/i/:drawer/files?:via")]
    Files { drawer: String, via: String },
    #[route("/i/:drawer/sessions?:via")]
    Sessions { drawer: String, via: String },
    #[route("/i/:drawer/guide?:via")]
    Guide { drawer: String, via: String },
    // A router with no route for a path has nothing to render and panics, so this one is required.
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}

/// What a page that could not load says: the HTTP status the server answers the document with, and
/// the sentence shown.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageError {
    pub status: u16,
    pub message: String,
}

/// The intxt in `drawer` read through the nodes `via` names (`a|b`).
#[server]
pub async fn load_intxt(
    drawer: String,
    via: String,
) -> Result<Result<IntxtView, PageError>, ServerFnError> {
    Ok(crate::server::load(&drawer, &via).await)
}

/// Where the viewer reads from, for the index page.
#[server]
pub async fn read_origin() -> Result<Result<Origin, PageError>, ServerFnError> {
    Ok(crate::server::origin().await)
}

/// A server function's answer as a page error when the call itself failed (the network, or a
/// server that is gone).
fn settle<T>(answer: Result<Result<T, PageError>, ServerFnError>) -> Result<T, PageError> {
    answer.unwrap_or_else(|error| {
        Err(PageError {
            status: 502,
            message: format!("the viewer's server did not answer: {error}"),
        })
    })
}

pub fn app() -> Element {
    rsx! { Router::<Route> {} }
}

/// The shell of every route: it gives the links of [`crate::ui`] the router to navigate with.
#[component]
fn Shell() -> Element {
    let navigator = navigator();
    use_hook(|| {
        provide_context(Navigate(Callback::new(move |target: String| {
            navigator.push(target);
        })))
    });
    rsx! { Outlet::<Route> {} }
}

/// The document's title and, on the server, its status, then the page.
///
/// The title is set the way `document::Title` sets it, but without rendering it: that component
/// renders an empty node, which the server's HTML has no counterpart for, so the client's hydration
/// would leave it without a DOM node and the first route change that removes it would fail.
#[component]
fn Titled(title: String, status: u16, children: Element) -> Element {
    dioxus::fullstack::FullstackContext::commit_http_status(
        StatusCode::from_u16(status).expect("a status the viewer chose"),
        None,
    );
    let document = use_hook(dioxus::document::document);
    let shown = use_hook(|| {
        document.set_title(title.clone());
        Rc::new(RefCell::new(title.clone()))
    });
    if title != *shown.borrow() {
        document.set_title(title.clone());
        *shown.borrow_mut() = title;
    }
    children
}

#[component]
fn Index() -> Element {
    let origin = use_server_future(read_origin)?;
    let origin = settle(
        origin
            .read()
            .clone()
            .expect("a resolved resource has a value"),
    );
    match origin {
        Ok(origin) => rsx! {
            Titled { title: "intxt", status: 200, IndexPage { origin } }
        },
        Err(error) => error_page(error),
    }
}

fn error_page(PageError { status, message }: PageError) -> Element {
    rsx! {
        Titled { title: "{status}", status, ErrorPage { status, message } }
    }
}

/// One tab of one intxt. The loader runs again when the drawer or the hint change, and the page
/// is not remounted when only the tab does.
#[component]
fn IntxtRoute(drawer: String, via: String, tab: Tab) -> Element {
    let loaded = use_server_future(use_reactive!(|(drawer, via)| load_intxt(drawer, via)))?;
    let loaded = settle(
        loaded
            .read()
            .clone()
            .expect("a resolved resource has a value"),
    );
    match loaded {
        Ok(view) => rsx! {
            Titled { title: view.title.clone(), status: 200, IntxtPage { view, tab } }
        },
        Err(error) => error_page(error),
    }
}

#[component]
fn Conversation(drawer: String, via: String) -> Element {
    rsx! { IntxtRoute { drawer, via, tab: Tab::Conversation } }
}

#[component]
fn Commits(drawer: String, via: String) -> Element {
    rsx! { IntxtRoute { drawer, via, tab: Tab::Commits } }
}

#[component]
fn Files(drawer: String, via: String) -> Element {
    rsx! { IntxtRoute { drawer, via, tab: Tab::Files } }
}

#[component]
fn Sessions(drawer: String, via: String) -> Element {
    rsx! { IntxtRoute { drawer, via, tab: Tab::Sessions } }
}

#[component]
fn Guide(drawer: String, via: String) -> Element {
    rsx! { IntxtRoute { drawer, via, tab: Tab::Guide } }
}

#[component]
fn NotFound(segments: Vec<String>) -> Element {
    error_page(PageError {
        status: 404,
        message: format!("no page at /{}", segments.join("/")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::FixtureSource;
    use crate::ui::render;
    use crate::ui::render_page;
    use std::str::FromStr;

    const DRAWER: &str = "z11111111111111111111111111111111";

    fn parse(path: &str) -> Option<Route> {
        Route::from_str(path).ok()
    }

    fn answer<T>(answer: Result<Result<T, PageError>, ServerFnError>) -> Result<T, PageError> {
        answer.expect("the server function itself does not fail")
    }

    /// The page of a tab, as the server renders it for the fixture.
    fn fixture_page(tab: Tab) -> String {
        let view = answer(pollster::block_on(load_intxt(
            DRAWER.to_owned(),
            String::new(),
        )))
        .expect("the fixture loads");
        render_page(view, tab)
    }

    #[test]
    fn the_conversation_tab_renders() {
        insta::assert_snapshot!(fixture_page(Tab::Conversation));
    }

    #[test]
    fn the_commits_tab_renders() {
        insta::assert_snapshot!(fixture_page(Tab::Commits));
    }

    #[test]
    fn the_files_tab_renders() {
        insta::assert_snapshot!(fixture_page(Tab::Files));
    }

    #[test]
    fn the_sessions_tab_renders() {
        insta::assert_snapshot!(fixture_page(Tab::Sessions));
    }

    #[test]
    fn the_guide_tab_renders() {
        insta::assert_snapshot!(fixture_page(Tab::Guide));
    }

    #[test]
    fn the_index_and_error_pages_render() {
        let origin = answer(pollster::block_on(read_origin())).expect("the fixture names itself");
        insta::assert_snapshot!(
            "index",
            render(|origin| rsx! { IndexPage { origin } }, origin)
        );
        insta::assert_snapshot!(
            "index_of_a_node",
            render(
                |origin| rsx! { IndexPage { origin } },
                Origin::Node {
                    url: "http://127.0.0.1:8080".to_owned()
                }
            )
        );
        insta::assert_snapshot!(
            "error",
            render(
                |(status, message)| rsx! { ErrorPage { status, message } },
                (404u16, "no page at /nope".to_owned())
            )
        );
    }

    #[test]
    fn a_tab_link_keeps_the_via_hint_and_an_intxt_without_a_guide_says_so() {
        let mut view = FixtureSource::view();
        let via = view.via.join("|");
        let page = render_page(view.clone(), Tab::Guide);
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
        let page = render_page(view, Tab::Guide);
        assert!(
            page.contains("The author left no guide for this intxt."),
            "{page}"
        );
    }

    /// The path a link carries is the route the client navigates to: every tab's path parses to
    /// its own route component, with the hint, and the path the route prints is the one it parsed.
    #[test]
    fn the_paths_the_view_links_to_are_routes() {
        let view = FixtureSource::view();
        let key = view.via.join("|");
        assert_eq!(parse("/"), Some(Route::Index {}));
        for tab in Tab::ALL {
            let path = view.tab_path(tab);
            let drawer = DRAWER.to_owned();
            let via = key.clone();
            let expected = match tab {
                Tab::Conversation => Route::Conversation { drawer, via },
                Tab::Commits => Route::Commits { drawer, via },
                Tab::Files => Route::Files { drawer, via },
                Tab::Sessions => Route::Sessions { drawer, via },
                Tab::Guide => Route::Guide { drawer, via },
            };
            assert_eq!(parse(&path), Some(expected.clone()), "{path}");
            assert_eq!(parse(&expected.to_string()), Some(expected), "{path}");
        }
        assert_eq!(
            parse(&format!("/i/{DRAWER}")),
            Some(Route::Conversation {
                drawer: DRAWER.to_owned(),
                via: String::new()
            })
        );
        assert_eq!(
            parse("/nope/at/all"),
            Some(Route::NotFound {
                segments: vec!["nope".into(), "at".into(), "all".into()]
            })
        );
    }

    #[test]
    fn the_server_functions_serve_the_fixture_and_map_failures_to_statuses() {
        let load = |drawer: &str, via: &str| {
            answer(pollster::block_on(load_intxt(
                drawer.to_owned(),
                via.to_owned(),
            )))
        };
        let view = load(DRAWER, "").expect("the fixture loads");
        assert_eq!(view.drawer, DRAWER);
        assert!(view.title.contains("Make the intxt viewer stateless"));
        assert!(!view.entries.is_empty());
        // A hint the fixture ignores still loads.
        assert!(load(DRAWER, "z6Mkdoesnotmatter").is_err_and(|error| error.status == 400));
        let status = |result: Result<IntxtView, PageError>| result.unwrap_err().status;
        assert_eq!(status(load("z11111111111111111111111111111119", "")), 404);
        assert_eq!(status(load("not-an-id", "")), 400);
        assert_eq!(
            answer(pollster::block_on(read_origin())),
            Ok(Origin::Fixture {
                example_path: format!("/i/{DRAWER}")
            })
        );
    }

    /// What the server answers a page with must survive the trip to the client, which hydrates
    /// from it.
    #[test]
    fn the_view_survives_the_wire() {
        let view = FixtureSource::view();
        let wire = serde_json::to_string(&view).expect("the view serializes");
        let back: IntxtView = serde_json::from_str(&wire).expect("the view deserializes");
        assert_eq!(back, view);
    }
}
