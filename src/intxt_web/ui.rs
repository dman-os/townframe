//! The components. [`IntxtPage`] is `view(state)`: it takes an [`IntxtView`] and renders it, and
//! nothing below it reads anything but its props, so any host that builds the view can render the
//! same markup. Everything under `Document` is the server's own business (the HTML envelope).

use crate::view::CommentView;
use crate::view::EntryRef;
use crate::view::EntryView;
use crate::view::IntxtView;
use crate::view::PrLinksView;
use crate::view::ThreadView;
use clay_dioxus_kit::AppBar;
use dioxus::prelude::*;

/// Where the server serves the kit's stylesheet.
pub const STYLESHEET_PATH: &str = "/assets/tailwind.css";

// Layout the kit's compiled stylesheet has no utility for. Colours stay on the kit's tokens.
const PAGE_STYLE: &str = "\
.intxt-page{max-width:48rem;margin:0 auto;padding:0 1rem 3rem}\
.intxt-page ul,.intxt-page ol{padding:0;list-style:none}\
.intxt-page li{padding:.5rem 0}\
.intxt-page h2{font-size:1.125rem;font-weight:600;margin:1.5rem 0 .5rem}\
.intxt-description{white-space:pre-wrap}\
.intxt-page code{font-family:ui-monospace,monospace;font-size:.875em}\
.intxt-muted{font-size:.875rem}\
.intxt-thread{border-left:2px solid var(--df-color-border,#ccc);padding-left:.75rem;margin:1rem 0}\
.intxt-comment{margin:.5rem 0}\
.intxt-comment p{margin:.25rem 0;white-space:pre-wrap}\
.intxt-quote{border-left:2px solid var(--df-color-border,#ccc);margin:.25rem 0;padding-left:.5rem}\
.intxt-page a{color:inherit}";

/// The head and body of a document. The `<html>` element is written by `route::render`, because
/// `html` is also the name of the element module inside rsx.
#[component]
pub fn Document(title: String, children: Element) -> Element {
    rsx! {
        head {
            meta { charset: "utf-8" }
            meta {
                name: "viewport",
                content: "width=device-width, initial-scale=1",
            }
            title { "{title}" }
            link { rel: "stylesheet", href: STYLESHEET_PATH }
            style { dangerous_inner_html: PAGE_STYLE }
        }
        body { class: "bg-canvas text-text", {children} }
    }
}

#[component]
pub fn IntxtPage(view: IntxtView) -> Element {
    rsx! {
        div { class: "intxt-page",
            AppBar { title: view.title.clone() }
            main {
                p { class: "intxt-muted text-text-muted",
                    "drawer "
                    code { "{view.drawer}" }
                }
                if let Some(description) = &view.description {
                    p { class: "intxt-description", "{description}" }
                }
                section { aria_labelledby: "entries",
                    h2 { id: "entries", "Entries" }
                    if view.entries.is_empty() {
                        p { class: "text-text-muted", "Nothing included yet." }
                    }
                    ul {
                        for entry in view.entries.iter().cloned() {
                            EntryRow { entry }
                        }
                    }
                }
                section { aria_labelledby: "comments",
                    h2 { id: "comments", "Comments" }
                    if view.threads.is_empty() {
                        p { class: "text-text-muted", "No comments." }
                    }
                    for thread in view.threads.iter().cloned() {
                        Thread { thread }
                    }
                }
            }
        }
    }
}

#[component]
fn EntryRow(entry: EntryView) -> Element {
    rsx! {
        li {
            strong { "{entry.kind.name()} " }
            if let Some(href) = &entry.href {
                a { href: "{href}", "{entry.label}" }
            } else {
                code { "{entry.label}" }
            }
            RefList { refs: entry.refs.clone() }
            PrLinks { links: entry.pr_links.clone() }
        }
    }
}

#[component]
fn RefList(refs: Vec<EntryRef>) -> Element {
    rsx! {
        if !refs.is_empty() {
            dl { class: "intxt-muted text-text-muted",
                for entry in refs {
                    dt { style: "display:inline", "{entry.name} " }
                    dd { style: "display:inline;margin:0 .75rem 0 0", code { "{entry.value}" } }
                }
            }
        }
    }
}

#[component]
fn PrLinks(links: PrLinksView) -> Element {
    rsx! {
        if !links.intxts.is_empty() {
            p { class: "intxt-muted text-text-muted", "This pull request's intxts" }
            ul {
                for link in links.intxts {
                    li {
                        a { href: "{link.path}", "{link.label}" }
                        for branch in link.refs {
                            " "
                            code { "{branch}" }
                        }
                    }
                }
            }
        }
        if let Some(problem) = &links.malformed {
            p { class: "intxt-muted text-text-muted", "{problem}" }
        }
    }
}

#[component]
fn Thread(thread: ThreadView) -> Element {
    rsx! {
        article { class: "intxt-thread",
            Comment { comment: thread.root }
            for reply in thread.replies {
                Comment { comment: reply }
            }
        }
    }
}

#[component]
fn Comment(comment: CommentView) -> Element {
    rsx! {
        div { class: "intxt-comment",
            p {
                strong { "{comment.author}" }
                if let Some(generator) = &comment.generator {
                    span { class: "text-text-muted", " via {generator}" }
                }
                span { class: "text-text-muted", " ({comment.motivation})" }
            }
            for target in comment.targets {
                p { class: "intxt-muted text-text-muted",
                    "on "
                    code { "{target.label}" }
                }
                if let Some(quote) = &target.quote {
                    blockquote { class: "intxt-quote", code { "{quote}" } }
                }
            }
            p { "{comment.body}" }
        }
    }
}

/// `/`: a pointer at the fixture until the viewer has a real index to show.
#[component]
pub fn IndexPage(fixture_path: String) -> Element {
    rsx! {
        div { class: "intxt-page",
            AppBar { title: "intxt".to_owned() }
            main {
                p {
                    "A read-only viewer of intxts, served at "
                    code { "/i/<drawer id>?via=<node key>" }
                    "."
                }
                p {
                    "Example: "
                    a { href: "{fixture_path}", "{fixture_path}" }
                }
            }
        }
    }
}

#[component]
pub fn ErrorPage(status: u16, message: String) -> Element {
    rsx! {
        div { class: "intxt-page",
            AppBar { title: format!("{status}") }
            main {
                p { "{message}" }
                p { a { href: "/", "intxt viewer" } }
            }
        }
    }
}
