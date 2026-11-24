//! The components. [`IntxtPage`] is `view(state)`: it takes an [`IntxtView`] and renders it, and
//! nothing below it reads anything but its props, so any host that builds the view can render the
//! same markup. Everything under `Document` is the server's own business (the HTML envelope).

use crate::diff::Diff;
use crate::markdown;
use crate::source::Origin;
use crate::view::CommentView;
use crate::view::CommitView;
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
.intxt-markdown p{margin:.5rem 0}\
.intxt-markdown pre{overflow-x:auto}\
.intxt-page code,.intxt-page pre{font-family:ui-monospace,monospace;font-size:.875em}\
.intxt-commit{margin:.5rem 0}\
.intxt-commit-message{white-space:pre-wrap;margin:.25rem 0}\
.intxt-diff{overflow-x:auto;border:1px solid var(--df-color-border,#ccc);padding:.5rem 0;margin:.5rem 0}\
.intxt-diff span{display:block;padding:0 .75rem;white-space:pre}\
.diff-file{font-weight:600}\
.diff-hunk{background:rgba(127,127,255,.15)}\
.diff-added{background:rgba(46,160,67,.2)}\
.diff-removed{background:rgba(248,81,73,.2)}\
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
                    Markdown { source: description.clone() }
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
            if let Some(description) = &entry.description {
                Markdown { source: description.clone() }
            }
            PrLinks { links: entry.pr_links.clone() }
            for commit in entry.commits.iter().cloned() {
                Commit { commit }
            }
            if let Some(diff) = &entry.patch {
                DiffBlock { diff: diff.clone() }
            }
        }
    }
}

/// Markdown source as HTML. The one place a page writes markup from text it did not write, which
/// `markdown::to_html` has made safe to do.
#[component]
fn Markdown(source: String) -> Element {
    let html = markdown::to_html(&source);
    rsx! {
        div { class: "intxt-markdown", dangerous_inner_html: "{html}" }
    }
}

#[component]
fn Commit(commit: CommitView) -> Element {
    rsx! {
        div { class: "intxt-commit",
            p { class: "intxt-muted text-text-muted",
                code { "{commit.sha.get(..12).unwrap_or(&commit.sha)}" }
                " {commit.author}, {commit.date}"
            }
            p { class: "intxt-commit-message", "{commit.message}" }
        }
    }
}

/// A unified diff as `<pre>` of one `<span>` per line, classed by what the line is. The lines are
/// text nodes, so they are escaped like any other text on the page.
#[component]
fn DiffBlock(diff: Diff) -> Element {
    rsx! {
        pre { class: "intxt-diff",
            for line in diff.lines {
                span { class: line.kind.class(), "{line.text}\n" }
            }
        }
        if diff.omitted_octets > 0 {
            p { class: "intxt-muted text-text-muted",
                "The diff is cut here: {diff.omitted_octets} more octets are not shown."
            }
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

/// `/`: what the viewer is, and where it reads from.
#[component]
pub fn IndexPage(origin: Origin) -> Element {
    rsx! {
        div { class: "intxt-page",
            AppBar { title: "intxt".to_owned() }
            main {
                p {
                    "A read-only viewer of intxts, served at "
                    code { "/i/<drawer id>?via=<node key>" }
                    "."
                }
                match origin {
                    Origin::Fixture { example_path } => rsx! {
                        p {
                            "No node is configured (INTXT_WEB_NODE and its capabilities are unset), so every page is the built-in fixture. Example: "
                            a { href: "{example_path}", "{example_path}" }
                        }
                    },
                    Origin::Node { url } => rsx! {
                        p {
                            "Reading intxts from the node at "
                            code { "{url}" }
                            "."
                        }
                    },
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
