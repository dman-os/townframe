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
use crate::view::SessionSummary;
use crate::view::ThreadView;
use dioxus::prelude::*;

/// Where the server serves the viewer's stylesheet (`intxt_web.css`, embedded by `route`).
pub const STYLESHEET_PATH: &str = "/assets/intxt_web.css";

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
        }
        body { {children} }
    }
}

/// The sticky top bar: the page's one `h1`.
#[component]
fn TopBar(title: String) -> Element {
    rsx! {
        header { class: "intxt-bar",
            h1 { "{title}" }
        }
    }
}

#[component]
pub fn IntxtPage(view: IntxtView) -> Element {
    rsx! {
        div { class: "intxt-page",
            TopBar { title: view.title.clone() }
            main {
                p { class: "intxt-muted intxt-small",
                    "drawer "
                    code { "{view.drawer}" }
                }
                if let Some(description) = &view.description {
                    Markdown { source: description.clone() }
                }
                section { aria_labelledby: "entries",
                    h2 { id: "entries", "Entries" }
                    if view.entries.is_empty() {
                        p { class: "intxt-muted", "Nothing included yet." }
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
                        p { class: "intxt-muted", "No comments." }
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
            if let Some(summary) = &entry.session {
                Session { summary: summary.clone() }
            }
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
            p { class: "intxt-muted intxt-small",
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
            p { class: "intxt-muted intxt-small",
                "The diff is cut here: {diff.omitted_octets} more octets are not shown."
            }
        }
    }
}

/// What happened in a session. Prompt text is a text node, so the operator's words are escaped
/// like any other text on the page.
#[component]
fn Session(summary: SessionSummary) -> Element {
    let when = |at: jiff::Timestamp| at.strftime("%Y-%m-%d %H:%M").to_string();
    let tools = summary
        .tools
        .iter()
        .map(|(kind, count)| format!("{kind} {count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (omitted, at_least) = summary.edited_omitted();
    let plus = if at_least { "+" } else { "" };
    rsx! {
        div { class: "intxt-session",
            p { class: "intxt-muted intxt-small",
                "{summary.turns} prompt turns, {summary.tool_calls} tool calls"
                if let Some((first, last)) = summary.span {
                    ", {when(first)} to {when(last)} UTC"
                }
            }
            if !tools.is_empty() {
                p { class: "intxt-muted intxt-small", "tools: {tools}" }
            }
            if summary.edited_files().next().is_some() {
                p { class: "intxt-muted intxt-small", "edited" }
                ul { class: "intxt-session-files",
                    for path in summary.edited_files() {
                        li { code { "{path}" } }
                    }
                }
                if omitted > 0 {
                    p { class: "intxt-muted intxt-small", "and {omitted}{plus} more files" }
                }
            }
            for prompt in summary.prompts() {
                details { class: "intxt-prompt",
                    summary { "{prompt.headline}" }
                    p { class: "intxt-prompt-text", "{prompt.text}" }
                    if prompt.cut {
                        p { class: "intxt-muted intxt-small", "The prompt is cut here." }
                    }
                }
            }
            if summary.prompts_omitted() > 0 {
                p { class: "intxt-muted intxt-small", "and {summary.prompts_omitted()} more prompts" }
            }
        }
    }
}

#[component]
fn RefList(refs: Vec<EntryRef>) -> Element {
    rsx! {
        if !refs.is_empty() {
            dl { class: "intxt-refs intxt-muted intxt-small",
                for entry in refs {
                    dt { "{entry.name} " }
                    dd { code { "{entry.value}" } }
                }
            }
        }
    }
}

#[component]
fn PrLinks(links: PrLinksView) -> Element {
    rsx! {
        if !links.intxts.is_empty() {
            p { class: "intxt-muted intxt-small", "This pull request's intxts" }
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
            p { class: "intxt-muted intxt-small", "{problem}" }
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
                    span { class: "intxt-muted", " via {generator}" }
                }
                span { class: "intxt-muted", " ({comment.motivation})" }
            }
            for target in comment.targets {
                p { class: "intxt-muted intxt-small",
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
            TopBar { title: "intxt".to_owned() }
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
            TopBar { title: format!("{status}") }
            main {
                p { "{message}" }
                p { a { href: "/", "intxt viewer" } }
            }
        }
    }
}
