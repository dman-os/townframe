//! The components. [`IntxtPage`] is `view(state)`: it takes an [`IntxtView`] and renders it, and
//! nothing below it reads anything but its props, so any host that builds the view can render the
//! same markup. Everything under `Document` is the server's own business (the HTML envelope).
//!
//! The page, top to bottom: the header, the description, the tabs and the selected tab's view.
//! Related intxts and discussions sit beside the view when the window is wide and under the
//! header when it is narrow (the stylesheet moves one element). Plain text and rules only: the
//! look is claytxt's (D88) and the vocabulary's Restraint list rules out the rest.

mod changes;
mod conversation;
mod guide;
mod header;
mod sessions;

use crate::markdown;
use crate::view::CommentView;
use crate::view::EntryRef;
use crate::view::IntxtView;
use crate::view::Origin;
use crate::view::PrLinksView;
use crate::view::Tab;
use crate::view::ThreadView;
use dioxus::prelude::*;
use jiff::Timestamp;

/// The sticky top bar: the page's one `h1`.
#[component]
fn TopBar(title: String) -> Element {
    rsx! {
        header { class: "intxt-bar",
            h1 { "{title}" }
        }
    }
}

/// `YYYY-MM-DD HH:MM UTC`: times are UTC, so a page reads the same wherever and whenever it is
/// rendered.
fn when(at: Timestamp) -> String {
    at.strftime("%Y-%m-%d %H:%M UTC").to_string()
}

/// "no replies", "1 reply", "3 replies".
fn replies(count: usize) -> String {
    match count {
        0 => "no replies".to_owned(),
        1 => "1 reply".to_owned(),
        count => format!("{count} replies"),
    }
}

/// A `<time>` element with `at` as its machine-readable value.
#[component]
fn At(at: Timestamp, #[props(default)] short: bool) -> Element {
    let text = if short {
        at.strftime("%H:%M UTC").to_string()
    } else {
        when(at)
    };
    rsx! {
        time { datetime: "{at}", "{text}" }
    }
}

/// One of the intxt's pages. The description is pinned above the tabs.
#[component]
pub fn IntxtPage(view: IntxtView, tab: Tab) -> Element {
    let selected = match tab {
        Tab::Conversation => conversation::conversation(&view),
        Tab::Commits => changes::commits(&view),
        Tab::Files => changes::files(&view),
        Tab::Sessions => sessions::sessions(&view),
        Tab::Guide => guide::guide(&view),
    };
    rsx! {
        div { class: "intxt-page",
            TopBar { title: view.title.clone() }
            div { class: "intxt-layout",
                {header::header(&view)}
                {header::aside(&view)}
                if let Some(description) = &view.description {
                    {description_block(description)}
                }
                div { class: "intxt-view",
                    {header::tabs(&view, tab)}
                    {selected}
                }
            }
        }
    }
}

/// The description above the tabs, folded after a few lines like a pull request's body.
fn description_block(source: &str) -> Element {
    let (head, rest) = markdown::fold(source);
    rsx! {
        section { class: "intxt-description", aria_label: "Description",
            Markdown { source: head.to_owned() }
            if let Some(rest) = rest {
                details {
                    summary { "Show the rest of the description" }
                    Markdown { source: rest.to_owned() }
                }
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
                        NavLink { href: "{link.path}", "{link.label}" }
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

/// A root comment and its replies, set in from the page edge by a rule.
#[component]
fn Thread(thread: ThreadView, id: String) -> Element {
    rsx! {
        article { class: "intxt-thread", id: "{id}",
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
                if let Some(at) = comment.at {
                    " "
                    span { class: "intxt-muted", At { at } }
                }
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
                            NavLink { href: "{example_path}", "{example_path}" }
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
                p { NavLink { href: "/", "intxt viewer" } }
            }
        }
    }
}

/// How the host turns a click on an in-app link into navigation. A host that has a router provides
/// it as context; without one (plain SSR, the review micro) [`NavLink`] is an ordinary anchor.
#[derive(Clone, Copy)]
pub struct Navigate(pub Callback<String>);

/// A link to another page of the viewer. It is an anchor whose `href` always works; when the host
/// provides [`Navigate`] a plain click navigates in place instead of loading the document. A path
/// with a fragment stays a plain anchor: the router does not carry fragments.
#[component]
pub fn NavLink(href: String, #[props(default)] current: bool, children: Element) -> Element {
    let navigate = try_use_context::<Navigate>();
    let in_app = href.starts_with('/') && !href.contains('#');
    rsx! {
        a {
            href: "{href}",
            "aria-current": current.then_some("page"),
            onclick: {
                let href = href.clone();
                move |event: MouseEvent| {
                    if let (Some(navigate), true) = (navigate, in_app) {
                        if event.modifiers().is_empty() {
                            event.prevent_default();
                            navigate.0.call(href.clone());
                        }
                    }
                }
            },
            {children}
        }
    }
}

/// Renders a component once, with no event loop and no router: SSR has no interactivity to run,
/// and a link is then a plain anchor.
#[cfg(test)]
pub(crate) fn render<P: Clone + 'static>(root: fn(P) -> Element, props: P) -> String {
    let mut dom = VirtualDom::new_with_props(root, props);
    dom.rebuild_in_place();
    dioxus_ssr::render(&dom)
}

#[cfg(test)]
pub(crate) fn render_page(view: IntxtView, tab: Tab) -> String {
    render(|(view, tab)| rsx! { IntxtPage { view, tab } }, (view, tab))
}
