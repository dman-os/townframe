//! The header, the tabs and the aside: what an intxt is and how to move around it.

use crate::view::IntxtView;
use crate::view::Tab;
use dioxus::prelude::*;

use super::At;

/// Short id, whether work is going on, who took part and where the work lives. Active now is
/// said in words, never marked with a dot or a colour.
pub(super) fn header(view: &IntxtView) -> Element {
    let actors = view.actors();
    let sources = view.sources();
    let last = view.last_activity();
    let working = actors.iter().any(|actor| actor.active_now);
    rsx! {
        section { class: "intxt-header", aria_label: "About this intxt",
            p { class: "intxt-muted intxt-small",
                "intxt "
                code { "{view.drawer}" }
            }
            if working {
                p { "Work is going on now." }
            }
            if let Some(at) = last {
                p { class: "intxt-muted intxt-small",
                    "Last activity "
                    At { at }
                }
            }
            if !actors.is_empty() {
                p { class: "intxt-people",
                    span { class: "intxt-muted", "People " }
                    for (index , actor) in actors.iter().enumerate() {
                        if index > 0 {
                            ", "
                        }
                        span {
                            strong { "{actor.name}" }
                            if actor.active_now {
                                " (active now)"
                            }
                        }
                    }
                }
            }
            if !sources.is_empty() {
                p { class: "intxt-sources",
                    span { class: "intxt-muted", "Sources " }
                    for (index , source) in sources.iter().enumerate() {
                        if index > 0 {
                            ", "
                        }
                        if let Some(href) = &source.href {
                            a { href: "{href}", "{source.label}" }
                        } else {
                            code { "{source.label}" }
                        }
                    }
                }
            }
        }
    }
}

/// The tabs as links, each with its count, keeping `?via=`. The current one is bold and ruled.
pub(super) fn tabs(view: &IntxtView, current: Tab) -> Element {
    rsx! {
        nav { class: "intxt-tabs", aria_label: "Pages of this intxt",
            for tab in Tab::ALL {
                a {
                    href: view.tab_path(tab),
                    "aria-current": (tab == current).then_some("page"),
                    "{tab.title()} "
                    span { class: "intxt-count", "{view.count(tab)}" }
                }
            }
        }
    }
}

/// Related intxts and the discussions: nothing at all when neither has anything to say.
pub(super) fn aside(view: &IntxtView) -> Element {
    let related = view.related();
    if related.is_empty() && view.threads.is_empty() {
        return rsx! {};
    }
    let conversation = view.tab_path(Tab::Conversation);
    rsx! {
        aside { class: "intxt-aside", aria_label: "Related and discussions",
            if !related.is_empty() {
                section {
                    h2 { "Related intxts" }
                    ul {
                        for link in related {
                            li {
                                a { href: "{link.path}", "{link.label}" }
                                " "
                                code { "{link.drawer.get(..8).unwrap_or(&link.drawer)}" }
                            }
                        }
                    }
                }
            }
            if !view.threads.is_empty() {
                section {
                    h2 { "Discussions" }
                    ul {
                        for (index , thread) in view.threads.iter().enumerate() {
                            li {
                                a {
                                    href: "{conversation}#thread-{index}",
                                    if let Some(target) = thread.root.targets.first() {
                                        code { "{target.label}" }
                                    } else {
                                        "a comment"
                                    }
                                }
                                span { class: "intxt-muted intxt-small",
                                    " {thread.root.author}, {super::replies(thread.replies.len())}"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
