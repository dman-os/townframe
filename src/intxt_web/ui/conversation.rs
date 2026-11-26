//! The conversation tab: the feed, newest first, under day dividers. Every item has one anatomy:
//! a word for its kind, the actor, a verb line, the time, then a preview specific to the kind.

use super::At;
use super::Markdown;
use super::RefList;
use super::replies;
use crate::feed::FeedItem;
use crate::feed::FeedKind;
use crate::session::cut;
use crate::view::EntryKind;
use crate::view::EntryView;
use crate::view::IntxtView;
use crate::view::Tab;
use dioxus::prelude::*;

/// The most octets of a comment's body the feed previews.
const COMMENT_PREVIEW_OCTETS: usize = 280;

pub(super) fn conversation(view: &IntxtView) -> Element {
    let items = view.feed();
    if items.is_empty() {
        return rsx! {
            p { class: "intxt-muted", "Nothing has happened in this intxt yet." }
        };
    }
    let mut rows: Vec<Element> = Vec::with_capacity(items.len() + 2);
    let mut day = None::<String>;
    let mut undated_started = false;
    for item in &items {
        match item.at {
            Some(at) => {
                let this_day = at.strftime("%Y-%m-%d").to_string();
                if day.as_deref() != Some(this_day.as_str()) {
                    rows.push(rsx! {
                        h3 { class: "intxt-day", "{this_day}" }
                    });
                    day = Some(this_day);
                }
            }
            None if !undated_started => {
                undated_started = true;
                rows.push(rsx! {
                    h3 { class: "intxt-day", "No time recorded" }
                });
            }
            None => {}
        }
        rows.push(row(view, item));
    }
    rsx! {
        div { class: "intxt-feed",
            for row in rows {
                {row}
            }
        }
    }
}

fn row(view: &IntxtView, item: &FeedItem) -> Element {
    match &item.what {
        FeedKind::Commits(_) => commits_item(item),
        FeedKind::Entry(index) => entry_item(view, item, &view.entries[*index]),
        FeedKind::Thread(index) => thread_item(view, item, *index),
        FeedKind::UnreadableTurns(index) => {
            let entry = &view.entries[*index];
            let unreadable = entry
                .session
                .as_ref()
                .map_or(0, |summary| summary.unreadable);
            rsx! {
                article { class: "intxt-item intxt-gap",
                    p { class: "intxt-item-head",
                        span { class: "intxt-mark", "gap" }
                        " {unreadable} "
                        if unreadable == 1 {
                            "turn"
                        } else {
                            "turns"
                        }
                        " of the session "
                        em { "{entry.label}" }
                        " could not be read and are not counted."
                    }
                }
            }
        }
    }
}

/// The head line of every item but a gap: kind word, actor, verb, time.
fn head(mark: &str, item: &FeedItem, verb: Element) -> Element {
    rsx! {
        p { class: "intxt-item-head",
            span { class: "intxt-mark", "{mark}" }
            " "
            if let Some(actor) = &item.actor {
                strong { "{actor}" }
                " "
            }
            {verb}
            if let Some(at) = item.at {
                " "
                span { class: "intxt-muted",
                    At { at, short: true }
                }
            }
        }
    }
}

fn commits_item(item: &FeedItem) -> Element {
    let FeedKind::Commits(commits) = &item.what else {
        unreachable!("commits_item is given commit items")
    };
    if let [only] = commits.as_slice() {
        return rsx! {
            article { class: "intxt-item",
                {head("commit", item, rsx! {
                    "committed "
                    code { "{short(&only.sha)}" }
                })}
                p { class: "intxt-item-preview", "{only.subject}" }
            }
        };
    }
    let span = match (item.at, item.oldest()) {
        (Some(newest), Some(oldest)) if newest.duration_since(oldest).as_mins() > 0 => {
            format!(" over {} minutes", newest.duration_since(oldest).as_mins())
        }
        _ => " within a minute".to_owned(),
    };
    rsx! {
        article { class: "intxt-item",
            {head("commits", item, rsx! {
                "added {commits.len()} commits{span}"
            })}
            details { class: "intxt-item-preview",
                summary { "{commits[0].subject}, and {commits.len() - 1} more" }
                ul {
                    for commit in commits {
                        li {
                            code { "{short(&commit.sha)}" }
                            " {commit.subject}"
                        }
                    }
                }
            }
        }
    }
}

fn short(sha: &str) -> &str {
    crate::view::short(sha)
}

fn entry_item(view: &IntxtView, item: &FeedItem, entry: &EntryView) -> Element {
    match entry.kind {
        EntryKind::Session => {
            let summary = entry
                .session
                .as_ref()
                .expect("a session entry has its summary");
            let first = summary.prompts().next();
            rsx! {
                article { class: "intxt-item",
                    {head("session", item, rsx! {
                        "ran "
                        em { "{entry.label}" }
                    })}
                    p { class: "intxt-item-preview intxt-muted",
                        "{summary.turns} prompt turns, {summary.tool_calls} tool calls. "
                        a { href: view.tab_path(Tab::Sessions), "The session" }
                    }
                    if let Some(prompt) = first {
                        p { class: "intxt-item-preview", "First prompt: {prompt.headline}" }
                    }
                }
            }
        }
        EntryKind::GithubPull => rsx! {
            article { class: "intxt-item",
                {head("pull request", item, rsx! {
                    "last updated "
                    if let Some(href) = &entry.href {
                        a { href: "{href}", "{entry.label}" }
                    } else {
                        "{entry.label}"
                    }
                })}
                RefList { refs: entry.refs.clone() }
                if let Some(description) = &entry.description {
                    details { class: "intxt-item-preview",
                        summary { "Description" }
                        Markdown { source: description.clone() }
                    }
                }
            }
        },
        EntryKind::GitRef => rsx! {
            article { class: "intxt-item",
                {head("branch", item, rsx! {
                    code { "{entry.label}" }
                })}
                RefList { refs: entry.refs.clone() }
            }
        },
        EntryKind::Generic => rsx! {
            article { class: "intxt-item",
                {head("datum", item, rsx! {
                    "added a "
                    code { "{entry.label}" }
                })}
                RefList { refs: entry.refs.clone() }
            }
        },
        EntryKind::Gap => rsx! {
            article { class: "intxt-item intxt-gap",
                p { class: "intxt-item-head",
                    span { class: "intxt-mark", "gap" }
                    " Could not read "
                    code { "{entry.label}" }
                    ": "
                    for reason in entry.refs.iter() {
                        "{reason.value}"
                    }
                }
            }
        },
        // Ranges and jj changes are shown by their commits.
        EntryKind::Commits | EntryKind::JjChange => rsx! {},
    }
}

fn thread_item(view: &IntxtView, item: &FeedItem, index: usize) -> Element {
    let thread = &view.threads[index];
    let (preview, was_cut) = cut(&thread.root.body, COMMENT_PREVIEW_OCTETS);
    let target = thread.root.targets.first();
    rsx! {
        article { class: "intxt-item", id: "thread-{index}",
            {head("comment", item, rsx! {
                if let Some(target) = target {
                    "commented on "
                    code { "{target.label}" }
                } else {
                    "commented"
                }
            })}
            p { class: "intxt-item-preview intxt-comment-body",
                "{preview}"
                if was_cut {
                    "…"
                }
            }
            p { class: "intxt-muted intxt-small", "{replies(thread.replies.len())}" }
        }
    }
}
