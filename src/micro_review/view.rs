//! The review micro's three screens, drawn: the pull requests, one pull request, one intxt.
//! [`crate::model`] decides what a row is and says; this file only lays it out, with the
//! semantic roles of the design language (`docs/design-language`): surfaces and text roles, the
//! `danger` role for failures, the hairline border for dividers, 48 px targets for rows that open
//! something.
use crate::model::Content;
use crate::model::Entry;
use crate::model::EntryKind;
use crate::model::Intxt;
use crate::model::LinkOutcome;
use crate::model::LinkRow;
use crate::model::Links;
use crate::model::Pull;
use crate::model::Read;
use crate::model::Review;
use crate::model::Screen;
use crate::model::entries_label;
use crate::model::paragraphs;
use crate::model::state_label;
use crate::model::teaser;
use crate::node::CallError;
use crate::node::NodeCalls;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::PanelPage;
use dioxus::prelude::*;
use intxt_core::work::PullState;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;

/// A row that opens something: a 48 px target, quiet until hovered or focused.
const OPENS: &str = "block min-h-12 w-full cursor-pointer text-left hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus";
/// The words under a title: secondary text.
const SECONDARY: &str = "text-sm text-text-muted break-words";
/// A section's heading.
const HEADING: &str = "text-sm font-semibold text-text-muted";

/// The whole micro in one root-scope render: a stack of screens, the top one read from the node.
pub(crate) fn screens<C: NodeCalls>(node: C) -> Element {
    let mut stack = use_signal(|| vec![Screen::Pulls]);
    let top = stack
        .read()
        .last()
        .cloned()
        .expect("the stack always holds the pull request list");
    // Reads the screen on top. Reading `stack` restarts it when the screen changes, and the
    // node as a dependency restarts it when the node reads something else (new configuration).
    let loaded = use_resource(use_reactive((&node,), move |(node,)| {
        let review = Review::new(node);
        let screen = stack
            .read()
            .last()
            .cloned()
            .expect("the stack always holds the pull request list");
        async move {
            let content = review.open(&screen).await;
            (screen, content)
        }
    }));
    // A resource keeps its last answer while it reads again, so an answer to another screen is
    // no answer to this one.
    let content = (*loaded.read())
        .clone()
        .filter(|(screen, _)| *screen == top)
        .map(|(_, content)| content);

    let open = EventHandler::new(move |screen: Screen| stack.with_mut(|stack| stack.push(screen)));
    let title = match &top {
        Screen::Pulls => "Pull requests".to_owned(),
        Screen::Pull(pull) => pull.repo_ref(),
        Screen::Intxt { title, .. } => title.clone(),
    };
    let can_go_back = stack.read().len() > 1;
    let page = match (&top, content) {
        (Screen::Pull(pull), content) => rsx! {
            PullHeader { pull: pull.clone() }
            match content {
                None => rsx! { Loading { what: "intxts" } },
                Some(Content::Pull(links)) => rsx! { PullIntxts { links, on_open: open } },
                Some(other) => unreachable!("a pull request screen reads its links, not {other:?}"),
            }
        },
        (Screen::Pulls, None) => rsx! { Loading { what: "pull requests" } },
        (Screen::Intxt { .. }, None) => rsx! { Loading { what: "the intxt" } },
        (Screen::Pulls, Some(Content::Pulls(pulls))) => rsx! { PullList { pulls, on_open: open } },
        (Screen::Intxt { .. }, Some(Content::Intxt(intxt))) => rsx! { IntxtPage { intxt } },
        (screen, Some(content)) => unreachable!("{screen:?} was read as {content:?}"),
    };
    let app_bar = if can_go_back {
        rsx! {
            AppBar {
                title,
                leading: rsx! {
                    button {
                        class: "min-h-12 rounded-md px-4 focus-visible:outline",
                        "aria-label": "Back",
                        onclick: move |_| stack.with_mut(|stack| {
                            stack.pop();
                        }),
                        "Back"
                    }
                },
            }
        }
    } else {
        rsx! { AppBar { title } }
    };
    // The bar stays put and the body scrolls: a micro's root clips at its pane (`PanelPage`).
    rsx! {
        section { "aria-label": "Review", height: "100%",
            PanelPage { app_bar,
                div { class: "flex flex-col gap-6 px-4 pb-6", {page} }
            }
        }
    }
}

/// A read in flight: the screen is already drawn and fills in when the node answers.
#[component]
fn Loading(what: String) -> Element {
    rsx! {
        p { role: "status", "aria-busy": "true", class: "py-8 text-center text-text-muted",
            "Loading {what}…"
        }
    }
}

/// A failure the reader must see: a heading and the detail. `danger` is the only failure role,
/// so the border and the words carry it; the heading is bold so it does not rest on colour.
#[component]
pub(crate) fn Alert(heading: String, detail: String) -> Element {
    rsx! {
        div { role: "alert", class: "rounded-md border border-danger px-4 py-3 text-danger",
            p { class: "font-semibold", "{heading}" }
            p { class: "text-sm break-words", "{detail}" }
        }
    }
}

/// Why a read failed, where the node could not be reached or refused.
#[component]
pub(crate) fn Failed(error: CallError) -> Element {
    let (heading, detail) = error.parts();
    rsx! { Alert { heading: heading.to_owned(), detail: detail.to_owned() } }
}

/// The state of a pull request as a chip. The words carry the state; the roles only reinforce
/// it: open is the accent container, merged the selected role, closed the quiet sunken surface.
#[component]
fn StateChip(state: PullState) -> Element {
    let tone = match state {
        PullState::Open => "bg-accent-container text-on-accent-container",
        PullState::Merged => "bg-selected text-on-selected",
        PullState::Closed | PullState::Unknown(_) => {
            "border border-border bg-surface-sunken text-text-muted"
        }
    };
    rsx! {
        span { class: "inline-flex shrink-0 rounded-pill px-2 py-1 text-xs font-semibold {tone}",
            "{state_label(&state)}"
        }
    }
}

/// `owner/repo#number` and the state chip, side by side and wrapping on a narrow screen.
#[component]
fn PullMeta(reference: String, state: PullState) -> Element {
    rsx! {
        div { class: "flex flex-wrap items-center gap-2",
            span { class: SECONDARY, "{reference}" }
            StateChip { state }
        }
    }
}

/// The pull requests the node holds: a divided list, one tappable row each.
#[component]
pub(crate) fn PullList(
    pulls: Result<Vec<Read<Pull>>, CallError>,
    on_open: EventHandler<Screen>,
) -> Element {
    let pulls = match pulls {
        Ok(pulls) => pulls,
        Err(error) => return rsx! { Failed { error } },
    };
    if pulls.is_empty() {
        return rsx! {
            div { class: "flex flex-col gap-2 py-8 text-center",
                p { class: "text-lg font-semibold", "No pull requests yet" }
                p { class: SECONDARY,
                    "They arrive with "
                    code { class: "rounded-sm bg-surface-sunken px-1 text-text", "day x intxt github pull" }
                    "."
                }
            }
        };
    }
    rsx! {
        ul { class: "flex flex-col", "aria-label": "Pull requests",
            for read in pulls {
                li { class: "border-b border-border", {pull_row(read, on_open)} }
            }
        }
    }
}

fn pull_row(read: Read<Pull>, on_open: EventHandler<Screen>) -> Element {
    match read {
        Read::Ready(pull) => {
            let label = format!(
                "Open {}, {}, {}",
                pull.title,
                pull.repo_ref(),
                pull.state_label()
            );
            rsx! {
                button {
                    class: "{OPENS} flex flex-col gap-1 px-2 py-3",
                    "aria-label": "{label}",
                    onclick: {
                        let pull = pull.clone();
                        move |_| on_open.call(Screen::Pull(pull.clone()))
                    },
                    span { class: "font-semibold text-text break-words", "{pull.title}" }
                    PullMeta { reference: pull.repo_ref(), state: pull.state.clone() }
                }
            }
        }
        Read::Unreadable(reason) => rsx! {
            Unreadable { what: "Pull request", name: "", reason }
        },
    }
}

/// Something the node holds but this micro cannot show: bordered like a failure (it is one for
/// the reader), but not an alert, because nothing the reader did is wrong. `name` is what names
/// it, empty when nothing does.
#[component]
fn Unreadable(what: String, name: String, reason: String) -> Element {
    rsx! {
        div {
            role: "group",
            "aria-label": "{what} cannot be read",
            class: "flex flex-col gap-1 rounded-md border border-danger px-4 py-3",
            if !name.is_empty() {
                p { class: "font-semibold text-text break-all", "{name}" }
            }
            p { class: "text-sm font-semibold text-danger", "{what} cannot be read" }
            p { class: SECONDARY, "{reason}" }
        }
    }
}

/// One pull request: its title, `owner/repo#number`, state and link, then the description with
/// the intxt block (machine data) taken out of it.
#[component]
fn PullHeader(pull: Pull) -> Element {
    let reading = pull.reading();
    rsx! {
        header { class: "flex flex-col gap-2",
            h2 { class: "text-lg font-semibold break-words", "{pull.title}" }
            PullMeta { reference: pull.repo_ref(), state: pull.state.clone() }
            // Shown as text: the shell has no way to open a link from a micro.
            p { class: "text-sm text-text-muted break-all", "{pull.url}" }
        }
        section { class: "flex flex-col gap-3", "aria-label": "Description",
            for paragraph in reading.paragraphs.iter() {
                p { class: "whitespace-pre-wrap break-words", "{paragraph}" }
            }
            if reading.paragraphs.is_empty() {
                p { class: SECONDARY, "No description." }
            }
            if let Some(line) = reading.names_line() {
                p { class: SECONDARY, "{line}" }
            }
        }
    }
}

/// The intxts a pull request's description names, each as the node summarized it: openable when
/// found, otherwise said to be not an intxt or unreadable. A block that cannot be read is an
/// error, never an empty list.
#[component]
fn PullIntxts(links: Result<Links, CallError>, on_open: EventHandler<Screen>) -> Element {
    let rows = match links {
        Err(error) => return rsx! { Failed { error } },
        Ok(Links::Malformed(problem)) => {
            return rsx! {
                Alert { heading: "The intxt block in this description cannot be read", detail: problem }
            };
        }
        Ok(Links::Named(rows)) => rows,
    };
    if rows.is_empty() {
        return rsx! { p { class: SECONDARY, "This pull request names no intxts." } };
    }
    rsx! {
        section { class: "flex flex-col gap-2", "aria-label": "Intxts",
            h3 { class: HEADING, "Intxts" }
            ul { class: "flex flex-col gap-2",
                for row in rows {
                    li { {link_row(row, on_open)} }
                }
            }
        }
    }
}

fn link_row(row: LinkRow, on_open: EventHandler<Screen>) -> Element {
    let name = row
        .hinted_title
        .clone()
        .unwrap_or_else(|| row.drawer.to_string());
    match row.outcome {
        LinkOutcome::Found {
            title,
            description,
            entries,
        } => rsx! {
            button {
                class: "{OPENS} flex flex-col gap-1 rounded-md border border-border px-4 py-3",
                "aria-label": "Open intxt {title}",
                onclick: {
                    let drawer = row.drawer.clone();
                    let title = title.clone();
                    move |_| on_open.call(Screen::Intxt { drawer: drawer.clone(), title: title.clone() })
                },
                span { class: "font-semibold text-text break-words", "{title}" }
                if let Some(description) = description {
                    span { class: SECONDARY, "{teaser(&description)}" }
                }
                span { class: SECONDARY, "{entries_label(entries)}" }
            }
        },
        LinkOutcome::NotFound => rsx! {
            div {
                role: "group",
                "aria-label": "Not an intxt",
                class: "flex flex-col gap-1 rounded-md border border-border px-4 py-3 text-text-muted",
                span { class: "break-all", "{name}" }
                span { class: "text-sm", "Not an intxt" }
            }
        },
        LinkOutcome::Unreadable(reason) => rsx! {
            Unreadable { what: "Intxt", name, reason }
        },
    }
}

/// One intxt: its title and guide, its entries grouped by kind, a commit's patch under it.
#[component]
fn IntxtPage(intxt: Result<Read<Intxt>, CallError>) -> Element {
    let intxt = match intxt {
        Ok(Read::Ready(intxt)) => intxt,
        Ok(Read::Unreadable(reason)) => {
            return rsx! { Alert { heading: "This intxt cannot be read", detail: reason } };
        }
        Err(error) => return rsx! { Failed { error } },
    };
    rsx! {
        header { class: "flex flex-col gap-3",
            h2 { class: "text-lg font-semibold break-words", "{intxt.title}" }
            if let Some(description) = &intxt.description {
                for paragraph in paragraphs(description) {
                    p { class: "whitespace-pre-wrap break-words", "{paragraph}" }
                }
            }
        }
        if intxt.entries.is_empty() {
            p { class: SECONDARY, "Nothing in it yet." }
        }
        for group in intxt.groups() {
            section { key: "{group.heading}", class: "flex flex-col", "aria-label": "{group.heading}",
                h3 { class: "{HEADING} pb-1", "{group.heading} · {group.entries.len()}" }
                ul { class: "flex flex-col",
                    for entry in group.entries {
                        li { class: "border-b border-border", EntryRow { entry: entry.clone() } }
                    }
                }
            }
        }
    }
}

/// One entry: its short id, and for a commit range its patch in the diff view under a heading.
/// An entry that could not be read says why instead of showing less.
#[component]
fn EntryRow(entry: Entry) -> Element {
    rsx! {
        div { class: "flex flex-col gap-2 px-2 py-3",
            span { class: "font-semibold break-all", "{entry.label}" }
            match &entry.kind {
                EntryKind::Unreadable(reason) => rsx! {
                    span { role: "alert", class: "text-sm text-danger break-words", "{reason}" }
                },
                EntryKind::Other(tag) => rsx! { span { class: SECONDARY, "{tag}" } },
                _ => rsx! {},
            }
            if let Some(patch) = entry.patch {
                h4 { class: HEADING, "Changes" }
                Patch { patch }
            }
        }
    }
}

/// A unified patch in the diff view, view-only: no annotations and no selection yet. The patch
/// is parsed once per mount (the view compares a parsed diff by identity), so an entry keys it.
#[component]
fn Patch(patch: String) -> Element {
    let input = use_hook(|| DiffInput::from_patch(&patch).map_err(|error| error.to_string()));
    match input {
        Ok(input) => rsx! {
            div { class: "overflow-x-auto rounded-md border border-border",
                FileDiffView::<()> { input, diff_style: DiffStyle::Unified }
            }
        },
        Err(problem) => {
            rsx! { Alert { heading: "The patch cannot be read", detail: problem } }
        }
    }
}
