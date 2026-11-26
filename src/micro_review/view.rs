//! The review micro's three screens, drawn: the pull requests, one pull request, one intxt.
//! [`crate::model`] decides what a row is and says; this file only lays it out, in the styling
//! contract's vocabulary (`docs/design-language/styling-contract.md`): the kit's `Button`,
//! `Alert`, `Chip`, `Card` and `Separator` where they fit, and for the rest a root class
//! (`micro-review`) with `part` and `data-*` names that [`STYLE`] (`review.css`) and any
//! theme key on. No class here but the root, and no inline style.
use crate::model::Changes;
use crate::model::Content;
use crate::model::Entry;
use crate::model::EntryKind;
use crate::model::Intxt;
use crate::model::LinkOutcome;
use crate::model::LinkRow;
use crate::model::Links;
use crate::model::PatchSource;
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
use clay_dioxus_kit::Alert;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Card;
use clay_dioxus_kit::CardContent;
use clay_dioxus_kit::Chip;
use clay_dioxus_kit::ChipVariant;
use clay_dioxus_kit::PanelPage;
use clay_dioxus_kit::Separator;
use clay_dioxus_kit::Status;
use clay_mdk::hooks::Stylesheet;
use dioxus::prelude::*;
use intxt_core::work::PullState;
use pierre_diffs_dioxus::CodeViewFile;
use pierre_diffs_dioxus::CodeViewView;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::PtrEq;
use pierre_diffs_dioxus::Runtime;
use pierre_diffs_dioxus::ScrollMode;
use std::rc::Rc;
use std::time::Duration;

/// The micro's own stylesheet: what the kit has no component or token for (the page's rhythm,
/// text roles, wrapping and the rows that open something). It is the micro's unlayered sheet
/// above the host's kit layers, never the `user` layer, which is the end user's theme.
const STYLE: &str = include_str!("review.css");

/// The micro's root: its stylesheet, then the page. `app_bar` stays put and the body scrolls (a
/// micro's root clips at its pane, `PanelPage`).
#[component]
pub(crate) fn Frame(app_bar: Element, children: Element) -> Element {
    rsx! {
        section { class: "micro-review", "aria-label": "Review",
            Stylesheet { css: STYLE }
            PanelPage { app_bar,
                div { "part": "review-page", {children} }
            }
        }
    }
}

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
        (Screen::Intxt { .. }, Some(Content::Intxt(intxt))) => {
            rsx! { IntxtPage { intxt, node: node.clone() } }
        }
        (screen, Some(content)) => unreachable!("{screen:?} was read as {content:?}"),
    };
    let app_bar = if can_go_back {
        rsx! {
            AppBar {
                title,
                leading: rsx! {
                    Button {
                        variant: ButtonVariant::Ghost,
                        label: "Back".to_owned(),
                        onclick: move |()| stack.with_mut(|stack| {
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
    rsx! {
        Frame { app_bar, {page} }
    }
}

/// A read in flight: the screen is already drawn and fills in when the node answers.
#[component]
fn Loading(what: String) -> Element {
    rsx! {
        p { role: "status", "aria-busy": "true", "part": "review-placeholder",
            "Loading {what}…"
        }
    }
}

/// A failure the reader must see: the kit's danger `Alert`, a heading and the detail. The
/// heading is bold so the failure does not rest on colour.
#[component]
pub(crate) fn Failure(heading: String, detail: String) -> Element {
    rsx! {
        Alert { status: Status::Danger, title: heading,
            span { "part": "review-detail", "{detail}" }
        }
    }
}

/// Why a read failed, where the node could not be reached or refused.
#[component]
pub(crate) fn Failed(error: CallError) -> Element {
    let (heading, detail) = error.parts();
    rsx! { Failure { heading: heading.to_owned(), detail: detail.to_owned() } }
}

/// The state of a pull request as a chip. The words carry the state; the status only reinforces
/// it: open is `success`, merged `info`, an unrecognised state `warning`, and closed has none
/// (a neutral chip).
#[component]
fn StateChip(state: PullState) -> Element {
    let status = match state {
        PullState::Open => Some(Status::Success),
        PullState::Merged => Some(Status::Info),
        PullState::Closed => None,
        PullState::Unknown(_) => Some(Status::Warning),
    };
    rsx! {
        Chip { label: state_label(&state).to_owned(), variant: ChipVariant::Filled, status }
    }
}

/// `owner/repo#number` and the state chip, side by side and wrapping on a narrow screen.
#[component]
fn PullMeta(reference: String, state: PullState) -> Element {
    rsx! {
        div { "part": "review-meta",
            span { "part": "review-secondary", "{reference}" }
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
            div { "part": "review-empty",
                p { "part": "review-name", "No pull requests yet" }
                p { "part": "review-secondary",
                    "They arrive with "
                    code { "part": "review-code", "day x intxt github pull" }
                    "."
                }
            }
        };
    }
    rsx! {
        ul { "part": "review-items", "aria-label": "Pull requests",
            for read in pulls {
                li {
                    {pull_row(read, on_open)}
                    Separator { decorative: true }
                }
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
                    "part": "review-row",
                    "aria-label": "{label}",
                    onclick: {
                        let pull = pull.clone();
                        move |_| on_open.call(Screen::Pull(pull.clone()))
                    },
                    span { "part": "review-name", "{pull.title}" }
                    PullMeta { reference: pull.repo_ref(), state: pull.state.clone() }
                }
            }
        }
        Read::Unreadable(reason) => rsx! {
            Unreadable { what: "Pull request", name: "", reason }
        },
    }
}

/// Something the node holds but this micro cannot show: a card marked as a failure (it is one
/// for the reader), but not an alert, because nothing the reader did is wrong. `name` is what
/// names it, empty when nothing does.
#[component]
fn Unreadable(what: String, name: String, reason: String) -> Element {
    rsx! {
        div {
            role: "group",
            "aria-label": "{what} cannot be read",
            "part": "review-unreadable",
            Card { status: Some(Status::Danger),
                CardContent {
                    if !name.is_empty() {
                        p { "part": "review-id", "{name}" }
                    }
                    p { "part": "review-failure", "{what} cannot be read" }
                    p { "part": "review-secondary", "{reason}" }
                }
            }
        }
    }
}

/// One pull request: its title, `owner/repo#number`, state and link, then the description with
/// the intxt block (machine data) taken out of it.
#[component]
fn PullHeader(pull: Pull) -> Element {
    let reading = pull.reading();
    rsx! {
        header { "part": "review-section",
            h2 { "part": "review-headline", "{pull.title}" }
            PullMeta { reference: pull.repo_ref(), state: pull.state.clone() }
            // Shown as text: the shell has no way to open a link from a micro.
            p { "part": "review-id", "{pull.url}" }
        }
        section { "part": "review-section", "aria-label": "Description",
            for paragraph in reading.paragraphs.iter() {
                p { "part": "review-prose", "{paragraph}" }
            }
            if reading.paragraphs.is_empty() {
                p { "part": "review-secondary", "No description." }
            }
            if let Some(line) = reading.names_line() {
                p { "part": "review-secondary", "{line}" }
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
                Failure { heading: "The intxt block in this description cannot be read", detail: problem }
            };
        }
        Ok(Links::Named(rows)) => rows,
    };
    if rows.is_empty() {
        return rsx! { p { "part": "review-secondary", "This pull request names no intxts." } };
    }
    rsx! {
        section { "part": "review-section", "aria-label": "Intxts",
            h3 { "part": "review-heading", "Intxts" }
            ul {
                "part": part("review-items", &[("variant", Some("spaced"))]),
                "data-variant": "spaced",
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
                "part": part("review-row", &[("variant", Some("outline"))]),
                "data-variant": "outline",
                "aria-label": "Open intxt {title}",
                onclick: {
                    let drawer = row.drawer.clone();
                    let title = title.clone();
                    move |_| on_open.call(Screen::Intxt { drawer: drawer.clone(), title: title.clone() })
                },
                span { "part": "review-name", "{title}" }
                if let Some(description) = description {
                    span { "part": "review-secondary", "{teaser(&description)}" }
                }
                span { "part": "review-secondary", "{entries_label(entries)}" }
            }
        },
        LinkOutcome::NotFound => rsx! {
            div { role: "group", "aria-label": "Not an intxt", "part": "review-unlisted",
                Card {
                    CardContent {
                        p { "part": "review-id", "{name}" }
                        p { "part": "review-secondary", "Not an intxt" }
                    }
                }
            }
        },
        LinkOutcome::Unreadable(reason) => rsx! {
            Unreadable { what: "Intxt", name, reason }
        },
    }
}

/// One intxt: its title and guide, its entries grouped by kind, a commit's patch under it.
#[component]
fn IntxtPage<C: NodeCalls>(intxt: Result<Read<Intxt>, CallError>, node: C) -> Element {
    let intxt = match intxt {
        Ok(Read::Ready(intxt)) => intxt,
        Ok(Read::Unreadable(reason)) => {
            return rsx! { Failure { heading: "This intxt cannot be read", detail: reason } };
        }
        Err(error) => return rsx! { Failed { error } },
    };
    rsx! {
        header { "part": "review-section",
            h2 { "part": "review-headline", "{intxt.title}" }
            if let Some(description) = &intxt.description {
                for paragraph in paragraphs(description) {
                    p { "part": "review-prose", "{paragraph}" }
                }
            }
        }
        if intxt.entries.is_empty() {
            p { "part": "review-secondary", "Nothing in it yet." }
        }
        for group in intxt.groups() {
            section { key: "{group.heading}", "part": "review-group", "aria-label": "{group.heading}",
                h3 { "part": "review-heading", "{group.heading} · {group.entries.len()}" }
                ul { "part": "review-items",
                    for entry in group.entries {
                        li {
                            EntryRow { entry: entry.clone(), node: node.clone() }
                            Separator { decorative: true }
                        }
                    }
                }
            }
        }
    }
}

/// One entry: its short id, and for a commit range or jj change its changes under a heading. An
/// entry that could not be read says why instead of showing less.
#[component]
fn EntryRow<C: NodeCalls>(entry: Entry, node: C) -> Element {
    rsx! {
        div { "part": "review-entry",
            span { "part": "review-id", "{entry.label}" }
            match &entry.kind {
                EntryKind::Unreadable(reason) => rsx! {
                    Alert { status: Status::Danger, "{reason}" }
                },
                EntryKind::Other(tag) => rsx! { span { "part": "review-secondary", "{tag}" } },
                _ => rsx! {},
            }
            match entry.changes {
                Changes::Absent => rsx! {},
                Changes::TooLarge => rsx! {
                    h4 { "part": "review-heading", "Changes" }
                    p { "part": "review-secondary", "No diff is shown: it was too large to carry with this entry." }
                },
                Changes::Blob(source) => rsx! {
                    h4 { "part": "review-heading", "Changes" }
                    PatchLoader { source, node }
                },
            }
        }
    }
}

/// An entry's patch, read from the node only once the reader asks to see it: the diff can be
/// large, and an intxt lists many entries. A failed read is the alert, never an empty diff.
#[component]
fn PatchLoader<C: NodeCalls>(source: PatchSource, node: C) -> Element {
    let mut requested = use_signal(|| false);
    // `requested` is read where the resource is built, so asking restarts it; until then it
    // reads nothing.
    let patch = use_resource(use_reactive((&source, &node), move |(source, node)| {
        let requested = requested();
        async move {
            if requested {
                Some(Review::new(node).patch(&source).await)
            } else {
                None
            }
        }
    }));
    if !requested() {
        return rsx! {
            Button {
                variant: ButtonVariant::Outline,
                onclick: move |()| requested.set(true),
                "Show changes"
            }
        };
    }
    match (*patch.read()).clone() {
        None | Some(None) => rsx! { Loading { what: "changes" } },
        Some(Some(Ok(patch))) => rsx! { Patch { patch, node } },
        Some(Some(Err(error))) => rsx! { Failed { error } },
    }
}

/// The diff's own scroller: a virtualizer needs a bound, and the micro has no page scroller of
/// its own to forward (`ScrollMode::Embedder`).
const DIFF_HEIGHT: &str = "60vh";

/// A git patch in the multi-file code view, view-only: no annotations and no selection yet. The
/// patch is parsed once per mount, every file of it (the view compares each file's diff by
/// identity), so an entry keys it. A patch that does not parse, or has no file in it (an empty
/// diff), is the failure alert, never an empty view.
#[component]
fn Patch<C: NodeCalls>(patch: String, node: C) -> Element {
    let files = use_hook(|| CodeViewFile::<()>::from_patch(&patch).map_err(|error| error.message));
    // The view paces its frames on the node's timer and clock: a micro has neither of its own.
    // Built once, since the view compares the sleep function by identity and a new one would
    // remount it.
    let runtime = use_hook(|| {
        let sleeper = node.clone();
        Runtime {
            sleep: PtrEq::new(Rc::new(move |duration: Duration| {
                let sleeper = sleeper.clone();
                Box::pin(async move { sleeper.sleep(duration).await })
            })),
            now: PtrEq::new(Rc::new(move || node.now().as_secs_f64() * 1000.0)),
        }
    });
    match files {
        Ok(files) => rsx! {
            div { "part": "review-diff",
                CodeViewView::<()> {
                    files,
                    scroll: ScrollMode::Own {
                        height: DIFF_HEIGHT.to_owned(),
                    },
                    runtime,
                    diff_style: DiffStyle::Unified,
                }
            }
        },
        Err(problem) => {
            rsx! { Failure { heading: "The patch cannot be read", detail: problem } }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Fixture;
    use crate::fixture::SAMPLE_PATCH;

    #[derive(Props, Clone, PartialEq)]
    struct Page {
        patch: String,
    }

    /// `Patch` over a fixture node, rendered once.
    fn render(patch: &str) -> String {
        let mut dom = VirtualDom::new_with_props(
            |page: Page| {
                rsx! {
                    Patch { patch: page.patch, node: Fixture::empty() }
                }
            },
            Page {
                patch: patch.to_owned(),
            },
        );
        dom.rebuild_in_place();
        dioxus_ssr::render(&dom)
    }

    #[test]
    fn a_patch_renders_as_a_code_view() {
        let html = render(SAMPLE_PATCH);
        assert!(html.contains("part=\"review-diff\""), "{html}");
        assert!(!html.contains("The patch cannot be read"), "{html}");
        // The code view's own scroller (`ScrollMode::Own`) with its slab inside. The files'
        // rows are not there yet: the viewer mounts them from the scroller's measured
        // geometry, which a first render has none of.
        assert!(
            html.contains("overflow-y:auto;height:60vh;overflow-anchor:none"),
            "{html}"
        );
        assert!(html.contains("position:sticky"), "{html}");
    }

    #[test]
    fn an_empty_patch_is_the_failure_alert_not_an_empty_view() {
        let html = render("");
        assert!(html.contains("The patch cannot be read"), "{html}");
        assert!(!html.contains("part=\"review-diff\""), "{html}");
    }
}
