//! The intxt review micro (ledger D62): read a GitHub pull request on the phone and find the
//! intxts it names. Three screens in one presentation, [`micro_review_manifest::VIEW`]: the pull
//! requests the node holds, one pull request with its description and the intxts its description
//! block names, and one intxt with its guide and its entries, a commit entry's patch in the diff
//! view.
//!
//! Everything the screens show is read through [`node::NodeCalls`] in a resource, so a screen
//! renders at once and fills in when the node answers; nothing awaits in mount or dispatch.
//! [`model`] holds what each screen reads and the rules of its rows; this file only draws them.
//! [`clay_mdk::export_micro!`] exports the micro as the `micro-client` guest on wasm32-wasip3 and
//! as the native `ACTIVATE` entry everywhere else.
//!
//! The manifest declares a `layout.panels` row, so the layout lists the micro as a panel. It
//! reads its PRs from the GitHub drawer its install configuration names ([`node::Configuration`]).
#[cfg(any(test, feature = "fixture"))]
pub mod fixture;
pub mod model;
pub mod node;

use clay_dioxus_kit::AppBar;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use clay_mdk::hooks::use_facet;
use dioxus::prelude::*;
use micro_review_manifest::ReviewConfig;
use micro_review_manifest::ReviewConfigDecl;
use micro_review_manifest::ReviewView;
use model::Content;
use model::Entry;
use model::EntryKind;
use model::Intxt;
use model::LinkOutcome;
use model::Links;
use model::Pull;
use model::Read;
use model::Review;
use model::Screen;
use node::CallError;
use node::ContextNode;
use node::NodeCalls;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;

const ROW: &str = "block min-h-12 w-full rounded-md px-4 py-3 text-left text-text hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus";

/// Publish the presentation over the node the micro is configured to read: the real node
/// through the micro's `Context` when the composition granted the configuration entry. With none
/// granted it publishes the "Unconfigured" alert: sample pull requests on a phone would read as
/// real, so the fixture node ([`fixture`], behind the `fixture` feature and tests) is never a
/// fallback here.
async fn activate(context: Context) -> Result<(), Error> {
    match context.config::<ReviewConfigDecl>()? {
        Some(facet) => context.publish::<ReviewView>({
            let context = context.clone();
            move || configured_view(context.clone(), facet.clone())
        })?,
        None => publish_unconfigured(&context)?,
    }
    Ok(())
}

/// No configuration entry was granted. A shipped guest publishes the "Unconfigured" alert. The
/// `fixture` feature, which only the browser e2e's guest is built with (`DAYFRAME_WEB_REVIEW_FIXTURE`
/// in `x/dayframe-web.ts`), publishes the view over [`fixture::Fixture::showcase`] instead, because
/// the web composition has no node to configure the micro against. The node is built once, outside
/// the publication, so every render reads the same node: a fresh one per render would restart the
/// reads forever.
fn publish_unconfigured(context: &Context) -> Result<(), Error> {
    #[cfg(feature = "fixture")]
    {
        let node = fixture::Fixture::showcase();
        context.publish::<ReviewView>(move || view(node.clone()))
    }
    #[cfg(not(feature = "fixture"))]
    context.publish::<ReviewView>(|| {
        rsx! {
            Failed {
                error: CallError::Unconfigured(
                    "grant the micro the org.example.dayframe.review.config entry (githubDrawer, intxtDrawers, docCap)"
                        .to_owned(),
                ),
            }
        }
    })
}

clay_mdk::export_micro!(activate, micro_review_manifest::manifest);

/// [`view`] over the real node. Binding the facet to reactivity makes a configuration entry
/// written, or changed, after mount re-render the view over a node that reads the new one.
fn configured_view(context: Context, facet: Facet<ReviewConfig>) -> Element {
    let snapshot = use_facet(&facet);
    view(ContextNode::new(context, snapshot.state))
}

/// The whole micro in one root-scope render: a stack of screens, the top one read from the node.
fn view<C: NodeCalls>(node: C) -> Element {
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
                None => rsx! { p { role: "status", "Loading intxts…" } },
                Some(Content::Pull(links)) => rsx! { PullLinks { links, on_open: open } },
                Some(other) => unreachable!("a pull request screen reads its links, not {other:?}"),
            }
        },
        (_, None) => rsx! { p { role: "status", "Loading…" } },
        (Screen::Pulls, Some(Content::Pulls(pulls))) => rsx! { PullList { pulls, on_open: open } },
        (Screen::Intxt { .. }, Some(Content::Intxt(intxt))) => rsx! { IntxtPage { intxt } },
        (screen, Some(content)) => unreachable!("{screen:?} was read as {content:?}"),
    };
    rsx! {
        section { class: "flex flex-col", "aria-label": "Review",
            if can_go_back {
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
            } else {
                AppBar { title }
            }
            div { class: "flex flex-col gap-4 px-4 pb-4", {page} }
        }
    }
}

/// Why a read failed, where the node could not be reached or refused.
#[component]
fn Failed(error: CallError) -> Element {
    rsx! { p { role: "alert", class: "text-danger", "{error}" } }
}

/// The pull requests the node holds: title, `owner/repo#number` and state.
#[component]
fn PullList(pulls: Result<Vec<Read<Pull>>, CallError>, on_open: EventHandler<Screen>) -> Element {
    let pulls = match pulls {
        Ok(pulls) => pulls,
        Err(error) => return rsx! { Failed { error } },
    };
    if pulls.is_empty() {
        return rsx! { p { "The node holds no pull requests." } };
    }
    rsx! {
        ul { class: "flex flex-col",
            for read in pulls {
                li {
                    match read {
                        Read::Ready(pull) => rsx! {
                            button {
                                class: ROW,
                                onclick: {
                                    let pull = pull.clone();
                                    move |_| on_open.call(Screen::Pull(pull.clone()))
                                },
                                div { class: "font-semibold", "{pull.title}" }
                                div { class: "text-sm text-text-muted", "{pull.repo_ref()} · {pull.state_label()}" }
                            }
                        },
                        Read::Unreadable(reason) => rsx! {
                            p { role: "alert", class: "px-4 py-3 text-danger", "Unreadable pull request: {reason}" }
                        },
                    }
                }
            }
        }
    }
}

/// One pull request: its title, state and description as text.
#[component]
fn PullHeader(pull: Pull) -> Element {
    rsx! {
        h2 { class: "text-lg font-semibold", "{pull.title}" }
        p { class: "text-sm text-text-muted", "{pull.repo_ref()} · {pull.state_label()}" }
        p { class: "whitespace-pre-wrap break-words", "{pull.body}" }
    }
}

/// The intxts a pull request's description names, each as the node summarized it: openable when
/// found, otherwise said to be not an intxt or unreadable. A block that cannot be read is an
/// error, never an empty list.
#[component]
fn PullLinks(links: Result<Links, CallError>, on_open: EventHandler<Screen>) -> Element {
    let rows = match links {
        Err(error) => return rsx! { Failed { error } },
        Ok(Links::Malformed(problem)) => {
            return rsx! {
                p { role: "alert", class: "text-danger", "The intxt block in this description cannot be read: {problem}" }
            };
        }
        Ok(Links::Named(rows)) => rows,
    };
    if rows.is_empty() {
        return rsx! { p { "This pull request names no intxts." } };
    }
    rsx! {
        h3 { class: "font-semibold", "Intxts" }
        ul { class: "flex flex-col",
            for row in rows {
                li {
                    match row.outcome {
                        LinkOutcome::Found { title, description, entries } => rsx! {
                            button {
                                class: ROW,
                                onclick: {
                                    let drawer = row.drawer.clone();
                                    let title = title.clone();
                                    move |_| on_open.call(Screen::Intxt { drawer: drawer.clone(), title: title.clone() })
                                },
                                div { class: "font-semibold", "{title}" }
                                if let Some(description) = description {
                                    div { class: "text-sm text-text-muted", "{description}" }
                                }
                                div { class: "text-sm text-text-muted", "{entries} entries" }
                            }
                        },
                        LinkOutcome::NotFound => rsx! {
                            UnopenableLink {
                                name: row.hinted_title.unwrap_or_else(|| row.drawer.to_string()),
                                what: "Not an intxt".to_owned(),
                            }
                        },
                        LinkOutcome::Unreadable(reason) => rsx! {
                            UnopenableLink {
                                name: row.hinted_title.unwrap_or_else(|| row.drawer.to_string()),
                                what: format!("Unreadable: {reason}"),
                            }
                        },
                    }
                }
            }
        }
    }
}

/// A link the node could not turn into an intxt: its name (what the block called it, or its
/// drawer) and what became of it.
#[component]
fn UnopenableLink(name: String, what: String) -> Element {
    rsx! {
        div { role: "alert", class: "px-4 py-3",
            div { class: "font-semibold break-all", "{name}" }
            div { class: "text-sm text-danger", "{what}" }
        }
    }
}

/// One intxt: its title, its guide and its entries, a commit's patch under it.
#[component]
fn IntxtPage(intxt: Result<Read<Intxt>, CallError>) -> Element {
    let intxt = match intxt {
        Ok(Read::Ready(intxt)) => intxt,
        Ok(Read::Unreadable(reason)) => {
            return rsx! { p { role: "alert", class: "text-danger", "Unreadable intxt: {reason}" } };
        }
        Err(error) => return rsx! { Failed { error } },
    };
    rsx! {
        h2 { class: "text-lg font-semibold", "{intxt.title}" }
        if let Some(description) = &intxt.description {
            p { class: "whitespace-pre-wrap break-words", "{description}" }
        }
        if intxt.entries.is_empty() {
            p { "Nothing in it yet." }
        }
        ul { class: "flex flex-col gap-4",
            for entry in intxt.entries {
                li { EntryRow { entry } }
            }
        }
    }
}

#[component]
fn EntryRow(entry: Entry) -> Element {
    rsx! {
        div { class: "flex flex-col gap-2",
            div {
                span { class: "text-sm text-text-muted", "{entry.kind.name()} " }
                span { class: "font-semibold break-all", "{entry.label}" }
            }
            if let EntryKind::Unreadable(reason) = &entry.kind {
                p { role: "alert", class: "text-sm text-danger", "{reason}" }
            }
            if let Some(patch) = entry.patch {
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
            div {
                class: "overflow-x-auto rounded-md border border-border",
                role: "region",
                "aria-label": "Patch",
                FileDiffView::<()> { input, diff_style: DiffStyle::Unified }
            }
        },
        Err(problem) => {
            rsx! { p { role: "alert", class: "text-sm text-danger", "The patch cannot be read: {problem}" } }
        }
    }
}
