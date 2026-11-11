//! The Diffs tab: `pierre_diffs_dioxus::FileDiffView` in two views, under one options bar that
//! drives every `FileDiffView` prop the port implements.
//!
//! - Specimens: two sample file pairs, and a generated three-thousand-line one that only renders
//!   the rows near its scroller's viewport. All three take the pointer: hover highlights a line,
//!   a click on a line number selects it, shift-click or a drag extends the range, the gutter
//!   utility button follows the hovered line or the selection, and a collapsed hunk's separator
//!   expands on click.
//! - Mock review: `review_mock::MockReview`, the same component the lab micro renders, so a
//!   difference between the two is the Clay runtime's. Selecting a line or clicking the gutter's
//!   "+" drafts a comment.

use crate::diff_fixtures;
use dioxus::prelude::*;
use pierre_diffs_dioxus::AnnotationSide;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffLineAnnotation;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;
use pierre_diffs_dioxus::Interaction;
use pierre_diffs_dioxus::LineHoverHighlight;
use pierre_diffs_dioxus::PtrEq;
use pierre_diffs_dioxus::SelectedLineRange;
use pierre_diffs_dioxus::Virtualization;
use review_mock::DiffOptions;
use review_mock::MockReview;
use std::rc::Rc;

/// A reviewer's note: the shape of content an embedding plug puts under a diff line.
#[derive(Clone, PartialEq)]
pub struct Note {
    author: &'static str,
    body: &'static str,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DiffsView {
    Specimens,
    MockReview,
}

impl DiffsView {
    const ALL: [Self; 2] = [Self::Specimens, Self::MockReview];

    fn label(self) -> &'static str {
        match self {
            Self::Specimens => "Specimens",
            Self::MockReview => "Mock review",
        }
    }
}

/// The tab: a view choice and the options bar over the chosen view. `DAYFRAME_LAB_DIFFS_VIEW`
/// (`review` or `pr84`) opens that view, so a screenshot run needs no click.
#[component]
pub fn DiffsFoundation() -> Element {
    let mut view = use_signal(
        || match std::env::var("DAYFRAME_LAB_DIFFS_VIEW").as_deref() {
            Ok("review") => DiffsView::MockReview,
                _ => DiffsView::Specimens,
        },
    );
    // The options start where the specimens always ran: hover on both parts, selection and the
    // gutter utility on, unchanged lines expanded.
    let mut options = use_signal(|| DiffOptions {
        line_hover_highlight: LineHoverHighlight::Both,
        ..DiffOptions::default()
    });
    // Not part of `DiffOptions`: only a diff of two full files can expand its unchanged lines
    // (the view panics on a patch that is asked to), so only the specimens take it.
    let mut expand_unchanged = use_signal(|| true);
    // Built once: the props compare the sleep function by identity, so making it again on every
    // render would re-render (and remount) a virtualized diff each time.
    let virtualized = use_hook(|| Virtualization {
        height: "32rem".to_owned(),
        sleep: PtrEq::new(Rc::new(|duration| Box::pin(tokio::time::sleep(duration)))),
    });
    let current = options();
    rsx! {
        div { class: "flex flex-col gap-6",
            Segmented {
                label: "Diff view",
                options: DiffsView::ALL.iter().map(|view| view.label()).collect(),
                selected: DiffsView::ALL.iter().position(|candidate| *candidate == view()).expect("every view is listed"),
                on_select: move |choice: usize| view.set(DiffsView::ALL[choice]),
            }
            div { class: "flex flex-wrap items-center gap-x-6 gap-y-3", role: "group", "aria-label": "Diff options",
                Segmented {
                    label: "Layout",
                    // The default is each view's own: the specimens' mix, the mock review's width
                    // class, a unified diff for the PR.
                    options: vec!["Default", "Split", "Unified"],
                    selected: match current.diff_style {
                        None => 0,
                        Some(DiffStyle::Split) => 1,
                        Some(DiffStyle::Unified) => 2,
                    },
                    on_select: move |choice: usize| {
                        options.write().diff_style = [None, Some(DiffStyle::Split), Some(DiffStyle::Unified)][choice];
                    },
                }
                Segmented {
                    label: "Line hover",
                    options: vec!["Off", "Both", "Number", "Line"],
                    selected: match current.line_hover_highlight {
                        LineHoverHighlight::Disabled => 0,
                        LineHoverHighlight::Both => 1,
                        LineHoverHighlight::Number => 2,
                        LineHoverHighlight::Line => 3,
                    },
                    on_select: move |choice: usize| {
                        options.write().line_hover_highlight = [
                            LineHoverHighlight::Disabled,
                            LineHoverHighlight::Both,
                            LineHoverHighlight::Number,
                            LineHoverHighlight::Line,
                        ][choice];
                    },
                }
                Switch {
                    label: "Line selection",
                    checked: current.enable_line_selection,
                    on_toggle: move |checked| options.write().enable_line_selection = checked,
                }
                Switch {
                    label: "Gutter utility",
                    checked: current.enable_gutter_utility,
                    on_toggle: move |checked| options.write().enable_gutter_utility = checked,
                }
                Switch {
                    label: "Expand unchanged",
                    checked: expand_unchanged() && view() == DiffsView::Specimens,
                    // A patch cannot expand: the specimens are the only views of full files.
                    disabled: view() != DiffsView::Specimens,
                    on_toggle: move |checked| expand_unchanged.set(checked),
                }
            }
            match view() {
                DiffsView::Specimens => rsx! { Specimens { options: current, expand_unchanged: expand_unchanged(), virtualized } },
                DiffsView::MockReview => rsx! {
                    MockReview {
                        options: current,
                        // "Back" at the review's root returns to the specimens.
                        close: move |()| view.set(DiffsView::Specimens),
                    }
                },
            }
        }
    }
}

/// A row of mutually exclusive choices, one pressed: a radio group in the tab bar's visual
/// grammar. Local to the lab until a second context wants it in the kit.
#[component]
fn Segmented(
    label: String,
    options: Vec<&'static str>,
    selected: usize,
    on_select: EventHandler<usize>,
) -> Element {
    rsx! {
        div { class: "flex items-center gap-2",
            span { class: "text-sm text-text-muted", "{label}" }
            div { role: "radiogroup", "aria-label": "{label}", class: "flex flex-wrap gap-1",
                for (index , option) in options.iter().enumerate() {
                    button {
                        key: "{option}",
                        role: "radio",
                        "aria-checked": index == selected,
                        "data-state": if index == selected { "active" } else { "inactive" },
                        class: "min-h-12 cursor-pointer rounded-md border border-border bg-card px-4 font-sans text-foreground-muted hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus data-[state=active]:bg-selected data-[state=active]:text-on-selected",
                        onclick: move |_| on_select.call(index),
                        "{option}"
                    }
                }
            }
        }
    }
}

/// An on/off choice: a button with the switch role, the same grammar as [`Segmented`].
#[component]
fn Switch(
    label: String,
    checked: bool,
    #[props(default)] disabled: bool,
    on_toggle: EventHandler<bool>,
) -> Element {
    rsx! {
        button {
            role: "switch",
            disabled,
            "aria-checked": checked,
            "data-state": if checked { "active" } else { "inactive" },
            class: "min-h-12 cursor-pointer rounded-md border border-border bg-card px-4 font-sans text-foreground-muted disabled:cursor-not-allowed disabled:opacity-50 hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus data-[state=active]:bg-selected data-[state=active]:text-on-selected",
            onclick: move |_| on_toggle.call(!checked),
            "{label}: " { if checked { "on" } else { "off" } }
        }
    }
}

/// Two stacked diffs, a split Rust edit and a unified TypeScript edit that carries one line
/// annotation, then the generated large one. The options bar's expansion applies to all three.
#[component]
fn Specimens(options: DiffOptions, expand_unchanged: bool, virtualized: Virtualization) -> Element {
    let (rust_old, rust_new) = diff_fixtures::rust_pair();
    let (ts_old, ts_new) = diff_fixtures::ts_pair();
    // Built once: the props compare the input by content, so making it again on every render
    // would re-render the diff each time.
    let large = use_hook(|| {
        let (old_file, new_file) = diff_fixtures::large_pair(LARGE_DIFF_BLOCKS);
        DiffInput::Files { old_file, new_file }
    });
    // The latest selection, shown above the diffs so a click is visibly a result and not only a
    // highlight.
    let mut status = use_signal(|| "Click a line number to select a line.".to_owned());
    let interaction = Interaction {
        on_line_selected: Some(Callback::new(move |range: Option<SelectedLineRange>| {
            status.set(match range {
                Some(range) if range.start == range.end => {
                    format!("Selected line {}.", range.start)
                }
                Some(range) => format!("Selected lines {} to {}.", range.start, range.end),
                None => "Selection cleared.".to_owned(),
            });
        })),
        on_gutter_utility_click: options.enable_gutter_utility.then(|| {
            Callback::new(move |range: SelectedLineRange| {
                status.set(format!(
                    "Gutter utility: lines {} to {}.",
                    range.start, range.end
                ));
            })
        }),
        ..options.interaction()
    };
    rsx! {
        div {
            class: "flex flex-col gap-8",
            p { class: "text-sm text-text-muted", "{status}" }
            div {
                class: "overflow-hidden rounded-md border border-border",
                FileDiffView::<Note> {
                    input: DiffInput::Files {
                        old_file: rust_old,
                        new_file: rust_new,
                    },
                    diff_style: options.diff_style.unwrap_or(DiffStyle::Split),
                    expand_unchanged,
                    interaction: interaction.clone(),
                }
            }
            div {
                class: "overflow-hidden rounded-md border border-border",
                FileDiffView::<Note> {
                    input: DiffInput::Files {
                        old_file: ts_old,
                        new_file: ts_new,
                    },
                    diff_style: options.diff_style.unwrap_or(DiffStyle::Unified),
                    expand_unchanged,
                    interaction: interaction.clone(),
                    annotations: vec![DiffLineAnnotation {
                        side: AnnotationSide::Additions,
                        line_number: 41,
                        metadata: Some(Note {
                            author: "Reviewer",
                            body: "Skipping resolved annotations here also hides them from the severity summary below.",
                        }),
                    }],
                    render_annotation: move |annotation: DiffLineAnnotation<Note>| {
                        let note = annotation.metadata.expect("the sample annotation carries a note");
                        rsx! {
                            aside {
                                class: "border-y border-border bg-surface p-3 text-sm text-foreground",
                                strong { class: "mr-2 font-semibold", "{note.author}" }
                                "{note.body}"
                            }
                        }
                    },
                }
            }
            div {
                id: "diffs-large",
                class: "overflow-hidden rounded-md border border-border",
                FileDiffView::<Note> {
                    input: large,
                    diff_style: options.diff_style.unwrap_or(DiffStyle::Unified),
                    expand_unchanged,
                    virtualized: Some(virtualized),
                    interaction,
                }
            }
        }
    }
}

/// 500 six-line functions: about three thousand lines on each side.
const LARGE_DIFF_BLOCKS: usize = 500;

#[cfg(test)]
mod tests {
    use super::*;

    // Dioxus turns a panicking render into an empty subtree, so the page being present at all is
    // the check that both fixtures apply and both layouts render.
    #[test]
    fn both_sample_diffs_render_with_the_annotation() {
        let mut dom = VirtualDom::new(DiffsFoundation);
        dom.rebuild_in_place();
        let page = dioxus_ssr::render(&dom);
        assert!(page.contains("data-diff-type=\"split\""));
        assert!(page.contains("data-diff-type=\"single\""));
        assert!(page.contains("<bdi>src/thumbnails.rs</bdi>"));
        assert!(page.contains("<bdi>review/annotations.ts</bdi>"));
        assert!(page.contains("Skipping resolved annotations"));
        // The generated diff is virtualized: its first rows are there, its far end is not.
        assert!(page.contains("<bdi>src/handlers.rs</bdi>"));
        assert!(page.contains("handler_1("));
        assert!(!page.contains("handler_400("));
    }
}
