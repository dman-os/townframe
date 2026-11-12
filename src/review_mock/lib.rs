//! Experiment: the review loop's first four journeys over mock data (the intxt review loop,
//! J1-J5). A list of changes, one change's overview, one file as a diff with annotation threads
//! inline, a thread, and new annotations drafted under a selection. The data starts as a constant
//! and is mutable for the micro's lifetime (a signal of threads: replies, resolve and reopen,
//! new threads); the intext lane replaces this experiment with the real micro over datums.
//!
//! The diff is `pierre_diffs_dioxus::FileDiffView` used as any Dioxus app would use it: the
//! threads are its `render_annotation` content, and nothing in this crate knows Clay's guest
//! surface exists. Two embedders render [`MockReview`]: the lab micro (a Clay guest) and the theme
//! lab's Diffs tab (a plain Dioxus desktop app), so a difference between the two is the runtime's.
mod data;

use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::WidthClass;
use data::CHANGES;
use data::Comment;
use std::borrow::Cow;

use dioxus::prelude::*;
use pierre_diffs_dioxus::AnnotationSide;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffLineAnnotation;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;
use pierre_diffs_dioxus::Interaction;
use pierre_diffs_dioxus::LineHoverHighlight;
use pierre_diffs_dioxus::SelectedLineRange;

/// The `FileDiffView` options an embedder drives. The default is what the lab micro runs: the
/// layout follows the width and a line or the gutter's "+" opens a draft. There is no
/// `expand_unchanged`: the mock files are patches, and the view cannot expand a patch's collapsed
/// lines without the full file contents.
#[derive(Clone, Copy, PartialEq)]
pub struct DiffOptions {
    /// `None` derives the layout from the width class: split when two panes fit.
    pub diff_style: Option<DiffStyle>,
    pub line_hover_highlight: LineHoverHighlight,
    /// Clicking a line number selects it, and a selection opens a draft annotation.
    pub enable_line_selection: bool,
    /// The gutter's "+" opens a draft annotation.
    pub enable_gutter_utility: bool,
}

impl DiffOptions {
    /// The view's `Interaction` for these options, without callbacks: an embedder that listens
    /// to selections spreads this under its own.
    pub fn interaction(&self) -> Interaction {
        Interaction {
            line_hover_highlight: self.line_hover_highlight,
            enable_line_selection: self.enable_line_selection,
            enable_gutter_utility: self.enable_gutter_utility,
            ..Interaction::default()
        }
    }
}

impl Default for DiffOptions {
    fn default() -> Self {
        Self {
            diff_style: None,
            line_hover_highlight: LineHoverHighlight::Disabled,
            enable_line_selection: true,
            enable_gutter_utility: true,
        }
    }
}

/// A thread while the micro runs: a root comment and its replies anchored at a line range of one
/// side of the diff, with the state the reviewer toggles.
#[derive(Clone, PartialEq)]
struct Thread {
    side: AnnotationSide,
    /// The range's first and last line, in `side`'s numbering; equal for one line.
    start: usize,
    end: usize,
    resolved: bool,
    comments: Vec<Comment>,
}

impl Thread {
    /// "line 6" or "lines 4-7": where the thread hangs, for headers and accessible names.
    fn range_label(&self) -> String {
        range_label(self.start, self.end)
    }
}

fn range_label(start: usize, end: usize) -> String {
    if start == end {
        format!("line {start}")
    } else {
        format!("lines {start}-{end}")
    }
}

/// Every change's threads, by change and file: the micro's mutable state, shared through
/// context. The reviewer is the only author of anything added here.
type Reviews = Signal<Vec<Vec<Vec<Thread>>>>;

/// The author of every comment the reviewer writes.
const REVIEWER: &str = "Reviewer";

/// What a line annotation of the file diff holds.
#[derive(Clone, PartialEq)]
enum Row {
    /// The thread at this index of the file's threads.
    Thread(usize),
    /// The draft of a new annotation, under the selection's last line.
    Draft,
}

/// A new annotation being written: the range the reviewer selected.
#[derive(Clone, PartialEq)]
struct Draft {
    side: AnnotationSide,
    start: usize,
    end: usize,
}

impl Draft {
    /// The draft a selection opens. A drag upwards reports its end before its start, and the
    /// range lives on the side the selection ended on.
    fn of(range: &SelectedLineRange) -> Self {
        Self {
            side: range
                .end_side
                .or(range.side)
                .unwrap_or(AnnotationSide::Additions),
            start: range.start.min(range.end),
            end: range.start.max(range.end),
        }
    }
}

/// Where the reviewer is: each variant is one journey, holding the indices it needs.
#[derive(Clone, Copy, PartialEq)]
enum Screen {
    /// J1.
    Changes,
    /// J2.
    Change(usize),
    /// J3.
    File { change: usize, file: usize },
    /// J4.
    Thread {
        change: usize,
        file: usize,
        thread: usize,
    },
}

impl Screen {
    /// Where "back" goes; `None` at the root, where back leaves the experiment.
    fn parent(self) -> Option<Self> {
        match self {
            Self::Changes => None,
            Self::Change(_) => Some(Self::Changes),
            Self::File { change, .. } => Some(Self::Change(change)),
            Self::Thread { change, file, .. } => Some(Self::File { change, file }),
        }
    }
}

/// The unified `+`/`-` line counts of a patch, header lines excluded.
fn counts(patch: &str) -> (usize, usize) {
    patch.lines().fold((0, 0), |(added, removed), line| {
        if line.starts_with("+++") || line.starts_with("---") {
            (added, removed)
        } else if line.starts_with('+') {
            (added + 1, removed)
        } else if line.starts_with('-') {
            (added, removed + 1)
        } else {
            (added, removed)
        }
    })
}

#[component]
pub fn MockReview(close: Callback<()>, #[props(default)] options: DiffOptions) -> Element {
    let mut screen = use_signal(|| Screen::Changes);
    use_context_provider::<Reviews>(|| {
        Signal::new(
            CHANGES
                .iter()
                .map(|change| {
                    change
                        .files
                        .iter()
                        .map(|file| {
                            file.threads
                                .iter()
                                .map(|seed| Thread {
                                    side: AnnotationSide::Additions,
                                    start: seed.line,
                                    end: seed.line,
                                    resolved: seed.resolved,
                                    comments: seed.comments.to_vec(),
                                })
                                .collect()
                        })
                        .collect()
                })
                .collect(),
        )
    });
    // The host's width class is not yet handed to a micro (C1, parked), so the experiment owns a
    // stand-in the reviewer flips: the diff layout is derived from it exactly as it will be from
    // the host's.
    let mut width = use_signal(|| WidthClass::Expanded);
    let current = screen();
    let title = match current {
        Screen::Changes => "Mock review".to_owned(),
        Screen::Change(change) => CHANGES[change].title.to_owned(),
        Screen::File { change, file } => CHANGES[change].files[file].path.to_owned(),
        Screen::Thread { .. } => "Thread".to_owned(),
    };
    let wide = width().fits_side_by_side();
    rsx! {
        div { class: "flex flex-col gap-4",
            AppBar {
                title,
                leading: rsx! {
                    button {
                        class: "min-h-12 rounded-md px-4 focus-visible:outline",
                        "aria-label": "Back",
                        onclick: move |_| match current.parent() {
                            Some(parent) => screen.set(parent),
                            None => close(()),
                        },
                        "Back"
                    }
                },
                trailing: rsx! {
                    button {
                        class: "min-h-12 rounded-md px-4 focus-visible:outline",
                        "aria-label": "Switch width",
                        "aria-pressed": "{wide}",
                        onclick: move |_| {
                            width.set(if wide { WidthClass::Compact } else { WidthClass::Expanded });
                        },
                        if wide { "Wide" } else { "Compact" }
                    }
                },
            }
            match current {
                Screen::Changes => rsx! { ChangeList { screen } },
                Screen::Change(change) => rsx! { Overview { change, screen } },
                Screen::File { change, file } => rsx! {
                    // Keyed by file so each opened file owns its parsed patch.
                    FileDiff { key: "{change}-{file}", change, file, width: width(), options, screen }
                },
                Screen::Thread { change, file, thread } => rsx! { ThreadView { change, file, thread } },
            }
        }
    }
}

/// J1: the changes waiting for review.
#[component]
fn ChangeList(screen: Signal<Screen>) -> Element {
    let reviews = use_context::<Reviews>();
    rsx! {
        ul { class: "mx-4 flex flex-col gap-2", "aria-label": "Changes",
            for (index , change) in CHANGES.iter().enumerate() {
                li { key: "{index}",
                    button {
                        class: "block min-h-12 w-full rounded-md border border-border px-4 py-3 text-left focus-visible:outline",
                        "aria-label": "Open change {change.title}",
                        onclick: move |_| screen.set(Screen::Change(index)),
                        strong { "{change.title}" }
                        p { "{change.files.len()} files, {open_threads(&reviews.read()[index])} open threads" }
                    }
                }
            }
        }
    }
}

/// The unresolved threads of one change's files.
fn open_threads(files: &[Vec<Thread>]) -> usize {
    files
        .iter()
        .flatten()
        .filter(|thread| !thread.resolved)
        .count()
}

/// J2: one change, its summary and the files it touches.
#[component]
fn Overview(change: usize, screen: Signal<Screen>) -> Element {
    let change_data = &CHANGES[change];
    rsx! {
        div { class: "mx-4 flex flex-col gap-4",
            p { "{change_data.summary}" }
            ul { class: "flex flex-col gap-2", "aria-label": "Files",
                for (file , data) in change_data.files.iter().enumerate() {
                    {
                        let (added, removed) = counts(data.patch);
                        rsx! {
                            li { key: "{data.path}",
                                button {
                                    class: "flex min-h-12 w-full items-center justify-between gap-4 rounded-md border border-border px-4 py-3 text-left focus-visible:outline",
                                    "aria-label": "Open file {data.path}",
                                    onclick: move |_| screen.set(Screen::File { change, file }),
                                    span { "{data.path}" }
                                    span { "+{added} -{removed}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// J3 and J5: a file as a diff, unified when the width does not fit two panes, with the file's
/// threads under their lines. Selecting a line or a range (or the gutter's "+") opens a draft
/// annotation row under the selection's last line.
#[component]
fn FileDiff(
    change: usize,
    file: usize,
    width: WidthClass,
    options: DiffOptions,
    screen: Signal<Screen>,
) -> Element {
    let mut reviews = use_context::<Reviews>();
    let mut draft = use_signal(|| None::<Draft>);
    // The diff's selection, held here so a submitted or cancelled draft clears its highlight:
    // the view applies `selected_lines` only when it changes (see `Interaction::selected_lines`).
    let mut selection = use_signal(|| None::<SelectedLineRange>);
    let data = &CHANGES[change].files[file];
    // The input is compared by identity (see `DiffInput::Parsed`), so it is parsed once.
    let input = use_hook(|| DiffInput::from_patch(data.patch).expect("the mock patches parse"));
    let mut annotations: Vec<DiffLineAnnotation<Row>> = reviews.read()[change][file]
        .iter()
        .enumerate()
        .map(|(thread, data)| DiffLineAnnotation {
            side: data.side,
            line_number: data.end,
            metadata: Some(Row::Thread(thread)),
        })
        .collect();
    if let Some(draft) = draft.read().as_ref() {
        annotations.push(DiffLineAnnotation {
            side: draft.side,
            line_number: draft.end,
            metadata: Some(Row::Draft),
        });
    }
    // A selection and the gutter's "+" both open the draft; a selection that ends up cleared
    // leaves a draft already being written alone, and only its Cancel removes it.
    let open_draft = Callback::new(move |range: SelectedLineRange| {
        draft.set(Some(Draft::of(&range)));
    });
    rsx! {
        div { class: "overflow-hidden rounded-md border border-border",
            FileDiffView::<Row> {
                input,
                diff_style: options
                    .diff_style
                    .unwrap_or(if width.fits_side_by_side() { DiffStyle::Split } else { DiffStyle::Unified }),
                annotations,
                interaction: Interaction {
                    selected_lines: selection(),
                    on_line_selected: Some(Callback::new(move |range: Option<SelectedLineRange>| {
                        selection.set(range.clone());
                        if let Some(range) = range {
                            open_draft(range);
                        }
                    })),
                    on_gutter_utility_click: options.enable_gutter_utility.then_some(open_draft),
                    ..options.interaction()
                },
                render_annotation: move |annotation: DiffLineAnnotation<Row>| {
                    match annotation.metadata.expect("every mock annotation carries its row") {
                        Row::Thread(thread) => rsx! {
                            ThreadRow { change, file, thread, screen }
                        },
                        Row::Draft => {
                            let current = draft.read().clone().expect("a draft row exists only while a draft does");
                            rsx! {
                                DraftRow {
                                    label: range_label(current.start, current.end),
                                    on_submit: move |body: String| {
                                        let current = draft.take().expect("the draft being submitted");
                                        selection.set(None);
                                        reviews.write()[change][file].push(Thread {
                                            side: current.side,
                                            start: current.start,
                                            end: current.end,
                                            resolved: false,
                                            comments: vec![
                                                Comment { author: REVIEWER, body: Cow::Owned(body) },
                                            ],
                                        });
                                    },
                                    on_cancel: move |()| {
                                        draft.set(None);
                                        selection.set(None);
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    }
}

/// A thread as a line annotation row: the opening comment and its state, or, when resolved, one
/// summary line.
#[component]
fn ThreadRow(change: usize, file: usize, thread: usize, screen: Signal<Screen>) -> Element {
    let mut reviews = use_context::<Reviews>();
    let data = reviews.read()[change][file][thread].clone();
    let label = data.range_label();
    let first = &data.comments[0];
    let count = data.comments.len();
    let comment_noun = if count == 1 { "comment" } else { "comments" };
    rsx! {
        aside {
            class: "flex flex-wrap items-center gap-2 border-y border-border bg-surface p-3 text-sm text-text",
            "aria-label": "Thread on {label}",
            if data.resolved {
                span { class: "truncate", "{label}, resolved: {first.author} {first.body}" }
            } else {
                span { "{label}: " strong { "{first.author}" } " {first.body}" }
            }
            button {
                class: "min-h-12 rounded-md px-2 focus-visible:outline",
                "aria-label": "Open thread on {label}",
                onclick: move |_| screen.set(Screen::Thread { change, file, thread }),
                "{count} {comment_noun}, " { if data.resolved { "resolved" } else { "open" } }
            }
            button {
                class: "min-h-12 rounded-md px-2 focus-visible:outline",
                "aria-label": if data.resolved { "Reopen thread on {label}" } else { "Resolve thread on {label}" },
                onclick: move |_| {
                    let thread = &mut reviews.write()[change][file][thread];
                    thread.resolved = !thread.resolved;
                },
                if data.resolved { "Reopen" } else { "Resolve" }
            }
        }
    }
}

/// A draft annotation as a line annotation row: a textarea, Submit and Cancel. It takes focus
/// when it mounts, with standard Dioxus. The text is the row's own state: the textarea is not
/// bound to it, so typing (and an IME composition) is never overwritten by a round trip, and
/// only Submit reads it.
#[component]
fn DraftRow(label: String, on_submit: Callback<String>, on_cancel: Callback<()>) -> Element {
    let mut body = use_signal(String::new);
    let empty = body.read().trim().is_empty();
    rsx! {
        aside {
            class: "flex flex-col gap-2 border-y border-border bg-surface p-3 text-sm text-text",
            "aria-label": "Draft annotation",
            strong { "New annotation on {label}" }
            textarea {
                class: "w-full rounded-md border border-border bg-surface px-3 py-2 text-foreground focus-visible:outline",
                "aria-label": "Annotation text",
                rows: "3",
                oninput: move |event| body.set(event.value()),
                onmounted: move |event| {
                    spawn(async move {
                        event
                            .data()
                            .set_focus(true)
                            .await
                            .expect("the draft's textarea takes focus when it opens");
                    });
                },
            }
            div { class: "flex gap-2",
                button {
                    class: "min-h-12 rounded-md px-4 focus-visible:outline",
                    "aria-label": "Submit annotation",
                    disabled: empty,
                    onclick: move |_| on_submit(body.read().trim().to_owned()),
                    "Submit"
                }
                button {
                    class: "min-h-12 rounded-md px-4 focus-visible:outline",
                    "aria-label": "Cancel annotation",
                    onclick: move |_| on_cancel(()),
                    "Cancel"
                }
            }
        }
    }
}

/// J4: a thread: its comments, a reply box, and resolve or reopen.
#[component]
fn ThreadView(change: usize, file: usize, thread: usize) -> Element {
    let mut reviews = use_context::<Reviews>();
    let data = reviews.read()[change][file][thread].clone();
    let mut reply = use_signal(String::new);
    let empty = reply.read().trim().is_empty();
    rsx! {
        div { class: "mx-4 flex flex-col gap-4",
            p { role: "status",
                "{CHANGES[change].files[file].path} {data.range_label()}, "
                if data.resolved { "resolved" } else { "open" }
            }
            ol { class: "flex flex-col gap-2", "aria-label": "Comments",
                for (index , comment) in data.comments.iter().enumerate() {
                    li { key: "{index}", class: "rounded-md border border-border bg-surface p-3",
                        strong { "{comment.author}" }
                        p { "{comment.body}" }
                    }
                }
            }
            // Keyed by the comment count: a submitted reply remounts the textarea empty, so it
            // needs no `value` binding (see `DraftRow`).
            textarea {
                key: "{data.comments.len()}",
                class: "w-full rounded-md border border-border bg-surface px-3 py-2 text-foreground focus-visible:outline",
                "aria-label": "Reply",
                rows: "3",
                oninput: move |event| reply.set(event.value()),
            }
            div { class: "flex gap-2",
                button {
                    class: "min-h-12 rounded-md px-4 focus-visible:outline",
                    "aria-label": "Submit reply",
                    disabled: empty,
                    onclick: move |_| {
                        let body = std::mem::take(&mut *reply.write());
                        reviews.write()[change][file][thread]
                            .comments
                            .push(Comment { author: REVIEWER, body: Cow::Owned(body.trim().to_owned()) });
                    },
                    "Reply"
                }
                button {
                    class: "min-h-12 rounded-md px-4 focus-visible:outline",
                    "aria-label": if data.resolved { "Reopen thread" } else { "Resolve thread" },
                    onclick: move |_| {
                        let thread = &mut reviews.write()[change][file][thread];
                        thread.resolved = !thread.resolved;
                    },
                    if data.resolved { "Reopen" } else { "Resolve" }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Dioxus turns a panicking render into an empty subtree, so the list being present at all is
    // the check that the mock patches parse and the threads seed.
    #[test]
    fn the_change_list_renders_each_change_with_its_open_threads() {
        // `Callback`s need a runtime, so the review is mounted under a root component.
        fn Root() -> Element {
            rsx! { MockReview { close: |()| () } }
        }
        let mut dom = VirtualDom::new(Root);
        dom.rebuild_in_place();
        let page = dioxus_ssr::render(&dom);
        assert!(page.contains("Cap the retry backoff"));
        assert!(page.contains("2 files, 1 open threads"));
        assert!(page.contains("1 files, 0 open threads"));
    }
}
