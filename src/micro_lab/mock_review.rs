//! Experiment: the review loop's first four journeys over mock data (the intxt review loop,
//! J1-J4). A list of changes, one change's overview, one file as a diff with annotation threads
//! inline, and a thread. The data is a constant here; the intext lane replaces this experiment
//! with the real micro over datums.
//!
//! The diff is `pierre_diffs_dioxus::FileDiffView` used as any Dioxus app would use it: the
//! threads are its `render_annotation` content, and nothing in this module knows Clay's guest
//! surface exists.
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;
use pierre_diffs_dioxus::AnnotationSide;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffLineAnnotation;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;

struct Change {
    title: &'static str,
    summary: &'static str,
    files: &'static [File],
}

struct File {
    path: &'static str,
    /// A unified patch of this one file, as the agent's commit produced it.
    patch: &'static str,
    threads: &'static [Thread],
}

struct Thread {
    /// The new-file line the thread hangs under.
    line: usize,
    resolved: bool,
    comments: &'static [Comment],
}

struct Comment {
    author: &'static str,
    body: &'static str,
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
pub fn MockReview(close: Callback<()>) -> Element {
    let mut screen = use_signal(|| Screen::Changes);
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
                    FileDiff { key: "{change}-{file}", change, file, width: width(), screen }
                },
                Screen::Thread { change, file, thread } => rsx! { ThreadView { change, file, thread } },
            }
        }
    }
}

/// J1: the changes waiting for review.
#[component]
fn ChangeList(screen: Signal<Screen>) -> Element {
    rsx! {
        ul { class: "mx-4 flex flex-col gap-2", "aria-label": "Changes",
            for (index , change) in CHANGES.iter().enumerate() {
                li { key: "{index}",
                    button {
                        class: "block min-h-12 w-full rounded-md border border-border px-4 py-3 text-left focus-visible:outline",
                        "aria-label": "Open change {change.title}",
                        onclick: move |_| screen.set(Screen::Change(index)),
                        strong { "{change.title}" }
                        p { "{change.files.len()} files, {open_threads(change)} open threads" }
                    }
                }
            }
        }
    }
}

fn open_threads(change: &Change) -> usize {
    change
        .files
        .iter()
        .flat_map(|file| file.threads)
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

/// J3: a file as a diff, unified when the width does not fit two panes, with the file's threads
/// under their lines.
#[component]
fn FileDiff(change: usize, file: usize, width: WidthClass, screen: Signal<Screen>) -> Element {
    let data = &CHANGES[change].files[file];
    // The input is compared by identity (see `DiffInput::Parsed`), so it is parsed once.
    let input = use_hook(|| DiffInput::from_patch(data.patch).expect("the mock patches parse"));
    let annotations: Vec<DiffLineAnnotation<usize>> = data
        .threads
        .iter()
        .enumerate()
        .map(|(thread, data)| DiffLineAnnotation {
            side: AnnotationSide::Additions,
            line_number: data.line,
            metadata: Some(thread),
        })
        .collect();
    rsx! {
        div { class: "overflow-hidden rounded-md border border-border",
            FileDiffView::<usize> {
                input,
                diff_style: if width.fits_side_by_side() { DiffStyle::Split } else { DiffStyle::Unified },
                annotations,
                render_annotation: move |annotation: DiffLineAnnotation<usize>| {
                    let thread = annotation.metadata.expect("every mock annotation carries its thread");
                    let data = &CHANGES[change].files[file].threads[thread];
                    let first = &data.comments[0];
                    rsx! {
                        aside { class: "border-y border-border bg-surface p-3 text-sm text-text",
                            strong { "{first.author}" }
                            " {first.body}"
                            button {
                                class: "ml-2 min-h-12 rounded-md px-2 focus-visible:outline",
                                "aria-label": "Open thread on line {data.line}",
                                onclick: move |_| screen.set(Screen::Thread { change, file, thread }),
                                "{data.comments.len()} comments, " { if data.resolved { "resolved" } else { "open" } }
                            }
                        }
                    }
                },
            }
        }
    }
}

/// J4: a thread, read only.
#[component]
fn ThreadView(change: usize, file: usize, thread: usize) -> Element {
    let data = &CHANGES[change].files[file].threads[thread];
    rsx! {
        div { class: "mx-4 flex flex-col gap-4",
            p { role: "status",
                "{CHANGES[change].files[file].path} line {data.line}, "
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
        }
    }
}

const CHANGES: &[Change] = &[
    Change {
        title: "Cap the retry backoff",
        summary: "Retries doubled forever and overflowed after 64 attempts. The delay now saturates and is capped at thirty seconds, and a push wakes the queue's waiters.",
        files: &[
            File {
                path: "src/retry.rs",
                patch: "diff --git a/src/retry.rs b/src/retry.rs
--- a/src/retry.rs
+++ b/src/retry.rs
@@ -1,9 +1,12 @@
 use std::time::Duration;
 
-pub fn backoff(attempt: u32) -> Duration {
-    Duration::from_millis(100 * 2u64.pow(attempt))
+const CAP: Duration = Duration::from_secs(30);
+
+pub fn backoff(attempt: u32) -> Duration {
+    let delay = Duration::from_millis(100 * 2u64.saturating_pow(attempt));
+    delay.min(CAP)
 }
 
 pub fn should_retry(error: &Error) -> bool {
     error.is_transient()
 }
",
                threads: &[Thread {
                    line: 6,
                    resolved: false,
                    comments: &[
                        Comment {
                            author: "Reviewer",
                            body: "The multiplication can still overflow before the cap applies; saturate that too.",
                        },
                        Comment {
                            author: "Agent",
                            body: "u64 saturating_pow covers the power; 100 * u64::MAX would not, so I will use saturating_mul.",
                        },
                    ],
                }],
            },
            File {
                path: "src/queue.rs",
                patch: "diff --git a/src/queue.rs b/src/queue.rs
--- a/src/queue.rs
+++ b/src/queue.rs
@@ -10,6 +10,7 @@ impl Queue {
     pub fn push(&mut self, job: Job) {
         self.jobs.push_back(job);
+        self.notify.notify_one();
     }
 
     pub fn pop(&mut self) -> Option<Job> {
         self.jobs.pop_front()
",
                threads: &[],
            },
        ],
    },
    Change {
        title: "Validate config at startup",
        summary: "A bad config file panicked with an opaque unwrap. Loading now names what is wrong and rejects a zero worker count or port.",
        files: &[File {
            path: "src/config.rs",
            patch: "diff --git a/src/config.rs b/src/config.rs
--- a/src/config.rs
+++ b/src/config.rs
@@ -20,6 +20,9 @@
 impl Config {
     pub fn load(path: &Path) -> Config {
-        let text = fs::read_to_string(path).unwrap();
-        toml::from_str(&text).unwrap()
+        let text = fs::read_to_string(path).expect(\"config file is readable\");
+        let config: Config = toml::from_str(&text).expect(\"config is valid TOML\");
+        assert!(config.workers > 0, \"workers must be positive\");
+        assert!(config.port != 0, \"port must be set\");
+        config
     }
 }
",
            threads: &[Thread {
                line: 24,
                resolved: true,
                comments: &[Comment {
                    author: "Reviewer",
                    body: "A user's config is input, not an invariant; this should be a returned error.",
                }],
            }],
        }],
    },
    Change {
        title: "Document the sync retry",
        summary: "The module doc now says why retries are expected, so nobody parks them.",
        files: &[File {
            path: "src/sync.rs",
            patch: "diff --git a/src/sync.rs b/src/sync.rs
--- a/src/sync.rs
+++ b/src/sync.rs
@@ -1,4 +1,5 @@
-//! Sync.
+//! Pulls membership before documents.
+//! Retries are expected while membership lands.
 
 pub fn pull() {}
 pub fn push() {}
",
            threads: &[],
        }],
    },
];
