//! The "PR #84" view: a real, 599-file, 78k-line unified git diff. It is read once for the
//! process (not per render, not per mount; from disk on desktop, fetched as a static asset next
//! to the page in the browser, which cannot read `/tmp`), split into per-file patches by their
//! `diff --git` boundaries, and listed by path and size. Only the selected file is parsed and
//! rendered, through the virtualized `FileDiffView`: the port exposes the parse of a single
//! patch (`DiffInput::from_patch`), so the multi-file text is split here and each file parsed
//! on first selection and cached. A patch cannot expand its collapsed lines (the view needs the
//! full file contents for that), so the options bar's expansion does not reach this view.

use dioxus::prelude::*;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;
use pierre_diffs_dioxus::Virtualization;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::OnceLock;
use std::time::Duration;

use super::Segmented;
use review_mock::DiffOptions;
use web_time::Instant;

/// Where the diff comes from unless `DAYFRAME_LAB_DIFF` says otherwise.
#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_PATH: &str = "/tmp/pr84.diff";

/// Where the page fetches the diff from: `x/dayframe.ts showcase-web` copies it beside the page at
/// serve time, so it is never in git (3.6 MB).
#[cfg(target_arch = "wasm32")]
const FETCH_URL: &str = "pr84.diff";

/// How to produce the file when it is missing.
const HOW_TO_PRODUCE: &str =
    "git diff origin/feat/datum-core...origin/feat/big-node > /tmp/pr84.diff";

/// One file of the diff: its path, its size, and where its patch lies in the text.
struct PrFile {
    path: String,
    added: usize,
    removed: usize,
    range: Range<usize>,
}

struct PrDiff {
    text: String,
    files: Vec<PrFile>,
    /// What reading and splitting took.
    load: Duration,
}

/// A prop of [`PrFiles`]: the one diff of the process, so identity is equality.
impl PartialEq for PrDiff {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl PrDiff {
    /// The file indices of the diff, largest changed-line count first.
    fn by_size(&self) -> Vec<usize> {
        let mut order: Vec<usize> = (0..self.files.len()).collect();
        order.sort_by_key(|&index| {
            let file = &self.files[index];
            std::cmp::Reverse(file.added + file.removed)
        });
        order
    }
}

/// The diff, loaded on first use: `Err` is the message for the operator, because a missing file
/// is an input the lab's user controls. Set once (the load is awaited, so no `get_or_init`).
static PR_DIFF: OnceLock<Result<PrDiff, String>> = OnceLock::new();

/// The diff text and where it was read from.
#[cfg(not(target_arch = "wasm32"))]
async fn read_text() -> Result<(String, String), String> {
    let path = std::env::var("DAYFRAME_LAB_DIFF").unwrap_or_else(|_| DEFAULT_PATH.to_owned());
    let text = std::fs::read_to_string(&path).map_err(|error| {
        format!("Could not read {path}: {error}. Produce it with `{HOW_TO_PRODUCE}`, or point DAYFRAME_LAB_DIFF at another unified git diff.")
    })?;
    Ok((path, text))
}

/// The diff text and where it was fetched from.
#[cfg(target_arch = "wasm32")]
async fn read_text() -> Result<(String, String), String> {
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    let unavailable = |why: String| {
        format!("Could not fetch {FETCH_URL}: {why}. Produce it with `{HOW_TO_PRODUCE}` and serve the lab with `x/dayframe.ts showcase-web`, which copies it beside the page.")
    };
    let window = web_sys::window().expect("the lab runs in a window");
    let response = JsFuture::from(window.fetch_with_str(FETCH_URL))
        .await
        .map_err(|error| unavailable(format!("{error:?}")))?
        .dyn_into::<web_sys::Response>()
        .expect("fetch resolves to a Response");
    if !response.ok() {
        return Err(unavailable(format!("HTTP {}", response.status())));
    }
    let text = JsFuture::from(
        response
            .text()
            .map_err(|error| unavailable(format!("{error:?}")))?,
    )
    .await
    .map_err(|error| unavailable(format!("{error:?}")))?
    .as_string()
    .expect("a response's text is a string");
    Ok((FETCH_URL.to_owned(), text))
}

async fn load() -> Result<PrDiff, String> {
    let started = Instant::now();
    let (source, text) = read_text().await?;
    let files = split_files(&text);
    if files.is_empty() {
        return Err(format!("{source} holds no `diff --git` file diffs."));
    }
    Ok(PrDiff {
        text,
        files,
        load: started.elapsed(),
    })
}

/// The per-file patches of a git diff: each starts at a line beginning `diff --git `. The `+` and
/// `-` counts are those of the lines after the first hunk header, so the `---`/`+++` file header
/// is never counted.
fn split_files(text: &str) -> Vec<PrFile> {
    let mut starts: Vec<usize> = Vec::new();
    if text.starts_with("diff --git ") {
        starts.push(0);
    }
    starts.extend(text.match_indices("\ndiff --git ").map(|(at, _)| at + 1));
    starts.push(text.len());
    starts
        .windows(2)
        .map(|bounds| {
            let range = bounds[0]..bounds[1];
            let patch = &text[range.clone()];
            let header = patch
                .lines()
                .next()
                .expect("a split patch starts at its header");
            let path = header
                .rsplit_once(" b/")
                .map_or(header, |(_, path)| path)
                .to_owned();
            let (added, removed) = patch.split_once("\n@@").map_or((0, 0), |(_, hunks)| {
                hunks.lines().fold((0, 0), |(added, removed), line| {
                    match line.as_bytes().first() {
                        Some(b'+') => (added + 1, removed),
                        Some(b'-') => (added, removed + 1),
                        _ => (added, removed),
                    }
                })
            });
            PrFile {
                path,
                added,
                removed,
                range,
            }
        })
        .collect()
}

thread_local! {
    /// Parsed files by index, so a file selected twice keeps its parsed diff (the view compares
    /// parsed input by identity, so a re-parse would also re-render it).
    static PARSED: RefCell<HashMap<usize, Result<DiffInput, String>>> =
        RefCell::new(HashMap::new());
}

/// The parsed diff of one file and what parsing it cost (zero when cached).
fn parsed(diff: &PrDiff, index: usize) -> (Result<DiffInput, String>, Duration) {
    PARSED.with_borrow_mut(|cache| {
        if let Some(input) = cache.get(&index) {
            return (input.clone(), Duration::ZERO);
        }
        let started = Instant::now();
        let input = DiffInput::from_patch(&diff.text[diff.files[index].range.clone()])
            .map_err(|error| error.message);
        let took = started.elapsed();
        cache.insert(index, input.clone());
        (input, took)
    })
}

#[component]
pub fn PrView(options: DiffOptions, virtualized: Virtualization) -> Element {
    // Loaded off the render path, once for the process; the resource resolves immediately on a
    // later mount.
    let loaded = use_resource(|| async {
        if let Some(diff) = PR_DIFF.get() {
            return diff;
        }
        let diff = load().await;
        PR_DIFF.get_or_init(|| diff)
    });
    let Some(diff) = *loaded.read() else {
        return rsx! { p { class: "text-sm text-text-muted", "Loading the diff..." } };
    };
    match diff {
        Ok(diff) => rsx! { PrFiles { diff, options, virtualized } },
        Err(message) => rsx! { p { role: "alert", class: "text-sm text-foreground", "{message}" } },
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Order {
    Size,
    Path,
}

#[component]
fn PrFiles(diff: &'static PrDiff, options: DiffOptions, virtualized: Virtualization) -> Element {
    let by_size = use_hook(|| diff.by_size());
    let mut order = use_signal(|| Order::Size);
    let mut filter = use_signal(String::new);
    // The file selector (order, filter and list) can be put away so the diff takes the full width.
    let mut show_files = use_signal(|| true);
    // The largest file opens first.
    let mut selected = use_signal(|| by_size[0]);
    let listed: Vec<usize> = {
        let filter = filter.read().to_lowercase();
        let mut listed: Vec<usize> = match order() {
            Order::Size => by_size.clone(),
            Order::Path => (0..diff.files.len()).collect(),
        };
        listed.retain(|&index| diff.files[index].path.to_lowercase().contains(&filter));
        listed
    };
    let index = selected();
    let (input, parse) = parsed(diff, index);
    let file = &diff.files[index];
    let (total_added, total_removed) = diff
        .files
        .iter()
        .fold((0, 0), |(a, r), file| (a + file.added, r + file.removed));
    rsx! {
        div { class: "flex flex-col gap-3",
            p { class: "text-sm text-text-muted",
                "{diff.files.len()} files, +{total_added} -{total_removed}. Loaded and split in {diff.load.as_secs_f64() * 1000.0:.0} ms. {file.path} parsed in {parse.as_secs_f64() * 1000.0:.1} ms (0 when cached)."
            }
            div { class: "flex flex-wrap items-center gap-3",
                button {
                    class: "min-h-12 cursor-pointer rounded-md border border-border bg-card px-4 font-sans text-foreground-muted hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus",
                    "aria-expanded": show_files(),
                    "aria-controls": "pr-file-list",
                    onclick: move |_| show_files.toggle(),
                    if show_files() {
                        "Hide file list"
                    } else {
                        "Show file list"
                    }
                }
                if show_files() {
                    Segmented {
                        label: "File order",
                        options: vec!["Largest first", "Path"],
                        selected: if order() == Order::Size { 0 } else { 1 },
                        on_select: move |choice: usize| {
                            order.set(if choice == 0 { Order::Size } else { Order::Path });
                        },
                    }
                    input {
                        class: "min-h-12 flex-1 rounded-md border border-border bg-surface px-3 text-foreground focus-visible:outline-2 focus-visible:outline-focus",
                        "aria-label": "Filter files",
                        placeholder: "Filter files by path",
                        oninput: move |event| filter.set(event.value()),
                    }
                }
            }
            div { class: "flex flex-col gap-3 lg:flex-row",
                if show_files() {
                    ul {
                        id: "pr-file-list",
                        class: "flex min-w-0 flex-col gap-1 overflow-y-auto lg:w-96 lg:shrink-0",
                        max_height: "32rem",
                        "aria-label": "Files of the diff",
                        for file_index in listed {
                            {
                                let file = &diff.files[file_index];
                                rsx! {
                                    li { key: "{file_index}",
                                        button {
                                            class: "flex min-h-12 w-full cursor-pointer items-center justify-between gap-3 rounded-md border border-border bg-card px-3 text-left text-sm text-foreground-muted hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus data-[state=active]:bg-selected data-[state=active]:text-on-selected",
                                            "data-state": if file_index == index { "active" } else { "inactive" },
                                            "aria-current": file_index == index,
                                            onclick: move |_| selected.set(file_index),
                                            span { class: "min-w-0 break-all", "{file.path}" }
                                            span { class: "shrink-0", "+{file.added} -{file.removed}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "min-w-0 flex-1 overflow-hidden rounded-md border border-border",
                    match input {
                        // Keyed by file: the view reads its virtualization when it mounts.
                        Ok(input) => rsx! {
                            FileDiffView::<()> {
                                key: "{index}",
                                input,
                                diff_style: options.diff_style.unwrap_or(DiffStyle::Unified),
                                virtualized: Some(virtualized),
                                interaction: options.interaction(),
                            }
                        },
                        Err(message) => rsx! {
                            p { role: "alert", class: "p-3 text-sm text-foreground", "{file.path}: {message}" }
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_files_cuts_at_each_git_header_and_counts_hunk_lines_only() {
        let text = "diff --git a/one.rs b/one.rs\n--- a/one.rs\n+++ b/one.rs\n@@ -1,2 +1,2 @@\n-old\n+new\n keep\ndiff --git a/dir/two.txt b/dir/two.txt\nnew file mode 100644\n--- /dev/null\n+++ b/dir/two.txt\n@@ -0,0 +1,1 @@\n+hello\n";
        let files = split_files(text);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "one.rs");
        assert_eq!((files[0].added, files[0].removed), (1, 1));
        assert_eq!(files[1].path, "dir/two.txt");
        assert_eq!((files[1].added, files[1].removed), (1, 0));
        assert!(text[files[1].range.clone()].starts_with("diff --git a/dir/two.txt"));
        let diff = PrDiff {
            text: text.to_owned(),
            files,
            load: Duration::ZERO,
        };
        assert_eq!(diff.by_size(), vec![0, 1]);
        let (input, _) = parsed(&diff, 1);
        assert!(input.is_ok());
    }
}
