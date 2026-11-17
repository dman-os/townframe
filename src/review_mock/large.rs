//! Experiment: a generated file of about three thousand lines as a virtualized `FileDiffView`,
//! the size virtualization exists for. The theme lab's Diffs tab renders the same pair, so the
//! lab micro and the desktop lab scroll the same rows and a difference between them is the
//! runtime's.

use clay_dioxus_kit::AppBar;
use dioxus::prelude::*;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileContents;
use pierre_diffs_dioxus::FileDiffView;
use pierre_diffs_dioxus::Interaction;
use pierre_diffs_dioxus::LineHoverHighlight;
use pierre_diffs_dioxus::Virtualization;

/// The blocks of [`large_pair`] that make about three thousand lines.
pub const LARGE_DIFF_BLOCKS: usize = 500;

/// A generated Rust file of `blocks` six-line functions, and the same file after a scattering of
/// edits: every 23rd body changes, every 47th block gains a line, every 61st is removed. At
/// [`LARGE_DIFF_BLOCKS`] that is about three thousand lines, without checking megabytes into the
/// repository.
pub fn large_pair(blocks: usize) -> (Box<FileContents>, Box<FileContents>) {
    let block = |index: usize, after: bool| {
        let edited = after && index.is_multiple_of(23);
        let scaled = if edited {
            format!("input * {index} + 1")
        } else {
            format!("input * {index}")
        };
        let trace = if after && index.is_multiple_of(47) {
            format!("    tracing::trace!(\"handler_{index} ran\");\n")
        } else {
            String::new()
        };
        format!(
            "/// Handler {index}: clamps its input to the limit.\npub fn handler_{index}(input: u32, limit_{index}: u32) -> u32 {{\n{trace}    let scaled = {scaled};\n    scaled.min(limit_{index})\n}}\n\n"
        )
    };
    let before: String = (0..blocks).map(|index| block(index, false)).collect();
    let after: String = (0..blocks)
        .filter(|index| index % 61 != 60)
        .map(|index| block(index, true))
        .collect();
    let file = |contents| {
        Box::new(FileContents {
            name: "src/handlers.rs".to_owned(),
            contents,
            lang: None,
            header: None,
            cache_key: None,
        })
    };
    (file(before), file(after))
}

/// The large pair as a unified, virtualized diff with every unchanged line expanded, under an app
/// bar whose "Back" closes it. The embedder supplies the scroller's height and its timer.
#[component]
pub fn LargeDiff(close: Callback<()>, virtualized: Virtualization) -> Element {
    let input = use_hook(|| {
        let (old_file, new_file) = large_pair(LARGE_DIFF_BLOCKS);
        DiffInput::Files { old_file, new_file }
    });
    rsx! {
        div { class: "flex flex-col gap-4",
            AppBar {
                title: "Large diff",
                leading: rsx! {
                    button {
                        class: "min-h-12 rounded-md px-4 focus-visible:outline",
                        "aria-label": "Back",
                        onclick: move |_| close(()),
                        "Back"
                    }
                },
            }
            div { class: "overflow-hidden rounded-md border border-border",
                FileDiffView::<()> {
                    input,
                    diff_style: DiffStyle::Unified,
                    expand_unchanged: true,
                    virtualized: Some(virtualized),
                    interaction: Interaction {
                        line_hover_highlight: LineHoverHighlight::Both,
                        ..Interaction::default()
                    },
                }
            }
        }
    }
}
