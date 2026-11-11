//! The Diffs tab: `pierre_diffs_dioxus::FileDiffView` over two sample file pairs, and a generated
//! three-thousand-line one that only renders the rows near its scroller's viewport.

use crate::diff_fixtures;
use dioxus::prelude::*;
use pierre_diffs_dioxus::AnnotationSide;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffLineAnnotation;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;
use pierre_diffs_dioxus::PtrEq;
use pierre_diffs_dioxus::Virtualization;
use std::rc::Rc;

/// A reviewer's note: the shape of content an embedding plug puts under a diff line.
#[derive(Clone, PartialEq)]
pub struct Note {
    author: &'static str,
    body: &'static str,
}

/// Two stacked diffs, a split Rust edit and a unified TypeScript edit that carries one line
/// annotation. Both expand their unchanged lines, so a few hundred lines are on screen.
#[component]
pub fn DiffsFoundation() -> Element {
    let (rust_old, rust_new) = diff_fixtures::rust_pair();
    let (ts_old, ts_new) = diff_fixtures::ts_pair();
    // Built once: the props compare the input by content and the sleep function by identity, so
    // making them again on every render would re-render the diff each time.
    let large = use_hook(|| {
        let (old_file, new_file) = diff_fixtures::large_pair(LARGE_DIFF_BLOCKS);
        DiffInput::Files { old_file, new_file }
    });
    let virtualized = use_hook(|| Virtualization {
        height: "32rem".to_owned(),
        sleep: PtrEq::new(Rc::new(|duration| Box::pin(tokio::time::sleep(duration)))),
    });
    rsx! {
        div {
            class: "flex flex-col gap-8",
            div {
                class: "overflow-hidden rounded-md border border-border",
                FileDiffView::<Note> {
                    input: DiffInput::Files {
                        old_file: rust_old,
                        new_file: rust_new,
                    },
                    diff_style: DiffStyle::Split,
                    expand_unchanged: true,
                }
            }
            div {
                class: "overflow-hidden rounded-md border border-border",
                FileDiffView::<Note> {
                    input: DiffInput::Files {
                        old_file: ts_old,
                        new_file: ts_new,
                    },
                    diff_style: DiffStyle::Unified,
                    expand_unchanged: true,
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
                    diff_style: DiffStyle::Unified,
                    expand_unchanged: true,
                    virtualized: Some(virtualized),
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
