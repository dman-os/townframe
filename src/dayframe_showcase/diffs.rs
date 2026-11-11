//! The Diffs tab: `pierre_diffs_dioxus::FileDiffView` over two sample file pairs.

use crate::diff_fixtures;
use dioxus::prelude::*;
use pierre_diffs_dioxus::AnnotationSide;
use pierre_diffs_dioxus::DiffInput;
use pierre_diffs_dioxus::DiffLineAnnotation;
use pierre_diffs_dioxus::DiffStyle;
use pierre_diffs_dioxus::FileDiffView;

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
        }
    }
}

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
    }
}
