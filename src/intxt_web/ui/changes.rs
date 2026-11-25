//! The commits tab (each revision with its commits) and the files tab (each revision's patch per
//! file, with comment threads where their anchors fall).

use super::At;
use super::Thread;
use crate::diff::Line;
use crate::files::FileView;
use crate::view::IntxtView;
use dioxus::prelude::*;

pub(super) fn commits(view: &IntxtView) -> Element {
    let revisions: Vec<_> = view.revisions().collect();
    if revisions.is_empty() {
        return rsx! {
            p { class: "intxt-muted", "No commits in this intxt." }
        };
    }
    rsx! {
        for (_index , entry) in revisions {
            section { class: "intxt-revision",
                h3 {
                    "{entry.kind.name()} "
                    code { "{entry.label}" }
                }
                if entry.commits.is_empty() {
                    p { class: "intxt-muted", "The intxt holds no commits for this one." }
                }
                ul {
                    for commit in entry.commits.iter() {
                        li {
                            p {
                                strong { "{commit.subject()}" }
                            }
                            p { class: "intxt-muted intxt-small",
                                code { "{crate::view::short(&commit.sha)}" }
                                " {commit.author}, "
                                At { at: commit.at }
                                if commit.files == 1 {
                                    ", 1 file"
                                } else {
                                    ", {commit.files} files"
                                }
                            }
                            if commit.message.trim() != commit.subject() {
                                details {
                                    summary { "Message" }
                                    p { class: "intxt-commit-message", "{commit.message}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn files(view: &IntxtView) -> Element {
    let revisions = view.files();
    if revisions.is_empty() {
        return rsx! {
            p { class: "intxt-muted", "No patch is recorded for this intxt." }
        };
    }
    rsx! {
        for revision in revisions.iter() {
            section { class: "intxt-revision",
                h3 {
                    "{view.entries[revision.entry].kind.name()} "
                    code { "{revision.label}" }
                }
                for file in revision.files.iter() {
                    {file_block(view, file)}
                }
                if revision.omitted_octets > 0 {
                    p { class: "intxt-muted intxt-small",
                        "The patch is cut here: {revision.omitted_octets} more octets are not shown."
                    }
                }
            }
        }
    }
}

fn file_block(view: &IntxtView, file: &FileView) -> Element {
    let name = if file.path.is_empty() {
        "(no path)"
    } else {
        &file.path
    };
    rsx! {
        section { class: "intxt-file", id: "file-{file.path}",
            h4 {
                code { "{name}" }
                span { class: "intxt-muted intxt-small", " +{file.additions} -{file.deletions}" }
            }
            div { class: "intxt-diff",
                for line in file.lines.iter() {
                    span { class: line.line.kind.class(), "{line.line.text}\n" }
                    for thread in line.threads.iter() {
                        div { class: "intxt-diff-thread",
                            Thread { thread: view.threads[*thread].clone(), id: "thread-{thread}" }
                        }
                    }
                }
            }
            if !file.loose.is_empty() {
                p { class: "intxt-muted intxt-small",
                    "Comments on this file, on lines the patch does not show"
                }
                for thread in file.loose.iter() {
                    Thread { thread: view.threads[*thread].clone(), id: "thread-{thread}" }
                }
            }
        }
    }
}

/// Diff lines as the spans of a diff box, for the guide's excerpts.
pub(super) fn diff_lines(lines: &[&Line]) -> Element {
    rsx! {
        div { class: "intxt-diff",
            for line in lines {
                span { class: line.kind.class(), "{line.text}\n" }
            }
        }
    }
}
