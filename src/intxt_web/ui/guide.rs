//! The guide tab: the author's sections in their order, each with the hunks it points at.

use super::Markdown;
use super::NavLink;
use super::changes::diff_lines;
use crate::files::excerpt;
use crate::view::IntxtView;
use crate::view::Tab;
use dioxus::prelude::*;

/// A section nests this deep at most on the page; deeper ones stay at the last indent.
const MAX_INDENT: usize = 3;

pub(super) fn guide(view: &IntxtView) -> Element {
    if view.guide.is_empty() {
        return rsx! {
            p { class: "intxt-muted", "The author left no guide for this intxt." }
        };
    }
    let files = view.files();
    let files_path = view.tab_path(Tab::Files);
    rsx! {
        ol { class: "intxt-guide",
            for section in view.guide.iter() {
                li { class: "intxt-guide-depth-{section.depth.min(MAX_INDENT)}",
                    h3 {
                        "{section.title}"
                        if section.kind != "section" {
                            span { class: "intxt-muted intxt-small", " ({section.kind})" }
                        }
                    }
                    if let Some(body) = &section.body {
                        Markdown { source: body.clone() }
                    }
                    for target in section.targets.iter() {
                        if let Some(path) = &target.path {
                            p { class: "intxt-small",
                                NavLink { href: "{files_path}#file-{path}",
                                    code { "{target.label}" }
                                }
                            }
                            if let Some(lines) = target.lines.map(|lines| excerpt(&files, path, lines)).filter(|lines| !lines.is_empty()) {
                                {diff_lines(&lines)}
                            }
                        } else {
                            p { class: "intxt-muted intxt-small",
                                code { "{target.label}" }
                            }
                        }
                    }
                }
            }
        }
    }
}
