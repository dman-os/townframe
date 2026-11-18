//! A rule between two groups of things (`ct-separator`).
//!
//! Parts: the root only. State: `data-orientation`. A separator that only decorates is
//! `role="none"` (assistive technology skips it); one that divides content a reader should hear
//! about is `role="separator"` with `aria-orientation`.
use super::Orientation;
use dioxus::prelude::*;

#[component]
pub fn Separator(
    #[props(default)] orientation: Orientation,
    /// Purely visual: hidden from assistive technology.
    #[props(default)]
    decorative: bool,
) -> Element {
    rsx! {
        div {
            class: "ct-separator",
            role: if decorative { "none" } else { "separator" },
            "aria-orientation": if decorative { None } else { Some(orientation.as_str()) },
            "data-orientation": orientation.as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app((orientation, decorative): (Orientation, bool)) -> Element {
        rsx! { Separator { orientation, decorative } }
    }

    #[test]
    fn a_separator_is_announced_with_its_orientation() {
        for orientation in [Orientation::Horizontal, Orientation::Vertical] {
            let page = Page::with_props(app, (orientation, false));
            let rule = page.by_role("separator", "");
            assert_eq!(
                page.attr(rule, "aria-orientation").as_deref(),
                Some(orientation.as_str())
            );
            assert_eq!(
                page.attr(rule, "data-orientation").as_deref(),
                Some(orientation.as_str())
            );
            assert_eq!(page.attr(rule, "class").as_deref(), Some("ct-separator"));
        }
    }

    #[test]
    fn a_decorative_separator_is_not_a_separator_to_assistive_technology() {
        let page = Page::with_props(app, (Orientation::Horizontal, true));
        assert!(!page.has_role("separator", ""));
        let rule = page.by_role("none", "");
        assert_eq!(page.attr(rule, "aria-orientation"), None);
        assert_eq!(
            page.attr(rule, "data-orientation").as_deref(),
            Some("horizontal")
        );
    }
}
