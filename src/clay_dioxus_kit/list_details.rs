//! The Dayframe skin of [`headless::list_details::ListDetails`].
use crate::WidthClass;
use crate::headless::list_details::ListDetailsSkin;
use crate::headless::list_details::{self};
use crate::headless::sections::Split;
use crate::sections::SKIN as SECTIONS;
use dioxus::prelude::*;

const SKIN: ListDetailsSkin = ListDetailsSkin {
    root: "bg-canvas text-text",
    list: "bg-surface text-text",
    details: "bg-surface text-text",
    sections: SECTIONS,
};

/// The list/details scaffold: one pane at a time on a compact host, both side by side otherwise.
#[component]
pub fn ListDetails(
    width: WidthClass,
    list_name: String,
    list: Element,
    details_name: String,
    details: Option<Element>,
    split: Split,
    on_resize: EventHandler<Vec<f64>>,
    on_open: EventHandler<String>,
    on_back: EventHandler<()>,
) -> Element {
    rsx! {
        list_details::ListDetails {
            width,
            list_name,
            list,
            details_name,
            details,
            split,
            on_resize,
            on_open,
            on_back,
            skin: SKIN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(width: WidthClass) -> Element {
        rsx! {
            ListDetails {
                width,
                list_name: "Settings",
                list: rsx! { "list" },
                details_name: "Details",
                details: Some(rsx! { "details" }),
                split: Split::new(vec![1.0, 2.0], 900.0),
                on_resize: |_| {},
                on_open: |_| {},
                on_back: |_| {},
            }
        }
    }

    #[test]
    fn the_width_class_picks_the_arrangement() {
        let compact = Page::with_props(app, WidthClass::Compact);
        assert!(!compact.has_role("separator", "Resize Settings and Details"));
        let expanded = Page::with_props(app, WidthClass::Expanded);
        assert!(expanded.has_role("separator", "Resize Settings and Details"));
    }
}
