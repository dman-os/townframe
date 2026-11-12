//! The Dayframe skin of [`headless::list_row::ListRow`].
use crate::headless::list_row::ListRowSkin;
use crate::headless::list_row::{self};
use dioxus::prelude::*;

// `data-[state=current]` is the open row, painted with the `selected` role (`KIT-107`). The row is
// at least 48 px tall (`KIT-106`).
const SKIN: ListRowSkin = ListRowSkin {
    root: "block min-h-12 w-full cursor-pointer rounded-md px-4 py-3 text-left text-text hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus data-[state=current]:bg-selected data-[state=current]:text-on-selected",
};

/// An openable row of a list inside a [`ListDetails`](crate::ListDetails): opens itself, and is
/// highlighted (with `aria-current`) while it is the open row.
#[component]
pub fn ListRow(id: String, name: String, children: Element) -> Element {
    rsx! {
        list_row::ListRow { id, name, skin: SKIN, {children} }
    }
}

#[cfg(test)]
mod tests {
    use crate::ListDetails;
    use crate::ListRow;
    use crate::Split;
    use crate::WidthClass;
    use crate::testing::Page;
    use dioxus::prelude::*;

    fn app(_: ()) -> Element {
        rsx! {
            ListDetails {
                width: WidthClass::Expanded,
                list_name: "Settings",
                list: rsx! { ListRow { id: "sync", name: "Open Sync", "Sync" } },
                details_name: "Details",
                details: Some(rsx! { "details" }),
                current: Some("sync".to_owned()),
                split: Split::new(vec![1.0, 2.0], 900.0),
                on_resize: |_| {},
                on_open: |_| {},
                on_back: |_| {},
            }
        }
    }

    #[test]
    fn the_open_row_takes_the_selected_role_and_every_row_is_a_48_px_target() {
        let page = Page::with_props(app, ());
        let row = page.by_role("button", "Open Sync");
        let class = page.attr(row, "class").unwrap();
        for expected in [
            "min-h-12",
            "data-[state=current]:bg-selected",
            "data-[state=current]:text-on-selected",
        ] {
            assert!(
                class.split(' ').any(|have| have == expected),
                "{expected} not in {class}"
            );
        }
        // The state the selected classes key on, and the semantics assistive technology reads.
        assert_eq!(page.attr(row, "data-state").as_deref(), Some("current"));
        assert_eq!(page.attr(row, "aria-current").as_deref(), Some("true"));
    }
}
