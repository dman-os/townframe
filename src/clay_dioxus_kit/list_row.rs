//! The Dayframe skin of [`headless::list_row::ListRow`].
use crate::headless::list_row::ListRowSkin;
use crate::headless::list_row::{self};
use dioxus::prelude::*;

// `data-[state=current]` is the highlight of the open row; `accent-container` is the design
// language's nearest "selected" role.
const SKIN: ListRowSkin = ListRowSkin {
    root: "block w-full cursor-pointer rounded-md px-4 py-3 text-left text-text hover:bg-surface-sunken focus-visible:outline-2 focus-visible:outline-focus data-[state=current]:bg-accent-container data-[state=current]:text-on-accent-container",
};

/// An openable row of a list inside a [`ListDetails`](crate::ListDetails): opens itself, and is
/// highlighted (with `aria-current`) while it is the open row.
#[component]
pub fn ListRow(id: String, name: String, children: Element) -> Element {
    rsx! {
        list_row::ListRow { id, name, skin: SKIN, {children} }
    }
}
