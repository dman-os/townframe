//! The Dayframe skin of [`headless::drawer::Drawer`].
use crate::headless::drawer::DrawerSkin;
use crate::headless::drawer::{self};
use dioxus::prelude::*;

// The scrim has no semantic role in the design language yet; see `sheet.rs` and "Design-language
// gaps" in the README. The drawer's `w-80` (20rem) leaves a strip of the host uncovered on the
// narrowest phone widths, which is the scrim's tap target.
const SKIN: DrawerSkin = DrawerSkin {
    root: "",
    scrim: "cursor-default bg-surface-sunken opacity-80",
    panel: "h-full w-80 max-w-5/6 overflow-hidden bg-surface-raised text-text shadow-raised",
};

/// The compact host's sidebar: a panel from the left edge over a scrim, shown while `open`.
#[component]
pub fn Drawer(
    name: String,
    open: bool,
    on_dismiss: EventHandler<()>,
    children: Element,
) -> Element {
    rsx! {
        drawer::Drawer { name, open, on_dismiss, skin: SKIN, {children} }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(open: bool) -> Element {
        rsx! {
            Drawer { name: "Sidebar", open, on_dismiss: |_| {}, "body" }
        }
    }

    #[test]
    fn the_skin_sizes_the_panel_and_dims_the_rest() {
        let page = Page::with_props(app, true);
        let dialog = page.by_role("dialog", "Sidebar");
        assert!(page.attr(dialog, "class").unwrap().contains("w-80"));
        let scrim = page.by_role("button", "Dismiss Sidebar");
        assert!(page.attr(scrim, "class").unwrap().contains("opacity-80"));
    }
}
