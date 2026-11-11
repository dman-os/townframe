//! The Dayframe skin of [`headless::drawer::Drawer`].
use crate::WidthClass;
use crate::headless::drawer::DrawerSkin;
use crate::headless::drawer::{self};
use crate::layer::SCRIM;
use dioxus::prelude::*;

// The panel is 20rem wide but never more than 5/6 of the host, so on the narrowest compact host
// (a 320 px phone: a 267 px panel) the rest of the host stays a scrim strip to tap.
const SKIN: DrawerSkin = DrawerSkin {
    root: "",
    scrim: SCRIM,
    panel: "h-full w-80 max-w-5/6 overflow-hidden bg-surface-raised text-text shadow-raised animate-slide-in-leading motion-reduce:animate-none",
};

/// The compact host's sidebar: a panel from the left edge over a scrim, shown while `open` and the
/// host is compact. A wider host has room for the sidebar beside its content, so the drawer is
/// never shown there whatever `open` says (`KIT-101`).
#[component]
pub fn Drawer(
    width: WidthClass,
    name: String,
    open: bool,
    on_dismiss: EventHandler<()>,
    children: Element,
) -> Element {
    let open = open && !width.fits_side_by_side();
    rsx! {
        drawer::Drawer { name, open, on_dismiss, skin: SKIN, {children} }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(width: WidthClass) -> Element {
        rsx! {
            Drawer { width, name: "Sidebar", open: true, on_dismiss: |_| {}, "body" }
        }
    }

    #[test]
    fn the_skin_sizes_the_panel_and_dims_the_rest_with_the_scrim_role() {
        let page = Page::with_props(app, WidthClass::Compact);
        let dialog = page.by_role("dialog", "Sidebar");
        let panel = page.attr(dialog, "class").unwrap();
        assert!(
            panel.contains("w-80") && panel.contains("max-w-5/6"),
            "{panel}"
        );
        let scrim = page
            .attr(page.by_role("button", "Dismiss Sidebar"), "class")
            .unwrap();
        assert!(
            scrim.contains("bg-scrim") && !scrim.contains("opacity-"),
            "{scrim}"
        );
    }

    #[test]
    fn it_slides_in_from_the_leading_edge_unless_motion_is_reduced() {
        let page = Page::with_props(app, WidthClass::Compact);
        let panel = page
            .attr(page.by_role("dialog", "Sidebar"), "class")
            .unwrap();
        assert!(panel.contains("animate-slide-in-leading"), "{panel}");
        assert!(panel.contains("motion-reduce:animate-none"), "{panel}");
    }

    #[test]
    fn only_a_compact_host_shows_a_drawer() {
        for (width, shown) in [
            (WidthClass::Compact, true),
            (WidthClass::Medium, false),
            (WidthClass::Expanded, false),
        ] {
            let page = Page::with_props(app, width);
            assert_eq!(page.has_role("dialog", "Sidebar"), shown, "{width:?}");
            assert_eq!(
                page.has_role("button", "Dismiss Sidebar"),
                shown,
                "{width:?}"
            );
        }
    }

    /// An owner as an app writes it: `open` is a signal, an "Open" button sets it. Dismissal only
    /// counts if the owner's state follows, so this drives the whole loop.
    fn owner(width: WidthClass) -> Element {
        let mut open = use_signal(|| true);
        rsx! {
            button { onclick: move |_| open.set(true), "Open drawer" }
            Drawer { width, name: "Sidebar", open: open(), on_dismiss: move |()| open.set(false),
                button { "Home" }
            }
        }
    }

    #[test]
    fn an_owner_can_dismiss_the_drawer_by_scrim_or_escape_and_reopen_it() {
        let mut page = Page::with_props(owner, WidthClass::Compact);
        assert!(page.has_role("dialog", "Sidebar"));

        let scrim = page.by_role("button", "Dismiss Sidebar");
        page.click(scrim);
        assert!(!page.has_role("dialog", "Sidebar"), "scrim dismiss");

        let open = page.by_role("button", "Open drawer");
        page.click(open);
        assert!(page.has_role("dialog", "Sidebar"), "reopens");

        let dialog = page.by_role("dialog", "Sidebar");
        page.key(dialog, Key::Named(NamedKey::Escape));
        assert!(!page.has_role("dialog", "Sidebar"), "escape dismiss");

        let open = page.by_role("button", "Open drawer");
        page.click(open);
        assert!(page.has_role("dialog", "Sidebar"), "reopens again");
    }
}
