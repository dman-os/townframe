//! Drawer (`ct-drawer`): the left sheet of a compact host (FDR 006 "on a phone the sidebar is the
//! left drawer").
//!
//! A modal panel that comes from the leading edge over its host, with a scrim over the rest that
//! dismisses it. Unlike [`Sheet`](super::sheet::Sheet), which a caller mounts only while it is
//! shown, the drawer is controlled through `open`: the owner keeps one in its tree and flips the
//! prop, and a closed drawer renders nothing (its content is unmounted, not hidden). It fills its
//! nearest positioned ancestor, like the sheet, so the panel that owns it decides what it covers.
//!
//! Only a compact host shows a drawer (`KIT-101`): a wider host has room for the sidebar beside
//! its content, so the drawer takes the `width` and drops itself there whatever `open` says,
//! rather than leaving every owner to repeat "drawer only when compact". A window that grows while
//! the drawer is open swaps it for the side panel without the owner doing anything.
//!
//! Parts: `scrim` (the dismiss button) and `panel` (the dialog). State: `data-state` (`open`, the
//! only one that renders). Axis: `data-placement` (`leading`). The skin puts scrim and panel on
//! the `drawer` layer together; tapping the scrim dismisses it, because the scrim is the whole
//! area the panel does not cover (`KIT-103`).
//!
//! "Leading" is the left edge: the kit has no right-to-left layout yet (see the README's
//! design-language gaps).
//!
//! # Renderer limits
//!
//! The drawer slides in from the leading edge (a skin animation, `KIT-108`) but does not slide
//! out: an exit would keep the closed drawer mounted until its animation ends, and neither Blitz
//! nor the guest wire reports that, so a closed drawer is gone at once. Dismissal is the scrim button (every renderer), Escape (the panel
//! takes focus on mount, `focus_on_mount`) and whatever the owner
//! wires to `on_dismiss`, such as the platform back gesture. Focus is neither trapped nor
//! restored yet (see `Sheet`); the web build needs both before it ships
//! (`KIT-052`). A swipe from the screen edge is not handled: Blitz touch drags become scroll pans
//! and Android reserves the edge for the system back gesture (the Android findings in the shell
//! ledger).
use crate::WidthClass;
use dioxus::prelude::*;

#[component]
pub fn Drawer(
    width: WidthClass,
    name: String,
    open: bool,
    on_dismiss: EventHandler<()>,
    children: Element,
) -> Element {
    if !open || width.fits_side_by_side() {
        return rsx! {};
    }
    rsx! {
        div {
            class: "ct-drawer",
            "data-state": "open",
            "data-placement": "leading",
            button {
                "part": "drawer-scrim",
                r#type: "button",
                "aria-label": "Dismiss {name}",
                onclick: move |_| on_dismiss.call(()),
            }
            div {
                "part": "drawer-panel",
                role: "dialog",
                "aria-modal": "true",
                "aria-label": "{name}",
                tabindex: "-1",
                onmounted: super::focus_on_mount,
                onkeydown: move |event| {
                    if event.key() == Key::Named(NamedKey::Escape) {
                        event.prevent_default();
                        on_dismiss.call(());
                    }
                },
                {children}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;
    use dioxus_storm::Storm;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        open: bool,
        width: WidthClass,
        dismissals: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let dismissals = probe.dismissals;
        rsx! {
            Drawer {
                width: probe.width,
                name: "Sidebar",
                open: probe.open,
                on_dismiss: move |()| *dismissals.borrow_mut() += 1,
                button { "Home" }
            }
        }
    }

    fn render(open: bool) -> (Page, Rc<RefCell<u32>>) {
        let dismissals = Rc::new(RefCell::new(0));
        let probe = Probe {
            open,
            width: WidthClass::Compact,
            dismissals: Rc::clone(&dismissals),
        };
        (Page::with_props(app, probe), dismissals)
    }

    #[test]
    fn open_is_a_named_modal_dialog_from_the_leading_edge() {
        let (page, _) = render(true);
        let dialog = page.by_role("dialog", "Sidebar");
        assert_eq!(page.attr(dialog, "aria-modal").as_deref(), Some("true"));
        let root = page.by_class("ct-drawer");
        assert_eq!(page.parent(dialog), root);
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("open"));
        assert_eq!(
            page.attr(root, "data-placement").as_deref(),
            Some("leading")
        );
        assert_eq!(page.parts(root), ["drawer-scrim", "drawer-panel"]);
        assert!(page.has_role("button", "Home"));
    }

    #[test]
    fn only_a_compact_host_shows_a_drawer() {
        for (width, shown) in [
            (WidthClass::Compact, true),
            (WidthClass::Medium, false),
            (WidthClass::Expanded, false),
        ] {
            let dismissals = Rc::new(RefCell::new(0));
            let page = Page::with_props(
                app,
                Probe {
                    open: true,
                    width,
                    dismissals,
                },
            );
            assert_eq!(page.has_role("dialog", "Sidebar"), shown, "{width:?}");
            assert_eq!(
                page.has_role("button", "Dismiss Sidebar"),
                shown,
                "{width:?}"
            );
        }
    }

    #[test]
    fn closed_renders_nothing() {
        let (page, _) = render(false);
        assert!(!page.has_role("dialog", "Sidebar"));
        assert!(!page.has_role("button", "Home"));
        assert!(!page.has_role("button", "Dismiss Sidebar"));
    }

    #[test]
    fn the_scrim_dismisses() {
        let (mut page, dismissals) = render(true);
        let scrim = page.by_role("button", "Dismiss Sidebar");
        page.click(scrim);
        assert_eq!(*dismissals.borrow(), 1);
    }

    #[test]
    fn escape_dismisses_and_other_keys_do_not() {
        let (mut page, dismissals) = render(true);
        let dialog = page.by_role("dialog", "Sidebar");
        page.key(dialog, Key::Named(NamedKey::Enter));
        assert_eq!(*dismissals.borrow(), 0);
        page.key(dialog, Key::Named(NamedKey::Escape));
        assert_eq!(*dismissals.borrow(), 1);
    }

    #[test]
    fn the_panel_takes_focus_on_mount_so_escape_works_with_no_focusable_child() {
        fn text_only() -> Element {
            rsx! {
                Drawer { width: WidthClass::Compact, name: "Sidebar", open: true, on_dismiss: |_| {}, p { "Nothing to focus" } }
            }
        }
        let mut page = Page::with_props(|()| text_only(), ());
        let dialog = page.by_role("dialog", "Sidebar");
        assert_eq!(page.attr(dialog, "tabindex").as_deref(), Some("-1"));
        assert_eq!(page.mount(dialog).get(), Some(true));
    }

    fn owner() -> Element {
        let mut open = use_signal(|| false);
        rsx! {
            button { onclick: move |_| open.toggle(), "Toggle" }
            Drawer {
                width: WidthClass::Compact,
                name: "Sidebar",
                open: open(),
                on_dismiss: move |()| open.set(false),
                button { "Inside" }
            }
        }
    }

    /// An owner as an app writes it: `open` is a signal and a button sets it. Dismissal only counts
    /// if the owner's state follows, so this drives the whole loop.
    #[test]
    fn an_owner_can_dismiss_the_drawer_by_scrim_or_escape_and_reopen_it() {
        let mut page = Page::with_props(|()| owner(), ());
        let toggle = page.by_role("button", "Toggle");
        page.click(toggle);
        assert!(page.has_role("dialog", "Sidebar"), "opens");

        let scrim = page.by_role("button", "Dismiss Sidebar");
        page.click(scrim);
        assert!(!page.has_role("dialog", "Sidebar"), "scrim dismiss");

        let toggle = page.by_role("button", "Toggle");
        page.click(toggle);
        assert!(page.has_role("dialog", "Sidebar"), "reopens");

        let dialog = page.by_role("dialog", "Sidebar");
        page.key(dialog, Key::Named(NamedKey::Escape));
        assert!(!page.has_role("dialog", "Sidebar"), "escape dismiss");
    }

    #[test]
    fn a_drawer_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..64);
    }
}
