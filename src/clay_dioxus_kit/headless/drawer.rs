//! Drawer: the left sheet of a compact host (FDR 006 "on a phone the sidebar is the left
//! drawer").
//!
//! A modal panel that comes from the leading edge over its host, with a scrim over the rest that
//! dismisses it. Unlike [`Sheet`](super::sheet::Sheet), which a caller mounts only while it is
//! shown, the drawer is controlled through `open`: the owner keeps one in its tree and flips the
//! prop, and a closed drawer renders nothing (its content is unmounted, not hidden). It fills its
//! nearest positioned ancestor, like the sheet, so the panel that owns it decides what it covers.
//!
//! "Leading" is the left edge: the kit has no right-to-left layout yet (see the README's
//! design-language gaps).
//!
//! # Renderer limits
//!
//! There is no slide: the design language has no motion tokens (`INTERACTION-004`), so the drawer
//! appears and disappears. Dismissal is the scrim button (every renderer), Escape (where the
//! renderer delivers key events inside the drawer: not on Blitz today) and whatever the owner
//! wires to `on_dismiss`, such as the platform back gesture. Focus is neither trapped nor
//! restored, for the reason documented on `Sheet`; the web build needs both before it ships
//! (`KIT-052`). A swipe from the screen edge is not handled: Blitz touch drags become scroll pans
//! and Android reserves the edge for the system back gesture (the Android findings in the shell
//! ledger).
use dioxus::prelude::*;

/// The classes a skin gives each part of [`Drawer`]. The panel's width belongs here.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DrawerSkin {
    pub root: &'static str,
    pub scrim: &'static str,
    pub panel: &'static str,
}

#[component]
pub fn Drawer(
    name: String,
    open: bool,
    on_dismiss: EventHandler<()>,
    #[props(default)] skin: DrawerSkin,
    children: Element,
) -> Element {
    if !open {
        return rsx! {};
    }
    rsx! {
        div {
            class: skin.root,
            "data-state": "open",
            "data-value": "leading",
            style: "position:absolute;top:0;right:0;bottom:0;left:0;z-index:40;display:flex;flex-direction:row;justify-content:flex-start",
            button {
                class: skin.scrim,
                r#type: "button",
                "aria-label": "Dismiss {name}",
                style: "position:absolute;top:0;right:0;bottom:0;left:0;border:0;padding:0",
                onclick: move |_| on_dismiss.call(()),
            }
            div {
                class: skin.panel,
                role: "dialog",
                "aria-modal": "true",
                "aria-label": "{name}",
                style: "position:relative;min-height:0;min-width:0",
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
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        open: bool,
        dismissals: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let dismissals = probe.dismissals;
        rsx! {
            Drawer {
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
            dismissals: Rc::clone(&dismissals),
        };
        (Page::with_props(app, probe), dismissals)
    }

    #[test]
    fn open_is_a_named_modal_dialog_from_the_leading_edge() {
        let (page, _) = render(true);
        let dialog = page.by_role("dialog", "Sidebar");
        assert_eq!(page.attr(dialog, "aria-modal").as_deref(), Some("true"));
        assert_eq!(
            page.attr(page.parent(dialog), "data-value").as_deref(),
            Some("leading")
        );
        assert!(page.has_role("button", "Home"));
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
}
