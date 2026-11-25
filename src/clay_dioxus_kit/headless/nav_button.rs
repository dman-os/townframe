//! The navigation rack's controls (`ct-nav-button`; ADR 058): one control for one thing, never a
//! button that changes meaning; [`NavRack`](crate::NavRack) groups the ones a host places
//! together.
//!
//! Parts: `icon` (decorative, `aria-hidden`), which holds the `glyph` of back and
//! forward or the three `bar`s of the sidebar toggle. State: `data-kind` (`sidebar`|`back`|
//! `forward`) and `data-disabled`. The glyphs are drawn by the stylesheet from boxes in
//! `currentColor`; the accessible name is the kind's label, so the icon adds none.
//!
//! # Renderer limits
//!
//! * Blitz has no keyboard focus today, so the button is reachable by pointer only there. It is a
//!   native `<button>` and sets no `tabindex`, which would panic Blitz (`KIT-052`); a renderer
//!   that has focus makes it keyboard-activatable with no change here.
use dioxus::prelude::*;

/// What a button does, which decides its accessible name and its glyph.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NavButtonKind {
    /// Shows the sidebar.
    Sidebar,
    /// Hides the sidebar: the same glyph, for the control that sits in the sidebar's own header
    /// while it is shown.
    HideSidebar,
    /// Steps back through where the user has been.
    Back,
    /// Steps forward over what back left.
    Forward,
}

impl NavButtonKind {
    /// The accessible name. Fixed per kind so every mount of the control reads the same to
    /// assistive technology and to tests (`KIT-050`).
    pub fn label(self) -> &'static str {
        match self {
            Self::Sidebar => "Open sidebar",
            Self::HideSidebar => "Hide sidebar",
            Self::Back => "Back",
            Self::Forward => "Forward",
        }
    }

    fn data_kind(self) -> &'static str {
        match self {
            Self::Sidebar | Self::HideSidebar => "sidebar",
            Self::Back => "back",
            Self::Forward => "forward",
        }
    }
}

/// A native `<button>`: focusable and activatable by keyboard wherever the renderer has focus,
/// and clickable everywhere (`KIT-052`; Blitz has no keyboard focus today). A disabled one stays
/// in the rack, so the rack keeps its shape, and ignores its click whatever the renderer does.
#[component]
pub fn NavButton(
    kind: NavButtonKind,
    #[props(default)] disabled: bool,
    onclick: EventHandler<()>,
) -> Element {
    rsx! {
        button {
            class: "ct-nav-button",
            r#type: "button",
            "aria-label": kind.label(),
            "data-kind": kind.data_kind(),
            "data-disabled": if disabled { Some("") } else { None },
            disabled,
            onclick: move |_| {
                if !disabled {
                    onclick.call(());
                }
            },
            span { "data-part": "icon", "aria-hidden": "true",
                match kind {
                    NavButtonKind::Sidebar | NavButtonKind::HideSidebar => rsx! {
                        for index in 0..3 {
                            span { key: "{index}", "data-part": "bar" }
                        }
                    },
                    NavButtonKind::Back | NavButtonKind::Forward => rsx! {
                        span { "data-part": "glyph" }
                    },
                }
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
        kind: NavButtonKind,
        disabled: bool,
        clicks: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let clicks = probe.clicks;
        rsx! {
            NavButton {
                kind: probe.kind,
                disabled: probe.disabled,
                onclick: move |()| *clicks.borrow_mut() += 1,
            }
        }
    }

    fn render(kind: NavButtonKind, disabled: bool) -> (Page, Rc<RefCell<u32>>) {
        let clicks = Rc::new(RefCell::new(0));
        let probe = Probe {
            kind,
            disabled,
            clicks: Rc::clone(&clicks),
        };
        (Page::with_props(app, probe), clicks)
    }

    #[test]
    fn each_kind_is_named_for_what_it_does() {
        for kind in [
            NavButtonKind::Sidebar,
            NavButtonKind::HideSidebar,
            NavButtonKind::Back,
            NavButtonKind::Forward,
        ] {
            let (page, _) = render(kind, false);
            assert!(page.has_role("button", kind.label()));
        }
        assert_eq!(NavButtonKind::Sidebar.label(), "Open sidebar");
        assert_eq!(NavButtonKind::HideSidebar.label(), "Hide sidebar");
    }

    #[test]
    fn the_button_names_its_vocabulary_and_draws_its_glyph_from_parts() {
        for (kind, data_kind, glyph) in [
            (NavButtonKind::Sidebar, "sidebar", vec!["bar", "bar", "bar"]),
            (
                NavButtonKind::HideSidebar,
                "sidebar",
                vec!["bar", "bar", "bar"],
            ),
            (NavButtonKind::Back, "back", vec!["glyph"]),
            (NavButtonKind::Forward, "forward", vec!["glyph"]),
        ] {
            let (page, _) = render(kind, false);
            let button = page.by_class("ct-nav-button");
            assert_eq!(page.attr(button, "type").as_deref(), Some("button"));
            assert_eq!(page.attr(button, "data-kind").as_deref(), Some(data_kind));
            assert_eq!(page.attr(button, "data-disabled"), None);
            assert_eq!(page.parts(button), ["icon"]);
            let icon = page.children(button)[0];
            assert_eq!(page.attr(icon, "aria-hidden").as_deref(), Some("true"));
            assert_eq!(page.parts(icon), glyph);
            // The glyph adds no accessible name of its own.
            assert_eq!(page.text(button), "");
        }
    }

    #[test]
    fn click_reports_once_per_press() {
        let (mut page, clicks) = render(NavButtonKind::Sidebar, false);
        let button = page.by_role("button", "Open sidebar");
        page.click(button);
        page.click(button);
        assert_eq!(*clicks.borrow(), 2);
    }

    #[test]
    fn a_disabled_button_stays_named_and_ignores_its_click() {
        let (mut page, clicks) = render(NavButtonKind::Forward, true);
        let button = page.by_role("button", "Forward");
        assert_eq!(page.attr(button, "data-disabled").as_deref(), Some(""));
        page.click(button);
        assert_eq!(*clicks.borrow(), 0);
    }

    fn owner() -> Element {
        let mut kind = use_signal(|| NavButtonKind::Sidebar);
        rsx! {
            NavButton {
                kind: kind(),
                onclick: move |()| {
                    kind.set(match kind() {
                        NavButtonKind::Sidebar => NavButtonKind::HideSidebar,
                        NavButtonKind::HideSidebar => NavButtonKind::Back,
                        NavButtonKind::Back => NavButtonKind::Forward,
                        NavButtonKind::Forward => NavButtonKind::Sidebar,
                    });
                },
            }
        }
    }

    #[test]
    fn a_nav_button_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..16);
    }
}
