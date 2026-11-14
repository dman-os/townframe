//! Sheet: a dismissable auxiliary surface that belongs to a panel (pickers, details,
//! confirmations).
//!
//! A bottom sheet while the host is compact, a side sheet otherwise. The sheet fills its nearest
//! positioned ancestor (`position: absolute`, not `fixed`), so the panel that owns it decides
//! what it covers and a specimen can show it inside a frame.
//!
//! It sits on [`Layer::Sheet`], scrim and panel together (see `layer.rs`). The scrim is whatever
//! the panel does not cover and tapping it dismisses the sheet (`KIT-103`), so a panel must leave
//! some of the host uncovered: the skin sizes it to.
//!
//! # Renderer limits
//!
//! The sheet rises (bottom) or slides in (side) with a skin animation (`KIT-108`) but has no
//! exit animation: that would keep the dismissed sheet mounted until the animation ends, and
//! neither Blitz nor the guest wire reports it. Dismissal paths are the scrim button (every renderer) and Escape (where the renderer
//! delivers key events to the sheet). The panel takes focus on mount (`focus_on_mount`, so Escape
//! works with no focusable child) but focus is neither trapped nor restored; the web
//! build needs both before it ships (`KIT-052`).
use crate::Layer;
use crate::WidthClass;
use dioxus::prelude::*;

/// Where the sheet sits.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SheetPlacement {
    Bottom,
    Side,
}

impl SheetPlacement {
    /// The placement for a host width: the one place that decides it.
    pub fn for_width(width: WidthClass) -> Self {
        if width.fits_side_by_side() {
            Self::Side
        } else {
            Self::Bottom
        }
    }

    fn value(self) -> &'static str {
        match self {
            Self::Bottom => "bottom",
            Self::Side => "side",
        }
    }
}

/// The classes a skin gives each part of [`Sheet`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SheetSkin {
    pub root: &'static str,
    pub scrim: &'static str,
    pub panel: &'static str,
}

#[component]
pub fn Sheet(
    name: String,
    placement: SheetPlacement,
    on_dismiss: EventHandler<()>,
    #[props(default)] skin: SheetSkin,
    children: Element,
) -> Element {
    let direction = match placement {
        SheetPlacement::Bottom => "column",
        SheetPlacement::Side => "row",
    };
    let layer = Layer::Sheet.css();
    rsx! {
        div {
            class: skin.root,
            "data-state": "open",
            "data-value": placement.value(),
            position: "absolute",
            top: "0",
            right: "0",
            bottom: "0",
            left: "0",
            z_index: "{layer}",
            display: "flex",
            flex_direction: "{direction}",
            justify_content: "flex-end",
            button {
                class: skin.scrim,
                r#type: "button",
                "aria-label": "Dismiss {name}",
                position: "absolute",
                top: "0",
                right: "0",
                bottom: "0",
                left: "0",
                border: "0",
                padding: "0",
                onclick: move |_| on_dismiss.call(()),
            }
            div {
                class: skin.panel,
                role: "dialog",
                "aria-modal": "true",
                "aria-label": "{name}",
                position: "relative",
                min_height: "0",
                min_width: "0",
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
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone)]
    struct Probe {
        placement: SheetPlacement,
        dismissals: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let dismissals = probe.dismissals;
        rsx! {
            Sheet {
                name: "Pick a file",
                placement: probe.placement,
                on_dismiss: move |()| *dismissals.borrow_mut() += 1,
                button { "Choose" }
            }
        }
    }

    fn render(placement: SheetPlacement) -> (Page, Rc<RefCell<u32>>) {
        let dismissals = Rc::new(RefCell::new(0));
        let probe = Probe {
            placement,
            dismissals: Rc::clone(&dismissals),
        };
        (Page::with_props(app, probe), dismissals)
    }

    #[test]
    fn compact_hosts_get_a_bottom_sheet_and_wider_hosts_a_side_sheet() {
        assert_eq!(
            SheetPlacement::for_width(WidthClass::Compact),
            SheetPlacement::Bottom
        );
        assert_eq!(
            SheetPlacement::for_width(WidthClass::Medium),
            SheetPlacement::Side
        );
        assert_eq!(
            SheetPlacement::for_width(WidthClass::Expanded),
            SheetPlacement::Side
        );
    }

    #[test]
    fn is_a_named_modal_dialog_with_its_content() {
        let (page, _) = render(SheetPlacement::Bottom);
        let dialog = page.by_role("dialog", "Pick a file");
        assert_eq!(page.attr(dialog, "aria-modal").as_deref(), Some("true"));
        assert!(page.has_role("button", "Choose"));
    }

    #[test]
    fn placement_is_visible_to_inspection() {
        for (placement, value) in [
            (SheetPlacement::Bottom, "bottom"),
            (SheetPlacement::Side, "side"),
        ] {
            let (page, _) = render(placement);
            let dialog = page.by_role("dialog", "Pick a file");
            assert_eq!(
                page.attr(page.parent(dialog), "data-value").as_deref(),
                Some(value)
            );
        }
    }

    #[test]
    fn the_overlay_is_on_the_sheet_layer_above_the_drawer() {
        let (page, _) = render(SheetPlacement::Bottom);
        let root = page.parent(page.by_role("dialog", "Pick a file"));
        assert_eq!(
            page.style(root, "z-index").as_deref(),
            Some(Layer::Sheet.css())
        );
        assert!(Layer::Sheet > Layer::Drawer);
    }

    #[test]
    fn the_scrim_dismisses() {
        let (mut page, dismissals) = render(SheetPlacement::Side);
        let scrim = page.by_role("button", "Dismiss Pick a file");
        page.click(scrim);
        assert_eq!(*dismissals.borrow(), 1);
    }

    #[test]
    fn escape_dismisses_and_other_keys_do_not() {
        let (mut page, dismissals) = render(SheetPlacement::Bottom);
        let dialog = page.by_role("dialog", "Pick a file");
        page.key(dialog, Key::Named(NamedKey::Enter));
        assert_eq!(*dismissals.borrow(), 0);
        page.key(dialog, Key::Named(NamedKey::Escape));
        assert_eq!(*dismissals.borrow(), 1);
    }

    #[test]
    fn the_panel_takes_focus_on_mount_so_escape_works_with_no_focusable_child() {
        // The shape of the lab's specimen: a panel holding only text.
        fn text_only() -> Element {
            rsx! {
                Sheet {
                    name: "Pick a file",
                    placement: SheetPlacement::Side,
                    on_dismiss: |_| {},
                    p { "Nothing to focus in here" }
                }
            }
        }
        let mut page = Page::with_props(|()| text_only(), ());
        let dialog = page.by_role("dialog", "Pick a file");
        assert_eq!(page.attr(dialog, "tabindex").as_deref(), Some("-1"));
        assert_eq!(page.mount(dialog).get(), Some(true));
    }
}

/// Every listener fed every event kind in seeded random order (see `dioxus_storm`): a panic in
/// any order of events is a bug, and a seed that found one stays here as a regression case.
#[cfg(test)]
mod storm {
    use super::*;
    use dioxus_storm::Storm;

    fn owner() -> Element {
        let mut open = use_signal(|| true);
        rsx! {
            button { onclick: move |_| open.set(true), "Open" }
            if open() {
                Sheet {
                    name: "Pick",
                    placement: SheetPlacement::Bottom,
                    on_dismiss: move |()| open.set(false),
                    button { "Choose" }
                }
            }
        }
    }

    #[test]
    fn a_sheet_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..64);
    }
}
