//! Sheet (`ct-sheet`): a dismissable auxiliary surface that belongs to a panel (pickers, details,
//! confirmations).
//!
//! A bottom sheet while the host is compact, a side sheet otherwise: the host's width class
//! decides. Parts: `scrim` (the dismiss button) and `panel` (the dialog). State: `data-state`
//! (`open`; a sheet exists only while it is shown). Axes: `data-placement` (`bottom`|`side`) and
//! `data-width` (`compact`|`medium`|`expanded`, the class itself, which sizes a side sheet). The
//! sheet fills its nearest positioned ancestor (the skin positions it `absolute`, not `fixed`), so
//! the panel that owns it decides what it covers and a specimen can show it inside a frame.
//!
//! The skin puts scrim and panel on the `sheet` layer together. The scrim is whatever the panel
//! does not cover and tapping it dismisses the sheet (`KIT-103`), so a skin must leave some of the
//! host uncovered.
//!
//! # Renderer limits
//!
//! The sheet rises (bottom) or slides in (side) with a skin animation (`KIT-108`) but has no
//! exit animation: that would keep the dismissed sheet mounted until the animation ends, and
//! neither Blitz nor the guest wire reports it. Dismissal paths are the scrim button (every renderer) and Escape (where the renderer
//! delivers key events to the sheet). The panel takes focus on mount (`focus_on_mount`, so Escape
//! works with no focusable child) but focus is neither trapped nor restored; the web
//! build needs both before it ships (`KIT-052`).
use crate::WidthClass;
use super::part;
use dioxus::prelude::*;

/// Where the sheet sits.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SheetPlacement {
    Bottom,
    Side,
}

impl SheetPlacement {
    /// The placement for a host width: the one place that decides it.
    fn for_width(width: WidthClass) -> Self {
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

#[component]
pub fn Sheet(
    width: WidthClass,
    name: String,
    on_dismiss: EventHandler<()>,
    children: Element,
) -> Element {
    let placement = SheetPlacement::for_width(width);
    rsx! {
        div {
            class: "ct-sheet",
            "part": part(
                "sheet",
                &[
                    ("state", Some("open")),
                    ("placement", Some(placement.value())),
                    ("width", Some(width.as_str())),
                ],
            ),
            "data-state": "open",
            "data-placement": placement.value(),
            "data-width": width.as_str(),
            button {
                "part": "sheet-scrim",
                r#type: "button",
                "aria-label": "Dismiss {name}",
                onclick: move |_| on_dismiss.call(()),
            }
            div {
                "part": "sheet-panel",
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
        width: WidthClass,
        dismissals: Rc<RefCell<u32>>,
    }

    fn app(probe: Probe) -> Element {
        let dismissals = probe.dismissals;
        rsx! {
            Sheet {
                name: "Pick a file",
                width: probe.width,
                on_dismiss: move |()| *dismissals.borrow_mut() += 1,
                button { "Choose" }
            }
        }
    }

    fn render(width: WidthClass) -> (Page, Rc<RefCell<u32>>) {
        let dismissals = Rc::new(RefCell::new(0));
        let probe = Probe {
            width,
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
        let (page, _) = render(WidthClass::Compact);
        let dialog = page.by_role("dialog", "Pick a file");
        assert_eq!(page.attr(dialog, "aria-modal").as_deref(), Some("true"));
        assert!(page.has_role("button", "Choose"));
    }

    #[test]
    fn the_sheet_names_its_vocabulary_for_every_width_class() {
        for (width, placement) in [
            (WidthClass::Compact, "bottom"),
            (WidthClass::Medium, "side"),
            (WidthClass::Expanded, "side"),
        ] {
            let (page, _) = render(width);
            let root = page.by_class("ct-sheet");
            assert_eq!(page.attr(root, "data-state").as_deref(), Some("open"));
            assert_eq!(
                page.attr(root, "data-placement").as_deref(),
                Some(placement)
            );
            assert_eq!(
                page.attr(root, "data-width").as_deref(),
                Some(width.as_str())
            );
            assert_eq!(page.parts(root), ["sheet-scrim", "sheet-panel"]);
            assert_eq!(page.parent(page.by_role("dialog", "Pick a file")), root);
            assert_eq!(
                page.parent(page.by_role("button", "Dismiss Pick a file")),
                root
            );
        }
    }

    #[test]
    fn the_scrim_dismisses() {
        let (mut page, dismissals) = render(WidthClass::Expanded);
        let scrim = page.by_role("button", "Dismiss Pick a file");
        page.click(scrim);
        assert_eq!(*dismissals.borrow(), 1);
    }

    #[test]
    fn escape_dismisses_and_other_keys_do_not() {
        let (mut page, dismissals) = render(WidthClass::Compact);
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
                    width: WidthClass::Expanded,
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

    fn owner() -> Element {
        let mut open = use_signal(|| true);
        rsx! {
            button { onclick: move |_| open.set(true), "Open" }
            if open() {
                Sheet {
                    name: "Pick",
                    width: WidthClass::Compact,
                    on_dismiss: move |()| open.set(false),
                    button { "Choose" }
                }
            }
        }
    }

    /// An owner as an app writes it: the sheet is mounted while a signal is set, an "Open" button
    /// sets it. Dismissal only counts if the owner's state follows, so this drives the whole loop.
    #[test]
    fn an_owner_can_dismiss_the_sheet_by_scrim_or_escape_and_reopen_it() {
        let mut page = Page::with_props(|()| owner(), ());
        assert!(page.has_role("dialog", "Pick"), "starts open");

        let scrim = page.by_role("button", "Dismiss Pick");
        page.click(scrim);
        assert!(!page.has_role("dialog", "Pick"), "scrim dismiss");
        assert!(!page.has_role("button", "Dismiss Pick"));

        let open = page.by_role("button", "Open");
        page.click(open);
        assert!(page.has_role("dialog", "Pick"), "reopens");

        let dialog = page.by_role("dialog", "Pick");
        page.key(dialog, Key::Named(NamedKey::Escape));
        assert!(!page.has_role("dialog", "Pick"), "escape dismiss");

        let open = page.by_role("button", "Open");
        page.click(open);
        assert!(page.has_role("dialog", "Pick"), "reopens again");
    }

    #[test]
    fn a_sheet_survives_any_order_of_events() {
        Storm::of(owner).run_seeds(0..64);
    }
}
