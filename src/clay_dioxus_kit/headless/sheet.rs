//! Sheet: a dismissable auxiliary surface that belongs to a panel (pickers, details,
//! confirmations).
//!
//! A bottom sheet while the host is compact, a side sheet otherwise. The sheet fills its nearest
//! positioned ancestor (`position: absolute`, not `fixed`), so the panel that owns it decides
//! what it covers and a specimen can show it inside a frame.
//!
//! # Renderer limits
//!
//! Dismissal paths are the scrim button (every renderer) and Escape (where the renderer
//! delivers key events to the sheet, which needs focus inside it: not on Blitz today). Focus is
//! neither trapped nor restored, because Blitz has no focus API to build either on; the web
//! build needs both before it ships (`KIT-052`).
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
    rsx! {
        div {
            class: skin.root,
            "data-state": "open",
            "data-value": placement.value(),
            style: "position:absolute;top:0;right:0;bottom:0;left:0;z-index:40;display:flex;flex-direction:{direction};justify-content:flex-end",
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
}
