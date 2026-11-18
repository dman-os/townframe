//! The Dayframe skin of [`headless::sheet::Sheet`].
use crate::WidthClass;
use crate::headless::sheet::SheetPlacement;
use crate::headless::sheet::SheetSkin;
use crate::headless::sheet::{self};
use crate::layer::SCRIM;
use dioxus::prelude::*;

// Sheet sizes by width class (`KIT-104`). No panel covers the whole host, so the scrim is always
// reachable: a bottom sheet is 5/6 of the host's height, a side sheet is at most 5/6 of its width.
const BOTTOM: SheetSkin = SheetSkin {
    root: "",
    scrim: SCRIM,
    panel: "h-5/6 w-full overflow-hidden rounded-t-xl bg-surface-raised text-text shadow-raised animate-slide-in-bottom motion-reduce:animate-none",
};
const SIDE_MEDIUM: SheetSkin = SheetSkin {
    root: "",
    scrim: SCRIM,
    panel: "h-full w-96 max-w-5/6 overflow-hidden rounded-l-xl bg-surface-raised text-text shadow-raised animate-slide-in-trailing motion-reduce:animate-none",
};
const SIDE_EXPANDED: SheetSkin = SheetSkin {
    root: "",
    scrim: SCRIM,
    panel: "h-full w-128 max-w-5/6 overflow-hidden rounded-l-xl bg-surface-raised text-text shadow-raised animate-slide-in-trailing motion-reduce:animate-none",
};

/// A dismissable page over its panel: a bottom sheet on a compact host, a side sheet otherwise,
/// sized by the width class.
#[component]
pub fn Sheet(
    width: WidthClass,
    name: String,
    on_dismiss: EventHandler<()>,
    children: Element,
) -> Element {
    let placement = SheetPlacement::for_width(width);
    let skin = match width {
        WidthClass::Compact => BOTTOM,
        WidthClass::Medium => SIDE_MEDIUM,
        WidthClass::Expanded => SIDE_EXPANDED,
    };
    rsx! {
        sheet::Sheet { name, placement, on_dismiss, skin, {children} }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(width: WidthClass) -> Element {
        rsx! {
            Sheet { width, name: "Details", on_dismiss: |_| {}, "body" }
        }
    }

    #[test]
    fn the_skin_follows_the_host_width_class() {
        // (class, placement, size and edge classes, entering animation)
        for (width, placement, size, enter) in [
            (
                WidthClass::Compact,
                "bottom",
                "h-5/6 w-full rounded-t-xl",
                "animate-slide-in-bottom",
            ),
            (
                WidthClass::Medium,
                "side",
                "w-96 rounded-l-xl",
                "animate-slide-in-trailing",
            ),
            (
                WidthClass::Expanded,
                "side",
                "w-128 rounded-l-xl",
                "animate-slide-in-trailing",
            ),
        ] {
            let page = Page::with_props(app, width);
            let dialog = page.by_role("dialog", "Details");
            let root = page.parent(dialog);
            assert_eq!(page.attr(root, "data-value").as_deref(), Some(placement));
            let panel = page.attr(dialog, "class").unwrap();
            for class in size.split(' ').chain([enter, "motion-reduce:animate-none"]) {
                assert!(
                    panel.split(' ').any(|have| have == class),
                    "{width:?}: {class} not in {panel}"
                );
            }
            // The panel never claims the whole host: the scrim is the part left over.
            assert!(
                panel.contains("h-5/6") || panel.contains("max-w-5/6"),
                "{width:?} leaves no scrim to tap: {panel}"
            );
        }
    }

    #[test]
    fn the_scrim_is_the_shared_scrim_role() {
        let page = Page::with_props(app, WidthClass::Compact);
        let scrim = page
            .attr(page.by_role("button", "Dismiss Details"), "class")
            .unwrap();
        assert_eq!(scrim, SCRIM);
        assert!(
            scrim.contains("bg-scrim") && scrim.contains("animate-fade-in"),
            "{scrim}"
        );
    }

    /// An owner as an app writes it: the sheet is mounted while a signal is set, an "Open" button
    /// sets it. Dismissal only counts if the owner's state follows, so this drives the whole loop.
    fn owner(width: WidthClass) -> Element {
        let mut open = use_signal(|| true);
        rsx! {
            button { onclick: move |_| open.set(true), "Open sheet" }
            if open() {
                Sheet { width, name: "Details", on_dismiss: move |()| open.set(false), "body" }
            }
        }
    }

    #[test]
    fn an_owner_can_dismiss_the_sheet_by_scrim_or_escape_and_reopen_it() {
        for width in [WidthClass::Compact, WidthClass::Expanded] {
            let mut page = Page::with_props(owner, width);
            assert!(page.has_role("dialog", "Details"), "{width:?} starts open");

            let scrim = page.by_role("button", "Dismiss Details");
            page.click(scrim);
            assert!(
                !page.has_role("dialog", "Details"),
                "{width:?} scrim dismiss"
            );
            assert!(!page.has_role("button", "Dismiss Details"));

            let open = page.by_role("button", "Open sheet");
            page.click(open);
            assert!(page.has_role("dialog", "Details"), "{width:?} reopens");

            let dialog = page.by_role("dialog", "Details");
            page.key(dialog, Key::Named(NamedKey::Escape));
            assert!(
                !page.has_role("dialog", "Details"),
                "{width:?} escape dismiss"
            );

            let open = page.by_role("button", "Open sheet");
            page.click(open);
            assert!(
                page.has_role("dialog", "Details"),
                "{width:?} reopens again"
            );
        }
    }
}
