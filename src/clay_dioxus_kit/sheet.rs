//! The Dayframe skin of [`headless::sheet::Sheet`].
use crate::WidthClass;
use crate::headless::sheet::SheetPlacement;
use crate::headless::sheet::SheetSkin;
use crate::headless::sheet::{self};
use dioxus::prelude::*;

// The scrim has no semantic role in the design language yet; `text` at reduced opacity is the
// nearest token-only dimming (see "Design-language gaps" in the README).
const BOTTOM: SheetSkin = SheetSkin {
    root: "",
    scrim: "cursor-default bg-text opacity-40",
    panel: "h-5/6 w-full overflow-hidden rounded-t-xl bg-surface-raised text-text shadow-raised",
};
const SIDE: SheetSkin = SheetSkin {
    root: "",
    scrim: "cursor-default bg-text opacity-40",
    panel: "h-full w-96 max-w-full overflow-hidden rounded-l-xl bg-surface-raised text-text shadow-raised",
};

/// A dismissable page over its panel: a bottom sheet on a compact host, a side sheet otherwise.
#[component]
pub fn Sheet(
    width: WidthClass,
    name: String,
    on_dismiss: EventHandler<()>,
    children: Element,
) -> Element {
    let placement = SheetPlacement::for_width(width);
    let skin = match placement {
        SheetPlacement::Bottom => BOTTOM,
        SheetPlacement::Side => SIDE,
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
        for (width, placement) in [
            (WidthClass::Compact, "bottom"),
            (WidthClass::Medium, "side"),
            (WidthClass::Expanded, "side"),
        ] {
            let page = Page::with_props(app, width);
            let dialog = page.by_role("dialog", "Details");
            let root = page.parent(dialog);
            assert_eq!(page.attr(root, "data-value").as_deref(), Some(placement));
            let panel_class = page.attr(dialog, "class").unwrap();
            assert_eq!(panel_class.contains("rounded-t-xl"), placement == "bottom");
            assert_eq!(panel_class.contains("rounded-l-xl"), placement == "side");
        }
    }
}
