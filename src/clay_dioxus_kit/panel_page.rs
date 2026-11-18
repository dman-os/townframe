//! A panel's page: its app bar, pinned, over a body that scrolls.
use dioxus::prelude::*;

/// The page of a panel micro: `app_bar` pinned at the top and `children` in a body that scrolls
/// within the panel. A micro's presentation root is exactly its pane's size and clips what
/// overflows it (`overflow: clip`, the Clay mount contract), so a body taller than the panel
/// scrolls here or nowhere. The page fills the root and the body takes the height the bar leaves
/// (`min-height: 0`, or a flex column would grow to its content instead of scrolling).
///
/// The layout is per-property style rather than utility classes: it is the mechanism every
/// panel's scrolling rests on, not a design token, and it must hold whatever sheet a micro is
/// given. (A guest's wire carries style per property, never a raw `style` attribute.)
#[component]
pub fn PanelPage(app_bar: Element, children: Element) -> Element {
    rsx! {
        div {
            display: "flex",
            flex_direction: "column",
            height: "100%",
            min_height: "0",
            {app_bar}
            div { flex: "1 1 auto", min_height: "0", overflow_y: "auto", {children} }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(_: ()) -> Element {
        rsx! {
            PanelPage { app_bar: rsx! { h1 { "Title" } },
                p { role: "status", "Body" }
            }
        }
    }

    #[test]
    fn the_body_scrolls_below_the_bar_and_the_bar_stays_outside_it() {
        let page = Page::with_props(app, ());
        let body = page.parent(page.by_role("status", "Body"));
        assert_eq!(page.style(body, "overflow-y").as_deref(), Some("auto"));
        assert_eq!(page.style(body, "min-height").as_deref(), Some("0"));
        let frame = page.parent(body);
        assert_eq!(page.style(frame, "height").as_deref(), Some("100%"));
        assert_eq!(
            page.parent(page.by_role("heading", "Title")),
            frame,
            "the bar sits beside the scrolling body, not inside it"
        );
    }
}
