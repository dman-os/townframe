//! One specimen page per component of the styling contract (`KIT-204`), each showing every
//! variant and state the component has. A page is a set of `FoundationSection`s (so the lab's
//! index strip can jump between them) whose cells are the kit's own components; the only styling
//! here is layout glue (`flex`, `gap`) around them.
//!
//! The pages are what a theme is judged against: every cell is a stable vocabulary
//! (`docs/design-language/styling-contract.md`) that a `user`-layer stylesheet can restyle.
mod alert;
mod button;
mod card;
mod chip;
mod scrub_strip;
mod separator;
mod tabs;

pub(super) use alert::AlertSpecimens;
pub(super) use button::ButtonSpecimens;
pub(super) use card::CardSpecimens;
pub(super) use chip::ChipSpecimens;
pub(super) use scrub_strip::ScrubStripSpecimens;
pub(super) use separator::SeparatorSpecimens;
pub(super) use tabs::TabsSpecimens;

use clay_dioxus_kit::Renderer;
use dioxus::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// The renderer the lab runs under, which decides whether the kit may move focus and set
/// `tabindex` (`KIT-052`): Blitz when `DAYTXT_RENDERER=blitz`, otherwise a browser engine (Wry
/// or the web build).
pub(super) fn renderer() -> Renderer {
    #[cfg(not(target_arch = "wasm32"))]
    if std::env::var("DAYTXT_RENDERER").as_deref() == Ok("blitz") {
        return Renderer::Blitz;
    }
    Renderer::Browser
}

/// A labelled row of cells.
#[component]
fn Row(label: String, children: Element) -> Element {
    rsx! {
        div { class: "flex flex-wrap items-center gap-3",
            span { class: "w-28 shrink-0 text-sm text-foreground-muted", "{label}" }
            {children}
        }
    }
}

/// Scrolls the section registered under `id` into view (the lab's sections register themselves,
/// see `FoundationSection`). A failure to scroll is a bug in the renderer integration.
pub(super) fn jump_to(anchors: Signal<BTreeMap<String, Rc<MountedData>>>, id: String) {
    let target = anchors.peek().get(&id).cloned();
    spawn(async move {
        if let Some(target) = target {
            target
                .scroll_to(ScrollBehavior::Instant)
                .await
                .expect("the renderer must be able to scroll a section into view");
        }
    });
}
