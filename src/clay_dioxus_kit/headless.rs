//! The behaviour half of every kit component: state, ARIA, events and the structural layout that
//! makes the behaviour work, with no appearance (`KIT-010`).
//!
//! A headless component renders real elements with their roles, accessible names, `data-state`
//! and listeners, plus only the inline style it cannot work without (flex weights, a gesture
//! overlay's `position: fixed`). Everything a theme decides arrives through the component's
//! `*Skin` prop, a plain struct of class strings, so a second skin (the agentic IDE's, say) is a
//! second set of class strings over the same behaviour. The styled components at the crate root
//! are the Dayframe skin and the only callers in this crate.

pub mod app_bar;
pub mod drawer;
pub mod list_details;
pub mod list_row;
pub mod nav_button;
pub mod sections;
pub mod sheet;

use dioxus::prelude::*;

/// The `onmounted` handler of a modal panel: moves keyboard focus into it, as the WAI-ARIA dialog
/// pattern asks, so Escape reaches the panel's `onkeydown` even when it holds no focusable child.
/// The panel carries `tabindex="-1"` (focusable by script, not a tab stop). A renderer that
/// cannot focus an element faults here instead of leaving a dialog that ignores the keyboard.
/// Focus is neither trapped inside the panel nor restored to the opener on dismissal yet.
pub(crate) fn focus_on_mount(event: MountedEvent) {
    spawn(async move {
        event
            .data()
            .set_focus(true)
            .await
            .expect("the renderer must be able to focus a modal panel");
    });
}
