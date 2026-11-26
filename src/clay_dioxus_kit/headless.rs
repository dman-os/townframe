//! The behaviour half of every kit component: state, ARIA, events and the structural layout that
//! makes the behaviour work, with no appearance (`KIT-010`).
//!
//! A headless component renders real elements with their roles, accessible names, `data-state`
//! and listeners, plus only the inline style it cannot work without (flex weights, a gesture
//! overlay's `position: fixed`). Everything a theme decides arrives through the component's
//! `*Skin` prop, a plain struct of class strings, so a second skin (the agentic IDE's, say) is a
//! second set of class strings over the same behaviour. The styled components at the crate root
//! are the claytxt skin and the only callers in this crate.

pub mod alert;
pub mod app_bar;
pub mod button;
pub mod card;
pub mod chip;
pub mod drawer;
pub mod list_details;
pub mod list_row;
pub mod nav_button;
pub mod panel_page;
pub mod scrub_strip;
pub mod sections;
pub mod separator;
pub mod sheet;
pub mod tabs;

use dioxus::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Which way a run of things lies. A contract component carries it as `data-orientation`
/// (`KIT-200`), and a widget with arrow-key navigation maps the matching arrow pair to it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum Orientation {
    #[default]
    Horizontal,
    Vertical,
}

impl Orientation {
    /// The attribute value, for `data-orientation` and `aria-orientation`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Horizontal => "horizontal",
            Self::Vertical => "vertical",
        }
    }
}

/// The semantic axis of `Alert` and `Chip`: what a thing says about itself, not how it looks.
/// A contract component carries it as `data-status`; the colour of each status is the
/// `--ct-color-<status>` role, so a theme recolours every status-carrying component at once.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum Status {
    #[default]
    Info,
    Success,
    Warning,
    Danger,
}

impl Status {
    /// The attribute value, for `data-status`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Success => "success",
            Self::Warning => "warning",
            Self::Danger => "danger",
        }
    }
}

/// Elements that script can focus, by key: the mechanism of roving focus (`Tabs`). A widget
/// registers each focusable part from its `onmounted` and moves focus with [`Self::move_to`], which
/// is, with [`focus_on_mount`], one of the two places that move focus (`KIT-052`). Only a renderer
/// whose elements take focus (`Renderer::takes_focus`) may call it.
#[derive(Clone, Copy)]
pub(crate) struct FocusRing {
    handles: Signal<BTreeMap<String, Rc<MountedData>>>,
}

/// A hook: the ring of one widget instance.
pub(crate) fn use_focus_ring() -> FocusRing {
    FocusRing {
        handles: use_signal(BTreeMap::new),
    }
}

impl FocusRing {
    /// The part keyed `key` has mounted.
    pub(crate) fn register(self, key: &str, event: MountedEvent) {
        let mut handles = self.handles;
        handles.write().insert(key.to_owned(), event.data());
    }

    /// Moves keyboard focus to the part keyed `key`. A key that never mounted is a programming
    /// error: the widget only offers keys it rendered.
    pub(crate) fn move_to(self, key: &str) {
        let handle = self
            .handles
            .peek()
            .get(key)
            .cloned()
            .expect("focus moves only to a part that mounted");
        spawn(async move {
            handle
                .set_focus(true)
                .await
                .expect("the renderer must be able to focus a roving part");
        });
    }
}

/// A process-unique number for an element id that two parts of a widget must agree on
/// (`aria-controls`, `aria-labelledby`). Unique per mounted instance, not stable across runs.
pub(crate) fn use_instance_id() -> usize {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    use_hook(|| NEXT.fetch_add(1, Ordering::Relaxed))
}

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

/// The value of a `part` attribute: the part's own name, then one token per enumerated axis it
/// carries, `<axis>-<value>` (`tabs-tab state-active`, `button variant-outline size-md`). A part
/// is what `::part()` selects by and it takes no attribute selectors, so every `data-state`,
/// `data-variant`, `data-size`, `data-status`, `data-orientation` and similar axis on an element is
/// mirrored here, and an outside stylesheet can select it (ADR 059). An axis that is absent
/// (`None`) adds no token, as its `data-*` attribute is absent. The first token is always the name.
pub fn part(name: &str, axes: &[(&str, Option<&str>)]) -> String {
    let mut value = String::with_capacity(
        name.len() + axes.iter().map(|(axis, v)| v.map_or(0, |v| axis.len() + v.len() + 2)).sum::<usize>(),
    );
    value.push_str(name);
    for (axis, axis_value) in axes {
        if let Some(axis_value) = axis_value {
            value.push(' ');
            value.push_str(axis);
            value.push('-');
            value.push_str(axis_value);
        }
    }
    value
}
