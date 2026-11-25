// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/scrollbarGutter.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the probe is a hast `Element` handed to a `ScrollbarGutterHost`, which mounts it in the host's shadow root, reads `offsetHeight - clientHeight`, and unmounts it (`host.isConnected === false` is the host answering `None`); the module-level `measuredScrollbarGutter` cache is a thread-local. The measured value is the same `Math.max(..., 0)`. `${scrollbarGutter}px` prints a JavaScript number (an integer without a fraction).

use std::cell::Cell;

use crate::constants::DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY;
use crate::constants::DIFFS_SCROLLBAR_MEASURE_ATTRIBUTE;
use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::Properties;

thread_local! {
    static MEASURED_SCROLLBAR_GUTTER: Cell<Option<f64>> = const { Cell::new(None) };
}

/// The shadow root of the diff host, as far as the probe needs it.
pub trait ScrollbarGutterHost {
    /// `false` when `shadowRoot.host` is an `HTMLElement` that is not connected
    /// (nothing can be measured yet).
    fn is_host_connected(&self) -> bool;

    /// Appends `probe`, returns `probe.offsetHeight - probe.clientHeight`, and
    /// removes it again.
    fn measure_probe(&self, probe: &Element) -> f64;
}

// Measures the horizontal scrollbar used by `[data-code]` once per page. The
// hidden probe opts into the same selector as real code panes so custom
// scrollbar CSS is reflected in the measured value.
pub fn get_measured_scrollbar_gutter(host: &impl ScrollbarGutterHost) -> Option<f64> {
    if let Some(measured) = MEASURED_SCROLLBAR_GUTTER.get() {
        return Some(measured);
    }

    if !host.is_host_connected() {
        return None;
    }

    let mut child_properties = Properties::new();
    child_properties.set_style_property("position", "relative");
    child_properties.set_style_property("width", "200%");
    child_properties.set_style_property("height", "200%");
    let child = Element::new("div", child_properties, Vec::new());

    let mut wrapper_properties = Properties::new();
    wrapper_properties.set("data-code", "");
    wrapper_properties.set(DIFFS_SCROLLBAR_MEASURE_ATTRIBUTE, "true");
    let wrapper = Element::new(
        "div",
        wrapper_properties,
        vec![ElementContent::Element(child)],
    );

    let measured = host.measure_probe(&wrapper).max(0.0);
    MEASURED_SCROLLBAR_GUTTER.set(Some(measured));
    Some(measured)
}

pub fn create_measured_scrollbar_gutter_declaration(scrollbar_gutter: Option<f64>) -> String {
    let value = match scrollbar_gutter {
        None => "var(--diffs-scrollbar-gutter-fallback)".to_owned(),
        Some(scrollbar_gutter) => format!("{scrollbar_gutter}px"),
    };
    format!("{DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY}: {value};")
}
