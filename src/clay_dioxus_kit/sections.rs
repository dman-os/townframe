//! The Dayframe skin of [`headless::sections::Sections`]. See that module for behaviour and
//! renderer limits.
use crate::headless::sections::SectionPane;
use crate::headless::sections::SectionsSkin;
use crate::headless::sections::Split;
use crate::headless::sections::{self};
use dioxus::prelude::*;

pub(crate) const SKIN: SectionsSkin = SectionsSkin {
    root: "bg-canvas text-text",
    pane: "bg-canvas",
    handle: "group flex cursor-col-resize justify-center focus-visible:outline-2 focus-visible:outline-focus",
    grip: "h-full w-px bg-border group-hover:w-0.5 group-hover:bg-primary group-data-[state=dragging]:w-0.5 group-data-[state=dragging]:bg-primary",
};

/// A run of panes with draggable handles between them; the layout is controlled through `split`.
#[component]
pub fn Sections(
    label: String,
    panes: Vec<SectionPane>,
    split: Split,
    on_resize: EventHandler<Vec<f64>>,
) -> Element {
    rsx! {
        sections::Sections { label, panes, split, on_resize, skin: SKIN }
    }
}
