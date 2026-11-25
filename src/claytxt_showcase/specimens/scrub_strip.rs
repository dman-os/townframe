use super::jump_to;
use super::renderer;
use crate::foundations::FoundationSection;
use clay_dioxus_kit::ScrubStrip;
use dioxus::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

const LETTERS: [&str; 26] = [
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
    "T", "U", "V", "W", "X", "Y", "Z",
];

/// `ScrubStrip`: an A to Z list with the strip pinned to the right edge of the window. Press and
/// drag the edge (or tab onto it and use the arrow keys, Home and End) to jump between letters.
/// The window's size is the strip's `viewport`.
#[component]
pub(crate) fn ScrubStripSpecimens(viewport: (f64, f64)) -> Element {
    let anchors = use_context::<Signal<BTreeMap<String, Rc<MountedData>>>>();
    rsx! {
        ScrubStrip {
            viewport,
            items: LETTERS.to_vec(),
            renderer: renderer(),
            label: "Letters".to_string(),
            on_select: move |index: usize| jump_to(anchors, format!("letter-{}", LETTERS[index])),
        }
        for letter in LETTERS {
            FoundationSection {
                key: "{letter}",
                id: format!("letter-{letter}"),
                title: letter.to_string(),
                description: format!("Entries under {letter}."),
                p { "Press the right edge of the window and drag to scrub the letters." }
            }
        }
    }
}
