use crate::foundations::FoundationSection;
use crate::specimens::Frame;
use crate::specimens::EXPANDED_PX;
use clay_dioxus_kit::ListDetails;
use clay_dioxus_kit::ListRow;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Split;
use clay_dioxus_kit::WidthClass;
use dioxus::prelude::*;

const ROWS: [&str; 4] = ["Appearance", "Sync", "Micros", "About"];

/// `ListRow`: an openable row of a list, shown in the list of a `ListDetails` (the row opens
/// itself through the scaffold, so it cannot be mounted outside one).
#[component]
pub fn ListRowSpecimens() -> Element {
    let mut open = use_signal(|| Some("Sync".to_owned()));
    let mut weights = use_signal(|| vec![1.0, 2.0]);
    rsx! {
        FoundationSection {
            id: "list-row-states".to_string(),
            title: "States".to_string(),
            description: "data-state current|idle, and aria-current on the open row, which takes the selected colour roles. Press a row to open it.".to_string(),
            Frame { title: "ListRow".to_string(), width_px: EXPANDED_PX,
                ListDetails {
                    width: WidthClass::from_px(EXPANDED_PX),
                    list_name: "Settings",
                    list: rsx! {
                        for title in ROWS {
                            ListRow { key: "{title}", id: title, name: "Open {title}", "{title}" }
                        }
                    },
                    details_name: "Section",
                    details: Some(rsx! {
                        p { class: "p-4 text-foreground-muted",
                            "Open: {open().unwrap_or_default()}."
                        }
                    }),
                    current: open(),
                    split: Split::new(weights(), EXPANDED_PX)
                        .rule(0, SizeRule { min: 160.0, ..SizeRule::default() }),
                    on_resize: move |next| weights.set(next),
                    on_open: move |key| open.set(Some(key)),
                    on_back: move |()| open.set(None),
                }
            }
        }
    }
}
