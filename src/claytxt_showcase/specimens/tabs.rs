use super::renderer;
use crate::foundations::FoundationSection;
use clay_dioxus_kit::Orientation;
use clay_dioxus_kit::TabItem;
use clay_dioxus_kit::TabPanel;
use clay_dioxus_kit::Tabs;
use dioxus::prelude::*;

fn sections() -> Vec<TabItem> {
    vec![
        TabItem::new("overview", "Overview"),
        TabItem::new("activity", "Activity").disabled(),
        TabItem::new("files", "Files"),
        TabItem::new("settings", "Settings"),
    ]
}

/// `Tabs`: a horizontal and a vertical list, each with a disabled tab. Arrow keys, Home and End
/// move between the enabled tabs and select as they go.
#[component]
pub(crate) fn TabsSpecimens() -> Element {
    let mut horizontal = use_signal(|| "overview".to_string());
    let mut vertical = use_signal(|| "files".to_string());
    rsx! {
        FoundationSection {
            id: "tabs-horizontal".to_string(),
            title: "Horizontal".to_string(),
            description: "role=tablist, data-part list|tab|panel, data-state active|inactive. Left and Right arrows, Home and End move and select; the disabled tab is skipped.".to_string(),
            Tabs {
                items: sections(),
                value: horizontal(),
                on_value_change: move |next| horizontal.set(next),
                label: "Horizontal sections".to_string(),
                renderer: renderer(),
                for item in sections() {
                    TabPanel { key: "{item.value}", value: item.value.clone(), p { "The {item.label} panel." } }
                }
            }
        }
        FoundationSection {
            id: "tabs-vertical".to_string(),
            title: "Vertical".to_string(),
            description: "data-orientation=vertical: Up and Down arrows.".to_string(),
            Tabs {
                items: sections(),
                value: vertical(),
                on_value_change: move |next| vertical.set(next),
                label: "Vertical sections".to_string(),
                orientation: Orientation::Vertical,
                renderer: renderer(),
                for item in sections() {
                    TabPanel { key: "{item.value}", value: item.value.clone(), p { "The {item.label} panel." } }
                }
            }
        }
    }
}
