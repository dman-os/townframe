use crate::foundations::FoundationSection;
use crate::specimens::Row;
use clay_dioxus_kit::Chip;
use clay_dioxus_kit::ChipVariant;
use clay_dioxus_kit::Status;
use dioxus::prelude::*;

/// `Chip`: static labels in both variants and every status, filter chips that toggle, and tags
/// that can be removed.
#[component]
pub fn ChipSpecimens() -> Element {
    let mut filters = use_signal(|| [true, false, false]);
    let mut tags = use_signal(|| vec!["rust", "dioxus", "css"]);
    rsx! {
        FoundationSection {
            id: "chip-statuses".to_string(),
            title: "Variants and statuses".to_string(),
            description: "data-variant filled|outline and data-status (absent is neutral).".to_string(),
            div { class: "grid gap-3",
                for variant in ChipVariant::ALL {
                    Row { key: "{variant.as_str()}", label: variant.as_str().to_string(),
                        Chip { label: "neutral".to_string(), variant }
                        for (status , name) in [
                            (Status::Info, "info"),
                            (Status::Success, "success"),
                            (Status::Warning, "warning"),
                            (Status::Danger, "danger"),
                        ] {
                            Chip { key: "{name}", label: name.to_string(), variant, status: Some(status) }
                        }
                    }
                }
            }
        }
        FoundationSection {
            id: "chip-interactive".to_string(),
            title: "Toggle and remove".to_string(),
            description: "A toggle chip is a button with aria-pressed and data-state on|off; a removable chip has a remove part named for what it removes.".to_string(),
            div { class: "grid gap-3",
                Row { label: "toggle".to_string(),
                    for (index , name) in ["Unread", "Starred", "Mine"].into_iter().enumerate() {
                        Chip {
                            key: "{name}",
                            label: name.to_string(),
                            variant: ChipVariant::Outline,
                            selected: Some(filters()[index]),
                            on_toggle: move |next| filters.write()[index] = next,
                        }
                    }
                }
                Row { label: "removable".to_string(),
                    for tag in tags() {
                        Chip {
                            key: "{tag}",
                            label: tag.to_string(),
                            status: Some(Status::Info),
                            on_remove: move |()| tags.write().retain(|kept| *kept != tag),
                        }
                    }
                }
            }
        }
    }
}
