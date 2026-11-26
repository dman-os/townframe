use crate::foundations::FoundationSection;
use clay_dioxus_kit::Alert;
use clay_dioxus_kit::Status;
use dioxus::prelude::*;

const STATUSES: [(Status, &str); 4] = [
    (Status::Info, "Info"),
    (Status::Success, "Success"),
    (Status::Warning, "Warning"),
    (Status::Danger, "Danger"),
];

/// `Alert`: every status with a title and without, and dismissible ones that can be brought back.
#[component]
pub fn AlertSpecimens() -> Element {
    let mut dismissed = use_signal(Vec::<Status>::new);
    rsx! {
        FoundationSection {
            id: "alert-statuses".to_string(),
            title: "Statuses".to_string(),
            description: "data-status info|success|warning|danger. Danger and warning are role=alert, info and success role=status. The fill is the status's container role, with no border; the icon carries the status.".to_string(),
            div { class: "grid gap-3",
                for (status , name) in STATUSES {
                    Alert { key: "{name}", status, title: Some(format!("{name} with a title")),
                        "What happened and what to do about it."
                    }
                }
                for (status , name) in STATUSES {
                    Alert { key: "{name}-body", status, "{name} with a body only." }
                }
            }
        }
        FoundationSection {
            id: "alert-dismissible".to_string(),
            title: "Dismissible".to_string(),
            description: "on_dismiss adds the dismiss part; the caller removes the alert.".to_string(),
            div { class: "grid gap-3",
                for (status , name) in STATUSES {
                    if !dismissed.read().contains(&status) {
                        Alert {
                            key: "{name}-dismissible",
                            status,
                            title: Some(format!("{name} you can dismiss")),
                            on_dismiss: move |()| dismissed.write().push(status),
                            "Press the cross."
                        }
                    }
                }
                if !dismissed.read().is_empty() {
                    clay_dioxus_kit::Button {
                        variant: clay_dioxus_kit::ButtonVariant::Outline,
                        onclick: move |()| dismissed.write().clear(),
                        "Bring them back"
                    }
                }
            }
        }
    }
}
