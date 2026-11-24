use crate::foundations::FoundationSection;
use crate::specimens::Row;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Card;
use clay_dioxus_kit::CardContent;
use clay_dioxus_kit::CardFooter;
use clay_dioxus_kit::CardHeader;
use clay_dioxus_kit::CardVariant;
use clay_dioxus_kit::Status;
use dioxus::prelude::*;

/// `Card`: the three variants with all three parts, and a card with only its content.
#[component]
pub fn CardSpecimens() -> Element {
    rsx! {
        FoundationSection {
            id: "card-variants".to_string(),
            title: "Variants".to_string(),
            description: "data-variant outline|raised|plain; data-part header|content|footer.".to_string(),
            div { class: "grid grid-cols-[repeat(auto-fit,minmax(16rem,1fr))] gap-4",
                for (variant , name) in [
                    (CardVariant::Outline, "Outline"),
                    (CardVariant::Raised, "Raised"),
                    (CardVariant::Plain, "Plain"),
                ] {
                    Card { key: "{name}", variant,
                        CardHeader { "{name}" }
                        CardContent { "A card groups related content and sets itself off from its surroundings." }
                        CardFooter {
                            Button { variant: ButtonVariant::Ghost, onclick: |()| {}, "Dismiss" }
                            Button { onclick: |()| {}, "Open" }
                        }
                    }
                }
            }
        }
        FoundationSection {
            id: "card-statuses".to_string(),
            title: "Statuses".to_string(),
            description: "data-status info|success|warning|danger, the colour roles Alert and Chip use; absent is neutral.".to_string(),
            div { class: "grid grid-cols-[repeat(auto-fit,minmax(12rem,1fr))] gap-4",
                for (status , name) in [
                    (Status::Info, "Info"),
                    (Status::Success, "Success"),
                    (Status::Warning, "Warning"),
                    (Status::Danger, "Danger"),
                ] {
                    Card { key: "{name}", status: Some(status),
                        CardHeader { "{name}" }
                        CardContent { "What the card says about its content." }
                    }
                }
            }
        }
        FoundationSection {
            id: "card-parts".to_string(),
            title: "Parts are optional".to_string(),
            description: "A card needs none of its parts.".to_string(),
            Row { label: "content only".to_string(),
                Card {
                    CardContent { "Only content." }
                }
                Card { variant: CardVariant::Raised,
                    CardHeader { "Header and content" }
                    CardContent { "No footer." }
                }
            }
        }
    }
}
