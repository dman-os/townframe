use dioxus::prelude::*;

#[component]
pub fn Separator() -> Element {
    rsx! {
        hr { class: "ct-separator" }
    }
}
