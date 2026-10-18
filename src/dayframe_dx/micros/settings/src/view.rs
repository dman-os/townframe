//! Settings presentation consumes its own typed collection and host-mediated component mounts.
use crate::SettingsMicro;
use dayframe_app::native::{ComponentMount, NativeComponents};
use dioxus::prelude::*;

#[derive(Props, Clone)]
pub struct SettingsViewProps {
    pub micro: SettingsMicro,
    pub components: NativeComponents,
}
impl PartialEq for SettingsViewProps {
    fn eq(&self, other: &Self) -> bool {
        self.micro.instance == other.micro.instance && self.components == other.components
    }
}

#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn SettingsView(props: SettingsViewProps) -> Element {
    let reader = props.micro.sections.clone();
    let mut sections = use_signal(|| reader.snapshot().unwrap());
    let mut selected = use_signal(|| sections().keys().next().copied());
    use_future(move || {
        let mut updates = reader.subscribe().unwrap();
        async move {
            loop {
                let snapshot = updates.borrow_and_update().clone();
                if selected().is_none_or(|id| !snapshot.contains_key(&id)) {
                    selected.set(snapshot.keys().next().copied());
                }
                sections.set(snapshot);
                if updates.changed().await.is_err() {
                    break;
                }
            }
        }
    });
    let current = selected().and_then(|id| sections().get(&id).cloned());
    rsx! {
        section { class: "flex flex-col gap-4", "aria-label": "Settings",
            h2 { class: "text-xl font-semibold", "Settings" }
            div { class: "flex flex-wrap gap-6",
                nav { class: "flex flex-col gap-2", "aria-label": "Settings sections",
                    for (id, section) in sections() {
                        button { key: "{id}", class: "rounded-md border border-border px-4 py-2 text-left focus-visible:outline", "aria-label": "Open {section.title} settings", "aria-pressed": selected() == Some(id), onclick: move |_| selected.set(Some(id)),
                            if selected() == Some(id) { "▸ " }
                            "{section.title}"
                        }
                    }
                    if sections().is_empty() { p { "No settings sections are available." } }
                }
                div { class: "min-w-0 flex-1",
                    if let Some(section) = current {
                        div { key: "{selected().unwrap()}",
                            h3 { class: "mb-4 text-lg font-semibold", "{section.title}" }
                            ComponentMount { components: props.components.clone(), owner: props.micro.instance.clone(), reference: section.detail }
                        }
                    } else { p { "Select a section." } }
                }
            }
        }
    }
}
