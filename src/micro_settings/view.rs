//! Settings presentation consumes its own typed collection and host-mediated component mounts.
use crate::SettingsMicro;
use clay_mdk::native::Query;
use clay_mdk::view::ComponentSlot;
use clay_mdk::view::use_collection;
use dioxus::prelude::*;

#[derive(Props, Clone)]
pub struct SettingsViewProps {
    pub micro: SettingsMicro,
}
impl PartialEq for SettingsViewProps {
    fn eq(&self, other: &Self) -> bool {
        self.micro.instance == other.micro.instance
    }
}

#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn SettingsView(props: SettingsViewProps) -> Element {
    let sections = use_collection(&props.micro.sections);
    let rows = sections.query(&Query::new());
    let mut selected = use_signal(|| rows.first().map(|(id, _)| *id));
    let active = selected()
        .filter(|id| sections.visible(id).is_some())
        .or_else(|| rows.first().map(|(id, _)| *id));
    let current = active.and_then(|id| sections.visible(&id));
    rsx! {
        section { class: "flex flex-col gap-4", "aria-label": "Settings",
            h2 { class: "text-xl font-semibold", "Settings" }
            div { class: "flex flex-wrap gap-6",
                nav { class: "flex flex-col gap-2", "aria-label": "Settings sections",
                    for (id, section) in &rows {
                        {
                            let id = *id;
                            rsx! {
                                button { key: "{id}", class: "rounded-md border border-border px-4 py-2 text-left focus-visible:outline", "aria-label": "Open {section.title} settings", "aria-pressed": active == Some(id), onclick: move |_| selected.set(Some(id)),
                                    if active == Some(id) { "▸ " }
                                    "{section.title}"
                                }
                            }
                        }
                    }
                    if rows.is_empty() { p { "No settings sections are available." } }
                }
                div { class: "min-w-0 flex-1",
                    if let Some(section) = current {
                        div { key: "{active.unwrap()}",
                            h3 { class: "mb-4 text-lg font-semibold", "{section.title}" }
                            ComponentSlot { owner: props.micro.instance.clone(), reference: section.detail }
                        }
                    } else { p { "Select a section." } }
                }
            }
        }
    }
}
