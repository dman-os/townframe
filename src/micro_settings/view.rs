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
    let watch = props.micro.config.watch.clone();
    let mut snapshot = use_signal(|| watch.current().unwrap());
    use_future(move || {
        let mut changes = watch.subscribe().unwrap();
        async move {
            loop {
                snapshot.set(changes.borrow_and_update().clone());
                if changes.changed().await.is_err() {
                    break;
                }
            }
        }
    });
    let mut catalog = use_signal(|| props.micro.declarations.borrow().clone());
    let declarations = props.micro.declarations.clone();
    use_future(move || {
        let mut changes = declarations.clone();
        async move {
            loop {
                catalog.set(changes.borrow_and_update().clone());
                if changes.changed().await.is_err() {
                    break;
                }
            }
        }
    });
    let preferences = props.micro.preferences(&snapshot());
    let mut receipt = use_signal(|| None::<clay_mdk::native::OperationReceipt>);
    let mut error = use_signal(|| None::<String>);
    let mut track =
        move |result: Result<clay_mdk::native::Operation, clay_mdk::native::AccessError>| {
            match result {
                Ok(operation) => {
                    error.set(None);
                    let initial = operation.current();
                    receipt.set(Some(initial.clone()));
                    spawn(async move {
                        if let Some(completed) = operation.next(initial.revision).await.unwrap()
                            && receipt
                                .peek()
                                .as_ref()
                                .is_some_and(|current| current.id == completed.id)
                        {
                            receipt.set(Some(completed));
                        }
                    });
                }
                Err(problem) => error.set(Some(problem.to_string())),
            }
        };
    let sections = use_collection(&props.micro.sections);
    let mut rows = sections.query(&Query::new());
    rows.retain(|(_, section)| {
        preferences
            .as_ref()
            .is_some_and(|config| !config.hidden.contains(&section.contributor))
    });
    if let Some(config) = &preferences {
        rows.sort_by(|(_, left), (_, right)| {
            (config.rank(&left.contributor), &left.contributor)
                .cmp(&(config.rank(&right.contributor), &right.contributor))
        });
    }
    let mut selected = use_signal(|| rows.first().map(|(_, section)| section.contributor.clone()));
    let active = selected()
        .and_then(|name| rows.iter().find(|(_, section)| section.contributor == name))
        .or_else(|| rows.first());
    let current = active.map(|(_, section)| section);
    let active_id = active.map(|(id, _)| *id);
    let mut available = catalog();
    if let Some(config) = &preferences {
        available.sort_by(|left, right| {
            (config.rank(&left.instance), &left.instance)
                .cmp(&(config.rank(&right.instance), &right.instance))
        });
    }
    rsx! {
        section { class: "flex flex-col gap-4", "aria-label": "Settings",
            h2 { class: "text-xl font-semibold", "Settings" }
            details { class: "rounded-md border border-border p-4",
                summary { "Choose settings sections" }
                if preferences.is_none() { p { role: "status", "Settings preferences are loading." } }
                for row in available {
                    {
                        let title = serde_json::from_value::<crate::contract::DeclaredSection>(row.payload).unwrap().title;
                        let hidden = preferences.as_ref().is_some_and(|config| config.hidden.contains(&row.instance));
                        let toggle_micro = props.micro.clone();
                        let order_micro = props.micro.clone();
                        let toggle_name = row.instance.clone();
                        let order_name = row.instance.clone();
                        rsx! {
                            div { key: "{row.instance}", class: "mt-4 flex flex-wrap gap-2",
                                button {
                                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                    disabled: preferences.is_none(),
                                    "aria-label": "Show {title} section",
                                    "aria-pressed": !hidden,
                                    onclick: move |_| track(toggle_micro.edit(|config| {
                                        if !config.hidden.remove(&toggle_name) { config.hidden.insert(toggle_name.clone()); }
                                    })),
                                    if hidden { "{title}: hidden" } else { "{title}: shown" }
                                }
                                button {
                                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                    disabled: preferences.is_none(),
                                    "aria-label": "Move {title} first",
                                    onclick: move |_| track(order_micro.edit(|config| {
                                        config.order.retain(|name| name != &order_name);
                                        config.order.insert(0, order_name.clone());
                                    })),
                                    "Move first"
                                }
                            }
                        }
                    }
                }
                if let Some(problem) = error() { p { role: "alert", "{problem}" } }
                if let Some(receipt) = receipt() { p { role: "status", "Settings operation {receipt.id}: {receipt.status:?}" } }
            }
            div { class: "flex flex-wrap gap-6",
                nav { class: "flex flex-col gap-2", "aria-label": "Settings sections",
                    for (id, section) in &rows {
                        {
                            let id = *id;
                            let name = section.contributor.clone();
                            rsx! {
                                button { key: "{id}", class: "rounded-md border border-border px-4 py-2 text-left focus-visible:outline", "aria-label": "Open {section.title} settings", "aria-pressed": active_id == Some(id), onclick: move |_| selected.set(Some(name.clone())),
                                    if active_id == Some(id) { "▸ " }
                                    "{section.title}"
                                }
                            }
                        }
                    }
                    if rows.is_empty() { p { "No settings sections are available." } }
                }
                div { class: "min-w-0 flex-1",
                    if let Some(section) = current {
                        div { key: "{active_id.unwrap()}",
                            h3 { class: "mb-4 text-lg font-semibold", "{section.title}" }
                            ComponentSlot { owner: props.micro.instance.clone(), reference: section.detail }
                        }
                    } else { p { "Select a section." } }
                }
            }
        }
    }
}
