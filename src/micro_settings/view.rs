//! Settings presentation: the sections collection's rows, ordered and filtered by the viewer's
//! preferences, with the selected section's presentation mounted from its row attachment.
use crate::SettingsMicro;
use clay_mdk::native::Row;
use clay_mdk::view::AttachedRef;
use clay_mdk::view::PresentationSlot;
use clay_mdk::view::use_rows;
use dioxus::prelude::*;
use micro_settings_manifest::Section;

#[derive(Props, Clone, PartialEq)]
pub struct SettingsViewProps {
    pub micro: SettingsMicro,
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

    let mut available: Vec<Row<String, Section>> = use_rows(&props.micro.sections);
    if let Some(config) = &preferences {
        available.sort_by(|left, right| {
            (config.rank(&left.key), &left.key).cmp(&(config.rank(&right.key), &right.key))
        });
    }
    let shown: Vec<&Row<String, Section>> = available
        .iter()
        .filter(|row| {
            preferences
                .as_ref()
                .is_some_and(|config| !config.hidden.contains(&row.key))
        })
        .collect();
    let mut selected = use_signal(|| None::<String>);
    let active = selected()
        .and_then(|name| shown.iter().find(|row| row.key == name).copied())
        .or_else(|| shown.first().copied());
    let active_key = active.map(|row| row.key.clone());
    rsx! {
        section { class: "flex flex-col gap-4", "aria-label": "Settings",
            h2 { class: "text-xl font-semibold", "Settings" }
            details { class: "rounded-md border border-border p-4",
                summary { "Choose settings sections" }
                if preferences.is_none() { p { role: "status", "Settings preferences are loading." } }
                for row in &available {
                    {
                        let title = row.value.title.clone();
                        let hidden = preferences.as_ref().is_some_and(|config| config.hidden.contains(&row.key));
                        let toggle_micro = props.micro.clone();
                        let order_micro = props.micro.clone();
                        let toggle_name = row.key.clone();
                        let order_name = row.key.clone();
                        rsx! {
                            div { key: "{row.key}", class: "mt-4 flex flex-wrap gap-2",
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
                    for row in &shown {
                        {
                            let name = row.key.clone();
                            let pressed = active_key.as_deref() == Some(row.key.as_str());
                            rsx! {
                                button { key: "{row.key}", class: "rounded-md border border-border px-4 py-2 text-left focus-visible:outline", "aria-label": "Open {row.value.title} settings", "aria-pressed": pressed, onclick: move |_| selected.set(Some(name.clone())),
                                    if pressed { "▸ " }
                                    "{row.value.title}"
                                }
                            }
                        }
                    }
                    if shown.is_empty() { p { "No settings sections are available." } }
                }
                div { class: "min-w-0 flex-1",
                    if let Some(row) = active {
                        div { key: "{row.key}",
                            h3 { class: "mb-4 text-lg font-semibold", "{row.value.title}" }
                            PresentationSlot {
                                owner: props.micro.instance.clone(),
                                reference: AttachedRef::new(
                                    row.attachments[row.value.detail.index()].share(),
                                ),
                            }
                        }
                    } else { p { "Select a section." } }
                }
            }
        }
    }
}
