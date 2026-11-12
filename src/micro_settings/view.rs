//! Settings presentations: the list panel (the sections collection's rows, ordered and filtered
//! by the viewer's preferences) and the section panel (the presentation the shell has open,
//! mounted from its row attachment). The shell owns which one is shown and where.
use crate::SettingsMicro;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::ListRow;
use clay_mdk::Error;
use clay_mdk::Operation;
use clay_mdk::OperationReceipt;
use clay_mdk::Row;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::use_datum;
use clay_mdk::hooks::use_rows;
use dioxus::prelude::*;
use micro_settings_manifest::ID;
use micro_settings_manifest::Section;

#[derive(Props, Clone, PartialEq)]
pub struct SettingsListProps {
    pub micro: SettingsMicro,
}

#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn SettingsList(props: SettingsListProps) -> Element {
    let preferences = props.micro.preferences(&use_datum(&props.micro.config));
    let mut receipt = use_signal(|| None::<OperationReceipt>);
    let mut error = use_signal(|| None::<String>);
    let mut track = move |result: Result<Operation, Error>| match result {
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
    };

    let rows = use_rows(&props.micro.sections);
    let mut available: Vec<&Row<String, Section>> = rows.iter().collect();
    if let Some(config) = &preferences {
        available.sort_by(|left, right| {
            (config.rank(&left.key), &left.key).cmp(&(config.rank(&right.key), &right.key))
        });
    }
    let shown: Vec<&Row<String, Section>> = available
        .iter()
        .copied()
        .filter(|row| {
            preferences
                .as_ref()
                .is_some_and(|config| !config.hidden.contains(&row.key))
        })
        .collect();
    rsx! {
        // The frame's region already carries the panel's title as its accessible name.
        div { class: "flex flex-col gap-4",
            AppBar {
                title: "Settings",
                leading: rsx! { PresentationSlot { reference: props.micro.shell.nav_button.clone() } },
            }
            details { class: "mx-4 rounded-md border border-border p-4",
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
            nav { class: "flex flex-col gap-1 px-2", "aria-label": "Settings sections",
                for row in &shown {
                    ListRow {
                        key: "{row.key}",
                        id: row.key.clone(),
                        name: "Open {row.value.title} settings",
                        "{row.value.title}"
                    }
                }
                if shown.is_empty() { p { class: "px-2", "No settings sections are available." } }
            }
        }
    }
}

/// The details of the Settings list: the section the shell has open. A presentation takes no
/// parameters, so it asks the shell which row is open when it mounts; the shell mounts it keyed
/// by that row, so a different row is a fresh mount and a fresh question.
#[derive(Props, Clone, PartialEq)]
pub struct SettingsSectionProps {
    pub micro: SettingsMicro,
}

#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn SettingsSection(props: SettingsSectionProps) -> Element {
    let rows = use_rows(&props.micro.sections);
    let opened = props.micro.shell.opened.clone();
    let open = use_resource(move || {
        let opened = opened.clone();
        async move { opened.call::<_, Option<String>>(&ID.to_owned()).await }
    });
    let key = match &*open.read() {
        None => return rsx! { p { role: "status", "Loading section…" } },
        Some(Err(problem)) => return rsx! { p { role: "alert", "{problem}" } },
        Some(Ok(None)) => return rsx! { p { "Select a section." } },
        Some(Ok(Some(key))) => key.clone(),
    };
    let Some(row) = rows.iter().find(|row| row.key == key) else {
        return rsx! { p { role: "alert", "The {key} section is not available." } };
    };
    rsx! {
        section { class: "flex flex-col gap-4", "aria-label": "{row.value.title}",
            AppBar {
                title: row.value.title.clone(),
                leading: rsx! { PresentationSlot { reference: props.micro.shell.nav_button.clone() } },
            }
            div { class: "px-4 pb-4",
                PresentationSlot {
                    reference: row.attachments[row.value.detail.index()].clone(),
                }
            }
        }
    }
}
