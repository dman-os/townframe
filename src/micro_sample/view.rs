//! Contributor-owned native presentation; Settings mounts it without reading its document.
use super::SampleConfig;
use clay_mdk::Facet;
use clay_mdk::FacetState;
use clay_mdk::Operation;
use clay_mdk::OperationReceipt;
use clay_mdk::hooks::use_facet;
use dioxus::prelude::*;

#[component]
pub(super) fn SampleDetail(config: Facet<SampleConfig>) -> Element {
    let facet = config;
    let snapshot = use_facet(&facet);
    let config_state = snapshot.state.clone();
    let mut status = use_signal(|| "Configuration loaded from the mock node.".to_string());
    let mut receipt = use_signal(|| None::<OperationReceipt>);
    let mut track = move |operation: Operation| {
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
    };
    let sample_for_toggle = facet.clone();
    let sample_for_refresh = facet.clone();
    rsx! {
        div { class: "flex flex-col gap-4",
            p { "This detail component belongs to the sample micro, not Settings." }
            match config_state {
                FacetState::Present(config) => {
                    let config_for_toggle = config.clone();
                    rsx! {
                        p { "Greeting preview: {config.greeting}" }
                        button {
                            class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                            "aria-label": if config.enabled { "Disable sample configuration flag" } else { "Enable sample configuration flag" },
                            "aria-pressed": config.enabled,
                            onclick: move |_| {
                                let mut next = config_for_toggle.clone();
                                next.enabled = !next.enabled;
                                match sample_for_toggle.write(next) {
                                    Ok(operation) => {
                                         track(operation);
                                         status.set("Optimistic edit is visible.".into());
                                     },
                                    Err(error) => status.set(error.to_string()),
                                }
                            },
                            if config.enabled { "Configuration flag: enabled — click to disable" } else { "Configuration flag: disabled — click to enable" }
                        }
                        label { class: "flex flex-col gap-2", "Greeting",
                            input {
                                class: "rounded-md border border-border bg-surface px-4 py-2 text-foreground focus-visible:outline",
                                "aria-label": "Greeting",
                                value: "{config.greeting}",
                                oninput: move |event| {
                                    let mut next = SampleConfig { enabled: config.enabled, greeting: event.value() };
                                    clamp_greeting(&mut next.greeting);
                                    match facet.write(next) {
                                        Ok(operation) => {
                                             track(operation);
                                             status.set("Optimistic greeting edit pending.".into());
                                         },
                                        Err(error) => status.set(error.to_string()),
                                    }
                                }
                            }
                        }
                    }
                }
                FacetState::Loading => rsx! { p { "Configuration is loading." } },
                FacetState::Absent => rsx! { p { "Configuration is absent." } },
            }
            if snapshot.refreshing {
                p { role: "status", "Refreshing sample configuration." }
            }
            div { class: "flex flex-wrap gap-2",
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": "Refresh configuration",
                    onclick: move |_| {
                        if sample_for_refresh.refresh().unwrap().refreshing {
                            status.set("Refreshing sample configuration; pending edits were retained.".into());
                        } else {
                            status.set("Targeted refresh completed; pending edits were retained.".into());
                        }
                    },
                    "Refresh configuration"
                }
            }
            p { role: "status", "{status}" }
            p { class: "text-sm text-muted-foreground", "Mirror revision {snapshot.revision} · pending writes: {snapshot.pending}" }
            if let Some(receipt) = receipt() {
                p { role: "status", "Operation {receipt.id}: {receipt.status:?}" }
            }
        }
    }
}

/// The most characters a greeting keeps. A byte limit would cut a multi-byte character in two,
/// which `String::truncate` refuses by panicking: Bombadil typed one into the field in the
/// browser build and the sample's view took the page's listener down with it.
const GREETING_LIMIT: usize = 120;

/// Cut `greeting` to [`GREETING_LIMIT`] characters.
pub(crate) fn clamp_greeting(greeting: &mut String) {
    if let Some((end, _)) = greeting.char_indices().nth(GREETING_LIMIT) {
        greeting.truncate(end);
    }
}
