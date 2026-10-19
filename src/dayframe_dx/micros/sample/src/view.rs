//! Contributor-owned native presentation; Settings mounts it without reading its document.
use super::SampleConfig;
use dayframe_app::documents::{Facet, Operation, OperationReceipt};
use dioxus::prelude::*;

#[component]
pub(super) fn SampleDetail(config: Facet<SampleConfig>) -> Element {
    let facet = config;
    let watch = facet.watch.clone();
    let mut state = use_signal(|| watch.current().unwrap());
    use_future(move || {
        let mut changes = watch.subscribe().unwrap();
        async move {
            loop {
                state.set(changes.borrow_and_update().clone());
                if changes.changed().await.is_err() {
                    break;
                }
            }
        }
    });
    let snapshot = state();
    let config = facet.value(&snapshot);
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
    let config_for_toggle = config.clone();
    let sample_for_toggle = facet.clone();
    let sample_for_refresh = facet.clone();
    rsx! {
        div { class: "flex flex-col gap-4",
            p { "This detail component belongs to the sample micro, not Settings." }
            if let Some(config) = config {
                p { "Greeting preview: {config.greeting}" }
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": if config.enabled { "Disable sample configuration flag" } else { "Enable sample configuration flag" },
                    "aria-pressed": config.enabled,
                    onclick: move |_| {
                        let mut next = config_for_toggle.clone().unwrap();
                        next.enabled = !next.enabled;
                        match sample_for_toggle.write(next) {
                            Ok(operation) => {
                                 track(operation);
                                 status.set("Optimistic edit is visible. Accept or reject it below.".into());
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
                            next.greeting.truncate(120);
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
            } else { p { "Configuration is not loaded." } }
            div { class: "flex flex-wrap gap-2",
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": "Refresh configuration",
                    onclick: move |_| {
                        sample_for_refresh.watch.refresh().unwrap();
                        status.set("Targeted refresh completed; pending edits were retained.".into());
                    },
                    "Refresh configuration"
                }
            }
            p { role: "status", "{status}" }
            p { class: "text-sm text-muted-foreground", "Mirror revision {snapshot.revision} · pending writes: {snapshot.pending.len()}" }
            if let Some(receipt) = receipt() {
                p { role: "status", "Operation {receipt.id}: {receipt.status:?}" }
            }
        }
    }
}
