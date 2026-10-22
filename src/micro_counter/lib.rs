#![cfg(target_arch = "wasm32")]
//! The Wasm counter, authored against the MDK's guest backend exactly as a native micro is
//! authored against its native one: one `activate(&Context)` that binds its granted facet and
//! publishes a Dioxus view. The host drives it through the `micro-client` world `export_micro!`
//! exports; nothing here names a WIT binding.
use clay_mdk::documents::DocumentError;
use clay_mdk::guest::Context;
use clay_mdk::guest::Facet;
use clay_mdk::guest::FacetState;
use clay_mdk::guest::MirrorOperation;
use clay_mdk::guest::MirrorStatus;
use clay_mdk::guest::use_document_changes;
use dioxus::prelude::*;
use micro_counter_manifest::CONFIG_KEY;
use micro_counter_manifest::CONFIG_NAME;
use micro_counter_manifest::CONFIG_TAG;

/// Bind the counter's configuration facet when the composition granted one — the counter also
/// runs standalone, counting in memory, when it did not — and publish the view over it.
fn activate(context: &Context) -> Result<(), DocumentError> {
    let count = match context.config(CONFIG_NAME) {
        Some(config) => Some(config.facet::<u64>(CONFIG_TAG, CONFIG_KEY)?),
        None => None,
    };
    context.publish_component(move || counter(count.clone()));
    Ok(())
}

clay_mdk::export_micro!(activate);

/// The counter's model and view in one root-scope render: its hooks are shared by every surface
/// the host mounts this incarnation into (see `Context::publish_component`).
fn counter(count: Option<Facet<u64>>) -> Element {
    use_document_changes();
    let mut standalone = use_signal(|| 0_u64);
    let mut operation = use_signal(|| None::<MirrorOperation>);
    let mut error = use_signal(|| None::<String>);
    let snapshot = count.as_ref().map(Facet::current);
    let value = match (&count, &snapshot) {
        (Some(count), Some(snapshot)) => count.state(snapshot),
        _ => FacetState::Present(standalone()),
    };
    let status = snapshot
        .as_ref()
        .map_or(MirrorStatus::Live, |snapshot| snapshot.status);
    let increment = count.clone();
    let document = count.zip(snapshot);
    rsx! {
        div { class: "flex flex-col gap-4",
            button {
                class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                "aria-label": "Increment Wasm counter",
                disabled: matches!(value, FacetState::Loading) || status != MirrorStatus::Live,
                onclick: move |_| {
                    let Some(count) = &increment else {
                        standalone += 1;
                        return;
                    };
                    let snapshot = count.current();
                    if snapshot.status != MirrorStatus::Live {
                        return;
                    }
                    let current = match count.state(&snapshot) {
                        FacetState::Loading => return,
                        FacetState::Absent => 0,
                        FacetState::Present(current) => current,
                    };
                    match count.write(&(current + 1)) {
                        Ok(submitted) => {
                            operation.set(Some(submitted));
                            error.set(count.resync().err().map(|error| {
                                format!("Write submitted; mirror resync failed: {error:?}")
                            }));
                        }
                        Err(failure) => error.set(Some(format!("Write failed: {failure:?}"))),
                    }
                },
                match &value {
                    FacetState::Loading => rsx! { "Loading counter" },
                    FacetState::Absent => rsx! { "0" },
                    FacetState::Present(count) => rsx! { "{count}" },
                }
            }
            if let Some((count, snapshot)) = document {
                if matches!(value, FacetState::Absent) {
                    p { "Counter configuration is absent; increment starts at zero." }
                }
                if snapshot.refreshing {
                    p { role: "status", "Refreshing counter configuration." }
                }
                match snapshot.status {
                    MirrorStatus::Live => rsx! {},
                    MirrorStatus::Lagged => rsx! { p { role: "status", "Counter mirror needs resynchronization." } },
                    MirrorStatus::Released => rsx! { p { role: "status", "Counter configuration watch released." } },
                    MirrorStatus::Revoked => rsx! { p { role: "status", "Counter configuration access revoked." } },
                }
                p { "Pending counter writes: {snapshot.pending.len()}" }
                if let Some(receipt) = operation().map(|operation| operation.current()) {
                    p { role: "status", "Operation {receipt.id}: {receipt.status:?}" }
                }
                if let Some(error) = error() {
                    p { role: "status", "{error}" }
                }
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": "Refresh Wasm counter",
                    onclick: move |_| {
                        error.set(count.refresh().err().map(|error| format!("Refresh failed: {error:?}")));
                    },
                    "Refresh counter"
                }
            }
        }
    }
}
