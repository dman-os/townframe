//! The counter, authored once against the MDK's [`Context`]: a configured facet it counts in (or
//! an in-memory count when the composition granted none), two presentations of it (`view`, the
//! one its Settings section mounts, and a compact `badge`) and a `count` call that answers the
//! current value. [`clay_mdk::export_micro!`] exports it as the `micro-client` guest on
//! wasm32-wasip3 and as the native `ACTIVATE` entry everywhere else; nothing here names a
//! backend.
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use clay_mdk::FacetState;
use clay_mdk::Operation;
use clay_mdk::hooks::use_facet;
use dioxus::prelude::*;
use micro_counter_manifest::BADGE;
use micro_counter_manifest::CONFIG_KEY;
use micro_counter_manifest::CONFIG_NAME;
use micro_counter_manifest::CONFIG_TAG;
use micro_counter_manifest::COUNT;
use micro_counter_manifest::VIEW;
use std::cell::Cell;
use std::rc::Rc;

/// What every presentation and the call count in: the granted configuration facet, or, when the
/// composition granted none (the counter also runs standalone), an in-memory count that only
/// the `view` increments.
#[derive(Clone)]
struct Counter {
    facet: Option<Facet<u64>>,
    standalone: Rc<Cell<u64>>,
}

impl Counter {
    /// The count the facet holds, `None` while it is still loading. An absent facet counts zero.
    fn value(&self) -> Result<Option<u64>, Error> {
        let Some(facet) = &self.facet else {
            return Ok(Some(self.standalone.get()));
        };
        Ok(match facet.current()?.state {
            FacetState::Loading => None,
            FacetState::Absent => Some(0),
            FacetState::Present(count) => Some(count),
        })
    }
}

/// Bind the counter's configuration facet when the composition granted one and publish its two
/// presentations and its call over it.
async fn activate(context: Context) -> Result<(), Error> {
    let counter = Counter {
        facet: match context.config(CONFIG_NAME) {
            Some(config) => Some(config.facet::<u64>(CONFIG_TAG, CONFIG_KEY)?),
            None => None,
        },
        standalone: Rc::default(),
    };
    context.publish_component(VIEW, {
        let counter = counter.clone();
        move || view(counter.clone())
    })?;
    context.publish_component(BADGE, {
        let counter = counter.clone();
        move || badge(counter.clone())
    })?;
    context.publish_call(COUNT, move |(): ()| {
        let counter = counter.clone();
        async move { counter.value()?.ok_or(Error::Unavailable) }
    })?;
    Ok(())
}

clay_mdk::export_micro!(activate, micro_counter_manifest::manifest);

/// The counter's model and view in one root-scope render: its hooks are shared by every surface
/// the host mounts this presentation into (see [`Context::publish_component`]).
fn view(counter: Counter) -> Element {
    let mut rerender = use_signal(|| 0_u64);
    let _ = rerender();
    let mut operation = use_signal(|| None::<Operation>);
    let mut error = use_signal(|| None::<String>);
    let snapshot = counter.facet.as_ref().map(use_facet);
    let value = match &snapshot {
        Some(snapshot) => snapshot.state.clone(),
        None => FacetState::Present(counter.standalone.get()),
    };
    let increment = counter.clone();
    let document = counter.facet.clone().zip(snapshot);
    rsx! {
        div { class: "flex flex-col gap-4",
            button {
                class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                "aria-label": "Increment Wasm counter",
                disabled: matches!(value, FacetState::Loading),
                onclick: move |_| {
                    let Some(facet) = &increment.facet else {
                        increment.standalone.set(increment.standalone.get() + 1);
                        rerender += 1;
                        return;
                    };
                    let current = match increment.value() {
                        Ok(Some(current)) => current,
                        Ok(None) => return,
                        Err(failure) => {
                            error.set(Some(format!("Write failed: {failure}")));
                            return;
                        }
                    };
                    match facet.write(current + 1) {
                        Ok(submitted) => {
                            operation.set(Some(submitted));
                            error.set(None);
                        }
                        Err(failure) => error.set(Some(format!("Write failed: {failure}"))),
                    }
                },
                match &value {
                    FacetState::Loading => rsx! { "Loading counter" },
                    FacetState::Absent => rsx! { "0" },
                    FacetState::Present(count) => rsx! { "{count}" },
                }
            }
            if let Some((facet, snapshot)) = document {
                if matches!(value, FacetState::Absent) {
                    p { "Counter configuration is absent; increment starts at zero." }
                }
                if snapshot.refreshing {
                    p { role: "status", "Refreshing counter configuration." }
                }
                p { "Pending counter writes: {snapshot.pending}" }
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
                        error.set(facet.refresh().err().map(|failure| format!("Refresh failed: {failure}")));
                    },
                    "Refresh counter"
                }
            }
        }
    }
}

/// The compact presentation: the count and nothing to act on. A standalone counter's badge shows
/// the in-memory count as of its last render, since only the `view` is notified of it changing.
fn badge(counter: Counter) -> Element {
    let snapshot = counter.facet.as_ref().map(use_facet);
    let text = match snapshot.map(|snapshot| snapshot.state) {
        None => counter.standalone.get().to_string(),
        Some(FacetState::Loading) => "…".into(),
        Some(FacetState::Absent) => "0".into(),
        Some(FacetState::Present(count)) => count.to_string(),
    };
    rsx! { span { role: "status", "aria-label": "Wasm counter badge", "Count: {text}" } }
}

/// The isomorphism proof: the same crate, linked natively through its `ACTIVATE` entry, publishes
/// what the manifest declares and serves its `count` call (the guest build is exercised by the
/// engine tests over the wasm component).
#[cfg(all(test, not(target_os = "wasi")))]
mod tests {
    use super::*;
    use clay_dioxus::NativeComponents;
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilitySession;
    use clay_host::ConfiguredMicro;
    use clay_host::Host;
    use clay_host::MicroId;
    use clay_host::Supervisor;
    use clay_mdk::MicroManifest;
    use clay_mdk::host::CollectionBindings;
    use clay_mdk::host::CollectionRegistry;
    use clay_mdk::host::DocumentService;
    use clay_mdk::host::ReplicaId;
    use clay_mdk::host::Resources;
    use clay_mdk::host::factory;
    use std::collections::BTreeMap;

    #[tokio::test(flavor = "current_thread")]
    async fn the_native_entry_publishes_both_presentations_and_serves_the_count_call() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let host = Host::new();
                let resources = Resources::new(
                    NativeComponents::new(&host),
                    DocumentService::new(16),
                    CollectionRegistry::new(ReplicaId::from_bytes([1; ReplicaId::LEN])),
                );
                // Standalone: no configuration document granted, so the count lives in memory.
                let manifest = MicroManifest {
                    configuration_requests: vec![],
                    ..micro_counter_manifest::manifest()
                };
                let counter = ConfiguredMicro {
                    identity: MicroId::from(&clay_mdk::MicroId::local(micro_counter_manifest::ID)),
                    eager: true,
                    enabled: true,
                    requirements: BTreeMap::new(),
                    unresolved_bindings: Vec::new(),
                    factory: factory(
                        resources,
                        clay_mdk::MicroId::local(micro_counter_manifest::ID),
                        manifest,
                        CollectionBindings::default(),
                        ACTIVATE,
                    ),
                };
                let supervisor = Supervisor::new(host.clone(), [counter]);
                let id = MicroId::from(&clay_mdk::MicroId::local(micro_counter_manifest::ID));
                assert!(supervisor.reconcile().await.is_empty());
                for name in [VIEW, BADGE] {
                    drop(supervisor.acquire(&id, name).await.unwrap());
                }
                let lease = supervisor.acquire(&id, COUNT).await.unwrap();
                let recipient = host.start("recipient");
                let reference = CapabilitySession::new(
                    host.clone(),
                    recipient.clone(),
                    CapabilityLimits::default(),
                )
                .mint(&lease)
                .unwrap();
                let mut request = Vec::new();
                ciborium::into_writer(&(), &mut request).unwrap();
                let response = reference.call(request).await.unwrap();
                assert_eq!(
                    ciborium::from_reader::<u64, _>(response.as_slice()).unwrap(),
                    0
                );
                drop((reference, lease));
                supervisor.shutdown().await.unwrap();
                host.close_instance(&recipient).unwrap();
                host.close();
            })
            .await;
    }
}
