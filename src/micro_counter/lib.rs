#![cfg(target_arch = "wasm32")]
use clay_mdk::{
    GuestDom, bindings, document_events, documents, micro_lifecycle,
    presentation::{MountPoint, PresentationError, Surface},
    presentation_events::{self, Event, EventResponse},
};
use dioxus::prelude::*;
use micro_counter_manifest::{CONFIG_KEY, CONFIG_NAME, CONFIG_TAG};
use std::{cell::RefCell, collections::BTreeMap};

#[derive(Clone, Default)]
struct Model {
    count: Option<u64>,
    pending: usize,
    document_backed: bool,
    status: String,
}

#[derive(Default)]
struct Documents {
    binding: Option<documents::DocumentBinding>,
    watch: Option<documents::DocumentWatch>,
    operations: BTreeMap<u64, documents::Operation>,
}

struct Counter;
thread_local! {
    static DOM: RefCell<Option<GuestDom>> = const { RefCell::new(None) };
    static DOCUMENTS: RefCell<Documents> = RefCell::new(Documents::default());
    static MODEL: RefCell<Model> = RefCell::new(Model::default());
    static SIGNAL: RefCell<Option<Signal<Model>>> = const { RefCell::new(None) };
}

fn publish_model() {
    let model = MODEL.with_borrow(Clone::clone);
    SIGNAL.with_borrow_mut(|signal| {
        if let Some(signal) = signal {
            signal.set(model);
        }
    });
}

fn apply_snapshot(snapshot: documents::DocumentSnapshot) {
    MODEL.with_borrow_mut(|model| {
        model.pending = snapshot.pending.len();
        model.count = snapshot.facets.into_iter().find_map(|entry| {
            if entry.route.key.tag != CONFIG_TAG || entry.route.key.key_id != CONFIG_KEY {
                return None;
            }
            match entry.value {
                documents::FacetValue::Loading => None,
                documents::FacetValue::Absent => Some(0),
                documents::FacetValue::Present(value) => match serde_json::from_str(&value) {
                    Ok(count) => Some(count),
                    Err(error) => {
                        model.status = format!("Invalid counter configuration: {error}");
                        None
                    }
                },
            }
        });
    });
    publish_model();
}

fn counter() -> Element {
    let model = use_signal(|| MODEL.with_borrow(Clone::clone));
    use_hook(|| SIGNAL.with_borrow_mut(|slot| *slot = Some(model)));
    let current = model();
    rsx! {
        div { class: "flex flex-col gap-4",
            button {
                class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                "aria-label": "Increment Wasm counter",
                disabled: current.count.is_none(),
                onclick: move |_| {
                    let Some(current) = MODEL.with_borrow(|model| model.count) else {
                        return;
                    };
                    let count = current + 1;
                    let snapshot = DOCUMENTS.with_borrow_mut(|state| {
                        let Some(binding) = &state.binding else {
                            MODEL.with_borrow_mut(|model| model.count = Some(count));
                            return None;
                        };
                        let route = documents::FacetRoute {
                            document: binding.document.clone(),
                            branch: binding.branch.clone(),
                            key: documents::FacetKey { tag: CONFIG_TAG.into(), key_id: CONFIG_KEY.into() },
                        };
                        match binding.access.mutate(&route, &documents::FacetMutation::Update(count.to_string())) {
                            Ok(operation) => {
                                let receipt = operation.current().unwrap();
                                assert!(state.operations.insert(receipt.id, operation).is_none());
                                Some(state.watch.as_ref().unwrap().current().unwrap())
                            }
                            Err(error) => {
                                MODEL.with_borrow_mut(|model| model.status = format!("Write failed: {error:?}"));
                                None
                            }
                        }
                    });
                    if let Some(snapshot) = snapshot {
                        apply_snapshot(snapshot);
                    } else {
                        publish_model();
                    }
                },
                if let Some(count) = current.count { "{count}" } else { "Loading counter" }
            }
            if current.document_backed {
                p { "Pending counter writes: {current.pending}" }
                p { role: "status", "{current.status}" }
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": "Refresh Wasm counter",
                    onclick: move |_| DOCUMENTS.with_borrow(|state| state.watch.as_ref().unwrap().refresh().unwrap()),
                    "Refresh counter"
                }
            }
        }
    }
}

impl micro_lifecycle::Guest for Counter {
    fn activate(mut configs: Vec<documents::DocumentBinding>) -> Result<(), String> {
        if configs.len() > 1
            || configs
                .first()
                .is_some_and(|binding| binding.name != CONFIG_NAME)
        {
            return Err("counter accepts only its named counter configuration binding".into());
        }
        MODEL.with_borrow_mut(|model| model.count = Some(0));
        if let Some(binding) = configs.pop() {
            let watch = binding
                .access
                .watch(&documents::WatchSelection {
                    documents: vec![binding.document.clone()],
                    branch: binding.branch.clone(),
                    facets: documents::FacetSelection::Keys(vec![documents::FacetKey {
                        tag: CONFIG_TAG.into(),
                        key_id: CONFIG_KEY.into(),
                    }]),
                })
                .map_err(|error| format!("counter configuration access failed: {error:?}"))?;
            let snapshot = watch
                .current()
                .map_err(|error| format!("counter configuration read failed: {error:?}"))?;
            MODEL.with_borrow_mut(|model| model.document_backed = true);
            DOCUMENTS.with_borrow_mut(|state| {
                state.binding = Some(binding);
                state.watch = Some(watch);
            });
            apply_snapshot(snapshot);
        }
        DOM.with_borrow_mut(|slot| {
            assert!(
                slot.replace(GuestDom::new(counter)).is_none(),
                "counter activated twice"
            );
        });
        Ok(())
    }
}

impl document_events::Guest for Counter {
    fn changed(snapshot: documents::DocumentSnapshot) {
        DOM.with_borrow_mut(|slot| {
            slot.as_mut()
                .unwrap()
                .update(|| apply_snapshot(snapshot))
                .unwrap()
        });
    }

    fn lagged(watch_id: u64) {
        let snapshot =
            DOCUMENTS.with_borrow(|state| state.watch.as_ref().unwrap().current().unwrap());
        assert_eq!(snapshot.watch_id, watch_id);
        Self::changed(snapshot);
    }

    fn completed(receipt: documents::OperationReceipt) {
        DOCUMENTS.with_borrow_mut(|state| {
            assert!(
                state.operations.remove(&receipt.id).is_some(),
                "unknown counter operation receipt"
            );
        });
        DOM.with_borrow_mut(|slot| {
            slot.as_mut()
                .unwrap()
                .update(|| {
                    MODEL.with_borrow_mut(|model| {
                        model.status = format!("Operation {}: {:?}", receipt.id, receipt.status)
                    });
                    publish_model();
                })
                .unwrap()
        });
    }
}

impl presentation_events::Guest for Counter {
    fn mount(
        placement: &MountPoint,
        components: Vec<clay_mdk::presentation::ComponentRef>,
    ) -> Result<Surface, PresentationError> {
        assert!(components.is_empty(), "counter declares no component slots");
        DOM.with_borrow_mut(|slot| {
            slot.as_mut()
                .expect("counter mounted before activation")
                .mount(placement)
        })
    }

    fn unmount(surface: &Surface) {
        DOM.with_borrow_mut(|slot| slot.as_mut().unwrap().unmount(surface));
    }

    fn dispatch(surface: &Surface, event: Event) -> EventResponse {
        DOM.with_borrow_mut(|slot| {
            slot.as_mut()
                .expect("counter dispatched before activation")
                .dispatch(surface, event)
        })
    }
}

bindings::export!(Counter with_types_in bindings);
