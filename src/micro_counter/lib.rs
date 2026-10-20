#![cfg(target_arch = "wasm32")]
use clay_mdk::{
    ConfigDocument, FacetState, GuestDom, MirrorWatch, bindings, document_events, documents,
    micro_lifecycle,
    mirror::{DocumentMirror, MirrorOperation, MirrorStatus},
    presentation::{MountPoint, PresentationError, Surface},
    presentation_events::{self, Event, EventResponse},
};
use dioxus::prelude::*;
use micro_counter_manifest::{CONFIG_KEY, CONFIG_NAME, CONFIG_TAG};
use std::cell::RefCell;

#[derive(Clone)]
struct Model {
    count: FacetState<u64>,
    pending: usize,
    refreshing: bool,
    mirror_status: MirrorStatus,
    document_backed: bool,
    receipt: Option<documents::OperationReceipt>,
    error: Option<String>,
}

#[derive(Default)]
struct CounterState {
    document: Option<(ConfigDocument, MirrorWatch)>,
    operation: Option<MirrorOperation>,
    standalone_count: u64,
    error: Option<String>,
}

impl CounterState {
    fn model(&self) -> Model {
        let mut model = Model {
            count: FacetState::Present(self.standalone_count),
            pending: 0,
            refreshing: false,
            mirror_status: MirrorStatus::Live,
            document_backed: self.document.is_some(),
            receipt: self.operation.as_ref().map(MirrorOperation::current),
            error: self.error.clone(),
        };
        if let Some((_, watch)) = &self.document {
            let snapshot = watch.current();
            model.pending = snapshot.pending.len();
            model.refreshing = snapshot.refreshing;
            model.mirror_status = snapshot.status;
            model.count = match snapshot.status {
                MirrorStatus::Live | MirrorStatus::Lagged => snapshot
                    .value(CONFIG_TAG, CONFIG_KEY)
                    .expect("counter watch must contain its authorized facet"),
                MirrorStatus::Released | MirrorStatus::Revoked => FacetState::Loading,
            };
        }
        model
    }

    fn increment(&mut self) -> Result<(), String> {
        if let Some((config, watch)) = &self.document {
            let snapshot = watch.current();
            if snapshot.status != MirrorStatus::Live {
                return Ok(());
            }
            let count = match snapshot
                .value::<u64>(CONFIG_TAG, CONFIG_KEY)
                .expect("counter watch must contain its authorized facet")
            {
                FacetState::Loading => return Ok(()),
                FacetState::Absent => 0,
                FacetState::Present(count) => count,
            };
            self.operation = Some(
                config
                    .write(CONFIG_TAG, CONFIG_KEY, &(count + 1))
                    .map_err(|error| format!("Write failed: {error:?}"))?,
            );
            watch
                .resync()
                .map_err(|error| format!("Write submitted; mirror resync failed: {error:?}"))?;
        } else {
            self.standalone_count += 1;
        }
        Ok(())
    }
}

struct Counter;
thread_local! {
    static DOM: RefCell<Option<GuestDom>> = const { RefCell::new(None) };
    static MIRROR: DocumentMirror = DocumentMirror::default();
    static STATE: RefCell<CounterState> = RefCell::new(CounterState::default());
    static SIGNAL: RefCell<Option<Signal<Model>>> = const { RefCell::new(None) };
}

fn publish_model() {
    let model = STATE.with_borrow(CounterState::model);
    SIGNAL.with_borrow_mut(|signal| {
        if let Some(signal) = signal {
            signal.set(model);
        }
    });
}

fn render_model() {
    DOM.with_borrow_mut(|slot| slot.as_mut().unwrap().update(publish_model).unwrap());
}

fn counter() -> Element {
    let model = use_signal(|| STATE.with_borrow(CounterState::model));
    use_hook(|| SIGNAL.with_borrow_mut(|slot| *slot = Some(model)));
    let current = model();
    rsx! {
        div { class: "flex flex-col gap-4",
            button {
                class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                "aria-label": "Increment Wasm counter",
                disabled: matches!(current.count, FacetState::Loading) || current.mirror_status != MirrorStatus::Live,
                onclick: move |_| {
                    STATE.with_borrow_mut(|state| {
                        state.error = state.increment().err();
                    });
                    publish_model();
                },
                match current.count {
                    FacetState::Loading => rsx! { "Loading counter" },
                    FacetState::Absent => rsx! { "0" },
                    FacetState::Present(count) => rsx! { "{count}" },
                }
            }
            if current.document_backed {
                if matches!(current.count, FacetState::Absent) {
                    p { "Counter configuration is absent; increment starts at zero." }
                }
                if current.refreshing {
                    p { role: "status", "Refreshing counter configuration." }
                }
                match current.mirror_status {
                    MirrorStatus::Live => rsx! {},
                    MirrorStatus::Lagged => rsx! { p { role: "status", "Counter mirror needs resynchronization." } },
                    MirrorStatus::Released => rsx! { p { role: "status", "Counter configuration watch released." } },
                    MirrorStatus::Revoked => rsx! { p { role: "status", "Counter configuration access revoked." } },
                }
                p { "Pending counter writes: {current.pending}" }
                if let Some(receipt) = current.receipt {
                    p { role: "status", "Operation {receipt.id}: {receipt.status:?}" }
                }
                if let Some(error) = current.error {
                    p { role: "status", "{error}" }
                }
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": "Refresh Wasm counter",
                    onclick: move |_| {
                        STATE.with_borrow_mut(|state| {
                            state.error = state.document.as_ref().unwrap().1.refresh().err()
                                .map(|error| format!("Refresh failed: {error:?}"));
                        });
                        publish_model();
                    },
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
        if let Some(binding) = configs.pop() {
            let config = MIRROR.with(|mirror| mirror.bind(binding));
            let watch = config
                .watch(CONFIG_TAG, CONFIG_KEY)
                .map_err(|error| format!("counter configuration access failed: {error:?}"))?;
            STATE.with_borrow_mut(|state| state.document = Some((config, watch)));
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
        if MIRROR.with(|mirror| mirror.changed(snapshot)) {
            render_model();
        }
    }

    fn lagged(watch_id: u64) {
        if let Some(watch) = MIRROR.with(|mirror| mirror.lagged(watch_id)) {
            let result = watch.resync();
            STATE.with_borrow_mut(|state| {
                state.error = result
                    .err()
                    .map(|error| format!("Resync failed: {error:?}"));
            });
            render_model();
        }
    }

    fn completed(receipt: documents::OperationReceipt) {
        if MIRROR.with(|mirror| mirror.completed(receipt)) {
            render_model();
        }
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
