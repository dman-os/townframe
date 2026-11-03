#![cfg(target_arch = "wasm32")]
//! ADR 024 lifecycle fixture: one package that plays a provider/consumer activation graph, its
//! role picked by the configuration-supplied CBOR text in `initial-context.arguments`.
//!
//! * `provider` returns two named publications from `initialize`: the `presentation`
//!   presentation and the `echo` call, whose exported handler answers `echo:` plus the request.
//!   When the provider is configured with a `service` requirement, it also publishes `relay`,
//!   whose handler forwards the request to that service and answers what it answered. A test
//!   binds `service` to a call it controls, so a `relay` call can be held suspended in this
//!   guest on something external.
//! * `failing-provider` publishes the same two and then rejects initialization, so the host
//!   must discard publications that were never visible.
//! * `consumer` redeems its `presentation` requirement during initialization and keeps the
//!   proxy. If it also has a `service` requirement, it calls the provider's `echo` through it,
//!   which crosses from this incarnation's store into the provider's. When the host closes its
//!   inbox, its `serve` redeems the proxy again after draining: it traps unless the provider is
//!   still live while the consumer closes, which is the close-order guarantee under test.
//!
//! State shared between `initialize`, the `serve` handlers and the sync presentation exports
//! lives in statics (ADR 024 §7a: `serve` does not share thread-locals with sync exports). The
//! one deliberate exception is `IN_FLIGHT_LOCAL`, which `mount` renders beside its static
//! twin so a conformance test can pin that difference.
//!
//! The package embeds `lifecycle_fixture.manifest.json` as its `dayframe.manifest` section.

use clay_mdk::bindings;
use clay_mdk::capability_bank;
use clay_mdk::collection_registration;
use clay_mdk::collections;
use clay_mdk::document_events;
use clay_mdk::documents;
use clay_mdk::micro_lifecycle;
use clay_mdk::micro_lifecycle::InitializationError;
use clay_mdk::presentation;
use clay_mdk::presentation_events::Event;
use clay_mdk::presentation_events::EventResponse;
use clay_mdk::presentation_events::{self};
use clay_mdk::serve;
use clay_mdk::wire::WireMutations;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;
use std::cell::Cell;
use std::future::ready;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

const MANIFEST: &[u8] = include_bytes!("lifecycle_fixture.manifest.json");

#[used]
#[unsafe(link_section = "dayframe.manifest")]
static DAYFRAME_MANIFEST: [u8; MANIFEST.len()] = *include_bytes!("lifecycle_fixture.manifest.json");

const PRESENTATION: &str = "presentation";
const ECHO: &str = "echo";
const RELAY: &str = "relay";

static ROLE: OnceLock<String> = OnceLock::new();
/// The provider's `service` requirement, which its `relay` call forwards to.
static SERVICE: Mutex<Option<Arc<capability_bank::CapabilityRef>>> = Mutex::new(None);
/// The consumer's redeemed `presentation` proxy, checked again when it closes.
static PROXY: Mutex<Option<capability_bank::PresentationComponent>> = Mutex::new(None);
/// How many `relay` handlers are in flight, counted in a static and in a thread-local.
static IN_FLIGHT: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static IN_FLIGHT_LOCAL: Cell<u32> = const { Cell::new(0) };
}

/// Counts a handler as in flight from its creation until it is dropped, finished or not.
struct InFlight;

impl InFlight {
    fn new() -> Self {
        IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        IN_FLIGHT_LOCAL.with(|count| count.set(count.get() + 1));
        Self
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        IN_FLIGHT_LOCAL.with(|count| count.set(count.get() - 1));
    }
}

struct Lifecycle;

impl micro_lifecycle::Guest for Lifecycle {
    async fn initialize(
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, InitializationError> {
        let role: String =
            ciborium::from_reader(context.arguments.as_slice()).map_err(|error| {
                InitializationError::InvalidConfiguration(format!(
                    "lifecycle fixture arguments must be one CBOR text role: {error}"
                ))
            })?;
        let initialized = match role.as_str() {
            "provider" => {
                let service = context
                    .requirements
                    .into_iter()
                    .find(|requirement| requirement.name == "service")
                    .map(|requirement| Arc::new(requirement.capability));
                let relays = service.is_some();
                *SERVICE.lock().unwrap() = service;
                micro_lifecycle::Initialized {
                    publications: publish_all(relays)?,
                }
            }
            "failing-provider" => {
                let _staged = publish_all(false)?;
                return Err(InitializationError::Rejected(
                    "failing provider rejects its initialization".into(),
                ));
            }
            "consumer" => {
                let mut requirements = context.requirements;
                let mut take = |name: &str| {
                    requirements
                        .iter()
                        .position(|requirement| requirement.name == name)
                        .map(|index| requirements.swap_remove(index).capability)
                };
                let presentation = take(PRESENTATION).ok_or_else(|| {
                    InitializationError::Unavailable("presentation requirement unbound".into())
                })?;
                let service = take("service");
                let proxy = presentation.redeem_presentation().await.map_err(|error| {
                    InitializationError::Unavailable(format!(
                        "presentation requirement unredeemable: {error:?}"
                    ))
                })?;
                proxy.component().map_err(|error| {
                    InitializationError::Unavailable(format!(
                        "presentation requirement withdrawn: {error:?}"
                    ))
                })?;
                if let Some(service) = service {
                    let response = service.call(b"ping".to_vec()).await.map_err(|error| {
                        InitializationError::Unavailable(format!("service call failed: {error:?}"))
                    })?;
                    if response != b"echo:ping" {
                        return Err(InitializationError::Rejected(format!(
                            "service answered {response:?}"
                        )));
                    }
                }
                *PROXY.lock().unwrap() = Some(proxy);
                micro_lifecycle::Initialized {
                    publications: Vec::new(),
                }
            }
            "registrations" | "failing-registrations" => {
                register(context.registrations).await;
                if role == "failing-registrations" {
                    return Err(InitializationError::Rejected(
                        "registration fixture rejects its initialization".into(),
                    ));
                }
                micro_lifecycle::Initialized {
                    publications: Vec::new(),
                }
            }
            other => {
                return Err(InitializationError::InvalidConfiguration(format!(
                    "unknown lifecycle fixture role {other:?}"
                )));
            }
        };
        ROLE.set(role).expect("lifecycle fixture initialized twice");
        Ok(initialized)
    }

    async fn serve(inbox: serve::Inbox) {
        serve::run(inbox, |publication, request| match publication {
            ECHO => Box::pin(ready(Ok([b"echo:".as_slice(), &request].concat()))),
            RELAY => Box::pin(relay(request)),
            other => {
                let message = format!("lifecycle fixture serves no call {other:?}");
                Box::pin(ready(Err(message)))
            }
        })
        .await;
        // The inbox closed and every handler finished: the incarnation's cleanup, run while its
        // dependencies are still live (ADR 024 §6). What the guest holds is released here.
        SERVICE.lock().unwrap().take();
        if let Some(proxy) = PROXY.lock().unwrap().take() {
            proxy
                .component()
                .expect("provider withdrawn before its consumer closed");
        }
    }
}

/// Forward `request` to the `service` requirement and answer what it answered.
async fn relay(request: Vec<u8>) -> serve::Outcome {
    let _counted = InFlight::new();
    // Clone the handle out so no lock is held across the suspension: other calls and sync
    // entries run in this instance meanwhile.
    let service = SERVICE
        .lock()
        .unwrap()
        .clone()
        .ok_or("relay has no service requirement")?;
    service
        .call(request)
        .await
        .map_err(|error| format!("relayed call failed: {error:?}"))
}

async fn register(bindings: Vec<collection_registration::RegistrationBinding>) {
    let [first, second]: [_; 2] = bindings.try_into().unwrap();
    assert_eq!((&*first.name, &*second.name), ("a", "b"));
    let (first, second) = (first.writer, second.writer);
    let value = |text: &str| {
        let mut body = Vec::new();
        ciborium::into_writer(text, &mut body).unwrap();
        collections::EncodedValue {
            body,
            capabilities: Vec::new(),
        }
    };
    let first_x = first
        .put(b"x".to_vec(), None, value("a-x"))
        .await
        .unwrap()
        .row_version;
    let first_y = first
        .put(b"y".to_vec(), None, value("a-y"))
        .await
        .unwrap()
        .row_version;
    let second_x = second
        .put(b"x".to_vec(), None, value("b-x"))
        .await
        .unwrap()
        .row_version;
    assert!(matches!(
        second.put(b"x".to_vec(), Some(first_x), value("stolen")).await,
        Err(collection_registration::MutationError::Conflict(Some(current)))
            if (current.incarnation, current.sequence)
                == (second_x.incarnation, second_x.sequence)
    ));
    assert!(matches!(
        second.remove(b"y".to_vec(), Some(first_y)).await,
        Err(collection_registration::MutationError::Conflict(None))
    ));
    assert!(second.remove(b"y".to_vec(), None).await.unwrap().is_none());
    assert!(
        first
            .remove(b"y".to_vec(), Some(first_y))
            .await
            .unwrap()
            .is_some()
    );
    first.put(b"y".to_vec(), None, value("a-y")).await.unwrap();
    first.clear().await.unwrap();
    first.clear().await.unwrap();
    assert!(matches!(
        first
            .put(b"x".to_vec(), Some(first_x), value("stale"))
            .await,
        Err(collection_registration::MutationError::Conflict(None))
    ));
    first.put(b"x".to_vec(), None, value("a-x")).await.unwrap();
    first.put(b"y".to_vec(), None, value("a-y")).await.unwrap();
    drop((first, second));
}

fn publish_all(
    relays: bool,
) -> Result<Vec<micro_lifecycle::NamedPublication>, InitializationError> {
    let unavailable = |what: &str, error: capability_bank::CapabilityError| {
        InitializationError::Unavailable(format!("publishing {what} failed: {error:?}"))
    };
    let call = |name: &str| {
        capability_bank::publish_call(
            name,
            &capability_bank::Contract {
                namespace: "dayframe.lifecycle".into(),
                name: name.into(),
                version: (1, 0, 0),
            },
        )
        .map(|capability| micro_lifecycle::NamedPublication {
            name: name.into(),
            capability,
        })
        .map_err(|error| unavailable(name, error))
    };
    let mut publications = vec![
        micro_lifecycle::NamedPublication {
            name: PRESENTATION.into(),
            capability: capability_bank::publish_presentation(PRESENTATION)
                .map_err(|error| unavailable(PRESENTATION, error))?,
        },
        call(ECHO)?,
    ];
    if relays {
        publications.push(call(RELAY)?);
    }
    Ok(publications)
}

impl presentation_events::Guest for Lifecycle {
    fn mount(
        _publication: String,
        placement: &presentation::MountPoint,
        _components: Vec<presentation::ComponentRef>,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let role = ROLE
            .get()
            .expect("host mounted the lifecycle fixture before initializing it");
        let (in_flight, local) = (
            IN_FLIGHT.load(Ordering::SeqCst),
            IN_FLIGHT_LOCAL.with(Cell::get),
        );
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text(&format!(
            "lifecycle {role} (relays in flight: static={in_flight} thread-local={local})"
        ));
        edits.append_children(1);
        edits.append_children(1);
        edits.pop();
        surface.apply(&edits.take())?;
        Ok(surface)
    }

    fn unmount(_surface: &presentation::Surface) {}

    fn dispatch(_surface: &presentation::Surface, _event: Event) -> EventResponse {
        EventResponse {
            prevent_default: false,
            stop_propagation: false,
        }
    }
}

impl document_events::Guest for Lifecycle {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("lifecycle fixture owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("lifecycle fixture owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("lifecycle fixture submits no document operations");
    }
}

bindings::export!(Lifecycle with_types_in bindings);
