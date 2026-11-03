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
//!   proxy in its root. If it also has a `service` requirement, it calls the provider's `echo`
//!   through it, which crosses from this incarnation's store into the provider's. Its `close`
//!   redeems the proxy again: it fails unless the provider is still live while the consumer
//!   closes, which is the close-order guarantee under test.
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
use clay_mdk::wire::WireMutations;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;
use std::cell::RefCell;
use std::rc::Rc;

const MANIFEST: &[u8] = include_bytes!("lifecycle_fixture.manifest.json");

#[used]
#[unsafe(link_section = "dayframe.manifest")]
static DAYFRAME_MANIFEST: [u8; MANIFEST.len()] = *include_bytes!("lifecycle_fixture.manifest.json");

const PRESENTATION: &str = "presentation";
const ECHO: &str = "echo";
const RELAY: &str = "relay";

thread_local! {
    static ROLE: RefCell<Option<String>> = const { RefCell::new(None) };
    /// The provider's `service` requirement, which its `relay` call forwards to.
    static SERVICE: RefCell<Option<Rc<capability_bank::CapabilityRef>>> = const { RefCell::new(None) };
}

struct Lifecycle;

enum Root {
    Provider,
    Consumer(capability_bank::PresentationComponent),
}

impl micro_lifecycle::GuestInstanceRoot for Root {
    async fn close(&self) -> Result<(), String> {
        match self {
            Root::Provider => {
                // The service reference is part of what the guest holds until its close.
                SERVICE.with_borrow_mut(Option::take);
                Ok(())
            }
            Root::Consumer(proxy) => proxy.component().map(drop).map_err(|error| {
                format!("provider withdrawn before its consumer closed: {error:?}")
            }),
        }
    }
}

impl micro_lifecycle::Guest for Lifecycle {
    type InstanceRoot = Root;

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
                    .map(|requirement| Rc::new(requirement.capability));
                let relays = service.is_some();
                SERVICE.with_borrow_mut(|slot| *slot = service);
                micro_lifecycle::Initialized {
                    root: micro_lifecycle::InstanceRoot::new(Root::Provider),
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
                micro_lifecycle::Initialized {
                    root: micro_lifecycle::InstanceRoot::new(Root::Consumer(proxy)),
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
                    root: micro_lifecycle::InstanceRoot::new(Root::Provider),
                    publications: Vec::new(),
                }
            }
            other => {
                return Err(InitializationError::InvalidConfiguration(format!(
                    "unknown lifecycle fixture role {other:?}"
                )));
            }
        };
        ROLE.with_borrow_mut(|slot| assert!(slot.replace(role).is_none()));
        Ok(initialized)
    }

    async fn call(publication: String, request: Vec<u8>) -> Result<Vec<u8>, String> {
        match publication.as_str() {
            ECHO => Ok([b"echo:".as_slice(), &request].concat()),
            RELAY => {
                // Clone the handle out so no borrow is held across the suspension: other
                // calls and sync entries run in this instance meanwhile.
                let service = SERVICE
                    .with_borrow(Clone::clone)
                    .ok_or("relay has no service requirement")?;
                service
                    .call(request)
                    .await
                    .map_err(|error| format!("relayed call failed: {error:?}"))
            }
            other => Err(format!("lifecycle fixture serves no call {other:?}")),
        }
    }
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
        let role = ROLE.with_borrow(|role| {
            role.clone()
                .expect("host mounted the lifecycle fixture before initializing it")
        });
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text(&format!("lifecycle {role}"));
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
