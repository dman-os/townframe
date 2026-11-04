#![cfg(target_arch = "wasm32")]
//! ADR 030 lifecycle fixture: one package that plays a provider/consumer activation graph, its
//! role picked by the configuration-supplied CBOR text in `initial-context.arguments`.
//!
//! * `provider` returns three named publications from `initialize`: the `presentation`
//!   presentation, the `echo` call, whose exported handler answers `echo:` plus the request,
//!   and the `relay` call, whose handler forwards its request to the optional
//!   `peer` requirement and answers `relay:` plus the peer's answer. What the peer is decides
//!   what a test exercises: a host-gated call (a handler suspended until the test releases
//!   it), another incarnation's `relay` (A to B to A), or the incarnation's own `echo`. The
//!   request `trap` traps the guest instead.
//! * `failing-provider` publishes the same three and then rejects initialization, so the host
//!   must discard publications that were never visible.
//! * `consumer` redeems its `presentation` requirement during initialization and keeps the
//!   proxy in its root. If it also has a `service` requirement, it calls the provider's `echo`
//!   through it, which crosses from this incarnation's store into the provider's. Its `close`
//!   redeems the proxy again: it fails unless the provider is still live while the consumer
//!   closes, which is the close-order guarantee under test.
//!
//! Its manifest is the checked-in sidecar `lifecycle_fixture.manifest.json`.

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

const PRESENTATION: &str = "presentation";
const ECHO: &str = "echo";
const RELAY: &str = "relay";
/// The contracts the checked-in sidecar manifest declares for the two calls.
const ECHO_CONTRACT: &str = "org.example.clay.test.lifecycle.echo";
const RELAY_CONTRACT: &str = "org.example.clay.test.lifecycle.relay";

thread_local! {
    static ROLE: RefCell<Option<String>> = const { RefCell::new(None) };
    /// The `relay` handler's peer. Handlers clone the `Rc` out before awaiting: a `RefCell`
    /// borrow must never be held across an `.await` (a second call overlapping the first
    /// would hit a live borrow).
    static PEER: RefCell<Option<Rc<capability_bank::CapabilityRef>>> = const { RefCell::new(None) };
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
                // Releases the peer reference (a guest resource); an in-flight handler that
                // already cloned it keeps its own share.
                PEER.with_borrow_mut(Option::take);
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
        // The role, as one CBOR text, or `[role, key-prefix]` for the registration roles: the
        // prefix namespaces the keys this incarnation writes, so the fixture can tell incarnations apart.
        let (role, prefix) = match ciborium::from_reader(context.arguments.as_slice()) {
            Ok(ciborium::Value::Text(role)) => (role, Vec::new()),
            Ok(ciborium::Value::Array(arguments)) => match <[_; 2]>::try_from(arguments) {
                Ok([ciborium::Value::Text(role), ciborium::Value::Bytes(prefix)]) => (role, prefix),
                _ => {
                    return Err(InitializationError::InvalidConfiguration(
                        "lifecycle fixture arguments must be [role, key-prefix]".into(),
                    ));
                }
            },
            _ => {
                return Err(InitializationError::InvalidConfiguration(
                    "lifecycle fixture arguments must be a CBOR role or [role, key-prefix]".into(),
                ));
            }
        };
        let initialized = match role.as_str() {
            "provider" => {
                let peer = context
                    .requirements
                    .into_iter()
                    .find(|requirement| requirement.name == "peer");
                PEER.with_borrow_mut(|slot| {
                    *slot = peer.map(|requirement| Rc::new(requirement.capability));
                });
                micro_lifecycle::Initialized {
                    root: micro_lifecycle::InstanceRoot::new(Root::Provider),
                    publications: publish_all()?,
                }
            }
            "failing-provider" => {
                let _staged = publish_all()?;
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
                register(&prefix, context.registrations).await;
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
                assert!(request != b"trap", "lifecycle fixture was told to trap");
                let peer = PEER
                    .with_borrow(Clone::clone)
                    .ok_or_else(|| "the relay has no peer requirement".to_owned())?;
                let response = peer
                    .call(request)
                    .await
                    .map_err(|error| format!("peer call failed: {error:?}"))?;
                Ok([b"relay:".as_slice(), &response].concat())
            }
            other => Err(format!("lifecycle fixture serves no call {other:?}")),
        }
    }
}

/// Exercise the causal-map write surface through `a` and `b`, two scopes of one micro under
/// the key `prefix`. The sequence has the same outcome whether the scopes are still
/// staged or already published (a scope sees its own staged candidates but not another's, so
/// only `Observed` bases and own-candidate comparisons are used across scopes). It leaves the
/// visible rows `b`'s `x` and `a`'s `y`.
async fn register(prefix: &[u8], bindings: Vec<collection_registration::RegistrationBinding>) {
    use collection_registration::MutationError;
    use collection_registration::WriteBasis;
    let [first, second]: [_; 2] = bindings.try_into().unwrap();
    assert_eq!((&*first.name, &*second.name), ("a", "b"));
    let (first, second) = (first.writer, second.writer);
    let value = |text: &str| {
        let mut body = Vec::new();
        ciborium::into_writer(text, &mut body).unwrap();
        collection_registration::WrittenValue {
            body,
            attachments: Vec::new(),
        }
    };
    let [x, y] = ["x", "y"].map(|name| [prefix, name.as_bytes()].concat());
    let a_x = first
        .put(x.clone(), WriteBasis::Concurrent, value("a-x"))
        .await
        .unwrap();
    let a_y = first
        .put(y.clone(), WriteBasis::Concurrent, value("a-y"))
        .await
        .unwrap();
    assert_eq!(a_x.lamport, 1);
    assert!(!a_x.writer.is_empty());
    // A write that observed a stamp supersedes it, whichever scope wrote it.
    let b_x = second
        .put(x.clone(), WriteBasis::Observed(a_x.clone()), value("b-x"))
        .await
        .unwrap();
    assert_eq!(b_x.lamport, 2);
    assert_eq!(b_x.writer, a_x.writer);
    // Compare-and-set against a superseded stamp fails with the visible one.
    assert!(matches!(
        second
            .put(x.clone(), WriteBasis::CompareAndSet(Some(a_x.clone())), value("stolen"))
            .await,
        Err(MutationError::Conflict(Some(current)))
            if (current.lamport, &current.writer) == (b_x.lamport, &b_x.writer)
    ));
    // Deleting is a stamped write too; `a` supersedes the tombstone because it was handed the
    // delete's stamp.
    let b_y = second
        .delete(y.clone(), WriteBasis::Observed(a_y))
        .await
        .unwrap();
    assert_eq!(b_y.lamport, 2);
    let a_y2 = first
        .put(y.clone(), WriteBasis::Observed(b_y), value("a-y"))
        .await
        .unwrap();
    assert_eq!(a_y2.lamport, 3);
    drop((first, second));
}

fn publish_all() -> Result<Vec<micro_lifecycle::NamedPublication>, InitializationError> {
    let unavailable = |what: &str, error: capability_bank::CapabilityError| {
        InitializationError::Unavailable(format!("publishing {what} failed: {error:?}"))
    };
    Ok(vec![
        micro_lifecycle::NamedPublication {
            name: PRESENTATION.into(),
            capability: capability_bank::publish_presentation(PRESENTATION)
                .map_err(|error| unavailable(PRESENTATION, error))?,
        },
        micro_lifecycle::NamedPublication {
            name: RELAY.into(),
            capability: capability_bank::publish_call(RELAY, RELAY_CONTRACT)
                .map_err(|error| unavailable(RELAY, error))?,
        },
        micro_lifecycle::NamedPublication {
            name: ECHO.into(),
            capability: capability_bank::publish_call(ECHO, ECHO_CONTRACT)
                .map_err(|error| unavailable(ECHO, error))?,
        },
    ])
}

impl presentation_events::Guest for Lifecycle {
    fn mount(
        _publication: String,
        placement: &presentation::MountPoint,
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
