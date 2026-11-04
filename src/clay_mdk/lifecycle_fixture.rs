#![cfg(target_arch = "wasm32")]
//! The raw-WIT fixture of the engine-only lifecycle scenarios (`guest_trap_*`, `sync_exports_*`,
//! `dropped_call_*`): the same wire as the `context` micro's `echo` and `relay`, written
//! straight against the WIT instead of the MDK, because those scenarios are about what a guest
//! store and the engine's per-call machinery do, not about the authoring surface.
//!
//! It publishes the presentation `detail`, the `echo` call, which answers its request, and the
//! `relay` call, which forwards its request (CBOR text) to the optional `peer` requirement and
//! answers `relay:` plus the peer's answer. What the peer is decides what a scenario exercises: a
//! host-gated call (a handler suspended until the test releases it), or another micro's `relay`.
//! The request `trap` traps the guest instead.
//!
//! Its manifest is the checked-in sidecar `lifecycle_fixture.manifest.json`.

use clay_mdk::bindings;
use clay_mdk::capability_bank;
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

const PRESENTATION: &str = "detail";
const ECHO: &str = "echo";
const RELAY: &str = "relay";
/// The contracts the checked-in sidecar manifest declares for the two calls, the `context`
/// micro's.
const ECHO_CONTRACT: &str = "org.example.clay.test.context.echo";
const RELAY_CONTRACT: &str = "org.example.clay.test.context.relay";

thread_local! {
    /// Written by `initialize`, read by `mount`: a different component-model task, which only
    /// finds it because all tasks of a store share one TLS.
    static ROLE: RefCell<Option<&'static str>> = const { RefCell::new(None) };
    /// The `relay` handler's peer. Handlers clone the `Rc` out before awaiting: a `RefCell`
    /// borrow must never be held across an `.await` (a second call overlapping the first
    /// would hit a live borrow).
    static PEER: RefCell<Option<Rc<capability_bank::CapabilityRef>>> = const { RefCell::new(None) };
}

struct Lifecycle;

struct Root;

impl micro_lifecycle::GuestInstanceRoot for Root {
    async fn close(&self) -> Result<(), String> {
        // Releases the peer reference (a guest resource); an in-flight handler that already
        // cloned it keeps its own share.
        PEER.with_borrow_mut(Option::take);
        Ok(())
    }
}

/// The text a CBOR-encoded request or response holds.
fn text(bytes: &[u8]) -> Result<String, String> {
    ciborium::from_reader(bytes).map_err(|error| format!("not CBOR text: {error}"))
}

fn cbor(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(text, &mut bytes).unwrap();
    bytes
}

impl micro_lifecycle::Guest for Lifecycle {
    type InstanceRoot = Root;

    async fn initialize(
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, InitializationError> {
        let peer = context
            .requirements
            .into_iter()
            .find(|requirement| requirement.name == "peer");
        PEER.with_borrow_mut(|slot| {
            *slot = peer.map(|requirement| Rc::new(requirement.capability));
        });
        ROLE.with_borrow_mut(|slot| assert!(slot.replace("provider").is_none()));
        let unavailable = |what: &str, error: capability_bank::CapabilityError| {
            InitializationError::Unavailable(format!("publishing {what} failed: {error:?}"))
        };
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(Root),
            publications: vec![
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
            ],
        })
    }

    async fn call(publication: String, request: Vec<u8>) -> Result<Vec<u8>, String> {
        match publication.as_str() {
            ECHO => Ok(request),
            RELAY => {
                assert!(
                    text(&request)? != "trap",
                    "lifecycle fixture was told to trap"
                );
                let peer = PEER
                    .with_borrow(Clone::clone)
                    .ok_or_else(|| "the relay has no peer requirement".to_owned())?;
                let response = peer
                    .call(request)
                    .await
                    .map_err(|error| format!("peer call failed: {error:?}"))?;
                Ok(cbor(&format!("relay:{}", text(&response)?)))
            }
            other => Err(format!("lifecycle fixture serves no call {other:?}")),
        }
    }
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
