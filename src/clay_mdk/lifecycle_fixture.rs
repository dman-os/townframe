#![cfg(target_arch = "wasm32")]
//! ADR 024 lifecycle fixture: one package that plays a provider/consumer activation graph, its
//! role picked by the configuration-supplied CBOR text in `initial-context.arguments`.
//!
//! * `provider` publishes a presentation component and provides it as the
//!   `dayframe.lifecycle.presentation` contract.
//! * `failing-provider` publishes the same capability and then rejects initialization, so the
//!   host must discard a publication that was never visible.
//! * `consumer` redeems its `presentation` requirement during initialization and keeps the
//!   proxy in its root. Its `close` redeems the proxy again: it fails unless the provider is
//!   still live while the consumer closes, which is the close-order guarantee under test.
//!
//! The package embeds `lifecycle_fixture.manifest.json` as its `dayframe.manifest` section.

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

const MANIFEST: &[u8] = include_bytes!("lifecycle_fixture.manifest.json");

#[used]
#[unsafe(link_section = "dayframe.manifest")]
static DAYFRAME_MANIFEST: [u8; MANIFEST.len()] = *include_bytes!("lifecycle_fixture.manifest.json");

const PRESENTATION_CONTRACT: &str = "dayframe.lifecycle.presentation";

thread_local! {
    static ROLE: RefCell<Option<String>> = const { RefCell::new(None) };
}

struct Lifecycle;

enum Root {
    Provider,
    Consumer(capability_bank::PresentationComponent),
}

impl micro_lifecycle::GuestInstanceRoot for Root {
    async fn close(&self) -> Result<(), String> {
        match self {
            Root::Provider => Ok(()),
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
            "provider" => micro_lifecycle::Initialized {
                root: micro_lifecycle::InstanceRoot::new(Root::Provider),
                provisions: vec![micro_lifecycle::Provision {
                    contract: PRESENTATION_CONTRACT.into(),
                    capability: publish()?,
                }],
            },
            "failing-provider" => {
                let _staged = publish()?;
                return Err(InitializationError::Rejected(
                    "failing provider rejects its initialization".into(),
                ));
            }
            "consumer" => {
                let requirement = context
                    .requirements
                    .into_iter()
                    .find(|requirement| requirement.name == "presentation")
                    .ok_or_else(|| {
                        InitializationError::Unavailable("presentation requirement unbound".into())
                    })?;
                let proxy =
                    requirement
                        .capability
                        .as_presentation_component()
                        .map_err(|error| {
                            InitializationError::Unavailable(format!(
                                "presentation requirement unredeemable: {error:?}"
                            ))
                        })?;
                proxy.component().map_err(|error| {
                    InitializationError::Unavailable(format!(
                        "presentation requirement withdrawn: {error:?}"
                    ))
                })?;
                micro_lifecycle::Initialized {
                    root: micro_lifecycle::InstanceRoot::new(Root::Consumer(proxy)),
                    provisions: Vec::new(),
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
}

fn publish() -> Result<capability_bank::PublishedCapability, InitializationError> {
    capability_bank::publish_presentation_component().map_err(|error| {
        InitializationError::Unavailable(format!("publishing presentation failed: {error:?}"))
    })
}

impl presentation_events::Guest for Lifecycle {
    fn mount(
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
