#![cfg(target_arch = "wasm32")]
//! Dispatch fixture: a guest that calls the node the way a micro does (`wasi:http/client` to
//! `http://dispatch.local`) and then tries what it must not be able to do (another origin,
//! another scheme, another verb or path, a forged identity header), when it initializes. It renders
//! one text node with each attempt's outcome, one `name=outcome` line per attempt, which
//! `clay_rt_wasmtime`'s tests compare against what a fake `MicroDispatch` saw.
//!
//! The calls are raw `wasi:http` on `clay_mdk::guest::dispatch`, not a typed method: the typed
//! `ctx.call::<M>(input)` rides the same transport once the node's `method_core` crates land.

use clay_mdk::bindings;
use clay_mdk::bindings::wasi::http::types::Scheme;
use clay_mdk::document_events;
use clay_mdk::documents;
use clay_mdk::guest::dispatch::DispatchRequest;
use clay_mdk::guest::dispatch::send_to;
use clay_mdk::micro_lifecycle;
use clay_mdk::presentation;
use clay_mdk::presentation_events::Event;
use clay_mdk::presentation_events::EventResponse;
use clay_mdk::presentation_events::{self};
use clay_mdk::wire::WireMutations;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;
use std::sync::OnceLock;

/// The attempt report, computed once in the async `initialize` (a `wasi:http` call is an async
/// import, which a sync `mount` export may not suspend on).
static REPORT: OnceLock<String> = OnceLock::new();

/// One attempt: where it goes, and what it sends.
struct Attempt {
    name: &'static str,
    scheme: Scheme,
    authority: &'static str,
    method: &'static str,
    path: &'static str,
    headers: Vec<(String, Vec<u8>)>,
    body: &'static [u8],
}

fn attempts() -> Vec<Attempt> {
    let to = |name: &'static str, method: &'static str, path: &'static str, body: &'static [u8]| {
        Attempt {
            name,
            scheme: Scheme::Http,
            authority: "dispatch.local",
            method,
            path,
            headers: Vec::new(),
            body,
        }
    };
    vec![
        Attempt {
            // The identity the host calls as is the micro's, whatever these claim.
            headers: vec![
                (
                    "x-micro-id".to_owned(),
                    b"did:example:mallory/other".to_vec(),
                ),
                ("authorization".to_owned(), b"Bearer stolen".to_vec()),
            ],
            ..to("echo", "POST", "/xrpc/test.echo", b"{\"n\":1}")
        },
        to("ping", "GET", "/xrpc/test.ping?x=1&y=%20", b""),
        to("delete", "DELETE", "/xrpc/test.echo", b""),
        to("put", "PUT", "/xrpc/test.echo", b""),
        to("other_path", "POST", "/other", b""),
        to("get_with_body", "GET", "/xrpc/test.ping", b"{}"),
        Attempt {
            authority: "example.com",
            ..to("other_host", "POST", "/xrpc/test.echo", b"{}")
        },
        Attempt {
            scheme: Scheme::Https,
            ..to("https", "POST", "/xrpc/test.echo", b"{}")
        },
        Attempt {
            authority: "dispatch.local:8080",
            ..to("port", "POST", "/xrpc/test.echo", b"{}")
        },
        Attempt {
            authority: "dispatch.local.example.com",
            ..to("suffix", "POST", "/xrpc/test.echo", b"{}")
        },
    ]
}

/// One line per attempt: `name=status:body` for an HTTP answer, `name=err(..)` for a request the
/// host refused before it was one.
async fn report() -> String {
    let mut lines = Vec::new();
    for attempt in attempts() {
        let outcome = send_to(
            attempt.scheme,
            attempt.authority,
            DispatchRequest {
                method: attempt.method.to_owned(),
                path_with_query: attempt.path.to_owned(),
                headers: attempt.headers,
                body: attempt.body.to_vec(),
            },
        )
        .await;
        lines.push(match outcome {
            Ok(response) => format!(
                "{}={}:{}",
                attempt.name,
                response.status,
                String::from_utf8_lossy(&response.body)
            ),
            Err(error) => format!("{}=err({error})", attempt.name),
        });
    }
    lines.join("\n")
}

struct Caller;

impl micro_lifecycle::Guest for Caller {
    type InstanceRoot = clay_mdk::StatelessRoot;

    async fn initialize(
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, micro_lifecycle::InitializationError> {
        REPORT.set(report().await).expect("initialize runs once");
        assert!(context.arguments.is_empty());
        assert!(context.configs.is_empty());
        assert!(context.collections.is_empty());
        assert!(context.registrations.is_empty());
        assert!(context.requirements.is_empty());
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(clay_mdk::StatelessRoot),
            publications: Vec::new(),
        })
    }

    async fn call(publication: String, _request: Vec<u8>) -> Result<Vec<u8>, String> {
        Err(format!("dispatch fixture serves no call {publication:?}"))
    }
}

impl presentation_events::Guest for Caller {
    fn mount(
        _publication: String,
        placement: &presentation::MountPoint,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text(REPORT.get().expect("initialize ran before mount"));
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

impl document_events::Guest for Caller {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("dispatch fixture owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("dispatch fixture owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("dispatch fixture submits no document operations");
    }
}

bindings::export!(Caller with_types_in bindings);
