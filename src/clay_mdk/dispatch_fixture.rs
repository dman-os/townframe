#![cfg(target_arch = "wasm32")]
//! The `dispatch` fixture: a micro that calls node methods with [`Context::call`] while it
//! activates and publishes one presentation, `view`, whose single text node reports what each
//! attempt returned (`name=ok(<answer>)` or `name=err(<reason>)`, one per line). The cross-crate
//! e2e of `clay_node` hosts it under Wasmtime against an in-process node, and pins:
//!
//! - `granted`: a call to a method the host granted this micro answers;
//! - `ungranted`: a call to a method it was not granted is refused by dispatch's own admission;
//! - `forged`: an `authorization` header the guest sets itself is dropped by the host, so a call
//!   that carries a made-up bearer still answers as the micro (the guest never holds capability
//!   bytes, and cannot substitute one);
//! - `refused`: a request to any authority other than `dispatch.local` is refused by the
//!   runtime, not routed anywhere;
//! - `resource`: the granted call made from the view's Dioxus `use_resource`, which Dioxus polls
//!   outside any component-model task, answers like `granted` (rendered once it resolves);
//! - `bytes`: [`Context::call_bytes`] from the view's `use_resource` reads a byte-body method of
//!   [`BLOB_OCTETS`] octets (many `wasi:http` body chunks), checks each octet against
//!   [`pattern`] as it arrives, and reports `ok(octets=<n>,chunks=<n>)`, or `err(...)` naming the
//!   first mismatch or the stream error. The body pump is spawned in the task that sent the
//!   request, which has ended by the time the view pulls.

use big_ndk::WasiHttp;
use big_ndk::wit::wasi::http::client;
use big_ndk::wit::wasi::http::types::Fields;
use big_ndk::wit::wasi::http::types::Method;
use big_ndk::wit::wasi::http::types::Request;
use big_ndk::wit::wasi::http::types::Scheme;
use clay_mdk::ComponentDecl;
use clay_mdk::Context;
use clay_mdk::Endpoint;
use clay_mdk::Entrypoints;
use clay_mdk::Error;
use clay_mdk::MicroManifest;
use clay_mdk::method_core::ContextError;
use dioxus::prelude::*;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_client_core_rs::Client;
use service_client_core_rs::xrpc::Xrpc;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::xrpc::bearer_header_value;

/// The answer of `echo`: its input back, which the node's test provider wraps with the actor it
/// verified the call was made by.
#[derive(Serialize, Deserialize)]
struct Echo {
    text: String,
}

/// A method that answers [`BLOB_OCTETS`] octets of [`pattern`] as a byte body.
#[derive(Serialize, Deserialize)]
struct Blob {}

/// The octets of the blob: several times the transport's body chunk (64 KiB), so the body cannot
/// arrive as one chunk. The host's `clay_node` e2e serves the same count and pattern.
const BLOB_OCTETS: usize = 1 << 20;

/// The octet at `offset` of the blob. Not periodic in a power of two, so a dropped, repeated or
/// reordered chunk shows at its first octet.
fn pattern(offset: usize) -> u8 {
    (offset % 251) as u8
}

/// What the view sleeps for through [`Context::sleep`].
const SLEEP: std::time::Duration = std::time::Duration::from_millis(5);

/// A method the micro is never granted.
#[derive(Serialize, Deserialize)]
struct Reveal {
    text: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum DispatchFixtureError {
    Context(ContextError),
}

impl std::fmt::Display for DispatchFixtureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self::Context(error) = self;
        error.fmt(formatter)
    }
}

impl From<ContextError> for DispatchFixtureError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}

clay_mdk::method_core::methods! {
    error = DispatchFixtureError, nsid_prefix = "org.example.clay.test.dispatch.";
    Echo => Value, procedure, "echo";
    Reveal => Value, procedure, "reveal";
    Blob => clay_mdk::method_core::ByteBody, procedure, "blob", bytes_output;
}

struct Report;

impl ComponentDecl for Report {
    const NAME: &'static str = "view";
}

fn manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        endpoints: [(Report::NAME.into(), Endpoint::View { implements: None })].into(),
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![],
        contributions: vec![],
        extra: Default::default(),
    }
}

fn outcome<T: std::fmt::Display, E: std::fmt::Display>(result: Result<T, E>) -> String {
    match result {
        Ok(answer) => format!("ok({answer})"),
        Err(error) => format!("err({error})"),
    }
}

/// A request to `example.com`, made with the raw `wasi:http` client: the typed client can only
/// address `dispatch.local`.
async fn other_origin() -> String {
    let (_trailers_tx, trailers_rx) = big_ndk::wit::wit_future::new(|| Ok(None));
    let (request, _completion) = Request::new(Fields::new(), None, trailers_rx, None);
    request.set_method(&Method::Get).expect("GET is a method");
    request
        .set_scheme(Some(&Scheme::Https))
        .expect("https is a scheme");
    request
        .set_authority(Some("example.com"))
        .expect("an authority");
    request.set_path_with_query(Some("/")).expect("a path");
    match client::send(request).await {
        Ok(_) => "ok".to_owned(),
        Err(error) => format!("err({error:?})"),
    }
}

/// Reads the blob through [`Context::call_bytes`] and reports what arrived.
async fn read_blob(cx: Context) -> String {
    let mut body = match cx.call_bytes(Blob {}).await {
        Ok(body) => body,
        Err(error) => return format!("err({error})"),
    };
    let (mut octets, mut chunks) = (0, 0);
    while let Some(chunk) = body.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => return format!("err(stream failed after {octets} octets: {error})"),
        };
        if let Some(at) = chunk
            .iter()
            .enumerate()
            .position(|(at, octet)| *octet != pattern(octets + at))
        {
            return format!("err(octet {} is out of place)", octets + at);
        }
        octets += chunk.len();
        chunks += 1;
    }
    format!("ok(octets={octets},chunks={chunks})")
}

async fn activate(cx: Context) -> Result<(), Error> {
    let granted = cx
        .call(Echo {
            text: "ping".to_owned(),
        })
        .await;
    let ungranted = cx
        .call(Reveal {
            text: "no".to_owned(),
        })
        .await;
    let forged = Client::new(Xrpc(WasiHttp))
        .call_with(
            RequestContext::root(),
            HttpMetadata {
                authorization: Some(bearer_header_value("00000000-0000-0000-0000-000000000000")),
                ..HttpMetadata::default()
            },
            Echo {
                text: "forged".to_owned(),
            },
        )
        .await;
    let report = [
        format!("granted={}", outcome(granted)),
        format!("ungranted={}", outcome(ungranted)),
        format!("forged={}", outcome(forged)),
        format!("refused={}", other_origin().await),
    ]
    .join("\n");
    let view_cx = cx.clone();
    cx.publish::<Report>(move || {
        // The same granted call from a view's Dioxus resource: Dioxus polls it from the
        // synchronous `mount` and `dispatch` exports, where no component-model task exists.
        let echo_cx = view_cx.clone();
        let resource = use_resource(move || {
            let cx = echo_cx.clone();
            async move {
                outcome(
                    cx.call(Echo {
                        text: "resource".to_owned(),
                    })
                    .await,
                )
            }
        });
        let bytes_cx = view_cx.clone();
        let bytes = use_resource(move || read_blob(bytes_cx.clone()));
        // A sleep from a view's resource: the wait runs on an owned task, which must mark the
        // views dirty when it ends, or this resource is never polled again and `slept` never
        // renders. The clock must also have advanced by at least the sleep.
        let slept_cx = view_cx.clone();
        let slept = use_resource(move || {
            let cx = slept_cx.clone();
            async move {
                let started = cx.now();
                cx.sleep(SLEEP).await;
                let waited = cx.now() - started;
                if waited >= SLEEP {
                    format!("ok(waited={waited:?})")
                } else {
                    format!("err(woke after {waited:?}, before the {SLEEP:?} asked)")
                }
            }
        });
        rsx! {
            "{report}"
            if let Some(answer) = resource() {
                "resource={answer}"
            }
            if let Some(answer) = bytes() {
                "bytes={answer}"
            }
            if let Some(answer) = slept() {
                "slept={answer}"
            }
        }
    })
}

clay_mdk::export_micro!(activate, manifest);
