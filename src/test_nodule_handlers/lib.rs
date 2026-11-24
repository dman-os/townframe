//! The handlers of the nodule runtime's test fixture, written on `big_ndk`. `test_nodule_guest`
//! exports them as a `wasi:http/handler@0.3.0` component for exercising the Wasm provider, and
//! `big_ndk_testing` serves the same [`Fixture`] natively, so both backends are checked against
//! one implementation. The crate has no wasm-only code, so it builds for both targets.
//!
//! The host dispatches XRPC calls to this component as plain HTTP requests, and the header
//! contract is the one every XRPC hop speaks (see `big_ndk::Cx`): `authorization: Bearer <caller
//! capability id>`, `idempotency-key`, the relative `x-timeout-ms` budget, `x-request-id`,
//! `traceparent`/`tracestate`, `x-nodule-capability` (the nodule's own dispatch capability
//! as a bare id, which the guest presents on its outgoing calls) and `x-nodule-actor-capability`
//! (its own actor identity capability, which it names in the calls that act as its actor).
//!
//! Routes (all `POST`), served for any NSID whose last segment is the route's name, so one
//! component can stand in for several nodules. `big_ndk::export_nodule!` routes by exact NSID,
//! which cannot say "any authority", so this fixture implements `big_ndk::Nodule` by hand and
//! matches the suffix itself:
//! - `/xrpc/<nsid>.echo` answers 200 with
//!   `{ input, authorization, idempotencyKey, timeoutMs, requestId, traceparent,
//!   noduleCapability, actorCapability }`, where the header members are `null` when absent and
//!   `timeoutMs` is the remaining budget in milliseconds as text.
//! - `/xrpc/<nsid>.relay` takes `{ operation, authority?, input }` and POSTs `input` to
//!   `http://dispatch.local/xrpc/<operation>` with `authorization: Bearer <authority>`, where
//!   `authority` defaults to the capability id in `x-nodule-capability`. The operation is
//!   dynamic, so the call goes through `Cx::transport` and `Cx::next_hop` rather than a typed
//!   `Cx::call`; an empty `operation` is the `InvalidRequest` domain error. It forwards the
//!   incoming `idempotency-key` and answers 200 with
//!   `{ status, body, authority }` (`body` is the upstream JSON, or a string when the upstream
//!   body is not JSON; `authority` is the capability id the call presented), or the `UpstreamFailed` domain error when the outgoing request fails. With a
//!   `query` it sends `GET /xrpc/<operation>?<query>` with no body instead, which is how a
//!   query is called.
//! - `/xrpc/<nsid>.adopt` takes `{ path }`: it opens the actor at `path` under the nodule's own
//!   actor (`openActor` with `x-nodule-actor-capability` as `parent`) and, when that opens,
//!   narrows the nodule's docs handle onto the new actor (`attenuateCapability { subject }`), both
//!   through dispatch. It answers 200 `{ open: { status, body }, attenuate: { status, body } | null }`.
//! - `/xrpc/<nsid>.fetch` takes `{ url, method?, headers? }` (`method` defaults to `GET`) and sends
//!   that request to the URL with `Cx::fetch`, answering 200 `{ status, body }` (`body` is the
//!   upstream JSON, or its text when it is not JSON), or the `UpstreamFailed` domain error when
//!   the request could not be sent. A refusal by the host is a `status` of 403 with its XRPC error
//!   as `body`.
//! - `/xrpc/<nsid>.ingest` takes a byte body and answers 200 with `{ chunks, octets, mime, sum }`:
//!   how many chunks the stream yielded to the guest, their total length, the request's
//!   `content-type` and the wrapping sum of every octet. It reads the stream one chunk at a time.
//! - `/xrpc/<nsid>.emit` answers a byte body of [`EMIT_CHUNKS`] chunks of [`EMIT_CHUNK_OCTETS`]
//!   octets (chunk `i` is all `i as u8`) as `application/x-emit`, produced lazily.
//! - `/xrpc/<nsid>.readfile` takes `{ path, write? }` and answers 200 with `{ found: true, length, sum }`
//!   for a file it reads in full (the length and the wrapping sum of its octets), or `{ found: false,
//!   kind }` where `kind` is the `std::io::ErrorKind` that stopped it. With `write` it only tries to
//!   open the file for writing and answers `{ opened, kind }`. This is how a test sees what a
//!   workload's working mount holds.
//! - `/xrpc/<nsid>.panic` panics with [`PANIC_MESSAGE`]. A Wasm guest prints it to stderr and traps
//!   on `unreachable`, which is what the host's guest-fault handling is tested against.
//! - Any other `/xrpc/` operation answers 404 `{ "error": "UnknownOperation" }` and any other
//!   path answers 404 `{ "error": "NotFound" }`.

use big_ndk::Cx;
use big_ndk::Handler;
use big_ndk::Nodule;
use big_ndk::Reply;
use big_ndk::XrpcBody;
use big_ndk::invoke;
use futures::StreamExt;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::header::CONTENT_TYPE;
use method_core::ByteBody;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_client_core_rs::xrpc::HttpTransport;

use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_client_core_rs::xrpc::write_context_headers;

use service_core_rs::HttpMetadata;
use service_core_rs::xrpc::bearer_header_value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, Serialize, Deserialize)]
enum FixtureError {
    Context(ContextError),
    InvalidRequest(String),
    UpstreamFailed(String),
    Config(String),
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(error) => error.fmt(f),
            Self::InvalidRequest(reason) => write!(f, "invalid request: {reason}"),
            Self::Config(error) => write!(f, "config: {error}"),
            Self::UpstreamFailed(reason) => write!(f, "upstream failed: {reason}"),
        }
    }
}

impl From<ContextError> for FixtureError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}

/// Any JSON the caller sent, answered with what the guest saw.
#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct Echo(Value);

#[derive(Debug, Serialize, Deserialize)]
struct Relay {
    operation: String,
    authority: Option<String>,
    input: Option<Value>,
    /// The percent-encoded query string of a `GET` to `operation`, which is how a query is
    /// called. Absent, `input` is POSTed as a procedure's body.
    query: Option<String>,
}

/// Opens the actor at `path` under the nodule's own actor and narrows the nodule's docs handle
/// onto it, both through dispatch.
#[derive(Debug, Serialize, Deserialize)]
struct Adopt {
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct Config(Value);

#[derive(Debug, Serialize, Deserialize)]
struct ReadFile {
    path: String,
    #[serde(default)]
    write: bool,
}

// The NSIDs are bare route names: `Fixture::route` matches the last segment of whatever
// authority the cartridge declared, so these `XRPC.nsid`s are never compared.
method_core::methods! {
    error = FixtureError, nsid_prefix = "";
    Echo => Value, procedure, "echo";
    Relay => Value, procedure, "relay";
    Config => Value, procedure, "config";
    ReadFile => Value, procedure, "readfile";
    Adopt => Value, procedure, "adopt";
}

/// `fetch`: one request to a URL that is not dispatch, through `Cx::fetch`.
async fn fetch(cx: &Cx, body: XrpcBody) -> Reply {
    #[derive(Deserialize)]
    struct Fetch {
        url: String,
        method: Option<String>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    }
    let reply = |status: http::StatusCode, body: Value| Reply {
        status,
        body: XrpcBody::json(serde_json::to_vec(&body).expect("a JSON value serializes")),
    };
    let XrpcBody::Json(body) = body else {
        return reply(
            http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            json!({"error": "InvalidRequest", "message": "fetch takes a JSON body"}),
        );
    };
    let Ok(Fetch {
        url,
        method,
        headers,
    }) = serde_json::from_slice(&body)
    else {
        return reply(
            http::StatusCode::BAD_REQUEST,
            json!({"error": "InvalidRequest", "message": "fetch takes { url, method?, headers? }"}),
        );
    };
    let method = method.map_or(Method::GET, |method| {
        Method::from_bytes(method.as_bytes()).expect("a test names a valid method")
    });
    let mut sent = HeaderMap::new();
    for (name, value) in headers {
        sent.insert(
            http::HeaderName::from_bytes(name.as_bytes()).expect("a test names a valid header"),
            HeaderValue::from_str(&value).expect("a test gives a valid header value"),
        );
    }
    match cx
        .fetch(method, &url, sent, XrpcBody::json(Vec::new()))
        .await
    {
        Ok(response) => {
            let body = match response.body {
                XrpcBody::Json(body) => body,
                XrpcBody::Bytes(mut stream) => {
                    let mut body = Vec::new();
                    while let Some(chunk) = stream.next().await {
                        body.extend_from_slice(&chunk.expect("an upstream body chunk arrives"));
                    }
                    body
                }
            };
            reply(
                http::StatusCode::OK,
                json!({
                    "status": response.status.as_u16(),
                    "body": serde_json::from_slice::<Value>(&body)
                        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned())),
                }),
            )
        }
        Err(error) => reply(
            http::StatusCode::BAD_GATEWAY,
            json!({"error": "UpstreamFailed", "message": error.message}),
        ),
    }
}

/// What the `panic` route panics with.
pub const PANIC_MESSAGE: &str = "the nodule fixture panicked on purpose";

/// How many chunks `emit` answers, and how long each is.
pub const EMIT_CHUNKS: usize = 4;
pub const EMIT_CHUNK_OCTETS: usize = 64 * 1024;

/// The fixture nodule: `echo` and `relay` under any authority.
#[derive(Default, Clone)]
pub struct Fixture;

/// Consumes a byte body chunk by chunk, keeping only the running totals.
async fn ingest(body: XrpcBody) -> Reply {
    let XrpcBody::Bytes(mut body) = body else {
        return Reply {
            status: http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            body: XrpcBody::json(
                br#"{"error":"InvalidRequest","message":"ingest takes a byte body"}"#.to_vec(),
            ),
        };
    };
    let mime = body.mime.clone();
    let (mut chunks, mut octets, mut sum) = (0u64, 0u64, 0u64);
    while let Some(chunk) = body.next().await {
        let chunk = chunk.expect("the host's request body stream does not fail");
        chunks += 1;
        octets += chunk.len() as u64;
        sum = chunk
            .iter()
            .fold(sum, |sum, octet| sum.wrapping_add(u64::from(*octet)));
    }
    Reply {
        status: http::StatusCode::OK,
        body: XrpcBody::json(
            serde_json::to_vec(&json!({
                "chunks": chunks,
                "octets": octets,
                "mime": mime,
                "sum": sum,
            }))
            .expect("a JSON value serializes"),
        ),
    }
}

/// A lazily produced byte answer of [`EMIT_CHUNKS`] chunks.
fn emit() -> Reply {
    let chunks = (0..EMIT_CHUNKS).map(|index| {
        Ok::<_, std::io::Error>(bytes::Bytes::from(vec![index as u8; EMIT_CHUNK_OCTETS]))
    });
    Reply {
        status: http::StatusCode::OK,
        body: XrpcBody::Bytes(
            ByteBody::new(futures::stream::iter(chunks))
                .with_mime("application/x-emit")
                .with_length((EMIT_CHUNKS * EMIT_CHUNK_OCTETS) as u64),
        ),
    }
}

/// `put` and `read`: forwards the call to the node's blobs service through dispatch, streaming the
/// body each way. A put presents the blobs handle the routine was delivered for its `blobs.put`
/// permbind (ADR 049 §6.2); a read's query names the handle for the blob it reads (`capability`
/// and `blobId`), which grants decide. The guest authenticates to dispatch with its own nodule
/// capability, as any hop does.
async fn forward_blob(cx: &Cx, nsid: &str, body: XrpcBody) -> Reply {
    let invalid = |message: &str| Reply {
        status: http::StatusCode::BAD_REQUEST,
        body: XrpcBody::json(
            serde_json::to_vec(&json!({"error": "InvalidRequest", "message": message}))
                .expect("a JSON value serializes"),
        ),
    };
    let (operation, query) = nsid.split_once('?').unwrap_or((nsid, ""));
    let is_put = operation.ends_with(".put");
    let query = if is_put {
        let Some(blobs) = cx.blobs_capability() else {
            return invalid("no blobs capability was delivered for put");
        };
        format!("capability={blobs}")
    } else if query.is_empty() {
        return invalid("a blob read needs query parameters");
    } else {
        query.to_owned()
    };
    let Some(authority) = cx.nodule_capability() else {
        return invalid("no nodule capability");
    };
    let mut headers = HeaderMap::new();
    write_context_headers(
        &mut headers,
        &cx.next_hop(),
        &HttpMetadata {
            authorization: Some(bearer_header_value(authority)),
            idempotency_key: cx.idempotency_key().map(str::to_owned),
            range: None,
        },
    )
    .expect("context headers are valid HTTP");
    let request = XrpcRequest {
        method: if is_put { Method::POST } else { Method::GET },
        path: format!(
            "xrpc/org.example.daybook.blobs.{}?{query}",
            if is_put { "put" } else { "read" }
        ),
        headers,
        body: if is_put {
            body
        } else {
            XrpcBody::json(Vec::new())
        },
    };
    match cx.transport().send(request).await {
        Ok(XrpcResponse { status, body, .. }) => Reply { status, body },
        Err(error) => Reply {
            status: http::StatusCode::BAD_GATEWAY,
            body: XrpcBody::json(
                serde_json::to_vec(&json!({
                    "error": "UpstreamFailed",
                    "message": error.message,
                }))
                .expect("a JSON error serializes"),
            ),
        },
    }
}

impl Nodule for Fixture {
    async fn route(&self, nsid: &str, cx: &Cx, body: XrpcBody) -> Option<Reply> {
        match nsid
            .split('?')
            .next()
            .unwrap_or_default()
            .rsplit('.')
            .next()
            .unwrap_or_default()
        {
            "echo" => Some(invoke::<Self, Echo>(self, cx, body).await),
            "relay" => Some(invoke::<Self, Relay>(self, cx, body).await),
            "put" | "read" => Some(forward_blob(cx, nsid, body).await),
            "fetch" => Some(fetch(cx, body).await),
            "ingest" => Some(ingest(body).await),
            "emit" => Some(emit()),
            "panic" => panic!("{PANIC_MESSAGE}"),
            "config" => Some(invoke::<Self, Config>(self, cx, body).await),
            "readfile" => Some(invoke::<Self, ReadFile>(self, cx, body).await),
            "adopt" => Some(invoke::<Self, Adopt>(self, cx, body).await),
            _ => None,
        }
    }
}

impl Handler<ReadFile> for Fixture {
    async fn call(&self, _cx: &Cx, input: ReadFile) -> Result<Value, FixtureError> {
        if input.write {
            return Ok(
                match std::fs::OpenOptions::new().write(true).open(&input.path) {
                    Ok(_) => json!({ "opened": true }),
                    Err(error) => json!({ "opened": false, "kind": format!("{:?}", error.kind()) }),
                },
            );
        }
        Ok(match std::fs::read(&input.path) {
            Ok(bytes) => json!({
                "found": true,
                "length": bytes.len(),
                "sum": bytes.iter().fold(0u64, |sum, octet| sum.wrapping_add(u64::from(*octet))),
            }),
            Err(error) => json!({ "found": false, "kind": format!("{:?}", error.kind()) }),
        })
    }
}

impl Handler<Echo> for Fixture {
    async fn call(&self, cx: &Cx, input: Echo) -> Result<Value, FixtureError> {
        let request = cx.request();
        Ok(json!({
            "input": input.0,
            "authorization": cx.caller_capability().map(bearer_header_value),
            "idempotencyKey": cx.idempotency_key(),
            "timeoutMs": request.remaining_budget().map(|budget| budget.as_millis().to_string()),
            "requestId": request.request_id,
            "traceparent": request.trace.traceparent(),
            "noduleCapability": cx.nodule_capability(),
            "actorCapability": cx.actor_capability(),
        }))
    }
}
#[derive(Debug, Deserialize)]

struct ConfigRequest {
    action: String,
    tag: Option<String>,
    slot: Option<String>,
    value: Option<Value>,
}

impl Handler<Config> for Fixture {
    async fn call(&self, cx: &Cx, input: Config) -> Result<Value, FixtureError> {
        let request: ConfigRequest = serde_json::from_value(input.0)
            .map_err(|error| FixtureError::InvalidRequest(error.to_string()))?;
        match request.action.as_str() {
            "capability" => Ok(json!({
                "docId": cx.config_doc_id(),
                "capability": cx.config_capability(),
            })),
            "read" => cx
                .read_config(None)
                .await
                .map(|read| read.get("datums").cloned().unwrap_or_else(|| json!({})))
                .map_err(|error| FixtureError::Config(error.to_string())),
            "write" => {
                let tag = request
                    .tag
                    .as_deref()
                    .ok_or_else(|| FixtureError::InvalidRequest("config tag is required".into()))?;
                let slot = request.slot.as_deref().ok_or_else(|| {
                    FixtureError::InvalidRequest("config slot is required".into())
                })?;
                let key = format!("{tag}/{slot}");
                let value = request.value.ok_or_else(|| {
                    FixtureError::InvalidRequest("config value is required".into())
                })?;
                cx.write_config(BTreeMap::from([(key, value)]), BTreeSet::new())
                    .await
                    .map(|receipt| serde_json::to_value(receipt).expect("write receipt serializes"))
                    .map_err(|error| FixtureError::Config(error.to_string()))
            }
            action => Err(FixtureError::InvalidRequest(format!(
                "unknown config action `{action}`"
            ))),
        }
    }
}

/// Sends `request` through `dispatch.local` and reads the upstream answer as `{ status, body }`,
/// the body being its JSON or, when it is not JSON, its text.
async fn send_to_dispatch(cx: &Cx, request: XrpcRequest) -> Result<(u16, Value), FixtureError> {
    let response = cx
        .transport()
        .send(request)
        .await
        .map_err(|error| FixtureError::UpstreamFailed(error.message))?;
    let body = match response.body {
        XrpcBody::Json(body) => serde_json::from_slice::<Value>(&body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned())),
        XrpcBody::Bytes(_) => {
            return Err(FixtureError::UpstreamFailed(
                "the fixture cannot relay a byte body yet".to_owned(),
            ));
        }
    };
    Ok((response.status.as_u16(), body))
}

/// The headers of a hop to dispatch presenting `authority` as the bearer, forwarding the incoming
/// idempotency key.
fn dispatch_headers(cx: &Cx, authority: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    write_context_headers(
        &mut headers,
        &cx.next_hop(),
        &HttpMetadata {
            authorization: Some(bearer_header_value(authority)),
            idempotency_key: cx.idempotency_key().map(str::to_owned),
            range: None,
        },
    )
    .expect("context headers are valid HTTP");
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers
}

impl Handler<Relay> for Fixture {
    async fn call(&self, cx: &Cx, input: Relay) -> Result<Value, FixtureError> {
        if input.operation.is_empty() {
            return Err(FixtureError::InvalidRequest(
                "no operation to relay".to_owned(),
            ));
        }
        let authority = input
            .authority
            .as_deref()
            .or(cx.nodule_capability())
            .ok_or_else(|| {
                FixtureError::InvalidRequest("no authority and no nodule capability".to_owned())
            })?;
        let headers = dispatch_headers(cx, authority);
        let request = match &input.query {
            Some(query) => XrpcRequest {
                method: Method::GET,
                path: format!("xrpc/{}?{query}", input.operation),
                headers,
                body: XrpcBody::json(Vec::new()),
            },
            None => XrpcRequest {
                method: Method::POST,
                path: format!("xrpc/{}", input.operation),
                headers,
                body: XrpcBody::json(
                    serde_json::to_vec(input.input.as_ref().unwrap_or(&Value::Null))
                        .expect("serializing a Value cannot fail"),
                ),
            },
        };
        let (status, body) = send_to_dispatch(cx, request).await?;
        Ok(json!({
            "status": status,
            "body": body,
            "authority": authority,
        }))
    }
}

impl Handler<Adopt> for Fixture {
    async fn call(&self, cx: &Cx, input: Adopt) -> Result<Value, FixtureError> {
        let (Some(actor), Some(authority), Some(docs)) = (
            cx.actor_capability(),
            cx.nodule_capability(),
            cx.config_capability(),
        ) else {
            return Err(FixtureError::InvalidRequest(
                "no actor, nodule and docs capabilities".to_owned(),
            ));
        };
        // Both calls present the nodule's dispatch handle as the authority. The actor identity
        // capability is the `parent` of the open, and the docs handle is what the attenuation
        // narrows onto the child. The egress signs as the same actor, so the authority's holder
        // binding passes.
        let post = |operation: &'static str, body: Value| {
            send_to_dispatch(
                cx,
                XrpcRequest {
                    method: Method::POST,
                    path: format!("xrpc/org.example.daybook.authority.{operation}"),
                    headers: dispatch_headers(cx, authority),
                    body: XrpcBody::json(
                        serde_json::to_vec(&body).expect("serializing a Value cannot fail"),
                    ),
                },
            )
        };
        let (status, opened) = post(
            "openActor",
            json!({ "parent": actor, "path": input.path, "rights": ["use"] }),
        )
        .await?;
        let attenuated = match opened.get("actorId") {
            Some(child) => {
                let (status, body) = post(
                    "attenuateCapability",
                    json!({ "capability": docs, "rights": ["use"], "subject": child }),
                )
                .await?;
                Some(json!({ "status": status, "body": body }))
            }
            None => None,
        };
        Ok(json!({
            "open": { "status": status, "body": opened },
            "attenuate": attenuated,
        }))
    }
}
