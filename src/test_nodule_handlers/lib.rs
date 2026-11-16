//! The handlers of the nodule runtime's test fixture, written on `big_ndk`. `test_nodule_guest`
//! exports them as a `wasi:http/handler@0.3.0` component for exercising the Wasm provider, and
//! `big_ndk_testing` serves the same [`Fixture`] natively, so both backends are checked against
//! one implementation. The crate has no wasm-only code, so it builds for both targets.
//!
//! The host dispatches XRPC calls to this component as plain HTTP requests, and the header
//! contract is the one every XRPC hop speaks (see `big_ndk::Cx`): `authorization: Bearer <caller
//! capability id>`, `idempotency-key`, the relative `x-timeout-ms` budget, `x-request-id`,
//! `traceparent`/`tracestate`, and `x-nodule-capability` (the nodule's own dispatch capability
//! as a bare id, which the guest presents on its outgoing calls).
//!
//! Routes (all `POST`), served for any NSID whose last segment is the route's name, so one
//! component can stand in for several nodules. `big_ndk::export_nodule!` routes by exact NSID,
//! which cannot say "any authority", so this fixture implements `big_ndk::Nodule` by hand and
//! matches the suffix itself:
//! - `/xrpc/<nsid>.echo` answers 200 with
//!   `{ input, authorization, idempotencyKey, timeoutMs, requestId, traceparent,
//!   noduleCapability }`, where the header members are `null` when absent and `timeoutMs` is
//!   the remaining budget in milliseconds as text.
//! - `/xrpc/<nsid>.relay` takes `{ operation, authority?, input }` and POSTs `input` to
//!   `http://dispatch.local/xrpc/<operation>` with `authorization: Bearer <authority>`, where
//!   `authority` defaults to the capability id in `x-nodule-capability`. The operation is
//!   dynamic, so the call goes through `Cx::transport` and `Cx::next_hop` rather than a typed
//!   `Cx::call`; an empty `operation` is the `InvalidRequest` domain error. It forwards the
//!   incoming `idempotency-key` and answers 200 with
//!   `{ status, body }` (`body` is the upstream JSON, or a string when the upstream body is
//!   not JSON), or the `UpstreamFailed` domain error when the outgoing request fails. With a
//!   `query` it sends `GET /xrpc/<operation>?<query>` with no body instead, which is how a
//!   query is called.
//! - `/xrpc/<nsid>.ingest` takes a byte body and answers 200 with `{ chunks, octets, mime, sum }`:
//!   how many chunks the stream yielded to the guest, their total length, the request's
//!   `content-type` and the wrapping sum of every octet. It reads the stream one chunk at a time.
//! - `/xrpc/<nsid>.emit` answers a byte body of [`EMIT_CHUNKS`] chunks of [`EMIT_CHUNK_OCTETS`]
//!   octets (chunk `i` is all `i as u8`) as `application/x-emit`, produced lazily.
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct Config(Value);

// The NSIDs are bare route names: `Fixture::route` matches the last segment of whatever
// authority the cartridge declared, so these `XRPC.nsid`s are never compared.
method_core::methods! {
    error = FixtureError, nsid_prefix = "";
    Echo => Value, procedure, "echo";
    Relay => Value, procedure, "relay";
    Config => Value, procedure, "config";
}

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
/// body each way. The query string names the blobs handle (`capability`, and `blobId` for a
/// read), which is a parameter of the blobs method; the guest authenticates to dispatch with its
/// own nodule capability, as any hop does.
async fn forward_blob(cx: &Cx, nsid: &str, body: XrpcBody) -> Reply {
    let invalid = |message: &str| Reply {
        status: http::StatusCode::BAD_REQUEST,
        body: XrpcBody::json(
            serde_json::to_vec(&json!({"error": "InvalidRequest", "message": message}))
                .expect("a JSON value serializes"),
        ),
    };
    let Some((operation, query)) = nsid.split_once('?') else {
        return invalid("blob calls need query parameters");
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
    let is_put = operation.ends_with(".put");
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
            "ingest" => Some(ingest(body).await),
            "emit" => Some(emit()),
            "config" => Some(invoke::<Self, Config>(self, cx, body).await),
            _ => None,
        }
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
        Ok(json!({ "status": response.status.as_u16(), "body": body }))
    }
}
