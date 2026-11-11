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
//!   `Cx::call`; it forwards the incoming `idempotency-key` and answers 200 with
//!   `{ status, body }` (`body` is the upstream JSON, or a string when the upstream body is
//!   not JSON), or the `UpstreamFailed` domain error when the outgoing request fails. With a
//!   `query` it sends `GET /xrpc/<operation>?<query>` with no body instead, which is how a
//!   query is called.
//! - Any other `/xrpc/` operation answers 404 `{ "error": "UnknownOperation" }` and any other
//!   path answers 404 `{ "error": "NotFound" }`.

use big_ndk::Cx;
use big_ndk::Handler;
use big_ndk::Nodule;
use big_ndk::Reply;
use big_ndk::invoke;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::header::CONTENT_TYPE;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::write_context_headers;

use service_core_rs::HttpMetadata;
use service_core_rs::xrpc::bearer_header_value;

#[derive(Debug, Serialize, Deserialize)]
enum FixtureError {
    Context(ContextError),
    InvalidRequest(String),
    UpstreamFailed(String),
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(error) => error.fmt(f),
            Self::InvalidRequest(reason) => write!(f, "invalid request: {reason}"),
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

// The NSIDs are bare route names: `Fixture::route` matches the last segment of whatever
// authority the cartridge declared, so these `XRPC.nsid`s are never compared.
method_core::methods! {
    error = FixtureError, nsid_prefix = "";
    Echo => Value, procedure, "echo";
    Relay => Value, procedure, "relay";
}

/// The fixture nodule: `echo` and `relay` under any authority.
#[derive(Default, Clone)]
pub struct Fixture;

impl Nodule for Fixture {
    async fn route(&self, nsid: &str, cx: &Cx, body: &[u8]) -> Option<Reply> {
        match nsid.rsplit('.').next().unwrap_or_default() {
            "echo" => Some(invoke::<Self, Echo>(self, cx, body).await),
            "relay" => Some(invoke::<Self, Relay>(self, cx, body).await),
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

impl Handler<Relay> for Fixture {
    async fn call(&self, cx: &Cx, input: Relay) -> Result<Value, FixtureError> {
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
            },
        )
        .expect("context headers are valid HTTP");
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let request = match &input.query {
            Some(query) => XrpcRequest {
                method: Method::GET,
                path: format!("xrpc/{}?{query}", input.operation),
                headers,
                body: Vec::new(),
            },
            None => XrpcRequest {
                method: Method::POST,
                path: format!("xrpc/{}", input.operation),
                headers,
                body: serde_json::to_vec(input.input.as_ref().unwrap_or(&Value::Null))
                    .expect("serializing a Value cannot fail"),
            },
        };
        let response = cx
            .transport()
            .send(request)
            .await
            .map_err(|error| FixtureError::UpstreamFailed(error.message))?;
        let body = serde_json::from_slice::<Value>(&response.body).unwrap_or_else(|_| {
            Value::String(String::from_utf8_lossy(&response.body).into_owned())
        });
        Ok(json!({ "status": response.status.as_u16(), "body": body }))
    }
}
