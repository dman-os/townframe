//! The `dispatch.local` egress, independent of what hosts the handler: the only outgoing request
//! a nodule's handler can make.
//!
//! `GET /xrpc/<nsid>?<params>` (a query) or `POST /xrpc/<nsid>` (a procedure), as the typed
//! client sends them, with `authorization: Bearer <capability id>`, is invoked through dispatch
//! with the calling workload's nodule actor as the signer. A GET reaches dispatch as the same
//! [`HttpForm::Get`] the node's XRPC listener (`big_dispatch::xrpc_router`) hands it, so the
//! query string is decoded by the method's Lexicon `parameters` in one place, and a verb that
//! is not the method's kind is refused by dispatch as it is for the listener. Dispatch then admits the call as it
//! admits any other: the capability must be live, permit the NSID and be bound to that actor, so
//! a handler that presents another nodule's capability id is denied there. Nothing else the
//! handler says (headers, body) is trusted as identity. The trace and `x-timeout-ms` budget
//! continue into the nested call, so it stays inside the outer call's budget; its request id is
//! new. An executor adapts its transport to [`call`] and supplies the signer from its own table,
//! so every executor authorizes a handler's calls the same way.

use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use method_core::ContextError;
use serde_json::Value;
use serde_json::json;
use service_client_native_rs::Client;
use service_core_rs::xrpc::bearer_token;
use service_core_rs::xrpc::request_context;
use service_utils_rs::ClientError;
use service_utils_rs::HttpMetadata;

/// Runs a handler's outgoing request through dispatch. A malformed request is answered like an
/// XRPC server would, not refused as a transport failure.
///
/// `signer` is the calling workload's own actor capability, from the executor's table: never
/// anything the handler said. `None` means the workload has been stopped (its nodule disabled,
/// upgraded or retired) while one of its requests was still running, so it has no identity left
/// to call with. That is a lifecycle race, not a broken invariant, and is answered the way a
/// proxy answers a call from a drained upstream rather than crashing the node.
pub async fn call(
    dispatch: &Client<DispatchProtocol>,
    signer: Option<CapId>,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> (StatusCode, Value) {
    let bad = |status: StatusCode, error: &str, message: &str| {
        (status, json!({ "error": error, "message": message }))
    };
    let Some(target) = path.strip_prefix("/xrpc/") else {
        return bad(
            StatusCode::NOT_FOUND,
            "NotFound",
            "dispatch.local serves GET and POST /xrpc/<nsid> only",
        );
    };
    // The query string stays percent-encoded: dispatch decodes it, as it does for the listener.
    let (operation, query) = target.split_once('?').unwrap_or((target, ""));
    let http = match *method {
        Method::GET => {
            if !body.is_empty() {
                return bad(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "query operations must not carry a request body",
                );
            }
            HttpForm::Get {
                query: query.to_owned(),
            }
        }
        Method::POST => HttpForm::Post,
        _ => {
            return bad(
                StatusCode::METHOD_NOT_ALLOWED,
                "InvalidRequest",
                "dispatch.local serves GET and POST /xrpc/<nsid> only",
            );
        }
    };
    let operation = operation.to_owned();
    // The same parser every XRPC server hop uses: trace, the budget from the relative
    // `x-timeout-ms`, `authorization` and `idempotency-key`. The nested call is a hop of its
    // own: a fresh request id (the handler's id is the delivery hop's, and the outer call is
    // still in flight under its own) and a child span of the incoming trace.
    let (request_context, metadata) = match request_context(headers) {
        Ok((parsed, metadata)) => (parsed.next_hop(), metadata),
        Err(message) => return bad(StatusCode::BAD_REQUEST, "InvalidRequest", &message),
    };
    // The token is the bare capability id the handler was delivered. Never the
    // `<authority>.<signer>` form: the signer is the workload's own.
    let Some(authority) = metadata
        .authorization
        .as_deref()
        .and_then(bearer_token)
        .and_then(|id| serde_json::from_value::<CapId>(Value::String(id.to_owned())).ok())
    else {
        return bad(
            StatusCode::UNAUTHORIZED,
            "AuthRequired",
            "an `authorization: Bearer <capability id>` header is required",
        );
    };
    let idempotency_key = metadata.idempotency_key;
    let input = if body.is_empty() {
        Value::Null
    } else if let Ok(input) = serde_json::from_slice(body) {
        input
    } else {
        return bad(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "the request body is not JSON",
        );
    };
    let Some(signer) = signer else {
        return bad(
            StatusCode::SERVICE_UNAVAILABLE,
            "WorkloadStopped",
            "the calling handler has been stopped",
        );
    };
    let result = dispatch
        .call_with(
            request_context,
            HttpMetadata {
                idempotency_key: idempotency_key.clone(),
                ..Authorization { authority, signer }.metadata()
            },
            Invoke {
                operation,
                input,
                context: InvocationContext {
                    idempotency_key,
                    http: Some(http),
                },
            },
        )
        .await;
    match result {
        Ok(output) => (StatusCode::OK, output.output),
        Err(ClientError::Method(error)) => dispatch_error_response(error),
        Err(ClientError::Transport { message, .. }) => {
            panic!("in-process dispatch transport failed: {message}")
        }
    }
}

/// The HTTP form of a dispatch failure, which is the inverse of
/// [`crate::wire::provider_result`]'s mapping of a handler's response onto dispatch's errors:
/// denied 403, unknown operation 404, not ready 503, invalid input 400 and an output off its Lexicon 500 as an XRPC
/// `{ error, message }`, a provider's own error as it came, a deadline 504. The failures of the
/// services behind dispatch are gateway errors.
fn dispatch_error_response(error: DispatchError) -> (StatusCode, Value) {
    let named = |status: StatusCode, name: &str| {
        (
            status,
            json!({ "error": name, "message": error.to_string() }),
        )
    };
    match &error {
        DispatchError::Denied => named(StatusCode::FORBIDDEN, "Denied"),
        DispatchError::UnknownOperation => named(StatusCode::NOT_FOUND, "UnknownOperation"),
        DispatchError::NotReady { .. } => named(StatusCode::SERVICE_UNAVAILABLE, "NotReady"),
        DispatchError::InvalidInput(message) => (
            StatusCode::BAD_REQUEST,
            json!({ "error": "InvalidRequest", "message": message }),
        ),
        DispatchError::InvalidOutput(_) => {
            named(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError")
        }
        DispatchError::Context(ContextError::DeadlineExceeded) => {
            named(StatusCode::GATEWAY_TIMEOUT, "DeadlineExceeded")
        }
        DispatchError::Provider { status, body } => (
            StatusCode::from_u16(*status).expect("a provider answered a valid status"),
            body.clone(),
        ),
        DispatchError::AuthorityUnavailable(_) => {
            named(StatusCode::SERVICE_UNAVAILABLE, "AuthorityUnavailable")
        }
        DispatchError::Context(_) | DispatchError::Authority(_) => {
            named(StatusCode::BAD_GATEWAY, "BadGateway")
        }
    }
}
