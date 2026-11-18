//! The `dispatch.local` egress, independent of what hosts the handler: the only outgoing request
//! a nodule's handler can make.
//!
//! `GET /xrpc/<nsid>?<params>` (a query) or `POST /xrpc/<nsid>` (a procedure), as the typed
//! client sends them, with `authorization: Bearer <capability id>`, is invoked through dispatch
//! with the calling workload's nodule actor as the signer. A GET reaches dispatch as the same
//! [`HttpForm::Get`] the node's XRPC listener (`big_dispatch::xrpc_router`) hands it, so the
//! query string is decoded by the method's Lexicon `parameters` in one place, and a verb that
//! is not the method's kind is refused by dispatch as it is for the listener. A guest's bodies
//! carry either application/json bytes or pull-based wasi:http byte streams (ADR 042 §2). Byte
//! methods receive their parameters from the query string beside the stream, and byte answers
//! preserve their MIME and known length. The call's origin is [`Origin::Remote`], never inherited
//! from whoever the nodule is serving, so a local-only method is refused (ADR 042 §3). Dispatch then admits the call as it
//! admits any other: the capability must be live, permit the NSID and be bound to that actor, so
//! a handler that presents another nodule's capability id is denied there. Nothing else the
//! handler says (headers, body) is trusted as identity. The trace and `x-timeout-ms` budget
//! continue into the nested call, so it stays inside the outer call's budget; its request id is
//! new. An executor adapts its transport to [`call`] and supplies the signer from its own table,
//! so every executor authorizes a handler's calls the same way.
//!
//! A request to any other URL is network access, which only a `#net` permbind opens (ADR 049
//! §2). [`authorize_net`] is the one check of it, as [`call`] is of dispatch: the routine, named
//! by the capability it presents in `x-nodule-capability` (never forwarded; it is the node's, not
//! the destination's), must take a `#net` permbind that matches the scheme's port, host and
//! method, and the cartridge must hold the matching `grant/net`. The executor then performs the
//! request itself with its own HTTP client.

use crate::requests::MissingAuthorityLookup;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::DrawerCreated;
use big_dispatch_core::Body;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::Origin;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use http::Uri;
use method_core::ContextError;
use method_core::Method as _;
use serde_json::Value;
use serde_json::json;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_native_rs::Client;
use service_core_rs::xrpc::NODULE_CAPABILITY_HEADER;
use service_core_rs::xrpc::bearer_token;
use service_core_rs::xrpc::domain_error_envelope;
use service_core_rs::xrpc::request_context;
use service_utils_rs::ClientError;
use service_utils_rs::HttpMetadata;
use service_utils_rs::RequestContext;

fn json_response(status: StatusCode, body: Value) -> (StatusCode, XrpcBody) {
    (
        status,
        XrpcBody::json(serde_json::to_vec(&body).expect("a JSON value serializes")),
    )
}

/// Who a handler's outgoing calls are made as, from the executor's table: never anything the
/// handler said.
#[derive(Clone)]
pub struct WorkloadCaller {
    /// The calling workload's own actor capability.
    pub signer: CapId,
    /// What refines a refused call of one of its routines (see [`call`]).
    pub missing_authority: MissingAuthorityLookup,
}

/// Runs a handler's outgoing request through dispatch. A malformed request is answered like an
/// XRPC server would, not refused as a transport failure.
///
/// `caller` is the calling workload's identity, from the executor's table. `None` means the workload has been stopped (its nodule disabled,
/// upgraded or retired) while one of its requests was still running, so it has no identity left
/// to call with. That is a lifecycle race, not a broken invariant, and is answered the way a
/// proxy answers a call from a drained upstream rather than crashing the node.
///
/// A call dispatch refuses as [`DispatchError::Denied`] is refined by `missing_authority`: when
/// the routine's `uses` declare the operation and no grant covers it yet, the answer is the
/// cartridge's `MissingAuthority`, which the guest's typed client decodes (ADR 049 §4.3).
pub async fn call(
    dispatch: &Client<DispatchProtocol>,
    caller: Option<&WorkloadCaller>,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: XrpcBody,
) -> (StatusCode, XrpcBody) {
    let bad = |status: StatusCode, error: &str, message: &str| {
        json_response(status, json!({ "error": error, "message": message }))
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
            if !matches!(&body, XrpcBody::Json(body) if body.is_empty()) {
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
        Method::POST => HttpForm::Post {
            query: query.to_owned(),
        },
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
    let input = match &body {
        XrpcBody::Json(body) if body.is_empty() => Value::Null,
        XrpcBody::Json(body) => match serde_json::from_slice(body) {
            Ok(input) => input,
            Err(_) => {
                return bad(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "the request body is not JSON",
                );
            }
        },
        XrpcBody::Bytes(_) => Value::Null,
    };
    let Some(caller) = caller else {
        return bad(
            StatusCode::SERVICE_UNAVAILABLE,
            "WorkloadStopped",
            "the calling handler has been stopped",
        );
    };
    let invocation = InvocationContext {
        idempotency_key: idempotency_key.clone(),
        http: Some(http),
        range: None,
        origin: Origin::Remote,
    };
    // `Invoke` takes the operation, which a refusal is attributed by.
    let called = operation.clone();
    let invoke = match body {
        XrpcBody::Json(_) => Invoke::json(operation, input, invocation),
        XrpcBody::Bytes(body) => Invoke {
            operation,
            parameters: Value::Null,
            body: Body::Bytes(body),
            context: invocation,
        },
    };
    let result = dispatch
        .call_with(
            request_context.clone(),
            HttpMetadata {
                idempotency_key: idempotency_key.clone(),
                ..Authorization {
                    authority,
                    signer: caller.signer,
                }
                .metadata()
            },
            invoke,
        )
        .await;
    match result {
        Ok(output) => match output.body {
            Body::Json(output) => {
                if called == CreateDrawer::XRPC.nsid
                    && let Err(refusal) =
                        grant_created_drawer(caller, &request_context, authority, &output).await
                {
                    return refusal;
                }
                json_response(StatusCode::OK, output)
            }
            Body::Bytes(body) => (StatusCode::OK, XrpcBody::Bytes(body)),
            #[cfg(unix)]
            Body::Descriptors(_) => bad(
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                "descriptor bodies are native-only",
            ),
        },
        Err(ClientError::Method(error)) => {
            dispatch_error_response(caller.missing_authority.refine(authority, &called, error))
        }
        Err(ClientError::Transport { message, .. }) => {
            panic!("in-process dispatch transport failed: {message}")
        }
    }
}

/// A drawer the routine just created (`output` is `createDrawer`'s answer) is the service's actor's,
/// because the routine called with a handle the service minted. The runtime grants it down to the
/// cartridge before the routine hears of it, so the routine's next invocation reaches it. `Err` is
/// the answer to give the routine instead.
async fn grant_created_drawer(
    caller: &WorkloadCaller,
    request_context: &RequestContext,
    authority: CapId,
    output: &Value,
) -> Result<(), (StatusCode, XrpcBody)> {
    let created: DrawerCreated = serde_json::from_value(output.clone())
        .expect("dispatch validated createDrawer's output against its Lexicon");
    match caller
        .missing_authority
        .drawer_created(request_context, authority, created.drawer_id)
        .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "WorkloadStopped", "message": "the nodule service is shutting down" }),
        )),
        Err(error) => Err(json_response(
            StatusCode::BAD_GATEWAY,
            json!({
                "error": "BadGateway",
                "message": format!("the created drawer could not be granted: {error}"),
            }),
        )),
    }
}

/// Whether the workload's handler may send `method` to `uri`, a URL that is not `dispatch.local`
/// (see the module docs). `Err` is the answer to give the handler instead of sending: a
/// declared but ungranted request is the cartridge's `MissingAuthority` (403, decoded by the
/// guest's typed client), an undeclared one `Denied` (403), and a URL that is not `http` or
/// `https` with a host is a 400. `caller` is `None` when the workload has been stopped (see
/// [`call`]).
pub fn authorize_net(
    caller: Option<&WorkloadCaller>,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, XrpcBody)> {
    let Some(caller) = caller else {
        return Err(json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "WorkloadStopped", "message": "the calling handler has been stopped" }),
        ));
    };
    let invalid = |message: &str| {
        json_response(
            StatusCode::BAD_REQUEST,
            json!({ "error": "InvalidRequest", "message": message }),
        )
    };
    let default_port = match uri.scheme_str() {
        Some("http") => 80,
        Some("https") => 443,
        _ => return Err(invalid("a network request must be to an http or https URL")),
    };
    let Some(host) = uri.host() else {
        return Err(invalid("a network request must name a host"));
    };
    let port = uri.port_u16().unwrap_or(default_port);
    // Without a routine's capability there is no `uses` to check: refused as undeclared.
    let Some(capability) = headers
        .get(NODULE_CAPABILITY_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|id| serde_json::from_value::<CapId>(Value::String(id.to_owned())).ok())
    else {
        return Err(dispatch_error_response(DispatchError::Denied));
    };
    caller
        .missing_authority
        .authorize_net(capability, method.as_str(), host, port)
        .map_err(dispatch_error_response)
}

/// The answer for a request [`authorize_net`] allowed but whose destination the executor refused
/// when it resolved it (loopback, link-local, metadata, unspecified, multicast, documentation or
/// a private range): the same `Denied` as an undeclared host, since no grant could change it.
pub fn net_refused() -> (StatusCode, XrpcBody) {
    dispatch_error_response(DispatchError::Denied)
}

/// Removes from the headers of a request [`authorize_net`] allowed what is the node's and never
/// the destination's: the routine's capability. Nothing else of the node's reaches a guest, so
/// nothing else of it can be sent; the guest's own headers (an `authorization` for the
/// destination included) go as they are.
pub fn strip_node_headers(headers: &mut HeaderMap) {
    headers.remove(NODULE_CAPABILITY_HEADER);
}

/// The HTTP form of a dispatch failure, which is the inverse of
/// [`crate::wire::provider_result`]'s mapping of a handler's response onto dispatch's errors:
/// denied 403, unknown operation 404, not ready 503, invalid input 400 and an output off its Lexicon 500 as an XRPC
/// `{ error, message }`, a provider's own error as it came, a deadline 504. The failures of the
/// services behind dispatch are gateway errors.
fn dispatch_error_response(error: DispatchError) -> (StatusCode, XrpcBody) {
    let named = |status: StatusCode, name: &str| {
        json_response(
            status,
            json!({ "error": name, "message": error.to_string() }),
        )
    };
    match &error {
        DispatchError::Denied => named(StatusCode::FORBIDDEN, "Denied"),
        // Whole under `operationError`, so a guest's typed client decodes it as the method's
        // error (every method error wraps `ContextError`) and a routine can match it.
        DispatchError::Context(ContextError::MissingAuthority { .. }) => json_response(
            StatusCode::FORBIDDEN,
            serde_json::to_value(domain_error_envelope(&error))
                .expect("an XRPC error envelope serializes"),
        ),
        DispatchError::LocalOnly => named(StatusCode::FORBIDDEN, "LocalOnly"),
        DispatchError::BodyMismatch { .. } => json_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            json!({ "error": "InvalidRequest", "message": error.to_string() }),
        ),
        DispatchError::TooLarge { .. } => json_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({ "error": "InvalidRequest", "message": error.to_string() }),
        ),
        DispatchError::UnknownOperation => named(StatusCode::NOT_FOUND, "UnknownOperation"),
        DispatchError::NotReady { .. } => named(StatusCode::SERVICE_UNAVAILABLE, "NotReady"),
        DispatchError::InvalidInput(message) => json_response(
            StatusCode::BAD_REQUEST,
            json!({ "error": "InvalidRequest", "message": message }),
        ),
        DispatchError::InvalidOutput(_) => {
            named(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError")
        }
        DispatchError::Context(ContextError::DeadlineExceeded) => {
            named(StatusCode::GATEWAY_TIMEOUT, "DeadlineExceeded")
        }
        DispatchError::Provider { status, body } => json_response(
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
