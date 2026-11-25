//! Dispatch served as XRPC (ADR 033 §3): `/xrpc/<nsid>` for any NSID the router claims.
//!
//! The surface is one fallback in front of the routing table, not a route per operation:
//! providers come and go at runtime (nodules enable and disable), so the table, not this surface,
//! says what exists. A POST's body is the operation's input, exactly what the operation's own
//! client would send, and the answer is the provider's output or its own XRPC error envelope,
//! untouched. A GET is a query: the query string travels to dispatch as is
//! ([`HttpForm::Get`]), which types it by the method's Lexicon `parameters` after admitting the
//! caller. Which verb a method takes, and whether input and output conform to its Lexicon, are
//! dispatch's to decide (it alone has the routing table), so every provider is held to them once.
//!
//! A body is JSON or raw bytes as the method declares (ADR 042). This surface cannot know which,
//! so a POST's body goes to dispatch as the stream it is, with the request's `Content-Type`, and
//! dispatch reads it as JSON or hands it on. A byte answer is the raw response body, and a `Range`
//! request header is passed to dispatch for a method that serves ranges to honour.
//!
//! The caller's dispatch capability is the standard XRPC credential, `Authorization: Bearer
//! <token>` ([`BearerToken`]). What a request that presents none may do is the listener's call
//! ([`Anonymous`]), as is where the call came from ([`Edge`]): this module knows nothing about
//! transports beyond that.

use big_cabinet_core::authority::Authorization;
use big_dispatch_core::Body;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::Origin;
use method_core::ContextError;
use method_core::HttpMethod;
use serde_json::Value;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::XrpcRouter;
use service_utils_rs::xrpc::XrpcErrorEnvelope;
use service_utils_rs::xrpc::XrpcFallbackHandler;
use service_utils_rs::xrpc::XrpcFallbackResponse;
use service_utils_rs::xrpc::XrpcHandlerError;

/// What a request that presents no bearer token is admitted as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Anonymous {
    /// Nothing: the request is refused (401) before dispatch sees it.
    Refused,
    /// This capability. For a listener that has already authenticated its connections by other
    /// means (a Unix socket's peer credentials) and so knows who every anonymous request is.
    Acts(Authorization),
}

/// The edge transport a router serves, which is what sets the [`Origin`] of every call through it
/// (ADR 042 §3). A caller can never say where it came from: the listener that built the router
/// did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edge {
    /// A TCP listener: nothing says the peer is on this machine, so its calls are remote.
    Tcp,
    /// A Unix socket listener. Its acceptor reports every peer's `SO_PEERCRED` credentials with
    /// the request, which the call's origin carries; a request without them is a listener
    /// wiring bug.
    Unix,
    /// A router served in the node's own process without a socket, as `day` runs the node itself.
    Embedded,
}

/// The XRPC router that serves every operation of `dispatch`'s routing table, admitting callers
/// as their bearer token says and, without one, as `anonymous` says, and stamping every call
/// with the origin `edge` implies.
pub fn xrpc_router(
    dispatch: Client<DispatchProtocol>,
    anonymous: Anonymous,
    edge: Edge,
) -> XrpcRouter {
    // A router that has no limit of its own: a registered operation would have its body bounded
    // here, but this one serves the fallback, where only a JSON body is gathered (by dispatch,
    // against `MAX_JSON_BODY_OCTETS`) and a byte body is streamed.
    let mut router = XrpcRouter::new(usize::MAX);
    router
        .register_fallback(XrpcFallbackHandler::new(move |request| {
            let dispatch = dispatch.clone();
            async move {
                let mut metadata = request.metadata;
                if metadata.authorization.is_none() {
                    match anonymous {
                        Anonymous::Acts(authorization) => {
                            metadata.authorization = Some(authorization.header_value());
                        }
                        Anonymous::Refused => {
                            return Err(XrpcHandlerError::domain(
                                http::StatusCode::UNAUTHORIZED,
                                "AuthenticationRequired",
                                "this endpoint requires an Authorization: Bearer token",
                            ));
                        }
                    }
                }
                let origin = match edge {
                    Edge::Tcp => Origin::Remote,
                    Edge::Embedded => Origin::Embedded,
                    Edge::Unix => Origin::UnixPeer {
                        uid: request
                            .peer
                            .expect("the Unix socket's acceptor reports every peer's credentials")
                            .uid,
                    },
                };
                // The credential travels in the request's metadata to dispatch, which
                // admits it (a malformed token is `Unauthenticated`, see `refusal`).
                let idempotency_key = metadata.idempotency_key.clone();
                let range = metadata.range;
                let query = request.raw_query.unwrap_or_default();
                let (body, http) = match (request.verb, request.body) {
                    (HttpMethod::Post, Some(body)) => (Body::Bytes(body), HttpForm::Post { query }),
                    (HttpMethod::Get, None) => (Body::Json(Value::Null), HttpForm::Get { query }),
                    (verb, body) => unreachable!(
                        "the router hands a POST its body and a GET none: {verb:?}, {}",
                        body.is_some()
                    ),
                };
                let invoked = dispatch
                    .call_with(
                        request.context,
                        metadata,
                        Invoke {
                            operation: request.nsid,
                            parameters: Value::Null,
                            body,
                            context: InvocationContext {
                                idempotency_key,
                                http: Some(http),
                                range,
                                origin,
                            },
                        },
                    )
                    .await
                    .map_err(refusal)?;
                Ok(match invoked.body {
                    Body::Json(output) => XrpcFallbackResponse::Json(output.into()),
                    Body::Bytes(body) => XrpcFallbackResponse::Bytes {
                        metadata: Default::default(),
                        body,
                    },
                    #[cfg(unix)]
                    Body::Descriptors(_) => {
                        unreachable!("descriptor body cannot be serialized over XRPC")
                    }
                })
            }
        }))
        .expect("a fresh router takes one fallback");
    router
}

/// The HTTP form of a failed call: gateway semantics, with a provider's own XRPC error passed
/// through with its status.
fn refusal(error: ClientError<DispatchError>) -> XrpcHandlerError {
    use http::StatusCode;
    let error = match error {
        ClientError::Method(error) => error,
        ClientError::Transport { message, .. } => return XrpcHandlerError::Unavailable(message),
    };
    match error {
        DispatchError::Context(ContextError::DeadlineExceeded) => XrpcHandlerError::domain(
            StatusCode::GATEWAY_TIMEOUT,
            "DeadlineExceeded",
            error.to_string(),
        ),
        DispatchError::Context(ContextError::Unauthenticated) => XrpcHandlerError::domain(
            StatusCode::UNAUTHORIZED,
            "InvalidToken",
            "the Authorization header is not a dispatch bearer token",
        ),
        DispatchError::Context(_) | DispatchError::InvalidInput(_) => {
            XrpcHandlerError::InvalidInput(error.to_string())
        }
        DispatchError::BodyMismatch { .. } => XrpcHandlerError::domain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "InvalidRequest",
            error.to_string(),
        ),
        DispatchError::TooLarge { .. } => XrpcHandlerError::domain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "InvalidRequest",
            error.to_string(),
        ),
        DispatchError::Denied => {
            XrpcHandlerError::domain(StatusCode::FORBIDDEN, "Forbidden", error.to_string())
        }
        DispatchError::LocalOnly => {
            XrpcHandlerError::domain(StatusCode::FORBIDDEN, "LocalOnly", error.to_string())
        }
        DispatchError::UnknownOperation => {
            XrpcHandlerError::domain(StatusCode::NOT_FOUND, "UnknownMethod", error.to_string())
        }
        DispatchError::NotReady { .. } | DispatchError::AuthorityUnavailable(_) => {
            XrpcHandlerError::Unavailable(error.to_string())
        }
        DispatchError::Authority(_) => XrpcHandlerError::Internal(error.to_string()),
        DispatchError::InvalidOutput(_) => XrpcHandlerError::domain(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            error.to_string(),
        ),
        DispatchError::Provider { status, body } => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            match serde_json::from_value::<XrpcErrorEnvelope>(body.clone()) {
                Ok(body) => XrpcHandlerError::Domain { status, body },
                Err(_) => XrpcHandlerError::domain(status, "ProviderError", body.to_string()),
            }
        }
    }
}
