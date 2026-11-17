//! Dispatch served as XRPC (ADR 033 §3): `/xrpc/<nsid>` for any NSID the router claims.
//!
//! The surface is one fallback procedure in front of the routing table, not a route per
//! operation: providers come and go at runtime (nodules enable and disable), so the table, not
//! this surface, says what exists. A POST's JSON body is the operation's input, exactly what the
//! operation's own client would send, and the answer is the provider's output or its own XRPC
//! error envelope, untouched. A GET is a query: the query string travels to dispatch as is
//! ([`HttpForm::Get`]), which types it by the method's Lexicon `parameters` after admitting the
//! caller. Which verb a method takes, and whether input and output conform to its Lexicon, are
//! dispatch's to decide (it alone has the routing table), so every provider is held to them once.
//! The answer is still one JSON value, as [`Invoke`] exchanges.
//!
//! The caller's dispatch capability is the standard XRPC credential, `Authorization: Bearer
//! <token>` ([`BearerToken`]). What a request that presents none may do is the listener's call
//! ([`Anonymous`]): this module knows nothing about transports.

use big_cabinet_core::authority::Authorization;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use method_core::ContextError;
use method_core::HttpMethod;
use serde_json::Value;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::XrpcRouter;
use service_utils_rs::xrpc::XrpcErrorEnvelope;
use service_utils_rs::xrpc::XrpcHandlerError;
use service_utils_rs::xrpc::XrpcMethodHandler;

/// What a request that presents no bearer token is admitted as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Anonymous {
    /// Nothing: the request is refused (401) before dispatch sees it.
    Refused,
    /// This capability. For a listener that has already authenticated its connections by other
    /// means (a Unix socket's peer credentials) and so knows who every anonymous request is.
    Acts(Authorization),
}

/// The XRPC router that serves every operation of `dispatch`'s routing table, admitting callers
/// as their bearer token says and, without one, as `anonymous` says. `max_body_bytes` bounds a
/// request body.
pub fn xrpc_router(
    dispatch: Client<DispatchProtocol>,
    anonymous: Anonymous,
    max_body_bytes: usize,
) -> XrpcRouter {
    let mut router = XrpcRouter::new(max_body_bytes);
    router
        .register_fallback(XrpcMethodHandler::new(
            method_core::XrpcMethod::procedure(""),
            move |request| {
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
                    // The credential travels in the request's metadata to dispatch, which
                    // admits it (a malformed token is `Unauthenticated`, see `refusal`).
                    let idempotency_key = metadata.idempotency_key.clone();
                    let (input, http) = match request.verb {
                        HttpMethod::Post => (request.params, HttpForm::Post),
                        HttpMethod::Get => (
                            Value::Null,
                            HttpForm::Get {
                                query: request.raw_query.unwrap_or_default(),
                            },
                        ),
                    };
                    dispatch
                        .call_with(
                            request.context,
                            metadata,
                            Invoke {
                                operation: request.nsid,
                                input,
                                context: InvocationContext {
                                    idempotency_key,
                                    http: Some(http),
                                },
                            },
                        )
                        .await
                        .map(|invoked| invoked.output)
                        .map_err(refusal)
                }
            },
        ))
        .expect("a fresh router takes one procedure fallback");
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
        DispatchError::Denied => {
            XrpcHandlerError::domain(StatusCode::FORBIDDEN, "Forbidden", error.to_string())
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
