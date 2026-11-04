//! Dispatch served as XRPC (ADR 032 §3): `/xrpc/<nsid>` for any NSID the router claims.
//!
//! The surface is one fallback procedure in front of the routing table, not a route per
//! operation: providers come and go at runtime (nodules enable and disable), so the table, not
//! this surface, says what exists. A POST's JSON body is the operation's input, exactly what the
//! operation's own client would send, and the answer is the provider's output or its own XRPC
//! error envelope, untouched. Queries (GET) are not served: dispatch exchanges one JSON value each
//! way, as [`Invoke`] does.
//!
//! The caller's dispatch capability is the standard XRPC credential, `Authorization: Bearer
//! <token>` ([`BearerToken`]). What a request that presents none may do is the listener's call
//! ([`Anonymous`]): this module knows nothing about transports.

use big_cabinet_core::authority::Authorization;
use big_dispatch_core::BearerToken;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::ContextError;
use service_utils_rs::XrpcRouter;
use service_utils_rs::xrpc::XrpcErrorEnvelope;
use service_utils_rs::xrpc::XrpcHandlerError;
use service_utils_rs::xrpc::XrpcOperationHandler;

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
        .register_fallback(XrpcOperationHandler::new(
            service_utils_rs::XrpcOperation::procedure(""),
            move |request| {
                let dispatch = dispatch.clone();
                async move {
                    let authorization =
                        match request.metadata.authorization.as_deref() {
                            Some(header) => BearerToken::from_header_value(header)
                                .ok_or_else(|| {
                                    XrpcHandlerError::domain(
                                        http::StatusCode::UNAUTHORIZED,
                                        "InvalidToken",
                                        "the Authorization header is not a dispatch bearer token",
                                    )
                                })?
                                .0,
                            None => match anonymous {
                                Anonymous::Acts(authorization) => authorization,
                                Anonymous::Refused => {
                                    return Err(XrpcHandlerError::domain(
                                        http::StatusCode::UNAUTHORIZED,
                                        "AuthenticationRequired",
                                        "this endpoint requires an Authorization: Bearer token",
                                    ));
                                }
                            },
                        };
                    dispatch
                        .call(
                            request.context,
                            Invoke {
                                authorization,
                                operation: request.nsid,
                                input: request.params,
                                context: InvocationContext {
                                    deadline_unix_ms: None,
                                    idempotency_key: request.metadata.idempotency_key,
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
        ClientError::Operation(error) => error,
        ClientError::Transport { message, .. } => return XrpcHandlerError::Unavailable(message),
    };
    match error {
        DispatchError::Context(ContextError::DeadlineExceeded)
        | DispatchError::DeadlineExceeded => XrpcHandlerError::domain(
            StatusCode::GATEWAY_TIMEOUT,
            "DeadlineExceeded",
            error.to_string(),
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
        DispatchError::Provider { status, body } => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            match serde_json::from_value::<XrpcErrorEnvelope>(body.clone()) {
                Ok(body) => XrpcHandlerError::Domain { status, body },
                Err(_) => XrpcHandlerError::domain(status, "ProviderError", body.to_string()),
            }
        }
    }
}
