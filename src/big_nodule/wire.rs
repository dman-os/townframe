//! The delivery side of the guest wire, shared by every executor: the request a handler is
//! served (the contract every XRPC hop speaks) and how its response maps back onto dispatch's
//! provider errors. [`crate::egress`] is the other direction.

use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_dispatch::Caller;
use big_dispatch::ProviderError;
use big_dispatch_core::InvocationContext;
use http::HeaderMap;
use http::StatusCode;
use serde_json::Value;
use serde_json::json;
use service_client_core_rs::xrpc::write_context_headers;
use service_core_rs::xrpc::DOMAIN_ERROR_FIELD;
use service_core_rs::xrpc::DOMAIN_ERROR_STATUS;
use service_core_rs::xrpc::NODULE_CAPABILITY_HEADER;
use service_core_rs::xrpc::bearer_header_value;
use service_utils_rs::HttpMetadata;
use service_utils_rs::RequestContext;

/// The status a transport failure (workload gone, body failed, oversize response) is reported
/// under, as an HTTP gateway would.
const BAD_GATEWAY: u16 = 502;

/// A transport failure between the executor and its handler, as a provider error.
pub fn gateway_error(message: impl std::fmt::Display) -> ProviderError {
    ProviderError::Failed {
        status: BAD_GATEWAY,
        body: json!({ "error": "BadGateway", "message": message.to_string() }),
    }
}

/// The headers of a delivery to a handler: the request id and trace of the next hop, the
/// remaining budget as a relative `x-timeout-ms`, the idempotency key, and the caller's
/// capability as a bare `Bearer` token (the caller's signer never enters a handler), plus
/// `x-nodule-capability`, the nodule's own dispatch capability, when it was granted one.
pub fn delivery_headers(
    caller: &Caller,
    request: &RequestContext,
    context: &InvocationContext,
    nodule_capability: Option<CapId>,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    write_context_headers(
        &mut headers,
        &request.next_hop(),
        &HttpMetadata {
            authorization: Some(bearer_header_value(&Authorization::id_text(
                caller.authorization.authority,
            ))),
            idempotency_key: context.idempotency_key.clone(),
        },
    )
    .expect("request context headers are valid HTTP");
    if let Some(capability) = nodule_capability {
        headers.insert(
            NODULE_CAPABILITY_HEADER,
            Authorization::id_text(capability)
                .parse()
                .expect("a capability id is a valid header value"),
        );
    }
    headers
}

/// A handler's response as dispatch's provider result: a success is its JSON body (`null` when
/// empty), a `400` without a method error is `InvalidInput` carrying the XRPC `message` (the
/// handler refused the request before running the method, e.g. undecodable JSON), and any other
/// status is the provider's own failure with its body. That includes the method's own domain
/// error (`400` with the `operationError` member): it passes through untouched as
/// `ProviderError::Failed`, so dispatch hands it to typed clients and XRPC listeners as the
/// envelope the handler wrote.
pub fn provider_result(status: StatusCode, body: &[u8]) -> Result<Value, ProviderError> {
    if status.is_success() {
        return if body.is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_slice(body)
                .map_err(|error| gateway_error(format!("response is not JSON: {error}")))
        };
    }
    let body = serde_json::from_slice::<Value>(body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()));
    if status == DOMAIN_ERROR_STATUS && body.get(DOMAIN_ERROR_FIELD).is_none() {
        // XRPC errors are `{ "error", "message" }`; the message is what a caller can act on.
        return Err(ProviderError::InvalidInput(
            body.get("message")
                .and_then(Value::as_str)
                .map_or_else(|| body.to_string(), str::to_owned),
        ));
    }
    Err(ProviderError::Failed {
        status: status.as_u16(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_domain_error_passes_through_and_a_refused_request_is_invalid_input() {
        let domain = json!({
            "error": "Refused",
            "message": "Refused",
            "operationError": { "Refused": { "reason": "no name" } },
        });
        assert_eq!(
            provider_result(DOMAIN_ERROR_STATUS, domain.to_string().as_bytes()),
            Err(ProviderError::Failed {
                status: 400,
                body: domain,
            })
        );
        assert_eq!(
            provider_result(
                StatusCode::BAD_REQUEST,
                br#"{"error":"InvalidRequest","message":"invalid JSON body"}"#
            ),
            Err(ProviderError::InvalidInput("invalid JSON body".into()))
        );
    }
}
