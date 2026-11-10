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
/// empty), a `400` is `InvalidInput` carrying the XRPC `message`, any other status is the
/// provider's own failure with its body.
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
    if status == StatusCode::BAD_REQUEST {
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
