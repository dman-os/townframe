use crate::Cx;
use crate::Handler;
use crate::XrpcBody;
use crate::cx::HttpHandle;
use http::Method;
use http::StatusCode;
use http::header;
use method_core::HttpInput;
use method_core::MethodKind;
use method_core::Unary;
use serde_json::json;
use service_core_rs::xrpc::DOMAIN_ERROR_STATUS;
use service_core_rs::xrpc::REQUEST_ID_HEADER;
use service_core_rs::xrpc::XrpcErrorEnvelope;
use service_core_rs::xrpc::domain_error_envelope;
use std::collections::BTreeMap;
use std::future::Future;

/// A nodule answer: JSON is encoded bytes and a byte answer stays a pull-based stream.
#[derive(Debug)]
pub struct Reply {
    pub status: StatusCode,
    pub body: XrpcBody,
}

impl Reply {
    /// An XRPC error with no detail beyond its name, as the host router answers one. `message`
    /// is left out when empty so a bare refusal is exactly `{ "error": <name> }`.
    fn error(status: StatusCode, error: &str, message: Option<String>) -> Self {
        Self {
            status,
            body: XrpcBody::json(
                serde_json::to_vec(
                    &XrpcErrorEnvelope::new(error.to_owned(), message, BTreeMap::new())
                        .expect("no details cannot collide with a reserved field"),
                )
                .expect("an XRPC error serializes"),
            ),
        }
    }
}

/// How a nodule picks the [`Handler`] for an NSID. [`export_nodule!`](crate::export_nodule)
/// generates it from a list of methods; a nodule that routes by something other than exact NSID
/// (the test fixture serves any authority's `.echo`) implements it by hand and calls
/// [`invoke`] for the method it picked.
pub trait Nodule: Default {
    /// Runs the method `nsid` names against its body, or `None` when this nodule serves no such
    /// method. A byte body is owned so a handler can consume it as a pull-based stream.
    fn route(&self, nsid: &str, cx: &Cx, body: XrpcBody) -> impl Future<Output = Option<Reply>>;
}

/// Serves one `wasi:http` request as XRPC: `POST /xrpc/<nsid>`, answered in the envelope the
/// host's router uses. JSON bodies are encoded bytes; byte bodies remain streams. Any other path
/// is `404 NotFound`, an NSID the nodule does not serve is `404 UnknownOperation`, and a malformed
/// header or body is `400 InvalidRequest`.
pub async fn serve<N: Nodule>(
    nodule: &N,
    transport: HttpHandle,
    method: &Method,
    path: &str,
    headers: &http::HeaderMap,
    body: XrpcBody,
) -> http::Response<XrpcBody> {
    let mut response = http::Response::builder();
    let reply = match path
        .strip_prefix("/xrpc/")
        .filter(|_| method == Method::POST)
    {
        None => Reply::error(StatusCode::NOT_FOUND, "NotFound", None),
        Some(nsid) => match Cx::from_headers(headers, transport) {
            Err(message) => Reply::error(StatusCode::BAD_REQUEST, "InvalidRequest", Some(message)),
            Ok(cx) => {
                // Like the router, answer with the id of the request that was served.
                response = response.header(REQUEST_ID_HEADER, cx.request().request_id.to_string());
                nodule.route(nsid, &cx, body).await.unwrap_or_else(|| {
                    Reply::error(StatusCode::NOT_FOUND, "UnknownOperation", None)
                })
            }
        },
    };
    let status = reply.status;
    let (body, response) = match reply.body {
        XrpcBody::Json(body) => (
            XrpcBody::Json(body),
            response.header(header::CONTENT_TYPE, "application/json"),
        ),
        XrpcBody::Bytes(body) => {
            let mime = body
                .mime
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_owned());
            let mut response = response.header(header::CONTENT_TYPE, mime);
            if let Some(length) = body.length {
                response = response.header(header::CONTENT_LENGTH, length);
            }
            (XrpcBody::Bytes(body), response)
        }
    };
    response
        .status(status)
        .body(body)
        .expect("a status and static headers are a valid response")
}

/// Runs the [`Handler`] of `N` for method `M` against a JSON body: decodes `M`, refuses an
/// already-spent budget with the method's own `ContextError` conversion, and encodes the output
/// (`200`) or the method's error in the domain-error envelope (`400`, the error under
/// `operationError`), exactly as the host router does.
pub async fn invoke<N, M>(nodule: &N, cx: &Cx, body: XrpcBody) -> Reply
where
    N: Handler<M>,
    M: Unary,
{
    const {
        assert!(
            matches!(M::XRPC.kind, MethodKind::Procedure)
                && matches!(M::XRPC.input, HttpInput::JsonBody),
            "big_ndk serves procedures with a JSON body only"
        );
    }
    let body = match body {
        XrpcBody::Json(body) => body,
        XrpcBody::Bytes(_) => {
            return Reply::error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                Some(
                    "a nodule handler takes JSON bodies only; byte bodies reach guests with wasi:http streams"
                        .to_owned(),
                ),
            );
        }
    };
    let input: M = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(error) => {
            return Reply::error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                Some(format!("invalid JSON body: {error}")),
            );
        }
    };
    let result = match cx.request().validate() {
        Ok(()) => nodule.call(cx, input).await,
        Err(error) => Err(M::Error::from(error)),
    };
    match result {
        Ok(output) => Reply {
            status: StatusCode::OK,
            body: XrpcBody::json(
                serde_json::to_vec(&output).expect("method outputs must serialize"),
            ),
        },
        Err(error) => Reply {
            status: DOMAIN_ERROR_STATUS,
            body: XrpcBody::json(
                serde_json::to_vec(&json!(domain_error_envelope(&error)))
                    .expect("a domain error serializes"),
            ),
        },
    }
}
