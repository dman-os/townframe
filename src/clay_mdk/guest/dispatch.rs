//! A guest's node calls: `wasi:http/client` to `http://dispatch.local`, the transport a nodule
//! uses (`big_ndk`'s `WasiHttp`) byte for byte. The host answers that origin by routing the
//! request into dispatch as this micro's own actor; the micro never names who it is, and nothing
//! it sends (headers, body) is trusted as identity (ADR 037 §1).
//!
//! Calls are async-only. A synchronous export (`mount`, `dispatch`, `unmount`) must not
//! suspend, so a call made from an event handler is spawned
//! ([`Context::spawn`](crate::Context::spawn)) and its answer delivered through a signal.
//!
//! This is the bare transport. The typed `ctx.call::<M>(input)` author API is
//! `service_client_core_rs::Client` over an `HttpTransport` that forwards to [`send`]; it lands
//! with the node's `method_core` / `service_client_core_rs` crates, which this branch does not
//! carry yet. Until then `big_ndk` and this module are two copies of one transport, and the copy
//! belongs in whichever crate hosts that `HttpTransport` impl for both.

use crate::bindings;
use bindings::wasi::http::client;
use bindings::wasi::http::types::Fields;
use bindings::wasi::http::types::Method;
use bindings::wasi::http::types::Request;
use bindings::wasi::http::types::Response;
use bindings::wasi::http::types::Scheme;

/// The host's egress authority: the one place a micro's outgoing requests can go.
pub const DISPATCH_AUTHORITY: &str = "dispatch.local";

/// One request to the node. `path_with_query` is `/xrpc/<nsid>` plus, for a `GET`, the encoded
/// query string.
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub method: String,
    pub path_with_query: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// What the host answered: always an HTTP response, including its XRPC error bodies.
#[derive(Debug, Clone)]
pub struct DispatchResponse {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// The request never became a response: the host refused it (an origin, scheme or other policy
/// refusal) or the transport failed. The text is the `wasi:http` error code.
#[derive(Debug, Clone, thiserror::Error)]
#[error("dispatch transport: {0}")]
pub struct TransportError(pub String);

/// Send `request` to the node's dispatch through the host. An origin other than
/// `dispatch.local` is not reachable from here; [`send_to`] exists to let a test aim elsewhere.
pub async fn send(request: DispatchRequest) -> Result<DispatchResponse, TransportError> {
    send_to(Scheme::Http, DISPATCH_AUTHORITY, request).await
}

/// [`send`] to an explicit origin. A micro has no business calling this: it exists so a fixture
/// can prove the host refuses every origin but dispatch.
pub async fn send_to(
    scheme: Scheme,
    authority: &str,
    request: DispatchRequest,
) -> Result<DispatchResponse, TransportError> {
    let headers = Fields::new();
    for (name, value) in &request.headers {
        headers
            .append(name, value)
            .map_err(|error| TransportError(format!("request header {name:?}: {error:?}")))?;
    }
    let (mut body_tx, body_rx) = bindings::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let (outgoing, _completion) = Request::new(headers, Some(body_rx), trailers_rx, None);
    outgoing
        .set_method(&match request.method.as_str() {
            "GET" => Method::Get,
            "POST" => Method::Post,
            other => Method::Other(other.to_owned()),
        })
        .map_err(|()| TransportError(format!("method {:?}", request.method)))?;
    outgoing
        .set_scheme(Some(&scheme))
        .map_err(|()| TransportError("scheme".to_owned()))?;
    outgoing
        .set_authority(Some(authority))
        .map_err(|()| TransportError(format!("authority {authority:?}")))?;
    outgoing
        .set_path_with_query(Some(&request.path_with_query))
        .map_err(|()| TransportError(format!("path {:?}", request.path_with_query)))?;

    let body = request.body;
    wit_bindgen::spawn_local(async move {
        // The host may refuse or answer a request without reading its body (a denied origin
        // drops the request outright). The reader closing the stream or the trailers future is
        // then its decision, not a fault here: `client::send`'s result below is the outcome.
        drop(body_tx.write_all(body).await);
        drop(body_tx);
        drop(trailers_tx.write(Ok(None)).await);
    });

    let response = client::send(outgoing)
        .await
        .map_err(|error| TransportError(format!("{error:?}")))?;
    let status = response.get_status_code();
    let headers = response.get_headers().copy_all();
    let (result_tx, result_rx) = bindings::wit_future::new(|| Ok(()));
    let (body, _trailers) = Response::consume_body(response, result_rx);
    let body = body.collect().await;
    result_tx
        .write(Ok(()))
        .await
        .expect("response body receiver disappeared");
    Ok(DispatchResponse {
        status,
        headers,
        body,
    })
}
