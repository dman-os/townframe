//! The `wasi:http` edge: the only place the kit touches the generated bindings. Everything above
//! it works on `http` types, so the routing and envelope logic runs natively in unit tests.

use crate::Nodule;
use crate::serve::serve;
use crate::wit;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use wit::wasi::http::client;
use wit::wasi::http::types::ErrorCode;
use wit::wasi::http::types::Fields;
use wit::wasi::http::types::Method;
use wit::wasi::http::types::Request;
use wit::wasi::http::types::Response;
use wit::wasi::http::types::Scheme;

/// The host's egress authority: the one place a nodule's outgoing requests can go, and which
/// admits them through dispatch.
const DISPATCH_AUTHORITY: &str = "dispatch.local";

/// The [`HttpTransport`] of a guest: `wasi:http/client` to `http://dispatch.local`. Wrapped in
/// `service_client_core_rs::Xrpc` it is a `UnaryTransport` for every unary method.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasiHttp;

impl HttpTransport for WasiHttp {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        let headers = Fields::new();
        for (name, value) in &request.headers {
            headers
                .append(name.as_str(), value.as_bytes())
                .expect("an `http` header is a valid wasi:http field");
        }
        let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
        let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
        let (outgoing, _completion) = Request::new(headers, Some(body_rx), trailers_rx, None);
        outgoing
            .set_method(&match request.method {
                http::Method::GET => Method::Get,
                http::Method::POST => Method::Post,
                other => Method::Other(other.as_str().to_owned()),
            })
            .expect("a method token is valid");
        outgoing
            .set_scheme(Some(&Scheme::Http))
            .expect("http is a valid scheme");
        outgoing
            .set_authority(Some(DISPATCH_AUTHORITY))
            .expect("a fixed authority is valid");
        outgoing
            .set_path_with_query(Some(&format!("/{}", request.path)))
            .expect("an encoded XRPC path is valid");

        let body = request.body;
        wit_bindgen::spawn_local(async move {
            let remaining = body_tx.write_all(body).await;
            assert!(remaining.is_empty(), "outgoing body stream closed early");
            drop(body_tx);
            trailers_tx
                .write(Ok(None))
                .await
                .expect("outgoing trailers receiver disappeared");
        });

        let response = client::send(outgoing)
            .await
            .map_err(|error| HttpTransportError {
                message: format!("{error:?}"),
            })?;
        let status = http::StatusCode::from_u16(response.get_status_code())
            .expect("the host answers a valid status");
        let mut headers = HeaderMap::new();
        for (name, value) in response.get_headers().copy_all() {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).map_err(|error| HttpTransportError {
                    message: format!("response header name {name:?}: {error}"),
                })?,
                HeaderValue::from_bytes(&value).map_err(|error| HttpTransportError {
                    message: format!("response header {name:?} value: {error}"),
                })?,
            );
        }
        let (result_tx, result_rx) = wit::wit_future::new(|| Ok(()));
        let (body, _trailers) = Response::consume_body(response, result_rx);
        let body = body.collect().await;
        result_tx
            .write(Ok(()))
            .await
            .expect("response body receiver disappeared");
        Ok(XrpcResponse {
            status,
            headers,
            body,
        })
    }
}

/// The body of [`export_nodule!`](crate::export_nodule)'s `handle`: reads the request, serves it
/// with a fresh `N`, and writes the answer.
pub async fn serve_wasi<N: Nodule>(request: Request) -> Result<Response, ErrorCode> {
    let method = match request.get_method() {
        Method::Get => http::Method::GET,
        Method::Head => http::Method::HEAD,
        Method::Post => http::Method::POST,
        Method::Put => http::Method::PUT,
        Method::Delete => http::Method::DELETE,
        Method::Connect => http::Method::CONNECT,
        Method::Options => http::Method::OPTIONS,
        Method::Trace => http::Method::TRACE,
        Method::Patch => http::Method::PATCH,
        Method::Other(other) => http::Method::from_bytes(other.as_bytes())
            .expect("the host delivers a valid method token"),
    };
    let path = request.get_path_with_query().unwrap_or_default();
    let path = path.split('?').next().unwrap_or_default();
    let headers = request.get_headers();
    let mut delivered = HeaderMap::new();
    for (name, value) in headers.copy_all() {
        delivered.append(
            HeaderName::from_bytes(name.as_bytes()).expect("the host delivers valid header names"),
            HeaderValue::from_bytes(&value).expect("the host delivers valid header values"),
        );
    }
    drop(headers);
    let path = path.to_owned();

    let (result_tx, result_rx) = wit::wit_future::new(|| Ok(()));
    let (body, _trailers) = Request::consume_body(request, result_rx);
    let body = body.collect().await;
    result_tx
        .write(Ok(()))
        .await
        .expect("request body receiver disappeared");

    let answer = serve(&N::default(), &method, &path, &delivered, &body).await;

    let response_headers = Fields::new();
    for (name, value) in answer.headers() {
        response_headers
            .append(name.as_str(), value.as_bytes())
            .expect("an `http` header is a valid wasi:http field");
    }
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (response, _completion) = Response::new(response_headers, Some(body_rx), trailers_rx);
    response
        .set_status_code(answer.status().as_u16())
        .expect("a valid status");
    let bytes = answer.into_body();
    wit_bindgen::spawn_local(async move {
        let remaining = body_tx.write_all(bytes).await;
        assert!(remaining.is_empty(), "response body stream closed early");
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("response trailers receiver disappeared");
    });
    Ok(response)
}

/// The entropy source getrandom's `custom` backend links on wasm32-wasip3. Request ids and span
/// ids come from it (`uuid` v7), so every nodule needs one; it is `wasi:random`, which the host
/// provides.
#[cfg(target_os = "wasi")]
#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> Result<(), getrandom03::Error> {
    let destination = unsafe { core::slice::from_raw_parts_mut(dest, len) };
    let mut offset = 0;
    while offset < len {
        let random = wit::wasi::random::random::get_random_bytes((len - offset) as u64);
        if random.is_empty() {
            return Err(getrandom03::Error::UNEXPECTED);
        }
        let amount = random.len().min(len - offset);
        destination[offset..offset + amount].copy_from_slice(&random[..amount]);
        offset += amount;
    }
    Ok(())
}
