//! The `wasi:http` edge: the only place the kit touches the generated bindings. Everything above
//! it works on `http` types, so the routing and envelope logic runs natively in unit tests.

use crate::HttpHandle;
use crate::Nodule;
use crate::serve::serve;
use crate::wit;
use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use method_core::ByteBody;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_core_rs::xrpc::TIMEOUT_HEADER;
use std::io;
use wit::wasi::http::client;
use wit::wasi::http::types::ErrorCode;
use wit::wasi::http::types::Fields;
use wit::wasi::http::types::Method;
use wit::wasi::http::types::Request;
use wit::wasi::http::types::RequestOptions;
use wit::wasi::http::types::Response;
use wit::wasi::http::types::Scheme;
use wit_bindgen::StreamReader;
use wit_bindgen::StreamResult;

/// The host's egress authority: the one place a nodule's outgoing requests can go, and which
/// admits them through dispatch.
const DISPATCH_AUTHORITY: &str = "dispatch.local";
const BODY_CHUNK_SIZE: usize = 64 * 1024;

/// Forwards a `wasi:http` byte stream to `sender` in chunks of [`BODY_CHUNK_SIZE`] octets, the
/// last one shorter. A `stream.read` appends into the buffer's spare capacity and returns as soon
/// as the host has written anything, often far less than a chunk, so each chunk is filled by as
/// many reads as it takes. Reading in bulk matters: `StreamReader::into_stream` reads one octet
/// per host call, which spends a synchronous guest call's whole fuel budget on about a megabyte.
/// Returns whether the stream ended; `false` means the receiver stopped reading first.
async fn forward_body(
    mut reader: StreamReader<u8>,
    sender: &mut futures::channel::mpsc::Sender<io::Result<Bytes>>,
) -> bool {
    loop {
        let mut chunk = Vec::with_capacity(BODY_CHUNK_SIZE);
        let ended = loop {
            let (result, filled) = reader.read(chunk).await;
            chunk = filled;
            match result {
                StreamResult::Complete(_) if chunk.len() < BODY_CHUNK_SIZE => {}
                StreamResult::Complete(_) => break false,
                StreamResult::Dropped => break true,
                StreamResult::Cancelled => unreachable!("nothing cancels a body read"),
            }
        };
        if !chunk.is_empty() && sender.send(Ok(Bytes::from(chunk))).await.is_err() {
            return false;
        }
        if ended {
            return true;
        }
    }
}

fn byte_body(reader: StreamReader<u8>, mime: Option<String>, length: Option<u64>) -> ByteBody {
    let (mut sender, receiver) = futures::channel::mpsc::channel(1);
    wit_bindgen::spawn_local(async move {
        forward_body(reader, &mut sender).await;
    });
    let mut body = ByteBody::new(receiver);
    if let Some(mime) = mime {
        body = body.with_mime(mime);
    }
    if let Some(length) = length {
        body = body.with_length(length);
    }
    body
}

/// Where a request's `path` goes: `dispatch.local` for a relative path, else the scheme,
/// authority and path-and-query of the absolute `http(s)` URL it is.
fn destination(path: &str) -> Result<(Scheme, String, String), HttpTransportError> {
    if !(path.starts_with("http://") || path.starts_with("https://")) {
        return Ok((
            Scheme::Http,
            DISPATCH_AUTHORITY.to_owned(),
            format!("/{path}"),
        ));
    }
    let url: http::Uri = path.parse().map_err(|error| HttpTransportError {
        message: format!("`{path}` is not a URL: {error}"),
    })?;
    let authority = url.authority().ok_or_else(|| HttpTransportError {
        message: format!("`{path}` names no host"),
    })?;
    Ok((
        match url.scheme_str() {
            Some("https") => Scheme::Https,
            _ => Scheme::Http,
        },
        authority.to_string(),
        url.path_and_query()
            .map_or("/".to_owned(), |target| target.to_string()),
    ))
}

/// The [`HttpTransport`] of a guest: `wasi:http/client` to `http://dispatch.local`, or to the
/// absolute URL a request's `path` names (see [`XrpcRequest::path`]). Wrapped in
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
        // A guest has no timer of its own, so the deadline is the host's: it parses the
        // `x-timeout-ms` this request carries, and the first-byte timeout below tells the
        // transport the same budget.
        let options = request.headers.get(TIMEOUT_HEADER).map(|value| {
            let millis: u64 = value
                .to_str()
                .ok()
                .and_then(|text| text.parse().ok())
                .expect("`write_context_headers` writes x-timeout-ms as integer milliseconds");
            let options = RequestOptions::new();
            options
                .set_first_byte_timeout(Some(millis.saturating_mul(1_000_000)))
                .expect("the host's wasi:http client supports a first-byte timeout");
            options
        });
        let (outgoing, _completion) = Request::new(headers, Some(body_rx), trailers_rx, options);
        outgoing
            .set_method(&match request.method {
                http::Method::GET => Method::Get,
                http::Method::POST => Method::Post,
                other => Method::Other(other.as_str().to_owned()),
            })
            .expect("a method token is valid");
        let (scheme, authority, path) = destination(&request.path)?;
        outgoing
            .set_scheme(Some(&scheme))
            .expect("http and https are valid schemes");
        outgoing
            .set_authority(Some(&authority))
            .expect("the authority of a parsed URL is valid");
        outgoing
            .set_path_with_query(Some(&path))
            .expect("the path of a parsed URL is valid");

        let body = request.body;
        wit_bindgen::spawn_local(async move {
            // The host closes the body stream when it refuses the request before reading it (an
            // origin it does not serve, or a micro it gave no egress). The refusal is the answer
            // `send` returns, so a stream that closed early ends the upload instead of crashing
            // the guest.
            match body {
                XrpcBody::Json(body) => {
                    if !body_tx.write_all(body).await.is_empty() {
                        return;
                    }
                }
                XrpcBody::Bytes(mut body) => {
                    while let Some(chunk) = body.next().await {
                        let chunk = chunk.expect("a byte request body stream cannot fail");
                        if !body_tx.write_all(chunk.to_vec()).await.is_empty() {
                            return;
                        }
                    }
                }
            }
            drop(body_tx);
            // A host that stopped reading the request has dropped the trailers future too.
            let _undelivered = trailers_tx.write(Ok(None)).await;
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
        let is_json = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(method_core::is_json_content_type);
        let body = if is_json {
            let body = body.collect().await;
            result_tx
                .write(Ok(()))
                .await
                .expect("response body receiver disappeared");
            XrpcBody::Json(body)
        } else {
            let mime = headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let length = headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok());
            let (mut sender, receiver) = futures::channel::mpsc::channel(1);
            wit_bindgen::spawn_local(async move {
                if !forward_body(body, &mut sender).await {
                    return;
                }
                result_tx
                    .write(Ok(()))
                    .await
                    .expect("response body receiver disappeared");
            });
            let mut body = ByteBody::new(receiver)
                .with_mime(mime.unwrap_or_else(|| "application/octet-stream".to_owned()));
            if let Some(length) = length {
                body = body.with_length(length);
            }
            XrpcBody::Bytes(body)
        };
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
    let path = path.to_owned();
    let headers = request.get_headers();
    let mut delivered = HeaderMap::new();
    for (name, value) in headers.copy_all() {
        delivered.append(
            HeaderName::from_bytes(name.as_bytes()).expect("the host delivers valid header names"),
            HeaderValue::from_bytes(&value).expect("the host delivers valid header values"),
        );
    }
    drop(headers);

    let is_json = delivered
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(method_core::is_json_content_type);
    let mime = delivered
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let length = delivered
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let (result_tx, result_rx) = wit::wit_future::new(|| Ok(()));
    let (body, _trailers) = Request::consume_body(request, result_rx);
    let mut result_tx = Some(result_tx);
    let body = if is_json {
        let body = body.collect().await;
        result_tx
            .take()
            .expect("request body completion is present")
            .write(Ok(()))
            .await
            .expect("request body receiver disappeared");
        XrpcBody::Json(body)
    } else {
        XrpcBody::Bytes(byte_body(body, mime, length))
    };
    let answer = serve(
        &N::default(),
        HttpHandle::new(WasiHttp),
        &method,
        &path,
        &delivered,
        body,
    )
    .await;
    if let Some(result_tx) = result_tx {
        result_tx
            .write(Ok(()))
            .await
            .expect("request body receiver disappeared");
    }

    let (parts, answer_body) = answer.into_parts();
    let response_headers = Fields::new();
    for (name, value) in &parts.headers {
        response_headers
            .append(name.as_str(), value.as_bytes())
            .expect("an `http` header is valid wasi:http");
    }
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (response, _completion) = Response::new(response_headers, Some(body_rx), trailers_rx);
    response
        .set_status_code(parts.status.as_u16())
        .expect("a valid status");
    wit_bindgen::spawn_local(async move {
        match answer_body {
            XrpcBody::Json(body) => {
                let remaining = body_tx.write_all(body).await;
                assert!(remaining.is_empty(), "response body stream closed early");
            }
            XrpcBody::Bytes(mut body) => {
                while let Some(chunk) = body.next().await {
                    let chunk = chunk.expect("a byte response body stream cannot fail");
                    let remaining = body_tx.write_all(chunk.to_vec()).await;
                    assert!(remaining.is_empty(), "response body stream closed early");
                }
            }
        }
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
