//! The `wasi:http/handler@0.3.0` export: GET only, stateless, the request body is never read.
//!
//! The handler picks its source per request, from the environment (`INTXT_WEB_NODE`,
//! `INTXT_WEB_DOC_CAP`, `INTXT_WEB_BLOB_CAP`), because a component's environment is the host's to
//! give at instantiation and nothing about it is known when the module is initialized. All three
//! unset serves the fixture; only some set is a misconfiguration, reported on every page.

use crate::node::BaseUrl;
use crate::node::NodeConfig;
use crate::node::NodeSource;
use crate::route;
use crate::source::FixtureSource;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        generate_all,
        async: ["export:wasi:http/handler@0.3.0#handle"],
    });
}

use wit::wasi::http::client;
use wit::wasi::http::types::ErrorCode;
use wit::wasi::http::types::Fields;
use wit::wasi::http::types::Method;
use wit::wasi::http::types::Request;
use wit::wasi::http::types::RequestOptions;
use wit::wasi::http::types::Response;
use wit::wasi::http::types::Scheme;

/// How long the node has to start answering a request. A page waits on several of these in turn,
/// so a node that never answers must not hold a page open for good.
const FIRST_BYTE_TIMEOUT_NANOS: u64 = 10_000_000_000;

struct Component;
wit::export!(Component with_types_in wit);

impl wit::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        let page = match request.get_method() {
            Method::Get => serve(&path).await,
            _ => route::method_not_allowed(),
        };
        Ok(respond(page))
    }
}

async fn serve(path: &str) -> route::Page {
    let environment = wit::wasi::cli::environment::get_environment();
    let config = NodeConfig::from_env(|name| {
        environment
            .iter()
            .find_map(|(key, value)| (key == name).then(|| value.clone()))
    });
    let config = match config {
        Ok(Some(config)) => config,
        Ok(None) => return route::serve(&FixtureSource, path).await,
        Err(error) => return route::misconfigured(error.to_string()),
    };
    let base = match BaseUrl::parse(&config.node) {
        Ok(base) => base,
        Err(error) => {
            return route::misconfigured(format!("{}: {error}", crate::node::NODE_VAR));
        }
    };
    route::serve(&NodeSource::new(WasiHttp { base }, config), path).await
}

fn respond(page: route::Page) -> Response {
    let headers = Fields::new();
    headers
        .append("content-type", page.content_type.as_bytes())
        .expect("static header");
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response.set_status_code(page.status).expect("valid status");
    wit_bindgen::spawn_local(async move {
        let remaining = body_tx.write_all(page.body.into_bytes()).await;
        assert!(remaining.is_empty(), "response body receiver went away");
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("response trailers receiver disappeared");
    });
    response
}

/// The [`HttpTransport`] over `wasi:http/client`, to the node at `base`. Every body, JSON or the
/// bytes of a blob, is collected whole and handed over as [`XrpcBody::Json`], the container of
/// "the bytes of the answer"; the viewer reads bounded answers only (a blob read asks for a range).
struct WasiHttp {
    base: BaseUrl,
}

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
        let options = RequestOptions::new();
        options
            .set_first_byte_timeout(Some(FIRST_BYTE_TIMEOUT_NANOS))
            .expect("the host's wasi:http client supports a first-byte timeout");
        let (outgoing, _completion) =
            Request::new(headers, Some(body_rx), trailers_rx, Some(options));
        outgoing
            .set_method(&match request.method {
                http::Method::GET => Method::Get,
                http::Method::POST => Method::Post,
                other => Method::Other(other.as_str().to_owned()),
            })
            .expect("a method token is valid");
        outgoing
            .set_scheme(Some(&if self.base.https {
                Scheme::Https
            } else {
                Scheme::Http
            }))
            .expect("http and https are valid schemes");
        outgoing
            .set_authority(Some(&self.base.authority))
            .expect("a parsed base URL's authority is valid");
        outgoing
            .set_path_with_query(Some(&self.base.target(&request.path)))
            .expect("a request path is a valid path and query");

        let XrpcBody::Json(body) = request.body else {
            unreachable!("the viewer sends no byte bodies");
        };
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
            .expect("the node answers a valid status");
        let mut headers = http::HeaderMap::new();
        for (name, value) in response.get_headers().copy_all() {
            headers.append(
                http::HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                    HttpTransportError {
                        message: format!("response header name {name:?}: {error}"),
                    }
                })?,
                http::HeaderValue::from_bytes(&value).map_err(|error| HttpTransportError {
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
            body: XrpcBody::Json(body),
        })
    }
}

/// The entropy source getrandom's `custom` backend links on wasm32-wasip3 (`.cargo/config.toml`).
/// Request ids (`uuid` v7) come from it; it is `wasi:random`, which the host provides.
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
