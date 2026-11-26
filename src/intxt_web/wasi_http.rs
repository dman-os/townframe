//! The `wasi:http/handler@0.3.0` export: the Dioxus fullstack server, served through axum.
//!
//! The handler turns each wasi request into an axum request, runs the router (server-rendered
//! pages, the server functions, and the embedded client under `/assets`), and writes the response
//! back. The router is built once, because the component's instance is reused across requests.
//!
//! The node is reached with `wasi:http/client`, and found through the environment
//! (`INTXT_WEB_NODE`, `INTXT_WEB_DOC_CAP`, `INTXT_WEB_BLOB_CAP`, `INTXT_WEB_DISPATCH_CAP`): a
//! component's environment is the host's to give at instantiation and nothing about it is known when
//! the module is initialized, so [`crate::server`] reads it per call. None of it is ever sent to the
//! client.

use crate::node::BaseUrl;
use axum::Router;
use axum::routing::get;
use dioxus::server::FullstackState;
use dioxus::server::IndexHtml;
use dioxus::server::ServeConfig;
use dioxus::server::ServerFunction;
use futures_util::StreamExt;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use tower::ServiceExt;

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

include!(concat!(env!("OUT_DIR"), "/client_assets.rs"));

/// The viewer's own stylesheet, served as is. Its colours are the kit's semantic tokens by name
/// (see the file), so no kit bundle is loaded.
const STYLESHEET: &str = include_str!("intxt_web.css");

/// The page the app renders into: the stylesheet, and the client's bootstrap, which loads the wasm
/// and hydrates `#main`.
const INDEX_HTML: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>intxt</title><link rel=\"stylesheet\" href=\"/assets/intxt_web.css\"></head><body><div id=\"main\"></div><script type=\"module\" async>import init from \"/assets/intxt_web.js\"; init({module_or_path: \"/assets/intxt_web_bg.wasm\"}).then((wasm) => { if (wasm.__wbindgen_start == undefined) { wasm.main(); } });</script></body></html>";

/// How long the node has to start answering a request. A page waits on several of these in turn,
/// so a node that never answers must not hold a page open for good.
const FIRST_BYTE_TIMEOUT_NANOS: u64 = 10_000_000_000;

struct Component;
wit::export!(Component with_types_in wit);

impl wit::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        static ROUTER: std::sync::OnceLock<Router> = std::sync::OnceLock::new();
        let router = ROUTER.get_or_init(router).clone();
        let request = into_axum(request).await;
        let response = router
            .oneshot(request)
            .await
            .expect("a router is infallible");
        Ok(into_wasi(response))
    }
}

/// The names of the environment of the component: what [`crate::server`] reads its source from.
pub(crate) fn environment(name: &str) -> Option<String> {
    wit::wasi::cli::environment::get_environment()
        .into_iter()
        .find_map(|(key, value)| (key == name).then_some(value))
}

fn router() -> Router {
    // Renders and server functions hold state that is not `Send`; this component has one thread,
    // so they run on its own executor.
    dioxus::server::set_local_spawner(|task| wit_bindgen::spawn_local(task));
    let index = IndexHtml::new(INDEX_HTML, "main").expect("the index page has the app's root");
    let state = FullstackState::new(ServeConfig::with_index_html(index), crate::app::app);
    let mut router = Router::new();
    for function in ServerFunction::collect() {
        router = router.route(function.path(), function.method_router());
    }
    router
        .route("/assets/{*path}", get(asset))
        .fallback(get(FullstackState::render_handler))
        .with_state(state)
}

/// `/assets/<name>`: the stylesheet, or a file of the embedded client build.
async fn asset(axum::extract::Path(name): axum::extract::Path<String>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (content_type, body): (&str, &[u8]) = if name == "intxt_web.css" {
        ("text/css; charset=utf-8", STYLESHEET.as_bytes())
    } else if let Some((_, bytes)) = CLIENT_ASSETS.iter().find(|(path, _)| *path == name) {
        let content_type = if name.ends_with(".wasm") {
            "application/wasm"
        } else {
            "text/javascript; charset=utf-8"
        };
        (content_type, bytes)
    } else {
        return http::StatusCode::NOT_FOUND.into_response();
    };
    ([(http::header::CONTENT_TYPE, content_type)], body.to_vec()).into_response()
}

async fn into_axum(request: Request) -> http::Request<axum::body::Body> {
    let mut builder = http::Request::builder();
    for (name, value) in request.get_headers().copy_all() {
        builder = builder.header(name, value);
    }
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
        Method::Other(token) => {
            http::Method::from_bytes(token.as_bytes()).expect("a method token the host admitted")
        }
    };
    let mut uri = http::uri::Parts::default();
    uri.scheme = request.get_scheme().map(|scheme| match scheme {
        Scheme::Http => http::uri::Scheme::HTTP,
        Scheme::Https => http::uri::Scheme::HTTPS,
        Scheme::Other(other) => {
            http::uri::Scheme::try_from(other.as_bytes()).expect("a scheme the host admitted")
        }
    });
    uri.authority = request
        .get_authority()
        .map(|authority| authority.parse().expect("an authority the host admitted"));
    uri.path_and_query = request
        .get_path_with_query()
        .map(|target| target.parse().expect("a target the host admitted"));
    let (result_tx, result_rx) = wit::wit_future::new(|| Ok(()));
    let (body, _trailers) = Request::consume_body(request, result_rx);
    let body = body.collect().await;
    result_tx
        .write(Ok(()))
        .await
        .expect("request body receiver disappeared");
    builder
        .method(method)
        .uri(http::Uri::from_parts(uri).expect("the parts of a request the host admitted"))
        .body(axum::body::Body::from(body))
        .expect("a request the host admitted")
}

fn into_wasi(response: axum::response::Response) -> Response {
    let headers = Fields::new();
    for (name, value) in response.headers() {
        headers
            .append(name.as_str(), value.as_bytes())
            .expect("an `http` header is a valid wasi:http field");
    }
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (wasi, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    wasi.set_status_code(response.status().as_u16())
        .expect("a valid status");
    let mut body = response.into_body().into_data_stream();
    wit_bindgen::spawn_local(async move {
        while let Some(chunk) = body.next().await {
            let chunk = chunk.expect("a response body is rendered before it is written");
            let remaining = body_tx.write_all(chunk.to_vec()).await;
            assert!(remaining.is_empty(), "response body receiver went away");
        }
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("response trailers receiver disappeared");
    });
    wasi
}

/// The [`HttpTransport`] over `wasi:http/client`, to the node at `base`. Every body, JSON or the
/// bytes of a blob, is collected whole and handed over as [`XrpcBody::Json`], the container of
/// "the bytes of the answer"; the viewer reads bounded answers only (a blob read asks for a range).
pub(crate) struct WasiHttp {
    base: BaseUrl,
}

impl WasiHttp {
    pub(crate) fn new(base: BaseUrl) -> Self {
        Self { base }
    }
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
