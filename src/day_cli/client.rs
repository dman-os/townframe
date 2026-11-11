//! One client type over the two ways to reach a node (ADR 033 section 5).
//!
//! - **Daemon:** a process already serves the node on its Unix socket; calls are `POST
//!   /xrpc/<nsid>` over it, and the kernel's peer credentials make this process the operator.
//! - **In process:** nothing serves the node, so this process boots it for as long as the
//!   client lives and serves the same XRPC router to itself, without a socket. The request goes
//!   through the very router and `Anonymous::Acts(operator)` admission the listener uses, so
//!   routing, admission and the error mapping are not reimplemented here.
//!
//! Commands see only [`NodeClient::call`], or `&NodeClient` as an `HttpTransport` for a typed
//! client (intxt's recorder); the mode is a fact they can report, not a branch they take.

use crate::context::Booted;
use crate::context::NodeDir;
use crate::context::SocketState;
use crate::interlude::*;

use big_cabinet_core::authority::Authorization;
use big_dispatch::Anonymous;
use method_core::HttpMethod;
use serde_json::Value;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use tower::ServiceExt;

/// The bound on one request or response body, the same as the listener's.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Mode {
    Daemon,
    InProcess,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::InProcess => "in-process",
        }
    }

    /// Why a node reached this way may report no peers service.
    pub fn peers_note(self) -> &'static str {
        match self {
            Self::Daemon => "the daemon's node.json has no peers section",
            Self::InProcess => "only a daemon attaches peers",
        }
    }
}

/// What the node answered: the HTTP status and the JSON body, whatever the transport. A
/// non-success status is the operation's own XRPC error envelope, not a failure of the client.
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
}

impl Reply {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

enum Transport {
    Daemon(service_client_native_rs::XrpcClient),
    /// The booted node and the router that serves it. They are dropped together on stop: the
    /// router holds the dispatch client, which must not outlive the node it calls.
    InProcess(Box<(Booted, axum::Router)>),
}

pub struct NodeClient {
    transport: Transport,
    socket: SocketState,
}

impl NodeClient {
    /// Reaches the node in `dir`: through its daemon if the socket is live, otherwise by booting
    /// it here. A node that something else has booted but is not yet serving (a daemon still
    /// starting, another command) is an error: two processes must not open one node's storage.
    pub async fn connect(dir: &NodeDir) -> Res<Self> {
        let socket = dir.probe_socket().await?;
        if socket == SocketState::Live {
            let http =
                service_client_native_rs::XrpcClient::unix(dir.socket_path()?, MAX_BODY_BYTES)
                    .map_err(|error| eyre::eyre!("{error:?}"))?;
            return Ok(Self {
                transport: Transport::Daemon(http),
                socket,
            });
        }
        let lock = match dir.try_lock()? {
            Ok(lock) => lock,
            Err(held) => eyre::bail!(
                "{held} has the node at {} open but is not serving its socket; try again once it has finished starting",
                dir.root().display()
            ),
        };
        let booted = Booted::boot(dir, lock, false).await?;
        let router = big_dispatch::xrpc_router(
            booted.node.dispatch.client.clone(),
            Anonymous::Acts(Authorization::identity(booted.node.authority.root)),
            MAX_BODY_BYTES,
        )
        .into_router();
        Ok(Self {
            transport: Transport::InProcess(Box::new((booted, router))),
            socket,
        })
    }

    pub fn mode(&self) -> Mode {
        match self.transport {
            Transport::Daemon(_) => Mode::Daemon,
            Transport::InProcess(_) => Mode::InProcess,
        }
    }

    /// The socket's state when the client connected: for an in-process client, whether it found
    /// no socket or a stale one.
    pub fn socket_state(&self) -> SocketState {
        self.socket
    }

    /// Calls the operation `nsid` through the node's router. A procedure (`HttpMethod::Post`)
    /// takes `input` as its JSON body; a query (`HttpMethod::Get`) takes it as its query string,
    /// so `input` must be an object of primitives (or arrays of them), the shape a query's
    /// Lexicon `parameters` take. The node holds the verb to the method's kind.
    pub async fn call(&self, nsid: &str, verb: HttpMethod, input: &Value) -> Res<Reply> {
        ensure_nsid(nsid)?;
        let request = match verb {
            HttpMethod::Post => {
                let mut headers = http::HeaderMap::new();
                headers.insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/json"),
                );
                XrpcRequest {
                    method: http::Method::POST,
                    path: format!("xrpc/{nsid}"),
                    headers,
                    body: serde_json::to_vec(input)?,
                }
            }
            HttpMethod::Get => XrpcRequest {
                method: http::Method::GET,
                path: format!(
                    "xrpc/{nsid}?{}",
                    serde_urlencoded::to_string(query_pairs(input)?)?
                ),
                headers: http::HeaderMap::new(),
                body: Vec::new(),
            },
        };
        let response = self.exchange(request).await?;
        Ok(Reply {
            status: response.status.as_u16(),
            body: serde_json::from_slice(&response.body)?,
        })
    }

    /// Sends one encoded request to the node: over the daemon's socket, or through the router the
    /// client serves itself. The one place either transport is spoken, for [`Self::call`] and
    /// for typed clients over [`HttpTransport`].
    async fn exchange(&self, request: XrpcRequest) -> Res<XrpcResponse> {
        match &self.transport {
            Transport::Daemon(client) => {
                let url = client.base_url().join(&request.path)?;
                let response = client
                    .http_client()
                    .request(request.method, url)
                    .headers(request.headers)
                    .body(request.body)
                    .send()
                    .await?;
                Ok(XrpcResponse {
                    status: response.status(),
                    headers: response.headers().clone(),
                    body: response.bytes().await?.to_vec(),
                })
            }
            Transport::InProcess(running) => {
                let mut builder = http::Request::builder()
                    .method(request.method)
                    .uri(format!("/{}", request.path));
                *builder
                    .headers_mut()
                    .expect("a builder with a valid method and URI has headers") = request.headers;
                let response = running
                    .1
                    .clone()
                    .oneshot(builder.body(axum::body::Body::from(request.body))?)
                    .await
                    .unwrap_or_else(|never| match never {});
                let (parts, body) = response.into_parts();
                Ok(XrpcResponse {
                    status: parts.status,
                    headers: parts.headers,
                    body: axum::body::to_bytes(body, MAX_BODY_BYTES).await?.to_vec(),
                })
            }
        }
    }

    /// Stops the node this client booted. A daemon client has nothing to stop.
    pub async fn stop(self) -> Res<()> {
        match self.transport {
            Transport::Daemon(_) => Ok(()),
            Transport::InProcess(running) => {
                let (booted, router) = *running;
                drop(router);
                booted.stop().await
            }
        }
    }
}

/// The node as a transport for typed clients (`service_client_core_rs`): what a command that
/// makes many dependent calls (intxt's recorder) is built over, with the node's operator as the
/// caller, so it presents no capability of its own.
impl HttpTransport for &NodeClient {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        self.exchange(request)
            .await
            .map_err(|error| HttpTransportError {
                message: format!("{error:#}"),
            })
    }
}

/// Refuses text that cannot be an NSID or an NSID prefix (an authority): what the router and
/// `node.json`'s dispatch scopes are keyed by.
pub fn ensure_nsid(nsid: &str) -> Res<()> {
    eyre::ensure!(
        !nsid.is_empty()
            && nsid
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'),
        "`{nsid}` is not an NSID (dot-separated letters, digits and hyphens)"
    );
    Ok(())
}

/// The query string pairs of a query's `input`: each property a pair, an array a pair per item
/// (atproto's `parameters` are primitives or arrays of them), `null` omitted.
fn query_pairs(input: &Value) -> Res<Vec<(String, String)>> {
    let Value::Object(object) = input else {
        eyre::bail!("a query's input is an object of parameters, not {input}");
    };
    let scalar = |name: &str, value: &Value| -> Res<String> {
        match value {
            Value::String(text) => Ok(text.clone()),
            Value::Bool(_) | Value::Number(_) => Ok(value.to_string()),
            other => eyre::bail!("parameter `{name}` is not a primitive: {other}"),
        }
    };
    let mut pairs = Vec::new();
    for (name, value) in object {
        match value {
            Value::Null => {}
            Value::Array(items) => {
                for item in items {
                    pairs.push((name.clone(), scalar(name, item)?));
                }
            }
            value => pairs.push((name.clone(), scalar(name, value)?)),
        }
    }
    Ok(pairs)
}
