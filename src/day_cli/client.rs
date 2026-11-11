//! One client type over the ways to reach a node (ADR 033 section 5).
//!
//! Whoever holds the node lock serves the node's Unix socket for as long as it holds it, so a
//! node has one process and every other `day` is a client of it.
//!
//! - **Socket:** another process holds the node; calls are `POST /xrpc/<nsid>` over its socket,
//!   and the kernel's peer credentials make this process the operator. If that process is still
//!   booting, the client blocks on the node's boot lock until its socket is up, bounded by the
//!   connect deadline.
//! - **Holder:** nothing holds the node, so this process boots it, serves the socket for other
//!   `day`s, and serves the same XRPC router to itself without a socket. The request goes
//!   through the very router and `Anonymous::Acts(operator)` admission the listener uses, so
//!   routing, admission and the error mapping are not reimplemented here. After its command the
//!   holder stays up until the last client has disconnected.
//!
//! Commands see only [`NodeClient::call`]; the mode is a fact they can report, not a branch they
//! take.

use crate::context::Booted;
use crate::context::LockRole;
use crate::context::NodeDir;
use crate::context::SocketState;
use crate::interlude::*;

use big_cabinet_core::authority::Authorization;
use big_dispatch::Anonymous;
use method_core::HttpMethod;
use serde_json::Value;
use tower::ServiceExt;

/// The bound on one request or response body, the same as the listener's.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// How long a client waits for a node another process is booting, or is shutting down, before it
/// gives up.
pub const CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Mode {
    /// The node is held by `day node run`, with the peers service its `node.json` asks for.
    Daemon,
    /// The node is held by a command (this one, or another running at the same time), which
    /// attaches no peers service.
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
    /// One HTTP/1 connection to the holder's socket, opened in [`NodeClient::acquire`] and never
    /// idled out. The holder counts it, and a holder that has finished its own command accepts
    /// no new connection, so every call goes over this one: `gate` lets one call at a time use
    /// it, which is what keeps the HTTP client from opening a second.
    Socket {
        http: reqwest::Client,
        gate: tokio::sync::Mutex<()>,
    },
    /// The booted node and the router that serves it. They are dropped together on stop: the
    /// router holds the dispatch client, which must not outlive the node it calls.
    Holder(Box<(Booted, axum::Router)>),
}

pub struct NodeClient {
    transport: Transport,
    mode: Mode,
    socket: SocketState,
}

impl NodeClient {
    /// Reaches the node in `dir`: through the socket of the process that holds it, or, if none
    /// does, by booting it here. Finding the node held by a process that is still booting waits
    /// for its socket, and finding it held by one that is shutting down waits for the lock, both
    /// bounded by `deadline`.
    pub async fn connect(dir: &NodeDir, deadline: std::time::Duration) -> Res<Self> {
        let found = dir.probe_socket().await?;
        tokio::time::timeout(deadline, Self::acquire(dir, found))
            .await
            .map_err(|_| {
                eyre::eyre!(
                    "the node at {} stayed held by another process for {deadline:?}",
                    dir.root().display()
                )
            })?
    }

    async fn acquire(dir: &NodeDir, found: SocketState) -> Res<Self> {
        loop {
            let held = match dir.try_lock(LockRole::Command)? {
                Ok(lock) => {
                    let booted = Booted::boot(dir, lock, LockRole::Command).await?;
                    let router = big_dispatch::xrpc_router(
                        booted.node.dispatch.client.clone(),
                        Anonymous::Acts(Authorization::identity(booted.node.authority.root)),
                        MAX_BODY_BYTES,
                    )
                    .into_router();
                    return Ok(Self {
                        transport: Transport::Holder(Box::new((booted, router))),
                        mode: Mode::InProcess,
                        socket: found,
                    });
                }
                Err(held) => held,
            };
            // The holder's socket is up once its boot lock is released.
            let waiting = dir.clone();
            tokio::task::spawn_blocking(move || waiting.wait_until_serving())
                .await
                .expect(ERROR_TOKIO)?;
            let socket = match &held.socket {
                Some(socket) => socket.clone(),
                None => dir.socket_path()?,
            };
            // A holder that has finished its own command closes its socket and exits once the
            // connections it accepted have ended. So this client opens one connection before
            // anything else, proves the holder accepted it by having it answer a request (a
            // connect alone can sit in the backlog of a listener about to close), and makes
            // every call over it. A refusal or a reset means the holder is on its way out.
            let http = reqwest::Client::builder()
                .unix_socket(socket.as_path())
                .pool_max_idle_per_host(1)
                .pool_idle_timeout(None)
                .build()?;
            match http
                .get(format!("http://localhost/xrpc/{}", big_node::GET_NODE_INFO))
                .send()
                .await
            {
                Ok(_) => {
                    return Ok(Self {
                        transport: Transport::Socket {
                            http,
                            gate: tokio::sync::Mutex::new(()),
                        },
                        mode: if held.role == Some(LockRole::Daemon) {
                            Mode::Daemon
                        } else {
                            Mode::InProcess
                        },
                        socket: SocketState::Live,
                    });
                }
                Err(error) if error.is_connect() => {}
                Err(error) => {
                    return Err(error)
                        .wrap_err_with(|| format!("connecting to {}", socket.display()));
                }
            }
            // No socket: the holder is shutting down, or it was only a process probing the lock.
            // Either way the lock is the signal, so wait for it to be free and try again.
            let waiting = dir.clone();
            tokio::task::spawn_blocking(move || waiting.wait_for_lock_release())
                .await
                .expect(ERROR_TOKIO)?;
        }
    }

    pub fn mode(&self) -> Mode {
        self.mode
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
        match &self.transport {
            Transport::Socket { http, gate } => {
                let _one_call_at_a_time = gate.lock().await;
                let url = format!("http://localhost/xrpc/{nsid}");
                let request = match verb {
                    HttpMethod::Post => http.post(url).json(input),
                    HttpMethod::Get => http.get(url).query(&query_pairs(input)?),
                };
                let response = request.send().await?;
                Ok(Reply {
                    status: response.status().as_u16(),
                    body: response.json().await?,
                })
            }
            Transport::Holder(running) => {
                let router = running.1.clone();
                let request = match verb {
                    HttpMethod::Post => http::Request::post(format!("/xrpc/{nsid}"))
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(axum::body::Body::from(serde_json::to_vec(input)?))?,
                    HttpMethod::Get => http::Request::get(format!(
                        "/xrpc/{nsid}?{}",
                        serde_urlencoded::to_string(query_pairs(input)?)?
                    ))
                    .body(axum::body::Body::empty())?,
                };
                let response = router
                    .oneshot(request)
                    .await
                    .unwrap_or_else(|never| match never {});
                let status = response.status().as_u16();
                let bytes = axum::body::to_bytes(response.into_body(), MAX_BODY_BYTES).await?;
                Ok(Reply {
                    status,
                    body: serde_json::from_slice(&bytes)?,
                })
            }
        }
    }

    /// Stops the node this client booted, once no other client is connected to it. A client of
    /// another process's node has nothing to stop.
    pub async fn stop(self) -> Res<()> {
        match self.transport {
            Transport::Socket { .. } => Ok(()),
            Transport::Holder(running) => {
                let (booted, router) = *running;
                drop(router);
                // No new client is accepted from here on; the ones already connected finish.
                booted
                    .node
                    .unix_clients
                    .as_ref()
                    .expect("a holder boots with its socket attached")
                    .drain()
                    .await?;
                booted.stop().await
            }
        }
    }
}

/// Refuses text that cannot be an NSID or an NSID prefix (an authority): what the router is
/// keyed by.
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
