//! One client type over the two ways to reach a node (ADR 032 section 5).
//!
//! - **Daemon:** a process already serves the node on its Unix socket; calls are `POST
//!   /xrpc/<nsid>` over it, and the kernel's peer credentials make this process the operator.
//! - **In process:** nothing serves the node, so this process boots it for as long as the
//!   client lives and serves the same XRPC router to itself, without a socket. The request goes
//!   through the very router and `Anonymous::Acts(operator)` admission the listener uses, so
//!   routing, admission and the error mapping are not reimplemented here.
//!
//! Commands see only [`NodeClient::call`]; the mode is a fact they can report, not a branch they
//! take.

use crate::context::Booted;
use crate::context::NodeDir;
use crate::context::SocketState;
use crate::interlude::*;

use big_cabinet_core::authority::Authorization;
use big_dispatch::Anonymous;
use serde_json::Value;
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
    Daemon(service_utils_rs::XrpcClient),
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
            let http = service_utils_rs::XrpcClient::unix(dir.socket_path(), MAX_BODY_BYTES)
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
        let booted = Booted::boot(dir, lock, default()).await?;
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

    /// Calls the operation `nsid` through the node's router with `input` as its JSON body.
    pub async fn call(&self, nsid: &str, input: &Value) -> Res<Reply> {
        eyre::ensure!(
            !nsid.is_empty()
                && nsid
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'),
            "`{nsid}` is not an NSID (dot-separated letters, digits and hyphens)"
        );
        match &self.transport {
            Transport::Daemon(client) => {
                let url = client.base_url().join(&format!("xrpc/{nsid}"))?;
                let response = client.http_client().post(url).json(input).send().await?;
                Ok(Reply {
                    status: response.status().as_u16(),
                    body: response.json().await?,
                })
            }
            Transport::InProcess(running) => {
                let router = running.1.clone();
                let request = http::Request::post(format!("/xrpc/{nsid}"))
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(serde_json::to_vec(input)?))?;
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
